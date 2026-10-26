package aead

import (
	"bufio"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flynn/noise"
	"github.com/jolynch/tx/internal/bufpool"
)

// A record on the wire is
//
//	uint16-BE ciphertext length || kind || AEAD(payload, ad = length || kind)
//
// The 3-byte header travels in the clear and is the AEAD's associated data,
// so it is authenticated with the payload. This is TLS 1.2's layout; TLS 1.3
// also authenticates its header but moves the real type into the plaintext
// to hide it. Here the kind stays out of the plaintext because an AEAD seals
// one contiguous plaintext: a kind byte inside it would force every write to
// copy the payload next to it first, where this layout seals a full record
// straight from the caller's slice and opens one straight into the caller's
// buffer. Unlike Noise transport messages, the associated data is not empty;
// nonces and REKEY follow Noise.
//
// Record kinds:
const (
	recordData   byte = 0
	recordEnd    byte = 1
	recordFinish byte = 2
)

const (
	recordPrefixSize    = 2                    // ciphertext length, as for handshake messages
	recordHeaderSize    = recordPrefixSize + 1 // length and kind
	minRecordCiphertext = aeadTagSize          // an empty END or FINISH
	maxRecordCiphertext = 65535
	// maxEmitPayload keeps each emitted record, header included, within
	// 64 KiB: 2 + 1 + 65517 + 16 = 65536. Readers accept the protocol
	// maximum of 65,519 payload bytes.
	maxEmitPayload = 1<<16 - recordHeaderSize - aeadTagSize
	recordWireSize = 1 << 16

	// writeBatchRecords full records share one socket write.
	writeBatchRecords = 4
	writeBufferSize   = writeBatchRecords * recordWireSize
	// readBufferSize holds a record and the next record's header with room
	// to spare, so the read-ahead rarely has to move a partial record.
	readBufferSize = 128 << 10
	// recordReadAhead is the most one read asks for past the unread start:
	// the largest record plus the next header, so a body and the header
	// that follows it arrive in one receive.
	recordReadAhead = recordHeaderSize + maxRecordCiphertext + recordHeaderSize

	// rekeyInterval rekeys each direction after this many records, about
	// 1 GiB per key at full records.
	rekeyInterval = 16384
)

var (
	errSessionFailed   = errors.New("noise session failed")
	errTrailingBytes   = errors.New("unexpected bytes after the end of the message")
	errMessageUnfinish = errors.New("previous message was not read to its end")
	errMessageEnded    = errors.New("message already ended; start another with Writer")
	// ErrPeerClosed reports a clean close before a message began. It wraps
	// io.EOF, so callers can treat it as a benign disconnect, but it is not
	// io.EOF itself and so never reads as an empty message.
	ErrPeerClosed = fmt.Errorf("peer closed the connection: %w", io.EOF)
)

// recordCipher is one direction's transport cipher. The handshake hands
// over each Split key once; from then on the record layer owns the AEAD, the
// nonce counter, and REKEY, exactly as Noise defines them, so sealing and
// opening allocate nothing. The nonce lives in the struct: 4 zero bytes and
// the 64-bit counter, big-endian for AES-GCM and little-endian for
// ChaCha20-Poly1305.
type recordCipher struct {
	alg   Algorithm
	aead  cipher.AEAD
	n     uint64
	nonce [aeadNonceSize]byte
}

const aeadNonceSize = 12

func newRecordCipher(alg Algorithm, cs *noise.CipherState) (*recordCipher, error) {
	key := cs.UnsafeKey()
	defer clear(key[:])
	a, err := newAEAD(alg, key[:])
	if err != nil {
		return nil, err
	}
	return &recordCipher{alg: alg, aead: a}, nil
}

// setNonce encodes n into c.nonce.
func (c *recordCipher) setNonce(n uint64) []byte {
	if c.alg == AlgorithmAES {
		binary.BigEndian.PutUint64(c.nonce[4:], n)
	} else {
		binary.LittleEndian.PutUint64(c.nonce[4:], n)
	}
	return c.nonce[:]
}

// seal encrypts plaintext into dst with the next nonce, then rekeys after
// every rekeyInterval records. dst may be plaintext[:0].
func (c *recordCipher) seal(dst, plaintext, ad []byte) ([]byte, error) {
	if c.n >= math.MaxUint64 {
		return nil, errors.New("nonce space exhausted")
	}
	out := c.aead.Seal(dst, c.setNonce(c.n), plaintext, ad)
	return out, c.advance()
}

// open decrypts ciphertext into dst with the next nonce. dst may be
// ciphertext[:0].
func (c *recordCipher) open(dst, ciphertext, ad []byte) ([]byte, error) {
	if c.n >= math.MaxUint64 {
		return nil, errors.New("nonce space exhausted")
	}
	out, err := c.aead.Open(dst, c.setNonce(c.n), ciphertext, ad)
	if err != nil {
		return nil, err
	}
	return out, c.advance()
}

// advance moves to the next nonce, rekeying after every rekeyInterval
// records. Both directions count every record, FINISH included.
func (c *recordCipher) advance() error {
	c.n++
	if c.n%rekeyInterval == 0 {
		if err := c.rekey(); err != nil {
			return fmt.Errorf("rekey: %w", err)
		}
	}
	return nil
}

// rekey replaces the key with the first 32 bytes of 32 zero bytes encrypted
// at the reserved nonce 2^64-1, Noise's REKEY. The counter carries on.
func (c *recordCipher) rekey() error {
	var block [32 + aeadTagSize]byte
	out := c.aead.Seal(block[:0], c.setNonce(math.MaxUint64), block[:32], nil)
	a, err := newAEAD(c.alg, out[:32])
	clear(block[:])
	if err != nil {
		return err
	}
	c.aead = a
	return nil
}

// Session is one connection's authenticated Noise transport: the two
// directional record ciphers from Split plus record framing. Each direction
// is guarded by its own mutex, so a misuse that interleaves two writers
// cannot reuse a nonce; commands must still not interleave. Any failure
// poisons the session and closes the connection.
type Session struct {
	conn net.Conn
	br   *bufio.Reader

	// readProgress, when positive, sets a read deadline before each read
	// that has to wait, so every byte received extends it.
	readProgress time.Duration
	// writeProgress, when positive, writes records in pieces of
	// progressWriteBytes, each under a fresh write deadline, so a socket
	// that keeps taking bytes is never cut off mid-batch.
	writeProgress time.Duration

	wmu     sync.Mutex
	send    *recordCipher
	writer  MessageWriter
	wptr    *[]byte // bufpool backing of wbuf
	wbuf    []byte
	woff    int  // end of sealed, unwritten records
	rec     int  // offset of the open record, or -1
	pending int  // payload bytes in the open record
	batch   int  // records per write for the current message
	sealed  int  // records sealed since the last write
	wEnded  bool // End ran for the current message; Writer starts the next

	rmu    sync.Mutex
	recv   *recordCipher
	reader MessageReader
	rptr   *[]byte // bufpool backing of rbuf
	rbuf   []byte
	roff   int // start of received bytes not yet consumed
	rend   int // end of received bytes
	rHigh  int // end of plaintext opened in place in rbuf, for clearing
	unread []byte
	inMsg  bool
	ended  bool
	// started reports whether the current message has received a record.
	started bool

	failed  atomic.Bool
	errMu   sync.Mutex
	failErr error
}

// newSession takes over the Split cipher states' keys; the states are not
// used afterwards.
func newSession(conn net.Conn, br *bufio.Reader, alg Algorithm, send, recv *noise.CipherState) (*Session, error) {
	sc, err := newRecordCipher(alg, send)
	if err != nil {
		return nil, err
	}
	rc, err := newRecordCipher(alg, recv)
	if err != nil {
		return nil, err
	}
	s := &Session{
		conn:  conn,
		br:    br,
		send:  sc,
		recv:  rc,
		rec:   -1,
		batch: writeBatchRecords,
	}
	s.writer.s, s.reader.s = s, s
	s.writer.end, s.writer.flush = s.writer.End, s.writer.Flush
	return s, nil
}

// SetReadProgressDeadline makes every read wait at most d for the peer's
// next bytes, so a record still arriving is progress. Zero stops re-arming
// it but leaves the last deadline set on the connection; clear that with
// SetReadDeadline before reusing the connection.
func (s *Session) SetReadProgressDeadline(d time.Duration) {
	s.rmu.Lock()
	s.readProgress = d
	s.rmu.Unlock()
}

// SetWriteProgressDeadline makes every progressWriteBytes of records written
// wait at most d for the socket to take them. Zero stops re-arming it but
// leaves the last deadline set on the connection; clear that with
// SetWriteDeadline before reusing the connection.
func (s *Session) SetWriteProgressDeadline(d time.Duration) {
	s.wmu.Lock()
	s.writeProgress = d
	s.wmu.Unlock()
}

// progressWriteBytes is the piece a write progress deadline covers. A
// piece must fit under the sender's TCP_NOTSENT_LOWAT, when one is set, so
// that it is accepted as bytes leave rather than after a backlog drains.
const progressWriteBytes = 16 << 10

// Failed reports whether the session is poisoned.
func (s *Session) Failed() bool { return s.failed.Load() }

// Buffered is the number of received bytes read ahead but not yet consumed.
// An idle session has none.
func (s *Session) Buffered() int {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	return s.br.Buffered() + s.rend - s.roff + len(s.unread)
}

// Close poisons the session and closes its connection exactly once; closing
// a session that already failed reports nil. It takes no direction lock, so
// it unblocks an owner stuck in I/O.
func (s *Session) Close() error {
	s.recordErr(net.ErrClosed)
	if s.failed.Swap(true) {
		return nil
	}
	return s.conn.Close()
}

func (s *Session) recordErr(err error) {
	s.errMu.Lock()
	if s.failErr == nil {
		s.failErr = err
	}
	s.errMu.Unlock()
}

func (s *Session) fail(err error) error {
	if err == nil {
		err = errSessionFailed
	}
	s.recordErr(err)
	if !s.failed.Swap(true) {
		_ = s.conn.Close()
	}
	return err
}

func (s *Session) checkFailed() error {
	if s.failed.Load() {
		s.errMu.Lock()
		defer s.errMu.Unlock()
		return fmt.Errorf("%w: %w", errSessionFailed, s.failErr)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

// MessageWriter writes one command or response. Write never forces a record
// boundary; Flush seals what is pending without ending the message; End
// seals the final END record and writes everything.
type MessageWriter struct {
	s *Session
	// end and flush are w.End and w.Flush, bound once so callers can hold
	// them without allocating.
	end   func() error
	flush func() error
}

// EndFunc returns End as a function value without allocating.
func (w *MessageWriter) EndFunc() func() error { return w.end }

// FlushFunc returns Flush as a function value without allocating.
func (w *MessageWriter) FlushFunc() func() error { return w.flush }

// Writer starts a message. A paced message writes each sealed record at once
// instead of batching, so a rate-limited writer keeps its granularity. One
// message is written at a time, so the session reuses one writer.
func (s *Session) Writer(paced bool) *MessageWriter {
	s.wmu.Lock()
	if paced {
		s.batch = 1
	} else {
		s.batch = writeBatchRecords
	}
	s.wEnded = false
	s.wmu.Unlock()
	return &s.writer
}

func (w *MessageWriter) Write(p []byte) (int, error) {
	s := w.s
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if err := s.checkFailed(); err != nil {
		return 0, err
	}
	if s.wEnded {
		return 0, errMessageEnded
	}
	total := 0
	for len(p) > 0 {
		if s.rec < 0 {
			if err := s.openRecordLocked(); err != nil {
				return total, err
			}
		}
		room := s.recordRoomLocked() - s.pending
		if s.pending == 0 && len(p) >= room {
			// A whole record is available: seal it straight from p,
			// skipping the copy into the buffer.
			if err := s.sealPayloadLocked(recordData, p[:room]); err != nil {
				return total, err
			}
			if err := s.afterSealLocked(); err != nil {
				return total, err
			}
			p = p[room:]
			total += room
			continue
		}
		start := s.rec + recordHeaderSize + s.pending
		n := copy(s.wbuf[start:start+room], p)
		s.pending += n
		p = p[n:]
		total += n
		if s.pending == s.recordRoomLocked() {
			if err := s.sealLocked(recordData); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

// Flush seals pending bytes as a DATA record and writes all sealed records.
// With nothing pending it only writes what is queued.
func (w *MessageWriter) Flush() error {
	s := w.s
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if err := s.checkFailed(); err != nil {
		return err
	}
	if s.wEnded {
		return errMessageEnded
	}
	if s.rec >= 0 && s.pending > 0 {
		if err := s.sealRecordLocked(recordData); err != nil {
			return err
		}
	}
	return s.writeQueuedLocked()
}

// End seals the final END record, writes everything, and releases the
// message's buffer.
func (w *MessageWriter) End() error {
	s := w.s
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if err := s.checkFailed(); err != nil {
		s.releaseWriteLocked()
		return err
	}
	if s.wEnded {
		return errMessageEnded
	}
	s.wEnded = true
	if s.rec < 0 {
		if err := s.openRecordLocked(); err != nil {
			return err
		}
	}
	if err := s.sealRecordLocked(recordEnd); err != nil {
		return err
	}
	err := s.writeQueuedLocked()
	s.releaseWriteLocked()
	return err
}

// writeFinish sends the client's session confirmation.
func (s *Session) writeFinish() error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if err := s.openRecordLocked(); err != nil {
		return err
	}
	if err := s.sealRecordLocked(recordFinish); err != nil {
		return err
	}
	err := s.writeQueuedLocked()
	s.releaseWriteLocked()
	return err
}

func (s *Session) openRecordLocked() error {
	if s.wbuf == nil {
		p, err := bufpool.Get(writeBufferSize)
		if err != nil {
			return s.fail(err)
		}
		s.wptr, s.wbuf = p, (*p)[:writeBufferSize]
	}
	// A record needs its header, at least one payload byte, and tag.
	if len(s.wbuf)-s.woff < recordHeaderSize+1+aeadTagSize {
		if err := s.writeQueuedLocked(); err != nil {
			return err
		}
	}
	s.rec = s.woff
	s.pending = 0
	return nil
}

// recordRoomLocked is the payload capacity of the open record.
func (s *Session) recordRoomLocked() int {
	return min(maxEmitPayload, len(s.wbuf)-s.rec-recordHeaderSize-aeadTagSize)
}

func (s *Session) sealLocked(kind byte) error {
	if err := s.sealRecordLocked(kind); err != nil {
		return err
	}
	return s.afterSealLocked()
}

// afterSealLocked writes the queued records once a batch is full or the
// buffer cannot hold another record.
func (s *Session) afterSealLocked() error {
	if s.sealed >= s.batch || len(s.wbuf)-s.woff < recordHeaderSize+1+aeadTagSize {
		return s.writeQueuedLocked()
	}
	return nil
}

// sealRecordLocked encrypts the open record's buffered payload in place;
// the ciphertext overwrites the plaintext exactly, so no plaintext remains
// in the buffer once a record is sealed.
func (s *Session) sealRecordLocked(kind byte) error {
	start := s.rec + recordHeaderSize
	return s.sealPayloadLocked(kind, s.wbuf[start:start+s.pending])
}

// sealPayloadLocked seals payload as the open record. payload is either the
// record's own buffered bytes or a caller's slice of at most the record's
// room; the ciphertext lands after the record header either way. The whole
// header, length and kind, is the associated data.
func (s *Session) sealPayloadLocked(kind byte, payload []byte) error {
	n := len(payload) + aeadTagSize
	if n > maxRecordCiphertext {
		return s.fail(errors.New("sealed record too large"))
	}
	start := s.rec + recordHeaderSize
	hdr := s.wbuf[s.rec:start]
	binary.BigEndian.PutUint16(hdr, uint16(n))
	hdr[recordPrefixSize] = kind
	if _, err := s.send.seal(s.wbuf[start:start], payload, hdr); err != nil {
		return s.fail(fmt.Errorf("seal record: %w", err))
	}
	s.woff = start + n
	s.rec = -1
	s.pending = 0
	s.sealed++
	return nil
}

func (s *Session) writeQueuedLocked() error {
	if s.woff == 0 {
		return nil
	}
	if err := s.writeConnLocked(s.wbuf[:s.woff]); err != nil {
		return s.fail(fmt.Errorf("write records: %w", err))
	}
	s.woff = 0
	s.sealed = 0
	return nil
}

func (s *Session) writeConnLocked(p []byte) error {
	if s.writeProgress <= 0 {
		return writeAll(s.conn, p)
	}
	for len(p) > 0 {
		n := min(len(p), progressWriteBytes)
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.writeProgress))
		if err := writeAll(s.conn, p[:n]); err != nil {
			return err
		}
		p = p[n:]
	}
	return nil
}

// releaseWriteLocked returns the buffer to its pool. Sealed records hold
// only ciphertext; an open record that never sealed holds plaintext, which
// is cleared.
func (s *Session) releaseWriteLocked() {
	if s.wbuf == nil {
		return
	}
	if s.rec >= 0 {
		start := s.rec + recordHeaderSize
		clear(s.wbuf[start : start+s.pending])
	}
	bufpool.Put(s.wptr)
	s.wptr, s.wbuf = nil, nil
	s.woff = 0
	s.rec = -1
	s.pending = 0
	s.sealed = 0
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

// MessageReader reads one command or response and returns io.EOF exactly at
// its authenticated END. It never returns bytes past that END; the session
// keeps any it has read ahead for the next message.
type MessageReader struct {
	s *Session
}

// Reader starts the next incoming message. The previous one must have been
// read to its END.
func (s *Session) Reader() (*MessageReader, error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	if err := s.checkFailed(); err != nil {
		return nil, err
	}
	if s.inMsg && (!s.ended || len(s.unread) > 0) {
		return nil, s.fail(errMessageUnfinish)
	}
	s.inMsg = true
	s.ended = false
	s.started = false
	return &s.reader, nil
}

// Read returns the message's bytes and io.EOF at its END. A record whose
// payload fits in p is opened straight into it, so p may briefly hold
// unauthenticated bytes; the AEAD zeroes them when authentication fails.
// Pass only scratch memory that nothing else observes during the call.
func (r *MessageReader) Read(p []byte) (int, error) {
	s := r.s
	s.rmu.Lock()
	defer s.rmu.Unlock()
	for len(s.unread) == 0 {
		if s.ended {
			s.releaseReadLocked()
			return 0, io.EOF
		}
		if err := s.checkFailed(); err != nil {
			return 0, err
		}
		kind, payload, direct, err := s.readRecordLocked(!s.started, p)
		if err != nil {
			return 0, err
		}
		s.started = true
		switch {
		case kind == recordData && len(payload) > 0:
		case kind == recordEnd:
			s.ended = true
		default:
			return 0, s.fail(fmt.Errorf("unexpected record kind %d", kind))
		}
		if direct {
			// Opened straight into p. An empty END loops to report EOF.
			if len(payload) == 0 {
				continue
			}
			if s.ended {
				s.releaseReadLocked()
			}
			return len(payload), nil
		}
		s.unread = payload
	}
	n := copy(p, s.unread)
	s.unread = s.unread[n:]
	if len(s.unread) == 0 && s.ended {
		s.releaseReadLocked()
	}
	return n, nil
}

// readFinish reads and checks the client's FINISH record. It must be the
// first record the server receives.
func (s *Session) readFinish() error {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	if s.recv.n != 0 {
		return s.fail(errors.New("FINISH is not the first record"))
	}
	kind, payload, _, err := s.readRecordLocked(false, nil)
	defer s.releaseReadLocked()
	if err != nil {
		return err
	}
	if kind != recordFinish || len(payload) != 0 {
		return s.fail(errors.New("first client record is not an empty FINISH"))
	}
	return nil
}

// readRecordLocked reads and decrypts one record. When dst can hold the
// whole payload, the record opens straight into it and direct is true;
// otherwise it opens in place in the read buffer. A clean EOF before any
// header byte of a message's first record is reported as ErrPeerClosed, so a
// peer closing an idle connection reads as a benign disconnect; any other
// truncation is an error.
func (s *Session) readRecordLocked(atMessageStart bool, dst []byte) (kind byte, payload []byte, direct bool, err error) {
	if err := s.fillLocked(recordHeaderSize, recordReadAhead); err != nil {
		if errors.Is(err, io.EOF) && s.rend == s.roff && atMessageStart {
			s.fail(ErrPeerClosed)
			return 0, nil, false, ErrPeerClosed
		}
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, false, s.fail(fmt.Errorf("read record header: %w", err))
	}
	n := int(binary.BigEndian.Uint16(s.rbuf[s.roff:]))
	if n < minRecordCiphertext {
		return 0, nil, false, s.fail(fmt.Errorf("invalid record length %d", n))
	}
	// Read the body and the next record's header together.
	if err := s.fillLocked(recordHeaderSize+n, recordHeaderSize+n+recordHeaderSize); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, false, s.fail(fmt.Errorf("read record: %w", err))
	}
	hdr := s.rbuf[s.roff : s.roff+recordHeaderSize]
	start := s.roff + recordHeaderSize
	ciphertext := s.rbuf[start : start+n]
	out := ciphertext[:0]
	if direct = len(dst) >= n-aeadTagSize; direct {
		out = dst[:0]
	} else {
		s.rHigh = max(s.rHigh, start+n-aeadTagSize)
	}
	kind = hdr[recordPrefixSize]
	payload, err = s.recv.open(out, ciphertext, hdr)
	if err != nil {
		return 0, nil, false, s.fail(fmt.Errorf("record authentication failed: %w", err))
	}
	s.roff = start + n
	return kind, payload, direct, nil
}

// fillLocked reads until need bytes are buffered from roff. No read asks for
// more than want bytes past roff, so a record's body and the next header
// arrive in one receive without running into a partial record that would
// then have to be moved; reads still return as soon as need is met.
func (s *Session) fillLocked(need, want int) error {
	if s.rend-s.roff >= need {
		return nil
	}
	if s.readProgress > 0 {
		// Only a read that has to wait extends the progress deadline.
		_ = s.conn.SetReadDeadline(time.Now().Add(s.readProgress))
	}
	if s.rbuf == nil {
		// Wait for the first byte before taking a buffer, so an idle
		// connection holds none between messages.
		if _, err := s.br.Peek(1); err != nil {
			return err
		}
		p, err := bufpool.Get(readBufferSize)
		if err != nil {
			return err
		}
		s.rptr, s.rbuf = p, (*p)[:readBufferSize]
		s.roff, s.rend = 0, 0
	}
	if s.roff+want > len(s.rbuf) {
		// Move the unconsumed bytes, at most a header in a steady stream,
		// to the front.
		s.rend = copy(s.rbuf, s.rbuf[s.roff:s.rend])
		s.roff = 0
	}
	for s.rend-s.roff < need {
		m, err := s.br.Read(s.rbuf[s.rend : s.roff+want])
		s.rend += m
		if err != nil && s.rend-s.roff < need {
			return err
		}
		if m > 0 && s.readProgress > 0 && s.rend-s.roff < need {
			// Part of the record arrived; wait afresh for the rest.
			_ = s.conn.SetReadDeadline(time.Now().Add(s.readProgress))
		}
	}
	return nil
}

// releaseReadLocked ends a message's use of the read buffer: it clears any
// plaintext opened in place and, unless bytes of the next message are
// already buffered, returns the buffer to the pool.
func (s *Session) releaseReadLocked() {
	s.unread = nil
	if s.rbuf == nil {
		return
	}
	// Clearing is defense in depth, kept unless a trace shows its cost.
	// Plaintext lies below roff or past rend: [roff, rend) holds only
	// ciphertext received since, which is kept.
	clear(s.rbuf[:min(s.rHigh, s.roff)])
	if s.rHigh > s.rend {
		clear(s.rbuf[s.rend:s.rHigh])
	}
	s.rHigh = 0
	if s.roff < s.rend {
		return
	}
	bufpool.Put(s.rptr)
	s.rptr, s.rbuf = nil, nil
	s.roff, s.rend = 0, 0
}

// FinishMessage verifies that the message r reads ended exactly at its
// authenticated END: nothing is buffered, and the next read reports io.EOF.
// r is the outermost parser buffer over a MessageReader, possibly through
// further buffered layers that pass reads through, so trailing bytes cannot
// hide in read-ahead.
func FinishMessage(r io.Reader) error {
	if b, ok := r.(interface{ Buffered() int }); ok && b.Buffered() > 0 {
		return errTrailingBytes
	}
	if br, ok := r.(io.ByteReader); ok {
		for {
			_, err := br.ReadByte()
			switch {
			case err == nil:
				return errTrailingBytes
			case errors.Is(err, io.EOF):
				return nil
			default:
				return err
			}
		}
	}
	var b [1]byte
	for {
		n, err := r.Read(b[:])
		switch {
		case n > 0:
			return errTrailingBytes
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		}
	}
}
