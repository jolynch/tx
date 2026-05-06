package tx

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"filippo.io/age"
	intencoding "github.com/jolynch/tx/internal/filexfer/encoding"
	intlimit "github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/metrics"
	"github.com/jolynch/tx/internal/utils"
	"github.com/zeebo/xxh3"
)

const (
	EncodingIdentity = "identity"
	EncodingZstd     = "zstd"
	EncodingLz4      = "lz4"
)

const (
	LoadStrategyFast   = "fast"
	LoadStrategyGentle = "gentle"
)

const defaultGentleCPUPct = intlimit.DefaultGentleCPUPct
const defaultGentleBWPct = intlimit.DefaultGentleBWPct

func NormalizeGentleCPUPct(v int) int { return intlimit.NormalizeGentleCPUPct(v) }

func NormalizeGentleBWPct(v int) int { return intlimit.NormalizeGentleBWPct(v) }

type DownloadStatus = intencoding.DownloadStatus

type TransferStatus = intencoding.TransferStatus

type ClientOption interface {
	apply(*Client)
}

type clientOptionFunc func(*Client)

func (f clientOptionFunc) apply(c *Client) {
	f(c)
}

func WithContextDialer(dialer func(context.Context, string) (net.Conn, error)) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.contextDialer = dialer
	})
}

func WithFileRequestWindowBytes(windowBytes int64) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.FileRequestWindowBytes = windowBytes
	})
}

func WithBatchMaxBytes(batchMaxBytes int64) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.BatchMaxBytes = batchMaxBytes
	})
}

func WithFrameBufferBytes(bufferBytes int) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.FrameBufferBytes = bufferBytes
	})
}

func WithMaxFrameReadBufferBytes(bufferBytes int) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.MaxFrameReadBufferBytes = bufferBytes
	})
}

func WithAckRequestTimeout(timeout time.Duration) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.AckRequestTimeout = timeout
	})
}

func WithSocketReadBufferBytes(bufferBytes int) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.SocketReadBufferBytes = bufferBytes
	})
}

func WithLoadStrategy(strategy string) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.LoadStrategy = normalizeLoadStrategy(strategy)
	})
}

func WithComp(comp string) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.Comp = normalizeComp(comp)
	})
}

func WithClientAgePublicKey(publicKey string) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.ClientAgePublicKey = strings.TrimSpace(publicKey)
	})
}

func WithClientAgeIdentity(identity string) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.ClientAgeIdentity = strings.TrimSpace(identity)
	})
}

func WithEncryptMode(mode string) ClientOption {
	return clientOptionFunc(func(c *Client) {
		c.EncryptMode = strings.ToLower(strings.TrimSpace(mode))
	})
}

func WithClientAuthTokens(tokens ...string) ClientOption {
	return clientOptionFunc(func(c *Client) {
		var out []string
		for _, t := range tokens {
			t = strings.TrimSpace(t)
			if t != "" {
				out = append(out, t)
			}
		}
		c.ClientAuthTokens = out
	})
}

func normalizeComp(comp string) string {
	switch strings.ToLower(strings.TrimSpace(comp)) {
	case EncodingLz4:
		return EncodingLz4
	case EncodingZstd:
		return EncodingZstd
	case "none", EncodingIdentity:
		return "none"
	case "adapt":
		return "adapt"
	default:
		return ""
	}
}

type Client struct {
	FileAddr                string
	FileRequestWindowBytes  int64
	BatchMaxBytes           int64
	FrameBufferBytes        int
	MaxFrameReadBufferBytes int
	AckRequestTimeout       time.Duration
	SocketReadBufferBytes   int
	WindowConcurrency       int
	LoadStrategy            string
	Comp                    string // adapt|none|lz4|zstd; empty means server default (adapt)
	ClientAgePublicKey      string
	ClientAgeIdentity       string
	EncryptMode             string   // "auto", "aes", or "chacha20" — selects post-AUTH stream cipher
	ClientAuthTokens        []string // tokens presented in the encrypted AUTH blob

	// Context dialer allows clients to setup custom connections
	// For example injecting TLS
	contextDialer func(context.Context, string) (net.Conn, error)

	tcpPoolMu sync.Mutex
	tcpPool   *tcpConnPool

	// bufferPool caches reusable frame-read buffers keyed by bucketed size.
	bufferPool sync.Map // map[int]*sync.Pool

	// lineReaderPool caches reusable *bufio.Reader objects for protocol header reading.
	// bufio.Reader is pooled (not its []byte) because Reset() reuses the internal allocation
	// and its Read(p) passes directly to the socket when len(p) >= internal size, avoiding
	// an extra copy that pooledLineReader would introduce for bulk payload reads.
	lineReaderPool sync.Pool

	// scratchBufferPool caches reusable temporary byte buffers.
	scratchBufferPool sync.Pool

	// clientMetrics owns all counters for this client. Mutate via its methods
	// (e.g. IncSyncConnectionFallback) and read via MetricSnapshot.
	clientMetrics metrics.ClientMetrics
}

type Manifest struct {
	TransferID  string
	Root        string
	Mode        string
	LinkMbps    int64
	Concurrency int
	DeadlineMS  int64
	Entries     []ManifestEntry
	rootEntry   ManifestEntry

	// wireBytes is the size of the manifest in its on-disk/wire FM/1 encoding.
	// Set by parseManifest from len(raw); zero for manifests built in-memory
	// (e.g. scanLocalDir) that have never been serialized.
	wireBytes int64

	// allocatedBytes caches the result of Size(). Zero means "not yet
	// computed". Uses atomic.Int64 so Size() is safe to call concurrently
	// without a mutex.
	allocatedBytes atomic.Int64
}

const manifestEntrySize = int64(unsafe.Sizeof(ManifestEntry{}))

// Size returns the in-memory (mem) and on-disk/wire (disk) byte sizes of the
// manifest. The mem result is cached after the first call. disk is the FM/1
// wire size recorded during parse; zero for manifests built in-memory.
func (m *Manifest) Size() (mem, disk int64) {
	if m == nil {
		return 0, 0
	}
	if cached := m.allocatedBytes.Load(); cached > 0 {
		return cached, m.wireBytes
	}
	total := int64(unsafe.Sizeof(*m))
	total += int64(len(m.TransferID) + len(m.Root) + len(m.Mode))
	total += int64(len(m.rootEntry.Path))
	total += int64(cap(m.Entries)) * manifestEntrySize
	for i := range m.Entries {
		total += int64(len(m.Entries[i].Path))
		total += int64(len(m.Entries[i].LinkPath))
	}
	m.allocatedBytes.Store(total)
	return total, m.wireBytes
}

type ManifestEntry struct {
	Type       byte // 'F', 'H', 'D', 'S'; 0 treated as 'F'
	ID         uint64
	Size       int64
	Mtime      int64
	Mode       os.FileMode
	Path       string
	Progress   ManifestProgress
	LinkTarget int64  // H: target file ID; -1 otherwise
	LinkPath   string // S: symlink target path; "" otherwise
	// PageCache is the optional encoded page-cache residency hint emitted
	// by the server when WithPageCache was set on the manifest request.
	// Decode via internal/filexfer/encoding.DecodePageCacheEntry to recover a
	// pagecache.CacheEntry usable for Touch.
	PageCache []byte
}

type ManifestProgress struct {
	AckBytes     int64
	MetadataDone bool
}

type FileTrailerMetadata struct {
	Size    int64
	MtimeNS int64
	Mode    string
	UID     string
	GID     string
	User    string
	Group   string
}

type FileFrameMeta struct {
	FileID          uint64
	Comp            string
	CompCounts      map[string]uint64
	Offset          int64
	Size            int64
	WireSize        int64
	MaxWireSizeHint int64
	HeaderTS        int64
	TrailerTS       int64
	HashToken       string
	FileHashToken   string
	TrailerMetadata *FileTrailerMetadata
}

type DownloadProgressUpdate struct {
	TransferID  string
	FileID      uint64
	CopiedBytes int64
	TargetBytes int64
	AckBytes    int64
	UpdateTime  time.Time
}

type DownloadFileResponse struct {
	Meta                 FileFrameMeta
	LocalFileHash        string
	WindowChecksumPassed int
	WindowChecksumTotal  int
}

type GetFilesRequest struct {
	Manifest     *Manifest
	FileIDs      []uint64
	OutputWriter func(ManifestEntry, int64) (io.WriteCloser, func() error, error)
	// BatchMaxBytes is the unit of parallel work for a single large file: when a file
	// exceeds BatchMaxBytes, it is split into windows of this size and downloaded
	// concurrently. The number of concurrent windows is bounded by
	// FileRequestWindowBytes/BatchMaxBytes unless SplitWindowWorkers is set to a
	// lower cap. BatchMaxBytes must be <= Client.FileRequestWindowBytes.
	BatchMaxBytes int64
	// SplitWindowWorkers caps the number of concurrent windows for a single large
	// file. Zero uses the legacy behavior of deriving concurrency from
	// FileRequestWindowBytes/BatchMaxBytes.
	SplitWindowWorkers int
	ProgressUpdates    chan<- DownloadProgressUpdate
}

// emitProgress sends a progress update on the ProgressUpdates channel if set.
// The send is non-blocking so a slow consumer never stalls downloads.
func (r *GetFilesRequest) emitProgress(update DownloadProgressUpdate) {
	if r.ProgressUpdates == nil {
		return
	}
	select {
	case r.ProgressUpdates <- update:
	default:
	}
}

type GetFilesResponse struct {
	Files []DownloadFileResponse
}

type StartFromManifestRequest struct {
	Manifest     *Manifest
	Entries      []ManifestEntry
	OutputWriter func(ManifestEntry, int64) (io.WriteCloser, func() error, error)
	Concurrency  int
	// BatchMaxBytes is the unit of parallel work per file. See GetFilesRequest.BatchMaxBytes.
	BatchMaxBytes int64
	// SplitWindowWorkers caps per-file split-window concurrency. See
	// GetFilesRequest.SplitWindowWorkers.
	SplitWindowWorkers int
	ProgressUpdates    chan<- DownloadProgressUpdate
	OnFileDone         func(StartFileDoneEvent)
}

type StartFileDoneEvent struct {
	File    DownloadFileResponse
	Elapsed time.Duration
}

type StartFromManifestResponse struct {
	Requested        int
	Downloaded       int
	Failed           int
	TransferredBytes int64
	Errors           []error
}

type GetManifestRequest struct {
	Directory   string
	Verbose     bool
	Mode        string
	LinkMbps    int64
	Concurrency int
	DeadlineMS  int64
	// WithPageCache, when true, asks the server to attach a per-file
	// page-cache residency hint to each manifest entry. Decode via
	// encoding.DecodePageCacheEntry to recover a pagecache.CacheEntry.
	WithPageCache bool
	// Comp selects the wire compression for the manifest stream. Empty
	// defaults to "zstd". Use "none" for an uncompressed FM/1 response.
	Comp string
	// RawSink, when non-nil and Comp=="zstd", receives compressed wire
	// payload bytes (concatenated FX/1 payloads, one per frame) as they are
	// read. The resulting bytes form a multi-frame zstd archive suitable for
	// writing directly to a .zst file, avoiding a second in-memory copy of the
	// compressed manifest while the decoded FM/1 is parsed.
	RawSink io.Writer
	// ManifestProgress, if non-nil, receives one update per FX/1 chunk as
	// the manifest streams in. Sends are non-blocking — if the channel is
	// full, updates are dropped. The caller owns the channel's lifecycle
	// (the client never closes it). Mirrors the SEND-side ProgressUpdates
	// pattern.
	ManifestProgress chan<- ManifestProgressUpdate
}

// ManifestProgressUpdate carries per-FX/1-chunk progress for a streaming
// TXFER response. Sent to GetManifestRequest.ManifestProgress as each
// chunk is received.
type ManifestProgressUpdate struct {
	FrameIndex   int   // 0-based chunk index
	WireBytes    int64 // wire bytes in this chunk (compressed length for comp=zstd)
	LogicalBytes int64 // uncompressed bytes in this chunk
	TotalWire    int64 // cumulative wire bytes received
	TotalLogical int64 // cumulative logical bytes accumulated
	Terminal     bool  // true on the final chunk (FXT/1 next=0)
}

type ProbeRequest struct {
	// ProbeBytes is the payload size for the throughput-measurement phase.
	// ProbeBytes <= 1 runs discovery only (1-byte round trip, no throughput phase).
	ProbeBytes int64
	// Parallelism controls the throughput phase:
	//   0: auto — fast mode fans out to ServerCPU conns; gentle runs linear samples.
	//   1: single-connection probe regardless of mode (cheap in-flight refresh).
	//   >1: forced fanout to this many parallel connections.
	Parallelism      int
	LoadStrategy     string
	TransferID       string
	ObservedLinkMbps int64
}

type ProbeResponse struct {
	ServerCPU              int
	ServerIODepth          int
	GentleCPUPct           int
	GentleBWPct            int
	AvgLatencyMS           int64 // RTT from the 1-byte discovery probe
	LinkMbps               int64 // primary link estimate (aggregate in fast, average in gentle)
	PerConnMbps            int64 // median per-connection throughput
	AggregateMbps          int64 // sum across parallel conns (== LinkMbps in fast, equal to PerConnMbps in gentle)
	ParallelConns          int   // connections used in the throughput phase
	SuggestedConcurrency   int
	WarmConnectionPoolSize int // warmed client connection pool target opened after probe
	ServerSendBufBytes     int64
	SuggestedCipher        string // resolved cipher suggested for this connection (e.g. "aes", "chacha20", or "" if none)
	ServerLimiterBps       int64  // server's current rate limiter in bytes/sec (0 = unlimited)
}

type GetManifestResponse struct {
	Manifest *Manifest
}

type SyncManifestRequest struct {
	Directory   string
	OldManifest *Manifest
	Mode        string
	LinkMbps    int64
	Concurrency int
	DeadlineMS  int64
}

type SyncManifestResponse struct {
	Manifest     *Manifest
	RemovedPaths []string
}

type FetchFileTarget struct {
	FileID   uint64
	FullPath string
	Offset   int64
	Size     int64
	Comp     string // adapt|none|lz4|zstd; empty means server default (adapt)
}

// RootFileID is the manifest file ID reserved for the transfer root.
const RootFileID = intencoding.RootFileID

type GetChecksumRequest struct {
	TransferID string
	Targets    []ChecksumTarget
}

type ChecksumTarget struct {
	FileID   uint64
	FullPath string
	Offset   int64
	Size     int64
	Algo     string // xxh128|xxh64; empty means server default (xxh128)
}

type GetChecksumResponse struct {
	Reader io.ReadCloser
}

type AcknowledgeFileProgressRequest struct {
	TransferID string
	FileID     uint64
	FullPath   string
	AckBytes   int64
	ServerTS   int64
	HashToken  string
	DeltaBytes int64
	RecvMS     int64
	SyncMS     int64
}

type AcknowledgeFileProgressResponse struct{}

type GetStatusRequest struct {
	TransferID string
}

type GetStatusResponse struct {
	Status *TransferStatus
}

type ListStatusesRequest struct{}

type ListStatusesResponse struct {
	Statuses []TransferStatus
}

type fileMissingError struct {
	Status int
	Body   string
}

func (e *fileMissingError) Error() string {
	return fmt.Sprintf("status=%d body=%s", e.Status, e.Body)
}

var ErrFileMissing = errors.New("file missing")

const (
	// Window and batch sizes. The window is max in-flight bytes per file; the batch is
	// the unit of parallel work. parallelism = window / batch.
	defaultClientRequestWindowBytes      int64 = 512 * 1024 * 1024
	defaultClientMaxFrameReadBufferBytes int   = 64 * 1024 * 1024
	defaultClientBatchMaxBytes           int64 = 64 * 1024 * 1024

	// Frame read-buffer bounds, scaled per stream based on file/frame size.
	defaultClientFrameBufferBytes int = 8 * 1024 * 1024
	minClientFrameReadBufferBytes int = 32 * 1024

	// Line reader buffers used for protocol header reading (matches minClientFrameReadBufferBytes).
	defaultClientLineReaderBytes int = 32 * 1024

	// Scratch buffers used for request/response payloads (headers, manifests, etc.).
	defaultClientScratchBufferBytes int = 64 * 1024
	maxClientScratchBufferPoolBytes int = 16 * 1024 * 1024

	defaultClientAckRequestTimeout = 15 * time.Second
	defaultClientLoadStrategy      = LoadStrategyFast

	// Probe parameters for server capability detection and concurrency selection.
	defaultClientProbeBytes   int64 = 1 * 1024 * 1024
	defaultClientProbeSamples       = 3

	// Concurrency bounds for StartFromManifest auto-selection.
	minAutoStartConcurrency = 2
	maxAutoStartConcurrency = 256
)

func NewClient(fileAddr string, opts ...ClientOption) *Client {
	trimmed := strings.TrimSpace(fileAddr)
	c := &Client{
		FileAddr:                trimmed,
		FileRequestWindowBytes:  defaultClientRequestWindowBytes,
		FrameBufferBytes:        defaultClientFrameBufferBytes,
		MaxFrameReadBufferBytes: defaultClientMaxFrameReadBufferBytes,
		AckRequestTimeout:       defaultClientAckRequestTimeout,
		SocketReadBufferBytes:   utils.MaxSocketReadBufferBytes(),
		LoadStrategy:            defaultClientLoadStrategy,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt.apply(c)
	}
	c.bufferPool = sync.Map{}
	c.lineReaderPool = sync.Pool{
		New: func() any {
			return bufio.NewReaderSize(nil, defaultClientLineReaderBytes)
		},
	}
	c.scratchBufferPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, defaultClientScratchBufferBytes))
		},
	}
	return c
}

// Close releases any warmed TCP pool state owned by the client. It is safe to
// call multiple times.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.tcpPoolMu.Lock()
	pool := c.tcpPool
	c.tcpPool = nil
	c.tcpPoolMu.Unlock()
	if pool != nil {
		pool.stop()
	}
	return nil
}

// ClientMetrics is a point-in-time snapshot of client-side counters. New
// fields may be added without breaking callers.
type ClientMetrics = metrics.ClientMetricsSnapshot

// MetricSnapshot returns a copy of the client's current metric counters.
func (c *Client) MetricSnapshot() ClientMetrics {
	if c == nil {
		return ClientMetrics{}
	}
	return c.clientMetrics.Snapshot()
}

func (c *Client) GetManifest(ctx context.Context, request GetManifestRequest) (GetManifestResponse, error) {
	ctx, task := trace.NewTask(ctx, "fetch-manifest")
	defer task.End()
	if c == nil {
		return GetManifestResponse{}, errors.New("nil client")
	}
	if request.Directory == "" {
		return GetManifestResponse{}, errors.New("missing directory")
	}
	request.Mode = strings.ToLower(strings.TrimSpace(request.Mode))
	if request.Mode != LoadStrategyFast && request.Mode != LoadStrategyGentle {
		return GetManifestResponse{}, errors.New("invalid mode")
	}
	if request.LinkMbps < 0 {
		return GetManifestResponse{}, errors.New("link mbps must be >= 0")
	}
	if request.Concurrency <= 0 {
		return GetManifestResponse{}, errors.New("concurrency must be > 0")
	}
	return c.getManifestTCP(ctx, request)
}

func (c *Client) SyncManifest(ctx context.Context, request SyncManifestRequest) (SyncManifestResponse, error) {
	ctx, task := trace.NewTask(ctx, "sync-manifest")
	defer task.End()
	if c == nil {
		return SyncManifestResponse{}, errors.New("nil client")
	}
	if request.Directory == "" {
		return SyncManifestResponse{}, errors.New("missing directory")
	}
	if request.OldManifest == nil {
		return SyncManifestResponse{}, errors.New("missing old manifest")
	}
	request.Mode = strings.ToLower(strings.TrimSpace(request.Mode))
	if request.Mode != LoadStrategyFast && request.Mode != LoadStrategyGentle {
		return SyncManifestResponse{}, errors.New("invalid mode")
	}
	if request.LinkMbps < 0 {
		return SyncManifestResponse{}, errors.New("link mbps must be >= 0")
	}
	if request.Concurrency <= 0 {
		return SyncManifestResponse{}, errors.New("concurrency must be > 0")
	}
	return c.syncManifestTCP(ctx, request)
}

func DefaultClientConcurrency() int {
	n := runtime.NumCPU() * 2
	if n < 1 {
		return 1
	}
	return n
}

func normalizeLoadStrategy(strategy string) string {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case LoadStrategyGentle:
		return LoadStrategyGentle
	default:
		return LoadStrategyFast
	}
}

func SaveManifest(path string, manifest *Manifest) error {
	if manifest == nil {
		return errors.New("nil manifest")
	}
	if path == "" {
		return errors.New("missing path")
	}
	raw, err := marshalManifest(manifest)
	if err != nil {
		return err
	}
	if strings.HasSuffix(path, ".zst") {
		raw, err = intencoding.CompressZstd(raw)
		if err != nil {
			return fmt.Errorf("compress manifest: %w", err)
		}
	}
	parent := filepath.Dir(path)
	if parent != "." && parent != "" {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("create manifest parent directory: %w", err)
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename manifest: %w", err)
	}
	return nil
}

func MarshalManifest(manifest *Manifest) ([]byte, error) {
	return marshalManifest(manifest)
}

func ParseManifest(raw []byte) (*Manifest, error) {
	return parseManifest(raw)
}

func LoadManifest(path string) (*Manifest, error) {
	if path == "" {
		return nil, errors.New("missing path")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	if strings.HasSuffix(path, ".zst") {
		raw, err = intencoding.DecompressZstd(raw)
		if err != nil {
			return nil, fmt.Errorf("decompress manifest: %w", err)
		}
	}
	return parseManifest(raw)
}

// ManifestFingerprint returns a stable hex-encoded xxh3-128 hash of the
// manifest's entry list. Header fields (TransferID, Mode, LinkMbps,
// Concurrency, DeadlineMS, Root) are excluded because they change per
// protocol call. Entry IDs are also excluded because SYNC reassigns them
// from filesystem walk order.
func ManifestFingerprint(m *Manifest) string {
	if m == nil || len(m.Entries) == 0 {
		var zero xxh3.Uint128
		return fmt.Sprintf("%016x%016x", zero.Hi, zero.Lo)
	}
	indices := make([]int, len(m.Entries))
	for i := range m.Entries {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return m.Entries[indices[i]].Path < m.Entries[indices[j]].Path
	})
	pathByID := make(map[uint64]string, len(m.Entries))
	for _, entry := range m.Entries {
		pathByID[entry.ID] = entry.Path
	}
	h := xxh3.New()
	var scratch [8]byte
	writeUint64 := func(v uint64) {
		scratch[0] = byte(v)
		scratch[1] = byte(v >> 8)
		scratch[2] = byte(v >> 16)
		scratch[3] = byte(v >> 24)
		scratch[4] = byte(v >> 32)
		scratch[5] = byte(v >> 40)
		scratch[6] = byte(v >> 48)
		scratch[7] = byte(v >> 56)
		_, _ = h.Write(scratch[:])
	}
	writeString := func(s string) {
		writeUint64(uint64(len(s)))
		_, _ = h.WriteString(s)
	}
	for _, idx := range indices {
		e := m.Entries[idx]
		entryType := e.Type
		if entryType == 0 {
			entryType = intencoding.EntryTypeFile
		}
		_, _ = h.Write([]byte{entryType})
		writeString(e.Path)
		writeUint64(uint64(e.Size))
		writeUint64(uint64(e.Mtime))
		writeUint64(uint64(e.Mode))
		switch entryType {
		case intencoding.EntryTypeHard:
			writeString(pathByID[uint64(e.LinkTarget)])
		case intencoding.EntryTypeSymlink:
			writeString(e.LinkPath)
		}
	}
	sum := h.Sum128()
	return fmt.Sprintf("%016x%016x", sum.Hi, sum.Lo)
}

func (m *Manifest) EntryByID(id uint64) (ManifestEntry, bool) {
	if m == nil {
		return ManifestEntry{}, false
	}
	for _, entry := range m.Entries {
		if entry.ID == id {
			return entry, true
		}
	}
	return ManifestEntry{}, false
}

func (c *Client) fetchFileWindow(
	ctx context.Context,
	txferID string,
	fileID uint64,
	fullPath string,
	offset int64,
	size int64,
) (io.ReadCloser, *FileFrameMeta, error) {
	if c == nil {
		return nil, nil, errors.New("nil client")
	}
	if txferID == "" {
		return nil, nil, errors.New("missing transfer id")
	}
	if fullPath == "" {
		return nil, nil, errors.New("missing full path")
	}
	target := FetchFileTarget{
		FileID:   fileID,
		FullPath: fullPath,
		Offset:   offset,
		Comp:     c.Comp,
	}
	if size > 0 {
		target.Size = size
	}
	stream, err := c.fetchFileBatchTCP(
		ctx,
		txferID,
		[]FetchFileTarget{target},
	)
	if err != nil {
		return nil, nil, err
	}
	fileStream, meta, err := c.newFileStream(stream, "", target.Size)
	if err != nil {
		_ = stream.Close()
		return nil, nil, err
	}
	if meta.FileID != fileID {
		_ = fileStream.Close()
		return nil, nil, fmt.Errorf("file id mismatch: expected %d got %d", fileID, meta.FileID)
	}
	return fileStream, meta, nil
}

// GetEntryMetadata issues metadata-only SEND requests for directory entries.
// The pathsByID map is keyed by manifest file ID and stores the absolute remote
// path for each entry. File ID 0 may be used for the transfer root.
func (c *Client) GetEntryMetadata(ctx context.Context, transferID string, pathsByID map[uint64]string) (map[uint64]*FileTrailerMetadata, error) {
	if c == nil {
		return nil, errors.New("nil client")
	}
	if transferID == "" {
		return nil, errors.New("missing transfer id")
	}
	if len(pathsByID) == 0 {
		return nil, errors.New("missing metadata targets")
	}

	ids := make([]uint64, 0, len(pathsByID))
	for fileID, fullPath := range pathsByID {
		if strings.TrimSpace(fullPath) == "" {
			return nil, fmt.Errorf("metadata target id=%d missing full path", fileID)
		}
		ids = append(ids, fileID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	requestTargets := make([]FetchFileTarget, 0, len(ids))
	for _, fileID := range ids {
		requestTargets = append(requestTargets, FetchFileTarget{
			FileID:   fileID,
			FullPath: pathsByID[fileID],
			Comp:     "none",
		})
	}
	stream, err := c.fetchFileBatchTCP(ctx, transferID, requestTargets)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	br := c.acquireLineReader(stream)
	defer c.releaseLineReader(br)

	results := make(map[uint64]*FileTrailerMetadata, len(ids))
	for i, fileID := range ids {
		meta, err := readEntryMetadataFrame(br, fileID)
		if err != nil {
			return nil, fmt.Errorf("entry metadata target %d id=%d: %w", i, fileID, err)
		}
		results[fileID] = meta
	}
	statusLine, err := readTCPLine(br, maxTCPLineBytes)
	if err != nil {
		return nil, fmt.Errorf("read metadata terminal status: %w", err)
	}
	if err := parseErrControlFrame(statusLine); err != nil {
		return nil, err
	}
	if _, ok := parseOKStatusLine(statusLine); !ok {
		return nil, fmt.Errorf("unexpected metadata terminal status: %s", strings.TrimSpace(statusLine))
	}
	return results, nil
}

func readEntryMetadataFrame(br *bufio.Reader, fileID uint64) (*FileTrailerMetadata, error) {
	headerLine, err := br.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read frame header: %w", err)
	}
	headerTrimmed := strings.TrimRight(headerLine, "\r\n")
	if isStatusLine(headerTrimmed) {
		return nil, fmt.Errorf("unexpected status line before metadata frame: %s", headerTrimmed)
	}
	frameMeta, err := parseFXHeader(headerTrimmed)
	if err != nil {
		return nil, err
	}
	if frameMeta.FileID != fileID {
		return nil, fmt.Errorf("file id mismatch: expected=%d got=%d", fileID, frameMeta.FileID)
	}
	if frameMeta.Offset != 0 || frameMeta.Size != 0 || frameMeta.WireSize != 0 {
		return nil, fmt.Errorf("metadata frame must be empty: offset=%d size=%d wsize=%d", frameMeta.Offset, frameMeta.Size, frameMeta.WireSize)
	}
	if frameMeta.Comp != "none" {
		return nil, fmt.Errorf("metadata frame comp=%q, want none", frameMeta.Comp)
	}
	trailerLine, err := br.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read frame trailer: %w", err)
	}
	trailer, err := parseFXTrailer(strings.TrimRight(trailerLine, "\r\n"))
	if err != nil {
		return nil, err
	}
	if trailer.FileID != fileID {
		return nil, fmt.Errorf("trailer file id mismatch: expected=%d got=%d", fileID, trailer.FileID)
	}
	if trailer.Next == nil || *trailer.Next != 0 {
		return nil, errors.New("metadata trailer must be terminal next=0")
	}
	if trailer.Metadata == nil {
		return nil, errors.New("metadata trailer missing meta fields")
	}
	if trailer.FileHashToken == "" {
		return nil, errors.New("metadata trailer missing file hash")
	}
	expectedHash := intencoding.FormatXXH128HashToken(xxh3.Hash128(nil))
	if !strings.EqualFold(trailer.FileHashToken, expectedHash) {
		return nil, fmt.Errorf("metadata frame hash mismatch: got=%s want=%s", trailer.FileHashToken, expectedHash)
	}
	return cloneTrailerMetadata(trailer.Metadata), nil
}

func (c *Client) GetChecksum(ctx context.Context, request GetChecksumRequest) (GetChecksumResponse, error) {
	if c == nil {
		return GetChecksumResponse{}, errors.New("nil client")
	}
	if request.TransferID == "" {
		return GetChecksumResponse{}, errors.New("missing transfer id")
	}
	if len(request.Targets) == 0 {
		return GetChecksumResponse{}, errors.New("missing checksum targets")
	}
	for _, target := range request.Targets {
		if target.FullPath == "" {
			return GetChecksumResponse{}, errors.New("missing full path")
		}
		if target.Offset < 0 {
			return GetChecksumResponse{}, errors.New("invalid checksum offset")
		}
		if target.Size < 0 {
			return GetChecksumResponse{}, errors.New("invalid checksum size")
		}
	}

	reader, err := c.getChecksumTCP(ctx, request)
	if err != nil {
		return GetChecksumResponse{}, err
	}
	return GetChecksumResponse{Reader: reader}, nil
}

func (c *Client) acknowledgeMissingFile(ctx context.Context, transferID string, fileID uint64, fullPath string) error {
	ackTimeout := c.AckRequestTimeout
	if ackTimeout <= 0 {
		ackTimeout = defaultClientAckRequestTimeout
	}
	return retryAck(ctx, func(callCtx context.Context) error {
		ackCtx, cancel := context.WithTimeout(callCtx, ackTimeout)
		defer cancel()
		_, err := c.AcknowledgeFileProgress(ackCtx, AcknowledgeFileProgressRequest{
			TransferID: transferID,
			FileID:     fileID,
			FullPath:   fullPath,
			AckBytes:   -1,
		})
		return err
	})
}

func (c *Client) GetStatus(ctx context.Context, request GetStatusRequest) (GetStatusResponse, error) {
	if c == nil {
		return GetStatusResponse{}, errors.New("nil client")
	}
	if request.TransferID == "" {
		return GetStatusResponse{}, errors.New("missing transfer id")
	}
	return c.getStatusTCP(ctx, request)
}

func (c *Client) ListStatuses(ctx context.Context, request ListStatusesRequest) (ListStatusesResponse, error) {
	if c == nil {
		return ListStatusesResponse{}, errors.New("nil client")
	}
	return c.listStatusesTCP(ctx, request)
}

type downloadBatchPlan struct {
	entry      ManifestEntry
	serverPath string
	resumeFrom int64
}

type splitWindow struct {
	start int64
	size  int64
	end   int64
}

type splitWindowResult struct {
	window   splitWindow
	response DownloadFileResponse
	ack      AcknowledgeFileProgressRequest
}

func (c *Client) GetFiles(ctx context.Context, req GetFilesRequest) (GetFilesResponse, error) {
	ctx, task := trace.NewTask(ctx, "download-batch")
	defer task.End()
	if req.Manifest == nil {
		return GetFilesResponse{}, errors.New("nil manifest")
	}
	if len(req.FileIDs) == 0 {
		return GetFilesResponse{}, errors.New("empty file batch")
	}
	if req.OutputWriter == nil {
		return GetFilesResponse{}, errors.New("missing output writer callback")
	}
	explicitBatch := req.BatchMaxBytes > 0 || (c != nil && c.BatchMaxBytes > 0)
	req.BatchMaxBytes = c.effectiveBatchMaxBytes(req.BatchMaxBytes)
	windowBytes := c.FileRequestWindowBytes
	if windowBytes <= 0 {
		windowBytes = defaultClientRequestWindowBytes
	}
	if explicitBatch && req.BatchMaxBytes > windowBytes {
		return GetFilesResponse{}, fmt.Errorf(
			"batch size %d exceeds window size %d: batch is the unit of parallel work per file, window is the max outstanding bytes across all concurrent requests for that file",
			req.BatchMaxBytes, windowBytes,
		)
	}

	plans := make([]downloadBatchPlan, 0, len(req.FileIDs))
	targets := make([]FetchFileTarget, 0, len(req.FileIDs))
	for _, fileID := range req.FileIDs {
		entry, serverPath, err := resolveManifestEntryPath(req.Manifest, fileID)
		if err != nil {
			return GetFilesResponse{}, err
		}
		resumeFrom := entry.Progress.AckBytes
		if resumeFrom < 0 {
			return GetFilesResponse{}, fmt.Errorf("file %d resume offset must be >= 0", fileID)
		}
		if entry.Size >= 0 && resumeFrom > entry.Size {
			return GetFilesResponse{}, fmt.Errorf("file %d resume offset %d exceeds file size %d", fileID, resumeFrom, entry.Size)
		}
		plans = append(plans, downloadBatchPlan{
			entry:      entry,
			serverPath: serverPath,
			resumeFrom: resumeFrom,
		})
		target := FetchFileTarget{FileID: fileID, FullPath: serverPath, Offset: resumeFrom, Comp: c.Comp}
		if entry.Size > resumeFrom {
			target.Size = entry.Size - resumeFrom
		}
		targets = append(targets, target)
	}

	if shouldSplitSingleFileBatch(req, plans) {
		return c.downloadManifestBatchWindows(ctx, req, plans[0])
	}
	return c.downloadManifestBatchSequential(ctx, req, plans, targets)
}

func shouldSplitSingleFileBatch(req GetFilesRequest, plans []downloadBatchPlan) bool {
	if len(plans) != 1 || req.BatchMaxBytes <= 0 {
		return false
	}
	entry := plans[0].entry
	if entry.Size < 0 {
		return false
	}
	return entry.Size-plans[0].resumeFrom > req.BatchMaxBytes
}

type seqAckProgress struct {
	fileID      uint64
	ackBytes    int64
	targetBytes int64
}

// downloadManifestGroupSequential downloads a contiguous group of files sequentially
// over a single TCP stream. It returns the file results and pending acks but does NOT
// send the ACK itself — that is the caller's responsibility so acks can be batched
// across parallel groups.
func (c *Client) downloadManifestGroupSequential(
	ctx context.Context,
	req GetFilesRequest,
	plans []downloadBatchPlan,
	targets []FetchFileTarget,
	emitProgressUpdate func(DownloadProgressUpdate),
) ([]DownloadFileResponse, []AcknowledgeFileProgressRequest, []seqAckProgress, error) {
	stream, err := c.fetchFileBatchTCP(ctx, req.Manifest.TransferID, targets)
	if err != nil {
		var missingErr *fileMissingError
		if len(plans) == 1 && errors.Is(err, ErrFileMissing) && errors.As(err, &missingErr) && shouldAcknowledgeMissing404(missingErr.Body) {
			if ackErr := c.acknowledgeMissingFile(ctx, req.Manifest.TransferID, plans[0].entry.ID, plans[0].serverPath); ackErr != nil {
				return nil, nil, nil, fmt.Errorf("%w (failed to ack missing: %v)", err, ackErr)
			}
		}
		return nil, nil, nil, err
	}
	defer stream.Close()
	br := c.acquireLineReader(stream)
	defer c.releaseLineReader(br)

	results := make([]DownloadFileResponse, 0, len(plans))
	pendingAcks := make([]AcknowledgeFileProgressRequest, 0, len(plans))
	ackProgresses := make([]seqAckProgress, 0, len(plans))
	var (
		frameBuf        []byte
		releaseFrameBuf func()
	)
	defer func() {
		if releaseFrameBuf != nil {
			releaseFrameBuf()
		}
	}()

	for _, plan := range plans {
		fileCtx, fileTask := trace.NewTask(ctx, "download-file")
		writer, syncOutput, err := req.OutputWriter(plan.entry, plan.resumeFrom)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("create output writer for file %d: %w", plan.entry.ID, err)
		}
		if writer == nil {
			return nil, nil, nil, fmt.Errorf("create output writer for file %d: nil writer", plan.entry.ID)
		}
		if syncOutput == nil {
			syncOutput = func() error { return nil }
		}
		writerClosed := false
		closeWriter := func() error {
			if writerClosed {
				return nil
			}
			writerClosed = true
			return writer.Close()
		}

		if plan.resumeFrom > 0 {
			emitProgressUpdate(DownloadProgressUpdate{
				TransferID:  req.Manifest.TransferID,
				FileID:      plan.entry.ID,
				CopiedBytes: plan.resumeFrom,
				TargetBytes: plan.entry.Size,
				UpdateTime:  time.Now(),
			})
		}

		fileStart := time.Now()
		meta := FileFrameMeta{
			FileID: plan.entry.ID,
			Comp:   "none",
			Offset: plan.resumeFrom,
		}
		offset := plan.resumeFrom
		lastTrailerTS := int64(0)
		var lastMetadata *FileTrailerMetadata
		serverHash := ""
		windowHasher := xxh3.New128()

		for {
			headerLine, readErr := br.ReadString('\n')
			if readErr != nil {
				_ = closeWriter()
				return nil, nil, nil, fmt.Errorf("read frame header: %w", readErr)
			}
			headerTrimmed := strings.TrimRight(headerLine, "\r\n")
			if isStatusLine(headerTrimmed) {
				_ = closeWriter()
				return nil, nil, nil, fmt.Errorf("unexpected status line before file complete: %s", headerTrimmed)
			}
			frameMeta, parseErr := parseFXHeader(headerTrimmed)
			if parseErr != nil {
				_ = closeWriter()
				return nil, nil, nil, parseErr
			}
			if frameMeta.FileID != plan.entry.ID {
				_ = closeWriter()
				return nil, nil, nil, fmt.Errorf("batched file id mismatch: expected=%d got=%d", plan.entry.ID, frameMeta.FileID)
			}
			if frameMeta.Offset != offset {
				_ = closeWriter()
				return nil, nil, nil, fmt.Errorf("batched offset mismatch: expected=%d got=%d", offset, frameMeta.Offset)
			}
			if frameBuf == nil {
				frameBuf, releaseFrameBuf = c.acquireFrameReadBuffer(frameMeta.MaxWireSizeHint)
			}

			payloadReader := io.LimitReader(br, frameMeta.WireSize)
			logicalReader, decodeErr := decodePayloadReader(payloadReader, frameMeta.Comp)
			if decodeErr != nil {
				_ = closeWriter()
				return nil, nil, nil, fmt.Errorf("decode payload reader: %w", decodeErr)
			}
			frameStartOffset := offset
			copyErr := copyStreamWithProgress(io.MultiWriter(writer, windowHasher), logicalReader, frameBuf, func(written int64) error {
				emitProgressUpdate(DownloadProgressUpdate{
					TransferID:  req.Manifest.TransferID,
					FileID:      plan.entry.ID,
					CopiedBytes: frameStartOffset + written,
					TargetBytes: plan.entry.Size,
					UpdateTime:  time.Now(),
				})
				return nil
			})
			closeLogicalErr := logicalReader.Close()
			if copyErr != nil {
				_ = closeWriter()
				return nil, nil, nil, fmt.Errorf("stream output file: %w", copyErr)
			}
			if closeLogicalErr != nil {
				_ = closeWriter()
				return nil, nil, nil, closeLogicalErr
			}
			meta.Size += frameMeta.Size
			meta.WireSize += frameMeta.WireSize
			meta.Comp = frameMeta.Comp
			offset += frameMeta.Size

			trailerLine, trailerReadErr := br.ReadString('\n')
			if trailerReadErr != nil {
				_ = closeWriter()
				return nil, nil, nil, fmt.Errorf("read frame trailer: %w", trailerReadErr)
			}
			trailer, trailerErr := parseFXTrailer(strings.TrimRight(trailerLine, "\r\n"))
			if trailerErr != nil {
				_ = closeWriter()
				return nil, nil, nil, trailerErr
			}
			if trailer.FileID != plan.entry.ID {
				_ = closeWriter()
				return nil, nil, nil, fmt.Errorf("trailer file id mismatch: expected=%d got=%d", plan.entry.ID, trailer.FileID)
			}
			lastTrailerTS = trailer.TS
			if trailer.Metadata != nil {
				lastMetadata = cloneTrailerMetadata(trailer.Metadata)
			}
			if trailer.Next == nil {
				serverHash = trailer.FileHashToken
				break
			}
			if *trailer.Next == 0 {
				serverHash = trailer.FileHashToken
				break
			}
			if *trailer.Next != offset {
				_ = closeWriter()
				return nil, nil, nil, fmt.Errorf("invalid trailer next offset: expected=%d got=%d", offset, *trailer.Next)
			}
		}
		if offset != plan.entry.Size {
			_ = closeWriter()
			return nil, nil, nil, fmt.Errorf("batched file size mismatch: expected=%d got=%d", plan.entry.Size, offset)
		}

		windowHash := intencoding.FormatXXH128HashToken(windowHasher.Sum128())
		if serverHash == "" {
			_ = closeWriter()
			return nil, nil, nil, errors.New("window hash missing from trailer")
		}
		if !strings.EqualFold(serverHash, windowHash) {
			_ = closeWriter()
			return nil, nil, nil, fmt.Errorf("window hash mismatch: server=%s client=%s", serverHash, windowHash)
		}

		syncMS := int64(0)
		syncStart := time.Now()
		_, syncTask := trace.NewTask(fileCtx, "sync")
		if err := syncOutput(); err != nil {
			syncTask.End()
			_ = closeWriter()
			return nil, nil, nil, fmt.Errorf("sync output for file %d: %w", plan.entry.ID, err)
		}
		syncTask.End()
		syncMS = time.Since(syncStart).Milliseconds()
		if err := closeWriter(); err != nil {
			return nil, nil, nil, fmt.Errorf("close output for file %d: %w", plan.entry.ID, err)
		}

		localHash := windowHash
		recvMS := time.Since(fileStart).Milliseconds()
		deltaBytes := offset - plan.resumeFrom
		pendingAcks = append(pendingAcks, AcknowledgeFileProgressRequest{
			TransferID: req.Manifest.TransferID,
			FileID:     plan.entry.ID,
			FullPath:   plan.serverPath,
			AckBytes:   offset,
			ServerTS:   lastTrailerTS,
			HashToken:  windowHash,
			DeltaBytes: deltaBytes,
			RecvMS:     recvMS,
			SyncMS:     syncMS,
		})
		ackProgresses = append(ackProgresses, seqAckProgress{
			fileID:      plan.entry.ID,
			ackBytes:    offset,
			targetBytes: plan.entry.Size,
		})

		meta.TrailerTS = lastTrailerTS
		meta.FileHashToken = windowHash
		meta.TrailerMetadata = lastMetadata
		results = append(results, DownloadFileResponse{
			Meta:          meta,
			LocalFileHash: localHash,
		})
		fileTask.End()
	}

	statusLine, err := readTCPLine(br, maxTCPLineBytes)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, nil, nil, fmt.Errorf("read batch terminal status: %w", err)
		}
	} else {
		if err := parseErrControlFrame(statusLine); err != nil {
			return nil, nil, nil, err
		}
		if _, ok := parseOKStatusLine(statusLine); !ok {
			return nil, nil, nil, fmt.Errorf("unexpected batch terminal response: %s", statusLine)
		}
	}

	return results, pendingAcks, ackProgresses, nil
}

// splitSequentialGroups splits parallel slices of plans and targets into k contiguous groups.
func splitSequentialGroups(plans []downloadBatchPlan, targets []FetchFileTarget, k int) ([][]downloadBatchPlan, [][]FetchFileTarget) {
	n := len(plans)
	if k > n {
		k = n
	}
	size := (n + k - 1) / k
	planGroups := make([][]downloadBatchPlan, 0, k)
	targetGroups := make([][]FetchFileTarget, 0, k)
	for i := 0; i < n; i += size {
		end := min(i+size, n)
		planGroups = append(planGroups, plans[i:end])
		targetGroups = append(targetGroups, targets[i:end])
	}
	return planGroups, targetGroups
}

func (c *Client) downloadManifestBatchSequential(
	ctx context.Context,
	req GetFilesRequest,
	plans []downloadBatchPlan,
	targets []FetchFileTarget,
) (GetFilesResponse, error) {
	ackTimeout := c.AckRequestTimeout
	if ackTimeout <= 0 {
		ackTimeout = defaultClientAckRequestTimeout
	}

	windowBytes := c.FileRequestWindowBytes
	if windowBytes <= 0 {
		windowBytes = defaultClientRequestWindowBytes
	}
	k := maxSplitWindowWorkers(windowBytes, req.BatchMaxBytes, req.SplitWindowWorkers, len(plans))

	var (
		allFiles    []DownloadFileResponse
		allAcks     []AcknowledgeFileProgressRequest
		allProgress []seqAckProgress
	)

	if k <= 1 {
		files, acks, progresses, err := c.downloadManifestGroupSequential(ctx, req, plans, targets, req.emitProgress)
		if err != nil {
			return GetFilesResponse{}, err
		}
		allFiles = files
		allAcks = acks
		allProgress = progresses
	} else {
		planGroups, targetGroups := splitSequentialGroups(plans, targets, k)
		type groupResult struct {
			files      []DownloadFileResponse
			acks       []AcknowledgeFileProgressRequest
			progresses []seqAckProgress
		}
		results := make([]groupResult, len(planGroups))
		errs := make([]error, len(planGroups))
		groupCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		for i, pg := range planGroups {
			wg.Add(1)
			go func(i int, pg []downloadBatchPlan, tg []FetchFileTarget) {
				defer wg.Done()
				files, acks, progresses, err := c.downloadManifestGroupSequential(groupCtx, req, pg, tg, req.emitProgress)
				results[i] = groupResult{files, acks, progresses}
				errs[i] = err
				if err != nil {
					cancel()
				}
			}(i, pg, targetGroups[i])
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return GetFilesResponse{}, err
			}
		}
		for _, r := range results {
			allFiles = append(allFiles, r.files...)
			allAcks = append(allAcks, r.acks...)
			allProgress = append(allProgress, r.progresses...)
		}
	}

	if len(allAcks) > 0 {
		_, ackTask := trace.NewTask(ctx, "ack")
		ackErr := retryAck(ctx, func(callCtx context.Context) error {
			ackCtx, cancel := context.WithTimeout(callCtx, ackTimeout)
			defer cancel()
			_, err := c.acknowledgeFileProgressBatch(ackCtx, allAcks)
			return err
		})
		ackTask.End()
		if ackErr != nil {
			return GetFilesResponse{}, fmt.Errorf("acknowledge download failed: %w", ackErr)
		}
	}

	for _, progress := range allProgress {
		req.emitProgress(DownloadProgressUpdate{
			TransferID:  req.Manifest.TransferID,
			FileID:      progress.fileID,
			TargetBytes: progress.targetBytes,
			AckBytes:    progress.ackBytes,
			UpdateTime:  time.Now(),
		})
	}
	return GetFilesResponse{Files: allFiles}, nil
}

func (c *Client) downloadManifestBatchWindows(
	ctx context.Context,
	req GetFilesRequest,
	plan downloadBatchPlan,
) (GetFilesResponse, error) {
	requestCtx := ctx
	windows := buildSplitWindows(plan.resumeFrom, plan.entry.Size, req.BatchMaxBytes)
	if len(windows) == 0 {
		return GetFilesResponse{}, errors.New("empty split window plan")
	}

	ackTimeout := c.AckRequestTimeout
	if ackTimeout <= 0 {
		ackTimeout = defaultClientAckRequestTimeout
	}
	if plan.resumeFrom > 0 {
		req.emitProgress(DownloadProgressUpdate{
			TransferID:  req.Manifest.TransferID,
			FileID:      plan.entry.ID,
			CopiedBytes: plan.resumeFrom,
			TargetBytes: plan.entry.Size,
			UpdateTime:  time.Now(),
		})
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var initialWriter io.WriteCloser
	var initialSync func() error
	if plan.resumeFrom == 0 {
		initialWriterResult, initialSyncResult, initialErr := req.OutputWriter(plan.entry, 0)
		if initialErr != nil {
			return GetFilesResponse{}, fmt.Errorf("create output writer for file %d: %w", plan.entry.ID, initialErr)
		}
		initialWriter, initialSync = initialWriterResult, initialSyncResult
		if initialWriter == nil {
			return GetFilesResponse{}, fmt.Errorf("create output writer for file %d: nil writer", plan.entry.ID)
		}
		if initialSync == nil {
			initialSync = func() error { return nil }
		}
	}

	maxWorkers := maxSplitWindowWorkers(c.FileRequestWindowBytes, req.BatchMaxBytes, req.SplitWindowWorkers, len(windows))
	limiter := make(chan struct{}, maxWorkers)
	results := make(chan splitWindowResult, len(windows))
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	setErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	// windows[0].start == plan.resumeFrom always, so initialWriter is only ever used by i==0.
	for i, window := range windows {
		w, s := io.WriteCloser(nil), func() error { return nil }
		if i == 0 && initialWriter != nil {
			w, s = initialWriter, initialSync
		}
		wg.Add(1)
		go func(window splitWindow, w io.WriteCloser, s func() error) {
			defer wg.Done()
			select {
			case limiter <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-limiter }()

			result, err := c.downloadSplitWindow(ctx, req, plan, window, w, s, req.emitProgress)
			if err != nil {
				setErr(err)
				return
			}
			select {
			case results <- result:
			case <-ctx.Done():
			}
		}(window, w, s)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	completions := make(map[int64]splitWindowResult, len(windows))
	pending := make(map[int64]splitWindowResult, len(windows))
	nextAckOffset := plan.resumeFrom

	for result := range results {
		completions[result.window.start] = result
		pending[result.window.start] = result

		// Collect the contiguous chain of completed windows starting at nextAckOffset.
		var ackBatch []AcknowledgeFileProgressRequest
		for next := nextAckOffset; ; {
			ready, ok := pending[next]
			if !ok {
				break
			}
			ackBatch = append(ackBatch, ready.ack)
			next = ready.window.end
		}
		if len(ackBatch) == 0 {
			continue
		}
		_, ackTask := trace.NewTask(requestCtx, "ack")
		ackErr := retryAck(requestCtx, func(callCtx context.Context) error {
			ackCtx, cancelAck := context.WithTimeout(callCtx, ackTimeout)
			defer cancelAck()
			_, err := c.acknowledgeFileProgressBatch(ackCtx, ackBatch)
			return err
		})
		ackTask.End()
		if ackErr != nil {
			setErr(fmt.Errorf("acknowledge download failed: %w", ackErr))
			break
		}
		for next := nextAckOffset; ; {
			ready, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, ready.window.start)
			nextAckOffset = ready.window.end
			req.emitProgress(DownloadProgressUpdate{
				TransferID:  req.Manifest.TransferID,
				FileID:      plan.entry.ID,
				TargetBytes: plan.entry.Size,
				AckBytes:    ready.ack.AckBytes,
				UpdateTime:  time.Now(),
			})
			next = nextAckOffset
		}
	}

	if firstErr != nil {
		var missingErr *fileMissingError
		if errors.Is(firstErr, ErrFileMissing) && errors.As(firstErr, &missingErr) && shouldAcknowledgeMissing404(missingErr.Body) {
			if ackErr := c.acknowledgeMissingFile(requestCtx, req.Manifest.TransferID, plan.entry.ID, plan.serverPath); ackErr != nil {
				return GetFilesResponse{}, fmt.Errorf("%w (failed to ack missing: %v)", firstErr, ackErr)
			}
		}
		return GetFilesResponse{}, firstErr
	}
	if err := ctx.Err(); err != nil {
		return GetFilesResponse{}, err
	}
	if nextAckOffset != plan.entry.Size {
		return GetFilesResponse{}, fmt.Errorf("split ack offset mismatch: expected=%d got=%d", plan.entry.Size, nextAckOffset)
	}

	aggregate, err := aggregateSplitWindowResults(plan, windows, completions)
	if err != nil {
		return GetFilesResponse{}, err
	}
	return GetFilesResponse{Files: []DownloadFileResponse{aggregate}}, nil
}

func buildSplitWindows(start int64, end int64, maxBytes int64) []splitWindow {
	if maxBytes <= 0 || end <= start {
		return nil
	}
	windows := make([]splitWindow, 0, int((end-start+maxBytes-1)/maxBytes))
	for offset := start; offset < end; {
		size := min(maxBytes, end-offset)
		windows = append(windows, splitWindow{
			start: offset,
			size:  size,
			end:   offset + size,
		})
		offset += size
	}
	return windows
}

func splitWindowWorkerLimit(windowBytes int64, batchBytes int64, plannedWorkers int) int {
	if batchBytes <= 0 {
		return max(1, plannedWorkers)
	}
	if windowBytes <= 0 {
		windowBytes = defaultClientRequestWindowBytes
	}
	derivedWorkers := max(1, int(windowBytes/batchBytes))
	if plannedWorkers <= 0 {
		return derivedWorkers
	}
	return max(1, min(plannedWorkers, derivedWorkers))
}

func maxSplitWindowWorkers(windowBytes int64, batchBytes int64, plannedWorkers int, numWindows int) int {
	if numWindows < 1 || batchBytes <= 0 {
		return 1
	}
	return max(1, min(numWindows, splitWindowWorkerLimit(windowBytes, batchBytes, plannedWorkers)))
}

func (c *Client) downloadSplitWindow(
	ctx context.Context,
	req GetFilesRequest,
	plan downloadBatchPlan,
	window splitWindow,
	writer io.WriteCloser,
	syncOutput func() error,
	emitProgressUpdate func(DownloadProgressUpdate),
) (splitWindowResult, error) {
	ctx, windowTask := trace.NewTask(ctx, "download-window")
	defer windowTask.End()
	start := time.Now()
	reader, meta, err := c.fetchFileWindow(
		ctx,
		req.Manifest.TransferID,
		plan.entry.ID,
		plan.serverPath,
		window.start,
		window.size,
	)
	if err != nil {
		if writer != nil {
			_ = writer.Close()
		}
		return splitWindowResult{}, err
	}

	if writer == nil {
		writer, syncOutput, err = req.OutputWriter(plan.entry, window.start)
		if err != nil {
			_ = reader.Close()
			return splitWindowResult{}, fmt.Errorf("create output writer for file %d: %w", plan.entry.ID, err)
		}
		if writer == nil {
			_ = reader.Close()
			return splitWindowResult{}, fmt.Errorf("create output writer for file %d: nil writer", plan.entry.ID)
		}
	}
	if syncOutput == nil {
		syncOutput = func() error { return nil }
	}

	writerClosed := false
	closeWriter := func() error {
		if writerClosed {
			return nil
		}
		writerClosed = true
		return writer.Close()
	}

	frameBuf, releaseFrameBuf := c.acquireFrameReadBuffer(meta.MaxWireSizeHint)
	defer releaseFrameBuf()

	windowHasher := xxh3.New128()
	copyErr := copyStreamWithProgress(io.MultiWriter(writer, windowHasher), reader, frameBuf, func(written int64) error {
		emitProgressUpdate(DownloadProgressUpdate{
			TransferID:  req.Manifest.TransferID,
			FileID:      plan.entry.ID,
			CopiedBytes: window.start + written,
			TargetBytes: plan.entry.Size,
			UpdateTime:  time.Now(),
		})
		return nil
	})
	closeReadErr := reader.Close()
	if copyErr != nil {
		_ = closeWriter()
		return splitWindowResult{}, fmt.Errorf("stream output file: %w", copyErr)
	}
	if closeReadErr != nil {
		_ = closeWriter()
		return splitWindowResult{}, closeReadErr
	}
	if meta.Offset != window.start {
		_ = closeWriter()
		return splitWindowResult{}, fmt.Errorf("split window offset mismatch: expected=%d got=%d", window.start, meta.Offset)
	}
	if meta.Size != window.size {
		_ = closeWriter()
		return splitWindowResult{}, fmt.Errorf("split window size mismatch: expected=%d got=%d", window.size, meta.Size)
	}

	syncStart := time.Now()
	_, syncTask := trace.NewTask(ctx, "sync")
	if err := syncOutput(); err != nil {
		syncTask.End()
		_ = closeWriter()
		return splitWindowResult{}, fmt.Errorf("sync output for file %d: %w", plan.entry.ID, err)
	}
	syncTask.End()
	syncMS := time.Since(syncStart).Milliseconds()
	if err := closeWriter(); err != nil {
		return splitWindowResult{}, fmt.Errorf("close output for file %d: %w", plan.entry.ID, err)
	}

	localHash := intencoding.FormatXXH128HashToken(windowHasher.Sum128())
	if meta.FileHashToken == "" {
		return splitWindowResult{}, errors.New("window hash missing from trailer")
	}
	if !strings.EqualFold(meta.FileHashToken, localHash) {
		return splitWindowResult{}, fmt.Errorf("window hash mismatch: server=%s client=%s", meta.FileHashToken, localHash)
	}

	recvMS := time.Since(start).Milliseconds()
	return splitWindowResult{
		window: window,
		response: DownloadFileResponse{
			Meta:          *meta,
			LocalFileHash: localHash,
		},
		ack: AcknowledgeFileProgressRequest{
			TransferID: req.Manifest.TransferID,
			FileID:     plan.entry.ID,
			FullPath:   plan.serverPath,
			AckBytes:   window.end,
			ServerTS:   meta.TrailerTS,
			HashToken:  meta.FileHashToken,
			DeltaBytes: window.size,
			RecvMS:     recvMS,
			SyncMS:     syncMS,
		},
	}, nil
}

func aggregateSplitWindowResults(
	plan downloadBatchPlan,
	windows []splitWindow,
	results map[int64]splitWindowResult,
) (DownloadFileResponse, error) {
	if len(windows) == 0 {
		return DownloadFileResponse{}, errors.New("missing split windows")
	}
	aggregate := DownloadFileResponse{
		Meta: FileFrameMeta{
			FileID: plan.entry.ID,
			Comp:   "none",
			Offset: plan.resumeFrom,
		},
	}
	for idx, window := range windows {
		result, ok := results[window.start]
		if !ok {
			return DownloadFileResponse{}, fmt.Errorf("missing split window result at offset %d", window.start)
		}
		meta := result.response.Meta
		if meta.HeaderTS > 0 {
			if aggregate.Meta.HeaderTS == 0 || meta.HeaderTS < aggregate.Meta.HeaderTS {
				aggregate.Meta.HeaderTS = meta.HeaderTS
			}
		}
		aggregate.Meta.Size += meta.Size
		aggregate.Meta.WireSize += meta.WireSize
		if idx == 0 {
			aggregate.Meta.Comp = meta.Comp
		}
		if len(meta.CompCounts) > 0 {
			if aggregate.Meta.CompCounts == nil {
				aggregate.Meta.CompCounts = copyCompCounts(meta.CompCounts)
			} else {
				mergeCompCounts(aggregate.Meta.CompCounts, meta.CompCounts)
			}
		} else if meta.Comp != "" {
			if aggregate.Meta.CompCounts == nil {
				aggregate.Meta.CompCounts = make(map[string]uint64, len(windows))
			}
			aggregate.Meta.CompCounts[meta.Comp]++
		}
		if idx == len(windows)-1 {
			aggregate.Meta.TrailerTS = meta.TrailerTS
			aggregate.Meta.TrailerMetadata = cloneTrailerMetadata(meta.TrailerMetadata)
		}
	}
	if len(aggregate.Meta.CompCounts) == 1 {
		for comp := range aggregate.Meta.CompCounts {
			aggregate.Meta.Comp = comp
		}
	}
	aggregate.Meta.FileHashToken = ""
	aggregate.LocalFileHash = ""
	aggregate.WindowChecksumPassed = len(windows)
	aggregate.WindowChecksumTotal = len(windows)
	return aggregate, nil
}

func (c *Client) StartFromManifest(ctx context.Context, req StartFromManifestRequest) (StartFromManifestResponse, error) {
	if c == nil {
		return StartFromManifestResponse{}, errors.New("nil client")
	}
	if req.Manifest == nil {
		return StartFromManifestResponse{}, errors.New("nil manifest")
	}
	entries := req.Entries
	if entries == nil {
		entries = req.Manifest.Entries
	}
	resp := StartFromManifestResponse{
		Requested: len(entries),
	}
	if len(entries) == 0 {
		return resp, nil
	}
	if req.OutputWriter == nil {
		return StartFromManifestResponse{}, errors.New("missing output writer callback")
	}
	if req.Concurrency <= 0 {
		if req.Manifest.Concurrency > 0 {
			req.Concurrency = req.Manifest.Concurrency
		} else {
			req.Concurrency = DefaultClientConcurrency()
		}
	}
	req.Concurrency = clampConcurrency(req.Concurrency)
	batchMaxBytes := c.effectiveBatchMaxBytes(req.BatchMaxBytes)
	batches := buildManifestBatchesByBytes(entries, batchMaxBytes)
	workCh := make(chan []ManifestEntry)
	errCh := make(chan error, len(entries))
	var wg sync.WaitGroup
	var downloaded atomic.Int64
	var transferred atomic.Int64

	worker := func() {
		defer wg.Done()
		for batch := range workCh {
			if len(batch) == 0 {
				continue
			}
			fileIDs := make([]uint64, 0, len(batch))
			for _, entry := range batch {
				fileIDs = append(fileIDs, entry.ID)
			}
			startOne := time.Now()
			downloadBatchResp, err := c.GetFiles(ctx, GetFilesRequest{
				Manifest:           req.Manifest,
				FileIDs:            fileIDs,
				OutputWriter:       req.OutputWriter,
				BatchMaxBytes:      batchMaxBytes,
				SplitWindowWorkers: req.SplitWindowWorkers,
				ProgressUpdates:    req.ProgressUpdates,
			})
			if err != nil {
				errCh <- fmt.Errorf("batch first-id=%d count=%d: %w", batch[0].ID, len(batch), err)
				continue
			}
			elapsedBatch := time.Since(startOne)
			for _, downloadResp := range downloadBatchResp.Files {
				downloaded.Add(1)
				transferred.Add(downloadResp.Meta.Size)
				if req.OnFileDone != nil {
					req.OnFileDone(StartFileDoneEvent{
						File:    downloadResp,
						Elapsed: elapsedBatch,
					})
				}
			}
		}
	}

	for i := 0; i < req.Concurrency; i++ {
		wg.Add(1)
		go worker()
	}
	submitBatch := func(batch []ManifestEntry) bool {
		select {
		case <-ctx.Done():
			return false
		case workCh <- batch:
			return true
		}
	}
	for _, batch := range batches {
		if !submitBatch(batch) {
			break
		}
	}
	close(workCh)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		resp.Errors = append(resp.Errors, err)
	}
	resp.Downloaded = int(downloaded.Load())
	resp.TransferredBytes = transferred.Load()
	resp.Failed = len(resp.Errors)
	return resp, nil
}

func (c *Client) effectiveBatchMaxBytes(reqBatchMaxBytes int64) int64 {
	if reqBatchMaxBytes > 0 {
		return reqBatchMaxBytes
	}
	if c != nil && c.BatchMaxBytes > 0 {
		return c.BatchMaxBytes
	}
	return defaultClientBatchMaxBytes
}

// SuggestBatchMaxBytes computes the batch size (unit of parallel TCP work per file)
// from the probe-derived suggested concurrency and the client's window parameters.
//
// The goal is to keep enough batches in flight to saturate the server:
//
//	perFileWorkers = suggestedConcurrency / windowConcurrency
//	rawBatch       = windowBytes / perFileWorkers
//
// When the suggested concurrency is too low to support both the default window
// concurrency and per-file parallelism (i.e. suggestedConcurrency / windowConcurrency < 2),
// window concurrency is reduced to suggestedConcurrency so all slots go to
// concurrent file downloads rather than per-file splitting. This happens
// naturally in gentle mode when the server advertises a low CPU budget
// (25% by default).
//
//	Example (gentle, 24 cpu):  conc=6, winConc reduced to 6, perFile=1 → batch = windowBytes
//	Example (gentle, 32 cpu):  conc=8, winConc=4, perFile=2 → batch = windowBytes/2
//	Example (fast, 24 cpu, io=8): conc=192, winConc=4, perFile=48 → batch ≈ 32 MiB
//
// The result is rounded up to the nearest power-of-2 MiB boundary for alignment,
// then clamped:
//   - Floor: max(serverWmemBytes, clientRmemBytes). A single batch becomes one TCP
//     SEND request, so there is no benefit in making it smaller than the socket
//     buffer the OS has already allocated for the connection.
//   - Ceiling: windowBytes. A batch larger than the window cannot be scheduled.
func SuggestBatchMaxBytes(suggestedConcurrency int, windowConcurrency int, windowBytes int64, serverWmemBytes int64, linkMbps int64) int64 {
	return buildBatchSizePlan(suggestedConcurrency, windowConcurrency, windowBytes, serverWmemBytes, linkMbps).BatchMaxBytes
}

// BatchSizePlan captures the inputs and intermediate values used to compute
// the batch size, for human-readable diagnostic output.
type BatchSizePlan struct {
	BatchMaxBytes      int64
	ConcBatchBytes     int64 // batch from concurrency math before bw cap
	BwCeilBytes        int64 // bandwidth ceiling (0 if not applied)
	FloorBytes         int64 // socket buffer floor
	WindowBytes        int64
	SplitWindowWorkers int
	SuggestedConc      int
	EffectiveWinConc   int
	PerFileWorkers     int
	LinkMbps           int64
}

// ExplainBatchMaxBytes computes the batch size and returns a plan with
// intermediate values suitable for diagnostic output.
func ExplainBatchMaxBytes(suggestedConcurrency int, windowConcurrency int, windowBytes int64, serverWmemBytes int64, linkMbps int64) BatchSizePlan {
	return buildBatchSizePlan(suggestedConcurrency, windowConcurrency, windowBytes, serverWmemBytes, linkMbps)
}

func buildBatchSizePlan(suggestedConcurrency int, windowConcurrency int, windowBytes int64, serverWmemBytes int64, linkMbps int64) BatchSizePlan {
	plan := BatchSizePlan{
		SuggestedConc: suggestedConcurrency,
		WindowBytes:   windowBytes,
		LinkMbps:      linkMbps,
	}
	if windowBytes <= 0 || windowConcurrency == 1 {
		plan.BatchMaxBytes = defaultClientBatchMaxBytes
		if windowBytes > 0 {
			plan.BatchMaxBytes = largestPow2MiBWithin(windowBytes)
		}
		plan.ConcBatchBytes = plan.BatchMaxBytes
		plan.EffectiveWinConc = max(1, windowConcurrency)
		plan.PerFileWorkers = 1
		plan.SplitWindowWorkers = 1
		return plan
	}
	conc := max(1, plan.SuggestedConc)
	plan.EffectiveWinConc = effectiveWindowConcurrency(conc, windowConcurrency)
	plan.PerFileWorkers = max(1, conc/plan.EffectiveWinConc)
	const mib = int64(1 << 20)
	if windowBytes < mib {
		plan.ConcBatchBytes = windowBytes
		plan.BatchMaxBytes = windowBytes
		plan.SplitWindowWorkers = 1
		return plan
	}
	plan.ConcBatchBytes = roundUpPow2MiB(windowBytes / int64(plan.PerFileWorkers))
	batch := plan.ConcBatchBytes
	if batch < mib {
		plan.BatchMaxBytes = batch
		plan.SplitWindowWorkers = splitWindowWorkerLimit(windowBytes, batch, plan.PerFileWorkers)
		return plan
	}
	plan.BwCeilBytes = bandwidthCeilBatchBytes(linkMbps, conc)
	if plan.BwCeilBytes >= mib {
		batch = min(batch, plan.BwCeilBytes)
	}
	plan.FloorBytes = roundedSocketFloorBytes(serverWmemBytes)
	batch = max(batch, plan.FloorBytes)
	plan.BatchMaxBytes = min(batch, largestPow2MiBWithin(windowBytes))
	plan.SplitWindowWorkers = splitWindowWorkerLimit(windowBytes, plan.BatchMaxBytes, plan.PerFileWorkers)
	return plan
}

func effectiveWindowConcurrency(suggestedConcurrency int, windowConcurrency int) int {
	if windowConcurrency <= 0 {
		windowConcurrency = 4
	}
	if windowConcurrency == 1 {
		return 1
	}
	if suggestedConcurrency/windowConcurrency < 2 {
		return suggestedConcurrency
	}
	return windowConcurrency
}

func roundUpPow2MiB(v int64) int64 {
	const mib = int64(1 << 20)
	if v <= 0 {
		return 0
	}
	if v < mib {
		return mib
	}
	if v == mib {
		return v
	}
	mibCount := max(int64(1), (v+mib-1)/mib)
	pow2 := int64(1)
	for pow2 < mibCount {
		pow2 <<= 1
	}
	return pow2 * mib
}

func bandwidthCeilBatchBytes(linkMbps int64, suggestedConcurrency int) int64 {
	if linkMbps <= 0 {
		return 0
	}
	perWorkerBytesPerSec := (linkMbps * 1_000_000 / 8) / int64(max(1, suggestedConcurrency))
	return largestPow2MiBWithin(perWorkerBytesPerSec / 2)
}

func roundedSocketFloorBytes(serverWmemBytes int64) int64 {
	return roundUpPow2MiB(max(serverWmemBytes, int64(utils.MaxSocketReadBufferBytes())))
}

// largestPow2MiBWithin returns the largest power-of-2 multiple of MiB that is
// <= v. If v < 1 MiB it returns v unchanged (sub-MiB callers handle their own
// alignment).
func largestPow2MiBWithin(v int64) int64 {
	const mib = int64(1 << 20)
	if v < mib {
		return v
	}
	p := int64(1)
	for p*2 <= v/mib {
		p <<= 1
	}
	return p * mib
}

func suggestedConcurrencyFromProbe(serverCPU int, ioDepth int, strategy string, gentleCPUPct int) int {
	if serverCPU <= 0 {
		serverCPU = runtime.NumCPU()
	}
	if ioDepth <= 0 {
		ioDepth = 1
	}
	strategy = normalizeLoadStrategy(strategy)
	if strategy == LoadStrategyGentle {
		// Gentle: a server-advertised percent of available CPUs, io-depth is ignored —
		// window concurrency and per-file parallelism are auto-ranged
		// in SuggestBatchMaxBytes to fill this budget.
		gentleCPUPct = NormalizeGentleCPUPct(gentleCPUPct)
		value := (serverCPU * gentleCPUPct) / 100
		if (serverCPU*gentleCPUPct)%100 != 0 {
			value++
		}
		return value
	}
	return serverCPU * ioDepth
}

func clampConcurrency(v int) int {
	if v < minAutoStartConcurrency {
		return minAutoStartConcurrency
	}
	if v > maxAutoStartConcurrency {
		return maxAutoStartConcurrency
	}
	return v
}

func (c *Client) ProbeLink(ctx context.Context, req ProbeRequest) (ProbeResponse, error) {
	ctx, task := trace.NewTask(ctx, "probe-link")
	defer task.End()
	if c == nil {
		return ProbeResponse{}, errors.New("nil client")
	}
	probeBytes := req.ProbeBytes
	if probeBytes <= 0 {
		probeBytes = defaultClientProbeBytes
	}
	loadStrategy := req.LoadStrategy
	if strings.TrimSpace(loadStrategy) == "" {
		loadStrategy = c.LoadStrategy
	}
	loadStrategy = normalizeLoadStrategy(loadStrategy)

	// Phase A: single 1-byte probe discovers server state and measures RTT.
	discovery, err := c.probeTCP(ctx, req, 1)
	if err != nil {
		return ProbeResponse{}, fmt.Errorf("probe discovery failed: %w", err)
	}
	response := probeDiscoveryResponse(discovery)
	response.SuggestedConcurrency = clampConcurrency(
		suggestedConcurrencyFromProbe(response.ServerCPU, response.ServerIODepth, loadStrategy, response.GentleCPUPct),
	)
	if authState, authErr := c.resolveTCPAuthState(ctx); authErr == nil {
		if authState.hasAuth {
			response.SuggestedCipher = authState.encMode
		}
		response.WarmConnectionPoolSize = c.ensureTCPPool(authState, response.SuggestedConcurrency)
	}

	// Mini-probe caller: discovery only.
	if probeBytes <= 1 {
		response.ParallelConns = 1
		return response, nil
	}

	// Phase B: throughput measurement. Fast mode fans out to ServerCPU conns;
	// gentle mode runs a sequence of linear samples. Parallelism=1 forces a
	// single-conn probe regardless of mode (used by the in-flight reporter).
	parallelism := req.Parallelism
	if parallelism == 0 {
		if loadStrategy == LoadStrategyGentle {
			parallelism = -1 // sentinel: gentle → linear default samples
		} else {
			parallelism = max(1, response.ServerCPU)
		}
	}

	var phaseB probeThroughput
	switch {
	case parallelism < 0:
		phaseB, err = c.probeLinear(ctx, req, probeBytes, defaultClientProbeSamples)
	case parallelism <= 1:
		phaseB, err = c.probeLinear(ctx, req, probeBytes, 1)
	default:
		phaseB, err = c.probeParallel(ctx, req, probeBytes, parallelism)
	}
	if err != nil {
		return ProbeResponse{}, err
	}

	response.LinkMbps = phaseB.aggregateMbps
	response.AggregateMbps = phaseB.aggregateMbps
	response.PerConnMbps = phaseB.perConnMbps
	response.ParallelConns = phaseB.conns
	if phaseB.limiterBps > 0 {
		response.ServerLimiterBps = phaseB.limiterBps
	}
	return response, nil
}

type probeThroughput struct {
	aggregateMbps int64
	perConnMbps   int64
	conns         int
	limiterBps    int64
}

func probeDiscoveryResponse(result probeResponse) ProbeResponse {
	intervalMS := max(int64(1), (result.CTS1-result.CTS0)-(result.STS1-result.STS0))
	return ProbeResponse{
		ServerCPU:          result.ServerCPU,
		ServerIODepth:      result.ServerIODepth,
		GentleCPUPct:       result.GentleCPUPct,
		GentleBWPct:        result.GentleBWPct,
		AvgLatencyMS:       intervalMS,
		ServerSendBufBytes: result.ServerWmemBytes,
		ServerLimiterBps:   result.LimiterBps,
	}
}

func probeConnMbps(result probeResponse, probeBytes int64) int64 {
	intervalMS := max(int64(1), (result.CTS1-result.CTS0)-(result.STS1-result.STS0))
	return (probeBytes * 8 * 1000) / intervalMS / 1_000_000
}

func roundMbps(mbps int64) int64 {
	return ((mbps + 50) / 100) * 100
}

func (c *Client) probeLinear(ctx context.Context, req ProbeRequest, probeBytes int64, samples int) (probeThroughput, error) {
	if samples <= 0 {
		samples = 1
	}
	results := make([]probeResponse, 0, samples)
	for i := 0; i < samples; i++ {
		result, err := c.probeTCP(ctx, req, probeBytes)
		if err != nil {
			return probeThroughput{}, fmt.Errorf("probe %d failed: %w", i+1, err)
		}
		results = append(results, result)
	}
	totalIntervalMS := int64(0)
	for _, r := range results {
		totalIntervalMS += max(int64(1), (r.CTS1-r.CTS0)-(r.STS1-r.STS0))
	}
	avgMS := max(int64(1), totalIntervalMS/int64(len(results)))
	mbps := roundMbps((probeBytes * 8 * 1000) / avgMS / 1_000_000)
	return probeThroughput{
		aggregateMbps: mbps,
		perConnMbps:   mbps,
		conns:         1,
		limiterBps:    results[len(results)-1].LimiterBps,
	}, nil
}

func (c *Client) probeParallel(ctx context.Context, req ProbeRequest, probeBytes int64, n int) (probeThroughput, error) {
	if n < 2 {
		return c.probeLinear(ctx, req, probeBytes, 1)
	}
	type outcome struct {
		result probeResponse
		err    error
	}
	outcomes := make([]outcome, n)
	startCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			<-startCh
			r, e := c.probeTCP(ctx, req, probeBytes)
			outcomes[idx] = outcome{result: r, err: e}
		}(i)
	}
	close(startCh)
	wg.Wait()

	rates := make([]int64, 0, n)
	var (
		minStart int64
		maxEnd   int64
		limiter  int64
	)
	for i, o := range outcomes {
		if o.err != nil {
			return probeThroughput{}, fmt.Errorf("parallel probe %d failed: %w", i+1, o.err)
		}
		if i == 0 || o.result.CTS0 < minStart {
			minStart = o.result.CTS0
		}
		if o.result.CTS1 > maxEnd {
			maxEnd = o.result.CTS1
		}
		rates = append(rates, probeConnMbps(o.result, probeBytes))
		limiter = o.result.LimiterBps
	}
	sort.Slice(rates, func(i, j int) bool { return rates[i] < rates[j] })
	perConn := roundMbps(rates[len(rates)/2])

	wallMS := max(int64(1), maxEnd-minStart)
	totalBytes := int64(n) * probeBytes
	aggMbps := roundMbps((totalBytes * 8 * 1000) / wallMS / 1_000_000)

	return probeThroughput{
		aggregateMbps: aggMbps,
		perConnMbps:   perConn,
		conns:         n,
		limiterBps:    limiter,
	}, nil
}

func buildManifestBatchesByBytes(entries []ManifestEntry, maxBytes int64) [][]ManifestEntry {
	if len(entries) == 0 {
		return nil
	}
	if maxBytes <= 0 {
		maxBytes = defaultClientBatchMaxBytes
	}
	batches := make([][]ManifestEntry, 0, len(entries))
	current := make([]ManifestEntry, 0, 8)
	var currentBytes int64
	for _, entry := range entries {
		size := max(int64(0), entry.Size)
		if len(current) > 0 && currentBytes+size > maxBytes {
			batches = append(batches, current)
			current = make([]ManifestEntry, 0, 8)
			currentBytes = 0
		}
		current = append(current, entry)
		currentBytes += size
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}

func shouldAcknowledgeMissing404(body string) bool {
	return strings.EqualFold(strings.TrimSpace(body), "file not found")
}

type acknowledgeFileProgressCommand struct {
	request  AcknowledgeFileProgressRequest
	ackToken string
}

func buildAcknowledgeFileProgressCommand(request AcknowledgeFileProgressRequest) (acknowledgeFileProgressCommand, error) {
	if request.TransferID == "" {
		return acknowledgeFileProgressCommand{}, errors.New("missing transfer id")
	}
	if request.FullPath == "" {
		return acknowledgeFileProgressCommand{}, errors.New("missing full path")
	}
	ackToken, err := buildAckToken(request.AckBytes, request.ServerTS, request.HashToken)
	if err != nil {
		return acknowledgeFileProgressCommand{}, err
	}
	if request.AckBytes >= 0 {
		if request.DeltaBytes < 0 {
			return acknowledgeFileProgressCommand{}, errors.New("ack delta bytes must be >= 0")
		}
		if request.RecvMS < 0 {
			return acknowledgeFileProgressCommand{}, errors.New("ack recv-ms must be >= 0")
		}
		if request.SyncMS < 0 {
			return acknowledgeFileProgressCommand{}, errors.New("ack sync-ms must be >= 0")
		}
	}
	return acknowledgeFileProgressCommand{request: request, ackToken: ackToken}, nil
}

func (c *Client) acknowledgeFileProgressBatch(ctx context.Context, requests []AcknowledgeFileProgressRequest) (AcknowledgeFileProgressResponse, error) {
	if len(requests) == 0 {
		return AcknowledgeFileProgressResponse{}, errors.New("missing ack requests")
	}
	commands := make([]acknowledgeFileProgressCommand, 0, len(requests))
	for _, request := range requests {
		cmd, err := buildAcknowledgeFileProgressCommand(request)
		if err != nil {
			return AcknowledgeFileProgressResponse{}, err
		}
		commands = append(commands, cmd)
	}
	return c.acknowledgeFileProgressBatchTCP(ctx, commands)
}

func (c *Client) AcknowledgeFileProgress(ctx context.Context, request AcknowledgeFileProgressRequest) (AcknowledgeFileProgressResponse, error) {
	return c.acknowledgeFileProgressBatch(ctx, []AcknowledgeFileProgressRequest{request})
}

func buildAckToken(ackBytes int64, serverTS int64, hashToken string) (string, error) {
	if ackBytes == -1 {
		return "-1", nil
	}
	if ackBytes < 0 {
		return "", errors.New("ack bytes must be -1 or non-negative")
	}
	if serverTS < 0 {
		return "", errors.New("ack server timestamp must be non-negative")
	}
	token := fmt.Sprintf("%d@%d", ackBytes, serverTS)
	if hashToken != "" {
		if !intencoding.ValidHashToken(hashToken) {
			return "", errors.New("invalid ack hash token")
		}
		token += "@" + hashToken
	}
	return token, nil
}

func retryAck(ctx context.Context, fn func(context.Context) error) error {
	const maxAttempts = 5
	backoff := 100 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := fn(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == maxAttempts {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
	}
	return lastErr
}

func copyCompCounts(src map[string]uint64) map[string]uint64 {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(src))
	maps.Copy(out, src)
	return out
}

func mergeCompCounts(dst map[string]uint64, src map[string]uint64) {
	if len(src) == 0 {
		return
	}
	if dst == nil {
		return
	}
	for k, v := range src {
		dst[k] += v
	}
}

func copyStreamWithProgress(dst io.Writer, src io.Reader, buf []byte, onWrite func(written int64) error) error {
	var written int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}
			written += int64(n)
			if onWrite != nil {
				if err := onWrite(written); err != nil {
					return err
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func effectiveFrameReadBufferSize(baseSize int, maxWireHint int64, capSize int) int {
	if baseSize <= 0 {
		baseSize = defaultClientFrameBufferBytes
	}
	if capSize <= 0 {
		capSize = defaultClientMaxFrameReadBufferBytes
	}
	if capSize < minClientFrameReadBufferBytes {
		capSize = minClientFrameReadBufferBytes
	}

	target := baseSize
	if maxWireHint > 0 {
		if maxWireHint > int64(capSize) {
			target = capSize
		} else {
			target = int(maxWireHint)
		}
	}
	target = max(minClientFrameReadBufferBytes, min(capSize, frameReadBucketSize(target)))
	return target
}

func frameReadBucketSize(target int) int {
	const mib = 1024 * 1024
	for _, bucket := range []int{
		1 * mib,
		2 * mib,
		4 * mib,
		8 * mib,
		16 * mib,
		32 * mib,
		64 * mib,
	} {
		if target <= bucket {
			return bucket
		}
	}
	return 64 * mib
}

func (c *Client) acquireFrameReadBuffer(maxWireHint int64) ([]byte, func()) {
	size := effectiveFrameReadBufferSize(c.FrameBufferBytes, maxWireHint, c.MaxFrameReadBufferBytes)
	pool := (*sync.Pool)(nil)
	if existing, ok := c.bufferPool.Load(size); ok {
		pool = existing.(*sync.Pool)
	} else {
		sz := size
		created := &sync.Pool{
			New: func() any {
				return make([]byte, sz)
			},
		}
		actual, _ := c.bufferPool.LoadOrStore(size, created)
		pool = actual.(*sync.Pool)
	}
	raw := pool.Get()
	buf, ok := raw.([]byte)
	if !ok || cap(buf) < size {
		buf = make([]byte, size)
	}
	buf = buf[:size]
	return buf, func() {
		pool.Put(buf[:size])
	}
}

func (c *Client) acquireScratchBuffer(sizeHint int) *bytes.Buffer {
	if sizeHint <= 0 {
		sizeHint = defaultClientScratchBufferBytes
	}
	if c == nil {
		return bytes.NewBuffer(make([]byte, 0, sizeHint))
	}
	raw := c.scratchBufferPool.Get()
	if buf, ok := raw.(*bytes.Buffer); ok && buf != nil {
		if buf.Cap() >= sizeHint {
			buf.Reset()
			return buf
		}
		c.releaseScratchBuffer(buf)
	}
	return bytes.NewBuffer(make([]byte, 0, sizeHint))
}

func (c *Client) releaseScratchBuffer(buf *bytes.Buffer) {
	if c == nil || buf == nil {
		return
	}
	if buf.Cap() > maxClientScratchBufferPoolBytes {
		return
	}
	buf.Reset()
	c.scratchBufferPool.Put(buf)
}

func (c *Client) acquireLineReader(r io.Reader) *bufio.Reader {
	raw := c.lineReaderPool.Get()
	br := raw.(*bufio.Reader)
	br.Reset(r)
	return br
}

func (c *Client) releaseLineReader(br *bufio.Reader) {
	br.Reset(nil) // release reference to the underlying reader
	c.lineReaderPool.Put(br)
}

func parseManifest(raw []byte) (*Manifest, error) {
	reader := bufio.NewReader(bytes.NewReader(raw))
	manifest := &Manifest{wireBytes: int64(len(raw))}
	// Pre-allocate based on newline count — an upper bound on entry count that
	// avoids repeated growslice doublings (and the GC pauses they trigger) for
	// large manifests.
	capacityHint := bytes.Count(raw, []byte{'\n'})
	manifest.Entries = make([]ManifestEntry, 0, capacityHint)
	seenHeader := false
	seenIDs := make(map[uint64]struct{}, capacityHint)
	var prevPath string
	var prevMtime string
	var lastID uint64
	haveLastID := false
	rootSeen := false

	for {
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read manifest line: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			if strings.HasPrefix(trimmed, "FM/1 ") {
				hdr, parseErr := intencoding.ParseManifestHeader(trimmed)
				if parseErr != nil {
					return nil, parseErr
				}
				if !seenHeader {
					manifest.TransferID = hdr.TransferID
					manifest.Root = hdr.Root
					manifest.Mode = hdr.Mode
					manifest.LinkMbps = hdr.LinkMbps
					manifest.Concurrency = hdr.Concurrency
					manifest.DeadlineMS = hdr.DeadlineMS
					seenHeader = true
				} else if manifest.TransferID != hdr.TransferID || (hdr.Root != "" && manifest.Root != hdr.Root) || manifest.Mode != hdr.Mode || manifest.LinkMbps != hdr.LinkMbps || manifest.Concurrency != hdr.Concurrency {
					return nil, errors.New("manifest chunk header mismatch")
				}
				prevPath = ""
				prevMtime = ""
			} else {
				if !seenHeader {
					return nil, errors.New("manifest entry before header")
				}
				raw, nextPath, nextMtime, parseErr := intencoding.ParseManifestEntry(trimmed, prevPath, prevMtime)
				if parseErr != nil {
					return nil, parseErr
				}
				entry := ManifestEntry{
					Type:       raw.Type,
					ID:         raw.ID,
					Size:       raw.Size,
					Mtime:      raw.Mtime,
					Mode:       raw.Mode,
					Path:       raw.Path,
					LinkTarget: raw.LinkTarget,
					LinkPath:   raw.LinkPath,
					PageCache:  raw.PageCache,
				}
				if entry.ID == intencoding.RootFileID && entry.Type == intencoding.EntryTypeDir && filepath.IsAbs(entry.Path) {
					if _, exists := seenIDs[entry.ID]; exists {
						return nil, fmt.Errorf("duplicate manifest id: %d", entry.ID)
					}
					if strings.TrimSpace(manifest.Root) != "" && manifest.Root != filepath.Clean(entry.Path) {
						return nil, errors.New("manifest root entry/header mismatch")
					}
					manifest.Root = filepath.Clean(entry.Path)
					manifest.rootEntry = entry
					rootSeen = true
					seenIDs[entry.ID] = struct{}{}
					lastID = entry.ID
					haveLastID = true
					prevPath = nextPath
					prevMtime = nextMtime
					continue
				}
				if !rootSeen && strings.TrimSpace(manifest.Root) == "" {
					return nil, errors.New("manifest missing root entry")
				}
				if _, exists := seenIDs[entry.ID]; exists {
					return nil, fmt.Errorf("duplicate manifest id: %d", entry.ID)
				}
				if haveLastID && entry.ID <= lastID {
					return nil, fmt.Errorf("manifest ids must be increasing: prev=%d curr=%d", lastID, entry.ID)
				}
				seenIDs[entry.ID] = struct{}{}
				lastID = entry.ID
				haveLastID = true
				manifest.Entries = append(manifest.Entries, entry)
				prevPath = nextPath
				prevMtime = nextMtime
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}

	if !seenHeader {
		return nil, errors.New("manifest missing header")
	}
	if strings.TrimSpace(manifest.Root) == "" {
		return nil, errors.New("manifest missing root entry")
	}

	sort.Slice(manifest.Entries, func(i, j int) bool { return manifest.Entries[i].ID < manifest.Entries[j].ID })
	manifest.Size() // warm cache
	return manifest, nil
}

func marshalManifest(manifest *Manifest) ([]byte, error) {
	if manifest == nil {
		return nil, errors.New("nil manifest")
	}
	if strings.TrimSpace(manifest.TransferID) == "" {
		return nil, errors.New("manifest transfer id is required")
	}
	entries := append([]ManifestEntry(nil), manifest.Entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	mode := strings.ToLower(strings.TrimSpace(manifest.Mode))
	if mode != LoadStrategyFast && mode != LoadStrategyGentle {
		return nil, errors.New("manifest mode must be fast or gentle")
	}
	if manifest.LinkMbps < 0 {
		return nil, errors.New("manifest link mbps must be >= 0")
	}
	if manifest.Concurrency <= 0 {
		return nil, errors.New("manifest concurrency must be > 0")
	}
	var b strings.Builder
	hdr := intencoding.FormatManifestHeader(intencoding.ManifestHeader{
		TransferID:  manifest.TransferID,
		Mode:        mode,
		LinkMbps:    manifest.LinkMbps,
		Concurrency: manifest.Concurrency,
		DeadlineMS:  manifest.DeadlineMS,
	})
	b.WriteString(hdr)
	b.WriteByte('\n')
	prevPath := ""
	prevMtime := ""
	if strings.TrimSpace(manifest.Root) == "" {
		return nil, errors.New("manifest root is required")
	}
	rootEntry := ManifestEntry{
		Type:       intencoding.EntryTypeDir,
		ID:         intencoding.RootFileID,
		Size:       0,
		Mtime:      0,
		Mode:       0o755,
		Path:       filepath.Clean(manifest.Root),
		LinkTarget: -1,
	}
	if manifest.rootEntry.Path != "" {
		rootEntry = manifest.rootEntry
		rootEntry.Type = intencoding.EntryTypeDir
		rootEntry.ID = intencoding.RootFileID
		rootEntry.Size = 0
		rootEntry.Path = filepath.Clean(rootEntry.Path)
		rootEntry.LinkTarget = -1
	}
	line, nextPath, nextMtime, err := intencoding.MarshalManifestEntry(intencoding.ManifestEntry{
		Type:       rootEntry.Type,
		ID:         rootEntry.ID,
		Size:       rootEntry.Size,
		Mtime:      rootEntry.Mtime,
		Mode:       rootEntry.Mode,
		Path:       rootEntry.Path,
		LinkTarget: rootEntry.LinkTarget,
	}, prevPath, prevMtime)
	if err != nil {
		return nil, err
	}
	b.WriteString(line)
	b.WriteByte('\n')
	prevPath = nextPath
	prevMtime = nextMtime
	seenIDs := map[uint64]struct{}{intencoding.RootFileID: {}}
	for i, entry := range entries {
		if _, exists := seenIDs[entry.ID]; exists {
			return nil, fmt.Errorf("duplicate manifest id: %d", entry.ID)
		}
		seenIDs[entry.ID] = struct{}{}
		if i > 0 && entry.ID <= entries[i-1].ID {
			return nil, fmt.Errorf("manifest ids must be increasing: prev=%d curr=%d", entries[i-1].ID, entry.ID)
		}
		line, nextPath, nextMtime, err := intencoding.MarshalManifestEntry(intencoding.ManifestEntry{
			Type:       entry.Type,
			ID:         entry.ID,
			Size:       entry.Size,
			Mtime:      entry.Mtime,
			Mode:       entry.Mode,
			Path:       entry.Path,
			LinkTarget: entry.LinkTarget,
			LinkPath:   entry.LinkPath,
			PageCache:  entry.PageCache,
		}, prevPath, prevMtime)
		if err != nil {
			return nil, err
		}
		b.WriteString(line)
		b.WriteByte('\n')
		prevPath = nextPath
		prevMtime = nextMtime
	}
	return []byte(b.String()), nil
}

func resolveManifestEntryPath(manifest *Manifest, fileID uint64) (ManifestEntry, string, error) {
	entry, ok := manifest.EntryByID(fileID)
	if !ok {
		return ManifestEntry{}, "", fmt.Errorf("file id %d not in manifest", fileID)
	}
	serverPath := filepath.Clean(filepath.Join(manifest.Root, filepath.FromSlash(entry.Path)))
	if !filepath.IsAbs(serverPath) {
		return ManifestEntry{}, "", fmt.Errorf("resolved file path is not absolute: %s", serverPath)
	}
	return entry, serverPath, nil
}

func cloneTrailerMetadata(meta *FileTrailerMetadata) *FileTrailerMetadata {
	if meta == nil {
		return nil
	}
	cloned := *meta
	return &cloned
}

type fileStream struct {
	respBody io.Closer
	br       frameLineReader
	identity age.Identity

	meta *FileFrameMeta

	frameMeta   FileFrameMeta
	logical     io.ReadCloser
	logicalRead int64

	expectOffset    bool
	expectedOffset  int64
	expectNextFrame bool

	pendingErr error
	finished   bool
	closed     bool
	pending    *FileFrameMeta
	releaseBr  func()
}

type readerWithCloser struct {
	io.Reader
	io.Closer
}

type frameLineReader interface {
	io.Reader
	ReadString(delim byte) (string, error)
}

type pooledLineReader struct {
	reader io.Reader
	buf    []byte
	off    int
	n      int
}

func newPooledLineReader(reader io.Reader, buf []byte) *pooledLineReader {
	if len(buf) == 0 {
		buf = make([]byte, 4096)
	}
	return &pooledLineReader{
		reader: reader,
		buf:    buf,
	}
}

func (r *pooledLineReader) Read(p []byte) (int, error) {
	if r == nil {
		return 0, errors.New("nil pooled line reader")
	}
	for {
		if r.off < r.n {
			n := copy(p, r.buf[r.off:r.n])
			r.off += n
			return n, nil
		}
		if err := r.fill(); err != nil {
			return 0, err
		}
	}
}

func (r *pooledLineReader) fill() error {
	for {
		n, err := r.reader.Read(r.buf)
		if n > 0 {
			r.off = 0
			r.n = n
			return nil
		}
		if err != nil {
			r.off = 0
			r.n = 0
			return err
		}
	}
}

func (r *pooledLineReader) ReadString(delim byte) (string, error) {
	if r == nil {
		return "", errors.New("nil pooled line reader")
	}
	out := make([]byte, 0, 128)
	for {
		if r.off >= r.n {
			if err := r.fill(); err != nil {
				if err == io.EOF && len(out) > 0 {
					return string(out), io.EOF
				}
				return string(out), err
			}
		}
		chunk := r.buf[r.off:r.n]
		if idx := bytes.IndexByte(chunk, delim); idx >= 0 {
			idx++
			out = append(out, chunk[:idx]...)
			r.off += idx
			return string(out), nil
		}
		out = append(out, chunk...)
		r.off = r.n
	}
}

func (s *fileStream) LastTrailerTS() int64 {
	if s == nil || s.meta == nil {
		return 0
	}
	return s.meta.TrailerTS
}

func (c *Client) fileStreamBufferHint(fileSizeHint int64, firstFrameSize int64) int64 {
	if fileSizeHint <= 0 {
		fileSizeHint = firstFrameSize
	}
	return int64(effectiveFrameReadBufferSize(c.FrameBufferBytes, fileSizeHint, c.MaxFrameReadBufferBytes))
}

func (c *Client) newFileStream(respBody io.ReadCloser, ageIdentity string, fileSizeHint int64) (io.ReadCloser, *FileFrameMeta, error) {
	identity, err := parseAgeIdentity(ageIdentity)
	if err != nil {
		return nil, nil, err
	}
	probe := bufio.NewReader(respBody)
	headerLine, err := probe.ReadString('\n')
	if err != nil {
		return nil, nil, fmt.Errorf("read frame header: %w", err)
	}
	firstMeta, err := parseFXHeader(strings.TrimRight(headerLine, "\r\n"))
	if err != nil {
		return nil, nil, err
	}
	if firstMeta.Size < 0 || firstMeta.WireSize < 0 {
		return nil, nil, errors.New("negative frame size")
	}
	if firstMeta.Offset < 0 {
		return nil, nil, errors.New("negative frame offset")
	}

	bufferHint := c.fileStreamBufferHint(fileSizeHint, firstMeta.Size)
	readBuf, release := c.acquireFrameReadBuffer(bufferHint)
	stream := &fileStream{
		respBody:  respBody,
		br:        newPooledLineReader(probe, readBuf),
		identity:  identity,
		meta:      &FileFrameMeta{},
		pending:   &firstMeta,
		releaseBr: release,
	}
	if err := stream.openNextFrame(); err != nil {
		release()
		return nil, nil, err
	}
	return stream, stream.meta, nil
}

func (s *fileStream) Read(p []byte) (int, error) {
	if s.pendingErr != nil {
		err := s.pendingErr
		s.pendingErr = nil
		return 0, err
	}
	if s.finished {
		return 0, io.EOF
	}

	for {
		if s.logical == nil {
			if err := s.openNextFrame(); err != nil {
				if errors.Is(err, io.EOF) {
					s.finished = true
					return 0, io.EOF
				}
				return 0, err
			}
		}

		n, err := s.logical.Read(p)
		if n > 0 {
			s.logicalRead += int64(n)
		}
		if err == nil {
			return n, nil
		}
		if !errors.Is(err, io.EOF) {
			return n, err
		}

		frameErr := s.finishFrame()
		if n > 0 {
			s.pendingErr = frameErr
			return n, nil
		}
		if frameErr != nil {
			return 0, frameErr
		}
	}
}

func (s *fileStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	if !s.finished {
		_, _ = io.Copy(io.Discard, s)
	}
	logicalErr := error(nil)
	if s.logical != nil {
		logicalErr = s.logical.Close()
	}
	if s.releaseBr != nil {
		s.releaseBr()
		s.releaseBr = nil
	}
	bodyErr := s.respBody.Close()
	if logicalErr != nil {
		return logicalErr
	}
	if bodyErr != nil {
		return bodyErr
	}
	return nil
}

func (s *fileStream) openNextFrame() error {
	var meta FileFrameMeta
	if s.pending != nil {
		meta = *s.pending
		s.pending = nil
	} else {
		headerLine, err := s.br.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && headerLine == "" {
				if s.expectNextFrame {
					return errors.New("missing next frame after trailer next offset")
				}
				return io.EOF
			}
			return fmt.Errorf("read frame header: %w", err)
		}
		trimmedHeader := strings.TrimRight(headerLine, "\r\n")
		if s.expectOffset && !s.expectNextFrame && isStatusLine(trimmedHeader) {
			if err := parseErrControlFrame(trimmedHeader); err != nil {
				return err
			}
			if _, ok := parseOKStatusLine(trimmedHeader); ok {
				return io.EOF
			}
			return errors.New("unexpected terminal status line")
		}
		if s.expectOffset && !s.expectNextFrame {
			return errors.New("unexpected extra frame after terminal trailer")
		}

		meta, err = parseFXHeader(trimmedHeader)
		if err != nil {
			return err
		}
	}
	if meta.Size < 0 || meta.WireSize < 0 {
		return errors.New("negative frame size")
	}
	if meta.Offset < 0 {
		return errors.New("negative frame offset")
	}
	if s.expectOffset && meta.Offset != s.expectedOffset {
		return fmt.Errorf("non-contiguous frame offset: expected=%d got=%d", s.expectedOffset, meta.Offset)
	}
	if s.meta.FileID == 0 && s.meta.Comp == "" && s.meta.Size == 0 && s.meta.WireSize == 0 {
		s.meta.FileID = meta.FileID
		s.meta.Comp = meta.Comp
		s.meta.Offset = meta.Offset
		s.meta.MaxWireSizeHint = meta.MaxWireSizeHint
		s.meta.HeaderTS = meta.HeaderTS
	} else {
		if meta.FileID != s.meta.FileID {
			return fmt.Errorf("file id mismatch across frames: expected=%d got=%d", s.meta.FileID, meta.FileID)
		}
	}

	payloadReader := io.LimitReader(s.br, meta.WireSize)
	logicalReader, err := decodePayloadReader(payloadReader, meta.Comp)
	if err != nil {
		return fmt.Errorf("decode payload reader: %w", err)
	}
	s.frameMeta = meta
	s.logical = logicalReader
	s.logicalRead = 0
	s.expectOffset = false
	s.expectNextFrame = false
	return nil
}

func (s *fileStream) finishFrame() error {
	if s.logicalRead != s.frameMeta.Size {
		return fmt.Errorf("logical size mismatch: declared=%d actual=%d", s.frameMeta.Size, s.logicalRead)
	}
	trailerLine, err := s.br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read frame trailer: %w", err)
	}
	trailer, err := parseFXTrailer(strings.TrimRight(trailerLine, "\r\n"))
	if err != nil {
		return err
	}
	if trailer.FileID != s.frameMeta.FileID {
		return fmt.Errorf("trailer file id mismatch: header=%d trailer=%d", s.frameMeta.FileID, trailer.FileID)
	}

	nextOffset := s.frameMeta.Offset + s.frameMeta.Size
	if trailer.Next != nil {
		if *trailer.Next == 0 {
			s.expectNextFrame = false
		} else {
			if *trailer.Next != nextOffset {
				return fmt.Errorf("invalid trailer next offset: expected=%d got=%d", nextOffset, *trailer.Next)
			}
			s.expectNextFrame = true
		}
	}
	s.expectOffset = true
	s.expectedOffset = nextOffset

	s.meta.Size += s.frameMeta.Size
	s.meta.WireSize += s.frameMeta.WireSize
	if s.meta.CompCounts == nil {
		s.meta.CompCounts = make(map[string]uint64, 3)
	}
	s.meta.CompCounts[s.frameMeta.Comp]++
	if s.meta.Comp != s.frameMeta.Comp {
		s.meta.Comp = "mixed"
	}
	s.meta.TrailerTS = trailer.TS
	s.meta.HashToken = trailer.HashToken
	if trailer.FileHashToken != "" {
		s.meta.FileHashToken = trailer.FileHashToken
	}
	if trailer.Metadata != nil {
		s.meta.TrailerMetadata = cloneTrailerMetadata(trailer.Metadata)
	}

	closeErr := s.logical.Close()
	s.logical = nil
	return closeErr
}

func parseFXHeader(line string) (FileFrameMeta, error) {
	fields := strings.Fields(line)
	if len(fields) < 3 || fields[0] != "FX/1" {
		return FileFrameMeta{}, errors.New("invalid FX/1 header")
	}
	fileID, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return FileFrameMeta{}, fmt.Errorf("invalid header file id: %w", err)
	}
	props := make(map[string]string, len(fields)-2)
	for _, token := range fields[2:] {
		parts := strings.SplitN(token, "=", 2)
		if len(parts) != 2 {
			continue
		}
		props[parts[0]] = parts[1]
	}

	comp := props["comp"]
	offset, err := parseHeaderInt(props["offset"], "offset")
	if err != nil {
		return FileFrameMeta{}, err
	}
	size, err := parseHeaderInt(props["size"], "size")
	if err != nil {
		return FileFrameMeta{}, err
	}
	wsize, err := parseHeaderInt(props["wsize"], "wsize")
	if err != nil {
		return FileFrameMeta{}, err
	}
	ts, err := parseHeaderInt(props["ts"], "ts")
	if err != nil {
		return FileFrameMeta{}, err
	}
	maxWSizeHint := int64(0)
	if raw, ok := props["max-wsize"]; ok {
		maxWSizeHint, err = parseHeaderInt(raw, "max-wsize")
		if err != nil {
			return FileFrameMeta{}, err
		}
		if maxWSizeHint <= 0 {
			return FileFrameMeta{}, errors.New("invalid header max-wsize")
		}
	}
	if ts < 0 {
		return FileFrameMeta{}, errors.New("invalid header ts")
	}
	if comp == "" {
		return FileFrameMeta{}, errors.New("missing required frame properties")
	}
	return FileFrameMeta{
		FileID:          fileID,
		Comp:            comp,
		Offset:          offset,
		Size:            size,
		WireSize:        wsize,
		MaxWireSizeHint: maxWSizeHint,
		HeaderTS:        ts,
	}, nil
}

func parseHeaderInt(raw string, key string) (int64, error) {
	if raw == "" {
		return 0, fmt.Errorf("missing header property: %s", key)
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid header property %s: %w", key, err)
	}
	return v, nil
}

type frameTrailer struct {
	FileID         uint64
	TS             int64
	HashToken      string
	FileHashToken  string
	ChecksumPrefix string
	Next           *int64
	Metadata       *FileTrailerMetadata
}

func parseFXTrailer(line string) (frameTrailer, error) {
	prefix, hashToken, err := intencoding.SplitTrailerPrefixAndHash(line)
	if err != nil {
		return frameTrailer{}, err
	}
	fields := strings.Fields(prefix)
	if len(fields) < 3 || fields[0] != "FXT/1" {
		return frameTrailer{}, errors.New("invalid FXT/1 trailer")
	}
	fileID, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return frameTrailer{}, fmt.Errorf("invalid trailer file id: %w", err)
	}
	status := ""
	var fileHashToken string
	var ts int64 = -1
	var nextOffset *int64
	meta := FileTrailerMetadata{
		Size:    -1,
		MtimeNS: -1,
	}
	hasMeta := false
	for _, token := range fields[2:] {
		if val, ok := strings.CutPrefix(token, "status="); ok {
			status = val
		} else if tsRaw, ok := strings.CutPrefix(token, "ts="); ok {
			parsedTS, parseErr := strconv.ParseInt(tsRaw, 10, 64)
			if parseErr != nil || parsedTS < 0 {
				return frameTrailer{}, errors.New("invalid trailer ts")
			}
			ts = parsedTS
		} else if val, ok := strings.CutPrefix(token, "file-hash="); ok {
			fileHashToken = val
		} else if nextRaw, ok := strings.CutPrefix(token, "next="); ok {
			nextValue, parseErr := strconv.ParseInt(nextRaw, 10, 64)
			if parseErr != nil || nextValue < 0 {
				return frameTrailer{}, errors.New("invalid trailer next offset")
			}
			nextOffset = &nextValue
		}
		if strings.HasPrefix(token, "meta:") {
			parts := strings.SplitN(token, "=", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimPrefix(parts[0], "meta:")
			val := parts[1]
			switch key {
			case "size":
				sizeVal, parseErr := strconv.ParseInt(val, 10, 64)
				if parseErr != nil || sizeVal < 0 {
					return frameTrailer{}, errors.New("invalid trailer meta:size")
				}
				meta.Size = sizeVal
				hasMeta = true
			case "mtime_ns":
				mtimeVal, parseErr := strconv.ParseInt(val, 10, 64)
				if parseErr != nil || mtimeVal < 0 {
					return frameTrailer{}, errors.New("invalid trailer meta:mtime_ns")
				}
				meta.MtimeNS = mtimeVal
				hasMeta = true
			case "mode":
				meta.Mode = val
				hasMeta = true
			case "uid":
				meta.UID = val
				hasMeta = true
			case "gid":
				meta.GID = val
				hasMeta = true
			case "user":
				meta.User = val
				hasMeta = true
			case "group":
				meta.Group = val
				hasMeta = true
			}
		}
	}
	if status != "ok" {
		return frameTrailer{}, fmt.Errorf("trailer status not ok: %s", status)
	}
	if ts < 0 {
		return frameTrailer{}, errors.New("trailer missing ts")
	}
	if fileHashToken != "" && !intencoding.ValidHashToken(fileHashToken) {
		return frameTrailer{}, errors.New("trailer invalid file hash token")
	}
	var metaPtr *FileTrailerMetadata
	if hasMeta {
		if meta.Size < 0 {
			meta.Size = 0
		}
		if meta.MtimeNS < 0 {
			meta.MtimeNS = 0
		}
		metaPtr = &meta
	}
	return frameTrailer{
		FileID:         fileID,
		TS:             ts,
		HashToken:      hashToken,
		FileHashToken:  fileHashToken,
		ChecksumPrefix: prefix,
		Next:           nextOffset,
		Metadata:       metaPtr,
	}, nil
}

func parseAgeIdentity(raw string) (age.Identity, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	identity, err := age.ParseX25519Identity(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid age identity: %w", err)
	}
	return identity, nil
}

func decodePayloadReader(payload io.Reader, comp string) (io.ReadCloser, error) {
	switch comp {
	case "none":
		return io.NopCloser(payload), nil
	case EncodingZstd, EncodingLz4:
		reader, err := intencoding.WrapDecompressedReader(payload, comp)
		if err != nil {
			return nil, err
		}
		return reader, nil
	default:
		return nil, fmt.Errorf("unsupported compression mode: %s", comp)
	}
}
