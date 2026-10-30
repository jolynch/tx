package faults

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/events"
	"golang.org/x/sys/unix"
)

// A nil injector must hand back exactly what it was given and allocate
// nothing, since production code calls it on every connection, file, and
// syscall site.
func TestNilInjectorIsIdentity(t *testing.T) {
	var inj *Injector
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if got := inj.WrapConn(a, Client); got != a {
		t.Fatalf("WrapConn returned %T, want the connection itself", got)
	}
	r := strings.NewReader("data")
	if got := inj.WrapReaderAt(context.Background(), SiteSendRead, 1, r); got != io.ReaderAt(r) {
		t.Fatalf("WrapReaderAt returned %T, want the reader itself", got)
	}
	w := nopWriteCloser{&bytes.Buffer{}}
	if got := inj.WrapWriter(SiteOutputWrite, 1, 0, w); got != io.WriteCloser(w) {
		t.Fatalf("WrapWriter returned %T, want the writer itself", got)
	}
	if err := inj.Sys(SysSplice); err != nil {
		t.Fatalf("Sys = %v", err)
	}
	if err := inj.OpenErr(SiteSendOpen, 1, 0); err != nil {
		t.Fatalf("OpenErr = %v", err)
	}
	if inj.LoseResponse() || inj.DupRequest() || inj.Busy() || inj.AcceptErr() != nil || inj.DialErr(context.Background()) != nil {
		t.Fatal("a nil injector fired a request or connection fault")
	}
	inj.DelayRequest()
	samePointer := func(name string, got, want any) {
		t.Helper()
		if reflect.ValueOf(got).Pointer() != reflect.ValueOf(want).Pointer() {
			t.Fatalf("%s wrapped the function", name)
		}
	}
	samePointer("WrapSplice", inj.WrapSplice(unix.Splice), unix.Splice)
	samePointer("WrapTee", inj.WrapTee(unix.Tee), unix.Tee)
	samePointer("WrapPoll", inj.WrapPoll(unix.Poll), unix.Poll)
	samePointer("WrapCopyFileRange", inj.WrapCopyFileRange(unix.CopyFileRange), unix.CopyFileRange)
	if FromContext(NewContext(context.Background(), inj)) != nil {
		t.Fatal("a nil injector reached the context")
	}
	if inj.Repro() != "" || inj.Seed() != 0 {
		t.Fatal("a nil injector reports a schedule")
	}
}

func TestNilInjectorDoesNotAllocate(t *testing.T) {
	var inj *Injector
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ctx := context.Background()
	allocs := testing.AllocsPerRun(100, func() {
		_ = inj.WrapConn(a, Server)
		_ = inj.WrapReaderAt(ctx, SiteCXSUMRead, 1, f)
		_ = inj.WrapWriter(SiteOutputWrite, 1, 0, f)
		_ = inj.Sys(SysFadvise)
		_ = inj.OpenErr(SiteCXSUMOpen, 1, 0)
		_, _, _, _ = inj.LoseResponse(), inj.DupRequest(), inj.Busy(), inj.AcceptErr()
		_ = inj.DialErr(ctx)
		inj.DelayRequest()
		_ = inj.WrapSplice(unix.Splice)
		_ = inj.WrapCopyFileRange(unix.CopyFileRange)
		_ = NewContext(ctx, inj)
	})
	if allocs != 0 {
		t.Fatalf("nil injector allocated %.1f times per run", allocs)
	}
}

// An injector with no fault kinds wraps everything and changes nothing.
func TestEmptyInjectorIsTransparent(t *testing.T) {
	inj := New(Config{Seed: 7})

	t.Run("conn", func(t *testing.T) {
		a, b := net.Pipe()
		wa, wb := inj.WrapConn(a, Client), inj.WrapConn(b, Server)
		defer wa.Close()
		defer wb.Close()
		if _, ok := wa.(*Conn); !ok {
			t.Fatalf("WrapConn returned %T, want *Conn", wa)
		}
		msg := []byte("SEND tid=1\r\n")
		go func() { _, _ = wa.Write(msg) }()
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(wb, got); err != nil || !bytes.Equal(got, msg) {
			t.Fatalf("read %q, %v; want %q", got, err, msg)
		}
		if wa.(*Conn).ZeroCopyConn() != nil {
			t.Fatal("a pipe offered a zero-copy socket")
		}
		if _, err := wa.(*Conn).SyscallConn(); err == nil {
			t.Fatal("a pipe offered a syscall.Conn")
		}
		if err := wa.(*Conn).CloseWrite(); !errors.Is(err, errors.ErrUnsupported) {
			t.Fatalf("CloseWrite on a pipe = %v, want ErrUnsupported", err)
		}
	})

	t.Run("tcp", func(t *testing.T) {
		ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
		if err != nil {
			t.Fatal(err)
		}
		server, err := ln.AcceptTCP()
		if err != nil {
			t.Fatal(err)
		}
		defer server.Close()
		wrapped := inj.WrapConn(client, Client).(*Conn)
		defer wrapped.Close()
		if wrapped.ZeroCopyConn() != client || wrapped.Unwrap() != net.Conn(client) {
			t.Fatal("wrapper hid its TCP socket")
		}
		if _, err := wrapped.SyscallConn(); err != nil {
			t.Fatalf("SyscallConn: %v", err)
		}
		if _, err := wrapped.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := wrapped.CloseWrite(); err != nil {
			t.Fatalf("CloseWrite: %v", err)
		}
		got, err := io.ReadAll(server)
		if err != nil || string(got) != "x" {
			t.Fatalf("server read %q, %v after CloseWrite", got, err)
		}
	})

	t.Run("disk", func(t *testing.T) {
		src := strings.NewReader("0123456789")
		r := inj.WrapReaderAt(context.Background(), SiteVerifyRead, 1, src)
		buf := make([]byte, 4)
		if n, err := r.ReadAt(buf, 3); n != 4 || err != nil || string(buf) != "3456" {
			t.Fatalf("ReadAt = %d %v %q", n, err, buf)
		}
		var out bytes.Buffer
		w := inj.WrapWriter(SiteOutputWrite, 1, 0, nopWriteCloser{&out})
		if _, err := w.Write([]byte("abc")); err != nil || out.String() != "abc" {
			t.Fatalf("Write: %v, wrote %q", err, out.String())
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("syscalls", func(t *testing.T) {
		calls := 0
		splice := inj.WrapSplice(func(int, *int64, int, *int64, int, int) (int64, error) {
			calls++
			return 5, nil
		})
		if n, err := splice(0, nil, 0, nil, 5, 0); n != 5 || err != nil || calls != 1 {
			t.Fatalf("splice = %d, %v after %d calls", n, err, calls)
		}
	})

	if FromContext(NewContext(context.Background(), inj)) != inj {
		t.Fatal("injector did not round-trip through a context")
	}
	if got := inj.Repro(); got != "TX_FAULTS_SEED=7" {
		t.Fatalf("Repro = %q", got)
	}
}

func TestFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     map[string]string
		enabled bool
		seed    uint64
		wantErr string
	}{
		{name: "unset", env: nil},
		{name: "seed", env: map[string]string{EnvSeed: "42"}, enabled: true, seed: 42},
		{name: "bad seed", env: map[string]string{EnvSeed: "x"}, wantErr: EnvSeed},
		{name: "unknown kind", env: map[string]string{EnvNet: "teleport=0.1"}, wantErr: `unknown NET fault kind "teleport"`},
		{name: "unbalanced", env: map[string]string{EnvSys: "splice.latency=1:β(1ms,2ms"}, wantErr: "unbalanced"},
		{name: "blank variables stay off", env: map[string]string{EnvDisk: "", EnvSeed: " "}},
		{name: "separators alone stay off", env: map[string]string{EnvNet: " , ,"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inj, err := FromEnv(func(k string) (string, bool) {
				v, ok := tc.env[k]
				return v, ok
			})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (inj != nil) != tc.enabled {
				t.Fatalf("enabled = %t, want %t", inj != nil, tc.enabled)
			}
			if tc.seed != 0 && inj.Seed() != tc.seed {
				t.Fatalf("seed = %d, want %d", inj.Seed(), tc.seed)
			}
		})
	}
}

func TestSplitTopLevel(t *testing.T) {
	got, err := splitTopLevel(" reset=0.1, pause=0.01:β(100ms,500ms,4s,5s) ,,flip=1e-6")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"reset=0.1", "pause=0.01:β(100ms,500ms,4s,5s)", "flip=1e-6"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if _, err := splitTopLevel("a=1)"); err == nil {
		t.Fatal("accepted an unbalanced ')'")
	}
}

// memFile is an in-memory file a writer can seek forward in, leaving
// zeros in any gap it later writes past.
type memFile struct {
	b   []byte
	pos int64
}

func (f *memFile) Write(p []byte) (int, error) {
	if end := f.pos + int64(len(p)); end > int64(len(f.b)) {
		f.b = append(f.b, make([]byte, end-int64(len(f.b)))...)
	}
	n := copy(f.b[f.pos:], p)
	f.pos += int64(n)
	return n, nil
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekCurrent {
		return 0, errors.New("memFile seeks only from the current position")
	}
	f.pos += offset
	return f.pos, nil
}

func (f *memFile) Close() error { return nil }

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// one returns an injector with a single family spec.
func one(t *testing.T, f Family, spec string) *Injector {
	t.Helper()
	i, err := Build(Config{Seed: 1, Specs: map[Family]string{f: spec}})
	if err != nil {
		t.Fatal(err)
	}
	return i
}

func near(got, want, tol float64) bool { return math.Abs(got-want) <= tol }

func TestParseDist(t *testing.T) {
	ms := func(d float64) float64 { return d / 1000 }
	type check struct {
		p, want float64 // p < 0 checks the mean
	}
	for _, tc := range []struct {
		spec   string
		checks []check
	}{
		{"β(1ms,5ms,40ms,50ms)", []check{{-1, ms(5)}, {0.99, ms(40)}}},
		{"Β(1ms,5ms,40ms,50ms)", []check{{-1, ms(5)}, {0.99, ms(40)}}},
		{"beta(1ms, 5ms, 40ms, 50ms)", []check{{-1, ms(5)}, {0.99, ms(40)}}},
		{"Beta(1ms,5ms,40ms,50ms)", []check{{-1, ms(5)}, {0.99, ms(40)}}},
		{"β(1ms,5ms,p75=6ms,50ms)", []check{{-1, ms(5)}, {0.75, ms(6)}}},
		{"β(1ms,p50=4ms,40ms,50ms)", []check{{0.5, ms(4)}, {0.99, ms(40)}}},
		{"β(1ms,p10=2ms,p50=5ms,50ms)", []check{{0.1, ms(2)}, {0.5, ms(5)}}},
		{"β(1ms,avg=5ms,40ms,50ms)", []check{{-1, ms(5)}}},
		{"β(1ms,mean=5ms,40ms,50ms)", []check{{-1, ms(5)}}},
		{"β(1ms,μ=5ms,40ms,50ms)", []check{{-1, ms(5)}}},
		{"β(1ms,µ=5ms,40ms,50ms)", []check{{-1, ms(5)}}},
		{"β(1ms,p10=2ms,avg=5ms,50ms)", []check{{-1, ms(5)}, {0.1, ms(2)}}},
		{"β(1ms,α=2,β=8,11ms)", []check{{-1, ms(3)}}},
		{"β(1ms,alpha=2,beta=8,11ms)", []check{{-1, ms(3)}}},
		{"io2", []check{{-1, 100e-6 + 0.2*1400e-6}}},
	} {
		d, err := ParseDist(tc.spec)
		if err != nil {
			t.Fatalf("%s: %v", tc.spec, err)
		}
		if d.String() != tc.spec {
			t.Fatalf("String() = %q, want %q", d.String(), tc.spec)
		}
		for _, c := range tc.checks {
			got := d.Beta().Mean()
			if c.p >= 0 {
				got = d.Beta().Quantile(c.p)
			}
			if !near(got, c.want, 0.002*c.want+1e-6) {
				t.Fatalf("%s: statistic at %g = %g, want %g", tc.spec, c.p, got, c.want)
			}
		}
	}
	for _, name := range []string{"gp3", "i4i", "ephem"} {
		if _, err := ParseDist(name); err != nil {
			t.Fatalf("preset %s: %v", name, err)
		}
	}
	for _, bad := range []string{
		"β(5ms,1ms,40ms,50ms)",         // not increasing
		"β(1ms,avg=2ms,mean=3ms,50ms)", // two averages
		"β(1ms,p50=2ms,p50=3ms,50ms)",  // same percentile twice
		"β(1ms,p90=2ms,p50=3ms,50ms)",  // percentiles out of order
		"β(1ms,2ms,3ms)",               // three values
		"β(1ms,q50=2ms,3ms,50ms)",      // unknown statistic
		"β(1ms,α=2,40ms,50ms)",         // a shape and a duration
		"β(1ms,p100=2ms,3ms,50ms)",     // percentile out of range
		"β(1ms,2ms,3ms,50ms",           // unclosed
		"β(1ms,2ms,3ms,-50ms)",         // negative
		"nope",                         // neither a preset nor a β
	} {
		if _, err := ParseDist(bad); err == nil {
			t.Fatalf("ParseDist(%q) accepted", bad)
		}
	}
}

func TestSpecs(t *testing.T) {
	for _, tc := range []struct {
		f       Family
		spec    string
		wantErr string
	}{
		{FamilyNet, "delay=0.1", "takes a duration"},
		{FamilyNet, "reset=0.1:β(1ms,2ms,3ms,4ms)", "takes no duration"},
		{FamilyNet, "reset=2", "probability"},
		{FamilyNet, "reset", "want kind=rate"},
		{FamilyNet, "teleport=0.1", `unknown NET fault kind "teleport"`},
		{FamilySys, "splice.bogus=0.1", "unknown SYS"},
		{FamilySys, "fstat.short=0.1", "unknown SYS"},
		{FamilyMsg, "delay=0.1:β(1ms,2ms,3ms)", "want 4 values"},
	} {
		_, err := Build(Config{Specs: map[Family]string{tc.f: tc.spec}})
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("%s %q: err %v, want %q", tc.f, tc.spec, err, tc.wantErr)
		}
	}
	// A specific SYS entry overrides a wildcard whatever the order.
	i := one(t, FamilySys, "fdatasync.fail=0.5, *.fail=0.1, *.short=0.2")
	if i.rates[syscallKind(SysFdatasync, sysFail)] != 0.5 || i.rates[syscallKind(SysFstat, sysFail)] != 0.1 {
		t.Fatal("wildcard overrode a specific entry")
	}
	if i.rates[syscallKind(SysSplice, sysShort)] != 0.2 || i.rates[syscallKind(SysFstat, sysShort)] != 0 {
		t.Fatal("*.short reached a syscall without a byte count")
	}
	// A profile, then the family variables kind by kind.
	bad := New(Config{Profile: "bad-day", Specs: map[Family]string{FamilyNet: "reset=0"}})
	if bad.rates[NetReset] != 0 || bad.rates[NetPartial] == 0 {
		t.Fatalf("bad-day with an override has kinds %v", bad.Kinds())
	}
	// bad-day leaves out UndetectedKinds and open.enoent, which leaves a file
	// missing for good.
	if bad.Undetected() || bad.rates[DiskOpenENOENT] != 0 {
		t.Fatalf("bad-day turns on %v", bad.Kinds())
	}
	// UndetectedKinds names exactly the three kinds bad-day leaves out.
	for _, k := range []Kind{DiskReadFlip, DiskWriteLost, DiskWriteFlip} {
		if !slices.Contains(UndetectedKinds(), k.String()) || bad.rates[k] != 0 {
			t.Fatalf("%s is not in UndetectedKinds or bad-day turns it on", k)
		}
	}
	if !one(t, FamilyDisk, "write.lost=0.5").Undetected() || one(t, FamilyDisk, "write.eio=0.5").Undetected() || (*Injector)(nil).Undetected() {
		t.Fatal("Undetected misjudges write.lost, write.eio, or a nil injector")
	}
	// A rate of 0 turns a profile's timed kind off without a duration.
	if quiet, err := Build(Config{Profile: "bad-day", Specs: map[Family]string{FamilySys: "*.latency=0"}}); err != nil || quiet.rates[syscallKind(SysSplice, sysLatency)] != 0 {
		t.Fatalf("bad-day with *.latency=0: %v", err)
	}
	if _, err := Build(Config{Specs: map[Family]string{FamilySys: "*.latency=0.1"}}); err == nil {
		t.Fatal("accepted a timed kind with a rate and no duration")
	}
	if _, err := Build(Config{Profile: "good-day"}); err == nil {
		t.Fatal("accepted an unknown profile")
	}
}

// TestReproRoundTrip checks the repro line builds the same injector.
func TestReproRoundTrip(t *testing.T) {
	want := New(Config{Seed: 42, Profile: "bad-day", Specs: map[Family]string{
		FamilyNet: "reset=0.5", FamilySys: "*.latency=0.1:β(1us,10us,200us,2ms)",
	}})
	env := map[string]string{}
	for _, field := range splitShell(want.Repro()) {
		k, v, _ := strings.Cut(field, "=")
		env[k] = strings.Trim(v, "'")
	}
	got, err := FromEnv(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	if got.Seed() != 42 || got.rates != want.rates || !slices.Equal(got.Kinds(), want.Kinds()) {
		t.Fatalf("repro %q rebuilt a different injector", want.Repro())
	}
}

// splitShell splits a repro line on spaces outside single quotes.
func splitShell(s string) []string {
	var out []string
	var cur strings.Builder
	quoted := false
	for _, r := range s {
		switch {
		case r == '\'':
			quoted = !quoted
			cur.WriteRune(r)
		case r == ' ' && !quoted:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	return append(out, cur.String())
}

// TestDecisionsReproduce checks a decision depends only on the seed, kind,
// key, and operation.
func TestDecisionsReproduce(t *testing.T) {
	a := New(Config{Seed: 7, Specs: map[Family]string{FamilyNet: "reset=0.3"}})
	b := New(Config{Seed: 7, Specs: map[Family]string{FamilyNet: "reset=0.3"}})
	c := New(Config{Seed: 8, Specs: map[Family]string{FamilyNet: "reset=0.3"}})
	same, fired := true, 0
	for op := range uint64(1000) {
		x := a.fires(NetReset, 99, op)
		if x != b.fires(NetReset, 99, op) {
			t.Fatalf("op %d: same seed, different decision", op)
		}
		same = same && x == c.fires(NetReset, 99, op)
		if x {
			fired++
		}
	}
	if same {
		t.Fatal("a different seed made every decision the same")
	}
	if fired < 240 || fired > 360 {
		t.Fatalf("rate 0.3 fired %d of 1000 times", fired)
	}
	// Retrying a file draws fresh decisions, in a reproducible sequence.
	x := one(t, FamilyDisk, "open.eio=0.5")
	y := one(t, FamilyDisk, "open.eio=0.5")
	var seq []bool
	for range 64 {
		ex := x.OpenErr(SiteSendOpen, 5, 0) != nil
		if ex != (y.OpenErr(SiteSendOpen, 5, 0) != nil) {
			t.Fatal("same seed and attempts, different open decision")
		}
		seq = append(seq, ex)
	}
	if !slices.Contains(seq, true) || !slices.Contains(seq, false) {
		t.Fatalf("64 attempts at one file all decided %v", seq[0])
	}
}

func TestFaultEvents(t *testing.T) {
	sink := events.NewSink()
	var got []string
	sink.Subscribe(func(e events.Event) {
		if v, ok := e.Get("kind"); ok && e.Name == "fault" {
			got = append(got, v.(string))
		}
	})
	i := New(Config{Seed: 1, Specs: map[Family]string{FamilyMsg: "lose-response=1"}, Events: sink})
	if !i.LoseResponse() || !slices.Equal(got, []string{"msg.lose-response"}) {
		t.Fatalf("events %v", got)
	}
	if c := i.Counts().InjectedByKind(); c["msg.lose-response"] != 1 {
		t.Fatalf("counts %v", c)
	}
}

func TestMsgFaults(t *testing.T) {
	if !one(t, FamilyMsg, "lose-response=1").LoseResponse() || !one(t, FamilyMsg, "dup=1").DupRequest() || !one(t, FamilyMsg, "busy=1").Busy() {
		t.Fatal("a MSG kind at rate 1 did not fire")
	}
	start := time.Now()
	one(t, FamilyMsg, "delay=1:β(20ms,25ms,30ms,40ms)").DelayRequest()
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("delay did not hold the request")
	}
	if New(Config{}).LoseResponse() {
		t.Fatal("a kind at rate 0 fired")
	}
}

func TestDiskFaults(t *testing.T) {
	ctx := context.Background()
	t.Run("open", func(t *testing.T) {
		if err := one(t, FamilyDisk, "open.enoent=1").OpenErr(SiteSendOpen, 1, 0); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("open.enoent: %v", err)
		}
		if err := one(t, FamilyDisk, "open.eio=1").OpenErr(SiteCXSUMOpen, 1, 0); !errors.Is(err, syscall.EIO) {
			t.Fatalf("open.eio: %v", err)
		}
	})
	data := []byte("0123456789abcdef")
	read := func(spec string) ([]byte, error, time.Duration) {
		r := one(t, FamilyDisk, spec).WrapReaderAt(ctx, SiteSendRead, 1, bytes.NewReader(data))
		p := make([]byte, len(data))
		start := time.Now()
		n, err := r.ReadAt(p, 0)
		return p[:n], err, time.Since(start)
	}
	if _, err, _ := read("read.eio=1"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("read.eio: %v", err)
	}
	if got, err, d := read("read.latency=1:β(20ms,25ms,30ms,40ms)"); err != nil || !bytes.Equal(got, data) || d < 20*time.Millisecond {
		t.Fatalf("read.latency read %q, %v after %v", got, err, d)
	}
	// read.latency ends with its request's context.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	slow := one(t, FamilyDisk, "read.latency=1:β(1s,2s,3s,4s)").WrapReaderAt(cancelled, SiteSendRead, 1, bytes.NewReader(data))
	start := time.Now()
	if _, err := slow.ReadAt(make([]byte, 4), 0); !errors.Is(err, context.Canceled) || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("read.latency with a cancelled context returned %v after %v", err, time.Since(start))
	}
	if got, err, _ := read("read.flip=1"); err != nil || len(got) != len(data) || bytes.Equal(got, data) {
		t.Fatalf("read.flip read %q, %v", got, err)
	}

	// Write faults are decided per 64 KiB block, so the write spans several.
	big := bytes.Repeat([]byte("0123456789abcdef"), 3*writeBlock/16)
	write := func(spec string) (int, error, []byte) {
		var out memFile
		w := one(t, FamilyDisk, spec).WrapWriter(SiteOutputWrite, 1, 0, &out)
		n, err := w.Write(big)
		return n, err, out.b
	}
	for _, tc := range []struct {
		spec  string
		errno syscall.Errno
	}{{"write.eio=1", syscall.EIO}, {"write.enospc=1", syscall.ENOSPC}} {
		if n, err, out := write(tc.spec); n != 0 || !errors.Is(err, tc.errno) || len(out) != 0 {
			t.Fatalf("%s: wrote %d (%d bytes), %v", tc.spec, n, len(out), err)
		}
	}
	if n, err, out := write("write.lost=1"); n != len(big) || err != nil || len(out) != 0 {
		t.Fatalf("write.lost: %d, %v, wrote %d bytes", n, err, len(out))
	}
	// Blocks start on 64 KiB file boundaries: a window that starts inside
	// one runs fault-free to the next boundary.
	var mid memFile
	midW := one(t, FamilyDisk, "write.eio=1").WrapWriter(SiteOutputWrite, 1, 100, &mid)
	if n, err := midW.Write(big); n != writeBlock-100 || !errors.Is(err, syscall.EIO) {
		t.Fatalf("write.eio from offset 100 wrote %d, %v; want %d, then EIO at the boundary", n, err, writeBlock-100)
	}
	// A writer that cannot seek past a lost block would shift every later
	// byte, so it loses none.
	var unseekable bytes.Buffer
	lostW := one(t, FamilyDisk, "write.lost=1").WrapWriter(SiteOutputWrite, 1, 0, nopWriteCloser{&unseekable})
	if n, err := lostW.Write(big); n != len(big) || err != nil || !bytes.Equal(unseekable.Bytes(), big) {
		t.Fatalf("write.lost on a writer that cannot seek: %d, %v, wrote %d bytes", n, err, unseekable.Len())
	}
	if n, err, out := write("write.torn=1"); n <= 0 || n >= writeBlock || !errors.Is(err, syscall.EIO) || !bytes.Equal(out, big[:n]) {
		t.Fatalf("write.torn: %d, %v, wrote %d bytes", n, err, len(out))
	}
	if n, err, out := write("write.short=1"); n <= 0 || n >= writeBlock || err != nil || !bytes.Equal(out, big[:n]) {
		t.Fatalf("write.short: %d, %v, wrote %d bytes", n, err, len(out))
	}
	if n, err, out := write("write.flip=1"); n != len(big) || err != nil || len(out) != len(big) || bitsDiffer(out, big) != 3 {
		t.Fatalf("write.flip: %d, %v, %d bits differ; want one per block", n, err, bitsDiffer(out, big))
	}
	// A lost block on a seekable file keeps the file's earlier bytes there
	// and every later byte in place.
	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// Sixteen blocks, so some are lost and some kept at rate 0.5, each
	// distinct, so a shifted block cannot pass for its neighbor.
	data16 := make([]byte, 16*writeBlock)
	for k := range data16 {
		data16[k] = byte(k>>16) + 'A'
	}
	old := bytes.Repeat([]byte{'x'}, len(data16))
	if _, err := f.Write(old); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	lost := one(t, FamilyDisk, "write.lost=0.5")
	if n, err := lost.WrapWriter(SiteOutputWrite, 1, 0, f).Write(data16); n != len(data16) || err != nil {
		t.Fatalf("write.lost=0.5 on a file: %d, %v", n, err)
	}
	got, err := os.ReadFile(f.Name())
	if err != nil || len(got) != len(data16) {
		t.Fatalf("file holds %d bytes (%v), want %d", len(got), err, len(data16))
	}
	lostBlocks := 0
	for b := 0; b < len(data16); b += writeBlock {
		switch block := got[b : b+writeBlock]; {
		case bytes.Equal(block, old[b:b+writeBlock]):
			lostBlocks++
		case !bytes.Equal(block, data16[b:b+writeBlock]):
			t.Fatalf("block at %d holds neither the new nor the old bytes", b)
		}
	}
	if lostBlocks == 0 || lostBlocks == len(data16)/writeBlock || lost.Counts().InjectedByKind()["disk.write.lost"] != uint64(lostBlocks) {
		t.Fatalf("%d of %d blocks lost, %v injected; want some but not all, matching", lostBlocks, len(data16)/writeBlock, lost.Counts().InjectedByKind())
	}
	start = time.Now()
	if n, err, out := write("write.latency=1:β(20ms,25ms,30ms,40ms)"); n != len(big) || err != nil || !bytes.Equal(out, big) || time.Since(start) < 20*time.Millisecond {
		t.Fatalf("write.latency: %d, %v, %d bytes", n, err, len(out))
	}
}

func bitsDiffer(a, b []byte) int {
	n := 0
	for k := range min(len(a), len(b)) {
		for x := a[k] ^ b[k]; x != 0; x &= x - 1 {
			n++
		}
	}
	return n
}

// TestWriteFaultsIgnoreChunking checks write faults land at the same file
// positions whether the bytes arrive in one write or many small ones, as
// they do when network reads cut them.
func TestWriteFaultsIgnoreChunking(t *testing.T) {
	data := make([]byte, 1<<20)
	for k := range data {
		data[k] = byte(k * 7)
	}
	const spec = "write.torn=0.15, write.short=0.1, write.flip=0.3, write.lost=0.2"
	// run writes data from offset 0 in pieces from size, stopping at the
	// first failure or short write as a caller would.
	run := func(size func() int) ([]byte, int) {
		var out memFile
		w := one(t, FamilyDisk, spec).WrapWriter(SiteOutputWrite, 3, 0, &out)
		done := 0
		for done < len(data) {
			piece := data[done:min(len(data), done+size())]
			n, err := w.Write(piece)
			done += n
			if err != nil || n < len(piece) {
				break
			}
		}
		return out.b, done
	}
	whole, wholeDone := run(func() int { return len(data) })
	if wholeDone == len(data) && bytes.Equal(whole, data) {
		t.Fatal("no write fault showed in the output; the test checks nothing")
	}
	r := rand.New(rand.NewPCG(1, 2))
	pieces, piecesDone := run(func() int { return 1 + r.IntN(40000) })
	if wholeDone != piecesDone || !bytes.Equal(whole, pieces) {
		t.Fatalf("one write stopped at %d with %d bytes out; pieces stopped at %d with %d bytes out",
			wholeDone, len(whole), piecesDone, len(pieces))
	}
}

func TestSysFaults(t *testing.T) {
	// Every errno a syscall can return gets picked.
	for sc := Syscall(1); sc < sysCount; sc++ {
		i := one(t, FamilySys, sc.String()+".fail=1")
		seen := map[syscall.Errno]bool{}
		for range 64 {
			var errno syscall.Errno
			if err := i.Sys(sc); !errors.As(err, &errno) || !slices.Contains(syscallErrnos[sc], errno) {
				t.Fatalf("%s.fail returned %v", sc, err)
			}
			seen[errno] = true
		}
		if len(seen) != len(syscallErrnos[sc]) {
			t.Fatalf("%s.fail returned only %v of %v", sc, seen, syscallErrnos[sc])
		}
	}
	start := time.Now()
	if err := one(t, FamilySys, "fstat.latency=1:β(20ms,25ms,30ms,40ms)").Sys(SysFstat); err != nil || time.Since(start) < 20*time.Millisecond {
		t.Fatalf("fstat.latency: %v after %v", err, time.Since(start))
	}
	i := one(t, FamilySys, "*.short=1")
	var asked []int
	_, _ = i.WrapSplice(func(_ int, _ *int64, _ int, _ *int64, n int, _ int) (int64, error) {
		asked = append(asked, n)
		return int64(n), nil
	})(0, nil, 0, nil, 100, 0)
	_, _ = i.WrapTee(func(_ int, _ int, n int, _ int) (int64, error) { asked = append(asked, n); return int64(n), nil })(0, 0, 100, 0)
	_, _ = i.WrapCopyFileRange(func(_ int, _ *int64, _ int, _ *int64, n int, _ int) (int, error) {
		asked = append(asked, n)
		return n, nil
	})(0, nil, 0, nil, 100, 0)
	if len(asked) != 3 || slices.ContainsFunc(asked, func(n int) bool { return n < 1 || n >= 100 }) {
		t.Fatalf("short asked for %v of 100 bytes", asked)
	}
}

// FuzzSpecRoundTrip checks the spec grammar: any family spec either fails to
// build or builds an injector whose repro line rebuilds it exactly.
func FuzzSpecRoundTrip(f *testing.F) {
	f.Add(uint8(0), "reset=0.5, pause=0.01:β(1ms,5ms,40ms,50ms)")
	f.Add(uint8(1), "delay=1:β(1ms,p50=4ms,40ms,50ms),dup=0.25")
	f.Add(uint8(2), "read.latency=0.3:gp3, write.torn=1")
	f.Add(uint8(3), "*.fail=0.1, fdatasync.fail=0.9, *.short=0.2, splice.latency=1:β(1us,α=2,β=8,9us)")
	f.Fuzz(func(t *testing.T, fam uint8, spec string) {
		family := Family(fam % uint8(familyCount))
		want, err := Build(Config{Seed: 3, Specs: map[Family]string{family: spec}})
		if err != nil {
			return
		}
		env := map[string]string{}
		for _, field := range splitShell(want.Repro()) {
			k, v, _ := strings.Cut(field, "=")
			env[k] = strings.TrimSuffix(strings.TrimPrefix(v, "'"), "'")
		}
		got, err := FromEnv(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
		if err != nil {
			t.Fatalf("repro %q: %v", want.Repro(), err)
		}
		if got.rates != want.rates {
			t.Fatalf("repro %q rebuilt different rates", want.Repro())
		}
		for k := range want.dists {
			if got.dists[k].Beta() != want.dists[k].Beta() {
				t.Fatalf("repro %q rebuilt a different %s distribution", want.Repro(), Kind(k))
			}
		}
	})
}

// TestDiskDecisionsIgnoreScheduling checks disk faults decide the same per
// file offset whatever order, or how concurrently, the reads happen.
func TestDiskDecisionsIgnoreScheduling(t *testing.T) {
	data := make([]byte, 100)
	outcome := func(i *Injector, order []int64, parallel bool) map[int64]bool {
		r := i.WrapReaderAt(context.Background(), SiteSendRead, 9, bytes.NewReader(data))
		got := make(map[int64]bool)
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, off := range order {
			read := func() {
				_, err := r.ReadAt(make([]byte, 1), off)
				mu.Lock()
				got[off] = err != nil
				mu.Unlock()
			}
			if parallel {
				wg.Go(read)
			} else {
				read()
			}
		}
		wg.Wait()
		return got
	}
	var forward, backward []int64
	for off := range int64(100) {
		forward = append(forward, off)
		backward = append(backward, 99-off)
	}
	spec := "read.eio=0.5"
	a := outcome(one(t, FamilyDisk, spec), forward, false)
	b := outcome(one(t, FamilyDisk, spec), backward, true)
	if !maps.Equal(a, b) {
		t.Fatal("the same seed faulted different offsets when reads ran in another order")
	}
}

// TestDiskKeysSeparate checks each site, file, and offset has its own
// attempt count at opens, reads, and writes, so calls for one never shift
// another's decisions. A key that left one out would let concurrent files
// or windows race for attempt numbers, and a seed would fault different
// files from run to run.
func TestDiskKeysSeparate(t *testing.T) {
	data := make([]byte, 2*writeBlock)
	read := func(i *Injector, site Site, file uint64, off int64) bool {
		_, err := i.WrapReaderAt(context.Background(), site, file, bytes.NewReader(data)).ReadAt(make([]byte, 1), off)
		return err != nil
	}
	write := func(i *Injector, file uint64, off int64) bool {
		_, err := i.WrapWriter(SiteOutputWrite, file, off, &memFile{}).Write(data[:writeBlock])
		return err != nil
	}
	type others map[string]func(*Injector)
	for _, tc := range []struct {
		name, spec string
		probe      func(*Injector) bool // the first site, file 1, offset 0
		others     others
	}{
		{"open", "open.eio=0.5", func(i *Injector) bool { return i.OpenErr(SiteSendOpen, 1, 0) != nil }, others{
			"another file":   func(i *Injector) { _ = i.OpenErr(SiteSendOpen, 2, 0) },
			"another site":   func(i *Injector) { _ = i.OpenErr(SiteCXSUMOpen, 1, 0) },
			"another offset": func(i *Injector) { _ = i.OpenErr(SiteSendOpen, 1, writeBlock) },
		}},
		{"read", "read.eio=0.5", func(i *Injector) bool { return read(i, SiteSendRead, 1, 0) }, others{
			"another file":   func(i *Injector) { read(i, SiteSendRead, 2, 0) },
			"another site":   func(i *Injector) { read(i, SiteCXSUMRead, 1, 0) },
			"another offset": func(i *Injector) { read(i, SiteSendRead, 1, 1) },
		}},
		// Output writes have one site.
		{"write", "write.eio=0.5", func(i *Injector) bool { return write(i, 1, 0) }, others{
			"another file":   func(i *Injector) { write(i, 2, 0) },
			"another window": func(i *Injector) { write(i, 1, writeBlock) },
		}},
	} {
		run := func(between func(*Injector)) []bool {
			i := one(t, FamilyDisk, tc.spec)
			var got []bool
			for range 32 {
				got = append(got, tc.probe(i))
				if between != nil {
					between(i)
				}
			}
			return got
		}
		alone := run(nil)
		for name, other := range tc.others {
			if got := run(other); !slices.Equal(got, alone) {
				t.Fatalf("%ss of file 1 changed when %s was used between them", tc.name, name)
			}
		}
	}
}

// TestDocExamplesBuild checks every family spec and concrete distribution
// the docs show builds, so a reader who copies one gets a running tx.
func TestDocExamplesBuild(t *testing.T) {
	envSpec := regexp.MustCompile(`(TX_FAULTS_[A-Z]+)='([^']*)'`)
	// A concrete distribution starts with a number, unlike a template such
	// as β(min,avg,p99,max) or prose naming a spelling.
	dist := regexp.MustCompile(`(?:β|Β|beta|Beta)\(\d[^()]*\)`)
	var specs, dists int
	for _, doc := range []string{"../../docs/arch/FAULTS.md", "../../docs/pub/CLI.md"} {
		text, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range envSpec.FindAllStringSubmatch(string(text), -1) {
			f := slices.Index(familyEnv[:], m[1])
			if f < 0 {
				t.Fatalf("%s: %s is not a family variable", doc, m[1])
			}
			if _, err := Build(Config{Specs: map[Family]string{Family(f): m[2]}}); err != nil {
				t.Errorf("%s: %s='%s': %v", doc, m[1], m[2], err)
			}
			specs++
		}
		for _, d := range dist.FindAllString(string(text), -1) {
			if _, err := ParseDist(d); err != nil {
				t.Errorf("%s: %s: %v", doc, d, err)
			}
			dists++
		}
	}
	if specs == 0 || dists == 0 {
		t.Fatalf("found %d specs and %d distributions; the patterns no longer match the docs", specs, dists)
	}
}
