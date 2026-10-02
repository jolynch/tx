package main

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/filexfer/ftcp"
)

func TestParsePercentFlag(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{name: "default", raw: "25%", want: 25},
		{name: "min", raw: "1%", want: 1},
		{name: "max", raw: "100%", want: 100},
		{name: "missing percent", raw: "25", wantErr: true},
		{name: "decimal", raw: "25.0%", wantErr: true},
		{name: "zero", raw: "0%", wantErr: true},
		{name: "too large", raw: "101%", wantErr: true},
		{name: "negative", raw: "-1%", wantErr: true},
		{name: "empty", raw: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePercentFlag(tc.raw, "--gentle-cpu")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("unexpected value: got=%d want=%d", got, tc.want)
			}
		})
	}
}

func TestRunSendCLI(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantCode int
	}{
		{name: "bad listen address", args: []string{"tree", "--listen", "bogus"}, wantCode: 2},
		{name: "tree help", args: []string{"tree", "--help"}, wantCode: 0},
		{name: "unknown command", args: []string{"bogus"}, wantCode: 2},
		{name: "no args", args: nil, wantCode: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := runSendCLI(tc.args, &stdout, &stderr); got != tc.wantCode {
				t.Fatalf("runSendCLI(%v) = %d, want %d (stderr=%q)", tc.args, got, tc.wantCode, stderr.String())
			}
		})
	}
}

func TestRunSendTreeRequireAuth(t *testing.T) {
	root := t.TempDir()
	keys := filepath.Join(root, "keys")
	if err := os.Mkdir(keys, 0o700); err != nil {
		t.Fatalf("create keys directory: %v", err)
	}
	called := false
	serve := func(listener net.Listener, opts ftcp.ServerOptions) error {
		called = true
		if !opts.RequireAuth || len(opts.AllowedAuthTokens) != 1 {
			t.Fatalf("server auth options: required=%v tokens=%d", opts.RequireAuth, len(opts.AllowedAuthTokens))
		}
		if err := aead.ValidateAuthToken(opts.AllowedAuthTokens[0]); err != nil {
			t.Fatalf("invalid generated token: %v", err)
		}
		if opts.ServerIdentity == nil || opts.RootDir != root {
			t.Fatalf("server identity/root: identity=%v root=%q", opts.ServerIdentity, opts.RootDir)
		}
		if _, err := os.Stat(filepath.Join(keys, "key")); err != nil {
			t.Fatalf("server did not persist its age identity: %v", err)
		}

		finished := make(chan error, 1)
		go func() { finished <- ftcp.Serve(listener, opts) }()
		defer func() {
			_ = listener.Close()
			if err := <-finished; err != nil {
				t.Errorf("serve: %v", err)
			}
		}()
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatalf("dial server: %v", err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if _, err := io.WriteString(conn, "STATUS\r\n"); err != nil {
			t.Fatalf("write STATUS: %v", err)
		}
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			t.Fatalf("read STATUS response: %v", err)
		}
		if !strings.HasPrefix(line, "ERR NOT_AUTHORIZED ") {
			t.Fatalf("unauthenticated STATUS response = %q", line)
		}
		return nil
	}
	if got := runSendTree([]string{"--listen", "127.0.0.1:0", "--keys", keys, "--require-auth", root}, io.Discard, serve); got != 0 {
		t.Fatalf("runSendTree exit code = %d", got)
	}
	if !called {
		t.Fatal("server was not called")
	}
}

func TestRunSendTreeExitWithStdin(t *testing.T) {
	root := t.TempDir()
	statsPath := filepath.Join(t.TempDir(), "stats.jsonl")
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pr.Close()
	sendTreeStdin = pr
	t.Cleanup(func() { sendTreeStdin = os.Stdin })
	serve := func(ln net.Listener, _ ftcp.ServerOptions) error {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return nil // the listener closed: shutdown began
			}
			conn.Close()
		}
	}
	done := make(chan int, 1)
	go func() {
		done <- runSendTree([]string{"--listen", "127.0.0.1:0", "--keys", t.TempDir(), "--exit-after", "never",
			"--exit-with", "stdin", "--stats", statsPath, root}, io.Discard, serve)
	}()
	select {
	case code := <-done:
		t.Fatalf("exited before stdin closed: %d", code)
	case <-time.After(200 * time.Millisecond):
	}
	pw.Close()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit code %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("still running after stdin closed")
	}
	data, err := os.ReadFile(statsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"exit":"exit-with:stdin"`) {
		t.Fatalf("stats lack the exit-with:stdin exit:\n%s", data)
	}
}

func TestRunSendTreeExitWithValidation(t *testing.T) {
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer devNull.Close()
	sendTreeStdin = devNull
	t.Cleanup(func() { sendTreeStdin = os.Stdin })
	served := false
	serve := func(net.Listener, ftcp.ServerOptions) error { served = true; return nil }
	run := func(args ...string) (int, string) {
		var stderr bytes.Buffer
		code := runSendTree(append([]string{"--listen", "127.0.0.1:0", "--keys", t.TempDir()}, append(args, t.TempDir())...), &stderr, serve)
		return code, stderr.String()
	}
	// stdin on /dev/null would mean an instant exit: refused up front.
	if code, out := run("--exit-with", "stdin"); code != 2 || !strings.Contains(out, "pipe or socket") || served {
		t.Fatalf("stdin=/dev/null: code %d served %v: %s", code, served, out)
	}
	if code, out := run("--exit-with", "pid:1"); code != 2 || !strings.Contains(out, "supported: none, stdin") {
		t.Fatalf("unknown mode: code %d: %s", code, out)
	}
	// The default never looks at stdin, so /dev/null is fine.
	if code, out := run(); code != 0 || !served {
		t.Fatalf("default: code %d served %v: %s", code, served, out)
	}
}
