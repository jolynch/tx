package ftcp

import (
	"bytes"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

func TestServeLogsExitAfterConfiguration(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var logBuf bytes.Buffer
	origWriter := log.Writer()
	origFlags := log.Flags()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(origWriter)
		log.SetFlags(origFlags)
	}()

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = ln.Close()
	}()

	if err := Serve(ln, ServerOptions{ExitAfter: 5 * time.Second}); err != nil {
		t.Fatalf("Serve returned error: %v", err)
	}

	if got := logBuf.String(); !strings.Contains(got, "exit-after: server will exit 5s after the last activity") {
		t.Fatalf("expected exit-after startup log, got %q", got)
	}
}
