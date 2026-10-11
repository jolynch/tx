package ftcp

import (
	"context"
	"io"
)

// handleAUTHCommand rejects AUTH anywhere but a connection's first line,
// where runAuth handles discovery and the Noise handshake.
func handleAUTHCommand(context.Context, Request, io.Writer, Deps) error {
	return protocolErr{code: "BAD_COMMAND", message: "AUTH must be first"}
}
