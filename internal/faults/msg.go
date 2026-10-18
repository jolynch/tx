package faults

import (
	"context"
	"net"
	"os"
	"syscall"
)

// Request-level faults (MSG) and the connection-setup faults of NET. Their
// operations are numbered in the order they happen, so under concurrency a
// seed fixes which request ordinal faults, not which request.

// LoseResponse reports whether the server should drop the response to a
// request it has applied, resetting the connection instead of replying.
func (i *Injector) LoseResponse() bool {
	return i != nil && i.famOn[FamilyMsg] && i.fires(MsgLoseResponse, 0, i.loseOps.Add(1))
}

// DupRequest reports whether the client should send a request twice, so
// the server sees it once more than intended.
func (i *Injector) DupRequest() bool {
	return i != nil && i.famOn[FamilyMsg] && i.fires(MsgDup, 1, i.dupOps.Add(1))
}

// DelayRequest holds a request back before the client sends it, so
// requests on other connections can overtake it.
func (i *Injector) DelayRequest() {
	if i == nil || !i.famOn[FamilyMsg] {
		return
	}
	if op := i.delayOps.Add(1); i.fires(MsgDelay, 2, op) {
		_ = sleep(nil, i.sample(MsgDelay, 2, op), noDeadline, nil)
	}
}

// Busy reports whether the server should refuse an accepted connection as
// busy.
func (i *Injector) Busy() bool {
	return i != nil && i.famOn[FamilyMsg] && i.fires(MsgBusy, 3, i.busyOps.Add(1))
}

// AcceptErr returns the error the server's accept loop should handle in
// place of the connection it just accepted, or nil to serve it. The error is
// a net.Error whose Temporary reports true, so an accept loop retries it as
// it would any transient accept failure.
func (i *Injector) AcceptErr() error {
	if i == nil || !i.famOn[FamilyNet] || !i.fires(NetAcceptFail, 4, i.accepts.Add(1)) {
		return nil
	}
	return &net.OpError{Op: "accept", Net: "tcp", Err: syscall.ECONNABORTED}
}

// DialErr is the gate in front of a client dial. It may wait for a slow
// dial, returning early with ctx's error, and returns the error to report
// instead of dialing, or nil to dial.
func (i *Injector) DialErr(ctx context.Context) error {
	if i == nil || !i.famOn[FamilyNet] {
		return nil
	}
	op := i.dials.Add(1)
	if i.fires(NetDialSlow, 5, op) {
		if err := sleep(ctx, i.sample(NetDialSlow, 5, op), noDeadline, nil); err != nil {
			return err
		}
	}
	if i.fires(NetDialFail, 5, op) {
		return &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	}
	return nil
}
