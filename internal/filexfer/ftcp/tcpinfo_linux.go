package ftcp

import (
	"net"

	"golang.org/x/sys/unix"
)

// tcpByteCounts returns the bytes a TCP connection has received and sent,
// from TCP_INFO. It returns zeros for other transports.
func tcpByteCounts(conn net.Conn) (received, sent int64) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return 0, 0
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return 0, 0
	}
	_ = raw.Control(func(fd uintptr) {
		info, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
		if err == nil {
			received, sent = int64(info.Bytes_received), int64(info.Bytes_sent)
		}
	})
	return received, sent
}
