package ftcp

import (
	"net"

	"github.com/jolynch/tx/internal/sysio"
)

// tcpByteCounts returns the bytes a TCP connection has received and sent,
// from TCP_INFO. It returns zeros for other transports.
func tcpByteCounts(conn net.Conn) (received, sent int64) {
	tc := rawTCPConn(conn)
	if tc == nil {
		return 0, 0
	}
	return sysio.TCPByteCounts(tc)
}
