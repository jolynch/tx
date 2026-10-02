//go:build !linux

package ftcp

import "net"

func tcpByteCounts(net.Conn) (received, sent int64) { return 0, 0 }
