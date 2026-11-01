package httpclient

import (
	"net"
	"syscall"
	"time"
)

const dialerKeepAlive = 30 * time.Second

// LoopbackDialer returns a dialer that refuses non-loopback peers unless allow reports true. The check
// runs on the resolved peer address, so a hostname that resolves elsewhere (or later rebinds) is refused.
func LoopbackDialer(allow func() bool, denied error) *net.Dialer {
	return &net.Dialer{
		KeepAlive: dialerKeepAlive,
		Control: func(_, address string, _ syscall.RawConn) error {
			if allow() {
				return nil
			}

			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}

			if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
				return denied
			}

			return nil
		},
	}
}
