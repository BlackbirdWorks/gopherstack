package ociapigateway

import (
	"net"
	"net/http"
	"net/netip"
)

// gatewayPort is the port a real gateway listens on; reachability is evaluated against it.
const gatewayPort = 443

// Reachability is how the network sees a connection from a client address to the gateway VNIC.
type Reachability int

const (
	// ReachUnknown means the source address belongs to no VNIC of the emulator.
	ReachUnknown Reachability = iota
	// ReachForeign means the source VNIC is in another VCN.
	ReachForeign
	// ReachAllowed means the security lists and NSGs permit the connection.
	ReachAllowed
	// ReachDenied means they do not.
	ReachDenied
)

func peerAddr(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	ip, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}

	return ip.Unmap()
}

// peerAllowed decides whether the caller can reach the gateway. A PRIVATE gateway has no public address, so
// only loopback and private-range peers get through; unmanaged peers reach a PUBLIC gateway as they would
// from the internet.
func peerAllowed(r *http.Request, tgt target) bool {
	ip := peerAddr(r)

	if tgt.reach != nil && tgt.vnic != "" && ip.IsValid() {
		switch tgt.reach(ip.String(), tgt.vnic, gatewayPort) {
		case ReachAllowed:
			return true
		case ReachDenied:
			return false
		case ReachUnknown, ReachForeign:
		}
	}

	if !tgt.private {
		return true
	}

	return ip.IsValid() && (ip.IsLoopback() || ip.IsPrivate())
}

// dropConnection ends the connection without an answer, as an unreachable address does.
func dropConnection(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		writeGWError(w, http.StatusGatewayTimeout, "Gateway Timeout")

		return
	}

	_ = conn.Close()
}
