package ociloadbalancer

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"math"
	"net"
	"net/netip"
	"slices"
	"strconv"
)

const (
	proxyV1            = 1
	proxyV2            = 2
	optAuthority       = "PP2_TYPE_AUTHORITY"
	ppv2CmdProxy       = 0x21
	ppv2FamTCP4        = 0x11
	ppv2FamTCP6        = 0x21
	ppv2TypeAuthority  = 0x02
	ppv2TLVHeaderBytes = 3
)

// ppv2Signature opens every PROXY protocol v2 header (haproxy proxy-protocol spec, section 2.2).
var ppv2Signature = []byte("\r\n\r\n\x00\r\nQUIT\n") //nolint:gochecknoglobals // immutable

// proxyHeader renders the PROXY protocol header for a client connection (haproxy proxy-protocol spec, sections 2.1
// and 2.2). authority is carried as PP2_TYPE_AUTHORITY when non-empty.
func proxyHeader(version int, src, dst netip.AddrPort, authority string) []byte {
	if version == proxyV1 {
		return proxyV1Header(src, dst)
	}

	return proxyV2Header(src, dst, authority)
}

func proxyV1Header(src, dst netip.AddrPort) []byte {
	fam := "TCP4"
	if src.Addr().Is6() {
		fam = "TCP6"
	}

	return []byte("PROXY " + fam + " " + src.Addr().String() + " " + dst.Addr().String() + " " +
		strconv.Itoa(int(src.Port())) + " " + strconv.Itoa(int(dst.Port())) + "\r\n")
}

func proxyV2Header(src, dst netip.AddrPort, authority string) []byte {
	fam := byte(ppv2FamTCP4)

	var addrs []byte

	if src.Addr().Is6() {
		fam = ppv2FamTCP6
		s, d := src.Addr().As16(), dst.Addr().As16()
		addrs = append(s[:], d[:]...)
	} else {
		s, d := src.Addr().As4(), dst.Addr().As4()
		addrs = append(s[:], d[:]...)
	}

	addrs = binary.BigEndian.AppendUint16(addrs, src.Port())
	addrs = binary.BigEndian.AppendUint16(addrs, dst.Port())

	if authority != "" {
		addrs = append(addrs, ppv2TypeAuthority)
		addrs = binary.BigEndian.AppendUint16(addrs, uint16(len(authority)&math.MaxUint16))
		addrs = append(addrs, authority...)
	}

	out := append([]byte{}, ppv2Signature...)
	out = append(out, ppv2CmdProxy, fam)
	out = binary.BigEndian.AppendUint16(out, uint16(len(addrs)&math.MaxUint16))

	return append(out, addrs...)
}

// proxyPreface builds the header for the connection c, or nil when the listener does not enable it.
func proxyPreface(ctx context.Context, cc *ConnectionConfig, c net.Conn) []byte {
	if cc == nil || cc.BackendTCPProxyProtocolVersion == nil {
		return nil
	}

	src, ok1 := addrPort(c.RemoteAddr())
	dst, ok2 := addrPort(c.LocalAddr())

	if !ok1 || !ok2 {
		return nil
	}

	authority := ""

	if tc, ok := c.(*tls.Conn); ok && *cc.BackendTCPProxyProtocolVersion == proxyV2 && hasOption(cc, optAuthority) {
		_ = tc.HandshakeContext(ctx)
		authority = tc.ConnectionState().ServerName
	}

	return proxyHeader(*cc.BackendTCPProxyProtocolVersion, src, dst, authority)
}

func hasOption(cc *ConnectionConfig, opt string) bool {
	return slices.Contains(cc.BackendTCPProxyProtocolOptions, opt)
}

func addrPort(a net.Addr) (netip.AddrPort, bool) {
	ta, ok := a.(*net.TCPAddr)
	if !ok {
		return netip.AddrPort{}, false
	}

	ap := ta.AddrPort()

	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()), true
}

func checkProxyProtocol(cc *ConnectionConfig) *APIError {
	if cc.BackendTCPProxyProtocolVersion == nil {
		if len(cc.BackendTCPProxyProtocolOptions) > 0 {
			return errParam("backendTcpProxyProtocolOptions needs backendTcpProxyProtocolVersion 2")
		}

		return nil
	}

	v := *cc.BackendTCPProxyProtocolVersion
	if v != proxyV1 && v != proxyV2 {
		return errParam("backendTcpProxyProtocolVersion must be 1 or 2")
	}

	for _, o := range cc.BackendTCPProxyProtocolOptions {
		if o != optAuthority {
			return errParam("backendTcpProxyProtocolOptions supports only " + optAuthority)
		}

		if v != proxyV2 {
			return errParam(optAuthority + " needs backendTcpProxyProtocolVersion 2")
		}
	}

	return nil
}
