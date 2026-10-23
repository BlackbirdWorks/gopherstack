package sgfilter_test

import (
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

func pfx(s string) netip.Prefix { return netip.MustParsePrefix(s) }

func tcpFlow(port int, addr string) sgfilter.Flow {
	return protoFlow("tcp", port, addr)
}

func protoFlow(proto string, port int, addr string) sgfilter.Flow {
	return sgfilter.Flow{Protocol: proto, Port: port, Peer: sgfilter.Peer{Addr: netip.MustParseAddr(addr)}}
}

func groupFlow(port int, groups ...string) sgfilter.Flow {
	return sgfilter.Flow{Protocol: "tcp", Port: port, Peer: sgfilter.Peer{Groups: groups}}
}

func grp(rules ...sgfilter.Rule) sgfilter.Group { return sgfilter.Group{Ingress: rules} }

func TestAllowIngress(t *testing.T) {
	t.Parallel()

	tcp := sgfilter.Rule{Protocol: "tcp", FromPort: 5432, ToPort: 5440, CIDRs: []netip.Prefix{pfx("10.0.0.0/8")}}
	v6 := sgfilter.Rule{Protocol: "tcp", FromPort: 80, ToPort: 80, CIDRs: []netip.Prefix{pfx("2001:db8::/32")}}
	all := sgfilter.Rule{Protocol: "-1", CIDRs: []netip.Prefix{pfx("192.168.1.5/32")}}
	udp := sgfilter.Rule{Protocol: "17", FromPort: 53, ToPort: 53, CIDRs: []netip.Prefix{pfx("0.0.0.0/0")}}
	ref := sgfilter.Rule{Protocol: "tcp", FromPort: 22, ToPort: 22, Groups: []string{"sg-b"}}

	tests := []struct {
		name   string
		groups []sgfilter.Group
		flow   sgfilter.Flow
		want   bool
	}{
		{"no groups denies", nil, tcpFlow(80, "10.1.1.1"), false},
		{"empty ingress denies", []sgfilter.Group{{ID: "sg-a"}}, tcpFlow(80, "10.1.1.1"), false},
		{"cidr v4 in range", []sgfilter.Group{grp(tcp)}, tcpFlow(5432, "10.9.9.9"), true},
		{"port range upper bound", []sgfilter.Group{grp(tcp)}, tcpFlow(5440, "10.9.9.9"), true},
		{"port above range", []sgfilter.Group{grp(tcp)}, tcpFlow(5441, "10.9.9.9"), false},
		{"cidr miss", []sgfilter.Group{grp(tcp)}, tcpFlow(5432, "11.0.0.1"), false},
		{"protocol mismatch", []sgfilter.Group{grp(tcp)}, protoFlow("udp", 5432, "10.9.9.9"), false},
		{"cidr v6", []sgfilter.Group{grp(v6)}, tcpFlow(80, "2001:db8::1"), true},
		{"v4 mapped v6", []sgfilter.Group{grp(tcp)}, tcpFlow(5432, "::ffff:10.1.1.1"), true},
		{"v6 vs v4 cidr", []sgfilter.Group{grp(tcp)}, tcpFlow(5432, "2001:db8::1"), false},
		{"protocol -1 any port", []sgfilter.Group{grp(all)}, protoFlow("udp", 9999, "192.168.1.5"), true},
		{"protocol -1 wrong peer", []sgfilter.Group{grp(all)}, tcpFlow(1, "192.168.1.6"), false},
		{"udp by number", []sgfilter.Group{grp(udp)}, protoFlow("udp", 53, "8.8.8.8"), true},
		{"sg reference match", []sgfilter.Group{grp(ref)}, groupFlow(22, "sg-x", "sg-b"), true},
		{"sg reference miss", []sgfilter.Group{grp(ref)}, groupFlow(22, "sg-x"), false},
		{"any group suffices", []sgfilter.Group{grp(ref), grp(tcp)}, tcpFlow(5433, "10.0.0.1"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, sgfilter.AllowIngress(tt.groups, tt.flow))
		})
	}
}

func TestAllowEgress(t *testing.T) {
	t.Parallel()

	world := []netip.Prefix{pfx("0.0.0.0/0")}
	open := sgfilter.Group{Egress: []sgfilter.Rule{{Protocol: "-1", CIDRs: world}}}
	web := sgfilter.Group{Egress: []sgfilter.Rule{{Protocol: "tcp", FromPort: 443, ToPort: 443, CIDRs: world}}}

	tests := []struct {
		name   string
		groups []sgfilter.Group
		flow   sgfilter.Flow
		want   bool
	}{
		{"default egress allows all", []sgfilter.Group{open}, tcpFlow(25, "1.2.3.4"), true},
		{"revoked egress denies", []sgfilter.Group{{}}, tcpFlow(25, "1.2.3.4"), false},
		{"restricted allows listed port", []sgfilter.Group{web}, tcpFlow(443, "1.2.3.4"), true},
		{"restricted denies other port", []sgfilter.Group{web}, tcpFlow(80, "1.2.3.4"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, sgfilter.AllowEgress(tt.groups, tt.flow))
		})
	}
}

func TestNormalizeProtocolAndAddr(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "tcp", sgfilter.NormalizeProtocol("TCP"))
	assert.Equal(t, "tcp", sgfilter.NormalizeProtocol("6"))
	assert.Equal(t, "icmp", sgfilter.NormalizeProtocol("58"))
	assert.Equal(t, "-1", sgfilter.NormalizeProtocol("-1"))
	assert.Equal(t, netip.MustParseAddr("127.0.0.1"), sgfilter.AddrOf(&net.TCPAddr{IP: net.ParseIP("127.0.0.1")}))
	assert.False(t, sgfilter.AddrOf(nil).IsValid())
}
