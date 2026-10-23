package sgfilter_test

import (
	"context"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

type fakeSource struct {
	groups atomic.Pointer[[]sgfilter.Group]
	byAddr map[netip.Addr][]string
}

func (s *fakeSource) SecurityGroups(ids []string) []sgfilter.Group {
	var out []sgfilter.Group

	for _, g := range *s.groups.Load() {
		for _, id := range ids {
			if g.ID == id {
				out = append(out, g)
			}
		}
	}

	return out
}

func (s *fakeSource) GroupsOfAddr(a netip.Addr) []string { return s.byAddr[a] }

type echoDialer struct{ dialed atomic.Int32 }

func (d *echoDialer) DialContext(_ context.Context, _, _ string) (net.Conn, error) {
	d.dialed.Add(1)

	c, s := net.Pipe()

	go func() {
		defer func() { _ = s.Close() }()

		_, _ = io.Copy(s, s)
	}()

	return c, nil
}

func startProxy(t *testing.T, admit sgfilter.Admitter, d sgfilter.Dialer) string {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := sgfilter.NewProxy(ln, "upstream:1", admit, d)
	p.Start()
	t.Cleanup(p.Close)

	return p.Addr().String()
}

func roundTrip(t *testing.T, addr string) bool {
	t.Helper()

	c, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)

	defer func() { _ = c.Close() }()

	if _, err = c.Write([]byte("ping")); err != nil {
		return false
	}

	buf := make([]byte, 4)
	_, err = io.ReadFull(c, buf)

	return err == nil && string(buf) == "ping"
}

func TestProxyAdmission(t *testing.T) {
	t.Parallel()

	allowLoop := []sgfilter.Group{{ID: "sg-1", Ingress: []sgfilter.Rule{
		{Protocol: "tcp", FromPort: 5432, ToPort: 5432, CIDRs: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}}}}
	allowOther := []sgfilter.Group{{ID: "sg-1", Ingress: []sgfilter.Rule{
		{
			Protocol: "tcp",
			FromPort: 5432,
			ToPort:   5432,
			CIDRs:    []netip.Prefix{netip.MustParsePrefix("203.0.113.0/24")},
		},
	}}}
	wrongPort := []sgfilter.Group{{ID: "sg-1", Ingress: []sgfilter.Rule{
		{Protocol: "tcp", FromPort: 3306, ToPort: 3306, CIDRs: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}}}}

	tests := []struct {
		name   string
		groups []sgfilter.Group
		ids    []string
		want   bool
	}{
		{"allowed cidr", allowLoop, []string{"sg-1"}, true},
		{"foreign cidr refused", allowOther, []string{"sg-1"}, false},
		{"wrong port refused", wrongPort, []string{"sg-1"}, false},
		{"no groups refused", allowLoop, nil, false},
		{"unknown group refused", allowLoop, []string{"sg-zzz"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := &fakeSource{}
			src.groups.Store(&tt.groups)

			enf := sgfilter.New()
			enf.SetSource(src)

			d := &echoDialer{}
			addr := startProxy(t, enf.Ingress(func() []string { return tt.ids }, "tcp", 5432), d)

			assert.Equal(t, tt.want, roundTrip(t, addr))
			assert.Equal(t, tt.want, d.dialed.Load() == 1)
		})
	}
}

func TestProxyLiveRuleUpdate(t *testing.T) {
	t.Parallel()

	src := &fakeSource{}
	closed := []sgfilter.Group{{ID: "sg-1"}}
	src.groups.Store(&closed)

	enf := sgfilter.New()
	enf.SetSource(src)

	addr := startProxy(t, enf.Ingress(func() []string { return []string{"sg-1"} }, "tcp", 5432), &echoDialer{})
	require.False(t, roundTrip(t, addr))

	opened := []sgfilter.Group{{ID: "sg-1", Ingress: []sgfilter.Rule{
		{Protocol: "-1", CIDRs: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}}}}
	src.groups.Store(&opened)
	assert.True(t, roundTrip(t, addr))

	src.groups.Store(&closed)
	assert.False(t, roundTrip(t, addr))
}

func TestEnforcerSourceGroupPeer(t *testing.T) {
	t.Parallel()

	peer := netip.MustParseAddr("10.0.0.7")
	src := &fakeSource{byAddr: map[netip.Addr][]string{peer: {"sg-app"}}}
	gs := []sgfilter.Group{
		{
			ID:      "sg-db",
			Ingress: []sgfilter.Rule{{Protocol: "tcp", FromPort: 5432, ToPort: 5432, Groups: []string{"sg-app"}}},
		},
	}
	src.groups.Store(&gs)

	enf := sgfilter.New()
	enf.SetSource(src)

	assert.True(t, enf.AdmitIngress([]string{"sg-db"}, "tcp", 5432, &net.TCPAddr{IP: net.ParseIP("10.0.0.7")}))
	assert.False(t, enf.AdmitIngress([]string{"sg-db"}, "tcp", 5432, &net.TCPAddr{IP: net.ParseIP("10.0.0.8")}))
}

func TestNilEnforcerAdmits(t *testing.T) {
	t.Parallel()

	var enf *sgfilter.Enforcer

	assert.True(t, enf.AdmitIngress(nil, "tcp", 1, nil))
	assert.Nil(t, enf.Ingress(nil, "tcp", 1))
	enf.SetSource(nil)
}

func TestNoSourceDenies(t *testing.T) {
	t.Parallel()

	assert.False(t, sgfilter.New().AdmitIngress([]string{"sg-1"}, "tcp", 1, &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}))
}
