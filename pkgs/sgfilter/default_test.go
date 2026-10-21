package sgfilter_test

import (
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter/sgtest"
)

func TestAdmitIngressDefaultGroup(t *testing.T) {
	t.Parallel()

	self := sgfilter.Group{ID: "sg-def", Ingress: []sgfilter.Rule{
		{Protocol: sgfilter.ProtoAll, Groups: []string{"sg-def"}},
	}}
	member := netip.MustParseAddr("10.0.0.5")
	stranger := netip.MustParseAddr("10.0.0.6")

	tests := []struct {
		client    netip.Addr
		name      string
		defaultID string
		ids       []string
		want      bool
	}{
		{name: "member of default admitted", ids: nil, client: member, defaultID: "sg-def", want: true},
		{name: "non member denied", ids: nil, client: stranger, defaultID: "sg-def", want: false},
		{name: "no default group denies", ids: nil, client: member, defaultID: "", want: false},
		{
			name:      "explicit group wins over default",
			ids:       []string{"sg-other"},
			client:    member,
			defaultID: "sg-def",
			want:      false,
		},
		{name: "explicit default group", ids: []string{"sg-def"}, client: member, defaultID: "sg-def", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := sgtest.New(self, sgfilter.Group{ID: "sg-other"})
			src.DefaultID = tt.defaultID
			src.SetMembers(map[netip.Addr][]string{member: {"sg-def"}})

			remote := &net.TCPAddr{IP: tt.client.AsSlice(), Port: 1}

			assert.Equal(t, tt.want, src.Enforcer().AdmitIngress(tt.ids, "tcp", 5432, remote))
		})
	}
}

func TestAdmitIngressPlainSourceDeniesEmptyGroups(t *testing.T) {
	t.Parallel()

	src := &fakeSource{byAddr: map[netip.Addr][]string{}}
	src.groups.Store(&[]sgfilter.Group{})

	enf := sgfilter.New()
	enf.SetSource(src)

	assert.False(t, enf.AdmitIngress(nil, "tcp", 80, sgtest.Loopback()))
}

func TestGuardDropsRejectedClients(t *testing.T) {
	t.Parallel()

	tests := []struct {
		admit sgfilter.Admitter
		name  string
		want  bool
	}{
		{name: "nil admit passes", want: true},
		{name: "admit passes", admit: func(net.Addr) bool { return true }, want: true},
		{name: "reject drops", admit: func(net.Addr) bool { return false }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)

			g := sgfilter.Guard(ln, tt.admit)
			t.Cleanup(func() { _ = g.Close() })

			got := make(chan bool, 1)

			go func() {
				c, aerr := g.Accept()
				if aerr == nil {
					_ = c.Close()
				}

				got <- aerr == nil
			}()

			c, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", ln.Addr().String())
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Close() })

			if !tt.want {
				buf := make([]byte, 1)
				_, rerr := c.Read(buf)
				require.Error(t, rerr)
				_ = c.Close()
				_ = g.Close()
			}

			assert.Equal(t, tt.want, <-got)
		})
	}
}
