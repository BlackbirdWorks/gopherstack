package dnstest_test

import (
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/dns/dnstest"
)

func TestStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		holdFirst  bool
		minPickups int
	}{
		{name: "free_port", minPickups: 1},
		{name: "first_tcp_port_taken", holdFirst: true, minPickups: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var lc net.ListenConfig

			pickups := 0

			pick := func(t *testing.T) (string, error) {
				t.Helper()

				pickups++

				if tt.holdFirst && pickups == 1 {
					l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
					if err != nil {
						return "", err
					}

					t.Cleanup(func() { _ = l.Close() })

					return l.Addr().String(), nil
				}

				return dnstest.FreeUDPAddr(t)
			}

			srv, addr, _ := dnstest.StartWith(t, gopherDNS.Config{ResolveIP: "127.0.0.1"}, pick)
			// Another process can steal a later free port, so only the injected
			// collision is exact: at least one retry per held port.
			assert.GreaterOrEqual(t, pickups, tt.minPickups)

			srv.RegisterRecord("x.test", "A", []string{"1.2.3.4"})

			m := new(dns.Msg)
			m.SetQuestion("x.test.", dns.TypeA)

			c := &dns.Client{Net: "udp", Timeout: 2 * time.Second}
			r, _, err := c.Exchange(m, addr)
			require.NoError(t, err)
			require.Len(t, r.Answer, 1)
		})
	}
}
