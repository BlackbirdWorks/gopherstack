package dns_test

import (
	"context"
	"net"
	"strconv"
	"testing"

	mdns "github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
)

func TestServer_StopWaitsForServeGoroutines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, s *gopherDNS.Server)
		name string
	}{
		{
			name: "start_stop",
			run: func(t *testing.T, s *gopherDNS.Server) {
				t.Helper()
				require.NoError(t, s.Start(t.Context()))
				require.NoError(t, s.Stop())
			},
		},
		{
			name: "stop_twice",
			run: func(t *testing.T, s *gopherDNS.Server) {
				t.Helper()
				require.NoError(t, s.Start(t.Context()))
				require.NoError(t, s.Stop())
				require.NoError(t, s.Stop())
			},
		},
		{
			name: "stop_without_start",
			run: func(t *testing.T, s *gopherDNS.Server) {
				t.Helper()
				require.NoError(t, s.Stop())
			},
		},
		{
			name: "ctx_cancelled_before_start",
			run: func(t *testing.T, s *gopherDNS.Server) {
				t.Helper()
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				require.Error(t, s.Start(ctx))
				require.NoError(t, s.Stop())
			},
		},
		{
			name: "ctx_cancel_after_start",
			run: func(t *testing.T, s *gopherDNS.Server) {
				t.Helper()
				ctx, cancel := context.WithCancel(t.Context())
				require.NoError(t, s.Start(ctx))
				cancel()
				require.NoError(t, s.Stop())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, err := gopherDNS.New(gopherDNS.Config{ListenAddr: "127.0.0.1:0"})
			require.NoError(t, err)

			tt.run(t, s)
		})
	}
}

func TestServer_StartFailureStopsOtherServer(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	s, err := gopherDNS.New(gopherDNS.Config{ListenAddr: l.Addr().String()})
	require.NoError(t, err)

	require.Error(t, s.Start(t.Context()))
	require.NoError(t, s.Stop())
}

func TestServer_EphemeralPort(t *testing.T) {
	t.Parallel()

	s, err := gopherDNS.New(gopherDNS.Config{ListenAddr: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))
	t.Cleanup(func() { _ = s.Stop() })
	require.NotZero(t, s.Port())

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(s.Port()))

	for _, network := range []string{"tcp", "udp"} {
		m := new(mdns.Msg)
		m.SetQuestion("example.test.", mdns.TypeA)

		_, _, err = (&mdns.Client{Net: network}).ExchangeContext(t.Context(), m, addr)
		require.NoError(t, err, network)
	}
}
