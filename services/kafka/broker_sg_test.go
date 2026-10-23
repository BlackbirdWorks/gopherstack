package kafka_test

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter/sgtest"
	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func serveEchoAt(t *testing.T, spec container.Spec) {
	t.Helper()

	ip, hostPort, _, err := container.ParsePortSpec(spec.Ports[0])
	require.NoError(t, err)

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", net.JoinHostPort(ip.String(), hostPort))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			go func() {
				defer func() { _ = c.Close() }()

				_, _ = io.Copy(c, c)
			}()
		}
	}()
}

func echoRoundTrip(t *testing.T, addr string) bool {
	t.Helper()

	conn, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		return false
	}

	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = conn.Write([]byte("x"))

	buf := make([]byte, 1)
	n, _ := conn.Read(buf)

	return n == 1
}

func TestBrokerSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enforce bool
	}{
		{"enforced relays the plaintext listener", true},
		{"flag off publishes the listener itself", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := sgtest.New(sgtest.OpenTo("sg-1", 9092, "203.0.113.0/24"))
			f := newBrokerFixture(t, &fakeBrokerRuntime{}, 0)
			f.ready.Store(true)

			if tt.enforce {
				f.backend.SetSecurityGroupEnforcer(src.Enforcer())
			}

			c, err := f.backend.CreateCluster(t.Context(), "sg", "3.9.1", 1,
				kafka.BrokerNodeGroupInfo{SecurityGroups: []string{"sg-1"}}, nil, nil)
			require.NoError(t, err)

			f.awaitState(t, c.ClusterArn, kafka.ClusterStateActive)

			servers, _ := f.backend.ManagedBootstrap(c.ClusterArn)
			require.Len(t, servers, 1)

			spec := f.rt.startedSpecs()[0]
			_, published, _, err := container.ParsePortSpec(spec.Ports[0])
			require.NoError(t, err)

			_, bootPort, err := net.SplitHostPort(servers[0])
			require.NoError(t, err)

			if !tt.enforce {
				assert.Equal(t, bootPort, published, "the container publishes the advertised port directly")

				return
			}

			assert.NotEqual(t, bootPort, published, "clients reach the relay, not the container")
			serveEchoAt(t, spec)
			assert.False(t, echoRoundTrip(t, servers[0]), "loopback is outside 203.0.113.0/24")

			g := sgtest.OpenTo("sg-1", 9092, "203.0.113.0/24")
			g.Ingress = append(g.Ingress, sgtest.OpenTo("", 9092, "127.0.0.1/32").Ingress...)
			src.Set(g)

			assert.True(t, echoRoundTrip(t, servers[0]), "authorized loopback connects on the next connection")
		})
	}
}
