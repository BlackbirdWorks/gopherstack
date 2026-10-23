package mq_test

import (
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter/sgtest"
	"github.com/blackbirdworks/gopherstack/services/mq"
)

func echoAtSpecPort(t *testing.T, portSpec string) {
	t.Helper()

	ip, hostPort, _, err := container.ParsePortSpec(portSpec)
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

func echoWorks(t *testing.T, addr string) bool {
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

func TestDockerBrokerSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enforce bool
	}{
		{"enforced relays every listener", true},
		{"flag off publishes directly", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := sgtest.New(sgtest.OpenTo("sg-1", 5671, "203.0.113.0/24"))
			rt := &fakeRuntime{}
			probed := atomic.Value{}

			b := mq.NewInMemoryBackend("000000000000", "us-east-1")
			b.EnableBrokers(mq.BrokerConfig{
				Runtime: rt, StartTimeout: time.Minute,
				Probe: func(_ context.Context, _, addr, _, _ string) error {
					probed.Store(addr)

					return nil
				},
			})
			t.Cleanup(b.Close)

			if tt.enforce {
				b.SetSecurityGroupEnforcer(src.Enforcer())
			}

			_, err := b.CreateBroker("b1", "", mq.EngineTypeRabbitMQ, "", "", false, false,
				[]string{"sg-1"}, nil, []*mq.User{{Username: "admin", Password: "s3cretpassword"}}, nil)
			require.NoError(t, err)

			require.Eventually(t, func() bool { return brokerRunning(b) }, 10*time.Second, 5*time.Millisecond)

			br := describeOnly(t, b)
			require.NotEmpty(t, br.BrokerInstances)
			public := strings.TrimPrefix(br.BrokerInstances[0].Endpoints[0], "amqps://")

			spec := rt.createdSpecs()[0]
			_, published, _, err := container.ParsePortSpec(spec.Ports[1])
			require.NoError(t, err)

			if !tt.enforce {
				assert.Contains(t, public, ":"+published)

				return
			}

			assert.NotContains(t, public, ":"+published, "clients reach the relay, not the container")
			assert.True(t, strings.HasPrefix(spec.Ports[1], "127.0.0.1:"))

			addr, _ := probed.Load().(string)
			assert.True(t, strings.HasPrefix(addr, "127.0.0.1:"), "readiness probes bypass the relay: %s", addr)

			echoAtSpecPort(t, spec.Ports[1])
			assert.False(t, echoWorks(t, public), "loopback is outside 203.0.113.0/24")

			g := sgtest.OpenTo("sg-1", 5671, "203.0.113.0/24")
			g.Ingress = append(g.Ingress, sgtest.OpenTo("", 5671, "127.0.0.1/32").Ingress...)
			src.Set(g)

			assert.True(t, echoWorks(t, public), "authorized loopback connects on the next connection")
		})
	}
}

func describeOnly(t *testing.T, b *mq.InMemoryBackend) *mq.Broker {
	t.Helper()

	brokers := b.ListBrokers()
	require.Len(t, brokers, 1)

	br, err := b.DescribeBroker(brokers[0].BrokerID)
	require.NoError(t, err)

	return br
}

func brokerRunning(b *mq.InMemoryBackend) bool {
	for _, br := range b.ListBrokers() {
		if br.BrokerState == mq.BrokerStateRunning {
			return true
		}
	}

	return false
}
