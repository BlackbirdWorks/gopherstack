package mq_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/mq"
)

const realBrokerWait = 4 * time.Minute

func newRealDockerBackend(t *testing.T) *mq.InMemoryBackend {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	br, ok := rt.(mq.BrokerRuntime)
	if !ok {
		t.Skip("container runtime cannot run broker containers")
	}

	b := mq.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableBrokers(mq.BrokerConfig{Runtime: br, StartTimeout: realBrokerWait})
	t.Cleanup(b.Close)

	return b
}

func tlsHandshake(ctx context.Context, addr string) error {
	d := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 10 * time.Second},
		Config:    &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
	}

	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err //nolint:wrapcheck // test helper
	}

	return conn.Close() //nolint:wrapcheck // test helper
}

func endpointAddr(t *testing.T, endpoint string) string {
	t.Helper()

	u, err := url.Parse(endpoint)
	require.NoError(t, err)

	return u.Host
}

func TestRealDockerBrokerTLSEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		engine      string
		mode        string
		wantSchemes []string
		wantNodes   int
	}{
		{
			name: "rabbitmq single", engine: mq.EngineTypeRabbitMQ, mode: mq.DeploymentModeSingleInstance,
			wantSchemes: []string{"amqps"}, wantNodes: 1,
		},
		{
			name: "activemq active standby", engine: mq.EngineTypeActiveMQ, mode: mq.DeploymentModeActiveStandby,
			wantSchemes: []string{"ssl", "amqp+ssl", "stomp+ssl", "mqtt+ssl", "wss"}, wantNodes: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRealDockerBackend(t)
			br := createBrokerMode(t, b, tt.engine, tt.mode)

			require.Eventually(t, func() bool {
				return brokerState(b, br.BrokerID) != mq.BrokerStateCreating
			}, realBrokerWait, time.Second)
			require.Equal(t, mq.BrokerStateRunning, brokerState(b, br.BrokerID))

			desc, err := b.DescribeBroker(br.BrokerID)
			require.NoError(t, err)
			require.Len(t, desc.BrokerInstances, tt.wantNodes)

			client := &http.Client{
				Timeout:   15 * time.Second,
				Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
			}
			t.Cleanup(client.CloseIdleConnections)

			for _, inst := range desc.BrokerInstances {
				require.Len(t, inst.Endpoints, len(tt.wantSchemes))

				for i, ep := range inst.Endpoints {
					assert.True(t, strings.HasPrefix(ep, tt.wantSchemes[i]+"://"), ep)
					require.NoError(t, tlsHandshake(t.Context(), endpointAddr(t, ep)), ep)
				}

				req, rerr := http.NewRequestWithContext(t.Context(), http.MethodGet, inst.ConsoleURL, nil)
				require.NoError(t, rerr)

				resp, rerr := client.Do(req)
				require.NoError(t, rerr, inst.ConsoleURL)
				_ = resp.Body.Close()
				assert.Less(t, resp.StatusCode, http.StatusInternalServerError)
			}
		})
	}
}
