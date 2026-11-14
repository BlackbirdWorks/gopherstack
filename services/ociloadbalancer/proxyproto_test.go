package ociloadbalancer_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captureBackend struct {
	ln   net.Listener
	data chan []byte
}

// startCapture listens for TCP and reports the bytes of every connection that sends any, echoing "ok".
func startCapture(t *testing.T) *captureBackend {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	cb := &captureBackend{ln: ln, data: make(chan []byte, 8)}

	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			go func() {
				defer c.Close()

				var got []byte

				buf := make([]byte, 4096)

				for !bytes.HasSuffix(got, []byte("hello")) {
					n, rerr := c.Read(buf)
					got = append(got, buf[:n]...)

					if rerr != nil {
						break
					}
				}

				if len(got) == 0 {
					return
				}

				cb.data <- got

				_, _ = io.WriteString(c, "ok")
			}()
		}
	}()

	return cb
}

func (cb *captureBackend) detail() loadbalancer.BackendDetails {
	ta := cb.ln.Addr().(*net.TCPAddr)

	return loadbalancer.BackendDetails{IpAddress: new("127.0.0.1"), Port: new(ta.Port)}
}

func tcpLB(t *testing.T, e *env, cb *captureBackend, cc *loadbalancer.ConnectionConfiguration) string {
	t.Helper()

	d := baseDetails("proxyproto")
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": {
		Policy: new("ROUND_ROBIN"), Backends: []loadbalancer.BackendDetails{cb.detail()},
		HealthChecker: &loadbalancer.HealthCheckerDetails{
			Protocol: new("TCP"), Retries: new(0), IntervalInMillis: new(1000), TimeoutInMillis: new(1000),
		},
	}}
	cc.IdleTimeout = new(int64(30))
	d.Listeners = map[string]loadbalancer.ListenerDetails{"l": {
		DefaultBackendSetName: new("bs"), Port: new(7001), Protocol: new("TCP"), ConnectionConfiguration: cc,
	}}
	id := e.mkLB(t, d)

	return e.addr(t, id, 7001)
}

func TestBackendProxyProtocol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(t *testing.T, got []byte, client, lb net.Addr)
		name    string
		version int64
	}{
		{name: "v1", version: 1, check: func(t *testing.T, got []byte, client, lb net.Addr) {
			t.Helper()

			ca, la := client.(*net.TCPAddr), lb.(*net.TCPAddr)
			want := "PROXY TCP4 127.0.0.1 127.0.0.1 " + strconv.Itoa(
				ca.Port,
			) + " " + strconv.Itoa(
				la.Port,
			) + "\r\nhello"
			assert.Equal(t, want, string(got))
		}},
		{name: "v2", version: 2, check: func(t *testing.T, got []byte, client, lb net.Addr) {
			t.Helper()

			require.GreaterOrEqual(t, len(got), 16+12)
			assert.Equal(t, "\r\n\r\n\x00\r\nQUIT\n", string(got[:12]))
			assert.Equal(t, byte(0x21), got[12])
			assert.Equal(t, byte(0x11), got[13])
			assert.Equal(t, 12, int(binary.BigEndian.Uint16(got[14:16])))
			assert.Equal(t, []byte{127, 0, 0, 1, 127, 0, 0, 1}, got[16:24])
			assert.Equal(t, client.(*net.TCPAddr).Port, int(binary.BigEndian.Uint16(got[24:26])))
			assert.Equal(t, lb.(*net.TCPAddr).Port, int(binary.BigEndian.Uint16(got[26:28])))
			assert.Equal(t, "hello", string(got[28:]))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			cb := startCapture(t)
			addr := tcpLB(t, e, cb, &loadbalancer.ConnectionConfiguration{
				BackendTcpProxyProtocolVersion: new(int(tt.version)),
			})

			var d net.Dialer

			conn, err := d.DialContext(t.Context(), "tcp", addr)
			require.NoError(t, err)

			defer conn.Close()

			_, err = io.WriteString(conn, "hello")
			require.NoError(t, err)

			reply := make([]byte, 2)
			_, err = io.ReadFull(bufio.NewReader(conn), reply)
			require.NoError(t, err)
			assert.Equal(t, "ok", string(reply))

			tt.check(t, <-cb.data, conn.LocalAddr(), conn.RemoteAddr())
		})
	}
}

func TestBackendProxyProtocolAbsent(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	cb := startCapture(t)
	addr := tcpLB(t, e, cb, &loadbalancer.ConnectionConfiguration{})

	var d net.Dialer

	conn, err := d.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)

	defer conn.Close()

	_, err = io.WriteString(conn, "hello")
	require.NoError(t, err)
	assert.Equal(t, "hello", string(<-cb.data))
}

func TestBackendProxyProtocolValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version *int
		options []loadbalancer.ConnectionConfigurationBackendTcpProxyProtocolOptionsEnum
		wantErr bool
	}{
		{name: "v1", version: new(1)},
		{name: "v2", version: new(2)},
		{
			name: "v2authority", version: new(2),
			options: []loadbalancer.ConnectionConfigurationBackendTcpProxyProtocolOptionsEnum{
				loadbalancer.ConnectionConfigurationBackendTcpProxyProtocolOptionsPp2TypeAuthority,
			},
		},
		{name: "v3", version: new(3), wantErr: true},
		{name: "v0", version: new(0), wantErr: true},
		{
			name: "v1authority", version: new(1), wantErr: true,
			options: []loadbalancer.ConnectionConfigurationBackendTcpProxyProtocolOptionsEnum{
				loadbalancer.ConnectionConfigurationBackendTcpProxyProtocolOptionsPp2TypeAuthority,
			},
		},
		{
			name: "optionsonly", wantErr: true,
			options: []loadbalancer.ConnectionConfigurationBackendTcpProxyProtocolOptionsEnum{
				loadbalancer.ConnectionConfigurationBackendTcpProxyProtocolOptionsPp2TypeAuthority,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			cb := startCapture(t)

			d := baseDetails("ppval")
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": {
				Policy: new("ROUND_ROBIN"), Backends: []loadbalancer.BackendDetails{cb.detail()}, HealthChecker: hc(),
			}}
			d.Listeners = map[string]loadbalancer.ListenerDetails{"l": {
				DefaultBackendSetName: new("bs"), Port: new(7002), Protocol: new("TCP"),
				ConnectionConfiguration: &loadbalancer.ConnectionConfiguration{
					IdleTimeout: new(int64(30)), BackendTcpProxyProtocolVersion: tt.version,
					BackendTcpProxyProtocolOptions: tt.options,
				},
			}}

			_, err := e.lb.CreateLoadBalancer(t.Context(),
				loadbalancer.CreateLoadBalancerRequest{CreateLoadBalancerDetails: d})
			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestIPBasedMaxConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rule      loadbalancer.IpBasedMaxConnectionsRule
		wantAdmit bool
	}{
		{name: "default", rule: loadbalancer.IpBasedMaxConnectionsRule{DefaultMaxConnections: new(1)}},
		{name: "override", wantAdmit: true, rule: loadbalancer.IpBasedMaxConnectionsRule{
			DefaultMaxConnections: new(1),
			IpMaxConnections: []loadbalancer.IpMaxConnections{
				{IpAddresses: []string{"127.0.0.0/8"}, MaxConnections: new(5)},
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			a := startBackend(t, "a")
			d := baseDetails("ipmax")
			d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": backendSet("ROUND_ROBIN", a)}
			d.RuleSets = map[string]loadbalancer.RuleSetDetails{"cap": {Items: []loadbalancer.Rule{tt.rule}}}
			l := listener()
			l.Port, l.RuleSetNames = new(80), []string{"cap"}
			d.Listeners = map[string]loadbalancer.ListenerDetails{"l": l}
			id := e.mkLB(t, d)
			addr := e.addr(t, id, 80)

			var dialer net.Dialer

			// status sends a keep-alive request and returns the response status, 0 when the server closed the conn.
			status := func(c net.Conn) int {
				_, werr := io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
				if werr != nil {
					return 0
				}

				resp, rerr := http.ReadResponse(bufio.NewReader(c), nil)
				if rerr != nil {
					return 0
				}

				_ = resp.Body.Close()

				return resp.StatusCode
			}

			first, err := dialer.DialContext(t.Context(), "tcp", addr)
			require.NoError(t, err)

			defer first.Close()

			require.Equal(t, http.StatusOK, status(first), "the connection under the cap is served")

			second, err := dialer.DialContext(t.Context(), "tcp", addr)
			require.NoError(t, err)

			defer second.Close()

			if tt.wantAdmit {
				assert.Equal(t, http.StatusOK, status(second))

				return
			}

			assert.Zero(t, status(second), "the connection over the cap is closed")
			require.NoError(t, first.Close())

			require.Eventually(t, func() bool {
				third, derr := dialer.DialContext(t.Context(), "tcp", addr)
				if derr != nil {
					return false
				}

				defer third.Close()

				return status(third) == http.StatusOK
			}, 5*time.Second, 20*time.Millisecond, "a closed connection frees its slot")
		})
	}
}
