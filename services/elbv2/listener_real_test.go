package elbv2_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

const (
	policyTLS12 = "ELBSecurityPolicy-TLS13-1-2-2021-06"
	policyTLS13 = "ELBSecurityPolicy-TLS13-1-3-2021-06"
	policyOld   = "ELBSecurityPolicy-2016-08"
)

type headerEcho struct {
	srv *httptest.Server
}

// newHeaderEcho replies with the request headers it saw as "Name: value" lines.
func newHeaderEcho(t *testing.T) *headerEcho {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, vs := range r.Header {
			for _, v := range vs {
				_, _ = w.Write([]byte(k + ": " + v + "\n"))
			}
		}
	}))
	t.Cleanup(srv.Close)

	return &headerEcho{srv: srv}
}

func (e *headerEcho) addr() string { return e.srv.Listener.Addr().String() }

func headerLines(body string) map[string]string {
	out := map[string]string{}

	for line := range strings.SplitSeq(body, "\n") {
		if k, v, ok := strings.Cut(line, ": "); ok {
			out[k] = v
		}
	}

	return out
}

func getBody(t *testing.T, c *http.Client, url string, hdr map[string]string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := c.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(b)
}

func TestRealListener_HTTPForwardHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs      map[string]string
		name       string
		priorXFF   string
		wantXFF    string
		wantPrefix string
		wantNoXFF  bool
	}{
		{name: "append default", wantXFF: "127.0.0.1"},
		{name: "append existing", priorXFF: "10.0.0.9", wantXFF: "10.0.0.9, 127.0.0.1"},
		{
			name: "preserve", priorXFF: "10.0.0.9", wantXFF: "10.0.0.9",
			attrs: map[string]string{"routing.http.xff_header_processing.mode": "preserve"},
		},
		{
			name: "remove", priorXFF: "10.0.0.9", wantNoXFF: true,
			attrs: map[string]string{"routing.http.xff_header_processing.mode": "remove"},
		},
		{
			name: "client port", wantPrefix: "127.0.0.1:",
			attrs: map[string]string{"routing.http.xff_client_port.enabled": "true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealFixture(t)
			echo := newHeaderEcho(t)
			lb := f.lb(t, "alb-http", elbv2types.LoadBalancerTypeEnumApplication)
			tg := f.tg(t, "tg-http", elbv2types.ProtocolEnumHttp, echo.addr())
			f.attrs(t, lb, tt.attrs)
			l := f.listener(t, lb, elbv2types.ProtocolEnumHttp, 80, tg, listenerOpts{})

			hdr := map[string]string{}
			if tt.priorXFF != "" {
				hdr["X-Forwarded-For"] = tt.priorXFF
			}

			status, body := getBody(t, f.httpClient(t, "", 0), "http://"+f.addr(t, l)+"/x?y=1", hdr)
			require.Equal(t, http.StatusOK, status)

			got := headerLines(body)
			assert.Equal(t, "http", got["X-Forwarded-Proto"])
			assert.Equal(t, "80", got["X-Forwarded-Port"], "port is the listener port, not the host port")
			assert.NotEmpty(t, got["X-Amzn-Trace-Id"])

			switch {
			case tt.wantNoXFF:
				assert.NotContains(t, got, "X-Forwarded-For")
			case tt.wantPrefix != "":
				assert.True(t, strings.HasPrefix(got["X-Forwarded-For"], tt.wantPrefix), got["X-Forwarded-For"])
			default:
				assert.Equal(t, tt.wantXFF, got["X-Forwarded-For"])
			}
		})
	}
}

func TestRealListener_HTTPSTerminationAndSNI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		serverName string
		wantDNS    string
	}{
		{name: "sni selects additional", serverName: "b.example.test", wantDNS: "b.example.test"},
		{name: "sni selects default", serverName: "a.example.test", wantDNS: "a.example.test"},
		{name: "no sni uses default", serverName: "", wantDNS: "a.example.test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealFixture(t)
			echo := newHeaderEcho(t)
			lb := f.lb(t, "alb-https", elbv2types.LoadBalancerTypeEnumApplication)
			tg := f.tg(t, "tg-https", elbv2types.ProtocolEnumHttp, echo.addr())
			certA := f.cert(t, "a", "a.example.test", "127.0.0.1")
			certB := f.cert(t, "b", "b.example.test")
			l := f.listener(
				t,
				lb,
				elbv2types.ProtocolEnumHttps,
				443,
				tg,
				listenerOpts{certs: []string{certA}, policy: policyTLS12},
			)

			_, err := f.client.AddListenerCertificates(t.Context(), &elbv2sdk.AddListenerCertificatesInput{
				ListenerArn:  aws.String(l),
				Certificates: []elbv2types.Certificate{{CertificateArn: aws.String(certB)}},
			})
			require.NoError(t, err)

			addr := f.addr(t, l)
			host, port, err := net.SplitHostPort(addr)
			require.NoError(t, err)

			url := "https://" + addr + "/p"
			name := tt.serverName

			if name == "" {
				name = host
			}

			if tt.serverName != "" {
				url = "https://" + net.JoinHostPort(tt.serverName, port) + "/p"
			}

			c := f.httpClient(t, name, 0)
			tr, ok := c.Transport.(*http.Transport)
			require.True(t, ok)

			tr.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer

				return d.DialContext(ctx, "tcp", addr)
			}

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
			require.NoError(t, err)

			resp, err := c.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			require.NotNil(t, resp.TLS)
			assert.Contains(t, resp.TLS.PeerCertificates[0].DNSNames, tt.wantDNS)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			got := headerLines(string(b))
			assert.Equal(t, "https", got["X-Forwarded-Proto"])
			assert.Equal(t, "443", got["X-Forwarded-Port"])
			assert.Equal(t, "TLSv1.3", got["X-Amzn-Tls-Version"])
			assert.NotEmpty(t, got["X-Amzn-Tls-Cipher-Suite"])
		})
	}
}

func TestRealListener_SecurityPolicyVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     string
		maxVersion uint16
		wantErr    bool
	}{
		{name: "tls13 only rejects tls12", policy: policyTLS13, maxVersion: tls.VersionTLS12, wantErr: true},
		{name: "tls13 only accepts tls13", policy: policyTLS13, maxVersion: tls.VersionTLS13},
		{name: "1-2 policy accepts tls12", policy: policyTLS12, maxVersion: tls.VersionTLS12},
		{name: "2016-08 accepts tls12", policy: policyOld, maxVersion: tls.VersionTLS12},
		{name: "default policy accepts tls12", policy: "", maxVersion: tls.VersionTLS12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealFixture(t)
			echo := newHeaderEcho(t)
			lb := f.lb(t, "alb-pol", elbv2types.LoadBalancerTypeEnumApplication)
			tg := f.tg(t, "tg-pol", elbv2types.ProtocolEnumHttp, echo.addr())
			cert := f.cert(t, "pol", "127.0.0.1")
			l := f.listener(
				t,
				lb,
				elbv2types.ProtocolEnumHttps,
				443,
				tg,
				listenerOpts{certs: []string{cert}, policy: tt.policy},
			)

			conn, err := tls.Dial("tcp", f.addr(t, l), &tls.Config{
				RootCAs: f.ca.Pool(), ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12, MaxVersion: tt.maxVersion,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NoError(t, conn.Close())
		})
	}
}

// wsTarget accepts an HTTP upgrade and then echoes raw bytes.
func wsTarget(t *testing.T) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "no upgrade", http.StatusBadRequest)

			return
		}

		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}

		defer conn.Close()

		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		_ = rw.Flush()
		_, _ = io.Copy(conn, rw)
	}))
	t.Cleanup(srv.Close)

	return srv.Listener.Addr().String()
}

func TestRealListener_WebSocketEcho(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		proto elbv2types.ProtocolEnum
		port  int32
	}{
		{name: "ws over http", proto: elbv2types.ProtocolEnumHttp, port: 80},
		{name: "wss over https", proto: elbv2types.ProtocolEnumHttps, port: 443},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealFixture(t)
			lb := f.lb(t, "alb-ws", elbv2types.LoadBalancerTypeEnumApplication)
			tg := f.tg(t, "tg-ws", elbv2types.ProtocolEnumHttp, wsTarget(t))

			opts := listenerOpts{}
			if tt.proto == elbv2types.ProtocolEnumHttps {
				opts.certs = []string{f.cert(t, "ws", "127.0.0.1")}
			}

			l := f.listener(t, lb, tt.proto, tt.port, tg, opts)

			var (
				conn net.Conn
				err  error
			)

			if tt.proto == elbv2types.ProtocolEnumHttps {
				conn, err = tls.Dial("tcp", f.addr(t, l), &tls.Config{
					RootCAs: f.ca.Pool(), ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12,
				})
			} else {
				conn, err = net.Dial("tcp", f.addr(t, l))
			}

			require.NoError(t, err)

			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

			_, err = io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\n"+
				"Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
			require.NoError(t, err)

			br := bufio.NewReader(conn)
			resp, err := http.ReadResponse(br, nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)

			_, err = io.WriteString(conn, "ping-frame")
			require.NoError(t, err)

			buf := make([]byte, len("ping-frame"))
			_, err = io.ReadFull(br, buf)
			require.NoError(t, err)
			assert.Equal(t, "ping-frame", string(buf))
		})
	}
}

func tcpEcho(t *testing.T, onConn func(net.Conn) io.Reader) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var wg sync.WaitGroup

	t.Cleanup(func() { _ = ln.Close(); wg.Wait() })

	wg.Go(func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			wg.Go(func() {
				defer c.Close()

				r := io.Reader(c)
				if onConn != nil {
					r = onConn(c)
				}

				_, _ = io.Copy(c, r)
			})
		}
	})

	return ln.Addr().String()
}

func TestRealListener_NLBTCPEcho(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		tls           bool
		proxyProtocol bool
	}{
		{name: "tcp"},
		{name: "tls", tls: true},
		{name: "tcp proxy protocol v2", proxyProtocol: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealFixture(t)

			ppHeader := make(chan string, 1)

			target := tcpEcho(t, func(c net.Conn) io.Reader {
				if !tt.proxyProtocol {
					return c
				}

				hdr := make([]byte, 16+12)
				if _, err := io.ReadFull(c, hdr); err != nil {
					return c
				}

				ppHeader <- fmt.Sprintf("%x|%x|%d|%s:%d|%d", hdr[12], hdr[13], binary.BigEndian.Uint16(hdr[14:16]),
					net.IP(hdr[16:20]), binary.BigEndian.Uint16(hdr[24:26]), binary.BigEndian.Uint16(hdr[26:28]))

				return c
			})

			lb := f.lb(t, "nlb-tcp", elbv2types.LoadBalancerTypeEnumNetwork)
			tgProto := elbv2types.ProtocolEnumTcp
			tg := f.tg(t, "tg-tcp", tgProto, target)

			if tt.proxyProtocol {
				_, err := f.client.ModifyTargetGroupAttributes(t.Context(), &elbv2sdk.ModifyTargetGroupAttributesInput{
					TargetGroupArn: aws.String(tg),
					Attributes: []elbv2types.TargetGroupAttribute{
						{Key: aws.String("proxy_protocol_v2.enabled"), Value: aws.String("true")},
					},
				})
				require.NoError(t, err)
			}

			proto := elbv2types.ProtocolEnumTcp
			opts := listenerOpts{}

			if tt.tls {
				proto = elbv2types.ProtocolEnumTls
				opts = listenerOpts{certs: []string{f.cert(t, "nlb", "127.0.0.1")}, policy: policyTLS12}
			}

			l := f.listener(t, lb, proto, 9000, tg, opts)

			var (
				conn net.Conn
				err  error
			)

			if tt.tls {
				conn, err = tls.Dial("tcp", f.addr(t, l), &tls.Config{
					RootCAs: f.ca.Pool(), ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12,
				})
			} else {
				conn, err = net.Dial("tcp", f.addr(t, l))
			}

			require.NoError(t, err)

			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

			_, err = io.WriteString(conn, "hello-nlb")
			require.NoError(t, err)

			buf := make([]byte, len("hello-nlb"))
			_, err = io.ReadFull(conn, buf)
			require.NoError(t, err)
			assert.Equal(t, "hello-nlb", string(buf))

			if tt.proxyProtocol {
				got := <-ppHeader
				assert.True(t, strings.HasPrefix(got, "21|11|12|127.0.0.1:"), got)
				assert.True(t, strings.HasSuffix(got, "|9000"), "destination port is the listener port: %s", got)
			}
		})
	}
}

func TestRealListener_NLBUDPEcho(t *testing.T) {
	t.Parallel()

	f := newRealFixture(t)

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		defer close(done)

		buf := make([]byte, 1500)

		for {
			n, addr, rerr := pc.ReadFrom(buf)
			if rerr != nil {
				return
			}

			_, _ = pc.WriteTo(buf[:n], addr)
		}
	}()

	t.Cleanup(func() { _ = pc.Close(); <-done })

	lb := f.lb(t, "nlb-udp", elbv2types.LoadBalancerTypeEnumNetwork)
	tg := f.tg(t, "tg-udp", elbv2types.ProtocolEnumUdp, pc.LocalAddr().String())
	l := f.listener(t, lb, elbv2types.ProtocolEnumUdp, 5353, tg, listenerOpts{})

	conn, err := net.Dial("udp", f.addr(t, l))
	require.NoError(t, err)

	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	_, err = conn.Write([]byte("dgram"))
	require.NoError(t, err)

	buf := make([]byte, 16)
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "dgram", string(buf[:n]))
}

func unreachable(t *testing.T, addr string) {
	t.Helper()

	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err == nil {
		_ = c.Close()
	}

	require.Error(t, err, "socket %s should be closed", addr)
}

func TestRealListener_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act  func(t *testing.T, f *realFixture, lb, l string)
		name string
	}{
		{name: "delete listener", act: func(t *testing.T, f *realFixture, _, l string) {
			t.Helper()

			_, err := f.client.DeleteListener(t.Context(), &elbv2sdk.DeleteListenerInput{ListenerArn: aws.String(l)})
			require.NoError(t, err)
		}},
		{name: "delete load balancer", act: func(t *testing.T, f *realFixture, lb, _ string) {
			t.Helper()

			_, err := f.client.DeleteLoadBalancer(
				t.Context(),
				&elbv2sdk.DeleteLoadBalancerInput{LoadBalancerArn: aws.String(lb)},
			)
			require.NoError(t, err)
		}},
		{name: "backend close", act: func(t *testing.T, f *realFixture, _, _ string) {
			t.Helper()

			f.be.Close()
		}},
		{name: "modify port rebinds", act: func(t *testing.T, f *realFixture, _, l string) {
			t.Helper()

			_, err := f.client.ModifyListener(t.Context(), &elbv2sdk.ModifyListenerInput{
				ListenerArn: aws.String(l), Port: aws.Int32(8081),
			})
			require.NoError(t, err)

			ep, ok := f.be.ListenerEndpoint(l)
			require.True(t, ok)
			assert.Equal(t, int32(8081), ep.Port)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealFixture(t)
			echo := newHeaderEcho(t)
			lb := f.lb(t, "alb-life", elbv2types.LoadBalancerTypeEnumApplication)
			tg := f.tg(t, "tg-life", elbv2types.ProtocolEnumHttp, echo.addr())
			l := f.listener(t, lb, elbv2types.ProtocolEnumHttp, 80, tg, listenerOpts{})
			addr := f.addr(t, l)

			status, _ := getBody(t, f.httpClient(t, "", 0), "http://"+addr+"/", nil)
			require.Equal(t, http.StatusOK, status)

			tt.act(t, f, lb, l)
			unreachable(t, addr)
		})
	}
}

func TestRealListener_PortMapping(t *testing.T) {
	t.Parallel()

	free, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	freePort := int32(free.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert // test listener is TCP
	require.NoError(t, free.Close())

	tests := []struct {
		name       string
		port       int32
		wantMapped bool
	}{
		{name: "unprivileged free port binds directly", port: freePort},
		{name: "privileged port maps to pool", port: 80, wantMapped: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealFixture(t)
			echo := newHeaderEcho(t)
			lb := f.lb(t, "alb-map", elbv2types.LoadBalancerTypeEnumApplication)
			tg := f.tg(t, "tg-map", elbv2types.ProtocolEnumHttp, echo.addr())
			l := f.listener(t, lb, elbv2types.ProtocolEnumHttp, tt.port, tg, listenerOpts{})

			ep, ok := f.be.ListenerEndpoint(l)
			require.True(t, ok)
			assert.Equal(t, tt.port, ep.Port)
			assert.Equal(t, tt.wantMapped, int32(ep.HostPort) != tt.port)
			assert.Len(t, f.be.ListenerEndpoints(), 1)
		})
	}
}

func TestRealListener_DisabledByDefault(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	tg := f.tg(t, "tg-off", elbv2types.TargetTypeEnumIp, "")
	l := f.listener(t, forwardAction(tg))

	be, ok := f.h.Backend.(*elbv2.InMemoryBackend)
	require.True(t, ok)

	_, bound := be.ListenerEndpoint(l)
	assert.False(t, bound)
	assert.Empty(t, be.ListenerEndpoints())
}

func TestRealListener_DeleteDuringConnectionStorm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		proto elbv2types.ProtocolEnum
	}{
		{name: "tcp", proto: elbv2types.ProtocolEnumTcp},
		{name: "udp", proto: elbv2types.ProtocolEnumUdp},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealFixture(t)
			lb := f.lb(t, "nlb-storm", elbv2types.LoadBalancerTypeEnumNetwork)
			target := tcpEcho(t, nil)

			tgProto := elbv2types.ProtocolEnumTcp
			if tt.proto == elbv2types.ProtocolEnumUdp {
				pc, err := net.ListenPacket("udp", "127.0.0.1:0")
				require.NoError(t, err)
				t.Cleanup(func() { _ = pc.Close() })

				target, tgProto = pc.LocalAddr().String(), elbv2types.ProtocolEnumUdp
			}

			tg := f.tg(t, "tg-storm", tgProto, target)
			l := f.listener(t, lb, tt.proto, 9100, tg, listenerOpts{})
			addr := f.addr(t, l)
			network := "tcp"

			if tt.proto == elbv2types.ProtocolEnumUdp {
				network = "udp"
			}

			var wg sync.WaitGroup

			for range 8 {
				wg.Go(func() {
					for range 2000 {
						c, err := net.Dial(network, addr)
						if err != nil {
							return
						}

						_, _ = c.Write([]byte("x"))
						_ = c.Close()
					}
				})
			}

			_, err := f.client.DeleteListener(t.Context(), &elbv2sdk.DeleteListenerInput{ListenerArn: aws.String(l)})
			require.NoError(t, err)

			if network == "tcp" {
				unreachable(t, addr)
			}

			wg.Wait()
		})
	}
}
