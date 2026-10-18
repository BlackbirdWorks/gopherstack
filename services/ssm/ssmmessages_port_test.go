package ssm_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

type dialCall struct {
	container string
	host      string
	port      int
}

type fakeDialer struct {
	calls chan dialCall
	addr  string
}

func (f *fakeDialer) DialTarget(ctx context.Context, containerID, host string, port int) (net.Conn, error) {
	f.calls <- dialCall{container: containerID, host: host, port: port}

	var d net.Dialer

	return d.DialContext(ctx, "tcp", f.addr)
}

func startEcho(t *testing.T, prefix string) (string, chan struct{}) {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	gone := make(chan struct{})

	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}

		defer close(gone)
		defer conn.Close()

		buf := make([]byte, 256)

		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				_, _ = conn.Write(append([]byte(prefix), buf[:n]...))
			}

			if rerr != nil {
				return
			}
		}
	}()

	return ln.Addr().String(), gone
}

func portHandshake(t *testing.T, c *dcClient, acked map[int64]bool, wantProps map[string]any) {
	t.Helper()

	req := c.nextNonAck(acked)
	require.Equal(t, ssm.PayloadHandshakeRequest, req.PayloadType)

	var hr ssm.HandshakeRequestPayload
	require.NoError(t, json.Unmarshal(req.Payload, &hr))
	require.Len(t, hr.RequestedClientActions, 1)
	assert.Equal(t, "2.3.0.0", hr.AgentVersion)
	assert.Equal(t, "Port", hr.RequestedClientActions[0].ActionParameters["SessionType"])
	assert.Equal(t, wantProps, hr.RequestedClientActions[0].ActionParameters["Properties"])

	resp, _ := json.Marshal(ssm.HandshakeResponsePayload{
		ClientVersion:          "1.2.0.0",
		ProcessedClientActions: []ssm.ProcessedClientAction{{ActionType: "SessionType", ActionStatus: 1}},
	})
	c.input(ssm.PayloadHandshakeResponse, resp)

	done := c.nextNonAck(acked)
	require.Equal(t, ssm.PayloadHandshakeComplete, done.PayloadType)
}

func newPortServer(t *testing.T) (*httptest.Server, *ssm.InMemoryBackend) {
	t.Helper()

	h, bk := newTestHandler(t)
	bk.SetCommandRunner(&ssm.CommandRunner{Instances: sessionInstances{}})

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv, bk
}

func startDoc(t *testing.T, srv *httptest.Server, doc string, params map[string][]string) (string, string) {
	t.Helper()

	out := ssmCall(
		t,
		srv,
		"StartSession",
		map[string]any{"Target": "i-0123", "DocumentName": doc, "Parameters": params},
	)
	sid, _ := out["SessionId"].(string)
	token, _ := out["TokenValue"].(string)
	require.NotEmpty(t, sid, "%v", out)

	return sid, token
}

func openChannel(t *testing.T, srv *httptest.Server, sid string) *dcClient {
	t.Helper()

	url := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/ssmmessages/v1/data-channel/" + sid
	ws, _, err := websocket.DefaultDialer.DialContext(t.Context(), url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })

	return &dcClient{t: t, ws: ws}
}

func TestSessionPortForwarding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params    map[string][]string
		wantProps map[string]any
		doc       string
		wantHost  string
		wantPort  int
		terminate bool
	}{
		{
			doc:       "AWS-StartPortForwardingSession",
			params:    map[string][]string{"portNumber": {"8080"}, "localPortNumber": {"9000"}},
			wantProps: map[string]any{"portNumber": "8080", "type": "LocalPortForwarding"},
			wantPort:  8080,
		},
		{
			doc:       "AWS-StartPortForwardingSessionToRemoteHost",
			params:    map[string][]string{"host": {"db.internal"}, "portNumber": {"5432"}},
			wantProps: map[string]any{"portNumber": "5432", "host": "db.internal", "type": "LocalPortForwarding"},
			wantHost:  "db.internal",
			wantPort:  5432,
			terminate: true,
		},
		{
			doc:       "AWS-StartSSHSession",
			wantProps: map[string]any{"portNumber": "22", "type": ""},
			wantPort:  22,
		},
	}

	for _, tt := range tests {
		t.Run(tt.doc, func(t *testing.T) {
			t.Parallel()

			srv, bk := newPortServer(t)
			addr, gone := startEcho(t, "echo:")
			fd := &fakeDialer{addr: addr, calls: make(chan dialCall, 1)}

			bk.SetPortDialer(fd)

			sid, token := startDoc(t, srv, tt.doc, tt.params)
			c := openChannel(t, srv, sid)
			c.open(token)

			acked := map[int64]bool{}
			portHandshake(t, c, acked, tt.wantProps)

			assert.Equal(t, dialCall{container: "c1", host: tt.wantHost, port: tt.wantPort}, <-fd.calls)

			c.input(ssm.PayloadOutput, []byte("ping"))

			out := c.nextNonAck(acked)
			assert.Equal(t, ssm.MsgOutputStreamData, out.MessageType)
			assert.Equal(t, ssm.PayloadOutput, out.PayloadType)
			assert.Equal(t, "echo:ping", string(out.Payload))

			if tt.terminate {
				flag := make([]byte, 4)
				binary.BigEndian.PutUint32(flag, 2)
				c.input(ssm.PayloadFlag, flag)
			} else {
				ssmCall(t, srv, "TerminateSession", map[string]string{"SessionId": sid})
			}

			closed := c.nextNonAck(acked)
			assert.Equal(t, ssm.MsgChannelClosed, closed.MessageType)
			<-gone
		})
	}
}

func TestSessionPortForwardingTargetClose(t *testing.T) {
	t.Parallel()

	srv, bk := newPortServer(t)

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}

		_, _ = io.WriteString(conn, "bye")
		_ = conn.Close()
	}()

	bk.SetPortDialer(&fakeDialer{addr: ln.Addr().String(), calls: make(chan dialCall, 1)})

	sid, token := startDoc(t, srv,
		"AWS-StartPortForwardingSession", map[string][]string{"portNumber": {"80"}})
	c := openChannel(t, srv, sid)
	c.open(token)

	acked := map[int64]bool{}
	portHandshake(t, c, acked, map[string]any{"portNumber": "80", "type": "LocalPortForwarding"})

	out := c.nextNonAck(acked)
	assert.Equal(t, "bye", string(out.Payload))

	flag := c.nextNonAck(acked)
	assert.Equal(t, ssm.PayloadFlag, flag.PayloadType)
	assert.Equal(t, uint32(1), binary.BigEndian.Uint32(flag.Payload))

	closed := c.nextNonAck(acked)
	assert.Equal(t, ssm.MsgChannelClosed, closed.MessageType)
}

func TestSessionPortForwardingErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params map[string][]string
		name   string
		doc    string
		dialer bool
	}{
		{name: "no_dialer", doc: "AWS-StartPortForwardingSession", params: map[string][]string{"portNumber": {"80"}}},
		{name: "missing_port", doc: "AWS-StartPortForwardingSession", dialer: true},
		{
			name:   "bad_port",
			doc:    "AWS-StartPortForwardingSession",
			dialer: true,
			params: map[string][]string{"portNumber": {"70000"}},
		},
		{
			name: "missing_host", doc: "AWS-StartPortForwardingSessionToRemoteHost", dialer: true,
			params: map[string][]string{"portNumber": {"80"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, bk := newPortServer(t)
			if tt.dialer {
				bk.SetPortDialer(&fakeDialer{addr: "127.0.0.1:1", calls: make(chan dialCall, 1)})
			}

			sid, token := startDoc(t, srv, tt.doc, tt.params)
			c := openChannel(t, srv, sid)
			c.open(token)

			req := c.nextNonAck(map[int64]bool{})
			require.Equal(t, ssm.PayloadHandshakeRequest, req.PayloadType)

			resp, _ := json.Marshal(ssm.HandshakeResponsePayload{
				ProcessedClientActions: []ssm.ProcessedClientAction{{ActionType: "SessionType", ActionStatus: 1}},
			})
			c.input(ssm.PayloadHandshakeResponse, resp)

			done := c.nextNonAck(map[int64]bool{})
			require.Equal(t, ssm.PayloadHandshakeComplete, done.PayloadType)

			closed := c.nextNonAck(map[int64]bool{})
			assert.Equal(t, ssm.MsgChannelClosed, closed.MessageType)
			assert.Contains(t, string(closed.Payload), "Cannot start session")
		})
	}
}
