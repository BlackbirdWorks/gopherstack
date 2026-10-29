package ssm_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestStreamMessageRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mtype   string
		payload []byte
		seq     int64
		ptype   uint32
	}{
		{"empty", ssm.MsgAcknowledge, nil, 0, 0},
		{"output", ssm.MsgOutputStreamData, []byte("hello\r\n"), 7, ssm.PayloadOutput},
		{"input_neg_seq", ssm.MsgInputStreamData, []byte{0, 1, 2, 255}, -1, ssm.PayloadSize},
		{"closed", ssm.MsgChannelClosed, []byte(`{"Output":"bye"}`), 3, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := ssm.NewStreamMessage(tt.mtype, tt.seq, tt.ptype, tt.payload)
			raw := in.Marshal()

			assert.Len(t, raw, 120+len(tt.payload))
			assert.Equal(t, uint32(116), binary.BigEndian.Uint32(raw))
			assert.Equal(t, tt.mtype+strings.Repeat(" ", 32-len(tt.mtype)), string(raw[4:36]))

			digest := sha256.Sum256(tt.payload)
			assert.Equal(t, digest[:], raw[80:112])

			out, err := ssm.ParseStreamMessage(raw)
			require.NoError(t, err)
			assert.Equal(t, in.MessageID, out.MessageID)
			assert.Equal(t, in.CreatedDate, out.CreatedDate)
			assert.Equal(t, tt.mtype, out.MessageType)
			assert.Equal(t, tt.seq, out.SequenceNumber)
			assert.Equal(t, tt.ptype, out.PayloadType)
			assert.Len(t, out.Payload, len(tt.payload))
		})
	}
}

func TestStreamMessageParseErrors(t *testing.T) {
	t.Parallel()

	good := ssm.NewStreamMessage(ssm.MsgOutputStreamData, 0, 1, []byte("abc")).Marshal()

	tests := []struct {
		mutate func([]byte) []byte
		name   string
	}{
		{func(b []byte) []byte { return b[:50] }, "short"},
		{func(b []byte) []byte {
			b[3] = 9

			return b
		}, "header_len"},
		{func(b []byte) []byte { return b[:len(b)-1] }, "payload_len"},
		{func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[116:], 0xFFFFFFFF)

			return b
		}, "payload_len_overflow"},
		{func(b []byte) []byte {
			b[len(b)-1] ^= 1

			return b
		}, "digest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ssm.ParseStreamMessage(tt.mutate(append([]byte(nil), good...)))
			require.Error(t, err)
		})
	}
}

type sessionInstances struct{}

func (sessionInstances) ContainerInstances(_ string, _ []string) []ssm.ManagedContainer {
	return []ssm.ManagedContainer{{InstanceID: "i-0123", ContainerID: "c1"}}
}

type fakeShell struct {
	out    chan []byte
	closed sync.Once
	mu     sync.Mutex
	size   [2]uint
}

func (s *fakeShell) Read(p []byte) (int, error) {
	b, ok := <-s.out
	if !ok {
		return 0, io.EOF
	}

	return copy(p, b), nil
}

func (s *fakeShell) Write(p []byte) (int, error) {
	if line, ok := strings.CutPrefix(string(p), "echo "); ok {
		s.out <- []byte(line)
	}

	if string(p) == "exit\n" {
		close(s.out)
	}

	return len(p), nil
}

func (s *fakeShell) Close() error {
	s.closed.Do(func() {
		defer func() { _ = recover() }()
		close(s.out)
	})

	return nil
}

func (s *fakeShell) Resize(_ context.Context, cols, rows uint) error {
	s.mu.Lock()
	s.size = [2]uint{cols, rows}
	s.mu.Unlock()

	return nil
}

type fakeOpener struct{}

func (fakeOpener) OpenShell(context.Context, string, uint, uint) (ssm.Shell, error) {
	return &fakeShell{out: make(chan []byte, 8)}, nil
}

type dcClient struct {
	t   *testing.T
	ws  *websocket.Conn
	seq int64
}

func newServer(t *testing.T, withShell bool) *httptest.Server {
	t.Helper()

	h, bk := newTestHandler(t)
	bk.SetCommandRunner(&ssm.CommandRunner{Instances: sessionInstances{}})

	if withShell {
		bk.SetShellOpener(fakeOpener{})
	}

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func ssmCall(t *testing.T, srv *httptest.Server, op string, in any) map[string]any {
	t.Helper()

	body, err := json.Marshal(in)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL, strings.NewReader(string(body)))
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", "AmazonSSM."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	return out
}

func dial(t *testing.T, srv *httptest.Server, target string) (*dcClient, string, string) {
	t.Helper()

	out := ssmCall(t, srv, "StartSession", map[string]string{"Target": target})
	streamURL, _ := out["StreamUrl"].(string)
	token, _ := out["TokenValue"].(string)
	sid, _ := out["SessionId"].(string)

	wantPrefix := "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/ssmmessages/v1/data-channel/" + sid
	require.True(t, strings.HasPrefix(streamURL, wantPrefix), streamURL)
	require.Contains(t, streamURL, "role=publish_subscribe")

	ws, _, err := websocket.DefaultDialer.DialContext(t.Context(), streamURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })

	return &dcClient{t: t, ws: ws}, sid, token
}

func (c *dcClient) open(token string) {
	c.t.Helper()

	b, _ := json.Marshal(ssm.OpenDataChannelInput{
		MessageSchemaVersion: "1.0", RequestID: uuid.NewString(), TokenValue: token,
		ClientID: uuid.NewString(), ClientVersion: "1.2.0.0",
	})
	require.NoError(c.t, c.ws.WriteMessage(websocket.TextMessage, b))
}

func (c *dcClient) input(ptype uint32, payload []byte) {
	c.t.Helper()

	m := ssm.NewStreamMessage(ssm.MsgInputStreamData, c.seq, ptype, payload)
	c.seq++
	require.NoError(c.t, c.ws.WriteMessage(websocket.BinaryMessage, m.Marshal()))
}

func (c *dcClient) inputAt(seq int64, ptype uint32, payload []byte) {
	c.t.Helper()

	m := ssm.NewStreamMessage(ssm.MsgInputStreamData, seq, ptype, payload)
	require.NoError(c.t, c.ws.WriteMessage(websocket.BinaryMessage, m.Marshal()))
}

func (c *dcClient) next() *ssm.StreamMessage {
	c.t.Helper()

	require.NoError(c.t, c.ws.SetReadDeadline(time.Now().Add(10*time.Second)))

	for {
		typ, raw, err := c.ws.ReadMessage()
		require.NoError(c.t, err)

		if typ == websocket.BinaryMessage {
			m, perr := ssm.ParseStreamMessage(raw)
			require.NoError(c.t, perr)

			return m
		}
	}
}

// nextNonAck skips acknowledgements, recording acked input sequence numbers.
func (c *dcClient) nextNonAck(acked map[int64]bool) *ssm.StreamMessage {
	c.t.Helper()

	for {
		m := c.next()
		if m.MessageType != ssm.MsgAcknowledge {
			return m
		}

		var a ssm.AckPayload
		require.NoError(c.t, json.Unmarshal(m.Payload, &a))
		acked[a.SequenceNumber] = true
	}
}

func (c *dcClient) handshake(acked map[int64]bool) {
	c.t.Helper()

	req := c.nextNonAck(acked)
	require.Equal(c.t, ssm.MsgOutputStreamData, req.MessageType)
	require.Equal(c.t, ssm.PayloadHandshakeRequest, req.PayloadType)
	assert.Contains(c.t, string(req.Payload), "Standard_Stream")

	resp, _ := json.Marshal(ssm.HandshakeResponsePayload{
		ClientVersion:          "1.2.0.0",
		ProcessedClientActions: []ssm.ProcessedClientAction{{ActionType: "SessionType", ActionStatus: 1}},
	})
	c.input(ssm.PayloadHandshakeResponse, resp)

	done := c.nextNonAck(acked)
	require.Equal(c.t, ssm.PayloadHandshakeComplete, done.PayloadType)
	assert.Equal(c.t, int64(1), done.SequenceNumber)
}

func TestSessionDataChannelShell(t *testing.T) {
	t.Parallel()

	srv := newServer(t, true)
	c, _, token := dial(t, srv, "i-0123")
	c.open(token)

	acked := map[int64]bool{}
	c.handshake(acked)

	c.input(ssm.PayloadSize, []byte(`{"cols":100,"rows":30}`))
	c.input(ssm.PayloadOutput, []byte("echo hi\n"))

	out := c.nextNonAck(acked)
	assert.Equal(t, ssm.MsgOutputStreamData, out.MessageType)
	assert.Equal(t, ssm.PayloadOutput, out.PayloadType)
	assert.Equal(t, "hi\n", string(out.Payload))
	assert.Equal(t, int64(2), out.SequenceNumber)

	c.input(ssm.PayloadOutput, []byte("exit\n"))

	closed := c.nextNonAck(acked)
	assert.Equal(t, ssm.MsgChannelClosed, closed.MessageType)
	assert.Contains(t, string(closed.Payload), "Exiting session")
	assert.Equal(t, map[int64]bool{0: true, 1: true, 2: true, 3: true}, acked)
}

func TestSessionDataChannelOrdering(t *testing.T) {
	t.Parallel()

	srv := newServer(t, true)
	c, _, token := dial(t, srv, "i-0123")
	c.open(token)

	acked := map[int64]bool{}
	c.handshake(acked)

	c.inputAt(2, ssm.PayloadOutput, []byte("echo two\n"))
	c.inputAt(1, ssm.PayloadOutput, []byte("echo one\n"))

	first := c.nextNonAck(acked)
	second := c.nextNonAck(acked)
	assert.Equal(t, "one\n", string(first.Payload))
	assert.Equal(t, "two\n", string(second.Payload))
}

func TestSessionDataChannelTerminate(t *testing.T) {
	t.Parallel()

	srv := newServer(t, true)
	c, sid, token := dial(t, srv, "i-0123")
	c.open(token)
	c.handshake(map[int64]bool{})

	ssmCall(t, srv, "TerminateSession", map[string]string{"SessionId": sid})

	closed := c.nextNonAck(map[int64]bool{})
	assert.Equal(t, ssm.MsgChannelClosed, closed.MessageType)
}

func TestSessionDataChannelNoShell(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		withShell bool
	}{
		{"no_backend", "i-0123", false},
		{"unknown_target", "i-9999", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newServer(t, tt.withShell)
			c, _, token := dial(t, srv, tt.target)
			c.open(token)
			c.handshake(map[int64]bool{})

			closed := c.nextNonAck(map[int64]bool{})
			assert.Equal(t, ssm.MsgChannelClosed, closed.MessageType)
			assert.Contains(t, string(closed.Payload), "Cannot start session")
		})
	}
}

func TestSessionDataChannelBadToken(t *testing.T) {
	t.Parallel()

	srv := newServer(t, true)
	c, _, _ := dial(t, srv, "i-0123")
	c.open("wrong")

	_, _, err := c.ws.ReadMessage()
	assert.True(t, websocket.IsCloseError(err, websocket.ClosePolicyViolation), "%v", err)
}
