package amqp1_test

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

type fakeHandler struct{ got chan []byte }

func (h *fakeHandler) Authenticate(mech amqp1.Symbol, _, pass string) bool {
	return mech == "ANONYMOUS" || pass == "ok"
}

func (h *fakeHandler) NewConn() amqp1.ConnHandler { return &fakeConn{got: h.got} }

type fakeConn struct{ got chan []byte }

func (*fakeConn) Close() {}

func (*fakeConn) AttachSender(*amqp1.Attach) (amqp1.SenderEndpoint, *amqp1.Error) {
	return nil, &amqp1.Error{Condition: amqp1.CondNotImplemented}
}

func (c *fakeConn) AttachReceiver(*amqp1.Attach) (amqp1.ReceiverEndpoint, *amqp1.Error) {
	return &fakeSink{got: c.got}, nil
}

type fakeSink struct{ got chan []byte }

func (*fakeSink) Close() {}

func (s *fakeSink) Deliver(_ context.Context, in *amqp1.Incoming) amqp1.DeliveryState {
	s.got <- in.Payload

	return amqp1.DeliveryState{Kind: amqp1.StateAccepted}
}

type rawClient struct {
	t  *testing.T
	nc net.Conn
	r  *bufio.Reader
}

func startServer(t *testing.T, maxMsg uint64) (*rawClient, chan []byte) {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	got := make(chan []byte, 4)
	srv := amqp1.NewServer(amqp1.Config{Handler: &fakeHandler{got: got}, MaxMessageSize: maxMsg, MaxFrameSize: 1024})

	go func() { _ = srv.Serve(ln, false) }()

	t.Cleanup(srv.Close)

	var d net.Dialer

	nc, err := d.DialContext(t.Context(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Close() })
	require.NoError(t, nc.SetDeadline(time.Now().Add(10*time.Second)))

	return &rawClient{t: t, nc: nc, r: bufio.NewReader(nc)}, got
}

func (c *rawClient) send(p amqp1.Performative, payload []byte) {
	c.t.Helper()

	body, err := amqp1.EncodePerformative(p)
	require.NoError(c.t, err)

	_, err = c.nc.Write(amqp1.AppendFrame(nil, amqp1.Frame{Body: append(body, payload...)}))
	require.NoError(c.t, err)
}

func (c *rawClient) recv() amqp1.Performative {
	c.t.Helper()

	f, err := amqp1.ReadFrame(c.r, 1<<20)
	require.NoError(c.t, err)

	p, _, err := amqp1.DecodePerformative(f.Body)
	require.NoError(c.t, err)

	return p
}

func (c *rawClient) openAndAttach() {
	c.t.Helper()

	_, err := c.nc.Write([]byte(amqp1.HeaderAMQP))
	require.NoError(c.t, err)

	hdr := make([]byte, 8)
	_, err = c.r.Read(hdr)
	require.NoError(c.t, err)

	c.send(&amqp1.Open{ContainerID: "c", MaxFrameSize: 1024}, nil)
	require.IsType(c.t, &amqp1.Open{}, c.recv())

	c.send(&amqp1.Begin{IncomingWindow: 100, OutgoingWindow: 100, HandleMax: 10}, nil)
	require.IsType(c.t, &amqp1.Begin{}, c.recv())

	c.send(&amqp1.Attach{Name: "l", Handle: 0, Role: false, Target: &amqp1.Target{Address: "q"},
		Source: &amqp1.Source{}}, nil)
	require.IsType(c.t, &amqp1.Attach{}, c.recv())
	require.IsType(c.t, &amqp1.Flow{}, c.recv())
}

func transfer(id uint32, more bool) *amqp1.Transfer {
	return &amqp1.Transfer{Handle: 0, DeliveryID: &id, DeliveryTag: []byte{1}, More: more}
}

func TestServer_MultiFrameTransfer(t *testing.T) {
	t.Parallel()

	c, got := startServer(t, 4096)
	c.openAndAttach()

	c.send(transfer(0, true), []byte("hello "))
	c.send(&amqp1.Transfer{Handle: 0}, []byte("world"))

	assert.Equal(t, "hello world", string((<-got)))

	d, ok := c.recv().(*amqp1.Disposition)
	require.True(t, ok)
	assert.True(t, d.Settled)
	assert.Equal(t, amqp1.StateAccepted, d.State.Kind)
}

func TestServer_OversizedMessageDetaches(t *testing.T) {
	t.Parallel()

	c, _ := startServer(t, 8)
	c.openAndAttach()

	c.send(transfer(0, false), make([]byte, 64))

	det, ok := c.recv().(*amqp1.Detach)
	require.True(t, ok)
	require.NotNil(t, det.Error)
	assert.Equal(t, amqp1.CondMessageTooLarge, det.Error.Condition)
}

func TestServer_RejectsOversizedFrame(t *testing.T) {
	t.Parallel()

	c, _ := startServer(t, 4096)

	_, err := c.nc.Write([]byte(amqp1.HeaderAMQP))
	require.NoError(t, err)

	hdr := make([]byte, 8)
	_, err = c.r.Read(hdr)
	require.NoError(t, err)

	c.send(&amqp1.Open{ContainerID: "c", MaxFrameSize: 1024}, nil)
	require.IsType(t, &amqp1.Open{}, c.recv())

	big := amqp1.AppendFrame(nil, amqp1.Frame{Body: make([]byte, 4000)})
	_, _ = c.nc.Write(big)

	_, err = amqp1.ReadFrame(c.r, 1<<20)
	require.Error(t, err, "server must drop the connection")
}

func TestServer_SASLPlain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pass     string
		wantCode uint8
	}{
		{name: "good credentials", pass: "ok", wantCode: amqp1.SASLOK},
		{name: "bad credentials", pass: "nope", wantCode: amqp1.SASLAuth},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, _ := startServer(t, 4096)

			_, err := c.nc.Write([]byte(amqp1.HeaderSASL))
			require.NoError(t, err)

			hdr := make([]byte, 8)
			_, err = c.r.Read(hdr)
			require.NoError(t, err)
			assert.Equal(t, amqp1.HeaderSASL, string(hdr))

			mechs, ok := c.recvSASL().(*amqp1.SASLMechanisms)
			require.True(t, ok)
			assert.Contains(t, mechs.Mechanisms, amqp1.Symbol("PLAIN"))

			c.sendSASL(&amqp1.SASLInit{Mechanism: "PLAIN", InitialResponse: []byte("\x00user\x00" + tt.pass)})

			out, ok := c.recvSASL().(*amqp1.SASLOutcome)
			require.True(t, ok)
			assert.Equal(t, tt.wantCode, out.Code)
		})
	}
}

func (c *rawClient) sendSASL(p amqp1.Performative) {
	c.t.Helper()

	body, err := amqp1.EncodePerformative(p)
	require.NoError(c.t, err)

	_, err = c.nc.Write(amqp1.AppendFrame(nil, amqp1.Frame{Type: amqp1.FrameSASL, Body: body}))
	require.NoError(c.t, err)
}

func (c *rawClient) recvSASL() amqp1.Performative { return c.recv() }

func TestMessageRoundTrip(t *testing.T) {
	t.Parallel()

	in := &amqp1.Message{
		Header: &amqp1.Header{Durable: true, DeliveryCount: 2, TTL: 5 * time.Second},
		Properties: &amqp1.Properties{
			MessageID:   "id",
			Subject:     "s",
			ContentType: "text/plain",
			GroupID:     "g",
		},
		Annotations:           amqp1.Map{amqp1.Symbol("x-opt-sequence-number"): int64(9)},
		ApplicationProperties: amqp1.Map{"k": "v"},
		Data:                  [][]byte{[]byte("body")},
	}

	b, err := in.Encode()
	require.NoError(t, err)

	out, err := amqp1.DecodeMessage(b)
	require.NoError(t, err)

	assert.Equal(t, in.Header, out.Header)
	assert.Equal(t, in.Properties.MessageID, out.Properties.MessageID)
	assert.Equal(t, in.Properties.Subject, out.Properties.Subject)
	assert.Equal(t, in.Annotations, out.Annotations)
	assert.Equal(t, in.ApplicationProperties, out.ApplicationProperties)
	assert.Equal(t, in.Data, out.Data)
}

func TestDecodeMessageRejectsGarbage(t *testing.T) {
	t.Parallel()

	_, err := amqp1.DecodeMessage([]byte{0x00, 0x53, 0x99, 0x45})
	require.Error(t, err)

	_, err = amqp1.DecodeMessage([]byte{0xff})
	require.Error(t, err)
}
