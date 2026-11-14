package amqp1_test

import (
	"bufio"
	"context"
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

type oneShotHandler struct{}

func (oneShotHandler) Authenticate(amqp1.Symbol, string, string) bool { return true }

func (oneShotHandler) NewConn() amqp1.ConnHandler { return oneShotConn{} }

type oneShotConn struct{}

func (oneShotConn) Close() {}

func (oneShotConn) AttachSender(*amqp1.Attach) (amqp1.SenderEndpoint, *amqp1.Error) {
	return &oneShotSender{sent: make(chan struct{}, 1)}, nil
}

func (oneShotConn) AttachReceiver(*amqp1.Attach) (amqp1.ReceiverEndpoint, *amqp1.Error) {
	return nil, &amqp1.Error{Condition: amqp1.CondNotImplemented}
}

type oneShotSender struct{ sent chan struct{} }

func (s *oneShotSender) Next(ctx context.Context) (*amqp1.Outgoing, error) {
	select {
	case s.sent <- struct{}{}:
		return &amqp1.Outgoing{Tag: []byte{1}, Payload: []byte("payload")}, nil
	default:
	}

	<-ctx.Done()

	return nil, ctx.Err()
}

func (*oneShotSender) Settle(*amqp1.Outgoing, amqp1.DeliveryState) {}
func (*oneShotSender) Release(*amqp1.Outgoing)                     {}
func (*oneShotSender) Close()                                      {}

func TestServer_SenderHonoursHugePeerWindows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		window uint32
		credit uint32
	}{
		{"small", 100, 10},
		{"max incoming window", math.MaxUint32, 10},
		{"max window and credit", math.MaxUint32, math.MaxUint32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var lc net.ListenConfig

			ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)

			srv := amqp1.NewServer(amqp1.Config{Handler: oneShotHandler{}, MaxMessageSize: 4096, MaxFrameSize: 1024})

			go func() { _ = srv.Serve(ln, false) }()

			t.Cleanup(srv.Close)

			var d net.Dialer

			nc, err := d.DialContext(t.Context(), "tcp", ln.Addr().String())
			require.NoError(t, err)
			t.Cleanup(func() { _ = nc.Close() })
			require.NoError(t, nc.SetDeadline(time.Now().Add(10*time.Second)))

			c := &rawClient{t: t, nc: nc, r: bufio.NewReader(nc)}

			_, err = nc.Write([]byte(amqp1.HeaderAMQP))
			require.NoError(t, err)

			hdr := make([]byte, 8)
			_, err = c.r.Read(hdr)
			require.NoError(t, err)

			c.send(&amqp1.Open{ContainerID: "c", MaxFrameSize: 1024}, nil)
			require.IsType(t, &amqp1.Open{}, c.recv())

			c.send(&amqp1.Begin{IncomingWindow: tt.window, OutgoingWindow: 100, HandleMax: 10}, nil)
			require.IsType(t, &amqp1.Begin{}, c.recv())

			c.send(&amqp1.Attach{Name: "l", Handle: 0, Role: true, Source: &amqp1.Source{Address: "q"},
				Target: &amqp1.Target{}}, nil)
			require.IsType(t, &amqp1.Attach{}, c.recv())

			zero, handle := uint32(0), uint32(0)
			c.send(&amqp1.Flow{
				NextIncomingID: &zero, IncomingWindow: tt.window, NextOutgoingID: 1, OutgoingWindow: 100,
				Handle: &handle, DeliveryCount: &zero, LinkCredit: &tt.credit,
			}, nil)

			tr, ok := c.recv().(*amqp1.Transfer)
			require.True(t, ok, "the message is delivered")
			assert.Equal(t, uint32(0), handle)
			assert.NotNil(t, tr.DeliveryID)
		})
	}
}
