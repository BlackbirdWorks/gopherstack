package ssm_test

import (
	"context"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

type commandOpener struct {
	fakeOpener

	container string
	cmd       []string
	mu        sync.Mutex
}

func (o *commandOpener) OpenCommand(_ context.Context, id string, cmd []string, _, _ uint) (ssm.Shell, error) {
	o.mu.Lock()
	o.container, o.cmd = id, cmd
	o.mu.Unlock()

	return &fakeShell{out: make(chan []byte, 8)}, nil
}

func TestContainerSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		opener  ssm.ShellOpener
		wantErr string
		name    string
		base    string
	}{
		{name: "streams_command", opener: &commandOpener{}, base: "ws://"},
		{name: "no_opener", wantErr: "no shell backend"},
		{name: "opener_without_commands", opener: fakeOpener{}, wantErr: "no shell backend"},
		{name: "no_base", opener: &commandOpener{}, base: "-", wantErr: "no host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newTestHandler(t)
			e := echo.New()
			e.Any("/*", h.Handler())

			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			if tt.opener != nil {
				bk.SetShellOpener(tt.opener)
			}

			base := ""
			if tt.base == "ws://" {
				base = "ws://" + strings.TrimPrefix(srv.URL, "http://")
			}

			out, err := bk.StartContainerSession(t.Context(), ssm.ContainerSessionInput{
				Target: "ecs:c_t_r", ContainerID: "cid-1", Command: "echo hi", StreamBase: base,
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			ws, _, err := websocket.DefaultDialer.DialContext(t.Context(), out.StreamURL, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ws.Close() })

			c := &dcClient{t: t, ws: ws}
			c.open(out.TokenValue)
			c.handshake(map[int64]bool{})
			c.input(ssm.PayloadOutput, []byte("echo ok\n"))

			got := c.nextNonAck(map[int64]bool{})
			assert.Equal(t, "ok\n", string(got.Payload))

			op, isOpener := tt.opener.(*commandOpener)
			require.True(t, isOpener)
			op.mu.Lock()
			defer op.mu.Unlock()
			assert.Equal(t, "cid-1", op.container)
			assert.Equal(t, []string{"sh", "-c", "echo hi"}, op.cmd)
		})
	}
}

type transcript struct {
	id   string
	body []byte
}

func TestContainerSessionTranscript(t *testing.T) {
	t.Parallel()

	tests := []struct {
		end     func(t *testing.T, c *dcClient, bk *ssm.InMemoryBackend, sid string, stop context.CancelFunc)
		name    string
		want    string
		wantMsg bool
	}{
		{name: "exit delivers output", want: "ok\n", wantMsg: true,
			end: func(_ *testing.T, c *dcClient, _ *ssm.InMemoryBackend, _ string, _ context.CancelFunc) {
				c.input(ssm.PayloadOutput, []byte("exit\n"))
			}},
		{name: "terminate delivers output", want: "ok\n", wantMsg: true,
			end: func(t *testing.T, _ *dcClient, bk *ssm.InMemoryBackend, sid string, _ context.CancelFunc) {
				t.Helper()

				_, err := bk.TerminateSession(t.Context(), &ssm.TerminateSessionInput{SessionID: sid})
				assert.NoError(t, err)
			}},
		{name: "client disconnect delivers output", want: "ok\n",
			end: func(_ *testing.T, c *dcClient, _ *ssm.InMemoryBackend, _ string, _ context.CancelFunc) {
				_ = c.ws.Close()
			}},
		{name: "server shutdown delivers output", want: "ok\n", wantMsg: true,
			end: func(_ *testing.T, _ *dcClient, _ *ssm.InMemoryBackend, _ string, stop context.CancelFunc) {
				stop()
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newTestHandler(t)
			e := echo.New()
			e.Any("/*", h.Handler())

			base, stop := context.WithCancel(t.Context())
			defer stop()

			srv := httptest.NewUnstartedServer(e)
			srv.Config.BaseContext = func(net.Listener) context.Context { return base }
			srv.Start()
			t.Cleanup(srv.Close)
			bk.SetShellOpener(&commandOpener{})

			var calls atomic.Int32

			done := make(chan transcript, 2)
			in := ssm.ContainerSessionInput{
				Target: "ecs:c_t_r", ContainerID: "cid-1", Command: "x",
				StreamBase: "ws://" + strings.TrimPrefix(srv.URL, "http://"),
			}

			in.OnEnd = func(id string, body []byte) {
				calls.Add(1)
				done <- transcript{id: id, body: body}
			}

			out, err := bk.StartContainerSession(t.Context(), in)
			require.NoError(t, err)

			ws, _, err := websocket.DefaultDialer.DialContext(t.Context(), out.StreamURL, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ws.Close() })

			c := &dcClient{t: t, ws: ws}
			c.open(out.TokenValue)
			c.handshake(map[int64]bool{})
			c.input(ssm.PayloadOutput, []byte("echo ok\n"))
			assert.Equal(t, "ok\n", string(c.nextNonAck(map[int64]bool{}).Payload))
			tt.end(t, c, bk, out.SessionID, stop)

			if tt.wantMsg {
				assert.Equal(t, ssm.MsgChannelClosed, c.nextNonAck(map[int64]bool{}).MessageType)
			}

			select {
			case got := <-done:
				assert.Equal(t, out.SessionID, got.id)
				assert.Equal(t, tt.want, string(got.body))
				assert.Equal(t, int32(1), calls.Load())
			case <-time.After(10 * time.Second):
				require.FailNow(t, "OnEnd was never called")
			}
		})
	}
}
