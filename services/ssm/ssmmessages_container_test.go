package ssm_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

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
