package azurearm_test

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestACIExecAndAttach(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	_, err := s.create(t, "rg1", "grp1", aciOpts{policy: armcontainerinstance.ContainerGroupRestartPolicyAlways})
	require.NoError(t, err)
	s.waitState(t, "rg1", "grp1", "Running")

	cc := s.factory.NewContainersClient()
	dialer := websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}

	exec, err := cc.ExecuteCommand(t.Context(), "rg1", "grp1", "main",
		armcontainerinstance.ContainerExecRequest{Command: new("echo hi")}, nil)
	require.NoError(t, err)

	conn, _, err := dialer.DialContext(t.Context(), *exec.WebSocketURI, nil)
	require.NoError(t, err)

	defer conn.Close()

	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(*exec.Password)))

	_, msg, err := conn.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, "ran:/bin/sh -c echo hi", string(msg))

	_, _, err = conn.ReadMessage()
	assert.True(t, websocket.IsCloseError(err, websocket.CloseNormalClosure))

	bad, err := cc.ExecuteCommand(t.Context(), "rg1", "grp1", "main",
		armcontainerinstance.ContainerExecRequest{Command: new("id")}, nil)
	require.NoError(t, err)

	wrong, _, err := dialer.DialContext(t.Context(), *bad.WebSocketURI, nil)
	require.NoError(t, err)

	defer wrong.Close()

	require.NoError(t, wrong.WriteMessage(websocket.TextMessage, []byte("nope")))
	_, _, err = wrong.ReadMessage()
	assert.True(t, websocket.IsCloseError(err, websocket.ClosePolicyViolation))

	att, err := cc.Attach(t.Context(), "rg1", "grp1", "main", nil)
	require.NoError(t, err)

	_, resp, err := dialer.DialContext(t.Context(), *att.WebSocketURI, http.Header{"Authorization": {"wrong"}})
	require.Error(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	_, err = cc.ExecuteCommand(t.Context(), "rg1", "grp1", "missing",
		armcontainerinstance.ContainerExecRequest{Command: new("id")}, nil)
	require.Error(t, err)
}

func TestACIExecStdinAndTerminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		term      *armcontainerinstance.ContainerExecRequestTerminalSize
		name      string
		command   string
		stdin     string
		wantFirst string
		wantTTY   string
		wantNext  string
	}{
		{name: "plain", command: "echo hi", wantFirst: "ran:/bin/sh -c echo hi"},
		{
			name: "terminal", command: "echo hi", wantFirst: "ran:/bin/sh -c echo hi", wantTTY: " tty=24x80",
			term: &armcontainerinstance.ContainerExecRequestTerminalSize{Rows: new(int32(24)), Cols: new(int32(80))},
		},
		{
			name: "stdin relay", command: "cat", stdin: "hello",
			wantFirst: "ran:/bin/sh -c cat", wantNext: "stdin:hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			_, err := s.create(
				t,
				"rg1",
				"grp1",
				aciOpts{policy: armcontainerinstance.ContainerGroupRestartPolicyAlways},
			)
			require.NoError(t, err)
			s.waitState(t, "rg1", "grp1", "Running")

			exec, err := s.factory.NewContainersClient().ExecuteCommand(t.Context(), "rg1", "grp1", "main",
				armcontainerinstance.ContainerExecRequest{Command: new(tt.command), TerminalSize: tt.term}, nil)
			require.NoError(t, err)

			dialer := websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
			conn, _, err := dialer.DialContext(t.Context(), *exec.WebSocketURI, nil)
			require.NoError(t, err)

			defer conn.Close()

			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(*exec.Password)))

			_, msg, err := conn.ReadMessage()
			require.NoError(t, err)
			assert.Equal(t, tt.wantFirst, string(msg))

			if tt.wantTTY != "" {
				_, msg, err = conn.ReadMessage()
				require.NoError(t, err)
				assert.Equal(t, tt.wantTTY, string(msg))
			}

			if tt.stdin != "" {
				require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(tt.stdin)))

				_, msg, err = conn.ReadMessage()
				require.NoError(t, err)
				assert.Equal(t, tt.wantNext, string(msg))

				require.NoError(t, conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")))
			}

			_, _, err = conn.ReadMessage()
			assert.True(t, websocket.IsCloseError(err, websocket.CloseNormalClosure))
		})
	}
}

func TestACIExecRejectsBadTerminal(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	_, err := s.create(t, "rg1", "grp1", aciOpts{policy: armcontainerinstance.ContainerGroupRestartPolicyAlways})
	require.NoError(t, err)
	s.waitState(t, "rg1", "grp1", "Running")

	_, err = s.factory.NewContainersClient().ExecuteCommand(t.Context(), "rg1", "grp1", "main",
		armcontainerinstance.ContainerExecRequest{
			Command: new("id"),
			TerminalSize: &armcontainerinstance.ContainerExecRequestTerminalSize{
				Rows: new(int32(0)),
				Cols: new(int32(80)),
			},
		}, nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidRequestContent", sdkErrCode(err))
}

func TestACIAttachFollowsLiveOutput(t *testing.T) {
	t.Parallel()

	rt := newFakeACIRuntime()
	release := make(chan struct{})
	rt.follow = func(ctx context.Context, out io.Writer) error {
		_, _ = io.WriteString(out, "live-1")

		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}

		_, err := io.WriteString(out, "live-2")

		return err
	}

	s := newACIStackWith(t, rt)
	_, err := s.create(t, "rg1", "grp1", aciOpts{policy: armcontainerinstance.ContainerGroupRestartPolicyAlways})
	require.NoError(t, err)
	s.waitState(t, "rg1", "grp1", "Running")

	att, err := s.factory.NewContainersClient().Attach(t.Context(), "rg1", "grp1", "main", nil)
	require.NoError(t, err)

	dialer := websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	conn, _, err := dialer.DialContext(t.Context(), *att.WebSocketURI, http.Header{"Authorization": {*att.Password}})
	require.NoError(t, err)

	defer conn.Close()

	_, msg, err := conn.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, "live-1", string(msg))

	close(release)

	_, msg, err = conn.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, "live-2", string(msg))

	_, _, err = conn.ReadMessage()
	assert.True(t, websocket.IsCloseError(err, websocket.CloseNormalClosure))
}
