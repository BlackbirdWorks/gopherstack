package azurearm_test

import (
	"crypto/tls"
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
