package azurearm_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIMGateway_WebSocketHopByHopHeaders(t *testing.T) {
	t.Parallel()

	e, be := newWSEnv(t)

	c, resp, err := e.wsDial(t, "/ws", map[string]string{
		"X-Token": "secret", "Proxy-Authorization": "Basic cHJveHk6cHc=", "Keep-Alive": "timeout=5",
		"Proxy-Connection": "keep-alive", "Te": "trailers",
	})
	require.NoError(t, err)

	defer resp.Body.Close()
	defer c.Close()

	h := <-be.seen
	for _, name := range []string{"Proxy-Authorization", "Keep-Alive", "Proxy-Connection", "Te"} {
		assert.Empty(t, h.Get(name), name)
	}

	assert.Equal(t, "secret", h.Get("X-Token"), "end-to-end headers are kept")
}

func TestAPIMGateway_WebSocketSlotsReleased(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		defer c.Close()

		for {
			if _, _, rerr := c.ReadMessage(); rerr != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	e.wsAPI(t, "ws", `"https","wss"`, "ws"+strings.TrimPrefix(srv.URL, "http"))

	for i := range 300 {
		c, resp, err := e.wsDial(t, "/ws", nil)
		require.NoError(t, err, "session %d", i)
		require.NoError(t, c.Close())
		require.NoError(t, resp.Body.Close())
	}
}
