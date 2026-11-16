package azurearm_test

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type deadlineConn struct {
	net.Conn
	set *atomic.Int64
}

func (c deadlineConn) SetWriteDeadline(t time.Time) error {
	if !t.IsZero() {
		c.set.Add(1)
	}

	return c.Conn.SetWriteDeadline(t)
}

type deadlineListener struct {
	net.Listener
	set *atomic.Int64
}

func (l deadlineListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	return deadlineConn{Conn: c, set: l.set}, nil
}

func TestExecFrameWriteHasDeadline(t *testing.T) {
	t.Parallel()

	var set atomic.Int64

	wrote := make(chan error, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			wrote <- err

			return
		}

		defer conn.Close()

		wrote <- azurearm.WriteWSFrameForTest(conn, []byte("out"))
	}))
	srv.Listener = deadlineListener{Listener: srv.Listener, set: &set}
	srv.Start()
	t.Cleanup(srv.Close)

	c, _, err := websocket.DefaultDialer.DialContext(t.Context(), "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)

	defer c.Close()

	require.NoError(t, <-wrote)
	assert.Positive(t, set.Load(), "a stalled client must not block the output stream forever")
}
