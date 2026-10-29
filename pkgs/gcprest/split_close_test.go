package gcprest_test

import (
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
)

func TestSplitterCloseReleasesUnsniffedConns(t *testing.T) {
	t.Parallel()

	srv := grpc.NewServer()
	longrunningpb.RegisterOperationsServer(srv, fakeOps{})

	var lc net.ListenConfig

	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = gcprest.ServeWithFallback(srv, l, nil)
	}()

	var d net.Dialer

	idle, err := d.DialContext(t.Context(), "tcp", l.Addr().String())
	require.NoError(t, err)

	t.Cleanup(func() { _ = idle.Close() })

	status, _ := do(t, http.MethodGet, "http://"+l.Addr().String()+"/v1/projects/p/locations/l/operations/op-1", "")
	require.Equal(t, http.StatusOK, status)

	srv.Stop()
	<-done

	require.NoError(t, idle.SetReadDeadline(time.Now().Add(5*time.Second)))

	_, readErr := idle.Read(make([]byte, 1))
	require.Error(t, readErr)

	if nerr, ok := errors.AsType[net.Error](readErr); ok {
		assert.False(t, nerr.Timeout(), "server kept the idle connection open after Close")
	}
}
