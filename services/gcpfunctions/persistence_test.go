package gcpfunctions_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t, true)
	fn := src.createFunction(t, "keep", fnSpec(src.upload(t, zipOf(t, nodeFilesFn()))))
	snap := src.handler.Snapshot(t.Context())

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.client.GetFunction(t.Context(), &functionspb.GetFunctionRequest{Name: fn.GetName()})
	require.NoError(t, err)
	assert.Equal(t, functionspb.Function_ACTIVE, got.GetState())
	assert.Equal(t, fn.GetServiceConfig().GetRevision(), got.GetServiceConfig().GetRevision())

	// The source archive survives, so the restored function serves from a fresh container.
	code, body := dst.call(t, http.MethodGet, hostOf(got), "/", "")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "hi", decode(t, body).Env["GREETING"])

	dl, err := dst.client.GenerateDownloadUrl(t.Context(), &functionspb.GenerateDownloadUrlRequest{Name: fn.GetName()})
	require.NoError(t, err)
	assert.NotEmpty(t, dl.GetDownloadUrl())

	snap2 := dst.handler.Snapshot(t.Context())
	assert.NotEmpty(t, snap2)
}

func TestPersistence_RestoreReplacesAndRejectsCorrupt(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.createFunction(t, "old", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	empty := newEnv(t, true).handler.Snapshot(t.Context())
	require.NoError(t, e.handler.Restore(t.Context(), empty))

	_, err := e.client.GetFunction(t.Context(), &functionspb.GetFunctionRequest{Name: parent + "/functions/old"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	require.Error(t, e.handler.Restore(t.Context(), []byte("{not json")))
}

func TestReset(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "gone", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))
	_, _ = e.call(t, http.MethodGet, hostOf(fn), "/", "")
	require.Equal(t, 1, e.rt.running())

	e.handler.Reset()
	assert.Equal(t, 0, e.rt.running())

	_, err := e.client.GetFunction(t.Context(), &functionspb.GetFunctionRequest{Name: fn.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
