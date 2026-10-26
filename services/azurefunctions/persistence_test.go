package azurefunctions_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t, false)
	src.backend.PutApp("keep", azurefunctions.AppConfig{Kind: "functionapp,linux", LinuxFx: "PYTHON|3.12"})
	require.NoError(t, src.backend.SetSettings("keep", map[string]string{"A": "1"}))
	_, err := src.backend.SetHostKey("keep", azurefunctions.KeyTypeFunction, "ci", "ci-key==")
	require.NoError(t, err)
	_, err = src.backend.FunctionKeys("keep", "hello")
	require.NoError(t, err)
	_, err = src.backend.Deploy("keep", helloZip(t), "ZipDeploy", "m")
	require.NoError(t, err)
	require.NoError(t, src.backend.SetStopped("keep", true))

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newEnv(t, true)
	dst.backend.PutApp("stale", azurefunctions.AppConfig{})
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	assert.Equal(t, []string{"keep"}, dst.backend.Names())

	wantKeys, err := src.backend.HostKeys("keep")
	require.NoError(t, err)
	gotKeys, err := dst.backend.HostKeys("keep")
	require.NoError(t, err)
	assert.Equal(t, wantKeys, gotKeys)

	settings, err := dst.backend.Settings("keep")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"A": "1"}, settings)

	assert.Equal(t, src.backend.Package("keep"), dst.backend.Package("keep"))

	ds, err := dst.backend.Deployments("keep")
	require.NoError(t, err)
	assert.Len(t, ds, 1)

	info, ok := dst.backend.App("keep")
	require.True(t, ok)
	assert.True(t, info.Stopped)
	assert.True(t, info.Deployed)
	assert.Equal(t, "mcr.microsoft.com/azure-functions/python:4-python3.12", info.Image)

	wantFn, err := src.backend.FunctionKeys("keep", "hello")
	require.NoError(t, err)
	gotFn, err := dst.backend.FunctionKeys("keep", "hello")
	require.NoError(t, err)
	assert.Equal(t, wantFn, gotFn)

	require.NoError(t, dst.backend.SetStopped("keep", false))

	resp, _ := dst.do(t, http.MethodGet, "keep."+testDomain, "/api/greet", nil, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "a restored app serves again from its restored package")
}

func TestPersistence_IncompatibleVersionStartsEmpty(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.backend.PutApp("a", azurefunctions.AppConfig{})
	require.NoError(t, e.handler.Restore(t.Context(), []byte(`{"version":999}`)))
	assert.Empty(t, e.backend.Names())
}
