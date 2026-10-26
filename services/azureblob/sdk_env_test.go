package azureblob_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func clockStart() time.Time { return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC) }

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(clockStart().UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type sdkEnv struct {
	svc     *azblob.Client
	cc      *container.Client
	clock   *fakeClock
	backend *azureblob.InMemoryBackend
	url     string
}

func newSDKEnv(t *testing.T) *sdkEnv {
	t.Helper()

	clock := newFakeClock()
	backend := azureblob.NewInMemoryBackendWithClock(clock.Now)

	e := echo.New()
	e.Any("/*", azureblob.NewHandler(backend).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cred, err := azblob.NewSharedKeyCredential(testAccount, sdkAccountKey)
	require.NoError(t, err)

	base := srv.URL + "/" + testAccount

	svc, err := azblob.NewClientWithSharedKeyCredential(base+"/", cred, nil)
	require.NoError(t, err)

	cc, err := container.NewClientWithSharedKeyCredential(base+"/sdkenv", cred, nil)
	require.NoError(t, err)

	_, err = cc.Create(t.Context(), nil)
	require.NoError(t, err)

	return &sdkEnv{svc: svc, cc: cc, clock: clock, backend: backend, url: base}
}

func body(s string) io.ReadSeekCloser { return streaming.NopCloser(strings.NewReader(s)) }

func requireStorageErr(t *testing.T, err error, code bloberror.Code, status int) {
	t.Helper()

	require.Error(t, err)
	assert.True(t, bloberror.HasCode(err, code), "want %s, got %v", code, err)

	var re *azcore.ResponseError
	require.ErrorAs(t, err, &re)
	assert.Equal(t, status, re.StatusCode)
}

func requireStatus(t *testing.T, err error, status int) {
	t.Helper()

	var re *azcore.ResponseError

	require.ErrorAs(t, err, &re)
	assert.Equal(t, status, re.StatusCode)
}

func newRestored(t *testing.T, env *sdkEnv, snap []byte) *azureblob.InMemoryBackend {
	t.Helper()

	restored := azureblob.NewInMemoryBackendWithClock(env.clock.Now)
	require.NoError(t, restored.Restore(t.Context(), snap))

	return restored
}
