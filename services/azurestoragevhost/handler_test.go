package azurestoragevhost_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azurequeue"
	"github.com/blackbirdworks/gopherstack/services/azurestoragevhost"
	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

const testAccount = "gopherstackm8data"

func doVHostRequest(
	t *testing.T,
	h *azurestoragevhost.Handler,
	method, host, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody io.Reader = http.NoBody
	if body != "" {
		reqBody = strings.NewReader(body)
	}

	req := httptest.NewRequest(method, path, reqBody)
	req.Host = host

	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

func TestHandler_RoutesByHostLabel(t *testing.T) {
	t.Parallel()

	blob := azureblob.NewHandler(azureblob.NewInMemoryBackend())
	queue := azurequeue.NewHandler(azurequeue.NewInMemoryBackend())
	table := azuretable.NewHandler(azuretable.NewInMemoryBackend())

	h := azurestoragevhost.NewHandler()
	h.Blob = blob
	h.Queue = queue
	h.Table = table

	// Create a container through the vhost listener...
	rec := doVHostRequest(t, h, http.MethodPut, testAccount+".blob.localhost:10010",
		"/m8-container?restype=container", "")
	require.Equal(t, http.StatusCreated, rec.Code)

	// ...and confirm it's visible through the real azureblob.Handler's own
	// path-style listener directly -- proving the vhost layer shares state
	// rather than duplicating it.
	pathReq := httptest.NewRequest(http.MethodGet, "/"+testAccount+"?comp=list", http.NoBody)
	e := echo.New()
	pathRec := httptest.NewRecorder()
	c := e.NewContext(pathReq, pathRec)
	require.NoError(t, blob.Handler()(c))
	assert.Contains(t, pathRec.Body.String(), "m8-container")

	// Create a queue through the vhost listener.
	rec = doVHostRequest(t, h, http.MethodPut, testAccount+".queue.localhost:10010", "/m8-queue", "")
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create a table through the vhost listener.
	rec = doVHostRequest(t, h, http.MethodPost, testAccount+".table.localhost:10010", "/Tables",
		`{"TableName":"m8table"}`)
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestHandler_UnrecognizedHostIs400(t *testing.T) {
	t.Parallel()

	h := azurestoragevhost.NewHandler()

	tests := []struct {
		name string
		host string
	}{
		{"no service label", "gopherstackm8data.localhost:10010"},
		{"unknown service label", "gopherstackm8data.file.localhost:10010"},
		{"empty account", ".blob.localhost:10010"},
		{"bare host", "localhost:10010"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doVHostRequest(t, h, http.MethodGet, tt.host, "/", "")
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_UnwiredServiceIs400(t *testing.T) {
	t.Parallel()

	h := azurestoragevhost.NewHandler()

	rec := doVHostRequest(t, h, http.MethodGet, testAccount+".blob.localhost:10010", "/", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := azurestoragevhost.NewHandler()
	assert.Equal(t, "AzureStorageVHost", h.Name())
}

func TestHandler_ExtractOperationAndResource(t *testing.T) {
	t.Parallel()

	h := azurestoragevhost.NewHandler()
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Host = testAccount + ".blob.localhost:10010"

	e := echo.New()
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "VHostBlob", h.ExtractOperation(c))
	assert.Equal(t, testAccount, h.ExtractResource(c))
}

func TestHandler_MatchPriorityAndRouteMatcher(t *testing.T) {
	t.Parallel()

	h := azurestoragevhost.NewHandler()
	assert.Equal(t, 0, h.MatchPriority())
	assert.False(t, h.RouteMatcher()(nil))
}

func TestHandler_ResetIsNoop(t *testing.T) {
	t.Parallel()

	h := azurestoragevhost.NewHandler()
	h.Reset() // must not panic
}
