package azurestoragevhost_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefile"
	"github.com/blackbirdworks/gopherstack/services/azurestoragevhost"
)

func TestHandler_RoutesFileLabel(t *testing.T) {
	t.Parallel()

	files := azurefile.NewHandler(azurefile.NewInMemoryBackend())

	h := azurestoragevhost.NewHandler()
	h.File = files

	rec := doVHostRequest(t, h, http.MethodPut, testAccount+".file.localhost:10010", "/vhshare?restype=share", "")
	require.Equal(t, http.StatusCreated, rec.Code)

	tests := []struct {
		name       string
		method     string
		host       string
		path       string
		wantStatus int
	}{
		{
			name:       "list via file label",
			method:     http.MethodGet,
			host:       testAccount + ".file.localhost:10010",
			path:       "/?comp=list",
			wantStatus: http.StatusOK,
		},
		{
			name:       "unwired label",
			method:     http.MethodGet,
			host:       testAccount + ".blob.localhost:10010",
			path:       "/?comp=list",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := doVHostRequest(t, h, tt.method, tt.host, tt.path, "")
			assert.Equal(t, tt.wantStatus, got.Code)
		})
	}

	pathReq := httptest.NewRequest(http.MethodGet, "/"+testAccount+"/vhshare?restype=share", http.NoBody)
	pathReq.Host = "127.0.0.1:10009"

	e := echo.New()
	pathRec := httptest.NewRecorder()
	require.NoError(t, files.Handler()(e.NewContext(pathReq, pathRec)))
	assert.Equal(t, http.StatusOK, pathRec.Code, "the share created by host is visible by path")
}
