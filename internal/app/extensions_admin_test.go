package app

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/extensions"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestExtensionManagementRoutes(t *testing.T) {
	t.Parallel()

	cfgFile := filepath.Join(t.TempDir(), "ext.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte("extensions:\n  - name: t\n    command: [\"true\"]\n"), 0o600))

	e := echo.New()
	cli := &CLI{Port: ":1", Extensions: extensions.Settings{Config: cfgFile}}
	require.NoError(t, cli.initExtensions(logger.NewTestLogger(), e))

	type row struct {
		name, method, path, remote string
		want                       int
	}

	const (
		local = "127.0.0.1:1"
		base  = "/_gopherstack/extensions"
	)

	tests := []row{
		{"gopherstack list", http.MethodGet, base, local, http.StatusOK},
		{"localstack alias", http.MethodGet, "/_localstack/extensions", local, http.StatusOK},
		{"output", http.MethodGet, base + "/t/output", local, http.StatusOK},
		{"output unknown", http.MethodGet, base + "/x/output", local, http.StatusNotFound},
		{"restart unknown", http.MethodPost, base + "/x/restart", local, http.StatusNotFound},
		{"restart remote", http.MethodPost, base + "/t/restart", "8.8.8.8:1", http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.RemoteAddr = tt.remote
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

func TestLocalstackExtensionsAliasWithoutExtensions(t *testing.T) {
	t.Parallel()

	e := echo.New()
	registerLocalstackExtensionsAlias(e, nil)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_localstack/extensions", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
