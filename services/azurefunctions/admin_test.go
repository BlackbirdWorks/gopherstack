package azurefunctions_test

import (
	"archive/zip"
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func zipNames(t *testing.T, data []byte) []string {
	t.Helper()

	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	out := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		out = append(out, f.Name)
	}

	return out
}

func TestDeleteFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		fn      string
		keep    bool
	}{
		{nil, "removes the function directory", "hello", false},
		{azurefunctions.ErrFunctionNotFound, "unknown function", "ghost", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
			_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
			require.NoError(t, err)

			_, err = e.backend.FunctionKeys("app", tt.fn)
			require.NoError(t, err)

			err = e.backend.DeleteFunction(t.Context(), "app", tt.fn)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Contains(t, zipNames(t, e.backend.Package("app")), "hello/function.json")

				return
			}

			require.NoError(t, err)

			for _, n := range zipNames(t, e.backend.Package("app")) {
				assert.False(t, strings.HasPrefix(n, tt.fn+"/"), n)
			}

			assert.Contains(t, zipNames(t, e.backend.Package("app")), "host.json")

			_, err = e.backend.Function(t.Context(), "app", tt.fn)
			require.ErrorIs(t, err, azurefunctions.ErrFunctionNotFound)

			keys, err := e.backend.FunctionKeys("app", tt.fn)
			require.NoError(t, err)
			assert.Len(t, keys, 1, "the deleted function's keys were dropped and a fresh default key was minted")
		})
	}

	t.Run("code defined function", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, true)
		e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
		_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
		require.NoError(t, err)

		err = e.backend.DeleteFunction(t.Context(), "app", "tick")
		require.NoError(t, err, "tick has a function.json in the package")

		_, err = e.backend.Deploy("app", zipOf(t, map[string]string{"host.json": "{}"}), "t", "m")
		require.NoError(t, err)

		err = e.backend.DeleteFunction(t.Context(), "app", "hello")
		require.ErrorIs(t, err, azurefunctions.ErrFunctionInCode, "the host reports hello but no file defines it")
	})
}

func TestKuduVFS(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
	_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
	require.NoError(t, err)

	auth := e.kuduAuth(t, "app")
	host := "app.scm." + testDomain

	resp, body := e.do(t, http.MethodGet, host, "/api/vfs/site/wwwroot/", nil, auth)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, body, `"name":"host.json"`)
	assert.Contains(t, body, `"mime":"inode/directory"`)

	resp, body = e.do(t, http.MethodGet, host, "/api/vfs/site/wwwroot/hello/index.js", nil, auth)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, body, "ctx.res")

	etag := resp.Header.Get("ETag")
	require.NotEmpty(t, etag)

	resp, _ = e.do(t, http.MethodPut, host, "/api/vfs/site/wwwroot/new/a.txt", []byte("one"), auth)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	resp, _ = e.do(t, http.MethodPut, host, "/api/vfs/site/wwwroot/new/a.txt", []byte("two"), auth)
	assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode, "overwrite needs If-Match")

	hdr := map[string]string{"If-Match": "*", "basic": auth["basic"]}
	resp, _ = e.do(t, http.MethodPut, host, "/api/vfs/site/wwwroot/new/a.txt", []byte("two"), hdr)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	_, body = e.do(t, http.MethodGet, host, "/api/vfs/site/wwwroot/new/a.txt", nil, auth)
	assert.Equal(t, "two", body)

	resp, _ = e.do(t, http.MethodPut, host, "/api/vfs/site/wwwroot/new/a.txt/b", []byte("x"), auth)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	resp, _ = e.do(t, http.MethodDelete, host, "/api/vfs/site/wwwroot/new/a.txt", nil, auth)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, _ = e.do(t, http.MethodGet, host, "/api/vfs/site/wwwroot/new/a.txt", nil, auth)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp, _ = e.do(t, http.MethodGet, host, "/api/vfs/site/other/", nil, auth)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp, _ = e.do(t, http.MethodGet, host, "/api/vfs/site/wwwroot/", nil, nil)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
