package dashboard_test

import (
	"bytes"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

func compressedGet(tb testing.TB, h echo.HandlerFunc, p, ae string) *httptest.ResponseRecorder {
	tb.Helper()

	req := httptest.NewRequest(http.MethodGet, p, nil)
	if ae != "" {
		req.Header.Set("Accept-Encoding", ae)
	}

	rec := httptest.NewRecorder()
	require.NoError(tb, h(echo.New().NewContext(req, rec)))

	return rec
}

func decodeBody(tb testing.TB, enc string, b []byte) []byte {
	tb.Helper()

	var (
		r   io.Reader
		err error
	)

	switch enc {
	case "br":
		r = brotli.NewReader(bytes.NewReader(b))
	case "gzip":
		r, err = gzip.NewReader(bytes.NewReader(b))
	case "zstd":
		var zr *zstd.Decoder

		zr, err = zstd.NewReader(bytes.NewReader(b))
		if err == nil {
			defer zr.Close()
		}

		r = zr
	default:
		return b
	}

	require.NoError(tb, err)

	out, err := io.ReadAll(r)
	require.NoError(tb, err)

	return out
}

func TestDashboardCompression(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	wrapped := dashboard.CompressionMiddleware()(h.Handler())
	plain := compressedGet(t, h.Handler(), "/dashboard/static/app.js", "")
	require.Equal(t, http.StatusOK, plain.Code)

	tests := []struct {
		name    string
		ae      string
		wantEnc string
	}{
		{name: "browser prefers zstd", ae: "gzip, deflate, br, zstd", wantEnc: "zstd"},
		{name: "br", ae: "br, gzip", wantEnc: "br"},
		{name: "gzip", ae: "gzip", wantEnc: "gzip"},
		{name: "identity", ae: "identity", wantEnc: ""},
		{name: "none", ae: "", wantEnc: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for range 2 {
				rec := compressedGet(t, wrapped, "/dashboard/static/app.js", tt.ae)

				assert.Equal(t, tt.wantEnc, rec.Header().Get("Content-Encoding"))
				assert.Contains(t, rec.Header().Values("Vary"), "Accept-Encoding")
				assert.Equal(t, plain.Body.Bytes(), decodeBody(t, tt.wantEnc, rec.Body.Bytes()))

				if tt.wantEnc != "" {
					assert.Less(t, rec.Body.Len(), plain.Body.Len())
				}
			}
		})
	}
}

func TestDashboardCompressionJSONAPI(t *testing.T) {
	t.Parallel()

	rows := make([]map[string]string, 300)
	for i := range rows {
		rows[i] = map[string]string{"name": "resource-" + strings.Repeat("x", i%17), "state": "ACTIVE"}
	}

	mw := dashboard.CompressionMiddleware()(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, rows)
	})

	rec := compressedGet(t, mw, "/dashboard/api/anything", "gzip")
	plain := compressedGet(t, mw, "/dashboard/api/anything", "")

	assert.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	assert.Less(t, rec.Body.Len(), plain.Body.Len()/3)
	assert.Equal(t, plain.Body.Bytes(), decodeBody(t, "gzip", rec.Body.Bytes()))
}

func TestDashboardCompressionConnectStreamingSkipped(t *testing.T) {
	t.Parallel()

	body := strings.Repeat(`{"k":"streaming frames are never compressed by the middleware"}`, 200)
	mw := dashboard.CompressionMiddleware()(func(c *echo.Context) error {
		c.Response().Header().Set("Content-Type", "application/connect+json")

		return c.String(http.StatusOK, body)
	})
	rec := compressedGet(t, mw, "/dashboard.v1.DashboardService/StreamConsole", "gzip, br, zstd")

	assert.Empty(t, rec.Header().Get("Content-Encoding"))
	assert.Equal(t, body, rec.Body.String())
}

func TestDashboardAssetSavings(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	wrapped := dashboard.CompressionMiddleware()(h.Handler())

	type tally struct{ raw, zstd, br, gzip int }

	byExt := map[string]*tally{}

	err := fs.WalkDir(os.DirFS("static"), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || path.Base(p) == ".keep" {
			return err
		}

		url := "/dashboard/static/" + p
		if rest, ok := strings.CutPrefix(p, "spa/"); ok {
			url = "/dashboard/" + rest
		}

		raw := compressedGet(t, h.Handler(), url, "")
		if raw.Code != http.StatusOK {
			return nil
		}

		ext := path.Ext(p)
		if byExt[ext] == nil {
			byExt[ext] = &tally{}
		}

		ty := byExt[ext]
		ty.raw += raw.Body.Len()
		ty.zstd += compressedGet(t, wrapped, url, "zstd").Body.Len()
		ty.br += compressedGet(t, wrapped, url, "br").Body.Len()
		ty.gzip += compressedGet(t, wrapped, url, "gzip").Body.Len()

		return nil
	})
	require.NoError(t, err)

	for ext, ty := range byExt {
		t.Logf("%-6s raw=%9d zstd=%9d (%.0f%%) br=%9d (%.0f%%) gzip=%9d (%.0f%%)", ext, ty.raw,
			ty.zstd, 100*float64(ty.zstd)/float64(ty.raw), ty.br, 100*float64(ty.br)/float64(ty.raw),
			ty.gzip, 100*float64(ty.gzip)/float64(ty.raw))

		if ext == ".js" || ext == ".css" {
			assert.Less(t, ty.br, ty.raw/2)
		}
	}
}
