package dashboard

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gzipBytes(t *testing.T, s string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	_, err := zw.Write([]byte(s))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestServeSPAFile(t *testing.T) {
	t.Parallel()

	const body = "console.log('hello');"

	fsys := fstest.MapFS{
		"app.js.gz": {Data: gzipBytes(t, body)},
		"plain.css": {Data: []byte("a{}")},
	}

	tests := []struct {
		name         string
		rel          string
		acceptEnc    string
		method       string
		wantBody     string
		wantEncoding string
		wantServed   bool
	}{
		{name: "gzip client gets raw gz", rel: "app.js", acceptEnc: "gzip, br", method: http.MethodGet, wantEncoding: "gzip", wantServed: true},
		{name: "no encoding inflated", rel: "app.js", acceptEnc: "", method: http.MethodGet, wantBody: body, wantServed: true},
		{name: "gzip q0 inflated", rel: "app.js", acceptEnc: "gzip;q=0", method: http.MethodGet, wantBody: body, wantServed: true},
		{name: "head has no body", rel: "app.js", acceptEnc: "", method: http.MethodHead, wantBody: "", wantServed: true},
		{name: "plain file", rel: "plain.css", acceptEnc: "gzip", method: http.MethodGet, wantBody: "a{}", wantServed: true},
		{name: "missing", rel: "nope.js", method: http.MethodGet, wantServed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, "/dashboard/"+tt.rel, nil)
			if tt.acceptEnc != "" {
				req.Header.Set("Accept-Encoding", tt.acceptEnc)
			}

			rec := httptest.NewRecorder()
			c := echo.NewContext(req, rec)

			assert.Equal(t, tt.wantServed, serveSPAFile(c, fsys, tt.rel))

			if !tt.wantServed {
				return
			}

			res := rec.Result()
			defer res.Body.Close()

			assert.Equal(t, tt.wantEncoding, res.Header.Get("Content-Encoding"))

			got, err := io.ReadAll(res.Body)
			require.NoError(t, err)

			if tt.wantEncoding == "gzip" {
				zr, zerr := gzip.NewReader(bytes.NewReader(got))
				require.NoError(t, zerr)

				got, err = io.ReadAll(zr)
				require.NoError(t, err)
				assert.Contains(t, res.Header.Get("Content-Type"), "javascript")

				tt.wantBody = body
			}

			assert.Equal(t, tt.wantBody, string(got))
		})
	}
}

func TestSPAStat(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"a.js.gz": {Data: []byte("x")},
		"b.css":   {Data: []byte("x")},
		"dir/c":   {Data: []byte("x")},
	}

	assert.True(t, spaStat(fsys, "a.js"))
	assert.True(t, spaStat(fsys, "b.css"))
	assert.False(t, spaStat(fsys, "dir"))
	assert.False(t, spaStat(fsys, "missing"))
}
