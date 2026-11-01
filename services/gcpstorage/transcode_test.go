package gcpstorage_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const transcodePlain = "hello decompressive transcoding, hello decompressive transcoding"

func gz(t *testing.T, s string) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := gzip.NewWriter(&buf)
	_, err := w.Write([]byte(s))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

func rawGet(t *testing.T, method, url string, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	require.NoError(t, err)

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	tr := &http.Transport{DisableCompression: true}
	t.Cleanup(tr.CloseIdleConnections)

	resp, err := (&http.Client{Transport: tr}).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, body
}

func TestDecompressiveTranscoding(t *testing.T) {
	t.Parallel()

	paths := map[string]string{
		"json media": "/storage/v1/b/bkt/o/%s?alt=media",
		"download":   "/download/storage/v1/b/bkt/o/%s?alt=media",
		"xml":        "/bkt/%s",
	}

	tests := []struct {
		hdr         map[string]string
		name        string
		object      string
		method      string
		cacheCtl    string
		contentEnc  string
		wantEnc     string
		wantStored  string
		wantBody    string
		transcoded  bool
		xmlOnly     bool
		wantChunked bool
	}{
		{
			name: "no accept-encoding is decompressed", object: "z.txt", contentEnc: "gzip", method: http.MethodGet,
			transcoded: true, wantBody: transcodePlain, wantStored: "gzip", wantChunked: true,
		},
		{
			name:       "accept-encoding gzip returns stored bytes",
			object:     "z.txt",
			contentEnc: "gzip",
			method:     http.MethodGet,
			hdr:        map[string]string{"Accept-Encoding": "gzip"},
			wantEnc:    "gzip",
			wantStored: "gzip",
		},
		{
			name: "gzip with q=0 is decompressed", object: "z.txt", contentEnc: "gzip", method: http.MethodGet,
			hdr: map[string]string{"Accept-Encoding": "gzip;q=0"}, transcoded: true, wantBody: transcodePlain,
			wantStored: "gzip", wantChunked: true,
		},
		{
			name: "no-transform keeps compression", object: "z.txt", contentEnc: "gzip", method: http.MethodGet,
			cacheCtl: "no-transform", wantEnc: "gzip", wantStored: "gzip",
		},
		{
			name: "range is ignored while transcoding", object: "z.txt", contentEnc: "gzip", method: http.MethodGet,
			hdr: map[string]string{"Range": "bytes=0-4"}, transcoded: true, wantBody: transcodePlain,
			wantStored: "gzip", wantChunked: true,
		},
		{
			xmlOnly:    true,
			name:       "head is transcoded without a body",
			object:     "z.txt",
			contentEnc: "gzip",
			method:     http.MethodHead,
			transcoded: true,
			wantStored: "gzip",
		},
		{
			name: "identity objects are untouched", object: "p.txt", method: http.MethodGet,
			wantBody: transcodePlain, wantStored: "identity",
		},
	}

	for pathName, pattern := range paths {
		for _, tt := range tests {
			t.Run(pathName+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				if tt.xmlOnly && pathName != "xml" {
					t.Skip("HEAD is served by the XML API only")
				}

				e := newSDK(t)
				e.bucket(t, "bkt", nil)

				data := []byte(transcodePlain)
				if tt.contentEnc == "gzip" {
					data = gz(t, transcodePlain)
				}

				write(t, e.client.Bucket("bkt").Object(tt.object), data, 0, func(w *storage.Writer) {
					w.ContentEncoding, w.CacheControl = tt.contentEnc, tt.cacheCtl
				})

				resp, body := rawGet(t, tt.method, e.url+strings.Replace(pattern, "%s", tt.object, 1), tt.hdr)
				require.Equal(t, http.StatusOK, resp.StatusCode)

				assert.Equal(t, tt.wantEnc, resp.Header.Get("Content-Encoding"))
				assert.Equal(t, tt.wantStored, resp.Header.Get("X-Goog-Stored-Content-Encoding"))

				if tt.wantChunked {
					assert.EqualValues(t, -1, resp.ContentLength, "no Content-Length while transcoding")
				}

				switch {
				case tt.method == http.MethodHead:
					assert.Empty(t, body)
				case tt.wantEnc == "gzip":
					assert.Equal(t, gz(t, transcodePlain), body)
				default:
					assert.Equal(t, tt.wantBody, string(body))
				}
			})
		}
	}
}

func TestTranscodingThroughClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		want       []byte
		compressed bool
	}{
		{name: "default reader gets decompressed bytes", want: []byte(transcodePlain)},
		{name: "read compressed gets stored bytes", compressed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSDK(t)
			b := e.bucket(t, "bkt", nil)
			stored := gz(t, transcodePlain)

			write(t, b.Object("z"), stored, 0, func(w *storage.Writer) { w.ContentEncoding = "gzip" })

			r, err := b.Object("z").ReadCompressed(tt.compressed).NewReader(t.Context())
			require.NoError(t, err)

			defer r.Close()

			got, err := io.ReadAll(r)
			require.NoError(t, err)

			want := tt.want
			if tt.compressed {
				want = stored
			}

			assert.Equal(t, want, got)
		})
	}
}
