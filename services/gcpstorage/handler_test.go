package gcpstorage_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

type rawEnv struct {
	h   *gcpstorage.Handler
	srv *httptest.Server
}

func newRaw(t *testing.T) *rawEnv {
	t.Helper()

	h := gcpstorage.NewHandler(gcpstorage.NewInMemoryBackend())
	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return &rawEnv{h: h, srv: srv}
}

func (r *rawEnv) do(t *testing.T, method, path, body string, hdr map[string]string) (*http.Response, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, r.srv.URL+path, strings.NewReader(body))
	require.NoError(t, err)

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, string(data)
}

func (r *rawEnv) mkBucket(t *testing.T, name string) {
	t.Helper()

	resp, body := r.do(t, http.MethodPost, "/storage/v1/b?project=p", `{"name":"`+name+`"}`, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
}

func TestErrorEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		reason string
		status int
	}{
		{"no bucket", http.MethodGet, "/storage/v1/b/nope", "", "notFound", http.StatusNotFound},
		{"no object", http.MethodGet, "/storage/v1/b/bkt/o/nope", "", "notFound", http.StatusNotFound},
		{"missing project", http.MethodGet, "/storage/v1/b", "", "required", http.StatusBadRequest},
		{"bad name", http.MethodPost, "/storage/v1/b?project=p", `{"name":"A"}`, "invalid", http.StatusBadRequest},
		{"bad json", http.MethodPost, "/storage/v1/b?project=p", `{`, "invalid", http.StatusBadRequest},
		{
			"bad param",
			http.MethodGet,
			"/storage/v1/b/bkt/o/x?ifGenerationMatch=zz",
			"",
			"invalid",
			http.StatusBadRequest,
		},
		{"duplicate", http.MethodPost, "/storage/v1/b?project=p", `{"name":"bkt"}`, "conflict", http.StatusConflict},
		{"unknown route", http.MethodGet, "/storage/v1/zzz", "", "notFound", http.StatusNotFound},
		{
			"bad upload type",
			http.MethodPost,
			"/upload/storage/v1/b/bkt/o?uploadType=zzz",
			"",
			"invalid",
			http.StatusBadRequest,
		},
		{
			"no name",
			http.MethodPost,
			"/upload/storage/v1/b/bkt/o?uploadType=media",
			"x",
			"required",
			http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := newRaw(t)
			r.mkBucket(t, "bkt")

			resp, body := r.do(t, tt.method, tt.path, tt.body, nil)
			assert.Equal(t, tt.status, resp.StatusCode)

			var env struct {
				Error struct {
					Message string `json:"message"`
					Errors  []struct {
						Reason string `json:"reason"`
						Domain string `json:"domain"`
					} `json:"errors"`
					Code int `json:"code"`
				} `json:"error"`
			}

			require.NoError(t, json.Unmarshal([]byte(body), &env), body)
			assert.Equal(t, tt.status, env.Error.Code)
			assert.NotEmpty(t, env.Error.Message)
			require.Len(t, env.Error.Errors, 1)
			assert.Equal(t, tt.reason, env.Error.Errors[0].Reason)
			assert.Equal(t, "global", env.Error.Errors[0].Domain)
		})
	}
}

func TestXMLAPI(t *testing.T) {
	t.Parallel()

	r := newRaw(t)
	r.mkBucket(t, "xml-bucket")

	resp, _ := r.do(t, http.MethodPut, "/xml-bucket/dir/file.txt", "hello world", map[string]string{
		"Content-Type": "text/plain", "X-Goog-Meta-Color": "blue",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("X-Goog-Generation"))
	assert.Contains(t, resp.Header.Get("X-Goog-Hash"), "crc32c=")
	assert.Regexp(t, `^"[0-9a-f]{32}"$`, resp.Header.Get("ETag"))

	resp, body := r.do(t, http.MethodGet, "/xml-bucket/dir/file.txt", "", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "hello world", body)
	assert.Equal(t, "text/plain", resp.Header.Get("Content-Type"))
	assert.Equal(t, "blue", resp.Header.Get("X-Goog-Meta-Color"))
	assert.Equal(t, "1", resp.Header.Get("X-Goog-Metageneration"))

	resp, body = r.do(t, http.MethodGet, "/xml-bucket/dir/file.txt", "", map[string]string{"Range": "bytes=6-"})
	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, "world", body)
	assert.Equal(t, "bytes 6-10/11", resp.Header.Get("Content-Range"))

	resp, _ = r.do(t, http.MethodHead, "/xml-bucket/dir/file.txt", "", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "11", resp.Header.Get("Content-Length"))

	resp, _ = r.do(t, http.MethodPut, "/xml-bucket/dir/file.txt", "again", map[string]string{
		"X-Goog-If-Generation-Match": "0",
	})
	assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)

	resp, body = r.do(t, http.MethodGet, "/xml-bucket/missing", "", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, body, "<Code>NoSuchKey</Code>")

	resp, body = r.do(t, http.MethodGet, "/no-bucket/x", "", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, body, "<Code>NoSuchBucket</Code>")

	resp, _ = r.do(t, http.MethodDelete, "/xml-bucket/dir/file.txt", "", nil)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	resp, _ = r.do(t, http.MethodGet, "/xml-bucket/dir/file.txt", "", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestUploadsAndDownloads(t *testing.T) {
	t.Parallel()

	r := newRaw(t)
	r.mkBucket(t, "up-bucket")

	resp, body := r.do(t, http.MethodPost, "/upload/storage/v1/b/up-bucket/o?uploadType=media&name=a%2Fb.bin",
		"binary", map[string]string{"Content-Type": "application/x-bin"})
	require.Equal(t, http.StatusOK, resp.StatusCode, body)

	var obj map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &obj))
	assert.Equal(t, "a/b.bin", obj["name"])
	assert.Equal(t, "6", obj["size"])
	assert.Equal(t, "application/x-bin", obj["contentType"])
	assert.Equal(t, "storage#object", obj["kind"])

	mp := "--BOUND\r\nContent-Type: application/json\r\n\r\n" +
		`{"name":"multi.txt","contentType":"text/plain","metadata":{"m":"1"}}` +
		"\r\n--BOUND\r\nContent-Type: text/other\r\n\r\nmulti body\r\n--BOUND--\r\n"
	resp, body = r.do(t, http.MethodPost, "/upload/storage/v1/b/up-bucket/o?uploadType=multipart", mp,
		map[string]string{"Content-Type": "multipart/related; boundary=BOUND"})
	require.Equal(t, http.StatusOK, resp.StatusCode, body)

	for _, path := range []string{
		"/download/storage/v1/b/up-bucket/o/multi.txt?alt=media",
		"/storage/v1/b/up-bucket/o/multi.txt?alt=media",
		"/up-bucket/multi.txt",
	} {
		resp, body = r.do(t, http.MethodGet, path, "", nil)
		assert.Equal(t, http.StatusOK, resp.StatusCode, path)
		assert.Equal(t, "multi body", body, path)
		assert.Equal(t, "text/plain", resp.Header.Get("Content-Type"), path)
	}

	resp, _ = r.do(t, http.MethodGet, "/storage/v1/b/up-bucket/o/multi.txt?alt=media&ifGenerationNotMatch="+
		resp.Header.Get("X-Goog-Generation"), "", nil)
	assert.Equal(t, http.StatusNotModified, resp.StatusCode)

	resp, body = r.do(t, http.MethodPost, "/upload/storage/v1/b/up-bucket/o?uploadType=multipart&name=bad.txt",
		"--B\r\nContent-Type: application/json\r\n\r\n"+`{"crc32c":"AAAAAA=="}`+
			"\r\n--B\r\nContent-Type: text/plain\r\n\r\nzz\r\n--B--\r\n",
		map[string]string{"Content-Type": "multipart/related; boundary=B"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, body)
	assert.Contains(t, body, "CRC32C")
}

func TestResumableProtocol(t *testing.T) {
	t.Parallel()

	r := newRaw(t)
	r.mkBucket(t, "res-bucket")

	resp, _ := r.do(t, http.MethodPost, "/upload/storage/v1/b/res-bucket/o?uploadType=resumable&name=big",
		`{"contentType":"text/plain"}`, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	loc := strings.TrimPrefix(resp.Header.Get("Location"), r.srv.URL)
	require.NotEmpty(t, loc)

	resp, _ = r.do(t, http.MethodPut, loc, "01234", map[string]string{"Content-Range": "bytes 0-4/10"})
	assert.Equal(t, http.StatusPermanentRedirect, resp.StatusCode)
	assert.Equal(t, "bytes=0-4", resp.Header.Get("Range"))

	resp, _ = r.do(t, http.MethodPut, loc, "", map[string]string{"Content-Range": "bytes */10"})
	assert.Equal(t, http.StatusPermanentRedirect, resp.StatusCode)
	assert.Equal(t, "bytes=0-4", resp.Header.Get("Range"))

	resp, _ = r.do(t, http.MethodPost, loc, "56789", map[string]string{
		"Content-Range": "bytes 5-9/10", "X-GUploader-No-308": "yes",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, body := r.do(t, http.MethodGet, "/res-bucket/big", "", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "0123456789", body)

	resp, _ = r.do(t, http.MethodPut, loc, "x", map[string]string{"Content-Range": "bytes 0-0/1"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "session is gone once committed")

	resp, _ = r.do(t, http.MethodPost, "/upload/storage/v1/b/res-bucket/o?uploadType=resumable&name=c", "{}", nil)
	cancel := strings.TrimPrefix(resp.Header.Get("Location"), r.srv.URL)

	resp, _ = r.do(t, http.MethodDelete, cancel, "", nil)
	assert.Equal(t, 499, resp.StatusCode)

	resp, _ = r.do(t, http.MethodDelete, cancel, "", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestRewriteAndPatch(t *testing.T) {
	t.Parallel()

	r := newRaw(t)
	r.mkBucket(t, "rw-bucket")
	r.do(t, http.MethodPut, "/rw-bucket/src", "data", map[string]string{"Content-Type": "text/x"})

	resp, body := r.do(t, http.MethodPost, "/storage/v1/b/rw-bucket/o/src/rewriteTo/b/rw-bucket/o/dst", "{}", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)

	var rw struct {
		Resource map[string]any `json:"resource"`
		Kind     string         `json:"kind"`
		Total    string         `json:"totalBytesRewritten"`
		Size     string         `json:"objectSize"`
		Done     bool           `json:"done"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &rw))
	assert.Equal(t, "storage#rewriteResponse", rw.Kind)
	assert.True(t, rw.Done)
	assert.Equal(t, "4", rw.Total)
	assert.Equal(t, "4", rw.Size)
	assert.Equal(t, "text/x", rw.Resource["contentType"])

	resp, body = r.do(t, http.MethodPatch, "/storage/v1/b/rw-bucket/o/dst",
		`{"metadata":{"a":"1","b":"2"},"contentLanguage":"en"}`, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)

	resp, body = r.do(t, http.MethodPatch, "/storage/v1/b/rw-bucket/o/dst",
		`{"metadata":{"a":null},"contentLanguage":null}`, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)

	var obj struct {
		Metadata       map[string]string `json:"metadata"`
		ContentLang    string            `json:"contentLanguage"`
		Metageneration string            `json:"metageneration"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &obj))
	assert.Equal(t, map[string]string{"b": "2"}, obj.Metadata)
	assert.Empty(t, obj.ContentLang)
	assert.Equal(t, "3", obj.Metageneration)

	resp, body = r.do(t, http.MethodPut, "/storage/v1/b/rw-bucket/o/dst", `{"contentType":"a/b"}`, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.NotContains(t, body, `"metadata"`)

	resp, body = r.do(t, http.MethodPost, "/storage/v1/b/rw-bucket/o/out/compose",
		`{"sourceObjects":[]}`, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, body)
}

func TestRoutingMetadata(t *testing.T) {
	t.Parallel()

	h := gcpstorage.NewHandler(gcpstorage.NewInMemoryBackend())
	e := echo.New()

	tests := []struct {
		method   string
		path     string
		op       string
		resource string
	}{
		{http.MethodGet, "/storage/v1/b?project=p", "ListBuckets", ""},
		{http.MethodGet, "/storage/v1/b/bk/o/a%2Fb", "GetObject", "bk/a/b"},
		{http.MethodPost, "/storage/v1/b/bk/o/x/compose", "ComposeObject", "bk/x"},
		{http.MethodPut, "/bk/dir/obj", "XMLPutObject", "bk/dir/obj"},
		{http.MethodPut, "/upload/storage/v1/b/bk/o?upload_id=1", "ResumableUpload", "bk"},
		{http.MethodGet, "/", "Unknown", ""},
	}

	for _, tt := range tests {
		t.Run(tt.op+" "+tt.path, func(t *testing.T) {
			t.Parallel()

			c := e.NewContext(httptest.NewRequest(tt.method, tt.path, bytes.NewReader(nil)), httptest.NewRecorder())
			assert.Equal(t, tt.op, h.ExtractOperation(c))
			assert.Equal(t, tt.resource, h.ExtractResource(c))
		})
	}

	assert.False(
		t,
		h.RouteMatcher()(e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), httptest.NewRecorder())),
	)
	assert.Contains(t, h.GetSupportedOperations(), "RewriteObject")
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	r := newRaw(t)
	r.do(
		t,
		http.MethodPost,
		"/storage/v1/b?project=p",
		`{"name":"persist-bkt","versioning":{"enabled":true},"labels":{"a":"b"},`+
			`"lifecycle":{"rule":[{"action":{"type":"Delete"},"condition":{"age":1}}]}}`,
		nil,
	)
	r.do(t, http.MethodPut, "/persist-bkt/o1", "one", nil)
	r.do(t, http.MethodPut, "/persist-bkt/o1", "two", nil)
	r.do(t, http.MethodPut, "/storage/v1/b/persist-bkt/iam",
		`{"bindings":[{"role":"roles/storage.admin","members":["user:a@b.c"]}]}`, nil)

	snap := r.h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	r2 := newRaw(t)
	require.NoError(t, r2.h.Restore(t.Context(), snap))

	_, body := r2.do(t, http.MethodGet, "/persist-bkt/o1", "", nil)
	assert.Equal(t, "two", body)

	_, body = r2.do(t, http.MethodGet, "/storage/v1/b/persist-bkt/o?versions=true", "", nil)
	assert.Equal(t, 2, strings.Count(body, `"storage#object"`))

	_, body = r2.do(t, http.MethodGet, "/storage/v1/b/persist-bkt", "", nil)
	assert.Contains(t, body, `"lifecycle"`)
	assert.Contains(t, body, `"enabled":true`)

	_, body = r2.do(t, http.MethodGet, "/storage/v1/b/persist-bkt/iam", "", nil)
	assert.Contains(t, body, "roles/storage.admin")

	resp, body := r2.do(t, http.MethodPut, "/persist-bkt/o1", "three", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Greater(t, resp.Header.Get("X-Goog-Generation"), "0", "generations stay monotonic after restore")

	r2.h.Reset()

	resp, _ = r2.do(t, http.MethodGet, "/storage/v1/b/persist-bkt", "", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"null bucket", `{"version":1,"buckets":{"b":null}}`, true},
		{"old version", `{"version":0,"buckets":{"b":{}}}`, false},
		{"not json", `{`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := gcpstorage.NewHandler(gcpstorage.NewInMemoryBackend())
			err := h.Restore(t.Context(), []byte(tt.data))
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestProvider(t *testing.T) {
	t.Parallel()

	p := &gcpstorage.Provider{}
	assert.Equal(t, "GCPStorage", p.Name())

	_, err := p.Init(nil)
	require.ErrorIs(t, err, gcpstorage.ErrNilAppContext)

	reg, err := p.Init(&service.AppContext{Config: stubConfig{}})
	require.NoError(t, err)

	h, ok := reg.(*gcpstorage.Handler)
	require.True(t, ok)
	assert.Equal(t, 5555, h.Port)

	reg, err = p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.Equal(t, gcpstorage.DefaultPort, reg.(*gcpstorage.Handler).Port)
}

type stubConfig struct{}

func (stubConfig) GetGCPStorageSettings() gcpstorage.Settings { return gcpstorage.Settings{Port: 5555} }

func TestListenerLifecycle(t *testing.T) {
	t.Parallel()

	h := gcpstorage.NewHandler(gcpstorage.NewInMemoryBackend())
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	h.Shutdown(t.Context())
	h.Shutdown(t.Context())
}
