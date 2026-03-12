//go:build !integration

package mediastoredata_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediastoredata"
)

func newTestHandler(t *testing.T) *mediastoredata.Handler {
	t.Helper()

	return mediastoredata.NewHandler(mediastoredata.NewInMemoryBackend())
}

func doRequest(
	t *testing.T,
	h *mediastoredata.Handler,
	method, path string,
	body []byte,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("User-Agent", "aws-sdk-go-v2/1.0.0 mediastoredata/1.29.18")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestMediaStoreData_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "MediaStoreData", h.Name())
}

func TestMediaStoreData_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "PutObject")
	assert.Contains(t, ops, "GetObject")
	assert.Contains(t, ops, "DeleteObject")
	assert.Contains(t, ops, "ListItems")
	assert.Contains(t, ops, "DescribeObject")
}

func TestMediaStoreData_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name      string
		userAgent string
		wantMatch bool
	}{
		{
			name:      "sdk_user_agent_matches",
			userAgent: "aws-sdk-go-v2/1.0.0 mediastoredata/1.29.18",
			wantMatch: true,
		},
		{
			name:      "s3_user_agent_does_not_match",
			userAgent: "aws-sdk-go-v2/1.0.0 s3/1.0.0",
			wantMatch: false,
		},
		{
			name:      "empty_user_agent_does_not_match",
			userAgent: "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("User-Agent", tt.userAgent)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.RouteMatcher()(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

func TestMediaStoreData_PutObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		contentType string
		body        []byte
		wantStatus  int
		wantETag    bool
	}{
		{
			name:        "simple_object",
			path:        "/video/clip.mp4",
			body:        []byte("video content"),
			contentType: "video/mp4",
			wantStatus:  http.StatusOK,
			wantETag:    true,
		},
		{
			name:        "empty_body",
			path:        "/empty/file.txt",
			body:        []byte{},
			contentType: "text/plain",
			wantStatus:  http.StatusOK,
			wantETag:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			headers := map[string]string{
				"Content-Type": tt.contentType,
			}
			rec := doRequest(t, h, http.MethodPut, tt.path, tt.body, headers)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantETag {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["ETag"])
				assert.NotEmpty(t, resp["StorageClass"])
			}
		})
	}
}

func TestMediaStoreData_GetObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		putPath    string
		getPath    string
		putBody    []byte
		wantStatus int
		wantBody   bool
	}{
		{
			name:       "existing_object",
			putPath:    "/video/clip.mp4",
			putBody:    []byte("hello world"),
			getPath:    "/video/clip.mp4",
			wantStatus: http.StatusOK,
			wantBody:   true,
		},
		{
			name:       "missing_object",
			putPath:    "",
			putBody:    nil,
			getPath:    "/missing/file.mp4",
			wantStatus: http.StatusNotFound,
			wantBody:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.putPath != "" {
				doRequest(t, h, http.MethodPut, tt.putPath, tt.putBody, nil)
			}

			rec := doRequest(t, h, http.MethodGet, tt.getPath, nil, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody {
				assert.Equal(t, tt.putBody, rec.Body.Bytes())
			}
		})
	}
}

func TestMediaStoreData_DeleteObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
		putFirst   bool
	}{
		{
			name:       "existing_object",
			putFirst:   true,
			path:       "/video/clip.mp4",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_object",
			putFirst:   false,
			path:       "/missing/file.mp4",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.putFirst {
				doRequest(t, h, http.MethodPut, tt.path, []byte("data"), nil)
			}

			rec := doRequest(t, h, http.MethodDelete, tt.path, nil, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestMediaStoreData_ListItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		objects   map[string][]byte
		listPath  string
		wantNames []string
		wantTypes []string
	}{
		{
			name: "list_root",
			objects: map[string][]byte{
				"/video/clip.mp4": []byte("a"),
				"/audio/song.mp3": []byte("b"),
			},
			listPath:  "/",
			wantNames: []string{"audio", "video"},
			wantTypes: []string{"FOLDER", "FOLDER"},
		},
		{
			name: "list_subfolder",
			objects: map[string][]byte{
				"/video/clip.mp4":    []byte("a"),
				"/video/trailer.mp4": []byte("b"),
			},
			listPath:  "/?Path=video",
			wantNames: []string{"clip.mp4", "trailer.mp4"},
			wantTypes: []string{"OBJECT", "OBJECT"},
		},
		{
			name:      "empty_result",
			objects:   map[string][]byte{},
			listPath:  "/",
			wantNames: []string{},
			wantTypes: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for path, body := range tt.objects {
				doRequest(t, h, http.MethodPut, path, body, nil)
			}

			rec := doRequest(t, h, http.MethodGet, tt.listPath, nil, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Items []struct {
					Name string `json:"Name"`
					Type string `json:"Type"`
				} `json:"Items"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Items, len(tt.wantNames))

			for i, item := range resp.Items {
				assert.Equal(t, tt.wantNames[i], item.Name)
				assert.Equal(t, tt.wantTypes[i], item.Type)
			}
		})
	}
}

func TestMediaStoreData_DescribeObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		putBody    []byte
		wantStatus int
		putFirst   bool
		wantETag   bool
	}{
		{
			name:       "existing_object",
			putFirst:   true,
			putBody:    []byte("some content"),
			path:       "/video/clip.mp4",
			wantStatus: http.StatusOK,
			wantETag:   true,
		},
		{
			name:       "missing_object",
			putFirst:   false,
			putBody:    nil,
			path:       "/missing.mp4",
			wantStatus: http.StatusNotFound,
			wantETag:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.putFirst {
				doRequest(t, h, http.MethodPut, tt.path, tt.putBody, map[string]string{"Content-Type": "video/mp4"})
			}

			rec := doRequest(t, h, http.MethodHead, tt.path, nil, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantETag {
				assert.NotEmpty(t, rec.Header().Get("ETag"))
				assert.NotEmpty(t, rec.Header().Get("Content-Length"))
			}
		})
	}
}

func TestMediaStoreData_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		contentType string
		wantFolder  string
		content     []byte
	}{
		{
			name:        "mp4_object_in_media_folder",
			path:        "/media/test.mp4",
			content:     []byte("test media content"),
			contentType: "video/mp4",
			wantFolder:  "media",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Put object.
			putRec := doRequest(t, h, http.MethodPut, tt.path, tt.content, map[string]string{
				"Content-Type": tt.contentType,
			})
			require.Equal(t, http.StatusOK, putRec.Code)

			// Get object and verify body matches.
			getRec := doRequest(t, h, http.MethodGet, tt.path, nil, nil)
			require.Equal(t, http.StatusOK, getRec.Code)
			assert.Equal(t, tt.content, getRec.Body.Bytes())

			// Describe object.
			headRec := doRequest(t, h, http.MethodHead, tt.path, nil, nil)
			require.Equal(t, http.StatusOK, headRec.Code)
			assert.NotEmpty(t, headRec.Header().Get("ETag"))

			// List items at root.
			listRec := doRequest(t, h, http.MethodGet, "/", nil, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp struct {
				Items []struct {
					Name string `json:"Name"`
					Type string `json:"Type"`
				} `json:"Items"`
			}

			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			require.Len(t, listResp.Items, 1)
			assert.Equal(t, tt.wantFolder, listResp.Items[0].Name)
			assert.Equal(t, "FOLDER", listResp.Items[0].Type)

			// Delete object.
			delRec := doRequest(t, h, http.MethodDelete, tt.path, nil, nil)
			require.Equal(t, http.StatusOK, delRec.Code)

			// Verify deletion.
			getRec2 := doRequest(t, h, http.MethodGet, tt.path, nil, nil)
			assert.Equal(t, http.StatusNotFound, getRec2.Code)
		})
	}
}
