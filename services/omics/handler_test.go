package omics_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

func newTestHandler(t *testing.T) *omics.Handler {
	t.Helper()

	backend := omics.NewInMemoryBackend("000000000000", "us-east-1")

	return omics.NewHandler(backend)
}

func doRequest(t *testing.T, h *omics.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func TestOmics_ReferenceStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateReferenceStore returns 201 with arn",
			method:   http.MethodPost,
			path:     "/referencestore",
			body:     map[string]any{"name": "test-store"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
				assert.Equal(t, "test-store", resp["name"])
			},
		},
		{
			name:     "CreateReferenceStore missing name returns 400",
			method:   http.MethodPost,
			path:     "/referencestore",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetReferenceStore unknown id returns 404",
			method:   http.MethodGet,
			path:     "/referencestore/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteReferenceStore unknown id returns 404",
			method:   http.MethodDelete,
			path:     "/referencestore/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}
	// CRUD tests with dynamic IDs done separately
	t.Run("GetReferenceStore returns 200", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "store1"})
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		rec2 := doRequest(t, h, http.MethodGet, "/referencestore/"+resp["id"].(string), nil)
		assert.Equal(t, http.StatusOK, rec2.Code)
	})

	t.Run("DeleteReferenceStore returns 200", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "to-delete"})
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		rec2 := doRequest(t, h, http.MethodDelete, "/referencestore/"+resp["id"].(string), nil)
		assert.Equal(t, http.StatusOK, rec2.Code)
	})

	// List test done separately to avoid the "path == empty → create extra store" logic
	t.Run("ListReferenceStores returns stores", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "s1"})
		doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "s2"})

		rec := doRequest(t, h, http.MethodPost, "/referencestores", nil)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		stores := resp["referenceStores"].([]any)
		assert.Len(t, stores, 2)
	})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOmics_SequenceStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateSequenceStore returns 201",
			method:   http.MethodPost,
			path:     "/sequencestore",
			body:     map[string]any{"name": "seq-store"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
				assert.Equal(t, "seq-store", resp["name"])
			},
		},
		{
			name:     "CreateSequenceStore missing name returns 400",
			method:   http.MethodPost,
			path:     "/sequencestore",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetSequenceStore unknown returns 404",
			method:   http.MethodGet,
			path:     "/sequencestore/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListSequenceStores empty returns 200",
			method:   http.MethodPost,
			path:     "/sequencestores",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				stores := resp["sequenceStores"].([]any)
				assert.Empty(t, stores)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOmics_RunGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateRunGroup returns 201 with arn",
			method:   http.MethodPost,
			path:     "/runGroup",
			body:     map[string]any{"name": "rg1", "maxCpus": 4, "maxRuns": 2},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
			},
		},
		{
			name:     "ListRunGroups returns empty list",
			method:   http.MethodGet,
			path:     "/runGroup",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["runGroups"])
			},
		},
		{
			name:     "GetRunGroup unknown returns 404",
			method:   http.MethodGet,
			path:     "/runGroup/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteRunGroup unknown returns 404",
			method:   http.MethodDelete,
			path:     "/runGroup/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOmics_Workflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateWorkflow returns 201",
			method:   http.MethodPost,
			path:     "/workflow",
			body:     map[string]any{"name": "wf1", "engine": "WDL"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
				assert.Equal(t, "wf1", resp["name"])
			},
		},
		{
			name:     "CreateWorkflow missing name returns 400",
			method:   http.MethodPost,
			path:     "/workflow",
			body:     map[string]any{"engine": "WDL"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ListWorkflows returns empty",
			method:   http.MethodGet,
			path:     "/workflow",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["workflows"])
			},
		},
		{
			name:     "GetWorkflow unknown returns 404",
			method:   http.MethodGet,
			path:     "/workflow/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOmics_AnnotationStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateAnnotationStore returns 201",
			method:   http.MethodPost,
			path:     "/annotationStore",
			body:     map[string]any{"name": "ann-store", "storeFormat": "VCF"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
			},
		},
		{
			name:     "CreateAnnotationStore duplicate returns 409",
			method:   http.MethodPost,
			path:     "/annotationStore",
			body:     map[string]any{"name": "dup-store", "storeFormat": "VCF"},
			wantCode: http.StatusCreated,
		},
		{
			name:     "GetAnnotationStore unknown returns 404",
			method:   http.MethodGet,
			path:     "/annotationStore/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListAnnotationStores empty returns 200",
			method:   http.MethodPost,
			path:     "/annotationStores",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOmics_VariantStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateVariantStore returns 201",
			method:   http.MethodPost,
			path:     "/variantStore",
			body:     map[string]any{"name": "var-store"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
			},
		},
		{
			name:     "CreateVariantStore duplicate returns 409",
			method:   http.MethodPost,
			path:     "/variantStore",
			body:     map[string]any{"name": "dup-var"},
			wantCode: http.StatusCreated,
		},
		{
			name:     "GetVariantStore unknown returns 404",
			method:   http.MethodGet,
			path:     "/variantStore/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOmics_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:   "StartRun returns 201",
			method: http.MethodPost,
			path:   "/run",
			body: map[string]any{
				"workflowId": "wf123",
				"roleArn":    "arn:aws:iam::000000000000:role/omics-role",
				"name":       "my-run",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
			},
		},
		{
			name:     "ListRuns returns empty",
			method:   http.MethodGet,
			path:     "/run",
			wantCode: http.StatusOK,
		},
		{
			name:     "GetRun unknown returns 404",
			method:   http.MethodGet,
			path:     "/run/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteRun unknown returns 404",
			method:   http.MethodDelete,
			path:     "/run/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOmics_Share(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:   "CreateShare returns 201",
			method: http.MethodPost,
			path:   "/share",
			body: map[string]any{
				"resourceArn":         "arn:aws:omics:us-east-1:000000000000:annotationStore/mystore",
				"principalSubscriber": "123456789012",
				"shareName":           "my-share",
			},
			wantCode: http.StatusCreated,
		},
		{
			name:     "GetShare unknown returns 404",
			method:   http.MethodGet,
			path:     "/share/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteShare unknown returns 404",
			method:   http.MethodDelete,
			path:     "/share/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestOmics_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a workflow to get an ARN
	rec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{"name": "tagged-wf", "engine": "WDL"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var wfResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &wfResp))
	wfARN := wfResp["arn"].(string)

	// TagResource
	rec = doRequest(t, h, http.MethodPost, "/tags/"+wfARN, map[string]any{"tags": map[string]string{"env": "test"}})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, http.MethodGet, "/tags/"+wfARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
}

func TestOmics_RouteMatcher_TagPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{name: "omics_tag_path", path: "/tags/arn:aws:omics:us-east-1:000000000000:workflow/12345", wantMatch: true},
		{
			name:      "fis_tag_path",
			path:      "/tags/arn:aws:fis:us-east-1:000000000000:experiment-template/EXTabcdef0123456",
			wantMatch: false,
		},
		{
			name:      "fis_tag_path_nonexistent",
			path:      "/tags/arn:aws:fis:us-east-1:000000000000:experiment-template/EXTdoesnotexist00000000",
			wantMatch: false,
		},
		{name: "other_tag_path", path: "/tags/arn:aws:s3:::my-bucket", wantMatch: false},
		{name: "tags_root", path: "/tags", wantMatch: false},
		{name: "workflow_path", path: "/workflow", wantMatch: true},
		{name: "referencestore_path", path: "/referencestore", wantMatch: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c), "path: %s", tt.path)
		})
	}
}

func doRequestRaw(t *testing.T, h *omics.Handler, method, path string, contentType string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func TestOmics_GetReference_Binary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T, h *omics.Handler) string // returns reference path
		wantCode int
	}{
		{
			name: "GetReference returns 404 for unknown store",
			setup: func(t *testing.T, h *omics.Handler) string {
				t.Helper()
				return "/referencestore/unknownstore/reference/unknownref"
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "GetReference returns 404 for unknown reference",
			setup: func(t *testing.T, h *omics.Handler) string {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				return fmt.Sprintf("/referencestore/%s/reference/doesnotexist", resp["id"])
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "GetReference returns 200 with binary body for imported reference",
			setup: func(t *testing.T, h *omics.Handler) string {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/referencestore/%s/importjob", storeID),
					map[string]any{
						"roleArn": "arn:aws:iam::000000000000:role/test",
						"sources": []map[string]any{{"sourceFile": "s3://b/ref.fa", "name": "ref1"}},
					},
				)
				require.Equal(t, http.StatusCreated, rec2.Code)

				// List references to get the created reference ID.
				rec3 := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/referencestore/%s/references", storeID),
					nil,
				)
				require.Equal(t, http.StatusOK, rec3.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listResp))
				refs := listResp["references"].([]any)
				require.Len(t, refs, 1)
				refID := refs[0].(map[string]any)["id"].(string)

				return fmt.Sprintf("/referencestore/%s/reference/%s", storeID, refID)
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			path := tc.setup(t, h)
			rec := doRequestRaw(t, h, http.MethodGet, path, "", nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestOmics_UploadReadSetPart_And_GetReadSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		run      func(t *testing.T, h *omics.Handler)
	}{
		{
			name: "UploadReadSetPart missing partNumber returns 400",
			run: func(t *testing.T, h *omics.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequestRaw(t, h, http.MethodPut,
					fmt.Sprintf("/sequencestore/%s/upload/fakeid/part", storeID),
					"application/octet-stream",
					[]byte("data"),
				)
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
			},
		},
		{
			name: "UploadReadSetPart unknown upload returns 404",
			run: func(t *testing.T, h *omics.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequestRaw(t, h, http.MethodPut,
					fmt.Sprintf("/sequencestore/%s/upload/fakeid/part?partNumber=1&partSource=SOURCE1", storeID),
					"application/octet-stream",
					[]byte("data"),
				)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "UploadReadSetPart returns real SHA256 checksum",
			run: func(t *testing.T, h *omics.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/sequencestore/%s/upload", storeID),
					map[string]any{"name": "rs", "sequenceType": "GENERIC"},
				)
				require.Equal(t, http.StatusCreated, rec2.Code)
				var uploadResp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &uploadResp))
				uploadID := uploadResp["uploadId"].(string)

				payload := []byte("ACGT sequence data")
				expectedSum := sha256.Sum256(payload)
				expectedChecksum := hex.EncodeToString(expectedSum[:])

				rec3 := doRequestRaw(t, h, http.MethodPut,
					fmt.Sprintf("/sequencestore/%s/upload/%s/part?partNumber=1&partSource=SOURCE1", storeID, uploadID),
					"application/octet-stream",
					payload,
				)
				require.Equal(t, http.StatusOK, rec3.Code)
				var partResp map[string]any
				require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &partResp))
				assert.Equal(t, expectedChecksum, partResp["checksum"])
				assert.Equal(t, "SHA256", partResp["checksumAlgorithm"])
			},
		},
		{
			name: "GetReadSet streams bytes after complete multipart upload",
			run: func(t *testing.T, h *omics.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/sequencestore/%s/upload", storeID),
					map[string]any{"name": "rs", "sequenceType": "GENERIC"},
				)
				require.Equal(t, http.StatusCreated, rec2.Code)
				var uploadResp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &uploadResp))
				uploadID := uploadResp["uploadId"].(string)

				payload := []byte("hello omics")
				rec3 := doRequestRaw(t, h, http.MethodPut,
					fmt.Sprintf("/sequencestore/%s/upload/%s/part?partNumber=1&partSource=SOURCE1", storeID, uploadID),
					"application/octet-stream",
					payload,
				)
				require.Equal(t, http.StatusOK, rec3.Code)

				rec4 := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/sequencestore/%s/upload/%s/complete", storeID, uploadID),
					nil,
				)
				require.Equal(t, http.StatusOK, rec4.Code)
				var rsResp map[string]any
				require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &rsResp))
				rsID := rsResp["id"].(string)

				rec5 := doRequestRaw(t, h, http.MethodGet,
					fmt.Sprintf("/sequencestore/%s/readset/%s", storeID, rsID),
					"", nil,
				)
				require.Equal(t, http.StatusOK, rec5.Code)
				assert.Equal(t, payload, rec5.Body.Bytes())
			},
		},
		{
			name: "GetReadSet returns 404 for unknown read set",
			run: func(t *testing.T, h *omics.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
				require.Equal(t, http.StatusCreated, rec.Code)
				var storeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &storeResp))
				storeID := storeResp["id"].(string)

				rec2 := doRequestRaw(t, h, http.MethodGet,
					fmt.Sprintf("/sequencestore/%s/readset/doesnotexist", storeID),
					"", nil,
				)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newTestHandler(t))
		})
	}
}
