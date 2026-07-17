package batch_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *batch.Handler) string
		addTags    map[string]string
		wantTags   map[string]string
		name       string
		removeKeys []string
		wantStatus int
	}{
		{
			name: "list_tags_on_ce",
			setup: func(t *testing.T, h *batch.Handler) string {
				t.Helper()
				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": "tag-ce",
					"type":                   "MANAGED",
					"tags":                   map[string]string{"env": "prod"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return out["computeEnvironmentArn"]
			},
			wantTags:   map[string]string{"env": "prod"},
			wantStatus: http.StatusOK,
		},
		{
			name: "tag_and_untag_ce",
			setup: func(t *testing.T, h *batch.Handler) string {
				t.Helper()
				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": "untag-ce",
					"type":                   "MANAGED",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return out["computeEnvironmentArn"]
			},
			addTags:    map[string]string{"key1": "val1", "key2": "val2"},
			removeKeys: []string{"key1"},
			wantTags:   map[string]string{"key2": "val2"},
			wantStatus: http.StatusOK,
		},
		{
			name: "list_tags_not_found",
			setup: func(_ *testing.T, _ *batch.Handler) string {
				return "arn:aws:batch:us-east-1:000000000000:compute-environment/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(t, h)

			if tt.addTags != nil {
				rec := doRequest(t, h, http.MethodPost, "/v1/tags/"+resourceARN, map[string]any{
					"tags": tt.addTags,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			if tt.removeKeys != nil {
				path := fmt.Sprintf("/v1/tags/%s?tagKeys=%s", resourceARN, tt.removeKeys[0])
				var queryBuilder strings.Builder
				for _, k := range tt.removeKeys[1:] {
					queryBuilder.WriteString("&tagKeys=" + k)
				}
				path += queryBuilder.String()

				rec := doRequest(t, h, http.MethodDelete, path, nil)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/v1/tags/"+resourceARN, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantTags != nil {
				var out map[string]map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, tt.wantTags, out["tags"])
			}
		})
	}
}

func TestHandler_Tags_OnJobQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": "tagged-jq",
		"priority":     1,
		"tags":         map[string]string{"team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var jqOut map[string]string
	mustUnmarshal(t, rec, &jqOut)
	jqARN := jqOut["jobQueueArn"]

	rec = doRequest(t, h, http.MethodGet, "/v1/tags/"+jqARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]map[string]string
	mustUnmarshal(t, rec, &out)
	assert.Equal(t, "platform", out["tags"]["team"])
}

func TestHandler_Tags_OnJobDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "tagged-jd",
		"type":              "container",
		"tags":              map[string]string{"owner": "alice"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var jdOut map[string]any
	mustUnmarshal(t, rec, &jdOut)
	jdARN := jdOut["jobDefinitionArn"].(string)

	rec = doRequest(t, h, http.MethodGet, "/v1/tags/"+jdARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]map[string]string
	mustUnmarshal(t, rec, &out)
	assert.Equal(t, "alice", out["tags"]["owner"])
}

// --- Stub operation tests ---

// TestHandler_Tags_ErrorCases covers the tags endpoint's 400 error paths:
// a missing resource ARN, a malformed JSON body, and tag/untag on a resource
// that doesn't exist.
func TestHandler_Tags_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, h *batch.Handler) string
		name   string
		method string
		body   string
	}{
		{
			name:   "invalid_arn",
			method: http.MethodGet,
			setup:  func(_ *testing.T, _ *batch.Handler) string { return "" },
		},
		{
			name:   "invalid_body",
			method: http.MethodPost,
			body:   "not-json",
			setup: func(t *testing.T, h *batch.Handler) string {
				t.Helper()

				// Create a CE so we have a valid ARN to tag.
				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": "body-ce",
					"type":                   "MANAGED",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return out["computeEnvironmentArn"]
			},
		},
		{
			name:   "untag_not_found",
			method: http.MethodDelete,
			setup: func(_ *testing.T, _ *batch.Handler) string {
				return "arn:aws:batch:us-east-1:000000000000:compute-environment/ghost?tagKeys=k1"
			},
		},
		{
			name:   "tag_not_found",
			method: http.MethodPost,
			body:   `{"tags":{"k":"v"}}`,
			setup: func(_ *testing.T, _ *batch.Handler) string {
				return "arn:aws:batch:us-east-1:000000000000:compute-environment/ghost"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourcePath := tt.setup(t, h)

			e := echo.New()
			req := httptest.NewRequest(tt.method, "/v1/tags/"+resourcePath, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			recW := httptest.NewRecorder()
			c := e.NewContext(req, recW)

			require.NoError(t, h.Handler()(c))
			assert.Equal(t, http.StatusBadRequest, recW.Code)
		})
	}
}

func TestBatch_TagsOnNewResourceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a consumable resource.
	rec := post(t, h, "/v1/createconsumableresource", map[string]any{
		"consumableResourceName": "cr-tag-test",
		"totalQuantity":          int64(5),
		"tags":                   map[string]string{"initial": "value"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]string
	mustUnmarshal(t, rec, &createOut)
	crARN := createOut["consumableResourceArn"]
	require.NotEmpty(t, crARN)

	// Tag the resource.
	e := echo.New()
	tagPath := "/v1/tags/" + crARN
	tagBody := map[string]any{"tags": map[string]string{"env": "prod"}}
	tagBytes, err := json.Marshal(tagBody)
	require.NoError(t, err)

	tagReq := httptest.NewRequest(http.MethodPost, tagPath, bytes.NewReader(tagBytes))
	tagReq.Header.Set("Content-Type", "application/json")
	tagRec := httptest.NewRecorder()
	tagC := e.NewContext(tagReq, tagRec)
	require.NoError(t, h.Handler()(tagC))
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// List tags.
	listReq := httptest.NewRequest(http.MethodGet, tagPath, nil)
	listRec := httptest.NewRecorder()
	listC := e.NewContext(listReq, listRec)
	require.NoError(t, h.Handler()(listC))
	assert.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]map[string]string
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsOut))
	assert.Equal(t, "prod", tagsOut["tags"]["env"])
}

// --- PersistenceWithNewResourceTypes tests ---
