package emrserverless_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags   map[string]string
		setup      func(h *emrserverless.Handler) string
		name       string
		wantStatus int
	}{
		{
			name:       "success_with_tags",
			wantStatus: http.StatusOK,
			wantTags:   map[string]string{"env": "test"},
			setup: func(h *emrserverless.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/applications", map[string]any{
					"name":         "tagged-app",
					"type":         "SPARK",
					"releaseLabel": "emr-6.6.0",
					"tags":         map[string]string{"env": "test"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return out["arn"]
			},
		},
		{
			name:       "not_found",
			wantStatus: http.StatusNotFound,
			setup: func(_ *emrserverless.Handler) string {
				return "arn:aws:emr-serverless:us-east-1:000000000000:/applications/nonexistent"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			rec := doRequest(t, h, http.MethodGet, "/tags/"+resourceARN, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				tags := out["tags"].(map[string]any)
				for k, v := range tt.wantTags {
					assert.Equal(t, v, tags[k])
				}
			}
		})
	}
}

// TestHandler_ListTagsForResource_NilTagsApplication verifies an application
// seeded with a nil Tags map returns an empty tags object, not null.
func TestHandler_ListTagsForResource_NilTagsApplication(t *testing.T) {
	t.Parallel()

	// Create an application without tags (nil Tags map seeded directly).
	b := emrserverless.NewInMemoryBackend("000000000000", "us-east-1")
	now := time.Now().UTC()
	appID := "nil-tags-app"
	app := &emrserverless.Application{
		ApplicationID: appID,
		Arn:           "arn:aws:emr-serverless:us-east-1:000000000000:/applications/" + appID,
		Name:          "nil-tags",
		Type:          "SPARK",
		State:         emrserverless.ApplicationStateCreated,
		CreatedAt:     now,
		UpdatedAt:     now,
		Tags:          nil,
	}
	b.AddApplicationInternal(app)

	h := emrserverless.NewHandler(b)
	rec := doRequest(t, h, http.MethodGet, "/tags/"+app.Arn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Should return an empty tags object, not null.
	assert.Contains(t, rec.Body.String(), `"tags":{}`)
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags       map[string]string
		setup      func(h *emrserverless.Handler) string
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			tags:       map[string]string{"new-key": "new-val"},
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) string {
				appID := createApp(t, h, "tag-app")
				rec := doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				app := out["application"].(map[string]any)

				return app["arn"].(string)
			},
		},
		{
			name:       "not_found",
			tags:       map[string]string{"k": "v"},
			wantStatus: http.StatusNotFound,
			setup: func(_ *emrserverless.Handler) string {
				return "arn:aws:emr-serverless:us-east-1:000000000000:/applications/nonexistent"
			},
		},
		{
			name:       "invalid_body",
			wantStatus: http.StatusBadRequest,
			setup: func(h *emrserverless.Handler) string {
				appID := createApp(t, h, "tag-app-invalid")
				rec := doRequest(t, h, http.MethodGet, "/applications/"+appID, nil)
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				app := out["application"].(map[string]any)

				return app["arn"].(string)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			var rec *httptest.ResponseRecorder
			if tt.name == "invalid_body" {
				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/tags/"+resourceARN, strings.NewReader("not-json"))
				req.Header.Set("Content-Type", "application/json")
				rec2 := httptest.NewRecorder()
				c := e.NewContext(req, rec2)
				err := h.Handler()(c)
				require.NoError(t, err)
				rec = rec2
			} else {
				rec = doRequest(t, h, http.MethodPost, "/tags/"+resourceARN, map[string]any{"tags": tt.tags})
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *emrserverless.Handler) (arn, query string)
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			setup: func(h *emrserverless.Handler) (string, string) {
				rec := doRequest(t, h, http.MethodPost, "/applications", map[string]any{
					"name":         "untag-app",
					"type":         "SPARK",
					"releaseLabel": "emr-6.6.0",
					"tags":         map[string]string{"remove-me": "val"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]string
				mustUnmarshal(t, rec, &out)

				return out["arn"], "?tagKeys=remove-me"
			},
		},
		{
			name:       "not_found",
			wantStatus: http.StatusNotFound,
			setup: func(_ *emrserverless.Handler) (string, string) {
				return "arn:aws:emr-serverless:us-east-1:000000000000:/applications/nonexistent", "?tagKeys=k"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN, query := tt.setup(h)

			rec := doRequest(t, h, http.MethodDelete, "/tags/"+resourceARN+query, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TagsOnJobRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	appID := createApp(t, h, "jr-tags-app")

	rec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
		"executionRoleArn": "arn:aws:iam::000000000000:role/r",
		"name":             "tagged-jr",
		"tags":             map[string]string{"key1": "val1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var jrOut map[string]string
	mustUnmarshal(t, rec, &jrOut)
	jrARN := jrOut["arn"]

	// List tags.
	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+jrARN, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagOut map[string]any
	mustUnmarshal(t, rec2, &tagOut)
	tags := tagOut["tags"].(map[string]any)
	assert.Equal(t, "val1", tags["key1"])

	// Add a tag.
	rec3 := doRequest(t, h, http.MethodPost, "/tags/"+jrARN, map[string]any{
		"tags": map[string]string{"key2": "val2"},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	// Untag.
	rec4 := doRequest(t, h, http.MethodDelete, "/tags/"+jrARN+"?tagKeys=key1", nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	// Verify.
	rec5 := doRequest(t, h, http.MethodGet, "/tags/"+jrARN, nil)
	var final map[string]any
	mustUnmarshal(t, rec5, &final)
	finalTags := final["tags"].(map[string]any)
	assert.NotContains(t, finalTags, "key1")
	assert.Equal(t, "val2", finalTags["key2"])
}
