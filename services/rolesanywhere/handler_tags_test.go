package rolesanywhere_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

// ---- Tag HTTP operations ----

// createTestTrustAnchor creates a trust anchor via the handler's REST surface
// and returns its ARN. TagResource/ListTagsForResource now validate that the
// ARN corresponds to an existing resource (real AWS models
// ResourceNotFoundException for both), so tag-flow tests need a real
// resourceArn rather than an arbitrary string.
func createTestTrustAnchor(t *testing.T, h *rolesanywhere.Handler) string {
	t.Helper()

	rec := doREST(t, h, http.MethodPost, "/trustanchors", map[string]any{
		"name":   t.Name(),
		"source": map[string]any{"sourceType": "CERTIFICATE_BUNDLE"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	ta, ok := resp["trustAnchor"].(map[string]any)
	require.True(t, ok, "response missing trustAnchor: %s", rec.Body.String())

	arn, ok := ta["trustAnchorArn"].(string)
	require.True(t, ok, "trustAnchor missing trustAnchorArn: %v", ta)

	return arn
}

func TestHandler_Tags_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantTagStatus  int
		wantListStatus int
	}{
		{"tag, list, untag resource", http.StatusCreated, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resARN := createTestTrustAnchor(t, h)

			// Tag resource. Real AWS's TagResource responds 201 Created (per
			// the service model's http.responseCode), not 200.
			recTag := doREST(t, h, http.MethodPost, "/TagResource", map[string]any{
				"resourceArn": resARN,
				"tags": []map[string]any{
					{"key": "env", "value": "test"},
					{"key": "team", "value": "security"},
				},
			})
			assert.Equal(t, tt.wantTagStatus, recTag.Code)

			// List tags.
			recList := doREST(t, h, http.MethodGet, "/ListTagsForResource?resourceArn="+resARN, nil)
			assert.Equal(t, tt.wantListStatus, recList.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
			tags := listResp["tags"].([]any)
			assert.Len(t, tags, 2)

			// Untag resource. AWS sends resourceArn/tagKeys as a JSON body on
			// POST /UntagResource, not as query params.
			recUntag := doREST(
				t,
				h,
				http.MethodPost,
				"/UntagResource",
				map[string]any{
					"resourceArn": resARN,
					"tagKeys":     []string{"env"},
				},
			)
			assert.Equal(t, http.StatusOK, recUntag.Code)

			// Verify tag removed.
			recList2 := doREST(
				t,
				h,
				http.MethodGet,
				"/ListTagsForResource?resourceArn="+resARN,
				nil,
			)
			require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listResp))
			tags2 := listResp["tags"].([]any)
			assert.Len(t, tags2, 1)
		})
	}
}

func TestHandler_TagResource_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"tag resource invalid json → 400", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPost,
				"/TagResource",
				bytes.NewReader([]byte(`{invalid`)),
			)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_Tags_UnknownResource proves ListTagsForResource/TagResource
// return 404 ResourceNotFoundException for an ARN that matches no trust
// anchor/profile/CRL, matching real AWS -- a prior version of this test
// asserted the opposite (200 with an empty list), which was itself a stub:
// the tag store was never checked against a real resource at all.
func TestHandler_Tags_UnknownResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{"list tags for unknown resource returns 404", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doREST(
				t,
				h,
				http.MethodGet,
				"/ListTagsForResource?resourceArn=arn:aws:unknown",
				nil,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

// TestHandler_Tags_EmptyList proves a real, taggless resource returns an
// empty (not error) tags list -- distinguishing "resource exists with no
// tags" from "resource does not exist" (TestHandler_Tags_UnknownResource).
func TestHandler_Tags_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	resARN := createTestTrustAnchor(t, h)

	rec := doREST(t, h, http.MethodGet, "/ListTagsForResource?resourceArn="+resARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tags := resp["tags"].([]any)
	assert.Empty(t, tags)
}
