package omics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

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

// createSharePair creates one share on an annotation store resource and one
// on a variant store resource, returning their shareIds for the filter tests
// below (ListShares derives the real ShareResourceType from the ARN).
func createSharePair(t *testing.T, h *omics.Handler) map[string]string {
	t.Helper()

	resources := map[string]string{
		"annotation": "arn:aws:omics:us-east-1:000000000000:annotationStore/share-ann",
		"variant":    "arn:aws:omics:us-east-1:000000000000:variantStore/share-var",
	}
	ids := map[string]string{}

	for key, resourceARN := range resources {
		rec := doRequest(t, h, http.MethodPost, "/share", map[string]any{
			"resourceArn":         resourceARN,
			"principalSubscriber": "123456789012",
			"shareName":           key + "-share",
		})
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		ids[key], _ = resp["shareId"].(string)
	}

	return ids
}

func shareIDs(t *testing.T, raw any) []string {
	t.Helper()

	items, ok := raw.([]any)
	require.True(t, ok)

	ids := make([]string, 0, len(items))

	for _, item := range items {
		m, itemOK := item.(map[string]any)
		require.True(t, itemOK)
		ids = append(ids, m["shareId"].(string))
	}

	return ids
}

// TestListShares_Filters verifies ListShares applies its resourceArns/status/
// type body filter (real AWS ListSharesInput body "filter", types.Filter).
func TestListShares_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reqBody func(ids map[string]string) map[string]any
		name    string
		wantKey string
	}{
		{
			name: "type filter alone matches annotation store share",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{"type": []string{"ANNOTATION_STORE"}}}
			},
			wantKey: "annotation",
		},
		{
			name: "resourceArns filter alone matches variant store share",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{
					"resourceArns": []string{"arn:aws:omics:us-east-1:000000000000:variantStore/share-var"},
				}}
			},
			wantKey: "variant",
		},
		{
			name: "type and status combined match",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{
					"type":   []string{"ANNOTATION_STORE"},
					"status": []string{"PENDING"},
				}}
			},
			wantKey: "annotation",
		},
		{
			name: "type and status combined mismatch returns empty",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{
					"type":   []string{"ANNOTATION_STORE"},
					"status": []string{"ACTIVE"},
				}}
			},
			wantKey: "",
		},
		{
			name: "unmatched type returns empty",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{"type": []string{"WORKFLOW"}}}
			},
			wantKey: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ids := createSharePair(t, h)

			rec := doRequest(t, h, http.MethodPost, "/shares", tc.reqBody(ids))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			var want []string
			if tc.wantKey != "" {
				want = []string{ids[tc.wantKey]}
			}

			assert.ElementsMatch(t, want, shareIDs(t, resp["shares"]))
		})
	}
}
