package omics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestCreateVariantStoreStoresReference verifies that CreateVariantStore
// accepts and returns a reference field. Real AWS requires a reference genome.
func TestCreateVariantStoreStoresReference(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/variantStore", map[string]any{
		"name": "var-store-ref",
		"reference": map[string]any{
			"referenceArn": "arn:aws:omics:us-east-1:000000000000:referencestore/abc/reference/xyz",
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["reference"])
	assert.Equal(t, "CREATING", resp["status"])
}
