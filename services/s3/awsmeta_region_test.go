package s3_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

// TestParity_Region_FromAwsmeta verifies the handler sources the request region
// from the central awsmeta context (the single source of identity), taking
// precedence over the local X-Amz-Region fallback.
func TestRegion_FromAwsmeta(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/awsmeta-bucket", nil)
	// awsmeta says eu-west-1; the header says us-west-2 — awsmeta must win.
	req.Header.Set("X-Amz-Region", "us-west-2")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{
		Region:  "eu-west-1",
		Account: awsmeta.DefaultAccount,
	}))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/awsmeta-bucket?location", nil)
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{
		Region:  "eu-west-1",
		Account: awsmeta.DefaultAccount,
	}))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "eu-west-1")
}

func TestHandler_GetBucketLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		path        string
		wantContain string
		wantAbsent  string
	}{
		{
			// AWS returns an empty LocationConstraint for us-east-1 buckets.
			name:        "us-east-1 returns empty LocationConstraint",
			bucket:      "bkt",
			path:        "/bkt?location",
			wantContain: "LocationConstraint",
			wantAbsent:  "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
			if tt.wantAbsent != "" {
				assert.NotContains(t, rec.Body.String(), tt.wantAbsent)
			}
		})
	}
}

func TestHandler_BucketLocationQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "bucket location returns LocationConstraint"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodGet, "/bkt?location", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "LocationConstraint")
		})
	}
}
