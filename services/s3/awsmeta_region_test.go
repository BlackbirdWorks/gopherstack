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
func TestParity_Region_FromAwsmeta(t *testing.T) {
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
