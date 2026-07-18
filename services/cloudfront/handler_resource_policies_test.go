package cloudfront_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResourcePolicy_NotFound verifies GetResourcePolicy 404s when no policy has
// been put for a resource ARN, and succeeds once one has been.
func TestResourcePolicy_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"
	const arn = "arn:aws:cloudfront::123456789012:distribution/ENOPOLICY"

	getRec := doXML(t, h, http.MethodGet, prefix+"resource-policy?arn="+arn, nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "NoSuchResourcePolicy")

	putBody := fmt.Sprintf(
		`<ResourcePolicy><Policy>{"Version":"2012-10-17"}</Policy><ResourceArn>%s</ResourceArn></ResourcePolicy>`,
		arn,
	)
	putRec := doXML(t, h, http.MethodPost, prefix+"resource-policy", []byte(putBody))
	require.Equal(t, http.StatusOK, putRec.Code)

	getAfterPut := doXML(t, h, http.MethodGet, prefix+"resource-policy?arn="+arn, nil)
	assert.Equal(t, http.StatusOK, getAfterPut.Code)
	assert.Contains(t, getAfterPut.Body.String(), `"Version":"2012-10-17"`)
}

// ---------------------------------------------------------------------------
// GetManagedCertificateDetails
// ---------------------------------------------------------------------------

// TestResourcePolicy_CRUD tests resource policy Put/Get/Delete.
func TestResourcePolicy_CRUD(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const arn = "arn:aws:cloudfront::123456789012:distribution/E1"
	const prefix = "/2020-05-31/"

	// Put
	putBody := fmt.Sprintf(
		`<ResourcePolicy><Policy>{"Version":"2012-10-17"}</Policy><ResourceArn>%s</ResourceArn></ResourcePolicy>`,
		arn,
	)
	cfOK(t, h, http.MethodPost, prefix+"resource-policy", putBody)

	// Get
	out := cfOK(t, h, http.MethodGet, prefix+"resource-policy?arn="+arn, "")
	if !strings.Contains(out, "ResourcePolicy") {
		t.Errorf("unexpected response: %s", out)
	}

	// Delete
	cfOK(t, h, http.MethodDelete, prefix+"resource-policy?arn="+arn, "")
}
