package cloudfront_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestCallerReferenceValidation verifies CallerReference is required.
func TestCallerReferenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantError  string
		body       []byte
		wantStatus int
	}{
		{
			name: "empty_caller_ref_distribution",
			body: []byte(
				`<DistributionConfig><CallerReference></CallerReference><Enabled>true</Enabled></DistributionConfig>`,
			),
			wantStatus: http.StatusBadRequest,
			wantError:  "InvalidArgument",
		},
		{
			name: "missing_caller_ref_oai",
			body: []byte(
				`<CloudFrontOriginAccessIdentityConfig>` +
					`<CallerReference></CallerReference>` +
					`<Comment>no-ref</Comment>` +
					`</CloudFrontOriginAccessIdentityConfig>`,
			),
			wantStatus: http.StatusBadRequest,
			wantError:  "InvalidArgument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var path string
			if strings.Contains(tt.name, "oai") {
				path = "/2020-05-31/origin-access-identity/cloudfront"
			} else {
				path = "/2020-05-31/distribution"
			}

			rec := doXML(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantError)
		})
	}
}

// TestCallerReferenceReuse verifies CloudFront's two different CallerReference-reuse
// policies: Distribution always conflicts on reuse (DistributionAlreadyExists,
// regardless of whether the config content matches -- see the real CreateDistribution
// API docs), while OAI is content-comparison idempotent (identical Comment returns the
// existing OAI; a different Comment conflicts with CloudFrontOriginAccessIdentityAlreadyExists
// -- see the real CloudFrontOriginAccessIdentityConfig.CallerReference doc).
func TestCallerReferenceReuse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "distribution_always_conflicts"},
		{name: "oai_idempotent_when_identical"},
		{name: "oai_conflicts_when_content_differs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			switch tt.name {
			case "distribution_always_conflicts":
				body := minimalDistConfig("idem-ref-001", "idem-dist", true)
				rec1 := doXML(t, h, http.MethodPost, "/2020-05-31/distribution", body)
				require.Equal(t, http.StatusCreated, rec1.Code)

				// Second call with the same CallerReference always conflicts, even
				// though the body is byte-identical to the first request.
				rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/distribution", body)
				assert.Equal(t, http.StatusConflict, rec2.Code)
				assert.Contains(t, rec2.Body.String(), "DistributionAlreadyExists")
				assert.Len(t, h.Backend.ListDistributions(), 1)
			case "oai_idempotent_when_identical":
				body := minimalOAIConfig("idem-oai-ref-001", "idem-oai")
				rec1 := doXML(t, h, http.MethodPost, "/2020-05-31/origin-access-identity/cloudfront", body)
				require.Equal(t, http.StatusCreated, rec1.Code)

				rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/origin-access-identity/cloudfront", body)
				require.Equal(t, http.StatusCreated, rec2.Code)
				assert.Equal(t, rec1.Body.String(), rec2.Body.String())
				assert.Len(t, h.Backend.ListOAIs(), 1)
			case "oai_conflicts_when_content_differs":
				body1 := minimalOAIConfig("idem-oai-ref-002", "first-comment")
				rec1 := doXML(t, h, http.MethodPost, "/2020-05-31/origin-access-identity/cloudfront", body1)
				require.Equal(t, http.StatusCreated, rec1.Code)

				body2 := minimalOAIConfig("idem-oai-ref-002", "different-comment")
				rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/origin-access-identity/cloudfront", body2)
				assert.Equal(t, http.StatusConflict, rec2.Code)
				assert.Contains(t, rec2.Body.String(), "CloudFrontOriginAccessIdentityAlreadyExists")
				assert.Len(t, h.Backend.ListOAIs(), 1)
			}
		})
	}
}

// TestDeleteDistributionCleansUp verifies aliases/webACLs are removed on delete.
func TestDeleteDistributionCleansUp(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	d, err := b.CreateDistribution("ref-del-cleanup", "del-dist", false, nil)
	require.NoError(t, err)

	err = b.AssociateAlias(d.ID, "cleanup.example.com")
	require.NoError(t, err)

	err = b.AssociateDistributionWebACL(d.ID, "arn:aws:wafv2:us-east-1:123:webacl/test")
	require.NoError(t, err)

	// Delete requires ETag via handler; do directly via backend.
	h := cloudfront.NewHandler(b)
	// Get ETag for delete.
	rec := doXML(t, h, http.MethodGet, "/2020-05-31/distribution/"+d.ID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	etag := rec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	rec = doXMLWithHeaders(t, h, http.MethodDelete, "/2020-05-31/distribution/"+d.ID, nil,
		map[string]string{"If-Match": etag})
	require.Equal(t, http.StatusNoContent, rec.Code)

	// After deletion, CallerReference re-use should create a new distribution.
	d2, err := b.CreateDistribution("ref-del-cleanup", "new-dist", true, nil)
	require.NoError(t, err)
	assert.NotEqual(t, d.ID, d2.ID, "new distribution should have different ID after callerRef is freed")
}

// TestListDistributions_SortedOutput verifies sorted listing results.
func TestListDistributions_SortedOutput(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	// Create multiple distributions.
	refs := []string{"s-ref-001", "s-ref-002", "s-ref-003"}
	for _, ref := range refs {
		_, err := b.CreateDistribution(ref, ref, true, nil)
		require.NoError(t, err)
	}

	dists := b.ListDistributions()
	require.Len(t, dists, 3)

	for i := 1; i < len(dists); i++ {
		assert.LessOrEqual(t, dists[i-1].ID, dists[i].ID,
			"distributions should be sorted by ID")
	}

	// Create multiple OAIs.
	for _, ref := range refs {
		_, err := b.CreateOAI(ref+"-oai", "comment")
		require.NoError(t, err)
	}

	oais := b.ListOAIs()
	require.Len(t, oais, 3)

	for i := 1; i < len(oais); i++ {
		assert.LessOrEqual(t, oais[i-1].ID, oais[i].ID,
			"OAIs should be sorted by ID")
	}
}

// TestAliasCountInListDistributions verifies alias count is reflected in list output.
func TestAliasCountInListDistributions(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
	h := cloudfront.NewHandler(b)

	d, err := b.CreateDistribution("ref-alias-list", "alias-list-dist", true, nil)
	require.NoError(t, err)

	err = b.AssociateAlias(d.ID, "www.example.com")
	require.NoError(t, err)

	err = b.AssociateAlias(d.ID, "api.example.com")
	require.NoError(t, err)

	rec := doXML(t, h, http.MethodGet, "/2020-05-31/distribution", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Quantity>2</Quantity>")
}

// TestCreateDistributionValidation verifies CallerReference is validated.
func TestCreateDistributionValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := []byte(
		`<DistributionConfig><CallerReference></CallerReference>` +
			`<Comment>no-ref</Comment><Enabled>true</Enabled></DistributionConfig>`,
	)
	rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidArgument")
}
