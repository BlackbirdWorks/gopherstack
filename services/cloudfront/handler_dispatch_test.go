package cloudfront_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestNewDispatchRefactoring verifies the refactored dispatch helpers route all operations correctly.
func TestNewDispatchRefactoring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		name       string
		method     string
		path       string
		wantStatus int
	}{
		// dispatchGetDistributionAndCachePolicyOps
		{
			name:   "get_cache_policy_routed",
			method: http.MethodGet,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("route-cp", "c", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "delete_cache_policy_routed",
			method: http.MethodDelete,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateCachePolicy("del-route-cp", "c", 86400, 31536000, 0)
				require.NoError(t, err)

				return "/2020-05-31/cache-policy/" + p.ID
			},
			// 412 PreconditionFailed confirms route reached handleDeleteCachePolicy (missing If-Match)
			wantStatus: http.StatusPreconditionFailed,
		},
		// dispatchGetIdentityAndPolicyDeleteOps
		{
			name:   "get_oai_config_routed",
			method: http.MethodGet,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oai, err := h.Backend.CreateOAI("route-oai-ref", "route oai")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-identity/cloudfront/" + oai.ID + "/config"
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "delete_distribution_routed",
			method: http.MethodDelete,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("del-route-ref", "del-route-dist", false, nil)
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			// 412 PreconditionFailed confirms route reached handleDeleteDistribution (missing If-Match)
			wantStatus: http.StatusPreconditionFailed,
		},
		// dispatchUpdateDeletePolicyAndOAIOps
		{
			name:   "delete_oai_routed",
			method: http.MethodDelete,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				oai, err := h.Backend.CreateOAI("del-route-oai-ref", "del-route-oai")
				require.NoError(t, err)

				return "/2020-05-31/origin-access-identity/cloudfront/" + oai.ID
			},
			// 412 PreconditionFailed confirms route reached handleDeleteOAI (missing If-Match header)
			wantStatus: http.StatusPreconditionFailed,
		},
		// dispatchFieldLevelEncryptionOps
		{
			name:   "get_fle_routed",
			method: http.MethodGet,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				fle, err := h.Backend.CreateFieldLevelEncryption("route-fle-cfg", "comment", nil)
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption/" + fle.ID
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "get_fle_profile_routed",
			method: http.MethodGet,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateFieldLevelEncryptionProfile("route-fle-profile", "comment", nil)
				require.NoError(t, err)

				return "/2020-05-31/field-level-encryption-profile/" + p.ID
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := ""
			if tt.setup != nil {
				path = tt.setup(t, h)
			}

			rec := doXML(t, h, tt.method, path, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestUnknownOperation verifies that unknown operations return 404.
func TestUnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	// Use an unrecognized path
	rec := doXML(t, h, http.MethodPatch, "/2020-05-31/distribution", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchOperation")
}

// TestMalformedXMLHandling verifies that malformed XML returns 400.
func TestMalformedXMLHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{
			name: "create_distribution_bad_xml",
			path: "/2020-05-31/distribution",
		},
		{
			name: "create_oai_bad_xml",
			path: "/2020-05-31/origin-access-identity/cloudfront",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doXML(t, h, http.MethodPost, tt.path, []byte(`<<<not xml`))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestRefinement1_ErrorMapping verifies handleError maps sentinel errors to correct HTTP codes.
func TestRefinement1_ErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantCode   string
		body       []byte
		wantStatus int
	}{
		{
			name:       "distribution_not_found",
			method:     http.MethodGet,
			path:       "/2020-05-31/distribution/NOTEXIST",
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchDistribution",
		},
		{
			name:       "oai_not_found",
			method:     http.MethodGet,
			path:       "/2020-05-31/origin-access-identity/cloudfront/NOTEXIST",
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchCloudFrontOriginAccessIdentity",
		},
		{
			name:   "cache_policy_duplicate",
			method: http.MethodPost,
			path:   "/2020-05-31/cache-policy",
			body: []byte(
				`<CachePolicyConfig><Name>dup-policy</Name>` +
					`<DefaultTTL>100</DefaultTTL><MaxTTL>200</MaxTTL><MinTTL>0</MinTTL>` +
					`</CachePolicyConfig>`,
			),
			wantStatus: http.StatusConflict,
			// The real aws-sdk-go-v2 cloudfront error type for a cache policy name
			// collision is CachePolicyAlreadyExists, not the generic
			// DistributionAlreadyExists the emulator used to return for every
			// AlreadyExists collision regardless of which resource type collided.
			wantCode: "CachePolicyAlreadyExists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// For the cache policy duplicate test, create it first.
			if tt.wantCode == "CachePolicyAlreadyExists" {
				rec := doXML(t, h, http.MethodPost, tt.path, tt.body)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doXML(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantCode)
		})
	}
}
