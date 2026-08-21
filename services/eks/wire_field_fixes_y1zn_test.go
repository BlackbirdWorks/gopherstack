package eks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAddon_NoMarketplaceVersionOrResolveConflicts_RealClient covers
// gopherstack-y1zn. addonToJSON emitted "marketplaceVersion" and
// "resolveConflicts"; types.Addon (eks@v1.90.4 deserializers.go's
// awsRestjson1_deserializeDocumentAddon) has neither -- the real Marketplace
// field is the nested "marketplaceInformation" object, and resolveConflicts
// is a CreateAddon/UpdateAddon request-only member, never echoed back. A
// typed client silently ignores both unknown keys, so the proof is the raw
// body.
func TestAddon_NoMarketplaceVersionOrResolveConflicts_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "y1zn-c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/y1zn-c1/addons", map[string]any{
		"addonName":        "vpc-cni",
		"resolveConflicts": "OVERWRITE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"marketplaceVersion"`,
		"types.Addon has no marketplaceVersion member")
	assert.NotContains(t, body, `"resolveConflicts"`,
		"types.Addon has no resolveConflicts member; it is request-only")
}
