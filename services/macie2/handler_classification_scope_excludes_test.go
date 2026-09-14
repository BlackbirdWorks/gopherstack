package macie2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// TestGetClassificationScope_ExcludesAlwaysPresent_RealClient drives
// ListClassificationScopes then GetClassificationScope through the real SDK
// client against a freshly-created default classification scope (nothing
// excluded yet). types.S3ClassificationScope.Excludes is "This member is
// required" (macie2@v1.54.4 types/types.go:2532), but the pre-fix default
// scope was seeded with a zero-valued ClassificationScopeS3{} (Excludes
// nil), and the field carried omitempty, so a fresh account's very first
// GetClassificationScope call dropped the required member entirely --
// gopherstack-mven required-output nested-domain-struct sweep.
func TestGetClassificationScope_ExcludesAlwaysPresent_RealClient(t *testing.T) {
	t.Parallel()

	backend := macie2.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMacie2SDKClient(t, macie2.NewHandler(backend))
	ctx := t.Context()

	listed, err := client.ListClassificationScopes(ctx, &macie2sdk.ListClassificationScopesInput{})
	require.NoError(t, err)
	require.Len(t, listed.ClassificationScopes, 1)
	scopeID := *listed.ClassificationScopes[0].Id

	got, err := client.GetClassificationScope(ctx, &macie2sdk.GetClassificationScopeInput{Id: &scopeID})
	require.NoError(t, err)
	require.NotNil(t, got.S3, "S3 must be present on a freshly-created default scope")
	require.NotNil(t, got.S3.Excludes,
		"Excludes is required and must round-trip even for an empty exclusion list")
	assert.Empty(t, got.S3.Excludes.BucketNames)
}
