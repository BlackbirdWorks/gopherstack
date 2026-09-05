package kms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestListAliases_SDKRoundTrip_StaleMarkerPastEndTerminates drives
// ListAliases through the real aws-sdk-go-v2/service/kms client with a
// Marker decoding to an offset past the current alias count -- proving
// paginateTagList's shared parseMarker/offset-clamp pattern (services/kms/
// handler_tags.go, store.go), which every List op in this package
// duplicates inline, degrades to an empty page instead of panicking or
// resetting to page one. Ties the direct-helper proof in
// pagination_arithmetic_internal_test.go to observable behaviour through
// the typed SDK client.
func TestListAliases_SDKRoundTrip_StaleMarkerPastEndTerminates(t *testing.T) {
	t.Parallel()

	h := kms.NewHandler(kms.NewInMemoryBackend())
	client := newTestKMSClient(t, h)

	keyOut, err := client.CreateKey(t.Context(), &kmssdk.CreateKeyInput{})
	require.NoError(t, err)

	_, err = client.CreateAlias(t.Context(), &kmssdk.CreateAliasInput{
		AliasName:   aws.String("alias/pagination-test"),
		TargetKeyId: keyOut.KeyMetadata.KeyId,
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		out, listErr := client.ListAliases(t.Context(), &kmssdk.ListAliasesInput{
			Marker: aws.String("9999"),
		})
		require.NoError(t, listErr)
		assert.Empty(t, out.Aliases, "a marker past the current alias count must return an empty page")
		assert.Nil(t, out.NextMarker)
		assert.False(t, out.Truncated)
	})
}
