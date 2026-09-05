package omics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

// TestListReferenceStores_SDKRoundTrip_PaginationSurvivesDeleteBetweenPages
// drives ListReferenceStores through the real aws-sdk-go-v2 omics client,
// deleting the store named by the returned NextToken before fetching the
// next page -- the scenario this pass found paginateStrings
// (services/omics/store.go, shared by paginatedCopies and 20 List*
// operations built on it) mishandling: an exact-match cursor lookup fell
// back to offset 0 whenever the named id was no longer present, restarting
// pagination from the beginning and re-delivering already-seen stores. Ties
// the unit-level reproduction in pagination_arithmetic_test.go to
// observable behaviour through the typed SDK client and its own
// deserializer.
func TestListReferenceStores_SDKRoundTrip_PaginationSurvivesDeleteBetweenPages(t *testing.T) {
	t.Parallel()

	backend := omics.NewInMemoryBackend("123456789012", "us-east-1")
	h := omics.NewHandler(backend)
	client := newTestOmicsClient(t, h)

	for i := range 8 {
		_, err := client.CreateReferenceStore(t.Context(), &omicssdk.CreateReferenceStoreInput{
			Name: aws.String("store-" + string(rune('a'+i))),
		})
		require.NoError(t, err)
	}

	page1, err := client.ListReferenceStores(t.Context(), &omicssdk.ListReferenceStoresInput{
		MaxResults: aws.Int32(5),
	})
	require.NoError(t, err)
	require.Len(t, page1.ReferenceStores, 5)
	require.NotNil(t, page1.NextToken)

	// Delete the store the cursor names before fetching the next page.
	staleID := aws.ToString(page1.NextToken)
	_, err = client.DeleteReferenceStore(t.Context(), &omicssdk.DeleteReferenceStoreInput{Id: aws.String(staleID)})
	require.NoError(t, err)

	page2, err := client.ListReferenceStores(t.Context(), &omicssdk.ListReferenceStoresInput{
		MaxResults: aws.Int32(5),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)

	seen := make(map[string]bool, len(page1.ReferenceStores))
	for _, s := range page1.ReferenceStores {
		seen[aws.ToString(s.Id)] = true
	}

	for _, s := range page2.ReferenceStores {
		assert.False(t, seen[aws.ToString(s.Id)],
			"page2 must not repeat id %q already returned in page1", aws.ToString(s.Id))
	}

	assert.Len(t, page1.ReferenceStores, 5)
	assert.Len(t, page2.ReferenceStores, 2, "7 remaining stores after 1 delete, page1 took 5, page2 gets the rest")
}
