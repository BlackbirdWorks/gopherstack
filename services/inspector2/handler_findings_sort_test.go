package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// TestListFindings_SortCriteria_AwsAccountId proves ListFindings honours
// SortCriteria. ListFindingsInput.SortCriteria (api_op_ListFindings.go,
// inspector2@v1.54.1) was previously parsed nowhere -- decodeFilterListRequest
// (handler.go) had no sortCriteria member at all, so every ListFindings
// response came back in FindingArn order regardless of what a client
// requested.
func TestListFindings_SortCriteria_AwsAccountId(t *testing.T) {
	t.Parallel()

	backend := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := inspector2.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	// Seeded out of AccountID order, so an ARN-order (or seed-order) default
	// would not coincidentally match the sorted assertion below.
	accounts := []string{"333333333333", "111111111111", "222222222222"}
	for _, acct := range accounts {
		_, err := h.Backend.SeedFinding(inspector2.Finding{AccountID: acct})
		require.NoError(t, err)
	}

	ascending, err := client.ListFindings(ctx, &inspector2sdk.ListFindingsInput{
		SortCriteria: &types.SortCriteria{Field: types.SortFieldAwsAccountId, SortOrder: types.SortOrderAsc},
	})
	require.NoError(t, err)
	require.Len(t, ascending.Findings, len(accounts))

	gotAscending := make([]string, len(ascending.Findings))
	for i, f := range ascending.Findings {
		gotAscending[i] = *f.AwsAccountId
	}

	assert.Equal(t, []string{"111111111111", "222222222222", "333333333333"}, gotAscending)

	descending, err := client.ListFindings(ctx, &inspector2sdk.ListFindingsInput{
		SortCriteria: &types.SortCriteria{Field: types.SortFieldAwsAccountId, SortOrder: types.SortOrderDesc},
	})
	require.NoError(t, err)

	gotDescending := make([]string, len(descending.Findings))
	for i, f := range descending.Findings {
		gotDescending[i] = *f.AwsAccountId
	}

	assert.Equal(t, []string{"333333333333", "222222222222", "111111111111"}, gotDescending)
}

// TestListFindings_FilterCriteria_ResourceId proves ListFindings honours
// FilterCriteria.ResourceId. FilterCriteria (api_op_ListFindings.go,
// inspector2@v1.54.1's types.FilterCriteria) declares ResourceId as a
// []StringFilter, which maps directly to Finding.ResourceID -- previously
// parseFindingFilterCriteria only recognized severity/findingType/
// findingStatus/awsAccountId, so resourceId narrowed nothing.
func TestListFindings_FilterCriteria_ResourceId(t *testing.T) {
	t.Parallel()

	backend := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := inspector2.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	wanted, err := h.Backend.SeedFinding(inspector2.Finding{ResourceID: "i-wanted"})
	require.NoError(t, err)

	_, err = h.Backend.SeedFinding(inspector2.Finding{ResourceID: "i-other"})
	require.NoError(t, err)

	out, err := client.ListFindings(ctx, &inspector2sdk.ListFindingsInput{
		FilterCriteria: &types.FilterCriteria{
			ResourceId: []types.StringFilter{
				{Comparison: types.StringComparisonEquals, Value: aws.String("i-wanted")},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Findings, 1)
	assert.Equal(t, wanted.FindingArn, *out.Findings[0].FindingArn)
}
