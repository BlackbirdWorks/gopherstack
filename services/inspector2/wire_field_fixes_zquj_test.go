package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/require"
)

// TestListFilters_CriteriaKey_RealSDKClient proves gopherstack-zquj:
// ListFilters wrote the per-item criteria under "filterCriteria" -- the real
// name of CreateFilterInput's request parameter (api_op_CreateFilter.go) --
// but the real Filter deserializer switches on "criteria"
// (deserializers.go's awsRestjson1_deserializeDocumentFilter, inspector2
// SDK), so every real client decoded Filter.Criteria as nil regardless of
// what was set at creation time.
func TestListFilters_CriteriaKey_RealSDKClient(t *testing.T) {
	t.Parallel()

	client := newRoundTripTestClient(t)
	ctx := t.Context()

	_, err := client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
		Name:   aws.String("zquj-criteria-filter"),
		Action: types.FilterActionNone,
		FilterCriteria: &types.FilterCriteria{
			AwsAccountId: []types.StringFilter{
				{Comparison: types.StringComparisonEquals, Value: aws.String("123456789012")},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.ListFilters(ctx, &inspector2sdk.ListFiltersInput{})
	require.NoError(t, err)
	require.Len(t, out.Filters, 1)
	require.NotNil(t, out.Filters[0].Criteria)
	require.NotEmpty(t, out.Filters[0].Criteria.AwsAccountId)
	require.Equal(t, "123456789012", aws.ToString(out.Filters[0].Criteria.AwsAccountId[0].Value))
}
