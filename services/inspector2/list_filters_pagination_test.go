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

// TestListFilters_Pagination covers gopherstack-o5of: handleListFilters
// parsed only arns/action, never maxResults/nextToken (real body params,
// ListFilters serializers.go:4896-4923).
func TestListFilters_Pagination(t *testing.T) {
	t.Parallel()

	backend := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
	h := inspector2.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	names := make([]string, 0, 3)

	for _, n := range []string{"pg-filter-a", "pg-filter-b", "pg-filter-c"} {
		_, err := client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
			Name:           aws.String(n),
			Action:         types.FilterActionNone,
			FilterCriteria: &types.FilterCriteria{},
		})
		require.NoError(t, err)
		names = append(names, n)
	}

	page1, err := client.ListFilters(ctx, &inspector2sdk.ListFiltersInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.Filters, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListFilters(ctx, &inspector2sdk.ListFiltersInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Filters, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListFilters(ctx, &inspector2sdk.ListFiltersInput{
		MaxResults: aws.Int32(1),
		NextToken:  page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.Filters, 1)
	assert.Nil(t, page3.NextToken)

	got := make([]string, 0, 3)
	for _, page := range [][]types.Filter{page1.Filters, page2.Filters, page3.Filters} {
		for _, f := range page {
			got = append(got, aws.ToString(f.Name))
		}
	}

	assert.ElementsMatch(t, names, got)
}

// TestListFilters_ActionAndArnsNarrow covers gopherstack-o5of's filter side:
// action and arns already narrowed the result set (before this fix, the
// only bug was missing pagination), this locks that behavior in alongside it.
func TestListFilters_ActionAndArnsNarrow(t *testing.T) {
	t.Parallel()

	backend := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
	h := inspector2.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	none, setupErr := client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
		Name:           aws.String("narrow-none"),
		Action:         types.FilterActionNone,
		FilterCriteria: &types.FilterCriteria{},
	})
	require.NoError(t, setupErr)

	_, setupErr = client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
		Name:           aws.String("narrow-suppress"),
		Action:         types.FilterActionSuppress,
		FilterCriteria: &types.FilterCriteria{},
	})
	require.NoError(t, setupErr)

	t.Run("action_narrows", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListFilters(ctx, &inspector2sdk.ListFiltersInput{Action: types.FilterActionSuppress})
		require.NoError(t, err)
		require.Len(t, out.Filters, 1)
		assert.Equal(t, "narrow-suppress", aws.ToString(out.Filters[0].Name))
	})

	t.Run("arns_narrows", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListFilters(ctx, &inspector2sdk.ListFiltersInput{Arns: []string{aws.ToString(none.Arn)}})
		require.NoError(t, err)
		require.Len(t, out.Filters, 1)
		assert.Equal(t, aws.ToString(none.Arn), aws.ToString(out.Filters[0].Arn))
	})

	t.Run("no_filter_returns_both", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListFilters(ctx, &inspector2sdk.ListFiltersInput{})
		require.NoError(t, err)
		assert.Len(t, out.Filters, 2)
	})
}
