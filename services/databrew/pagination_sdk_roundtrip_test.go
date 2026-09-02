package databrew_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// TestListDatasets_SDKRoundTrip_BoundaryWalk drives ListDatasets through the
// real aws-sdk-go-v2 DataBrew client, exercising the shared paginateKeys
// helper (services/databrew/paginate_helper.go) -- verified for pure
// arithmetic in paginate_helper_internal_test.go and found clean, with no
// bug to report -- end-to-end through the typed client's own
// serializer/deserializer. Confirms concatenating every page reproduces the
// full dataset name set with no drops or duplicates.
func TestListDatasets_SDKRoundTrip_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := databrew.NewHandler(b)
	client := newRoundTripClient(t, h)
	ctx := context.Background()

	want := make(map[string]bool, 9)

	for i := range 9 {
		name := "ds-" + string(rune('a'+i))
		_, err := b.CreateDataset(
			ctx, name, "CSV", s3Input("bucket", name+"/"), databrew.DatasetFormatOptions{}, nil, nil,
		)
		require.NoError(t, err)
		want[name] = true
	}

	collected := make(map[string]bool, 9)

	var nextToken *string

	for {
		out, err := client.ListDatasets(t.Context(), &databrewsdk.ListDatasetsInput{
			MaxResults: aws.Int32(4),
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		for _, ds := range out.Datasets {
			name := aws.ToString(ds.Name)
			require.False(t, collected[name], "duplicate dataset %q returned across pages", name)
			collected[name] = true
		}

		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}

		nextToken = out.NextToken
	}

	require.Equal(t, want, collected)
}
