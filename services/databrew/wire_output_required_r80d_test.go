package databrew_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"
	"github.com/aws/aws-sdk-go-v2/service/databrew/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// Test_SDKRoundTrip_DescribeDataset_InputRequiredButReachablyEmpty proves
// gopherstack-r80d batch 11's finding: DescribeDatasetOutput.Input and
// ListDatasetsOutput.Datasets[].Input (types.Dataset.Input) are both marked
// "This member is required." in the pinned SDK
// (api_op_DescribeDataset.go:39-45, types/types.go:230-236), but a real
// client can construct a types.Input with none of
// S3InputDefinition/DataCatalogInputDefinition/DatabaseInputDefinition set --
// the SDK's own client-side validateInput (validators.go:1271-1296) only
// validates whichever branch is non-nil, it never requires at least one
// branch to be present, and validateOpCreateDatasetInput/
// validateOpUpdateDatasetInput (validators.go:1695-1718 and the UpdateDataset
// equivalent) only require the *Input pointer itself to be non-nil. Once
// stored, Dataset.Input (models.go) was tagged `json:"Input,omitzero"` --
// with all three sub-fields nil, DatasetInput's zero value is empty, so
// omitzero dropped the whole required "Input" key from both DescribeDataset
// and ListDatasets, and a real client silently decoded a nil Input for a
// dataset that genuinely exists.
func Test_SDKRoundTrip_DescribeDataset_InputRequiredButReachablyEmpty(t *testing.T) {
	t.Parallel()

	backend := databrew.NewInMemoryBackend("000000000000", rtTestRegion)
	h := databrew.NewHandler(backend)
	client := newRoundTripClient(t, h)

	_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
		Name:  aws.String("bare-input-ds"),
		Input: &types.Input{},
	})
	require.NoError(t, err)

	t.Run("describedataset", func(t *testing.T) {
		t.Parallel()

		descOut, descErr := client.DescribeDataset(t.Context(), &databrewsdk.DescribeDatasetInput{
			Name: aws.String("bare-input-ds"),
		})
		require.NoError(t, descErr)
		require.NotNil(t, descOut.Input, "required member Input must be present even when no branch was set")
	})

	t.Run("listdatasets", func(t *testing.T) {
		t.Parallel()

		listOut, listErr := client.ListDatasets(t.Context(), &databrewsdk.ListDatasetsInput{})
		require.NoError(t, listErr)
		require.Len(t, listOut.Datasets, 1)
		require.NotNil(
			t,
			listOut.Datasets[0].Input,
			"required member Input must be present even when no branch was set",
		)
	})
}
