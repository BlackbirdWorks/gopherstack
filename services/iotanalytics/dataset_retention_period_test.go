package iotanalytics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotanalyticssdk "github.com/aws/aws-sdk-go-v2/service/iotanalytics"
	iotanalyticstypes "github.com/aws/aws-sdk-go-v2/service/iotanalytics/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// TestDataset_RetentionPeriod_RoundTrips verifies that RetentionPeriod --
// a real, settable CreateDatasetInput/CreateDatasetOutput/Dataset member
// (deserializers.go's awsRestjson1_deserializeDocumentDataset, confirmed
// against api_op_CreateDataset.go) -- is stored and echoed back by
// CreateDataset and DescribeDataset instead of being accepted then silently
// dropped, matching the same field this backend already models correctly
// on Datastore.
func TestDataset_RetentionPeriod_RoundTrips(t *testing.T) {
	t.Parallel()

	backend := iotanalytics.NewInMemoryBackend()
	client := newTestIoTAnalyticsClient(t, iotanalytics.NewHandler(backend))

	createOut, err := client.CreateDataset(t.Context(), &iotanalyticssdk.CreateDatasetInput{
		DatasetName: aws.String("retention_dataset"),
		Actions: []iotanalyticstypes.DatasetAction{
			{
				ActionName: aws.String("action1"),
				QueryAction: &iotanalyticstypes.SqlQueryDatasetAction{
					SqlQuery: aws.String("SELECT * FROM some_datastore"),
				},
			},
		},
		RetentionPeriod: &iotanalyticstypes.RetentionPeriod{
			NumberOfDays: aws.Int32(30),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.RetentionPeriod)
	require.NotNil(t, createOut.RetentionPeriod.NumberOfDays)
	require.Equal(t, int32(30), *createOut.RetentionPeriod.NumberOfDays)

	descOut, err := client.DescribeDataset(t.Context(), &iotanalyticssdk.DescribeDatasetInput{
		DatasetName: aws.String("retention_dataset"),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Dataset)
	require.NotNil(t, descOut.Dataset.RetentionPeriod)
	require.NotNil(t, descOut.Dataset.RetentionPeriod.NumberOfDays)
	require.Equal(t, int32(30), *descOut.Dataset.RetentionPeriod.NumberOfDays)
}
