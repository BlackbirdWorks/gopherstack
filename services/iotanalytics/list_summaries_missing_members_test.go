package iotanalytics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotanalyticssdk "github.com/aws/aws-sdk-go-v2/service/iotanalytics"
	iotanalyticstypes "github.com/aws/aws-sdk-go-v2/service/iotanalytics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListDatastores_SummaryCarriesPartitionsAndFileFormatType drives
// ListDatastores through the real aws-sdk-go-v2 client and asserts the
// returned DatastoreSummary carries DatastorePartitions/FileFormatType --
// both real members of types.DatastoreSummary (types.go:952) that the full
// Datastore detail type (types.go:707) does not carry (FileFormatType) or
// does carry under the same name (DatastorePartitions), so they cannot be
// papered over by the detail-view code path.
func TestListDatastores_SummaryCarriesPartitionsAndFileFormatType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestIoTAnalyticsClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDatastore(ctx, &iotanalyticssdk.CreateDatastoreInput{
		DatastoreName: aws.String("summary_partitions_ds"),
		DatastorePartitions: &iotanalyticstypes.DatastorePartitions{
			Partitions: []iotanalyticstypes.DatastorePartition{
				{
					AttributePartition: &iotanalyticstypes.Partition{AttributeName: aws.String("region")},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.ListDatastores(ctx, &iotanalyticssdk.ListDatastoresInput{})
	require.NoError(t, err)
	require.Len(t, out.DatastoreSummaries, 1)

	summary := out.DatastoreSummaries[0]
	require.NotNil(t, summary.DatastorePartitions, "DatastoreSummary.DatastorePartitions must round-trip")
	require.Len(t, summary.DatastorePartitions.Partitions, 1)
	require.NotNil(t, summary.DatastorePartitions.Partitions[0].AttributePartition)
	assert.Equal(t, "region", aws.ToString(summary.DatastorePartitions.Partitions[0].AttributePartition.AttributeName))

	assert.Equal(t, iotanalyticstypes.FileFormatTypeJson, summary.FileFormatType,
		"DatastoreSummary.FileFormatType must default to JSON, matching real AWS")
}

// TestListDatasets_SummaryCarriesActionsAndTriggers drives ListDatasets
// through the real aws-sdk-go-v2 client and asserts the returned
// DatasetSummary carries Actions/Triggers -- both real members of
// types.DatasetSummary (types.go:652), Actions in the narrower
// DatasetActionSummary shape (types.go:522, ActionName+ActionType only).
func TestListDatasets_SummaryCarriesActionsAndTriggers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestIoTAnalyticsClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDataset(ctx, &iotanalyticssdk.CreateDatasetInput{
		DatasetName: aws.String("summary_actions_ds"),
		Actions: []iotanalyticstypes.DatasetAction{
			{
				ActionName: aws.String("query-action"),
				QueryAction: &iotanalyticstypes.SqlQueryDatasetAction{
					SqlQuery: aws.String("SELECT * FROM summary_actions_ds"),
				},
			},
		},
		Triggers: []iotanalyticstypes.DatasetTrigger{
			{Schedule: &iotanalyticstypes.Schedule{Expression: aws.String("rate(1 hour)")}},
		},
	})
	require.NoError(t, err)

	out, err := client.ListDatasets(ctx, &iotanalyticssdk.ListDatasetsInput{})
	require.NoError(t, err)
	require.Len(t, out.DatasetSummaries, 1)

	summary := out.DatasetSummaries[0]
	require.Len(t, summary.Actions, 1, "DatasetSummary.Actions must round-trip")
	assert.Equal(t, "query-action", aws.ToString(summary.Actions[0].ActionName))
	assert.Equal(t, iotanalyticstypes.DatasetActionTypeQuery, summary.Actions[0].ActionType)

	require.Len(t, summary.Triggers, 1, "DatasetSummary.Triggers must round-trip")
	require.NotNil(t, summary.Triggers[0].Schedule)
	assert.Equal(t, "rate(1 hour)", aws.ToString(summary.Triggers[0].Schedule.Expression))
}
