package iotanalytics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotanalyticssdk "github.com/aws/aws-sdk-go-v2/service/iotanalytics"
	iotanalyticstypes "github.com/aws/aws-sdk-go-v2/service/iotanalytics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// TestRealClient_TagsAndUpdateOps covers iotanalytics's last four
// typed-client-uncovered ops (gopherstack-n3zi): TagResource,
// UntagResource, UpdateDataset, UpdatePipeline.
//
// Found and fixed a real bug while building this test's UpdatePipeline
// assertion: pipelineDetail.Activities (models.go) was tagged
// json:"pipelineActivities", copying CreatePipelineInput/UpdatePipelineInput's
// top-level request field name, but DescribePipelineOutput.Pipeline is
// deserialized by a distinct function (iotanalytics@v1.32.0 deserializers.go:
// awsRestjson1_deserializeDocumentPipeline, case "activities") that expects
// the unprefixed key -- so a real client's DescribePipeline (and thus every
// Activities-observing caller, this test's UpdatePipeline verification
// included) always decoded an empty slice regardless of backend state.
func TestRealClient_TagsAndUpdateOps(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	backend := iotanalytics.NewInMemoryBackend()
	h := iotanalytics.NewHandler(backend)
	client := newTestIoTAnalyticsClient(t, h)

	_, err := client.CreateChannel(ctx, &iotanalyticssdk.CreateChannelInput{
		ChannelName: aws.String("slice17_channel"),
	})
	require.NoError(t, err)

	_, err = client.CreateDatastore(ctx, &iotanalyticssdk.CreateDatastoreInput{
		DatastoreName: aws.String("slice17_datastore"),
	})
	require.NoError(t, err)

	createdDataset, err := client.CreateDataset(ctx, &iotanalyticssdk.CreateDatasetInput{
		DatasetName: aws.String("slice17_dataset"),
		Actions: []iotanalyticstypes.DatasetAction{
			{
				ActionName: aws.String("action1"),
				QueryAction: &iotanalyticstypes.SqlQueryDatasetAction{
					SqlQuery: aws.String("SELECT * FROM slice17_datastore"),
				},
			},
		},
	})
	require.NoError(t, err)
	datasetARN := aws.ToString(createdDataset.DatasetArn)

	createdPipeline, err := client.CreatePipeline(ctx, &iotanalyticssdk.CreatePipelineInput{
		PipelineName: aws.String("slice17_pipeline"),
		PipelineActivities: []iotanalyticstypes.PipelineActivity{
			{Channel: &iotanalyticstypes.ChannelActivity{
				Name:        aws.String("channelActivity"),
				ChannelName: aws.String("slice17_channel"),
				Next:        aws.String("datastoreActivity"),
			}},
			{Datastore: &iotanalyticstypes.DatastoreActivity{
				Name:          aws.String("datastoreActivity"),
				DatastoreName: aws.String("slice17_datastore"),
			}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createdPipeline.PipelineArn))

	_, err = client.TagResource(ctx, &iotanalyticssdk.TagResourceInput{
		ResourceArn: aws.String(datasetARN),
		Tags:        []iotanalyticstypes.Tag{{Key: aws.String("owner"), Value: aws.String("slice17")}},
	})
	require.NoError(t, err)

	tagged, err := client.ListTagsForResource(ctx, &iotanalyticssdk.ListTagsForResourceInput{
		ResourceArn: aws.String(datasetARN),
	})
	require.NoError(t, err)
	require.Len(t, tagged.Tags, 1)
	assert.Equal(t, "owner", aws.ToString(tagged.Tags[0].Key))

	_, err = client.UntagResource(ctx, &iotanalyticssdk.UntagResourceInput{
		ResourceArn: aws.String(datasetARN),
		TagKeys:     []string{"owner"},
	})
	require.NoError(t, err)

	untagged, err := client.ListTagsForResource(ctx, &iotanalyticssdk.ListTagsForResourceInput{
		ResourceArn: aws.String(datasetARN),
	})
	require.NoError(t, err)
	assert.Empty(t, untagged.Tags)

	_, err = client.UpdateDataset(ctx, &iotanalyticssdk.UpdateDatasetInput{
		DatasetName: aws.String("slice17_dataset"),
		Actions: []iotanalyticstypes.DatasetAction{
			{
				ActionName: aws.String("action2"),
				QueryAction: &iotanalyticstypes.SqlQueryDatasetAction{
					SqlQuery: aws.String("SELECT * FROM slice17_datastore WHERE 1=1"),
				},
			},
		},
	})
	require.NoError(t, err)

	describedDataset, err := client.DescribeDataset(ctx, &iotanalyticssdk.DescribeDatasetInput{
		DatasetName: aws.String("slice17_dataset"),
	})
	require.NoError(t, err)
	require.NotNil(t, describedDataset.Dataset)
	require.Len(t, describedDataset.Dataset.Actions, 1)
	assert.Equal(t, "action2", aws.ToString(describedDataset.Dataset.Actions[0].ActionName))
	assert.Equal(
		t,
		"SELECT * FROM slice17_datastore WHERE 1=1",
		aws.ToString(describedDataset.Dataset.Actions[0].QueryAction.SqlQuery),
	)

	_, err = client.UpdatePipeline(ctx, &iotanalyticssdk.UpdatePipelineInput{
		PipelineName: aws.String("slice17_pipeline"),
		PipelineActivities: []iotanalyticstypes.PipelineActivity{
			{Channel: &iotanalyticstypes.ChannelActivity{
				Name:        aws.String("channelActivity2"),
				ChannelName: aws.String("slice17_channel"),
				Next:        aws.String("datastoreActivity2"),
			}},
			{Datastore: &iotanalyticstypes.DatastoreActivity{
				Name:          aws.String("datastoreActivity2"),
				DatastoreName: aws.String("slice17_datastore"),
			}},
		},
	})
	require.NoError(t, err)

	describedPipeline, err := client.DescribePipeline(ctx, &iotanalyticssdk.DescribePipelineInput{
		PipelineName: aws.String("slice17_pipeline"),
	})
	require.NoError(t, err)
	require.NotNil(t, describedPipeline.Pipeline)
	require.Len(t, describedPipeline.Pipeline.Activities, 2)

	var sawChannelActivity2 bool

	for _, a := range describedPipeline.Pipeline.Activities {
		if a.Channel != nil && aws.ToString(a.Channel.Name) == "channelActivity2" {
			sawChannelActivity2 = true
		}
	}

	assert.True(t, sawChannelActivity2, "expected updated pipeline activity name to persist")
}
