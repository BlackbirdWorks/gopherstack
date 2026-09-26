package iotanalytics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotanalyticssdk "github.com/aws/aws-sdk-go-v2/service/iotanalytics" //nolint:staticcheck // AWS has deprecated this service; gopherstack still supports it
	iotanalyticstypes "github.com/aws/aws-sdk-go-v2/service/iotanalytics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for iotanalytics's five flagged List ops.
// ListDatasetContents already matched types.DatasetContentSummary exactly;
// ListChannels already matched types.ChannelSummary exactly. ListDatasets and
// ListDatastores were already fixed by a prior pass (see
// list_summaries_missing_members_test.go: DatasetSummary.Actions/Triggers,
// DatastoreSummary.DatastorePartitions/FileFormatType). ListPipelines had a
// real leak: pipelineReprocessingSummary carried startTime/endTime, which
// have no case in awsRestjson1_deserializeDocumentReprocessingSummary at
// all -- fixed by dropping both from the wire struct (models.go).
//
//nolint:staticcheck // iotanalytics is AWS-deprecated; gopherstack still emulates it
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("channels exact", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestIoTAnalyticsClient(t, h)
		ctx := t.Context()

		_, err := client.CreateChannel(ctx, &iotanalyticssdk.CreateChannelInput{
			ChannelName: aws.String("c1"),
		})
		require.NoError(t, err)

		out, err := client.ListChannels(ctx, &iotanalyticssdk.ListChannelsInput{})
		require.NoError(t, err)
		require.Len(t, out.ChannelSummaries, 1)
		s := out.ChannelSummaries[0]
		assert.Equal(t, "c1", aws.ToString(s.ChannelName))
		assert.NotEmpty(t, s.Status)
		assert.NotNil(t, s.CreationTime)
	})

	t.Run("dataset contents exact", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestIoTAnalyticsClient(t, h)
		ctx := t.Context()

		_, err := client.CreateDataset(ctx, &iotanalyticssdk.CreateDatasetInput{
			DatasetName: aws.String("d1"),
			Actions: []iotanalyticstypes.DatasetAction{
				{ActionName: aws.String("a1"), QueryAction: &iotanalyticstypes.SqlQueryDatasetAction{
					SqlQuery: aws.String("SELECT * FROM d1"),
				}},
			},
		})
		require.NoError(t, err)

		_, err = client.CreateDatasetContent(ctx, &iotanalyticssdk.CreateDatasetContentInput{
			DatasetName: aws.String("d1"),
		})
		require.NoError(t, err)

		out, err := client.ListDatasetContents(ctx, &iotanalyticssdk.ListDatasetContentsInput{
			DatasetName: aws.String("d1"),
		})
		require.NoError(t, err)
		require.Len(t, out.DatasetContentSummaries, 1)
		s := out.DatasetContentSummaries[0]
		assert.NotEmpty(t, aws.ToString(s.Version))
		require.NotNil(t, s.Status)
		assert.NotEmpty(t, s.Status.State)
		assert.NotNil(t, s.CreationTime)
	})

	t.Run("pipelines reprocessing summary no start end time leak", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		client := newTestIoTAnalyticsClient(t, h)
		ctx := t.Context()

		_, err := client.CreateDatastore(ctx, &iotanalyticssdk.CreateDatastoreInput{
			DatastoreName: aws.String("ds1"),
		})
		require.NoError(t, err)

		_, err = client.CreatePipeline(ctx, &iotanalyticssdk.CreatePipelineInput{
			PipelineName: aws.String("p1"),
			PipelineActivities: []iotanalyticstypes.PipelineActivity{
				{Channel: &iotanalyticstypes.ChannelActivity{
					Name: aws.String("ch"), ChannelName: aws.String("c1"), Next: aws.String("store"),
				}},
				{Datastore: &iotanalyticstypes.DatastoreActivity{
					Name: aws.String("store"), DatastoreName: aws.String("ds1"),
				}},
			},
		})
		require.NoError(t, err)

		rec := doRequest(t, h, "POST", "/pipelines/p1/reprocessing", map[string]any{
			"startTime": 1000.0,
			"endTime":   2000.0,
		})
		assert.Contains(t, []int{200, 201}, rec.Code)

		out, err := client.ListPipelines(ctx, &iotanalyticssdk.ListPipelinesInput{})
		require.NoError(t, err)
		require.Len(t, out.PipelineSummaries, 1)
		p := out.PipelineSummaries[0]
		require.Len(t, p.ReprocessingSummaries, 1)
		rp := p.ReprocessingSummaries[0]
		assert.NotEmpty(t, aws.ToString(rp.Id))
		assert.NotEmpty(t, rp.Status)
		assert.NotNil(t, rp.CreationTime)

		body := rec.Body.String()
		assert.NotContains(t, body, "startTime")
		assert.NotContains(t, body, "endTime")
	})
}
