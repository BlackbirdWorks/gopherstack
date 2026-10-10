package kinesis_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutRecords_OverFiveHundredRejected_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		count   int
		wantErr bool
	}{
		{name: "at limit", count: 500},
		{name: "over limit", count: 501, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKinesisClient(t, newTestHandler(t))
			_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
				StreamName: aws.String("s"), ShardCount: aws.Int32(1),
			})
			require.NoError(t, err)

			records := make([]types.PutRecordsRequestEntry, tc.count)
			for i := range records {
				records[i] = types.PutRecordsRequestEntry{Data: []byte("x"), PartitionKey: aws.String(strconv.Itoa(i))}
			}

			out, err := client.PutRecords(t.Context(), &kinesissdk.PutRecordsInput{
				StreamName: aws.String("s"), Records: records,
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Records, tc.count)
		})
	}
}

func TestGetRecords_LimitOverMaxRejected_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limit   int32
		wantErr bool
	}{
		{name: "at max", limit: 10000},
		{name: "over max", limit: 10001, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKinesisClient(t, newTestHandler(t))
			_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
				StreamName: aws.String("s"), ShardCount: aws.Int32(1),
			})
			require.NoError(t, err)

			it, err := client.GetShardIterator(t.Context(), &kinesissdk.GetShardIteratorInput{
				StreamName: aws.String("s"), ShardId: aws.String("shardId-000000000000"),
				ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			})
			require.NoError(t, err)

			_, err = client.GetRecords(t.Context(), &kinesissdk.GetRecordsInput{
				ShardIterator: it.ShardIterator, Limit: aws.Int32(tc.limit),
			})
			if tc.wantErr {
				var target *types.InvalidArgumentException
				require.ErrorAs(t, err, &target)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestEnhancedMonitoring_MetricNames_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantErrCode string
		enable      []types.MetricsName
		wantCount   int
	}{
		{name: "single", enable: []types.MetricsName{types.MetricsNameIncomingBytes}, wantCount: 1},
		{name: "all expands", enable: []types.MetricsName{types.MetricsNameAll}, wantCount: 7},
		{name: "unknown", enable: []types.MetricsName{"Bogus"}, wantErrCode: "ValidationException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestKinesisClient(t, newTestHandler(t))
			_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
				StreamName: aws.String("s"), ShardCount: aws.Int32(1),
			})
			require.NoError(t, err)

			out, err := client.EnableEnhancedMonitoring(t.Context(), &kinesissdk.EnableEnhancedMonitoringInput{
				StreamName: aws.String("s"), ShardLevelMetrics: tc.enable,
			})
			if tc.wantErrCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr, err)
				assert.Equal(t, tc.wantErrCode, apiErr.ErrorCode())
				assert.Contains(t, apiErr.ErrorMessage(), "shardLevelMetrics")

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.DesiredShardLevelMetrics, tc.wantCount)
		})
	}
}

func TestCreateStream_InvalidNameMessage_SDK(t *testing.T) {
	t.Parallel()

	client := newTestKinesisClient(t, newTestHandler(t))
	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String("bad name"), ShardCount: aws.Int32(1),
	})

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, err)
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())
	assert.Contains(t, apiErr.ErrorMessage(), "at 'streamName'")
}
