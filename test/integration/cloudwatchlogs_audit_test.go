package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudwatchlogssdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CWLogsAudit_GetLogFields verifies that GetLogFields returns
// the set of field names discovered from real stored log events. JSON-formatted
// event messages have their top-level keys surfaced alongside the standard
// system fields.
func TestIntegration_CWLogsAudit_GetLogFields(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchLogsClient(t)
	ctx := t.Context()

	groupName := "/test/audit-fields-" + uuid.NewString()[:8]
	streamName := "stream-" + uuid.NewString()[:8]

	_, err := client.CreateLogGroup(ctx, &cloudwatchlogssdk.CreateLogGroupInput{
		LogGroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.DeleteLogGroup(ctx, &cloudwatchlogssdk.DeleteLogGroupInput{
			LogGroupName: aws.String(groupName),
		})
	})

	_, err = client.CreateLogStream(ctx, &cloudwatchlogssdk.CreateLogStreamInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(streamName),
	})
	require.NoError(t, err)

	now := time.Now().UnixMilli()
	_, err = client.PutLogEvents(ctx, &cloudwatchlogssdk.PutLogEventsInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(streamName),
		LogEvents: []cwlogstypes.InputLogEvent{
			{Message: aws.String(`{"level":"ERROR","requestId":"abc","latency":42}`), Timestamp: aws.Int64(now)},
			{Message: aws.String(`{"level":"INFO","userId":"u-1"}`), Timestamp: aws.Int64(now + 1)},
			{Message: aws.String("plain text message"), Timestamp: aws.Int64(now + 2)},
		},
	})
	require.NoError(t, err)

	// The AWS SDK models GetLogFields via dataSourceName/dataSourceType. The
	// emulator interprets the data source name as the log group identifier.
	out, err := client.GetLogFields(ctx, &cloudwatchlogssdk.GetLogFieldsInput{
		DataSourceName: aws.String(groupName),
		DataSourceType: aws.String("LogGroup"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.LogFields)

	names := make(map[string]bool, len(out.LogFields))
	for _, f := range out.LogFields {
		require.NotNil(t, f.LogFieldName)
		names[*f.LogFieldName] = true
	}

	// Standard system fields are always present.
	assert.True(t, names["@message"], "expected @message field")
	assert.True(t, names["@timestamp"], "expected @timestamp field")
	// Discovered fields from JSON event messages.
	assert.True(t, names["level"], "expected discovered field level")
	assert.True(t, names["requestId"], "expected discovered field requestId")
	assert.True(t, names["latency"], "expected discovered field latency")
	assert.True(t, names["userId"], "expected discovered field userId")
}

// TestIntegration_CWLogsAudit_GetLogFieldsNotFound verifies that GetLogFields on
// an unknown log group returns ResourceNotFoundException rather than silently
// succeeding.
func TestIntegration_CWLogsAudit_GetLogFieldsNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchLogsClient(t)
	ctx := t.Context()

	_, err := client.GetLogFields(ctx, &cloudwatchlogssdk.GetLogFieldsInput{
		DataSourceName: aws.String("/test/missing-" + uuid.NewString()[:8]),
		DataSourceType: aws.String("LogGroup"),
	})
	require.Error(t, err)

	var notFound *cwlogstypes.ResourceNotFoundException
	assert.ErrorAs(t, err, &notFound)
}

// TestIntegration_CWLogsAudit_TestTransformer verifies that TestTransformer
// applies the supplied transformer config to the supplied sample log events and
// returns the deterministically transformed results (a round trip through the
// addKeys / upperCaseString / deleteKeys processors).
func TestIntegration_CWLogsAudit_TestTransformer(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchLogsClient(t)
	ctx := t.Context()

	input := []string{
		`{"level":"error","msg":"boom"}`,
		`{"level":"info","msg":"ok"}`,
	}

	out, err := client.TestTransformer(ctx, &cloudwatchlogssdk.TestTransformerInput{
		LogEventMessages: input,
		TransformerConfig: []cwlogstypes.Processor{
			{
				AddKeys: &cwlogstypes.AddKeys{
					Entries: []cwlogstypes.AddKeyEntry{
						{Key: aws.String("env"), Value: aws.String("prod")},
					},
				},
			},
			{
				UpperCaseString: &cwlogstypes.UpperCaseString{
					WithKeys: []string{"level"},
				},
			},
			{
				DeleteKeys: &cwlogstypes.DeleteKeys{
					WithKeys: []string{"msg"},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.TransformedLogs, len(input))

	for i, rec := range out.TransformedLogs {
		require.NotNil(t, rec.TransformedEventMessage)
		require.NotNil(t, rec.EventMessage)
		assert.Equal(t, int64(i+1), rec.EventNumber)
		// Original message round-trips unchanged.
		assert.Equal(t, input[i], *rec.EventMessage)

		transformed := *rec.TransformedEventMessage
		// addKeys injected env=prod.
		assert.Contains(t, transformed, `"env":"prod"`)
		// deleteKeys removed msg.
		assert.NotContains(t, transformed, `"msg"`)
	}

	// upperCaseString folded the level field deterministically.
	assert.Contains(t, *out.TransformedLogs[0].TransformedEventMessage, `"level":"ERROR"`)
	assert.Contains(t, *out.TransformedLogs[1].TransformedEventMessage, `"level":"INFO"`)
}
