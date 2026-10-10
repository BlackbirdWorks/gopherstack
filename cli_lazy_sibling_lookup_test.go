package main

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSiblingLookupsAreLiveInRealServer(t *testing.T) {
	t.Parallel()

	cfg := newWiredSDKConfig(t, CLI{})

	t.Run("sagemakerruntime_unknown_endpoint", func(t *testing.T) {
		t.Parallel()

		_, err := sagemakerruntime.NewFromConfig(cfg).InvokeEndpoint(t.Context(), &sagemakerruntime.InvokeEndpointInput{
			EndpointName: aws.String("no-such-endpoint"),
			Body:         []byte(`{}`),
			ContentType:  aws.String("application/json"),
		})
		require.Error(t, err)
	})

	t.Run("bedrockruntime_guardrail", func(t *testing.T) {
		t.Parallel()

		content := []brtypes.GuardrailContentBlock{
			&brtypes.GuardrailContentBlockMemberText{Value: brtypes.GuardrailTextBlock{Text: aws.String("hello")}},
		}
		rt := bedrockruntime.NewFromConfig(cfg)

		_, err := rt.ApplyGuardrail(t.Context(), &bedrockruntime.ApplyGuardrailInput{
			GuardrailIdentifier: aws.String("nosuchguard"),
			GuardrailVersion:    aws.String("DRAFT"),
			Source:              brtypes.GuardrailContentSourceInput,
			Content:             content,
		})
		require.Error(t, err)

		g, err := bedrock.NewFromConfig(cfg).CreateGuardrail(t.Context(), &bedrock.CreateGuardrailInput{
			Name:                    aws.String("live-guard"),
			BlockedInputMessaging:   aws.String("blocked"),
			BlockedOutputsMessaging: aws.String("blocked"),
		})
		require.NoError(t, err)
		assert.Regexp(t, `^[a-z0-9]{10}$`, aws.ToString(g.GuardrailId))

		_, err = rt.ApplyGuardrail(t.Context(), &bedrockruntime.ApplyGuardrailInput{
			GuardrailIdentifier: g.GuardrailId,
			GuardrailVersion:    aws.String("DRAFT"),
			Source:              brtypes.GuardrailContentSourceInput,
			Content:             content,
		})
		require.NoError(t, err)
	})

	t.Run("cloudwatchlogs_export_to_s3", func(t *testing.T) {
		t.Parallel()

		logs := cloudwatchlogs.NewFromConfig(cfg)
		s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })

		_, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("export-dest")})
		require.NoError(t, err)

		_, err = logs.CreateLogGroup(t.Context(), &cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String("/live")})
		require.NoError(t, err)
		_, err = logs.CreateLogStream(t.Context(), &cloudwatchlogs.CreateLogStreamInput{
			LogGroupName: aws.String("/live"), LogStreamName: aws.String("s1"),
		})
		require.NoError(t, err)

		now := time.Now().UnixMilli()
		_, err = logs.PutLogEvents(t.Context(), &cloudwatchlogs.PutLogEventsInput{
			LogGroupName: aws.String("/live"), LogStreamName: aws.String("s1"),
			LogEvents: []cwltypes.InputLogEvent{{Timestamp: aws.Int64(now), Message: aws.String("line")}},
		})
		require.NoError(t, err)

		_, err = logs.CreateExportTask(t.Context(), &cloudwatchlogs.CreateExportTaskInput{
			LogGroupName: aws.String("/live"), Destination: aws.String("export-dest"),
			From: aws.Int64(now - 60_000), To: aws.Int64(now + 60_000),
		})
		require.NoError(t, err)

		list, err := s3c.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{Bucket: aws.String("export-dest")})
		require.NoError(t, err)
		require.NotEmpty(t, list.Contents)
		assert.True(t, strings.HasSuffix(aws.ToString(list.Contents[0].Key), ".gz"))
	})
}
