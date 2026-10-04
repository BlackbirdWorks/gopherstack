package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kintypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func kinesisStreamARN(t *testing.T, fx *sfnFixture, name string, waitActive bool) string {
	t.Helper()

	kc := kinesis.NewFromConfig(fx.cfg)
	_, err := kc.CreateStream(t.Context(), &kinesis.CreateStreamInput{
		StreamName: aws.String(name), ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	var arn string

	require.Eventually(t, func() bool {
		d, derr := kc.DescribeStreamSummary(t.Context(), &kinesis.DescribeStreamSummaryInput{
			StreamName: aws.String(name),
		})
		if derr != nil {
			return false
		}

		arn = aws.ToString(d.StreamDescriptionSummary.StreamARN)

		return !waitActive || d.StreamDescriptionSummary.StreamStatus == kintypes.StreamStatusActive
	}, authzDeadline, authzTick)

	return arn
}

func kinesisRecords(t *testing.T, fx *sfnFixture, name string) []kintypes.Record {
	t.Helper()

	kc := kinesis.NewFromConfig(fx.cfg)
	it, err := kc.GetShardIterator(t.Context(), &kinesis.GetShardIteratorInput{
		StreamName: aws.String(name), ShardId: aws.String("shardId-000000000000"),
		ShardIteratorType: kintypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	out, err := kc.GetRecords(t.Context(), &kinesis.GetRecordsInput{ShardIterator: it.ShardIterator})
	require.NoError(t, err)

	return out.Records
}

func TestEventBridgeKinesisTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		partitionKey string
		wantKey      string
		enforce      bool
		withRole     bool
		noRetry      bool
		creating     bool
	}{
		{name: "no_params_uses_event_id", withRole: true, noRetry: true},
		{name: "partition_key_path", withRole: true, noRetry: true, partitionKey: "$.detail.pk", wantKey: "k-1"},
		{name: "no_role_enforcement_off", noRetry: true},
		{name: "default_retry_policy", withRole: true},
		{name: "stream_still_creating_is_retried", withRole: true, creating: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			authzStartWorkers(t, fx, "EventBridge")

			arn := kinesisStreamARN(t, fx, "eb-stream", !tt.creating)
			target := ebtypes.Target{Id: aws.String("t"), Arn: aws.String(arn)}

			if tt.withRole {
				target.RoleArn = aws.String(authzRole(t, fx, "eb-kin", "events.amazonaws.com", "kinesis:PutRecord"))
			}

			if tt.partitionKey != "" {
				target.KinesisParameters = &ebtypes.KinesisParameters{PartitionKeyPath: aws.String(tt.partitionKey)}
			}

			if tt.noRetry {
				target.RetryPolicy = &ebtypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(0)}
			}

			ebc := eventbridge.NewFromConfig(fx.cfg)
			_, err := ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
				Name: aws.String("r"), EventPattern: aws.String(`{"source":["kin.test"]}`),
			})
			require.NoError(t, err)

			_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
				Rule: aws.String("r"), Targets: []ebtypes.Target{target},
			})
			require.NoError(t, err)

			_, err = ebc.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
				Source: aws.String("kin.test"), DetailType: aws.String("t"), Detail: aws.String(`{"pk":"k-1"}`),
			}}})
			require.NoError(t, err)

			var recs []kintypes.Record

			require.Eventually(t, func() bool {
				recs = kinesisRecords(t, fx, "eb-stream")

				return len(recs) > 0
			}, authzDeadline, authzTick)

			require.Len(t, recs, 1)

			if tt.wantKey != "" {
				assert.Equal(t, tt.wantKey, aws.ToString(recs[0].PartitionKey))
			} else {
				assert.NotEmpty(t, aws.ToString(recs[0].PartitionKey))
			}
		})
	}
}

func TestServiceRoleAuthzEventBridgeKinesis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string
		wantData  bool
		enforce   bool
		untrusted bool
	}{
		{name: "allowed", enforce: true, action: "kinesis:PutRecord", wantData: true},
		{name: "denied_goes_to_dlq", enforce: true, action: "sqs:SendMessage"},
		{name: "untrusted_goes_to_dlq", enforce: true, action: "kinesis:PutRecord", untrusted: true},
		{name: "enforcement_off_unchanged", action: "sqs:SendMessage", wantData: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixtureIAM(t, tt.enforce)
			authzStartWorkers(t, fx, "EventBridge")

			principal := "events.amazonaws.com"
			if tt.untrusted {
				principal = "scheduler.amazonaws.com"
			}

			role := authzRole(t, fx, "eb-kin-role", principal, tt.action)
			streamARN := kinesisStreamARN(t, fx, "authz-stream", true)
			dlqURL, dlqARN := authzQueue(t, fx, "eb-kin-dlq")

			ebc := eventbridge.NewFromConfig(fx.cfg)
			_, err := ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
				Name: aws.String("r"), EventPattern: aws.String(`{"source":["authz.kin"]}`),
			})
			require.NoError(t, err)

			_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
				Rule: aws.String("r"),
				Targets: []ebtypes.Target{{
					Id: aws.String("t"), Arn: aws.String(streamARN), RoleArn: aws.String(role),
					DeadLetterConfig: &ebtypes.DeadLetterConfig{Arn: aws.String(dlqARN)},
					RetryPolicy:      &ebtypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(0)},
				}},
			})
			require.NoError(t, err)

			_, err = ebc.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
				Source: aws.String("authz.kin"), DetailType: aws.String("t"), Detail: aws.String(`{}`),
			}}})
			require.NoError(t, err)

			if tt.wantData {
				require.Eventually(t, func() bool { return len(kinesisRecords(t, fx, "authz-stream")) > 0 },
					authzDeadline, authzTick)

				return
			}

			require.Eventually(t, func() bool { return authzReceived(t, fx, dlqURL) }, authzDeadline, authzTick)
			assert.Empty(t, kinesisRecords(t, fx, "authz-stream"))
		})
	}
}
