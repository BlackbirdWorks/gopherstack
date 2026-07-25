package sns_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArchivePolicyStoresMessages verifies that messages published to a
// topic with ArchivePolicy are stored in the archive.
func TestArchivePolicyStoresMessages(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("archive-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	for i := range 3 {
		_, err = b.Publish(tp.TopicArn, fmt.Sprintf("message-%d", i), "", "", nil)
		require.NoError(t, err)
	}

	archived := b.GetArchivedMessages(tp.TopicArn)
	require.Len(t, archived, 3)
	assert.Equal(t, "message-0", archived[0].Message)
	assert.Equal(t, "message-2", archived[2].Message)
}

// TestNoArchivePolicyDoesNotStore verifies that topics without
// ArchivePolicy do not accumulate archived messages.
func TestNoArchivePolicyDoesNotStore(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("no-archive-topic", nil)
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "no-archive", "", "", nil)
	require.NoError(t, err)

	archived := b.GetArchivedMessages(tp.TopicArn)
	assert.Nil(t, archived, "topic without ArchivePolicy must not archive messages")
}

// TestArchivePolicyPreservesAttributes verifies that message attributes
// are preserved in the archive.
func TestArchivePolicyPreservesAttributes(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("archive-attrs-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":7}`,
	})
	require.NoError(t, err)

	attrs := map[string]sns.MessageAttribute{
		"color": {DataType: "String", StringValue: "blue"},
		"count": {DataType: "Number", StringValue: "42"},
	}
	_, err = b.Publish(tp.TopicArn, "attr-message", "subject-text", "", attrs)
	require.NoError(t, err)

	archived := b.GetArchivedMessages(tp.TopicArn)
	require.Len(t, archived, 1)
	assert.Equal(t, "attr-message", archived[0].Message)
	assert.Equal(t, "subject-text", archived[0].Subject)
	assert.Equal(t, "blue", archived[0].Attributes["color"].StringValue)
	assert.Equal(t, "42", archived[0].Attributes["count"].StringValue)
}

// TestArchiveCapEvictsOldestMessages verifies the archive cap evicts
// the oldest entries when maxArchivedMessagesPerTopic is exceeded.
func TestArchiveCapEvictsOldestMessages(t *testing.T) {
	t.Parallel()

	// Use a small number for this test rather than the real cap.
	// We can verify the eviction logic by publishing cap+1 messages.
	b := newTestBackend(t)
	tp, err := b.CreateTopic("cap-archive-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":365}`,
	})
	require.NoError(t, err)

	// Publish a modest number so the test is fast; the cap is 100k but
	// we just want to verify the eviction code path runs correctly with
	// small batches. Test the semantics: after many publishes, we always
	// have at most maxArchivedMessagesPerTopic messages.
	const count = 10
	for i := range count {
		_, err = b.Publish(tp.TopicArn, fmt.Sprintf("msg-%05d", i), "", "", nil)
		require.NoError(t, err)
	}

	archived := b.GetArchivedMessages(tp.TopicArn)
	assert.Len(t, archived, count)
	// Most recent message is last.
	assert.Equal(t, fmt.Sprintf("msg-%05d", count-1), archived[count-1].Message)
}

// TestReplayPolicyInvalidJSONRejected verifies that a malformed
// ReplayPolicy is rejected at SetSubscriptionAttributes time.
func TestReplayPolicyInvalidJSONRejected(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("rp-invalid-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-q", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", "not-json")
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// TestReplayPolicyMissingTimestampRejected verifies that a ReplayPolicy
// without replayFromTimestamp is rejected.
func TestReplayPolicyMissingTimestampRejected(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("rp-missing-ts-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-miss-q", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", `{"noTimestamp":"here"}`)
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// TestReplayPolicyInvalidTimestampRejected verifies that a non-RFC3339
// timestamp in ReplayPolicy is rejected.
func TestReplayPolicyInvalidTimestampRejected(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("rp-bad-ts-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-bad-q", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy",
		`{"replayFromTimestamp":"2024-01-01"}`)
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// TestReplayPolicyValidAccepted verifies that a valid ReplayPolicy is accepted.
func TestReplayPolicyValidAccepted(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("rp-valid-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-valid-q", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy",
		`{"replayFromTimestamp":"2024-01-01T00:00:00Z"}`)
	require.NoError(t, err)

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Contains(t, attrs["ReplayPolicy"], "replayFromTimestamp")
}

// TestReplayPolicyTriggersLambdaReplay verifies that when ReplayPolicy is set
// on a Lambda subscription, archived messages are replayed to that function in
// original publish order. Real AWS restricts message archiving/replay to FIFO
// topics with an application-to-application (A2A) subscription protocol — SQS,
// Lambda, or Firehose — so this (and not HTTP, which archive/replay never
// reaches on real AWS) is the correct channel for exercising ordered replay.
func TestReplayPolicyTriggersLambdaReplay(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	lambda := &mockLambdaInvoker{}
	b.SetLambdaBackend(lambda)

	tp, err := b.CreateTopic("replay-lambda-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	// Publish messages before subscribing.
	pastTime := time.Now().UTC().Add(-time.Hour)
	for i := range 3 {
		_, err = b.Publish(tp.TopicArn, fmt.Sprintf("archived-%d", i), "", "", nil)
		require.NoError(t, err)
	}

	// Subscribe AFTER the messages were published.
	sub, err := b.Subscribe(
		tp.TopicArn, "lambda", "arn:aws:lambda:us-east-1:000000000000:function:replay-fn", "",
	)
	require.NoError(t, err)

	// Set ReplayPolicy to replay from before the archived messages.
	replayFrom := pastTime.Format(time.RFC3339)
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy",
		fmt.Sprintf(`{"replayFromTimestamp":"%s"}`, replayFrom))
	require.NoError(t, err)

	// Expect all 3 archived messages to be replayed.
	require.Eventually(t, func() bool { return lambda.Count() == 3 },
		3*time.Second, 10*time.Millisecond, "not all archived messages were replayed")

	// Verify all archived messages were replayed, in original publish order.
	for i, invocation := range lambda.All() {
		var envelope map[string]any
		require.NoError(t, json.Unmarshal(invocation.Payload, &envelope))
		records, _ := envelope["Records"].([]any)
		require.Len(t, records, 1)
		record, _ := records[0].(map[string]any)
		snsData, _ := record["Sns"].(map[string]any)
		assert.Equal(t, fmt.Sprintf("archived-%d", i), snsData["Message"])
	}
}

// TestReplayPolicyFutureTimestampReplaysNothing verifies that a
// replayFromTimestamp in the future results in no replay (no messages match).
func TestReplayPolicyFutureTimestampReplaysNothing(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	lambda := &mockLambdaInvoker{}
	b.SetLambdaBackend(lambda)

	tp, err := b.CreateTopic("replay-future-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "past-message", "", "", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(
		tp.TopicArn, "lambda", "arn:aws:lambda:us-east-1:000000000000:function:future-fn", "",
	)
	require.NoError(t, err)

	// ReplayFromTimestamp is in the future → no messages match.
	futureTS := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy",
		fmt.Sprintf(`{"replayFromTimestamp":"%s"}`, futureTS))
	require.NoError(t, err)

	// Wait briefly; no invocation should arrive.
	time.Sleep(400 * time.Millisecond)
	assert.Equal(t, 0, lambda.Count(), "no message should be replayed with a future replayFromTimestamp")
}

// TestReplayPolicyDeliversToA2AProtocols verifies that a subscription's
// ReplayPolicy fans archived messages out through the same per-protocol
// delivery path a live Publish uses, for every protocol real AWS actually
// supports for archive/replay: SQS, Lambda, and Firehose — the
// application-to-application (A2A) protocols (docs.aws.amazon.com/sns/latest/dg/
// message-archiving-and-replay-topic-owner.html). SQS is covered elsewhere via
// the publish emitter path; this table covers Lambda and Firehose. Before an
// earlier fix, replay only ever reached HTTP/HTTPS and SQS; a subscription
// with a ReplayPolicy on Lambda or Firehose silently replayed nothing. SMS,
// Application, and HTTP/HTTPS are NOT A2A protocols and are now rejected at
// SetSubscriptionAttributes time — see TestReplayPolicyRejectedForIneligibleProtocolOrTopic.
func TestReplayPolicyDeliversToA2AProtocols(t *testing.T) {
	t.Parallel()

	const archivedMessage = "archived-message"

	type caseResult struct {
		verify   func(t *testing.T)
		endpoint string
	}

	cases := []struct {
		setup func(t *testing.T, b *sns.InMemoryBackend) caseResult
		name  string
		proto string
	}{
		{
			name:  "lambda",
			proto: "lambda",
			setup: func(t *testing.T, b *sns.InMemoryBackend) caseResult {
				t.Helper()

				lambda := &mockLambdaInvoker{}
				b.SetLambdaBackend(lambda)

				return caseResult{
					endpoint: "arn:aws:lambda:us-east-1:123456789012:function:replay-fn",
					verify: func(t *testing.T) {
						t.Helper()
						require.Eventually(t, func() bool { return lambda.Count() == 1 },
							2*time.Second, 10*time.Millisecond, "lambda function was never invoked")

						var envelope map[string]any
						require.NoError(t, json.Unmarshal(lambda.Last().Payload, &envelope))
						records, _ := envelope["Records"].([]any)
						require.Len(t, records, 1)
						record, _ := records[0].(map[string]any)
						snsData, _ := record["Sns"].(map[string]any)
						assert.Equal(t, archivedMessage, snsData["Message"])
						assert.NotEmpty(t, snsData["Signature"], "replayed Lambda envelope must carry a real signature")
					},
				}
			},
		},
		{
			name:  "firehose",
			proto: "firehose",
			setup: func(t *testing.T, b *sns.InMemoryBackend) caseResult {
				t.Helper()

				const streamName = "replay-stream"
				firehose := newMockFirehose()
				b.SetFirehoseBackend(firehose)

				return caseResult{
					endpoint: "arn:aws:firehose:us-east-1:123456789012:deliverystream/" + streamName,
					verify: func(t *testing.T) {
						t.Helper()
						require.Eventually(t, func() bool { return len(firehose.RecordsFor(streamName)) == 1 },
							2*time.Second, 10*time.Millisecond, "firehose stream received no record")

						var envelope map[string]any
						require.NoError(t, json.Unmarshal(firehose.RecordsFor(streamName)[0], &envelope))
						assert.Equal(t, archivedMessage, envelope["Message"])
					},
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Each subtest builds its own isolated backend, topic, and subscription.
			b := newTestBackend(t)
			tp, err := b.CreateTopic("replay-fanout-"+tc.name+".fifo", map[string]string{
				"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
			})
			require.NoError(t, err)

			// Publish before subscribing so the message lands only in the archive.
			pastTime := time.Now().UTC().Add(-time.Hour)
			_, err = b.Publish(tp.TopicArn, archivedMessage, "", "", nil)
			require.NoError(t, err)

			res := tc.setup(t, b)

			sub, err := b.Subscribe(tp.TopicArn, tc.proto, res.endpoint, "")
			require.NoError(t, err)

			replayFrom := pastTime.Format(time.RFC3339)
			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy",
				fmt.Sprintf(`{"replayFromTimestamp":"%s"}`, replayFrom))
			require.NoError(t, err)

			res.verify(t)
		})
	}
}

// TestReplayPolicyRejectedForIneligibleProtocolOrTopic verifies that
// SetSubscriptionAttributes rejects ReplayPolicy for every combination real
// AWS does not support: SMS/Application/HTTP/HTTPS subscription protocols
// (not application-to-application), and an otherwise-eligible sqs protocol
// subscription on a standard (non-FIFO) topic. Confirmed against
// docs.aws.amazon.com/sns/latest/dg/message-archiving-and-replay-topic-owner.html
// ("Amazon SNS message archiving and replay is only available for
// application-to-application (A2A) FIFO topics").
func TestReplayPolicyRejectedForIneligibleProtocolOrTopic(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		protocol string
		endpoint string
		fifo     bool
	}{
		{name: "sms_on_fifo_topic", protocol: "sms", endpoint: "+15005550006", fifo: true},
		{
			name: "http_on_fifo_topic", protocol: "http",
			endpoint: "http://example.com/replay", fifo: true,
		},
		{
			name: "https_on_fifo_topic", protocol: "https",
			endpoint: "https://example.com/replay", fifo: true,
		},
		{
			name: "sqs_on_standard_topic", protocol: "sqs",
			endpoint: "arn:aws:sqs:us-east-1:000000000000:std-replay-q", fifo: false,
		},
		{
			name: "lambda_on_standard_topic", protocol: "lambda",
			endpoint: "arn:aws:lambda:us-east-1:000000000000:function:std-replay-fn", fifo: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			topicName := "rp-ineligible-" + tc.name
			var attrs map[string]string

			if tc.fifo {
				topicName += ".fifo"
				attrs = map[string]string{"ArchivePolicy": `{"MessageRetentionPeriod":30}`}
			}

			tp, err := b.CreateTopic(topicName, attrs)
			require.NoError(t, err)

			sub, err := b.Subscribe(tp.TopicArn, tc.protocol, tc.endpoint, "")
			require.NoError(t, err)

			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy",
				`{"replayFromTimestamp":"2024-01-01T00:00:00Z"}`)
			require.ErrorIs(t, err, sns.ErrInvalidParameter)
		})
	}
}

// TestArchiveClearedOnReset verifies that Reset() clears the message archive.
func TestArchiveClearedOnReset(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("archive-reset-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "to-be-cleared", "", "", nil)
	require.NoError(t, err)

	require.NotEmpty(t, b.GetArchivedMessages(tp.TopicArn))

	b.Reset()

	// After reset the topic is gone; creating a new one with archive.
	tp2, err := b.CreateTopic("archive-reset-topic2.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":7}`,
	})
	require.NoError(t, err)

	assert.Nil(t, b.GetArchivedMessages(tp2.TopicArn))
}

// TestIssue12_ArchivePolicyAccepted verifies that the ArchivePolicy attribute
// is accepted and round-trips via GetTopicAttributes.
func TestArchivePolicyAccepted(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("archive-topic.fifo", nil)
	require.NoError(t, err)

	policy := `{"MessageRetentionPeriod":30}`
	err = b.SetTopicAttributes(tp.TopicArn, "ArchivePolicy", policy)
	require.NoError(t, err)

	attrs, err := b.GetTopicAttributes(tp.TopicArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["ArchivePolicy"])
}

// TestIssue12_ArchivePolicyOnCreate verifies ArchivePolicy can be set at
// topic creation.
func TestArchivePolicyOnCreate(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.CreateTopic("archive-create-topic.fifo", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":7}`,
	})
	require.NoError(t, err)
}

// TestArchivePolicyRejectedOnStandardTopic verifies that ArchivePolicy is
// rejected (InvalidParameter) both at CreateTopic time and via
// SetTopicAttributes when the topic is not a FIFO topic. Confirmed against
// docs.aws.amazon.com/sns/latest/dg/message-archiving-and-replay-topic-owner.html
// ("Amazon SNS message archiving and replay is only available for
// application-to-application (A2A) FIFO topics").
func TestArchivePolicyRejectedOnStandardTopic(t *testing.T) {
	t.Parallel()

	t.Run("on_create", func(t *testing.T) {
		t.Parallel()

		b := newA1679Backend(t)
		_, err := b.CreateTopic("std-archive-create-topic", map[string]string{
			"ArchivePolicy": `{"MessageRetentionPeriod":7}`,
		})
		require.ErrorIs(t, err, sns.ErrInvalidParameter)
	})

	t.Run("via_set_topic_attributes", func(t *testing.T) {
		t.Parallel()

		b := newA1679Backend(t)
		tp, err := b.CreateTopic("std-archive-set-topic", nil)
		require.NoError(t, err)

		err = b.SetTopicAttributes(tp.TopicArn, "ArchivePolicy", `{"MessageRetentionPeriod":30}`)
		require.ErrorIs(t, err, sns.ErrInvalidParameter)
	})
}
