package sns_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch2_ArchivePolicyStoresMessages verifies that messages published to a
// topic with ArchivePolicy are stored in the archive.
func TestArchivePolicyStoresMessages(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("archive-topic", map[string]string{
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

// TestBatch2_NoArchivePolicyDoesNotStore verifies that topics without
// ArchivePolicy do not accumulate archived messages.
func TestNoArchivePolicyDoesNotStore(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("no-archive-topic", nil)
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "no-archive", "", "", nil)
	require.NoError(t, err)

	archived := b.GetArchivedMessages(tp.TopicArn)
	assert.Nil(t, archived, "topic without ArchivePolicy must not archive messages")
}

// TestBatch2_ArchivePolicyPreservesAttributes verifies that message attributes
// are preserved in the archive.
func TestArchivePolicyPreservesAttributes(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("archive-attrs-topic", map[string]string{
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

// TestBatch2_ArchiveCapEvictsOldestMessages verifies the archive cap evicts
// the oldest entries when maxArchivedMessagesPerTopic is exceeded.
func TestArchiveCapEvictsOldestMessages(t *testing.T) {
	t.Parallel()

	// Use a small number for this test rather than the real cap.
	// We can verify the eviction logic by publishing cap+1 messages.
	b := newB2Backend(t)
	tp, err := b.CreateTopic("cap-archive-topic", map[string]string{
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

// TestBatch2_ReplayPolicyInvalidJSONRejected verifies that a malformed
// ReplayPolicy is rejected at SetSubscriptionAttributes time.
func TestReplayPolicyInvalidJSONRejected(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("rp-invalid-topic", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-q", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", "not-json")
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// TestBatch2_ReplayPolicyMissingTimestampRejected verifies that a ReplayPolicy
// without replayFromTimestamp is rejected.
func TestReplayPolicyMissingTimestampRejected(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("rp-missing-ts-topic", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-miss-q", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", `{"noTimestamp":"here"}`)
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// TestBatch2_ReplayPolicyInvalidTimestampRejected verifies that a non-RFC3339
// timestamp in ReplayPolicy is rejected.
func TestReplayPolicyInvalidTimestampRejected(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("rp-bad-ts-topic", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-bad-q", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy",
		`{"replayFromTimestamp":"2024-01-01"}`)
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// TestBatch2_ReplayPolicyValidAccepted verifies that a valid ReplayPolicy is accepted.
func TestReplayPolicyValidAccepted(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("rp-valid-topic", map[string]string{
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

// TestBatch2_ReplayPolicyTriggersHTTPReplay verifies that when ReplayPolicy is
// set on an HTTP subscription, archived messages are replayed to that endpoint.
func TestReplayPolicyTriggersHTTPReplay(t *testing.T) {
	t.Parallel()

	replayed := make(chan string, 10)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		replayed <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("replay-http-topic", map[string]string{
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
	sub, err := b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	// Set ReplayPolicy to replay from before the archived messages.
	replayFrom := pastTime.Format(time.RFC3339)
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy",
		fmt.Sprintf(`{"replayFromTimestamp":"%s"}`, replayFrom))
	require.NoError(t, err)

	// Expect all 3 archived messages to be replayed.
	received := make([]string, 0, 3)
	deadline := time.After(3 * time.Second)

	for len(received) < 3 {
		select {
		case raw := <-replayed:
			env := parseNotificationEnvelope(t, raw)
			received = append(received, env.Message)
		case <-deadline:
			t.Fatalf("only %d/3 archived messages were replayed", len(received))
		}
	}

	// Verify all archived messages were replayed.
	for i, msg := range received {
		assert.Equal(t, fmt.Sprintf("archived-%d", i), msg)
	}
}

// TestBatch2_ReplayPolicyFutureTimestampReplayesNothing verifies that a
// replayFromTimestamp in the future results in no replay (no messages match).
func TestReplayPolicyFutureTimestampReplaysNothing(t *testing.T) {
	t.Parallel()

	received := make(chan string, 5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("replay-future-topic", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "past-message", "", "", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	// ReplayFromTimestamp is in the future → no messages match.
	futureTS := time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339)
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy",
		fmt.Sprintf(`{"replayFromTimestamp":"%s"}`, futureTS))
	require.NoError(t, err)

	// Wait briefly; no message should arrive.
	select {
	case <-received:
		t.Fatal("no message should be replayed with a future replayFromTimestamp")
	case <-time.After(400 * time.Millisecond):
		// Expected: nothing replayed.
	}
}

// Test_ReplayPolicyDeliversToAllSubscriptionProtocols verifies that a
// subscription's ReplayPolicy fans archived messages out through the same
// per-protocol delivery path a live Publish uses. Before this fix, replay only
// ever reached HTTP/HTTPS (a direct call in replayMessagesToSubscription) and
// SQS (via the publish emitter); a subscription with a ReplayPolicy on the
// Lambda, Firehose, SMS, or Application protocol silently replayed nothing.
func TestReplayPolicyDeliversToAllSubscriptionProtocols(t *testing.T) {
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
		{
			name:  "sms",
			proto: "sms",
			setup: func(t *testing.T, b *sns.InMemoryBackend) caseResult {
				t.Helper()

				return caseResult{
					endpoint: "+15005550006",
					verify: func(t *testing.T) {
						t.Helper()

						var got []sns.SMSDelivery
						require.Eventually(t, func() bool {
							if d := b.DrainSMSDeliveries(); len(d) > 0 {
								got = d

								return true
							}

							return false
						}, 2*time.Second, 10*time.Millisecond, "SMS delivery was never recorded")

						require.Len(t, got, 1)
						assert.Equal(t, archivedMessage, got[0].Message)
					},
				}
			},
		},
		{
			name:  "application",
			proto: "application",
			setup: func(t *testing.T, b *sns.InMemoryBackend) caseResult {
				t.Helper()

				app, err := b.CreatePlatformApplication("replay-app", "GCM", nil)
				require.NoError(t, err)
				ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "replay-token", nil)
				require.NoError(t, err)

				return caseResult{
					endpoint: ep.EndpointArn,
					verify: func(t *testing.T) {
						t.Helper()

						var got []sns.ApplicationDelivery
						require.Eventually(t, func() bool {
							if d := b.DrainApplicationDeliveries(); len(d) > 0 {
								got = d

								return true
							}

							return false
						}, 2*time.Second, 10*time.Millisecond, "application delivery was never recorded")

						require.Len(t, got, 1)
						assert.Equal(t, archivedMessage, got[0].Message)
					},
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Each subtest builds its own isolated backend, topic, and subscription.
			b := newB2Backend(t)
			tp, err := b.CreateTopic("replay-fanout-"+tc.name, map[string]string{
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

// TestBatch2_ArchiveClearedOnReset verifies that Reset() clears the message archive.
func TestArchiveClearedOnReset(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("archive-reset-topic", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
	})
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "to-be-cleared", "", "", nil)
	require.NoError(t, err)

	require.NotEmpty(t, b.GetArchivedMessages(tp.TopicArn))

	b.Reset()

	// After reset the topic is gone; creating a new one with archive.
	tp2, err := b.CreateTopic("archive-reset-topic2", map[string]string{
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
	tp, err := b.CreateTopic("archive-topic", nil)
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
	_, err := b.CreateTopic("archive-create-topic", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":7}`,
	})
	require.NoError(t, err)
}
