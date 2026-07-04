package sns_test

// Tests for parity §B: SNS Firehose subscriptions send to DLQ on delivery failure,
// SetSubscriptionAttributes validates DLQ queue existence, certURL region is correct.

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// --- Firehose DLQ helpers ---

var errFirehoseFailure = errors.New("simulated firehose failure")

type failingFirehosePutter struct{}

func (f *failingFirehosePutter) PutRecordBatch(_ string, _ [][]byte) (int, error) {
	return 0, errFirehoseFailure
}

type successFirehosePutter struct {
	records [][]byte
	mu      sync.Mutex
}

func (s *successFirehosePutter) PutRecordBatch(_ string, records [][]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, records...)

	return len(records), nil
}

// --- SQS queue checker helper ---

type stubSQSChecker struct {
	existingARNs map[string]bool
}

func (s *stubSQSChecker) QueueExists(_ context.Context, queueARN string) (bool, error) {
	return s.existingARNs[queueARN], nil
}

// --- Firehose DLQ tests ---

func TestParityB_SNS_Firehose_DLQ_OnDeliveryFailure(t *testing.T) {
	t.Parallel()

	dlqARN := "arn:aws:sqs:us-east-1:123456789012:firehose-dlq"

	tests := []struct {
		putter    sns.FirehosePutter
		name      string
		expectDLQ bool
	}{
		{
			name:      "firehose_success_no_dlq",
			putter:    &successFirehosePutter{},
			expectDLQ: false,
		},
		{
			name:      "firehose_failure_triggers_dlq",
			putter:    &failingFirehosePutter{},
			expectDLQ: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sqsSender := &mockLambdaDLQSender{}
			b := sns.NewInMemoryBackend()
			b.SetFirehoseBackend(tt.putter)
			b.SetSQSSender(sqsSender)

			topic, err := b.CreateTopic("firehose-dlq-topic", nil)
			require.NoError(t, err)

			streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream"
			sub, err := b.Subscribe(topic.TopicArn, "firehose", streamARN, "")
			require.NoError(t, err)

			redrivePolicy, _ := json.Marshal(map[string]string{
				"deadLetterTargetArn": dlqARN,
			})
			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy))
			require.NoError(t, err)

			_, err = b.Publish(topic.TopicArn, "test-message", "", "", nil)
			require.NoError(t, err)

			sns.WaitDeliveriesForTest(b)

			if tt.expectDLQ {
				assert.Equal(t, 1, sqsSender.count(), "DLQ must receive message on Firehose failure")
			} else {
				assert.Equal(t, 0, sqsSender.count(), "DLQ must not be called on Firehose success")
			}
		})
	}
}

func TestParityB_SNS_Firehose_DLQ_NoSQSSender(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	b.SetFirehoseBackend(&failingFirehosePutter{})

	topic, err := b.CreateTopic("firehose-no-sender", nil)
	require.NoError(t, err)

	streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream"
	sub, err := b.Subscribe(topic.TopicArn, "firehose", streamARN, "")
	require.NoError(t, err)

	redrivePolicy, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": "arn:aws:sqs:us-east-1:123456789012:dlq",
	})
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy))
	require.NoError(t, err)

	_, err = b.Publish(topic.TopicArn, "msg", "", "", nil)
	require.NoError(t, err, "must not error when SQSSender absent")
	sns.WaitDeliveriesForTest(b)
}

// --- SQS queue existence validation tests ---

func TestParityB_SetSubscriptionAttributes_DLQ_ExistenceCheck(t *testing.T) {
	t.Parallel()

	existingARN := "arn:aws:sqs:us-east-1:123456789012:existing-dlq"
	missingARN := "arn:aws:sqs:us-east-1:123456789012:missing-dlq"

	tests := []struct {
		name       string
		dlqARN     string
		errContain string
		wantErr    bool
	}{
		{
			name:    "existing_queue_accepted",
			dlqARN:  existingARN,
			wantErr: false,
		},
		{
			name:       "missing_queue_rejected",
			dlqARN:     missingARN,
			wantErr:    true,
			errContain: "does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			b.SetSQSChecker(&stubSQSChecker{
				existingARNs: map[string]bool{existingARN: true},
			})

			topic, err := b.CreateTopic("check-topic", nil)
			require.NoError(t, err)

			sub, err := b.Subscribe(topic.TopicArn, "http", "http://example.com/notify", "")
			require.NoError(t, err)

			redrivePolicy, _ := json.Marshal(map[string]string{
				"deadLetterTargetArn": tt.dlqARN,
			})
			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy))

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContain)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestParityB_SetSubscriptionAttributes_DLQ_NoChecker_Skips(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	// No SQSChecker wired — existence check must be skipped, not panic or error.

	topic, err := b.CreateTopic("no-checker-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(topic.TopicArn, "http", "http://example.com/notify", "")
	require.NoError(t, err)

	redrivePolicy, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": "arn:aws:sqs:us-east-1:123456789012:any-dlq",
	})
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy))
	require.NoError(t, err, "no checker means existence check is skipped")
}

// --- certURL region tests ---

func TestParityB_CertURL_UsesRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		region string
		name   string
	}{
		{name: "us_east_1", region: "us-east-1"},
		{name: "eu_west_1", region: "eu-west-1"},
		{name: "ap_southeast_2", region: "ap-southeast-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackendWithConfig("000000000000", tt.region)
			certURL := sns.SigningCertURLForTest(b)

			expected := "https://sns." + tt.region + ".amazonaws.com/SimpleNotificationService.pem"
			assert.Equal(t, expected, certURL, "certURL must use backend region, not hardcoded us-east-1")
			assert.Contains(t, certURL, tt.region, "certURL must embed region %s", tt.region)
		})
	}
}
