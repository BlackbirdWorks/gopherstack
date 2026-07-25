package sns_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// --- mock backends ---

type mockLambdaInvoker struct {
	invocations []lambdaInvocation
	mu          sync.Mutex
}

type lambdaInvocation struct {
	Name           string
	InvocationType string
	Payload        []byte
}

func (m *mockLambdaInvoker) InvokeFunction(
	_ context.Context,
	name, invocationType string,
	payload []byte,
) ([]byte, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.invocations = append(m.invocations, lambdaInvocation{
		Name:           name,
		InvocationType: invocationType,
		Payload:        payload,
	})

	return nil, 200, nil
}

func (m *mockLambdaInvoker) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.invocations)
}

func (m *mockLambdaInvoker) Last() lambdaInvocation {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.invocations) == 0 {
		return lambdaInvocation{}
	}

	return m.invocations[len(m.invocations)-1]
}

// All returns a snapshot of every recorded invocation in call order, for
// tests that need to assert delivery ordering (e.g. archive replay).
func (m *mockLambdaInvoker) All() []lambdaInvocation {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]lambdaInvocation, len(m.invocations))
	copy(out, m.invocations)

	return out
}

type mockFirehosePutter struct {
	streams map[string][][]byte
	mu      sync.Mutex
}

func newMockFirehose() *mockFirehosePutter {
	return &mockFirehosePutter{streams: make(map[string][][]byte)}
}

func (m *mockFirehosePutter) PutRecordBatch(streamName string, records [][]byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.streams[streamName] = append(m.streams[streamName], records...)

	return 0, nil
}

func (m *mockFirehosePutter) RecordsFor(streamName string) [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([][]byte{}, m.streams[streamName]...)
}

// --- helpers ---

func newTestSNSBackend(t *testing.T) *sns.InMemoryBackend {
	t.Helper()

	b := sns.NewInMemoryBackend()

	return b
}

func createTopicAndSubscribe(
	t *testing.T,
	b *sns.InMemoryBackend,
	protocol, endpoint string,
) string {
	t.Helper()

	topic, err := b.CreateTopic("test-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(topic.TopicArn, protocol, endpoint, "")
	require.NoError(t, err)

	return topic.TopicArn
}

// --- Lambda delivery tests ---

func TestSNS_LambdaDelivery_InvokesFunction(t *testing.T) {
	t.Parallel()

	b := newTestSNSBackend(t)
	lambda := &mockLambdaInvoker{}
	b.SetLambdaBackend(lambda)

	functionARN := "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
	topicARN := createTopicAndSubscribe(t, b, "lambda", functionARN)

	_, err := b.Publish(topicARN, "hello world", "subject", "", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, lambda.Count())

	inv := lambda.Last()
	assert.Equal(t, functionARN, inv.Name)
	assert.Equal(t, "Event", inv.InvocationType)

	// Verify payload is valid AWS SNS → Lambda envelope.
	var envelope map[string]any
	err = json.Unmarshal(inv.Payload, &envelope)
	require.NoError(t, err)

	records, ok := envelope["Records"].([]any)
	require.True(t, ok, "payload must have Records array")
	require.Len(t, records, 1)

	record, ok := records[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "aws:sns", record["EventSource"])
	assert.Equal(t, "1.0", record["EventVersion"])

	snsData, ok := record["Sns"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "hello world", snsData["Message"])
	assert.Equal(t, "Notification", snsData["Type"])
	assert.Equal(t, topicARN, snsData["TopicArn"])
}

func TestSNS_LambdaDelivery_NoBackendDoesNothing(t *testing.T) {
	t.Parallel()

	b := newTestSNSBackend(t)
	// no lambda backend set

	topicARN := createTopicAndSubscribe(
		t,
		b,
		"lambda",
		"arn:aws:lambda:us-east-1:123456789012:function:no-backend",
	)

	_, err := b.Publish(topicARN, "msg", "", "", nil)
	require.NoError(t, err)
	// should not panic or return error
}

func TestSNS_LambdaDelivery_SQSSubscriptionNotInvokedAsLambda(t *testing.T) {
	t.Parallel()

	b := newTestSNSBackend(t)
	lambda := &mockLambdaInvoker{}
	b.SetLambdaBackend(lambda)

	topic, err := b.CreateTopic("mixed-topic", nil)
	require.NoError(t, err)

	queueARN := "arn:aws:sqs:us-east-1:123456789012:my-queue"
	_, err = b.Subscribe(topic.TopicArn, "sqs", queueARN, "")
	require.NoError(t, err)

	_, err = b.Publish(topic.TopicArn, "sqs-message", "", "", nil)
	require.NoError(t, err)

	// Lambda should not be invoked for sqs protocol.
	assert.Equal(t, 0, lambda.Count())
}

func TestSNS_LambdaDelivery_MultipleSubscribersEachInvoked(t *testing.T) {
	t.Parallel()

	b := newTestSNSBackend(t)
	lambda := &mockLambdaInvoker{}
	b.SetLambdaBackend(lambda)

	topic, err := b.CreateTopic("multi-lambda-topic", nil)
	require.NoError(t, err)

	fn1 := "arn:aws:lambda:us-east-1:123456789012:function:fn1"
	fn2 := "arn:aws:lambda:us-east-1:123456789012:function:fn2"

	_, err = b.Subscribe(topic.TopicArn, "lambda", fn1, "")
	require.NoError(t, err)

	_, err = b.Subscribe(topic.TopicArn, "lambda", fn2, "")
	require.NoError(t, err)

	_, err = b.Publish(topic.TopicArn, "broadcast", "", "", nil)
	require.NoError(t, err)

	assert.Equal(t, 2, lambda.Count())
}

// --- Firehose delivery tests ---

func TestSNS_FirehoseDelivery_PutsRecord(t *testing.T) {
	t.Parallel()

	b := newTestSNSBackend(t)
	firehose := newMockFirehose()
	b.SetFirehoseBackend(firehose)

	streamName := "my-delivery-stream"
	streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/" + streamName
	topicARN := createTopicAndSubscribe(t, b, "firehose", streamARN)

	_, err := b.Publish(topicARN, "firehose-message", "", "", nil)
	require.NoError(t, err)

	records := firehose.RecordsFor(streamName)
	require.Len(t, records, 1)

	// AWS SNS→Firehose delivery defaults to RawMessageDelivery=false (the
	// subscription created above never sets it), so the delivered record is the
	// full SNS Notification JSON envelope, not the bare message body.
	var envelope map[string]any
	require.NoError(t, json.Unmarshal(records[0], &envelope))
	assert.Equal(t, "Notification", envelope["Type"])
	assert.Equal(t, "firehose-message", envelope["Message"])
	assert.Equal(t, topicARN, envelope["TopicArn"])
	assert.NotEmpty(t, envelope["Signature"])
	assert.NotEmpty(t, envelope["SigningCertURL"])
}

func TestSNS_FirehoseDelivery_RawMessageDeliveryPutsBareMessage(t *testing.T) {
	t.Parallel()

	b := newTestSNSBackend(t)
	firehose := newMockFirehose()
	b.SetFirehoseBackend(firehose)

	streamName := "raw-delivery-stream"
	streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/" + streamName

	topic, err := b.CreateTopic("raw-firehose-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(topic.TopicArn, "firehose", streamARN, "")
	require.NoError(t, err)
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "RawMessageDelivery", "true"))

	_, err = b.Publish(topic.TopicArn, "raw-message", "", "", nil)
	require.NoError(t, err)

	records := firehose.RecordsFor(streamName)
	require.Len(t, records, 1)
	assert.Equal(t, "raw-message", string(records[0]))
}

func TestSNS_FirehoseDelivery_NoBackendDoesNothing(t *testing.T) {
	t.Parallel()

	b := newTestSNSBackend(t)
	// no firehose backend set

	streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/no-backend"
	topicARN := createTopicAndSubscribe(t, b, "firehose", streamARN)

	_, err := b.Publish(topicARN, "msg", "", "", nil)
	require.NoError(t, err)
}

func TestSNS_FirehoseDelivery_NonFirehoseSubscriptionSkipped(t *testing.T) {
	t.Parallel()

	b := newTestSNSBackend(t)
	firehose := newMockFirehose()
	b.SetFirehoseBackend(firehose)

	// Add only an sqs subscriber.
	topic, err := b.CreateTopic("no-firehose-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:123456789012:queue", "")
	require.NoError(t, err)

	_, err = b.Publish(topic.TopicArn, "msg", "", "", nil)
	require.NoError(t, err)

	// No firehose stream should have received anything.
	assert.Empty(t, firehose.streams)
}

// Compile-time interface checks.
var (
	_ sns.LambdaInvoker  = (*mockLambdaInvoker)(nil)
	_ sns.FirehosePutter = (*mockFirehosePutter)(nil)
)

// TestFirehoseSubscriptionAutoConfirmed verifies that a Firehose
// subscription is auto-confirmed (not pending) and appears in attributes.
func TestFirehoseSubscriptionAutoConfirmed(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("firehose-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(
		tp.TopicArn, "firehose",
		"arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream",
		"",
	)
	require.NoError(t, err)
	assert.False(t, sub.PendingConfirmation, "firehose subscription must be auto-confirmed")
	assert.NotEmpty(t, sub.SubscriptionArn)

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Equal(t, "firehose", attrs["Protocol"])
	assert.Equal(t, "false", attrs["PendingConfirmation"])
}

// TestFirehoseSubscriptionReceivesPublish verifies that a Firehose
// subscription's endpoint appears in the subscription list after a publish.
func TestFirehoseSubscriptionReceivesPublish(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("firehose-pub-topic", nil)
	require.NoError(t, err)

	firehoseEndpoint := "arn:aws:firehose:us-east-1:000000000000:deliverystream/events"
	_, err = b.Subscribe(tp.TopicArn, "firehose", firehoseEndpoint, "")
	require.NoError(t, err)

	// Publish succeeds without error.
	msgID, err := b.Publish(tp.TopicArn, "firehose-data", "", "", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, msgID)
}

// TestFirehoseSubscriptionWithFilterPolicy verifies Firehose subscriptions
// honour the filter policy.
func TestFirehoseSubscriptionWithFilterPolicy(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("firehose-fp-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(
		tp.TopicArn, "firehose",
		"arn:aws:firehose:us-east-1:000000000000:deliverystream/filtered",
		`{"source":["app"]}`,
	)
	require.NoError(t, err)

	// Publish with matching attribute — should succeed.
	attrs := map[string]sns.MessageAttribute{
		"source": {DataType: "String", StringValue: "app"},
	}
	msgID, err := b.Publish(tp.TopicArn, "app-event", "", "", attrs)
	require.NoError(t, err)
	assert.NotEmpty(t, msgID)
}

// TestSubscribeFirehoseMissingRoleArnRejected verifies that subscribing
// with the firehose protocol without SubscriptionRoleArn returns 400.
func TestSubscribeFirehoseMissingRoleArnRejected(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	tp, err := b.CreateTopic("firehose-role-topic", nil)
	require.NoError(t, err)

	rec := doTestRequest(t, h, url.Values{
		"Action":   {"Subscribe"},
		"TopicArn": {tp.TopicArn},
		"Protocol": {"firehose"},
		"Endpoint": {"arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream"},
		"Version":  {"2010-03-31"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"firehose Subscribe without SubscriptionRoleArn must return 400")
	assert.Contains(t, rec.Body.String(), "SubscriptionRoleArn")
}

// TestSubscribeFirehoseWithRoleArnSucceeds verifies that subscribing
// with the firehose protocol and SubscriptionRoleArn succeeds.
func TestSubscribeFirehoseWithRoleArnSucceeds(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	tp, err := b.CreateTopic("firehose-role-ok-topic", nil)
	require.NoError(t, err)

	rec := doTestRequest(t, h, url.Values{
		"Action":                   {"Subscribe"},
		"TopicArn":                 {tp.TopicArn},
		"Protocol":                 {"firehose"},
		"Endpoint":                 {"arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream"},
		"Attributes.entry.1.key":   {"SubscriptionRoleArn"},
		"Attributes.entry.1.value": {"arn:aws:iam::000000000000:role/firehose-role"},
		"Version":                  {"2010-03-31"},
	})
	assert.Equal(t, http.StatusOK, rec.Code,
		"firehose Subscribe with SubscriptionRoleArn must return 200")
	assert.Contains(t, rec.Body.String(), "SubscriptionArn")
}

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

func TestFirehoseDeliveryFailureSendsToDLQ(t *testing.T) {
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

func TestFirehoseDeliveryFailureNoSQSSenderNoError(t *testing.T) {
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

var errSimulatedFailure = errors.New("simulated invocation failure")

// failingLambdaInvoker always returns an error from InvokeFunction.
type failingLambdaInvoker struct{}

func (f *failingLambdaInvoker) InvokeFunction(
	_ context.Context, _, _ string, _ []byte,
) ([]byte, int, error) {
	return nil, 500, errSimulatedFailure
}

// successLambdaInvoker always succeeds.
type successLambdaInvoker struct{}

func (s *successLambdaInvoker) InvokeFunction(
	_ context.Context, _, _ string, _ []byte,
) ([]byte, int, error) {
	return []byte(`"ok"`), 200, nil
}

// mockLambdaDLQSender records SQS messages sent.
type mockLambdaDLQSender struct {
	messages []string // before mu to minimize GC pointer scan span
	mu       sync.Mutex
}

func (m *mockLambdaDLQSender) SendMessageToQueue(_ context.Context, _ string, body string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, body)

	return nil
}

func (m *mockLambdaDLQSender) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.messages)
}

func TestLambdaDeliveryFailureSendsToDLQ(t *testing.T) {
	t.Parallel()

	dlqARN := "arn:aws:sqs:us-east-1:123456789012:lambda-dlq"

	tests := []struct {
		invoker   sns.LambdaInvoker
		name      string
		expectDLQ bool
	}{
		{
			name:      "lambda_success_no_dlq",
			invoker:   &successLambdaInvoker{},
			expectDLQ: false,
		},
		{
			name:      "lambda_failure_triggers_dlq",
			invoker:   &failingLambdaInvoker{},
			expectDLQ: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sqsSender := &mockLambdaDLQSender{}
			b := sns.NewInMemoryBackend()
			b.SetLambdaBackend(tt.invoker)
			b.SetSQSSender(sqsSender)

			topic, err := b.CreateTopic("lambda-dlq-test", nil)
			require.NoError(t, err)

			sub, err := b.Subscribe(topic.TopicArn, "lambda", "arn:aws:lambda:us-east-1:123:function:my-fn", "")
			require.NoError(t, err)

			redrivePolicy, _ := json.Marshal(map[string]string{
				"deadLetterTargetArn": dlqARN,
			})
			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy))
			require.NoError(t, err)

			_, err = b.Publish(topic.TopicArn, "test-message", "subject", "", nil)
			require.NoError(t, err)

			sns.WaitDeliveriesForTest(b)

			if tt.expectDLQ {
				assert.Equal(t, 1, sqsSender.count(), "DLQ must receive message on Lambda failure")
			} else {
				assert.Equal(t, 0, sqsSender.count(), "DLQ must not be called on Lambda success")
			}
		})
	}
}

// TestLambdaDeliveryFailureNoSenderNoError verifies no panic when no SQSSender configured.
func TestLambdaDeliveryFailureNoSenderNoError(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	b.SetLambdaBackend(&failingLambdaInvoker{})

	topic, err := b.CreateTopic("no-sender-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(topic.TopicArn, "lambda", "arn:aws:lambda:us-east-1:123:function:fn", "")
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
