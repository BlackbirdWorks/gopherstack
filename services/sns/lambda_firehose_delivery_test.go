package sns_test

import (
	"context"
	"encoding/json"
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
	assert.Equal(t, "firehose-message", string(records[0]))
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
var _ sns.LambdaInvoker = (*mockLambdaInvoker)(nil)
var _ sns.FirehosePutter = (*mockFirehosePutter)(nil)
