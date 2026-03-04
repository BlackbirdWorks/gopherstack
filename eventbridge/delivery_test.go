package eventbridge_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/eventbridge"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSQSSender records messages sent to queues.
type mockSQSSender struct {
	messages map[string][]string
	mu       sync.Mutex
}

func newMockSQSSender() *mockSQSSender {
	return &mockSQSSender{messages: make(map[string][]string)}
}

func (m *mockSQSSender) SendMessageToQueue(_ context.Context, queueARN, messageBody string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages[queueARN] = append(m.messages[queueARN], messageBody)

	return nil
}

func (m *mockSQSSender) MessagesFor(queueARN string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]string{}, m.messages[queueARN]...)
}

// mockLambdaInvoker records Lambda invocations.
type mockLambdaInvoker struct {
	invocations []lambdaInvocation
	mu          sync.Mutex
}

type lambdaInvocation struct {
	name    string
	payload string
}

func newMockLambdaInvoker() *mockLambdaInvoker {
	return &mockLambdaInvoker{}
}

func (m *mockLambdaInvoker) InvokeFunction(_ context.Context, name, _ string, payload []byte) ([]byte, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.invocations = append(m.invocations, lambdaInvocation{name: name, payload: string(payload)})

	return []byte(`{}`), 200, nil
}

func (m *mockLambdaInvoker) Invocations() []lambdaInvocation {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]lambdaInvocation{}, m.invocations...)
}

// setupDeliveryBackend creates a backend wired with a mock SQS sender.
func setupDeliveryBackend(t *testing.T, sqs *mockSQSSender, lam *mockLambdaInvoker) *eventbridge.InMemoryBackend {
	t.Helper()
	log := logger.NewLogger(slog.LevelDebug)
	backend := eventbridge.NewInMemoryBackend()
	backend.SetLogger(log)
	backend.SetDeliveryTargets(&eventbridge.DeliveryTargets{
		SQS:    sqs,
		Lambda: lam,
	})

	return backend
}

func TestDelivery_SQSFanout(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	backend := setupDeliveryBackend(t, sqsMock, nil)

	// Create a rule with event pattern and SQS target.
	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:         "my-rule",
		EventPattern: `{"source": ["test.service"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:my-queue"
	_, err = backend.PutTargets("my-rule", "default", []eventbridge.Target{
		{ID: "t1", Arn: queueARN},
	})
	require.NoError(t, err)

	// Put events.
	backend.PutEvents([]eventbridge.EventEntry{
		{Source: "test.service", DetailType: "MyEvent", Detail: `{"key": "value"}`},
	})

	// Allow async delivery goroutine to run.
	require.Eventually(t, func() bool {
		return len(sqsMock.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msgs := sqsMock.MessagesFor(queueARN)
	assert.Len(t, msgs, 1)
	assert.Contains(t, msgs[0], "test.service")
}

func TestDelivery_NoMatchNoDelivery(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	backend := setupDeliveryBackend(t, sqsMock, nil)

	// Create a rule that matches "other.service" only.
	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:         "other-rule",
		EventPattern: `{"source": ["other.service"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:my-queue"
	_, err = backend.PutTargets("other-rule", "default", []eventbridge.Target{
		{ID: "t1", Arn: queueARN},
	})
	require.NoError(t, err)

	// Put event from "test.service" - should NOT match.
	backend.PutEvents([]eventbridge.EventEntry{
		{Source: "test.service", DetailType: "MyEvent", Detail: `{}`},
	})

	// Wait briefly and confirm no messages.
	time.Sleep(100 * time.Millisecond)
	msgs := sqsMock.MessagesFor(queueARN)
	assert.Empty(t, msgs)
}

func TestDelivery_DisabledRuleNoDelivery(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	backend := setupDeliveryBackend(t, sqsMock, nil)

	// Create a DISABLED rule.
	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:         "disabled-rule",
		EventPattern: `{"source": ["test.service"]}`,
		State:        "DISABLED",
	})
	require.NoError(t, err)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:disabled-queue"
	_, err = backend.PutTargets("disabled-rule", "default", []eventbridge.Target{
		{ID: "t1", Arn: queueARN},
	})
	require.NoError(t, err)

	backend.PutEvents([]eventbridge.EventEntry{
		{Source: "test.service", DetailType: "MyEvent", Detail: `{}`},
	})

	time.Sleep(100 * time.Millisecond)
	msgs := sqsMock.MessagesFor(queueARN)
	assert.Empty(t, msgs)
}

func TestDelivery_LambdaFanout(t *testing.T) {
	t.Parallel()

	lamMock := newMockLambdaInvoker()
	backend := setupDeliveryBackend(t, nil, lamMock)

	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:         "lambda-rule",
		EventPattern: `{"source": ["my.source"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	_, err = backend.PutTargets("lambda-rule", "default", []eventbridge.Target{
		{ID: "t1", Arn: lambdaARN},
	})
	require.NoError(t, err)

	backend.PutEvents([]eventbridge.EventEntry{
		{Source: "my.source", DetailType: "Evt", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(lamMock.Invocations()) > 0
	}, 2*time.Second, 10*time.Millisecond)

	invocations := lamMock.Invocations()
	assert.Len(t, invocations, 1)
	assert.Equal(t, lambdaARN, invocations[0].name)
}

func TestDelivery_InputOverride(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	backend := setupDeliveryBackend(t, sqsMock, nil)

	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:         "override-rule",
		EventPattern: `{"source": ["svc"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:override-queue"
	_, err = backend.PutTargets("override-rule", "default", []eventbridge.Target{
		{ID: "t1", Arn: queueARN, Input: `{"custom": "payload"}`},
	})
	require.NoError(t, err)

	backend.PutEvents([]eventbridge.EventEntry{
		{Source: "svc", DetailType: "Evt", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsMock.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msgs := sqsMock.MessagesFor(queueARN)
	assert.Len(t, msgs, 1)
	assert.JSONEq(t, `{"custom": "payload"}`, msgs[0])
}

func TestDelivery_ResourcesArrayMatching(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	backend := setupDeliveryBackend(t, sqsMock, nil)

	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:         "resources-rule",
		EventPattern: `{"resources": [{"prefix": "arn:aws:s3:::"}]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:resources-queue"
	_, err = backend.PutTargets("resources-rule", "default", []eventbridge.Target{
		{ID: "t1", Arn: queueARN},
	})
	require.NoError(t, err)

	// Event with a resources list that contains an S3 ARN — should match.
	backend.PutEvents([]eventbridge.EventEntry{
		{
			Source:     "aws.s3",
			DetailType: "Object Created",
			Detail:     `{}`,
			Resources:  []string{"arn:aws:s3:::my-bucket", "arn:aws:iam:::role/myrole"},
		},
	})

	require.Eventually(t, func() bool {
		return len(sqsMock.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msgs := sqsMock.MessagesFor(queueARN)
	assert.Len(t, msgs, 1)
	assert.Contains(t, msgs[0], "aws.s3")
}

func TestDelivery_ResourcesArrayNoMatch(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	backend := setupDeliveryBackend(t, sqsMock, nil)

	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:         "resources-no-match-rule",
		EventPattern: `{"resources": ["arn:aws:s3:::specific-bucket"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:resources-nomatch-queue"
	_, err = backend.PutTargets("resources-no-match-rule", "default", []eventbridge.Target{
		{ID: "t1", Arn: queueARN},
	})
	require.NoError(t, err)

	// Event with a resources list that does NOT contain the specific bucket — should not match.
	backend.PutEvents([]eventbridge.EventEntry{
		{
			Source:     "aws.s3",
			DetailType: "Object Created",
			Detail:     `{}`,
			Resources:  []string{"arn:aws:s3:::other-bucket"},
		},
	})

	time.Sleep(100 * time.Millisecond)
	msgs := sqsMock.MessagesFor(queueARN)
	assert.Empty(t, msgs)
}
