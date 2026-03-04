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

func TestDelivery_SQS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		ruleName      string
		eventPattern  string
		ruleState     string
		queueARN      string
		targetInput   string
		events        []eventbridge.EventEntry
		wantDelivered bool
		wantLen       int
		wantContains  string
		wantJSONEq    string
	}{
		{
			name:         "fanout_delivers_to_matching_queue",
			ruleName:     "my-rule",
			eventPattern: `{"source": ["test.service"]}`,
			ruleState:    "ENABLED",
			queueARN:     "arn:aws:sqs:us-east-1:000000000000:my-queue",
			events: []eventbridge.EventEntry{
				{Source: "test.service", DetailType: "MyEvent", Detail: `{"key": "value"}`},
			},
			wantDelivered: true,
			wantLen:       1,
			wantContains:  "test.service",
		},
		{
			name:         "no_match_no_delivery",
			ruleName:     "other-rule",
			eventPattern: `{"source": ["other.service"]}`,
			ruleState:    "ENABLED",
			queueARN:     "arn:aws:sqs:us-east-1:000000000000:no-match-queue",
			events: []eventbridge.EventEntry{
				{Source: "test.service", DetailType: "MyEvent", Detail: `{}`},
			},
			wantDelivered: false,
		},
		{
			name:         "disabled_rule_no_delivery",
			ruleName:     "disabled-rule",
			eventPattern: `{"source": ["test.service"]}`,
			ruleState:    "DISABLED",
			queueARN:     "arn:aws:sqs:us-east-1:000000000000:disabled-queue",
			events: []eventbridge.EventEntry{
				{Source: "test.service", DetailType: "MyEvent", Detail: `{}`},
			},
			wantDelivered: false,
		},
		{
			name:         "input_override_sends_custom_payload",
			ruleName:     "override-rule",
			eventPattern: `{"source": ["svc"]}`,
			ruleState:    "ENABLED",
			queueARN:     "arn:aws:sqs:us-east-1:000000000000:override-queue",
			targetInput:  `{"custom": "payload"}`,
			events: []eventbridge.EventEntry{
				{Source: "svc", DetailType: "Evt", Detail: `{}`},
			},
			wantDelivered: true,
			wantLen:       1,
			wantJSONEq:    `{"custom": "payload"}`,
		},
		{
			name:         "resources_prefix_match_delivers",
			ruleName:     "resources-rule",
			eventPattern: `{"resources": [{"prefix": "arn:aws:s3:::"}]}`,
			ruleState:    "ENABLED",
			queueARN:     "arn:aws:sqs:us-east-1:000000000000:resources-queue",
			events: []eventbridge.EventEntry{
				{
					Source:     "aws.s3",
					DetailType: "Object Created",
					Detail:     `{}`,
					Resources:  []string{"arn:aws:s3:::my-bucket", "arn:aws:iam:::role/myrole"},
				},
			},
			wantDelivered: true,
			wantLen:       1,
			wantContains:  "aws.s3",
		},
		{
			name:         "resources_no_match_no_delivery",
			ruleName:     "resources-no-match-rule",
			eventPattern: `{"resources": ["arn:aws:s3:::specific-bucket"]}`,
			ruleState:    "ENABLED",
			queueARN:     "arn:aws:sqs:us-east-1:000000000000:resources-nomatch-queue",
			events: []eventbridge.EventEntry{
				{
					Source:     "aws.s3",
					DetailType: "Object Created",
					Detail:     `{}`,
					Resources:  []string{"arn:aws:s3:::other-bucket"},
				},
			},
			wantDelivered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sqsMock := newMockSQSSender()
			backend := setupDeliveryBackend(t, sqsMock, nil)

			_, err := backend.PutRule(eventbridge.PutRuleInput{
				Name:         tt.ruleName,
				EventPattern: tt.eventPattern,
				State:        tt.ruleState,
			})
			require.NoError(t, err)

			target := eventbridge.Target{ID: "t1", Arn: tt.queueARN}
			if tt.targetInput != "" {
				target.Input = tt.targetInput
			}

			_, err = backend.PutTargets(tt.ruleName, "default", []eventbridge.Target{target})
			require.NoError(t, err)

			backend.PutEvents(tt.events)

			if tt.wantDelivered {
				require.Eventually(t, func() bool {
					return len(sqsMock.MessagesFor(tt.queueARN)) > 0
				}, 2*time.Second, 10*time.Millisecond)

				msgs := sqsMock.MessagesFor(tt.queueARN)
				assert.Len(t, msgs, tt.wantLen)

				if tt.wantContains != "" {
					assert.Contains(t, msgs[0], tt.wantContains)
				}

				if tt.wantJSONEq != "" {
					assert.JSONEq(t, tt.wantJSONEq, msgs[0])
				}
			} else {
				time.Sleep(100 * time.Millisecond)
				msgs := sqsMock.MessagesFor(tt.queueARN)
				assert.Empty(t, msgs)
			}
		})
	}
}

func TestDelivery_Lambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		ruleName        string
		eventPattern    string
		lambdaARN       string
		events          []eventbridge.EventEntry
		wantInvocations int
	}{
		{
			name:         "fanout_invokes_lambda_function",
			ruleName:     "lambda-rule",
			eventPattern: `{"source": ["my.source"]}`,
			lambdaARN:    "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
			events: []eventbridge.EventEntry{
				{Source: "my.source", DetailType: "Evt", Detail: `{}`},
			},
			wantInvocations: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lamMock := newMockLambdaInvoker()
			backend := setupDeliveryBackend(t, nil, lamMock)

			_, err := backend.PutRule(eventbridge.PutRuleInput{
				Name:         tt.ruleName,
				EventPattern: tt.eventPattern,
				State:        "ENABLED",
			})
			require.NoError(t, err)

			_, err = backend.PutTargets(tt.ruleName, "default", []eventbridge.Target{
				{ID: "t1", Arn: tt.lambdaARN},
			})
			require.NoError(t, err)

			backend.PutEvents(tt.events)

			require.Eventually(t, func() bool {
				return len(lamMock.Invocations()) >= tt.wantInvocations
			}, 2*time.Second, 10*time.Millisecond)

			invocations := lamMock.Invocations()
			assert.Len(t, invocations, tt.wantInvocations)
			assert.Equal(t, tt.lambdaARN, invocations[0].name)
		})
	}
}
