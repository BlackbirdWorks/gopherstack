package eventbridge_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
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

// mockSNSPublisher records messages published to topics.
type mockSNSPublisher struct {
	messages map[string][]string
	mu       sync.Mutex
}

func newMockSNSPublisher() *mockSNSPublisher {
	return &mockSNSPublisher{messages: make(map[string][]string)}
}

func (m *mockSNSPublisher) PublishToTopic(_ context.Context, topicARN, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages[topicARN] = append(m.messages[topicARN], message)

	return nil
}

func (m *mockSNSPublisher) MessagesFor(topicARN string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]string{}, m.messages[topicARN]...)
}

// setupDeliveryBackend creates a backend wired with a mock SQS sender.
func setupDeliveryBackend(t *testing.T, sqs *mockSQSSender, lam *mockLambdaInvoker) *eventbridge.InMemoryBackend {
	t.Helper()
	backend := eventbridge.NewInMemoryBackend()
	backend.SetDeliveryTargets(&eventbridge.DeliveryTargets{
		SQS:    sqs,
		Lambda: lam,
	})

	return backend
}

// setupDeliveryBackendFull creates a backend wired with mock SQS, Lambda, and SNS.
func setupDeliveryBackendFull(
	t *testing.T,
	sqs *mockSQSSender,
	lam *mockLambdaInvoker,
	sns *mockSNSPublisher,
) *eventbridge.InMemoryBackend {
	t.Helper()
	backend := eventbridge.NewInMemoryBackend()
	backend.SetDeliveryTargets(&eventbridge.DeliveryTargets{
		SQS:    sqs,
		Lambda: lam,
		SNS:    sns,
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
		wantContains  string
		wantJSONEq    string
		events        []eventbridge.EventEntry
		wantLen       int
		wantDelivered bool
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

func TestDelivery_FullEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		events     []eventbridge.EventEntry
		wantFields []string
	}{
		{
			name: "full_envelope_includes_standard_fields",
			events: []eventbridge.EventEntry{
				{Source: "test.service", DetailType: "MyEvent", Detail: `{"key": "value"}`},
			},
			wantFields: []string{
				"version",
				"id",
				"source",
				"account",
				"time",
				"region",
				"detail-type",
				"detail",
				"resources",
			},
		},
		{
			name: "detail_is_parsed_as_object_not_string",
			events: []eventbridge.EventEntry{
				{Source: "test.service", DetailType: "MyEvent", Detail: `{"nested": {"key": "value"}}`},
			},
			wantFields: []string{`"nested"`},
		},
		{
			name: "nil_resources_serializes_as_empty_array",
			events: []eventbridge.EventEntry{
				{Source: "test.service", DetailType: "MyEvent", Detail: `{}`, Resources: nil},
			},
			wantFields: []string{`"resources":[]`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sqsMock := newMockSQSSender()
			backend := setupDeliveryBackend(t, sqsMock, nil)
			queueARN := "arn:aws:sqs:us-east-1:000000000000:envelope-queue-" + tt.name

			_, err := backend.PutRule(eventbridge.PutRuleInput{
				Name:         "envelope-rule-" + tt.name,
				EventPattern: `{"source": ["test.service"]}`,
				State:        "ENABLED",
			})
			require.NoError(t, err)

			_, err = backend.PutTargets("envelope-rule-"+tt.name, "default", []eventbridge.Target{
				{ID: "t1", Arn: queueARN},
			})
			require.NoError(t, err)

			backend.PutEvents(tt.events)

			require.Eventually(t, func() bool {
				return len(sqsMock.MessagesFor(queueARN)) > 0
			}, 2*time.Second, 10*time.Millisecond)

			msgs := sqsMock.MessagesFor(queueARN)
			require.Len(t, msgs, 1)

			for _, field := range tt.wantFields {
				assert.Contains(t, msgs[0], field, "expected field %q in payload", field)
			}
		})
	}
}

func TestDelivery_SharedEventIDAcrossTargets(t *testing.T) {
	t.Parallel()

	sqsMock1 := newMockSQSSender()
	sqsMock2 := newMockSQSSender()

	backend := eventbridge.NewInMemoryBackend()
	backend.SetDeliveryTargets(&eventbridge.DeliveryTargets{
		SQS: &multiQueueSender{senders: map[string]*mockSQSSender{
			"arn:aws:sqs:us-east-1:000000000000:queue-a": sqsMock1,
			"arn:aws:sqs:us-east-1:000000000000:queue-b": sqsMock2,
		}},
	})

	_, err := backend.PutRule(eventbridge.PutRuleInput{
		Name:         "shared-id-rule",
		EventPattern: `{"source": ["shared.id.service"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	_, err = backend.PutTargets("shared-id-rule", "default", []eventbridge.Target{
		{ID: "t1", Arn: "arn:aws:sqs:us-east-1:000000000000:queue-a"},
		{ID: "t2", Arn: "arn:aws:sqs:us-east-1:000000000000:queue-b"},
	})
	require.NoError(t, err)

	backend.PutEvents([]eventbridge.EventEntry{
		{Source: "shared.id.service", DetailType: "Evt", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsMock1.MessagesFor("arn:aws:sqs:us-east-1:000000000000:queue-a")) > 0 &&
			len(sqsMock2.MessagesFor("arn:aws:sqs:us-east-1:000000000000:queue-b")) > 0
	}, 2*time.Second, 10*time.Millisecond)

	var id1, id2 struct {
		ID string `json:"id"`
	}

	msg1 := sqsMock1.MessagesFor("arn:aws:sqs:us-east-1:000000000000:queue-a")[0]
	msg2 := sqsMock2.MessagesFor("arn:aws:sqs:us-east-1:000000000000:queue-b")[0]

	require.NoError(t, json.Unmarshal([]byte(msg1), &id1))
	require.NoError(t, json.Unmarshal([]byte(msg2), &id2))
	assert.NotEmpty(t, id1.ID)
	assert.Equal(t, id1.ID, id2.ID, "all targets for the same rule+event must share the same event id")
}

// multiQueueSender routes SendMessageToQueue calls to the matching mockSQSSender by ARN.
type multiQueueSender struct {
	senders map[string]*mockSQSSender
}

func (m *multiQueueSender) SendMessageToQueue(ctx context.Context, queueARN, messageBody string) error {
	if s, ok := m.senders[queueARN]; ok {
		return s.SendMessageToQueue(ctx, queueARN, messageBody)
	}

	return nil
}

func TestDelivery_InputPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		inputPath    string
		wantContains string
		wantJSONEq   string
		events       []eventbridge.EventEntry
	}{
		{
			name:      "input_path_extracts_source",
			inputPath: "$.source",
			events: []eventbridge.EventEntry{
				{Source: "path.service", DetailType: "Evt", Detail: `{}`},
			},
			wantJSONEq: `"path.service"`,
		},
		{
			name:      "input_path_extracts_detail_field",
			inputPath: "$.detail",
			events: []eventbridge.EventEntry{
				{Source: "path.service", DetailType: "Evt", Detail: `{"key": "extracted"}`},
			},
			wantJSONEq: `{"key":"extracted"}`,
		},
		{
			name:      "input_path_extracts_nested_field",
			inputPath: "$.detail.key",
			events: []eventbridge.EventEntry{
				{Source: "path.service", DetailType: "Evt", Detail: `{"key": "nested-value"}`},
			},
			wantJSONEq: `"nested-value"`,
		},
		{
			name:      "input_path_root_returns_full_envelope",
			inputPath: "$",
			events: []eventbridge.EventEntry{
				{Source: "path.service", DetailType: "Evt", Detail: `{}`},
			},
			wantContains: "path.service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sqsMock := newMockSQSSender()
			backend := setupDeliveryBackend(t, sqsMock, nil)
			queueARN := "arn:aws:sqs:us-east-1:000000000000:path-queue-" + tt.name
			ruleName := "path-rule-" + tt.name

			_, err := backend.PutRule(eventbridge.PutRuleInput{
				Name:         ruleName,
				EventPattern: `{"source": ["path.service"]}`,
				State:        "ENABLED",
			})
			require.NoError(t, err)

			_, err = backend.PutTargets(ruleName, "default", []eventbridge.Target{
				{ID: "t1", Arn: queueARN, InputPath: tt.inputPath},
			})
			require.NoError(t, err)

			backend.PutEvents(tt.events)

			require.Eventually(t, func() bool {
				return len(sqsMock.MessagesFor(queueARN)) > 0
			}, 2*time.Second, 10*time.Millisecond)

			msgs := sqsMock.MessagesFor(queueARN)
			require.Len(t, msgs, 1)

			if tt.wantJSONEq != "" {
				assert.JSONEq(t, tt.wantJSONEq, msgs[0])
			}

			if tt.wantContains != "" {
				assert.Contains(t, msgs[0], tt.wantContains)
			}
		})
	}
}

func TestDelivery_InputTransformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		inputTransformer *eventbridge.InputTransformer
		name             string
		wantJSONEq       string
		wantContains     string
		events           []eventbridge.EventEntry
	}{
		{
			name: "input_transformer_simple_variable_substitution",
			inputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"src":  "$.source",
					"type": "$.detail-type",
				},
				InputTemplate: `{"event_source": "<src>", "event_type": "<type>"}`,
			},
			events: []eventbridge.EventEntry{
				{Source: "transform.service", DetailType: "TransformEvent", Detail: `{}`},
			},
			wantJSONEq: `{"event_source": "transform.service", "event_type": "TransformEvent"}`,
		},
		{
			name: "input_transformer_extracts_detail_field",
			inputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"orderID": "$.detail.orderId",
				},
				InputTemplate: `{"orderId": "<orderID>"}`,
			},
			events: []eventbridge.EventEntry{
				{Source: "order.service", DetailType: "OrderPlaced", Detail: `{"orderId": "abc-123"}`},
			},
			wantJSONEq: `{"orderId": "abc-123"}`,
		},
		{
			name: "input_transformer_missing_path_uses_empty_string",
			inputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"missing": "$.detail.nonexistent",
				},
				InputTemplate: `{"val": "<missing>"}`,
			},
			events: []eventbridge.EventEntry{
				{Source: "transform.service", DetailType: "Evt", Detail: `{"other": "field"}`},
			},
			wantJSONEq: `{"val": ""}`,
		},
		{
			name: "input_transformer_plain_text_template",
			inputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"src": "$.source",
				},
				InputTemplate: `"Event from <src>"`,
			},
			events: []eventbridge.EventEntry{
				{Source: "text.service", DetailType: "Evt", Detail: `{}`},
			},
			wantContains: "text.service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sqsMock := newMockSQSSender()
			backend := setupDeliveryBackend(t, sqsMock, nil)
			queueARN := "arn:aws:sqs:us-east-1:000000000000:transform-queue-" + tt.name
			ruleName := "transform-rule-" + tt.name

			_, err := backend.PutRule(eventbridge.PutRuleInput{
				Name:         ruleName,
				EventPattern: `{"source": ["transform.service", "order.service", "text.service"]}`,
				State:        "ENABLED",
			})
			require.NoError(t, err)

			_, err = backend.PutTargets(ruleName, "default", []eventbridge.Target{
				{ID: "t1", Arn: queueARN, InputTransformer: tt.inputTransformer},
			})
			require.NoError(t, err)

			backend.PutEvents(tt.events)

			require.Eventually(t, func() bool {
				return len(sqsMock.MessagesFor(queueARN)) > 0
			}, 2*time.Second, 10*time.Millisecond)

			msgs := sqsMock.MessagesFor(queueARN)
			require.Len(t, msgs, 1)

			if tt.wantJSONEq != "" {
				assert.JSONEq(t, tt.wantJSONEq, msgs[0])
			}

			if tt.wantContains != "" {
				assert.Contains(t, msgs[0], tt.wantContains)
			}
		})
	}
}

func TestDelivery_SNS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		ruleName      string
		eventPattern  string
		topicARN      string
		wantContains  string
		events        []eventbridge.EventEntry
		wantLen       int
		wantDelivered bool
	}{
		{
			name:         "fanout_delivers_to_matching_topic",
			ruleName:     "sns-rule",
			eventPattern: `{"source": ["sns.test.service"]}`,
			topicARN:     "arn:aws:sns:us-east-1:000000000000:my-topic",
			events: []eventbridge.EventEntry{
				{Source: "sns.test.service", DetailType: "SnsEvent", Detail: `{"msg": "hello"}`},
			},
			wantDelivered: true,
			wantLen:       1,
			wantContains:  "sns.test.service",
		},
		{
			name:         "disabled_rule_no_sns_delivery",
			ruleName:     "sns-disabled-rule",
			eventPattern: `{"source": ["sns.test.service"]}`,
			topicARN:     "arn:aws:sns:us-east-1:000000000000:disabled-topic",
			events: []eventbridge.EventEntry{
				{Source: "sns.test.service", DetailType: "SnsEvent", Detail: `{}`},
			},
			wantDelivered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snsMock := newMockSNSPublisher()
			backend := setupDeliveryBackendFull(t, nil, nil, snsMock)

			state := "ENABLED"
			if !tt.wantDelivered {
				state = "DISABLED"
			}

			_, err := backend.PutRule(eventbridge.PutRuleInput{
				Name:         tt.ruleName,
				EventPattern: tt.eventPattern,
				State:        state,
			})
			require.NoError(t, err)

			_, err = backend.PutTargets(tt.ruleName, "default", []eventbridge.Target{
				{ID: "t1", Arn: tt.topicARN},
			})
			require.NoError(t, err)

			backend.PutEvents(tt.events)

			if tt.wantDelivered {
				require.Eventually(t, func() bool {
					return len(snsMock.MessagesFor(tt.topicARN)) > 0
				}, 2*time.Second, 10*time.Millisecond)

				msgs := snsMock.MessagesFor(tt.topicARN)
				assert.Len(t, msgs, tt.wantLen)

				if tt.wantContains != "" {
					assert.Contains(t, msgs[0], tt.wantContains)
				}
			} else {
				require.Never(t, func() bool {
					return len(snsMock.MessagesFor(tt.topicARN)) > 0
				}, 100*time.Millisecond, 10*time.Millisecond, "expected no messages for disabled rule")
			}
		})
	}
}

func TestDelivery_UnsupportedARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		targets []eventbridge.Target
		events  []eventbridge.EventEntry
	}{
		{
			name: "unsupported_arn_logs_warning_no_panic",
			targets: []eventbridge.Target{
				{ID: "t1", Arn: "arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream"},
			},
			events: []eventbridge.EventEntry{
				{Source: "warn.test.service", DetailType: "Evt", Detail: `{}`},
			},
		},
		{
			name: "nil_sqs_target_does_not_panic",
			targets: []eventbridge.Target{
				{ID: "t1", Arn: "arn:aws:sqs:us-east-1:000000000000:no-sqs-backend"},
			},
			events: []eventbridge.EventEntry{
				{Source: "warn.test.service", DetailType: "Evt", Detail: `{}`},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// No SQS/SNS/Lambda configured - targets get dropped gracefully.
			backend := eventbridge.NewInMemoryBackend()
			backend.SetDeliveryTargets(&eventbridge.DeliveryTargets{})

			_, err := backend.PutRule(eventbridge.PutRuleInput{
				Name:         "warn-rule-" + tt.name,
				EventPattern: `{"source": ["warn.test.service"]}`,
				State:        "ENABLED",
			})
			require.NoError(t, err)

			_, err = backend.PutTargets("warn-rule-"+tt.name, "default", tt.targets)
			require.NoError(t, err)

			// Should not panic even with no backend configured.
			require.NotPanics(t, func() {
				backend.PutEvents(tt.events)
			})
		})
	}
}
