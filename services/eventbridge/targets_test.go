package eventbridge_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTarget_DeadLetterConfigStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	dlqARN := "arn:aws:sqs:us-east-1:123456789012:dlq"
	_, err = b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
		{
			ID:               "t1",
			Arn:              "arn:aws:lambda:us-east-1:123456789012:function:fn",
			DeadLetterConfig: &eventbridge.DeadLetterConfig{Arn: dlqARN},
		},
	})
	require.NoError(t, err)

	targets, _, err := b.ListTargetsByRule(context.Background(), "r", "", "", 0)
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.NotNil(t, targets[0].DeadLetterConfig)
	assert.Equal(t, dlqARN, targets[0].DeadLetterConfig.Arn)
}

func TestTarget_BatchParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: "arn:aws:batch:us-east-1:123456789012:job-queue/my-queue",
			BatchParameters: &eventbridge.BatchParameters{
				JobDefinition: "arn:aws:batch:us-east-1:123456789012:job-definition/my-job",
				JobName:       "my-job-run",
			},
		},
	})
	require.NoError(t, err)

	targets, _, err := b.ListTargetsByRule(context.Background(), "r", "", "", 0)
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.NotNil(t, targets[0].BatchParameters)
	assert.Equal(t, "my-job-run", targets[0].BatchParameters.JobName)
}

func TestPutTargets_InputTransformerKeyValidation(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	// Valid keys accepted.
	failed, err := b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: "arn:aws:sqs:us-east-1:123456789012:q",
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"myVar":   "$.source",
					"myVar_2": "$.detail.type",
					"V123":    "$.id",
				},
				InputTemplate: `{"v": "<myVar>"}`,
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, failed)

	// Invalid key (contains hyphen) — rejected via FailedEntry.
	failed, err = b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
		{
			ID:  "t2",
			Arn: "arn:aws:sqs:us-east-1:123456789012:q",
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"bad-key": "$.source",
				},
				InputTemplate: `{"v": "<bad-key>"}`,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, failed, 1)
	assert.Equal(t, "t2", failed[0].TargetID)
}

func TestTargets_MaxTargetsPerRuleEnforced(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "limit-rule",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	targets := make([]eventbridge.Target, 5)
	for i := range targets {
		targets[i] = eventbridge.Target{
			ID:  fmt.Sprintf("t%d", i),
			Arn: fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:q%d", i),
		}
	}

	_, err = b.PutTargets(context.Background(), "limit-rule", "", targets)
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "limit-rule", "", []eventbridge.Target{
		{ID: "t-overflow", Arn: "arn:aws:sqs:us-east-1:123456789012:overflow"},
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestTargets_InputOverridePayload(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sqsSink := newMockSQSSender()
	queueARN := "arn:aws:sqs:us-east-1:123456789012:input-override-q"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sqsSink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "override-rule",
		EventPattern: `{"source":["override.test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "override-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: queueARN, Input: `{"static":"payload"}`},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "override.test", DetailType: "T", Detail: `{"dynamic":"field"}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsSink.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsSink.MessagesFor(queueARN)[0]
	assert.Contains(t, msg, "static")
	assert.NotContains(t, msg, "dynamic")
}

func TestTargets_InputPathExtractsField(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sqsSink := newMockSQSSender()
	queueARN := "arn:aws:sqs:us-east-1:123456789012:input-path-q"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sqsSink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "path-rule",
		EventPattern: `{"source":["path.test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "path-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: queueARN, InputPath: "$.source"},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "path.test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsSink.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsSink.MessagesFor(queueARN)[0]
	assert.Contains(t, msg, "path.test")
}

func TestTargets_InputTransformerApplied(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sqsSink := newMockSQSSender()
	queueARN := "arn:aws:sqs:us-east-1:123456789012:transform-q"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sqsSink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "transform-rule",
		EventPattern: `{"source":["transform.test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "transform-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: queueARN,
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{"src": "$.source"},
				InputTemplate: `{"event_source": "<src>"}`,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "transform.test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsSink.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsSink.MessagesFor(queueARN)[0]
	assert.Contains(t, msg, "event_source")
	assert.Contains(t, msg, "transform.test")
}

func TestTargets_InputTransformer_ArrayIndexing(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sqsSink := newMockSQSSender()
	queueARN := "arn:aws:sqs:us-east-1:123456789012:array-q"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sqsSink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "array-rule",
		EventPattern: `{"source":["array.test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "array-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: queueARN,
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"firstItem": "$.detail.items[0]",
				},
				InputTemplate: `{"item": "<firstItem>"}`,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "array.test", DetailType: "T", Detail: `{"items":["alpha","beta","gamma"]}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsSink.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsSink.MessagesFor(queueARN)[0]
	assert.Contains(t, msg, "alpha")
	assert.NotContains(t, msg, "beta")
}

func TestTargets_InputTransformer_NestedArrayIndex(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sqsSink := newMockSQSSender()
	queueARN := "arn:aws:sqs:us-east-1:123456789012:nested-array-q"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sqsSink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "nested-array-rule",
		EventPattern: `{"source":["nested.test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "nested-array-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: queueARN,
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"userId": "$.detail.users[1].id",
				},
				InputTemplate: `{"user_id": "<userId>"}`,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{
			Source:     "nested.test",
			DetailType: "T",
			Detail:     `{"users":[{"id":"u0"},{"id":"u1"},{"id":"u2"}]}`,
		},
	})

	require.Eventually(t, func() bool {
		return len(sqsSink.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsSink.MessagesFor(queueARN)[0]
	assert.Contains(t, msg, "u1")
}

func TestPutTargets_EnforcesLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		targetCount int
		wantErr     bool
	}{
		{name: "5 targets allowed", targetCount: 5, wantErr: false},
		{name: "6 targets rejected", targetCount: 6, wantErr: true},
		{name: "0 targets rejected", targetCount: 0, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.PutRule(
				context.Background(),
				eventbridge.PutRuleInput{Name: "r", ScheduleExpression: "rate(1 minute)"},
			)
			require.NoError(t, err)

			targets := make([]eventbridge.Target, tt.targetCount)
			for i := range targets {
				targets[i] = eventbridge.Target{
					ID:  "t" + string(rune('0'+i)),
					Arn: "arn:aws:sqs:us-east-1:123:q",
				}
			}

			_, err = b.PutTargets(context.Background(), "r", "", targets)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestPutTargets_TypeSpecificParametersRoundTrip proves that all
// of the target-type-specific parameter structs defined on aws-sdk-go-v2's
// eventbridge.types.Target (EcsParameters, HttpParameters, KinesisParameters,
// RedshiftDataParameters, RunCommandParameters, SageMakerPipelineParameters,
// SqsParameters, AppSyncParameters) round-trip through PutTargets and
// ListTargetsByRule. Before this fix, the gopherstack Target struct only
// modeled Input/InputPath/InputTransformer/DeadLetterConfig/RetryPolicy/
// BatchParameters: any of these other target-type parameters set by a
// client were silently dropped by json.Unmarshal (unknown fields are
// ignored), so a client configuring, say, an ECS target's
// NetworkConfiguration or task Tags would see them vanish on the very next
// DescribeRule/ListTargetsByRule call.
func TestPutTargets_TypeSpecificParametersRoundTrip(t *testing.T) {
	t.Parallel()

	newRule := func(t *testing.T, b *eventbridge.InMemoryBackend, name string) {
		t.Helper()
		_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
			Name:         name,
			EventPattern: `{"source":["x"]}`,
		})
		require.NoError(t, err)
	}

	target := eventbridge.Target{
		ID:  "t1",
		Arn: "arn:aws:ecs:us-east-1:123456789012:cluster/my-cluster",
		AppSyncParameters: &eventbridge.AppSyncParameters{
			GraphQLOperation: "mutation Publish { publish { id } }",
		},
		EcsParameters: &eventbridge.EcsParameters{
			TaskDefinitionArn: "arn:aws:ecs:us-east-1:123456789012:task-definition/my-task:1",
			LaunchType:        "FARGATE",
			NetworkConfiguration: &eventbridge.NetworkConfiguration{
				AwsvpcConfiguration: &eventbridge.AwsVpcConfiguration{
					Subnets:        []string{"subnet-1", "subnet-2"},
					SecurityGroups: []string{"sg-1"},
					AssignPublicIP: "ENABLED",
				},
			},
			CapacityProviderStrategy: []eventbridge.CapacityProviderStrategyItem{
				{CapacityProvider: "FARGATE_SPOT", Weight: 1},
			},
			PlacementConstraints: []eventbridge.PlacementConstraint{
				{Type: "distinctInstance"},
			},
			PlacementStrategy: []eventbridge.PlacementStrategy{
				{Field: "cpu", Type: "binpack"},
			},
			Tags: []eventbridge.EcsTag{{Key: "team", Value: "platform"}},
		},
		HTTPParameters: &eventbridge.HTTPParameters{
			HeaderParameters:      map[string]string{"X-Custom": "1"},
			PathParameterValues:   []string{"seg1"},
			QueryStringParameters: map[string]string{"q": "1"},
		},
		KinesisParameters: &eventbridge.KinesisParameters{
			PartitionKeyPath: "$.detail.id",
		},
		RedshiftDataParameters: &eventbridge.RedshiftDataParameters{
			Database: "mydb",
			DBUser:   "admin",
			SQL:      "select 1",
		},
		RunCommandParameters: &eventbridge.RunCommandParameters{
			RunCommandTargets: []eventbridge.RunCommandTarget{
				{Key: "tag:Name", Values: []string{"my-instance"}},
			},
		},
		SageMakerPipelineParameters: &eventbridge.SageMakerPipelineParameters{
			PipelineParameterList: []eventbridge.SageMakerPipelineParameter{
				{Name: "env", Value: "prod"},
			},
		},
		SqsParameters: &eventbridge.SqsParameters{MessageGroupID: "group-1"},
	}

	b := newBackend()
	newRule(t, b, "r")

	failed, err := b.PutTargets(context.Background(), "r", "", []eventbridge.Target{target})
	require.NoError(t, err)
	assert.Empty(t, failed)

	got, _, err := b.ListTargetsByRule(context.Background(), "r", "", "", 0)
	require.NoError(t, err)
	require.Len(t, got, 1)

	assert.Equal(t, target.AppSyncParameters, got[0].AppSyncParameters)
	assert.Equal(t, target.EcsParameters, got[0].EcsParameters)
	assert.Equal(t, target.HTTPParameters, got[0].HTTPParameters)
	assert.Equal(t, target.KinesisParameters, got[0].KinesisParameters)
	assert.Equal(t, target.RedshiftDataParameters, got[0].RedshiftDataParameters)
	assert.Equal(t, target.RunCommandParameters, got[0].RunCommandParameters)
	assert.Equal(t, target.SageMakerPipelineParameters, got[0].SageMakerPipelineParameters)
	assert.Equal(t, target.SqsParameters, got[0].SqsParameters)
}

// TestPutTargets_TypeSpecificParametersValidation proves PutTargets
// rejects target-type-specific parameter structs missing their AWS-required
// member, mirroring the client-side constraint validation aws-sdk-go-v2
// performs (validateEcsParameters, validateKinesisParameters,
// validateRedshiftDataParameters, validateRunCommandParameters/Target).
func TestPutTargets_TypeSpecificParametersValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target eventbridge.Target
	}{
		{
			name: "EcsParameters missing TaskDefinitionArn",
			target: eventbridge.Target{
				ID: "t1", Arn: "arn:aws:ecs:us-east-1:123456789012:cluster/c",
				EcsParameters: &eventbridge.EcsParameters{},
			},
		},
		{
			name: "KinesisParameters missing PartitionKeyPath",
			target: eventbridge.Target{
				ID: "t1", Arn: "arn:aws:kinesis:us-east-1:123456789012:stream/s",
				KinesisParameters: &eventbridge.KinesisParameters{},
			},
		},
		{
			name: "RedshiftDataParameters missing Database",
			target: eventbridge.Target{
				ID: "t1", Arn: "arn:aws:redshift:us-east-1:123456789012:cluster:c",
				RedshiftDataParameters: &eventbridge.RedshiftDataParameters{},
			},
		},
		{
			name: "RunCommandParameters missing RunCommandTargets",
			target: eventbridge.Target{
				ID: "t1", Arn: "arn:aws:ec2:us-east-1:123456789012:instance/i",
				RunCommandParameters: &eventbridge.RunCommandParameters{},
			},
		},
		{
			name: "RunCommandParameters target missing Values",
			target: eventbridge.Target{
				ID: "t1", Arn: "arn:aws:ec2:us-east-1:123456789012:instance/i",
				RunCommandParameters: &eventbridge.RunCommandParameters{
					RunCommandTargets: []eventbridge.RunCommandTarget{{Key: "tag:Name"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name: "r", EventPattern: `{"source":["x"]}`,
			})
			require.NoError(t, err)

			failed, err := b.PutTargets(context.Background(), "r", "", []eventbridge.Target{tt.target})
			require.NoError(t, err, "PutTargets itself must not error; failures are per-entry")
			require.Len(t, failed, 1)
			assert.Equal(t, "InvalidParameter", failed[0].ErrorCode)
			assert.NotEmpty(t, failed[0].ErrorMessage)

			got, _, err := b.ListTargetsByRule(context.Background(), "r", "", "", 0)
			require.NoError(t, err)
			assert.Empty(t, got, "the invalid target must not have been stored")
		})
	}
}

// TestListTargetsByRule_Limit asserts the Limit request parameter is threaded
// through to backend pagination for ListTargetsByRule.
func TestListTargetsByRule_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		limit      int
		numTargets int
		wantPage   int
		wantMore   bool
	}{
		{name: "limit_2_of_5", limit: 2, numTargets: 5, wantPage: 2, wantMore: true},
		{name: "zero_limit_uses_default", limit: 0, numTargets: 4, wantPage: 4, wantMore: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.PutRule(t.Context(), eventbridge.PutRuleInput{
				Name:         "target-limit-rule",
				EventPattern: `{"source":["x"]}`,
				State:        "ENABLED",
			})
			require.NoError(t, err)

			targets := make([]eventbridge.Target, 0, tt.numTargets)
			for i := range tt.numTargets {
				targets = append(targets, eventbridge.Target{
					ID:  fmt.Sprintf("t-%02d", i),
					Arn: fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:q-%02d", i),
				})
			}
			_, err = b.PutTargets(t.Context(), "target-limit-rule", "default", targets)
			require.NoError(t, err)

			got, next, err := b.ListTargetsByRule(t.Context(), "target-limit-rule", "default", "", tt.limit)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantPage)
			if tt.wantMore {
				assert.NotEmpty(t, next)
			} else {
				assert.Empty(t, next)
			}
		})
	}
}

// TestInputTransformer_ProducesValidJSON exercises the context-aware input
// transformer scanner. Placeholders in value position ({"k":<v>}) must gain
// JSON quoting/encoding so the result is valid JSON, while placeholders inside
// string literals ("<v>") must be spliced as escaped scalar content without
// doubling quotes.
func TestInputTransformer_ProducesValidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vars       map[string]any
		name       string
		template   string
		wantJSONEq string
		wantExact  string
		wantValid  bool
	}{
		{
			name:       "value_position_string_gets_quoted",
			template:   `{"k":<v>}`,
			vars:       map[string]any{"v": "hello"},
			wantJSONEq: `{"k":"hello"}`,
			wantValid:  true,
		},
		{
			name:       "value_position_number_stays_bare",
			template:   `{"n":<v>}`,
			vars:       map[string]any{"v": float64(42)},
			wantJSONEq: `{"n":42}`,
			wantValid:  true,
		},
		{
			name:       "value_position_object_spliced",
			template:   `{"detail":<d>}`,
			vars:       map[string]any{"d": map[string]any{"a": "b"}},
			wantJSONEq: `{"detail":{"a":"b"}}`,
			wantValid:  true,
		},
		{
			name:       "string_context_not_double_quoted",
			template:   `{"src":"<v>"}`,
			vars:       map[string]any{"v": "svc.name"},
			wantJSONEq: `{"src":"svc.name"}`,
			wantValid:  true,
		},
		{
			name:       "string_context_escapes_quotes",
			template:   `{"msg":"value is <v>"}`,
			vars:       map[string]any{"v": `a"b\c`},
			wantJSONEq: `{"msg":"value is a\"b\\c"}`,
			wantValid:  true,
		},
		{
			name:       "value_context_escapes_embedded_quotes",
			template:   `{"k":<v>}`,
			vars:       map[string]any{"v": `he said "hi"`},
			wantJSONEq: `{"k":"he said \"hi\""}`,
			wantValid:  true,
		},
		{
			name:       "missing_var_in_string_context_empty",
			template:   `{"k":"<missing>"}`,
			vars:       map[string]any{"missing": nil},
			wantJSONEq: `{"k":""}`,
			wantValid:  true,
		},
		{
			name:       "missing_var_in_value_context_empty_string",
			template:   `{"k":<missing>}`,
			vars:       map[string]any{"missing": nil},
			wantJSONEq: `{"k":""}`,
			wantValid:  true,
		},
		{
			name:      "unknown_placeholder_untouched",
			template:  `{"k":"<unknown>"}`,
			vars:      map[string]any{"known": "x"},
			wantExact: `{"k":"<unknown>"}`,
		},
		{
			name:      "plain_string_literal_template",
			template:  `"Event from <v>"`,
			vars:      map[string]any{"v": "text.service"},
			wantExact: `"Event from text.service"`,
			wantValid: true,
		},
		{
			name:       "no_html_escaping_of_angle_brackets",
			template:   `{"k":<v>}`,
			vars:       map[string]any{"v": "a<b>c&d"},
			wantJSONEq: `{"k":"a<b>c&d"}`,
			wantValid:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := eventbridge.SubstituteInputTemplateForTest(tt.template, tt.vars)

			if tt.wantExact != "" {
				assert.Equal(t, tt.wantExact, got)
			}
			if tt.wantJSONEq != "" {
				assert.JSONEq(t, tt.wantJSONEq, got)
			}
			if tt.wantValid {
				assert.True(t, json.Valid([]byte(got)), "expected valid JSON, got %q", got)
			}
		})
	}
}

// TestInputTransformer_ValueContextEndToEnd confirms the value-position fix
// flows through real delivery: an unquoted placeholder produces valid JSON in
// the delivered SQS message.
func TestInputTransformer_ValueContextEndToEnd(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	backend := setupDeliveryBackend(t, sqsMock, nil)
	queueARN := "arn:aws:sqs:us-east-1:000000000000:it-value-queue"

	_, err := backend.PutRule(t.Context(), eventbridge.PutRuleInput{
		Name:         "it-value-rule",
		EventPattern: `{"source":["it.svc"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	_, err = backend.PutTargets(t.Context(), "it-value-rule", "default", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: queueARN,
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{"src": "$.source"},
				// Unquoted placeholder in value position — the old code emitted
				// {"wrapped":it.svc} (invalid JSON); the fix quotes it.
				InputTemplate: `{"wrapped":<src>}`,
			},
		},
	})
	require.NoError(t, err)

	backend.PutEvents(t.Context(), []eventbridge.EventEntry{
		{Source: "it.svc", DetailType: "Evt", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsMock.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsMock.MessagesFor(queueARN)[0]
	assert.True(t, json.Valid([]byte(msg)), "delivered payload must be valid JSON: %q", msg)
	assert.JSONEq(t, `{"wrapped":"it.svc"}`, msg)
}

// TestManagedRuleException_RejectsTargetMutations verifies PutTargets and
// RemoveTargets reject mutations against a service-managed rule
// (Rule.ManagedBy non-empty) with ManagedRuleException, matching real AWS.
// ManagedBy is only reachable via the internal PutRuleInput.ManagedBy seeding
// path (see rules_test.go's TestPutRule_ManagedByNotWireSettable) -- no real
// AWS SDK client can set it directly.
func TestManagedRuleException_RejectsTargetMutations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		op   func(b *eventbridge.InMemoryBackend) ([]eventbridge.FailedEntry, error)
		name string
	}{
		{
			name: "PutTargets",
			op: func(b *eventbridge.InMemoryBackend) ([]eventbridge.FailedEntry, error) {
				return b.PutTargets(context.Background(), "managed-rule", "", []eventbridge.Target{
					{ID: "t1", Arn: "arn:aws:sqs:us-east-1:123456789012:q"},
				})
			},
		},
		{
			name: "RemoveTargets",
			op: func(b *eventbridge.InMemoryBackend) ([]eventbridge.FailedEntry, error) {
				return b.RemoveTargets(context.Background(), "managed-rule", "", []string{"t1"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name:         "managed-rule",
				EventPattern: `{"source":["x"]}`,
				ManagedBy:    "scheduler.amazonaws.com",
			})
			require.NoError(t, err)

			_, err = tt.op(b)
			require.ErrorIs(t, err, eventbridge.ErrManagedRule)
		})
	}
}
