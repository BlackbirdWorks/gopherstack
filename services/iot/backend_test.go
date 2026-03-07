package iot_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func TestBackend_CreateAndDescribeThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *iot.CreateThingInput
		name    string
		wantErr bool
	}{
		{
			name: "create_basic_thing",
			input: &iot.CreateThingInput{
				ThingName:     "sensor-1",
				ThingTypeName: "TemperatureSensor",
				AttributePayload: &iot.AttributePayload{
					Attributes: map[string]string{"location": "lab"},
				},
			},
		},
		{
			name: "create_thing_no_attributes",
			input: &iot.CreateThingInput{
				ThingName: "sensor-2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()

			out, err := b.CreateThing(tt.input)

			require.NoError(t, err)
			assert.Equal(t, tt.input.ThingName, out.ThingName)
			assert.NotEmpty(t, out.ThingARN)
			assert.NotEmpty(t, out.ThingID)

			described, dErr := b.DescribeThing(tt.input.ThingName)
			require.NoError(t, dErr)
			assert.Equal(t, tt.input.ThingName, described.ThingName)
		})
	}
}

func TestBackend_DescribeThing_NotFound(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.DescribeThing("nonexistent")
	require.ErrorIs(t, err, iot.ErrThingNotFound)
}

func TestBackend_DeleteThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*iot.InMemoryBackend)
		name      string
		thingName string
		wantErr   bool
	}{
		{
			name:      "delete_existing",
			thingName: "my-thing",
			setup: func(b *iot.InMemoryBackend) {
				_, _ = b.CreateThing(&iot.CreateThingInput{ThingName: "my-thing"})
			},
		},
		{
			name:      "delete_nonexistent",
			thingName: "missing",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()

			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.DeleteThing(tt.thingName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, descErr := b.DescribeThing(tt.thingName)
			require.ErrorIs(t, descErr, iot.ErrThingNotFound)
		})
	}
}

func TestBackend_TopicRuleLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *iot.CreateTopicRuleInput
		name    string
		wantErr bool
	}{
		{
			name: "create_rule",
			input: &iot.CreateTopicRuleInput{
				RuleName: "TemperatureRule",
				TopicRulePayload: &iot.TopicRulePayload{
					SQL:     "SELECT * FROM 'sensor/temperature' WHERE temperature > 50",
					Actions: []iot.RuleAction{{SQS: &iot.SQSAction{QueueURL: "http://localhost/queue"}}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()

			err := b.CreateTopicRule(tt.input)
			require.NoError(t, err)

			r, getErr := b.GetTopicRule(tt.input.RuleName)
			require.NoError(t, getErr)
			assert.Equal(t, tt.input.RuleName, r.RuleName)
			assert.Equal(t, tt.input.TopicRulePayload.SQL, r.SQL)
			assert.True(t, r.Enabled)

			rules := b.ListTopicRules()
			assert.Len(t, rules, 1)

			delErr := b.DeleteTopicRule(tt.input.RuleName)
			require.NoError(t, delErr)

			_, getErr2 := b.GetTopicRule(tt.input.RuleName)
			require.ErrorIs(t, getErr2, iot.ErrRuleNotFound)
		})
	}
}

func TestBackend_GetTopicRule_NotFound(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.GetTopicRule("missing")
	require.ErrorIs(t, err, iot.ErrRuleNotFound)
}

func TestBackend_PolicyLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *iot.CreatePolicyInput
		name  string
	}{
		{
			name: "create_policy",
			input: &iot.CreatePolicyInput{
				PolicyName:     "AllowAll",
				PolicyDocument: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()

			out, err := b.CreatePolicy(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.input.PolicyName, out.PolicyName)
			assert.NotEmpty(t, out.PolicyARN)
			assert.Equal(t, tt.input.PolicyDocument, out.PolicyDocument)

			attachErr := b.AttachPrincipalPolicy(&iot.AttachPrincipalPolicyInput{
				PolicyName: tt.input.PolicyName,
				Principal:  "arn:aws:iot:us-east-1:000000000000:cert/abc123",
			})
			require.NoError(t, attachErr)
		})
	}
}

func TestBackend_DescribeEndpoint(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackendWithConfig("123456789012", "eu-west-1")

	tests := []struct {
		name         string
		endpointType string
	}{
		{name: "data_ats", endpointType: "iot:Data-ATS"},
		{name: "data", endpointType: "iot:Data"},
		{name: "empty", endpointType: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.DescribeEndpoint(tt.endpointType)
			require.NoError(t, err)
			assert.NotEmpty(t, out.EndpointAddress)
		})
	}
}

func TestBackend_GetRules(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	_ = b.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName:         "RuleA",
		TopicRulePayload: &iot.TopicRulePayload{SQL: "SELECT * FROM 'a/#'"},
	})
	_ = b.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName:         "RuleB",
		TopicRulePayload: &iot.TopicRulePayload{SQL: "SELECT * FROM 'b/#'"},
	})

	rules := b.GetRules()
	assert.Len(t, rules, 2)
}

func TestBackend_SetRuleDispatcher(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	assert.Nil(t, b.GetDispatcher())

	d := &mockDispatcher{}
	b.SetRuleDispatcher(d)
	assert.Equal(t, d, b.GetDispatcher())
}

// mockDispatcher is a test implementation of RuleDispatcher.
type mockDispatcher struct{}

func (m *mockDispatcher) SendToSQS(_, _ string) error { return nil }

func (m *mockDispatcher) InvokeLambda(_ context.Context, _ string, _ []byte) error {
	return nil
}
