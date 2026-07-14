package iot_test

import (
	"context"
	"encoding/json"
	"strings"
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

// TestRefinement1_Reset verifies that Reset clears all backend state.
func TestReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		populate func(b *iot.InMemoryBackend)
		name     string
	}{
		{name: "empty_backend"},
		{
			name: "populated_backend",
			populate: func(b *iot.InMemoryBackend) {
				b.AddThingInternal(iot.Thing{ThingName: "t1"})
				b.AddPolicyInternal(iot.Policy{PolicyName: "p1"})
				b.AddRuleInternal(iot.TopicRule{RuleName: "r1"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			if tt.populate != nil {
				tt.populate(b)
			}

			b.Reset()

			assert.Equal(t, 0, b.ThingCount())
			assert.Equal(t, 0, b.PolicyCount())
			assert.Equal(t, 0, b.RuleCount())
		})
	}
}

// TestRefinement1_MultipleResetCycle verifies Reset can be called multiple times.
func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cycles int
	}{
		{name: "single_reset", cycles: 1},
		{name: "triple_reset", cycles: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			b.AddThingInternal(iot.Thing{ThingName: "t1"})

			for range tt.cycles {
				b.Reset()
			}

			assert.Equal(t, 0, b.ThingCount())
		})
	}
}

// TestRefinement1_HandlerReset verifies Handler.Reset delegates to the backend.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_backend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefHandler()
			b.AddThingInternal(iot.Thing{ThingName: "thing-before-reset"})
			assert.Equal(t, 1, b.ThingCount())

			h.Reset()

			assert.Equal(t, 0, b.ThingCount())
		})
	}
}

// TestRefinement1_GetSupportedOperations_AllOps verifies the supported operation list.
func TestGetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantOp  string
		wantMin int
	}{
		{name: "has_create_thing", wantOp: "CreateThing"},
		{name: "has_attach_policy", wantOp: "AttachPolicy"},
		{name: "has_cancel_audit_task", wantOp: "CancelAuditTask"},
		{name: "has_at_least_19_ops", wantMin: 19},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			ops := h.GetSupportedOperations()

			if tt.wantOp != "" {
				assert.Contains(t, ops, tt.wantOp)
			}

			if tt.wantMin > 0 {
				assert.GreaterOrEqual(t, len(ops), tt.wantMin)
			}
		})
	}
}

// TestRefinement1_HandlerOpsLen verifies the HandlerOpsLen export helper.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantMin int
	}{
		{name: "ops_len_at_least_19", wantMin: 19},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			assert.GreaterOrEqual(t, iot.HandlerOpsLen(h), tt.wantMin)
		})
	}
}

// TestRefinement1_SeedHelpers verifies AddThingInternal / AddPolicyInternal / AddRuleInternal.
func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		thingName    string
		policyName   string
		ruleName     string
		wantThings   int
		wantPolicies int
		wantRules    int
	}{
		{
			name:      "seed_one_of_each",
			thingName: "t1", policyName: "p1", ruleName: "r1",
			wantThings: 1, wantPolicies: 1, wantRules: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			b.AddThingInternal(iot.Thing{ThingName: tt.thingName})
			b.AddPolicyInternal(iot.Policy{PolicyName: tt.policyName})
			b.AddRuleInternal(iot.TopicRule{RuleName: tt.ruleName})

			assert.Equal(t, tt.wantThings, b.ThingCount())
			assert.Equal(t, tt.wantPolicies, b.PolicyCount())
			assert.Equal(t, tt.wantRules, b.RuleCount())
		})
	}
}

// TestRefinement1_SeedHelper_ARN verifies seed helpers generate ARNs automatically.
func TestSeedHelper_ARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "thing_arn_auto_generated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			b.AddThingInternal(iot.Thing{ThingName: "auto-arn-thing"})

			thing, err := b.DescribeThing("auto-arn-thing")
			require.NoError(t, err)
			assert.Contains(t, thing.ARN, "arn:aws:iot:")
			assert.Contains(t, thing.ARN, "auto-arn-thing")
		})
	}
}

// TestRefinement1_SortedListThings verifies ListThings returns items sorted by name.
func TestSortedListThings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedNames []string
		wantOrder []string
	}{
		{
			name:      "three_things_sorted",
			seedNames: []string{"zebra", "alpha", "mango"},
			wantOrder: []string{"alpha", "mango", "zebra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			for _, n := range tt.seedNames {
				b.AddThingInternal(iot.Thing{ThingName: n})
			}

			things := b.ListThings()
			require.Len(t, things, len(tt.wantOrder))

			for i, want := range tt.wantOrder {
				assert.Equal(t, want, things[i].ThingName)
			}
		})
	}
}

// TestRefinement1_DeepCopy_DescribeThing verifies mutations do not affect backend state.
func TestDeepCopy_DescribeThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutate_copy_does_not_affect_backend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			b.AddThingInternal(iot.Thing{
				ThingName:  "sensor",
				Attributes: map[string]string{"env": "prod"},
			})

			t1, err := b.DescribeThing("sensor")
			require.NoError(t, err)

			// Mutate the returned copy.
			t1.Attributes["env"] = "mutated"

			// Fetch again – original should be unchanged.
			t2, err := b.DescribeThing("sensor")
			require.NoError(t, err)
			assert.Equal(t, "prod", t2.Attributes["env"])
		})
	}
}

// TestRefinement1_ThingID_StoredAndReturned verifies ThingID is stored and returned.
func TestThingID_StoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "thingid_roundtrip"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()

			out, err := b.CreateThing(&iot.CreateThingInput{ThingName: "id-test"})
			require.NoError(t, err)
			assert.NotEmpty(t, out.ThingID)

			thing, err := b.DescribeThing("id-test")
			require.NoError(t, err)
			assert.Equal(t, out.ThingID, thing.ThingID)
		})
	}
}

// TestRefinement1_NonNilAttributes verifies Thing.Attributes is never nil.
func TestNonNilAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "no_attributes_payload_gives_empty_map"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()

			_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "no-attrs"})
			require.NoError(t, err)

			thing, err := b.DescribeThing("no-attrs")
			require.NoError(t, err)
			assert.NotNil(t, thing.Attributes)
		})
	}
}

// TestRefinement1_NonNilActions verifies TopicRule.Actions is never nil.
func TestNonNilActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "no_actions_gives_empty_slice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()

			err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
				RuleName:         "NoActionsRule",
				TopicRulePayload: &iot.TopicRulePayload{SQL: "SELECT temperature FROM 'devices/#'"},
			})
			require.NoError(t, err)

			rule, err := b.GetTopicRule("NoActionsRule")
			require.NoError(t, err)
			assert.NotNil(t, rule.Actions)
		})
	}
}

// TestRefinement1_CertTransferCount verifies AcceptCertificateTransfer tracking.
func TestCertTransferCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		certs []string
		want  int
	}{
		{name: "two_certs", certs: []string{"cert1", "cert2"}, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			for _, c := range tt.certs {
				require.NoError(t, b.AcceptCertificateTransfer(&iot.AcceptCertificateTransferInput{CertificateID: c}))
			}

			assert.Equal(t, tt.want, b.CertTransferCount())
		})
	}
}

// TestRefinement1_CreateThing_Validation verifies empty ThingName is rejected at backend.
func TestCreateThing_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		thingName string
	}{
		{name: "empty_name", thingName: "", wantErr: iot.ErrValidation},
		{name: "non_empty_name", thingName: "valid", wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			_, err := b.CreateThing(&iot.CreateThingInput{ThingName: tt.thingName})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRefinement1_ChaosMetadata verifies chaos-related metadata methods.
func TestChaosMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "chaos_service_name_is_iot"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			assert.Equal(t, "iot", h.ChaosServiceName())
			assert.NotEmpty(t, h.ChaosOperations())
			assert.NotEmpty(t, h.ChaosRegions())
		})
	}
}

// TestRefinement1_MQTTPort verifies the MQTT port is returned.
func TestMQTTPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantPort int
	}{
		{name: "default_mqtt_port", wantPort: 1883},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			assert.Equal(t, tt.wantPort, b.MQTTPort())
		})
	}
}

// TestRefinement1_ThingKey_ARNFallback verifies thingKey uses ARN when name is empty.
func TestThingKey_ARNFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "billing_group_with_arn_only"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			// Pass ARN only, no thing name.
			err := b.AddThingToBillingGroup(&iot.AddThingToBillingGroupInput{
				BillingGroupName: "bg1",
				ThingArn:         "arn:aws:iot:us-east-1:123456789012:thing/my-thing",
			})
			require.NoError(t, err)
		})
	}
}

func TestThing_VersionStartsAt1(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.CreateThing(&iot.CreateThingInput{ThingName: "v1-thing"})
	require.NoError(t, err)

	th, err := backend.DescribeThing("v1-thing")
	require.NoError(t, err)
	assert.Equal(t, int64(1), th.Version)
}

func TestUpdateThing_IncrementsVersion(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.CreateThing(&iot.CreateThingInput{ThingName: "update-thing"})
	require.NoError(t, err)

	err = backend.UpdateThing(&iot.UpdateThingInput{
		ThingName: "update-thing",
		AttributePayload: &iot.AttributePayload{
			Attributes: map[string]string{"env": "prod"},
		},
	})
	require.NoError(t, err)

	th, err := backend.DescribeThing("update-thing")
	require.NoError(t, err)
	assert.Equal(t, int64(2), th.Version)
	assert.Equal(t, "prod", th.Attributes["env"])
}

func TestUpdateThing_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.UpdateThing(&iot.UpdateThingInput{ThingName: "nonexistent"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrThingNotFound)
}

func TestListThingPrincipals(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddThingInternal(iot.Thing{ThingName: "principal-thing"})

	require.NoError(t, backend.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
		ThingName: "principal-thing",
		Principal: "arn:aws:iot:us-east-1:000000000000:cert/abc123",
	}))

	principals, err := backend.ListThingPrincipals("principal-thing")
	require.NoError(t, err)
	require.Len(t, principals, 1)
	assert.Equal(t, "arn:aws:iot:us-east-1:000000000000:cert/abc123", principals[0])
}

func TestListThingPrincipals_EmptyOnNoAttachment(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddThingInternal(iot.Thing{ThingName: "empty-thing"})

	principals, err := backend.ListThingPrincipals("empty-thing")
	require.NoError(t, err)
	assert.NotNil(t, principals)
	assert.Empty(t, principals)
}

func TestListThingPrincipals_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	_, err := backend.ListThingPrincipals("ghost-thing")
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrThingNotFound)
}

func TestThingPrincipalCount_Helper(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddThingInternal(iot.Thing{ThingName: "count-thing"})
	assert.Equal(t, 0, backend.ThingPrincipalCount("count-thing"))

	require.NoError(t, backend.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
		ThingName: "count-thing",
		Principal: "cert-1",
	}))
	assert.Equal(t, 1, backend.ThingPrincipalCount("count-thing"))
}

func TestGetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()

	expected := []string{
		"GetPolicy", "DeletePolicy", "ListPolicies",
		"DisableTopicRule", "EnableTopicRule", "ReplaceTopicRule",
		"UpdateThing", "ListThings", "ListTopicRules", "ListThingPrincipals",
	}

	for _, exp := range expected {
		assert.Contains(t, ops, exp, "missing operation: %s", exp)
	}
}

func TestUpdateThing_ExpectedVersionMatch(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "ver-thing"})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "ver-thing",
		ExpectedVersion: 1,
		AttributePayload: &iot.AttributePayload{
			Attributes: map[string]string{"env": "test"},
		},
	})
	require.NoError(t, err)

	t2, err := b.DescribeThing("ver-thing")
	require.NoError(t, err)
	assert.Equal(t, int64(2), t2.Version)
}

func TestUpdateThing_ExpectedVersionMismatch_ReturnsVersionConflict(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "conflict-thing"})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "conflict-thing",
		ExpectedVersion: 99,
	})
	require.ErrorIs(t, err, iot.ErrVersionConflict)
}

func TestUpdateThing_ZeroExpectedVersion_Ignored(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "nocheck-thing"})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "nocheck-thing",
		ExpectedVersion: 0,
	})
	require.NoError(t, err)
}

func TestUpdateThing_VersionConflict_AfterSuccessfulUpdate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "seq-ver-thing"})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "seq-ver-thing",
		ExpectedVersion: 1,
	})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{
		ThingName:       "seq-ver-thing",
		ExpectedVersion: 1,
	})
	require.ErrorIs(t, err, iot.ErrVersionConflict)
}

func TestUpdateThing_EmptyPayload_IncrementsVersion(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "empty-update-thing"})
	require.NoError(t, err)

	err = b.UpdateThing(&iot.UpdateThingInput{ThingName: "empty-update-thing"})
	require.NoError(t, err)

	th, err := b.DescribeThing("empty-update-thing")
	require.NoError(t, err)
	assert.Equal(t, int64(2), th.Version)
}

func TestCreateThing_DuplicateName_Conflict(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "dup-thing"})
	require.NoError(t, err)

	_, err = b.CreateThing(&iot.CreateThingInput{ThingName: "dup-thing"})
	require.ErrorIs(t, err, iot.ErrAlreadyExists)
}

func TestDeleteThing_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.DeleteThing("ghost-thing")
	require.ErrorIs(t, err, iot.ErrThingNotFound)
}

func TestUpdateThing_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.UpdateThing(&iot.UpdateThingInput{ThingName: "ghost-thing"})
	require.ErrorIs(t, err, iot.ErrThingNotFound)
}

func TestListThings_SortedByName(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	names := []string{"zebra", "alpha", "mango"}
	for _, n := range names {
		_, err := b.CreateThing(&iot.CreateThingInput{ThingName: n})
		require.NoError(t, err)
	}

	things := b.ListThings()
	require.Len(t, things, 3)
	assert.Equal(t, "alpha", things[0].ThingName)
	assert.Equal(t, "mango", things[1].ThingName)
	assert.Equal(t, "zebra", things[2].ThingName)
}

func TestDescribeThing_ReturnsThingID(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	out, err := b.CreateThing(&iot.CreateThingInput{ThingName: "id-thing"})
	require.NoError(t, err)

	th, err := b.DescribeThing("id-thing")
	require.NoError(t, err)
	assert.Equal(t, out.ThingID, th.ThingID)
	assert.NotEmpty(t, th.ThingID)
}

func TestThing_ARNFormat(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackendWithConfig("999988887777", "ap-southeast-1")
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "arn-test-thing"})
	require.NoError(t, err)

	th, err := b.DescribeThing("arn-test-thing")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(th.ARN, "arn:aws:iot:ap-southeast-1:999988887777:thing/"),
		"thing ARN should contain region+account, got: %s", th.ARN)
}

func TestReset_MultipleTimesIsIdempotent(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "multi-reset-thing"})
	require.NoError(t, err)

	b.Reset()
	b.Reset()
	b.Reset()

	assert.Empty(t, b.ListThings())
}

func TestDescribeEndpoint_ReturnsAddress(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	out, err := b.DescribeEndpoint("")
	require.NoError(t, err)
	assert.NotEmpty(t, out.EndpointAddress)
}

func TestAttachThingPrincipal_Stored(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "principal-thing"})
	require.NoError(t, err)

	err = b.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
		ThingName: "principal-thing",
		Principal: "arn:aws:iot:us-east-1:123456789012:cert/" + strings.Repeat("a", 64),
	})
	require.NoError(t, err)

	principals, err := b.ListThingPrincipals("principal-thing")
	require.NoError(t, err)
	require.Len(t, principals, 1)
}

func TestListThingPrincipals_ThingNotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.ListThingPrincipals("ghost-thing")
	require.ErrorIs(t, err, iot.ErrThingNotFound)
}

// TestParityB_UpdateThing_MergeFalse_ReplacesAttributes verifies merge:false replaces attributes.
func TestUpdateThing_MergeFalse_ReplacesAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialAttr map[string]string
		updateBody  map[string]any
		wantAttrs   map[string]any
		name        string
	}{
		{
			name:        "merge_true_merges",
			initialAttr: map[string]string{"a": "1", "b": "2"},
			updateBody: map[string]any{
				"attributePayload": map[string]any{
					"attributes": map[string]string{"c": "3"},
					"merge":      true,
				},
			},
			wantAttrs: map[string]any{"a": "1", "b": "2", "c": "3"},
		},
		{
			name:        "merge_false_replaces",
			initialAttr: map[string]string{"a": "1", "b": "2"},
			updateBody: map[string]any{
				"attributePayload": map[string]any{
					"attributes": map[string]string{"c": "3"},
					"merge":      false,
				},
			},
			wantAttrs: map[string]any{"c": "3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			_, err := b.CreateThing(&iot.CreateThingInput{
				ThingName: "merge-test",
				AttributePayload: &iot.AttributePayload{
					Attributes: tt.initialAttr,
				},
			})
			require.NoError(t, err)

			var bodyAttr iot.AttributePayload
			raw, _ := json.Marshal(tt.updateBody["attributePayload"])
			require.NoError(t, json.Unmarshal(raw, &bodyAttr))

			err = b.UpdateThing(&iot.UpdateThingInput{
				ThingName:        "merge-test",
				AttributePayload: &bodyAttr,
			})
			require.NoError(t, err)

			thing, err := b.DescribeThing("merge-test")
			require.NoError(t, err)

			got := make(map[string]any, len(thing.Attributes))
			for k, v := range thing.Attributes {
				got[k] = v
			}

			assert.Equal(t, tt.wantAttrs, got, "attributes mismatch after update")
		})
	}
}

// TestParityB_DeleteThing_WithPrincipals_Blocked verifies things with attached principals
// cannot be deleted.
func TestDeleteThing_WithPrincipals_Blocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantDeleteErr error
		name          string
		attachPrinc   bool
	}{
		{
			name:          "with_principal_blocked",
			attachPrinc:   true,
			wantDeleteErr: iot.ErrDeleteConflict,
		},
		{
			name:          "no_principal_allowed",
			attachPrinc:   false,
			wantDeleteErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "del-thing-" + tt.name})
			require.NoError(t, err)

			if tt.attachPrinc {
				err = b.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
					ThingName: "del-thing-" + tt.name,
					Principal: "arn:aws:iot:us-east-1:000000000000:cert/abc123",
				})
				require.NoError(t, err)
			}

			err = b.DeleteThing("del-thing-" + tt.name)
			if tt.wantDeleteErr != nil {
				require.ErrorIs(t, err, tt.wantDeleteErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
