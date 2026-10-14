package route53recoverycontrolconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAssertion(topo topology, mutate func(*types.NewAssertionRule)) *sdk.CreateSafetyRuleInput {
	r := &types.NewAssertionRule{
		AssertedControls: []string{topo.ctlA, topo.ctlB}, ControlPanelArn: aws.String(topo.defaultPanel),
		Name: aws.String("keep-one"), WaitPeriodMs: aws.Int32(5000),
		RuleConfig: &types.RuleConfig{Inverted: aws.Bool(false), Threshold: aws.Int32(1), Type: types.RuleTypeAtleast},
	}
	if mutate != nil {
		mutate(r)
	}

	return &sdk.CreateSafetyRuleInput{AssertionRule: r}
}

func TestSafetyRuleCRUD(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	topo := f.topology(t)
	ctx := t.Context()

	created, err := f.client.CreateSafetyRule(ctx, newAssertion(topo, nil))
	require.NoError(t, err)
	require.NotNil(t, created.AssertionRule)
	assert.Nil(t, created.GatingRule)

	ar := created.AssertionRule
	assert.Equal(t, "keep-one", aws.ToString(ar.Name))
	assert.Equal(t, types.StatusDeployed, ar.Status)
	assert.Equal(t, []string{topo.ctlA, topo.ctlB}, ar.AssertedControls)
	assert.Equal(t, types.RuleTypeAtleast, ar.RuleConfig.Type)
	assert.EqualValues(t, 1, aws.ToInt32(ar.RuleConfig.Threshold))
	assert.False(t, aws.ToBool(ar.RuleConfig.Inverted))
	assert.EqualValues(t, 5000, aws.ToInt32(ar.WaitPeriodMs))
	assert.Equal(t, testAccount, aws.ToString(ar.Owner))
	assert.Regexp(t, `/safetyrule/[0-9a-f]{12}$`, aws.ToString(ar.SafetyRuleArn))

	gating, err := f.client.CreateSafetyRule(ctx, &sdk.CreateSafetyRuleInput{GatingRule: &types.NewGatingRule{
		ControlPanelArn: aws.String(
			topo.defaultPanel,
		),
		GatingControls: []string{topo.ctlA},
		TargetControls: []string{topo.ctlB},
		Name:           aws.String("gate"),
		WaitPeriodMs:   aws.Int32(0),
		RuleConfig:     &types.RuleConfig{Inverted: aws.Bool(true), Threshold: aws.Int32(1), Type: types.RuleTypeOr},
	}, Tags: map[string]string{"k": "v"}})
	require.NoError(t, err)
	require.NotNil(t, gating.GatingRule)
	assert.Equal(t, []string{topo.ctlA}, gating.GatingRule.GatingControls)
	assert.Equal(t, []string{topo.ctlB}, gating.GatingRule.TargetControls)
	assert.True(t, aws.ToBool(gating.GatingRule.RuleConfig.Inverted))

	list, err := f.client.ListSafetyRules(
		ctx,
		&sdk.ListSafetyRulesInput{ControlPanelArn: aws.String(topo.defaultPanel)},
	)
	require.NoError(t, err)
	require.Len(t, list.SafetyRules, 2)
	assert.Equal(t, "gate", aws.ToString(list.SafetyRules[0].GATING.Name))
	assert.Equal(t, "keep-one", aws.ToString(list.SafetyRules[1].ASSERTION.Name))

	desc, err := f.client.DescribeSafetyRule(ctx, &sdk.DescribeSafetyRuleInput{SafetyRuleArn: ar.SafetyRuleArn})
	require.NoError(t, err)
	assert.Equal(t, "keep-one", aws.ToString(desc.AssertionRule.Name))

	upd, err := f.client.UpdateSafetyRule(
		ctx,
		&sdk.UpdateSafetyRuleInput{AssertionRuleUpdate: &types.AssertionRuleUpdate{
			SafetyRuleArn: ar.SafetyRuleArn, Name: aws.String("renamed"), WaitPeriodMs: aws.Int32(100),
		}},
	)
	require.NoError(t, err)
	assert.Equal(t, "renamed", aws.ToString(upd.AssertionRule.Name))
	assert.EqualValues(t, 100, aws.ToInt32(upd.AssertionRule.WaitPeriodMs))

	_, err = f.client.UpdateSafetyRule(ctx, &sdk.UpdateSafetyRuleInput{GatingRuleUpdate: &types.GatingRuleUpdate{
		SafetyRuleArn: ar.SafetyRuleArn, Name: aws.String("x"), WaitPeriodMs: aws.Int32(1),
	}})
	assert.Equal(t, "ValidationException", apiCode(err), "an assertion rule cannot be updated as a gating rule")

	tags, err := f.client.ListTagsForResource(
		ctx,
		&sdk.ListTagsForResourceInput{ResourceArn: gating.GatingRule.SafetyRuleArn},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, tags.Tags)

	_, err = f.client.DeleteRoutingControl(
		ctx,
		&sdk.DeleteRoutingControlInput{RoutingControlArn: aws.String(topo.ctlA)},
	)
	assert.Equal(t, "ConflictException", apiCode(err), "a routing control used by a rule cannot be deleted")
	assert.Contains(t, apiMessage(err), "renamed")

	_, err = f.client.DeleteSafetyRule(ctx, &sdk.DeleteSafetyRuleInput{SafetyRuleArn: ar.SafetyRuleArn})
	require.NoError(t, err)
	_, err = f.client.DescribeSafetyRule(ctx, &sdk.DescribeSafetyRuleInput{SafetyRuleArn: ar.SafetyRuleArn})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteControlPanel(
		ctx,
		&sdk.DeleteControlPanelInput{ControlPanelArn: aws.String(topo.customPanel)},
	)
	assert.Equal(t, "ConflictException", apiCode(err))
}

func TestSafetyRuleValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(topology, *types.NewAssertionRule)
		name   string
	}{
		{
			name:   "threshold zero",
			mutate: func(_ topology, r *types.NewAssertionRule) { r.RuleConfig.Threshold = aws.Int32(0) },
		},
		{
			name:   "threshold above control count",
			mutate: func(_ topology, r *types.NewAssertionRule) { r.RuleConfig.Threshold = aws.Int32(3) },
		},
		{name: "unknown rule type", mutate: func(_ topology, r *types.NewAssertionRule) { r.RuleConfig.Type = "XOR" }},
		{
			name:   "negative wait period",
			mutate: func(_ topology, r *types.NewAssertionRule) { r.WaitPeriodMs = aws.Int32(-1) },
		},
		{
			name:   "empty controls",
			mutate: func(_ topology, r *types.NewAssertionRule) { r.AssertedControls = []string{} },
		},
		{name: "unknown control", mutate: func(_ topology, r *types.NewAssertionRule) {
			r.AssertedControls = []string{
				"arn:aws:route53-recovery-control::123456789012:controlpanel/x/routingcontrol/y",
			}
		}},
		{
			name:   "control from another panel",
			mutate: func(topo topology, r *types.NewAssertionRule) { r.AssertedControls = []string{topo.ctlC} },
		},
		{
			name:   "duplicate control",
			mutate: func(topo topology, r *types.NewAssertionRule) { r.AssertedControls = []string{topo.ctlA, topo.ctlA} },
		},
		{name: "empty name", mutate: func(_ topology, r *types.NewAssertionRule) { r.Name = aws.String("") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			topo := f.topology(t)

			_, err := f.client.CreateSafetyRule(
				t.Context(),
				newAssertion(topo, func(r *types.NewAssertionRule) { tt.mutate(topo, r) }),
			)
			assert.Equal(t, "ValidationException", apiCode(err))
		})
	}

	t.Run("both rule kinds", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		in := newAssertion(f.topology(t), nil)
		in.GatingRule = &types.NewGatingRule{
			ControlPanelArn: aws.String(
				"p",
			),
			GatingControls: []string{"a"},
			TargetControls: []string{"b"},
			Name:           aws.String("g"),
			WaitPeriodMs: aws.Int32(
				0,
			),
			RuleConfig: &types.RuleConfig{Inverted: aws.Bool(false), Threshold: aws.Int32(1), Type: types.RuleTypeOr},
		}
		_, err := f.client.CreateSafetyRule(t.Context(), in)
		assert.Equal(t, "ValidationException", apiCode(err))
	})

	t.Run("gating control also a target", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		topo := f.topology(t)
		_, err := f.client.CreateSafetyRule(t.Context(), &sdk.CreateSafetyRuleInput{GatingRule: &types.NewGatingRule{
			ControlPanelArn: aws.String(
				topo.defaultPanel,
			),
			GatingControls: []string{topo.ctlA},
			TargetControls: []string{topo.ctlA},
			Name:           aws.String("g"),
			WaitPeriodMs:   aws.Int32(0),
			RuleConfig: &types.RuleConfig{
				Inverted:  aws.Bool(false),
				Threshold: aws.Int32(1),
				Type:      types.RuleTypeOr,
			},
		}})
		assert.Equal(t, "ValidationException", apiCode(err))
	})
}
