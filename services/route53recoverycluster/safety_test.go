package route53recoverycluster_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycluster"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycluster/types"
	rccfg "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	rctypes "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *fixture) atleastOne(t *testing.T, topo topology, inverted bool) string {
	t.Helper()

	out, err := f.ccClient.CreateSafetyRule(
		t.Context(),
		&rccfg.CreateSafetyRuleInput{AssertionRule: &rctypes.NewAssertionRule{
			AssertedControls: []string{topo.controlA, topo.controlB},
			ControlPanelArn:  aws.String(topo.panelArn),
			Name:             aws.String("keep-one"),
			WaitPeriodMs:     aws.Int32(0),
			RuleConfig: &rctypes.RuleConfig{
				Inverted:  aws.Bool(inverted),
				Threshold: aws.Int32(1),
				Type:      rctypes.RuleTypeAtleast,
			},
		}},
	)
	require.NoError(t, err)

	return aws.ToString(out.AssertionRule.SafetyRuleArn)
}

func set(arn string, s types.RoutingControlState) types.UpdateRoutingControlStateEntry {
	return types.UpdateRoutingControlStateEntry{RoutingControlArn: aws.String(arn), RoutingControlState: s}
}

func TestAssertionRuleBlocksBothOff(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	topo := f.build(t)
	ctx := t.Context()
	rule := f.atleastOne(t, topo, false)

	_, err := topo.client.UpdateRoutingControlStates(ctx, &route53recoverycluster.UpdateRoutingControlStatesInput{
		UpdateRoutingControlStateEntries: []types.UpdateRoutingControlStateEntry{
			set(topo.controlA, types.RoutingControlStateOn), set(topo.controlB, types.RoutingControlStateOn),
		},
	})
	require.NoError(t, err)

	_, err = topo.client.UpdateRoutingControlStates(ctx, &route53recoverycluster.UpdateRoutingControlStatesInput{
		UpdateRoutingControlStateEntries: []types.UpdateRoutingControlStateEntry{
			set(topo.controlA, types.RoutingControlStateOff), set(topo.controlB, types.RoutingControlStateOff),
		},
	})
	require.Error(t, err)
	assert.Equal(t, "ConflictException", apiCode(err))

	var conflict *types.ConflictException
	require.ErrorAs(t, err, &conflict)
	assert.Contains(t, aws.ToString(conflict.Message), "keep-one")
	assert.Contains(t, aws.ToString(conflict.Message), rule)
	assert.Equal(t, rule, aws.ToString(conflict.ResourceId))

	for _, arn := range []string{topo.controlA, topo.controlB} {
		got, gErr := topo.client.GetRoutingControlState(
			ctx,
			&route53recoverycluster.GetRoutingControlStateInput{RoutingControlArn: aws.String(arn)},
		)
		require.NoError(t, gErr)
		assert.Equal(t, types.RoutingControlStateOn, got.RoutingControlState, "a rejected batch changes nothing")
	}

	_, err = topo.client.UpdateRoutingControlStates(ctx, &route53recoverycluster.UpdateRoutingControlStatesInput{
		UpdateRoutingControlStateEntries: []types.UpdateRoutingControlStateEntry{
			set(topo.controlA, types.RoutingControlStateOff), set(topo.controlB, types.RoutingControlStateOff),
		},
		SafetyRulesToOverride: []string{rule},
	})
	require.NoError(t, err, "SafetyRulesToOverride lets the update through")

	got, err := topo.client.GetRoutingControlState(
		ctx,
		&route53recoverycluster.GetRoutingControlStateInput{RoutingControlArn: aws.String(topo.controlA)},
	)
	require.NoError(t, err)
	assert.Equal(t, types.RoutingControlStateOff, got.RoutingControlState)
}

func TestSingleUpdateEvaluatesRules(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	topo := f.build(t)
	ctx := t.Context()
	rule := f.atleastOne(t, topo, true)

	_, err := topo.client.UpdateRoutingControlState(ctx, &route53recoverycluster.UpdateRoutingControlStateInput{
		RoutingControlArn: aws.String(topo.controlA), RoutingControlState: types.RoutingControlStateOn,
	})
	assert.Equal(t, "ConflictException", apiCode(err), "an inverted ATLEAST-1 forbids any control being On")

	_, err = topo.client.UpdateRoutingControlState(ctx, &route53recoverycluster.UpdateRoutingControlStateInput{
		RoutingControlArn: aws.String(
			topo.controlA,
		),
		RoutingControlState:   types.RoutingControlStateOn,
		SafetyRulesToOverride: []string{rule},
	})
	require.NoError(t, err)
}

func TestEndpointIsScopedToItsCluster(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	topo := f.build(t)
	ctx := t.Context()

	other, err := f.ccClient.CreateCluster(ctx, &rccfg.CreateClusterInput{ClusterName: aws.String("other")})
	require.NoError(t, err)

	otherClient := f.dataPlane(aws.ToString(other.Cluster.ClusterEndpoints[0].Endpoint))

	_, err = otherClient.GetRoutingControlState(
		ctx,
		&route53recoverycluster.GetRoutingControlStateInput{RoutingControlArn: aws.String(topo.controlA)},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	list, err := otherClient.ListRoutingControls(ctx, &route53recoverycluster.ListRoutingControlsInput{})
	require.NoError(t, err)
	assert.Empty(t, list.RoutingControls)

	all, err := topo.client.ListRoutingControls(ctx, &route53recoverycluster.ListRoutingControlsInput{})
	require.NoError(t, err)
	assert.Len(t, all.RoutingControls, 2)
}

func TestDataPlaneErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	topo := f.build(t)
	ctx := t.Context()

	tests := []struct {
		call func() error
		name string
		want string
	}{
		{name: "unknown control", call: func() error {
			_, err := topo.client.GetRoutingControlState(
				ctx,
				&route53recoverycluster.GetRoutingControlStateInput{RoutingControlArn: aws.String(topo.controlA + "z")},
			)

			return err
		}, want: "ResourceNotFoundException"},
		{name: "invalid state", call: func() error {
			_, err := topo.client.UpdateRoutingControlState(ctx, &route53recoverycluster.UpdateRoutingControlStateInput{
				RoutingControlArn: aws.String(topo.controlA), RoutingControlState: "Maybe",
			})

			return err
		}, want: "ValidationException"},
		{name: "duplicate entries", call: func() error {
			_, err := topo.client.UpdateRoutingControlStates(
				ctx,
				&route53recoverycluster.UpdateRoutingControlStatesInput{
					UpdateRoutingControlStateEntries: []types.UpdateRoutingControlStateEntry{
						set(
							topo.controlA,
							types.RoutingControlStateOn,
						),
						set(topo.controlA, types.RoutingControlStateOff),
					},
				},
			)

			return err
		}, want: "ValidationException"},
		{name: "unknown override", call: func() error {
			_, err := topo.client.UpdateRoutingControlState(ctx, &route53recoverycluster.UpdateRoutingControlStateInput{
				RoutingControlArn: aws.String(
					topo.controlA,
				),
				RoutingControlState:   types.RoutingControlStateOn,
				SafetyRulesToOverride: []string{"arn:nope"},
			})

			return err
		}, want: "ValidationException"},
		{name: "unknown panel", call: func() error {
			_, err := topo.client.ListRoutingControls(
				ctx,
				&route53recoverycluster.ListRoutingControlsInput{ControlPanelArn: aws.String("arn:nope")},
			)

			return err
		}, want: "ResourceNotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, apiCode(tt.call()))
		})
	}
}

func TestListRoutingControlsPaginates(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	topo := f.build(t)

	p := route53recoverycluster.NewListRoutingControlsPaginator(
		topo.client,
		&route53recoverycluster.ListRoutingControlsInput{
			ControlPanelArn: aws.String(topo.panelArn), MaxResults: aws.Int32(1),
		},
	)

	pages := 0

	for p.HasMorePages() {
		pg, err := p.NextPage(t.Context())
		require.NoError(t, err)
		assert.Len(t, pg.RoutingControls, 1)

		pages++
	}

	assert.Equal(t, 2, pages)
}
