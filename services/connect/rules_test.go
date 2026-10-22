package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func eventAction() types.RuleAction {
	return types.RuleAction{
		ActionType:        types.ActionTypeGenerateEventbridgeEvent,
		EventBridgeAction: &types.EventBridgeActionDefinition{Name: aws.String("evt")},
	}
}

func TestRuleCreateValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "rulevalid"))

	tests := []struct {
		mutate func(in *sdk.CreateRuleInput)
		name   string
		code   string
	}{
		{
			name:   "bad publish status",
			mutate: func(in *sdk.CreateRuleInput) { in.PublishStatus = "LIVE" },
			code:   "InvalidParameterException",
		},
		{name: "bad event source", mutate: func(in *sdk.CreateRuleInput) {
			in.TriggerEventSource.EventSourceName = "OnNothing"
		}, code: "InvalidParameterException"},
		{
			name:   "no actions",
			mutate: func(in *sdk.CreateRuleInput) { in.Actions = []types.RuleAction{} },
			code:   "InvalidParameterException",
		},
		{name: "action without definition", mutate: func(in *sdk.CreateRuleInput) {
			in.Actions = []types.RuleAction{{ActionType: types.ActionTypeCreateTask}}
		}, code: "InvalidParameterException"},
		{name: "task flow missing", mutate: func(in *sdk.CreateRuleInput) {
			in.Actions = []types.RuleAction{{
				ActionType: types.ActionTypeCreateTask,
				TaskAction: &types.TaskActionDefinition{ContactFlowId: aws.String("nope"), Name: aws.String("t")},
			}}
		}, code: "ResourceNotFoundException"},
		{name: "zendesk needs association", mutate: func(in *sdk.CreateRuleInput) {
			in.TriggerEventSource.EventSourceName = types.EventSourceNameOnZendeskTicketCreate
		}, code: "InvalidParameterException"},
		{name: "unknown association", mutate: func(in *sdk.CreateRuleInput) {
			in.TriggerEventSource.IntegrationAssociationId = aws.String("gone")
		}, code: "ResourceNotFoundException"},
		{
			name:   "unknown instance",
			mutate: func(in *sdk.CreateRuleInput) { in.InstanceId = aws.String("missing") },
			code:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.CreateRuleInput{
				InstanceId: inst, Name: aws.String("r-" + tt.name), Function: aws.String(`{"fn":1}`),
				PublishStatus: types.RulePublishStatusPublished,
				TriggerEventSource: &types.RuleTriggerEventSource{
					EventSourceName: types.EventSourceNameOnPostCallAnalysisAvailable,
				},
				Actions: []types.RuleAction{eventAction()},
			}
			tt.mutate(in)
			_, err := f.client.CreateRule(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestRuleLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "rules"))
	flow := f.newFlow(t, aws.ToString(inst), "taskflow")

	create := func(name string, src types.EventSourceName, status types.RulePublishStatus) *sdk.CreateRuleOutput {
		out, err := f.client.CreateRule(t.Context(), &sdk.CreateRuleInput{
			InstanceId: inst, Name: aws.String(name), Function: aws.String(`{"fn":1}`), PublishStatus: status,
			TriggerEventSource: &types.RuleTriggerEventSource{EventSourceName: src},
			Actions: []types.RuleAction{
				eventAction(),
				{
					ActionType: types.ActionTypeCreateTask,
					TaskAction: &types.TaskActionDefinition{ContactFlowId: aws.String(flow), Name: aws.String("t")},
				},
			},
			Tags: map[string]string{"k": "v"},
		})
		require.NoError(t, err)

		return out
	}

	r1 := create("one", types.EventSourceNameOnPostCallAnalysisAvailable, types.RulePublishStatusPublished)
	create("two", types.EventSourceNameOnPostChatAnalysisAvailable, types.RulePublishStatusDraft)

	_, err := f.client.CreateRule(t.Context(), &sdk.CreateRuleInput{
		InstanceId:    inst,
		Name:          aws.String("one"),
		Function:      aws.String("{}"),
		PublishStatus: types.RulePublishStatusDraft,
		TriggerEventSource: &types.RuleTriggerEventSource{
			EventSourceName: types.EventSourceNameOnPostCallAnalysisAvailable,
		},
		Actions: []types.RuleAction{eventAction()},
	})
	assert.Equal(t, "ResourceConflictException", apiCode(err))

	got, err := f.client.DescribeRule(t.Context(), &sdk.DescribeRuleInput{InstanceId: inst, RuleId: r1.RuleId})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(r1.RuleArn), aws.ToString(got.Rule.RuleArn))
	assert.Len(t, got.Rule.Actions, 2)
	assert.Equal(t, map[string]string{"k": "v"}, got.Rule.Tags)
	assert.NotNil(t, got.Rule.CreatedTime)

	tags, err := f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: r1.RuleArn})
	require.NoError(t, err)
	assert.Equal(t, "v", tags.Tags["k"])

	listTests := []struct {
		name   string
		in     sdk.ListRulesInput
		wanted int
	}{
		{name: "all", in: sdk.ListRulesInput{}, wanted: 2},
		{name: "published", in: sdk.ListRulesInput{PublishStatus: types.RulePublishStatusPublished}, wanted: 1},
		{
			name:   "source",
			in:     sdk.ListRulesInput{EventSourceName: types.EventSourceNameOnPostChatAnalysisAvailable},
			wanted: 1,
		},
	}
	for _, tt := range listTests {
		in := tt.in
		in.InstanceId = inst
		out, lerr := f.client.ListRules(t.Context(), &in)
		require.NoError(t, lerr, tt.name)
		assert.Len(t, out.RuleSummaryList, tt.wanted, tt.name)
	}

	page, err := f.client.ListRules(t.Context(), &sdk.ListRulesInput{InstanceId: inst, MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page.RuleSummaryList, 1)
	assert.NotEmpty(t, aws.ToString(page.NextToken))
	assert.Len(t, page.RuleSummaryList[0].ActionSummaries, 2)

	_, err = f.client.UpdateRule(t.Context(), &sdk.UpdateRuleInput{
		InstanceId: inst, RuleId: r1.RuleId, Name: aws.String("renamed"), Function: aws.String("{}"),
		PublishStatus: types.RulePublishStatusDraft, Actions: []types.RuleAction{eventAction()},
	})
	require.NoError(t, err)

	dst := f.restored(t)
	got, err = dst.client.DescribeRule(t.Context(), &sdk.DescribeRuleInput{InstanceId: inst, RuleId: r1.RuleId})
	require.NoError(t, err)
	assert.Equal(t, "renamed", aws.ToString(got.Rule.Name))
	assert.Len(t, got.Rule.Actions, 1)

	_, err = f.client.DeleteRule(t.Context(), &sdk.DeleteRuleInput{InstanceId: inst, RuleId: r1.RuleId})
	require.NoError(t, err)

	_, err = f.client.DescribeRule(t.Context(), &sdk.DescribeRuleInput{InstanceId: inst, RuleId: r1.RuleId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
	_, err = f.client.DeleteRule(t.Context(), &sdk.DeleteRuleInput{InstanceId: inst, RuleId: r1.RuleId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
