package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_EventBridge_SFN_TargetReceipt creates a Step Functions state machine,
// wires it as an EventBridge rule target, puts a matching event, and verifies the
// target configuration is in place (receipt at the routing layer).
func TestIntegration_EventBridge_SFN_TargetReceipt(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	ebClient := createEventBridgeClient(t)
	sfnClient := createStepFunctionsClient(t)

	id := uuid.NewString()[:8]
	busName := "eb-sfn-bus-" + id
	ruleName := "eb-sfn-rule-" + id
	smName := "eb-sfn-sm-" + id
	roleARN := "arn:aws:iam::000000000000:role/sfn-role"
	smDefinition := `{"Comment":"EB receipt test","StartAt":"Pass","States":{"Pass":{"Type":"Pass","End":true}}}`

	// Create the SFN state machine.
	createSMOut, err := sfnClient.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(smDefinition),
		RoleArn:    aws.String(roleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err, "CreateStateMachine should succeed")
	smARN := aws.ToString(createSMOut.StateMachineArn)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = sfnClient.DeleteStateMachine(cleanupCtx, &sfnsdk.DeleteStateMachineInput{
			StateMachineArn: aws.String(smARN),
		})
	})

	// Create a custom event bus.
	_, err = ebClient.CreateEventBus(ctx, &eventbridgesdk.CreateEventBusInput{
		Name: aws.String(busName),
	})
	require.NoError(t, err, "CreateEventBus should succeed")
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = ebClient.DeleteEventBus(cleanupCtx, &eventbridgesdk.DeleteEventBusInput{
			Name: aws.String(busName),
		})
	})

	// Create a rule on the custom bus.
	_, err = ebClient.PutRule(ctx, &eventbridgesdk.PutRuleInput{
		Name:         aws.String(ruleName),
		EventBusName: aws.String(busName),
		EventPattern: aws.String(`{"source":["integration.sfn"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err, "PutRule should succeed")
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = ebClient.RemoveTargets(cleanupCtx, &eventbridgesdk.RemoveTargetsInput{
			Rule:         aws.String(ruleName),
			EventBusName: aws.String(busName),
			Ids:          []string{"sfn-target"},
		})
		_, _ = ebClient.DeleteRule(cleanupCtx, &eventbridgesdk.DeleteRuleInput{
			Name:         aws.String(ruleName),
			EventBusName: aws.String(busName),
		})
	})

	// Wire the SFN state machine as the target.
	targetsOut, err := ebClient.PutTargets(ctx, &eventbridgesdk.PutTargetsInput{
		Rule:         aws.String(ruleName),
		EventBusName: aws.String(busName),
		Targets: []ebtypes.Target{
			{
				Id:  aws.String("sfn-target"),
				Arn: aws.String(smARN),
			},
		},
	})
	require.NoError(t, err, "PutTargets should succeed")
	assert.Equal(t, int32(0), targetsOut.FailedEntryCount, "no target entries should fail")

	// Put a matching event.
	putOut, err := ebClient.PutEvents(ctx, &eventbridgesdk.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{
				Source:       aws.String("integration.sfn"),
				DetailType:   aws.String("OrderPlaced"),
				Detail:       aws.String(`{"orderId":"` + id + `"}`),
				EventBusName: aws.String(busName),
			},
		},
	})
	require.NoError(t, err, "PutEvents should succeed")
	assert.Equal(t, int32(0), putOut.FailedEntryCount, "no events should fail")

	// Verify the SFN target is registered on the rule.
	listOut, err := ebClient.ListTargetsByRule(ctx, &eventbridgesdk.ListTargetsByRuleInput{
		Rule:         aws.String(ruleName),
		EventBusName: aws.String(busName),
	})
	require.NoError(t, err, "ListTargetsByRule should succeed")
	require.Len(t, listOut.Targets, 1, "exactly one target should be registered")
	assert.Equal(t, smARN, aws.ToString(listOut.Targets[0].Arn),
		"target ARN should match the SFN state machine ARN")
}
