package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_EventBridge_EventBusAndRule(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createEventBridgeClient(t)
	ctx := t.Context()

	busName := "test-bus-" + uuid.NewString()[:8]
	ruleName := "test-rule-" + uuid.NewString()[:8]

	// CreateEventBus
	busOut, err := client.CreateEventBus(ctx, &eventbridgesdk.CreateEventBusInput{Name: aws.String(busName)})
	require.NoError(t, err)
	assert.NotEmpty(t, busOut.EventBusArn)

	// PutRule
	ruleOut, err := client.PutRule(ctx, &eventbridgesdk.PutRuleInput{
		Name:         aws.String(ruleName),
		EventBusName: aws.String(busName),
		EventPattern: aws.String(`{"source":["test"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, ruleOut.RuleArn)

	// ListRules
	listOut, err := client.ListRules(ctx, &eventbridgesdk.ListRulesInput{EventBusName: aws.String(busName)})
	require.NoError(t, err)
	assert.NotEmpty(t, listOut.Rules)

	// DescribeRule
	descOut, err := client.DescribeRule(ctx, &eventbridgesdk.DescribeRuleInput{
		Name:         aws.String(ruleName),
		EventBusName: aws.String(busName),
	})
	require.NoError(t, err)
	assert.Equal(t, ruleName, *descOut.Name)

	// DeleteRule
	_, err = client.DeleteRule(ctx, &eventbridgesdk.DeleteRuleInput{
		Name:         aws.String(ruleName),
		EventBusName: aws.String(busName),
	})
	require.NoError(t, err)

	// DeleteEventBus
	_, err = client.DeleteEventBus(ctx, &eventbridgesdk.DeleteEventBusInput{Name: aws.String(busName)})
	require.NoError(t, err)
}

func TestIntegration_EventBridge_PutEvents(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createEventBridgeClient(t)
	ctx := t.Context()

	// PutEvents to default bus
	putOut, err := client.PutEvents(ctx, &eventbridgesdk.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{
				Source:     aws.String("test.source"),
				DetailType: aws.String("TestEvent"),
				Detail:     aws.String(`{"key":"value"}`),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), putOut.FailedEntryCount)
}
