package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

func TestInitializeServices_ConnectTagsWiring(t *testing.T) {
	t.Parallel()

	byName, rgtH := newRGTWiringEnv(t)

	h, ok := byName["Connect"].(*connectbackend.Handler)
	require.True(t, ok)

	ctx := t.Context()
	tags := map[string]string{"Team": "emu"}
	enabled := true

	inst, err := h.Backend.CreateInstance(ctx, &connectbackend.CreateInstanceInput{
		IdentityManagementType: "CONNECT_MANAGED", InstanceAlias: "wiring",
		InboundCallsEnabled: &enabled, OutboundCallsEnabled: &enabled, Tags: tags,
	})
	require.NoError(t, err)

	hoursIn := &connectbackend.CreateHoursOfOperationInput{
		Name: "hours", TimeZone: "UTC", Tags: tags,
		Config: []connectbackend.HoursOfOperationConfig{{Day: "MONDAY"}},
	}
	hoursIn.InstanceID = inst.ID

	hours, err := h.Backend.CreateHoursOfOperation(ctx, hoursIn)
	require.NoError(t, err)

	queueIn := &connectbackend.CreateQueueInput{Name: "queue", HoursOfOperationID: hours.ID, Tags: tags}
	queueIn.InstanceID = inst.ID

	queue, err := h.Backend.CreateQueue(ctx, queueIn)
	require.NoError(t, err)

	statusIn := &connectbackend.CreateAgentStatusInput{Name: "status", State: "ENABLED", Tags: tags}
	statusIn.InstanceID = inst.ID

	status, err := h.Backend.CreateAgentStatus(ctx, statusIn)
	require.NoError(t, err)

	assocIn := &connectbackend.CreateIntegrationAssociationInput{
		IntegrationType: "WISDOM_ASSISTANT",
		IntegrationArn:  "arn:aws:wisdom:us-east-1:000000000000:assistant/a",
		Tags:            tags,
	}
	assocIn.InstanceID = inst.ID

	assoc, err := h.Backend.CreateIntegrationAssociation(ctx, assocIn)
	require.NoError(t, err)

	useCaseIn := &connectbackend.CreateUseCaseInput{
		UseCaseType: "RULES_EVALUATION", AssociationID: assoc.IntegrationAssociationID, Tags: tags,
	}
	useCaseIn.InstanceID = inst.ID

	useCase, err := h.Backend.CreateUseCase(ctx, useCaseIn)
	require.NoError(t, err)

	numbers, err := h.Backend.SearchAvailablePhoneNumbers(&connectbackend.SearchAvailablePhoneNumbersInput{
		PhoneNumberCountryCode: "US", PhoneNumberType: "DID", TargetArn: inst.Arn,
	})
	require.NoError(t, err)
	require.NotEmpty(t, numbers.AvailableNumbersList)

	phone, err := h.Backend.ClaimPhoneNumber(ctx, &connectbackend.ClaimPhoneNumberInput{
		PhoneNumber: numbers.AvailableNumbersList[0].PhoneNumber, TargetArn: inst.Arn, Tags: tags,
	})
	require.NoError(t, err)

	list := func(arn string) func(context.Context) (map[string]string, error) {
		return func(context.Context) (map[string]string, error) { return h.Backend.ListTagsForResource(arn) }
	}

	runRGTWiringCases(t, rgtH, []rgtWiringCase{
		{name: "instance", arn: inst.Arn, resourceType: "connect:instance", listTags: list(inst.Arn)},
		{
			name: "hours", arn: hours.Arn,
			resourceType: "connect:instance/operating-hours", listTags: list(hours.Arn),
		},
		{name: "queue", arn: queue.QueueArn, resourceType: "connect:instance/queue", listTags: list(queue.QueueArn)},
		{
			name: "agent status", arn: status.AgentStatusARN,
			resourceType: "connect:instance/agent-state", listTags: list(status.AgentStatusARN),
		},
		{
			name: "integration association", arn: assoc.IntegrationAssociationArn,
			resourceType: "connect:instance/integration-association", listTags: list(assoc.IntegrationAssociationArn),
		},
		{
			name: "use case", arn: useCase.UseCaseArn,
			resourceType: "connect:instance/integration-association/use-case", listTags: list(useCase.UseCaseArn),
		},
		{
			name: "phone number", arn: phone.PhoneNumberArn,
			resourceType: "connect:phone-number", listTags: list(phone.PhoneNumberArn),
		},
	})
}
