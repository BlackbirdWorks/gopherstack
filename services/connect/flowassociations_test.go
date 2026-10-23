package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlowAssociations(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "flowassoc"))
	flowA := aws.String(f.newFlow(t, aws.ToString(inst), "flow-a"))
	flowB := aws.String(f.newFlow(t, aws.ToString(inst), "flow-b"))
	const res = "arn:aws:sms-voice:us-east-1:123456789012:phone-number/pn-1"

	tests := []struct {
		name string
		code string
		in   sdk.AssociateFlowInput
	}{
		{
			name: "bad type",
			in:   sdk.AssociateFlowInput{FlowId: flowA, ResourceId: aws.String(res), ResourceType: "FAX"},
			code: "InvalidParameterException",
		},
		{
			name: "unknown flow",
			in: sdk.AssociateFlowInput{
				FlowId:       aws.String("gone"),
				ResourceId:   aws.String(res),
				ResourceType: types.FlowAssociationResourceTypeSmsPhoneNumber,
			},
			code: "ResourceNotFoundException",
		},
		{
			name: "ok",
			in: sdk.AssociateFlowInput{
				FlowId:       flowA,
				ResourceId:   aws.String(res),
				ResourceType: types.FlowAssociationResourceTypeSmsPhoneNumber,
			},
		},
	}
	for _, tt := range tests {
		in := tt.in
		in.InstanceId = inst
		_, err := f.client.AssociateFlow(t.Context(), &in)
		assert.Equal(t, tt.code, apiCode(err), tt.name)
	}

	_, err := f.client.AssociateFlow(t.Context(), &sdk.AssociateFlowInput{
		InstanceId:   inst,
		FlowId:       flowB,
		ResourceId:   aws.String(res),
		ResourceType: types.FlowAssociationResourceTypeSmsPhoneNumber,
	})
	require.NoError(t, err, "re-association replaces the flow")

	got, err := f.client.GetFlowAssociation(t.Context(), &sdk.GetFlowAssociationInput{
		InstanceId: inst, ResourceId: aws.String(res), ResourceType: types.FlowAssociationResourceTypeSmsPhoneNumber,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(flowB), aws.ToString(got.FlowId))

	_, err = f.client.DeleteContactFlow(
		t.Context(),
		&sdk.DeleteContactFlowInput{InstanceId: inst, ContactFlowId: flowB},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	list, err := f.client.ListFlowAssociations(t.Context(), &sdk.ListFlowAssociationsInput{InstanceId: inst})
	require.NoError(t, err)
	require.Len(t, list.FlowAssociationSummaryList, 1)

	byType, err := f.client.ListFlowAssociations(t.Context(), &sdk.ListFlowAssociationsInput{
		InstanceId: inst, ResourceType: types.ListFlowAssociationResourceTypeInboundEmail,
	})
	require.NoError(t, err)
	assert.Empty(t, byType.FlowAssociationSummaryList)

	batch, err := f.client.BatchGetFlowAssociation(t.Context(), &sdk.BatchGetFlowAssociationInput{
		InstanceId: inst, ResourceIds: []string{res, "other"}, ResourceType: "SMS_PHONE_NUMBER",
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err), "SMS is not a list type")
	assert.Nil(t, batch)

	dst := f.restored(t)
	got, err = dst.client.GetFlowAssociation(t.Context(), &sdk.GetFlowAssociationInput{
		InstanceId: inst, ResourceId: aws.String(res), ResourceType: types.FlowAssociationResourceTypeSmsPhoneNumber,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(flowB), aws.ToString(got.FlowId))

	_, err = f.client.DisassociateFlow(t.Context(), &sdk.DisassociateFlowInput{
		InstanceId: inst, ResourceId: aws.String(res), ResourceType: types.FlowAssociationResourceTypeSmsPhoneNumber,
	})
	require.NoError(t, err)
	_, err = f.client.DisassociateFlow(t.Context(), &sdk.DisassociateFlowInput{
		InstanceId: inst, ResourceId: aws.String(res), ResourceType: types.FlowAssociationResourceTypeSmsPhoneNumber,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestFlowAssociationsListVoicePhoneNumbers(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "voiceassoc"))
	flow := aws.String(f.newFlow(t, aws.ToString(inst), "flow"))

	found, err := f.client.SearchAvailablePhoneNumbers(t.Context(), &sdk.SearchAvailablePhoneNumbersInput{
		InstanceId:             inst,
		PhoneNumberCountryCode: types.PhoneNumberCountryCodeUs,
		PhoneNumberType:        types.PhoneNumberTypeDid,
	})
	require.NoError(t, err)
	require.NotEmpty(t, found.AvailableNumbersList)

	claimed, err := f.client.ClaimPhoneNumber(t.Context(), &sdk.ClaimPhoneNumberInput{
		InstanceId: inst, PhoneNumber: found.AvailableNumbersList[0].PhoneNumber,
	})
	require.NoError(t, err)

	_, err = f.client.AssociatePhoneNumberContactFlow(t.Context(), &sdk.AssociatePhoneNumberContactFlowInput{
		InstanceId: inst, PhoneNumberId: claimed.PhoneNumberId, ContactFlowId: flow,
	})
	require.NoError(t, err)

	list, err := f.client.ListFlowAssociations(t.Context(), &sdk.ListFlowAssociationsInput{
		InstanceId: inst, ResourceType: types.ListFlowAssociationResourceTypeVoicePhoneNumber,
	})
	require.NoError(t, err)
	require.Len(t, list.FlowAssociationSummaryList, 1)
	assert.Equal(t, aws.ToString(claimed.PhoneNumberArn), aws.ToString(list.FlowAssociationSummaryList[0].ResourceId))

	batch, err := f.client.BatchGetFlowAssociation(t.Context(), &sdk.BatchGetFlowAssociationInput{
		InstanceId: inst, ResourceIds: []string{aws.ToString(claimed.PhoneNumberArn)},
		ResourceType: types.ListFlowAssociationResourceTypeVoicePhoneNumber,
	})
	require.NoError(t, err)
	assert.Len(t, batch.FlowAssociationSummaryList, 1)
}
