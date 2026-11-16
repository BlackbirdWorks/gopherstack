package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartOutboundChatContact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(e *contactEnv, in *sdk.StartOutboundChatContactInput)
		name   string
		code   string
	}{
		{name: "created", mutate: func(*contactEnv, *sdk.StartOutboundChatContactInput) {}},
		{name: "source must be a phone number arn", code: "InvalidParameterException",
			mutate: func(_ *contactEnv, in *sdk.StartOutboundChatContactInput) {
				in.SourceEndpoint.Type = types.EndpointTypeTelephoneNumber
			}},
		{name: "destination must be e164", code: "InvalidParameterException",
			mutate: func(_ *contactEnv, in *sdk.StartOutboundChatContactInput) {
				in.DestinationEndpoint.Address = aws.String("5551234")
			}},
		{name: "unclaimed source", code: "ResourceNotFoundException",
			mutate: func(_ *contactEnv, in *sdk.StartOutboundChatContactInput) {
				in.SourceEndpoint.Address = aws.String("arn:aws:connect:us-east-1:111122223333:phone-number/ghost")
			}},
		{name: "unknown flow", code: "ResourceNotFoundException",
			mutate: func(_ *contactEnv, in *sdk.StartOutboundChatContactInput) {
				in.ContactFlowId = aws.String("ghost")
			}},
		{name: "duration range", code: "InvalidParameterException",
			mutate: func(_ *contactEnv, in *sdk.StartOutboundChatContactInput) {
				in.ChatDurationInMinutes = aws.Int32(10)
			}},
		{name: "plain text required", code: "InvalidParameterException",
			mutate: func(_ *contactEnv, in *sdk.StartOutboundChatContactInput) {
				in.SupportedMessagingContentTypes = []string{"text/markdown"}
			}},
		{name: "templates unsupported", code: "InvalidRequestException",
			mutate: func(_ *contactEnv, in *sdk.StartOutboundChatContactInput) {
				in.InitialTemplatedSystemMessage = &types.TemplatedMessageConfig{
					KnowledgeBaseId: aws.String("kb"), MessageTemplateId: aws.String("t"),
					TemplateAttributes: &types.TemplateAttributes{},
				}
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			arnOut, err := e.f.client.ListPhoneNumbersV2(t.Context(), &sdk.ListPhoneNumbersV2Input{
				TargetArn: aws.String("arn:aws:connect:us-east-1:" + testAccount + ":instance/" + e.inst),
			})
			require.NoError(t, err)
			require.NotEmpty(t, arnOut.ListPhoneNumbersSummaryList)

			in := &sdk.StartOutboundChatContactInput{
				InstanceId: aws.String(e.inst), ContactFlowId: aws.String(e.flow),
				SourceEndpoint: &types.Endpoint{
					Type:    types.EndpointTypeConnectPhonenumberArn,
					Address: arnOut.ListPhoneNumbersSummaryList[0].PhoneNumberArn,
				},
				DestinationEndpoint: &types.Endpoint{
					Type: types.EndpointTypeTelephoneNumber, Address: aws.String("+442071230000"),
				},
				SegmentAttributes: map[string]types.SegmentAttributeValue{
					"connect:Subtype": {ValueString: aws.String("connect:SMS")},
				},
				Attributes: map[string]string{"k": "v"},
			}
			tt.mutate(e, in)

			out, err := e.f.client.StartOutboundChatContact(t.Context(), in)
			if tt.code != "" {
				assert.Equal(t, tt.code, apiCode(err))

				return
			}

			require.NoError(t, err)

			got, err := e.f.restored(t).client.DescribeContact(t.Context(),
				&sdk.DescribeContactInput{InstanceId: aws.String(e.inst), ContactId: out.ContactId})
			require.NoError(t, err)
			assert.Equal(t, types.ChannelChat, got.Contact.Channel)
			assert.Equal(t, types.ContactInitiationMethodOutbound, got.Contact.InitiationMethod)
			assert.Equal(t, "+442071230000", aws.ToString(got.Contact.CustomerEndpoint.Address))
			assert.Equal(t, e.phone, aws.ToString(got.Contact.SystemEndpoint.Address))
			assert.Equal(t, "v", got.Contact.Attributes["k"])
		})
	}
}
