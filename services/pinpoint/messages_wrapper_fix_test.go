package pinpoint_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// These tests cover gopherstack-lffs, the re-audit of the gopherstack-6flj
// wrapper-key sweep on pinpoint. pinpoint is 120-of-122 ops flat/payload: the
// live restjson1 deserializer for every op decodes the whole HTTP body
// directly into the SDK output's single member field
// (awsRestjson1_deserializeDocument<Type>(&output.<Member>, shape) in
// pinpoint@v1.42.4 deserializers.go), bypassing the generated
// deserializeOpDocument<Op>Output wrapper functions entirely (those are dead
// code, never referenced from HandleDeserialize). SendMessages, SendOTPMessage,
// SendUsersMessages, and PhoneNumberValidate had all been implemented as if
// the wrapper were live -- nesting the response under a "MessageResponse" /
// "SendUsersMessageResponse" / "NumberValidateResponse" key that a real SDK
// client never reads, leaving the typed output's required fields (e.g.
// MessageResponse.ApplicationId) nil. Driven through the real SDK client
// since a raw-body map assertion cannot distinguish "wrapped" from "flat".
//
// The same trap exists symmetrically on the REQUEST side: pinpoint's
// awsRestjson1_serializeOp<Op> functions serialize the input's single member
// (MessageRequest/SendUsersMessageRequest/VerifyOTPMessageRequestParameters/
// EventsRequest/NumberValidateRequest) directly as the body too -- no
// "<Member>" wrapper key going in either. gopherstack's SendMessages,
// SendUsersMessages, VerifyOTPMessage, and PutEvents request parsers expected
// that same now-nonexistent wrapper key, so a real client's request fields
// (e.g. VerifyOTPMessage's Otp) never reached the backend.

func TestSendMessages_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{
			Name: aws.String("lffs-send-messages-app"),
		},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	const address = "+15555550100"

	out, err := client.SendMessages(t.Context(), &pinpointsdk.SendMessagesInput{
		ApplicationId: aws.String(appID),
		MessageRequest: &types.MessageRequest{
			MessageConfiguration: &types.DirectMessageConfiguration{},
			Addresses: map[string]types.AddressConfiguration{
				address: {ChannelType: types.ChannelTypeSms},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.MessageResponse)
	assert.Equal(t, appID, aws.ToString(out.MessageResponse.ApplicationId))
	require.Contains(t, out.MessageResponse.Result, address)
	assert.Equal(t, "SUCCESSFUL", string(out.MessageResponse.Result[address].DeliveryStatus))
}

func TestSendUsersMessages_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{
			Name: aws.String("lffs-send-users-messages-app"),
		},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	_, err = client.UpdateEndpoint(t.Context(), &pinpointsdk.UpdateEndpointInput{
		ApplicationId: aws.String(appID),
		EndpointId:    aws.String("lffs-endpoint"),
		EndpointRequest: &types.EndpointRequest{
			ChannelType: types.ChannelTypeEmail,
			Address:     aws.String("lffs@example.com"),
			User:        &types.EndpointUser{UserId: aws.String("lffs-user")},
		},
	})
	require.NoError(t, err)

	out, err := client.SendUsersMessages(t.Context(), &pinpointsdk.SendUsersMessagesInput{
		ApplicationId: aws.String(appID),
		SendUsersMessageRequest: &types.SendUsersMessageRequest{
			MessageConfiguration: &types.DirectMessageConfiguration{},
			Users: map[string]types.EndpointSendConfiguration{
				"lffs-user": {},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.SendUsersMessageResponse)
	assert.Equal(t, appID, aws.ToString(out.SendUsersMessageResponse.ApplicationId))
	require.Contains(t, out.SendUsersMessageResponse.Result, "lffs-user")
	assert.Len(t, out.SendUsersMessageResponse.Result["lffs-user"], 1)
}

func TestSendOTPMessage_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{
			Name: aws.String("lffs-send-otp-app"),
		},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	out, err := client.SendOTPMessage(t.Context(), &pinpointsdk.SendOTPMessageInput{
		ApplicationId: aws.String(appID),
		SendOTPMessageRequestParameters: &types.SendOTPMessageRequestParameters{
			BrandName:           aws.String("lffs-brand"),
			Channel:             aws.String("SMS"),
			DestinationIdentity: aws.String("+15555550100"),
			OriginationIdentity: aws.String("+15555550199"),
			ReferenceId:         aws.String("lffs-ref"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.MessageResponse)
	assert.Equal(t, appID, aws.ToString(out.MessageResponse.ApplicationId))
	assert.NotEmpty(t, out.MessageResponse.Result)
}

func TestPhoneNumberValidate_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	out, err := client.PhoneNumberValidate(t.Context(), &pinpointsdk.PhoneNumberValidateInput{
		NumberValidateRequest: &types.NumberValidateRequest{
			PhoneNumber: aws.String("+12125551234"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.NumberValidateResponse)
	assert.Equal(t, "US", aws.ToString(out.NumberValidateResponse.CountryCodeIso2))
	assert.Equal(t, "+12125551234", aws.ToString(out.NumberValidateResponse.OriginalPhoneNumber))
	assert.NotEmpty(t, aws.ToString(out.NumberValidateResponse.CleansedPhoneNumberE164))
}

// TestVerifyOTPMessage_WrongCode_RealClient proves VerifyOTPMessageInput's
// Otp field reaches the backend through the real SDK client. Before the
// gopherstack-lffs fix, gopherstack expected the request body nested under a
// "VerifyOTPMessageRequestParameters" key that a real client never sends, so
// Otp was always read as empty and VerifyOTPMessage silently fell back to
// its no-code "was an OTP ever sent?" path -- returning Valid=true for any
// wrong code as long as one had been sent.
func TestVerifyOTPMessage_WrongCode_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{
			Name: aws.String("lffs-verify-otp-app"),
		},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	_, err = client.SendOTPMessage(t.Context(), &pinpointsdk.SendOTPMessageInput{
		ApplicationId: aws.String(appID),
		SendOTPMessageRequestParameters: &types.SendOTPMessageRequestParameters{
			BrandName:           aws.String("lffs-brand"),
			Channel:             aws.String("SMS"),
			DestinationIdentity: aws.String("+15555550100"),
			OriginationIdentity: aws.String("+15555550199"),
			ReferenceId:         aws.String("lffs-ref"),
		},
	})
	require.NoError(t, err)

	out, err := client.VerifyOTPMessage(t.Context(), &pinpointsdk.VerifyOTPMessageInput{
		ApplicationId: aws.String(appID),
		VerifyOTPMessageRequestParameters: &types.VerifyOTPMessageRequestParameters{
			DestinationIdentity: aws.String("+15555550100"),
			Otp:                 aws.String("definitely-the-wrong-code"),
			ReferenceId:         aws.String("lffs-ref"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.VerificationResponse)
	assert.False(t, aws.ToBool(out.VerificationResponse.Valid), "wrong OTP code must not validate")
}

// TestPutEvents_RealClient proves PutEventsInput's EventsRequest.BatchItem
// reaches the backend through the real SDK client. Before the gopherstack-lffs
// fix, gopherstack expected the request body nested under an "EventsRequest"
// key that a real client never sends (pinpoint@v1.42.4
// awsRestjson1_serializeOpPutEvents serializes EventsRequest's own BatchItem
// field flat), so BatchItem was always read as empty and every event silently
// vanished.
func TestPutEvents_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{
			Name: aws.String("lffs-put-events-app"),
		},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	out, err := client.PutEvents(t.Context(), &pinpointsdk.PutEventsInput{
		ApplicationId: aws.String(appID),
		EventsRequest: &types.EventsRequest{
			BatchItem: map[string]types.EventsBatch{
				"lffs-endpoint": {
					Endpoint: &types.PublicEndpoint{},
					Events: map[string]types.Event{
						"lffs-event": {
							EventType: aws.String("lffs.custom"),
							Timestamp: aws.String("2026-08-20T00:00:00Z"),
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.EventsResponse)
	require.Contains(t, out.EventsResponse.Results, "lffs-endpoint")
	require.Contains(t, out.EventsResponse.Results["lffs-endpoint"].EventsItemResponse, "lffs-event")
	assert.EqualValues(t, 202,
		aws.ToInt32(out.EventsResponse.Results["lffs-endpoint"].EventsItemResponse["lffs-event"].StatusCode))
}
