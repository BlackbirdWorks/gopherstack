package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// gopherstack-tp8x: toChannelResponse used to echo the raw ExtraData map
// straight onto the wire for every channel type. That map is keyed by the
// *request* field names (ApiKey, ClientId, ClientSecret, SecretKey, BundleId,
// Certificate, TeamId, TokenKey, TokenKeyId), none of which the real
// *ChannelResponse types (aws-sdk-go-v2/service/pinpoint/types) declare --
// GCM/Baidu's real credential member is "Credential", and ADM/APNS/Baidu's
// SecretKey/ClientId/ClientSecret/BundleId/Certificate/TeamId/TokenKey/
// TokenKeyId have no response member at all, only the HasCredential/
// HasTokenKey/HasFcmServiceCredentials booleans. A real typed SDK client
// decoding these responses would see the credential fields as zero-value,
// unset -- these tests decode through the real client to prove that.

func TestGetGcmChannel_CredentialField_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("tp8x-gcm-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	_, err = client.UpdateGcmChannel(t.Context(), &pinpointsdk.UpdateGcmChannelInput{
		ApplicationId: aws.String(appID),
		GCMChannelRequest: &types.GCMChannelRequest{
			ApiKey:                      aws.String("AIzaSy-real-gcm-key"),
			DefaultAuthenticationMethod: aws.String("KEY"),
			Enabled:                     aws.Bool(true),
		},
	})
	require.NoError(t, err)

	out, err := client.GetGcmChannel(t.Context(), &pinpointsdk.GetGcmChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err)
	require.NotNil(t, out.GCMChannelResponse)

	assert.Equal(t, "AIzaSy-real-gcm-key", aws.ToString(out.GCMChannelResponse.Credential),
		"GCMChannelResponse.Credential must round-trip the API key through the real client")
	assert.True(t, aws.ToBool(out.GCMChannelResponse.HasCredential))
}

func TestGetGcmChannel_ServiceJSON_HasFcmServiceCredentials_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("tp8x-gcm-svcjson-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	_, err = client.UpdateGcmChannel(t.Context(), &pinpointsdk.UpdateGcmChannelInput{
		ApplicationId: aws.String(appID),
		GCMChannelRequest: &types.GCMChannelRequest{
			ServiceJson: aws.String(`{"type":"service_account","project_id":"tp8x"}`),
			Enabled:     aws.Bool(true),
		},
	})
	require.NoError(t, err)

	out, err := client.GetGcmChannel(t.Context(), &pinpointsdk.GetGcmChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err)
	require.NotNil(t, out.GCMChannelResponse)

	assert.True(t, aws.ToBool(out.GCMChannelResponse.HasFcmServiceCredentials),
		"GCMChannelResponse.HasFcmServiceCredentials must be set when a service-account JSON was provided")
	assert.Empty(t, aws.ToString(out.GCMChannelResponse.Credential))
}

func TestGetBaiduChannel_CredentialField_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("tp8x-baidu-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	_, err = client.UpdateBaiduChannel(t.Context(), &pinpointsdk.UpdateBaiduChannelInput{
		ApplicationId: aws.String(appID),
		BaiduChannelRequest: &types.BaiduChannelRequest{
			ApiKey:    aws.String("baidu-real-api-key"),
			SecretKey: aws.String("baidu-real-secret-key"),
			Enabled:   aws.Bool(true),
		},
	})
	require.NoError(t, err)

	out, err := client.GetBaiduChannel(t.Context(), &pinpointsdk.GetBaiduChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err)
	require.NotNil(t, out.BaiduChannelResponse)

	assert.Equal(t, "baidu-real-api-key", aws.ToString(out.BaiduChannelResponse.Credential),
		"BaiduChannelResponse.Credential must round-trip the API key through the real client")
	assert.True(t, aws.ToBool(out.BaiduChannelResponse.HasCredential))
}

func TestGetAdmChannel_NoCredentialLeak_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("tp8x-adm-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	_, err = client.UpdateAdmChannel(t.Context(), &pinpointsdk.UpdateAdmChannelInput{
		ApplicationId: aws.String(appID),
		ADMChannelRequest: &types.ADMChannelRequest{
			ClientId:     aws.String("amzn1.application-oa2-client.real"),
			ClientSecret: aws.String("adm-real-client-secret"),
			Enabled:      aws.Bool(true),
		},
	})
	require.NoError(t, err)

	out, err := client.GetAdmChannel(t.Context(), &pinpointsdk.GetAdmChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err)
	require.NotNil(t, out.ADMChannelResponse)

	// ADMChannelResponse has no field the real client secret could land in --
	// the type only declares Platform/ApplicationId/CreationDate/Enabled/
	// HasCredential/Id/IsArchived/LastModifiedBy/LastModifiedDate/Version.
	assert.True(t, aws.ToBool(out.ADMChannelResponse.HasCredential))
}

func TestGetApnsChannel_NoCredentialLeak_RealClient(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("us-east-1", "000000000000")
	h := pinpoint.NewHandler(backend)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("tp8x-apns-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	_, err = client.UpdateApnsChannel(t.Context(), &pinpointsdk.UpdateApnsChannelInput{
		ApplicationId: aws.String(appID),
		APNSChannelRequest: &types.APNSChannelRequest{
			BundleId:                    aws.String("com.example.tp8x"),
			Certificate:                 aws.String("-----BEGIN CERTIFICATE-----"),
			TeamId:                      aws.String("TP8XTEAM"),
			DefaultAuthenticationMethod: aws.String("CERTIFICATE"),
			Enabled:                     aws.Bool(true),
		},
	})
	require.NoError(t, err)

	out, err := client.GetApnsChannel(t.Context(), &pinpointsdk.GetApnsChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err)
	require.NotNil(t, out.APNSChannelResponse)

	assert.True(t, aws.ToBool(out.APNSChannelResponse.HasCredential))
	assert.Equal(t, "CERTIFICATE", aws.ToString(out.APNSChannelResponse.DefaultAuthenticationMethod))
}

// A typed SDK client can't tell a stray unknown JSON key apart from one
// that's simply absent -- ADMChannelResponse/APNSChannelResponse have no
// field for the leaked secrets, so decoding through the real client (above)
// can't distinguish "never sent" from "sent under a nonexistent key, silently
// dropped." These two assert directly against the raw response body instead.

func TestGetAdmChannel_NoRawSecretInBody(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "tp8x-adm-rawbody-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/adm",
		map[string]any{"ClientId": "adm-client-id", "ClientSecret": "adm-client-secret", "Enabled": true})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/adm", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &body))

	assert.NotContains(t, body, "ClientId")
	assert.NotContains(t, body, "ClientSecret")
	assert.Equal(t, true, body["HasCredential"])
}

func TestGetApnsChannel_NoRawSecretInBody(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "tp8x-apns-rawbody-app")

	putRec := doPinpointRequest(t, h, http.MethodPut, "/v1/apps/"+appID+"/channels/apns",
		map[string]any{
			"BundleId": "com.example.tp8x", "Certificate": "-----BEGIN CERTIFICATE-----",
			"TeamId": "TP8XTEAM", "TokenKey": "tok", "TokenKeyId": "tokid", "Enabled": true,
		})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels/apns", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &body))

	for _, k := range []string{"BundleId", "Certificate", "TeamId", "TokenKey", "TokenKeyId"} {
		assert.NotContains(t, body, k)
	}

	assert.Equal(t, true, body["HasCredential"])
	assert.Equal(t, true, body["HasTokenKey"])
}
