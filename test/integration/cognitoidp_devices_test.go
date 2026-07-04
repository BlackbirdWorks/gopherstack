package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	cognitotypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// signedInCognitoUser creates a user pool, an app client, and a confirmed user, then
// authenticates and returns the resulting AccessToken plus identifying info for cleanup.
func signedInCognitoUser(t *testing.T, client *cognitoidpsdk.Client) (string, string, string) {
	t.Helper()

	var accessToken, poolID, username string
	ctx := t.Context()

	poolOut, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("test-pool-" + t.Name() + "-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err, "CreateUserPool failed")
	poolID = aws.ToString(poolOut.UserPool.Id)

	clientOut, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("test-client"),
	})
	require.NoError(t, err, "CreateUserPoolClient failed")
	appClientID := aws.ToString(clientOut.UserPoolClient.ClientId)

	username = "device-test-user"

	_, err = client.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String(username),
		UserAttributes: []cognitotypes.AttributeType{
			{Name: aws.String("email"), Value: aws.String("device-test@example.com")},
		},
		TemporaryPassword: aws.String("TempPass123!"),
	})
	require.NoError(t, err, "AdminCreateUser failed")

	_, err = client.AdminSetUserPassword(ctx, &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String(username),
		Password:   aws.String("PermPass456!"),
		Permanent:  true,
	})
	require.NoError(t, err, "AdminSetUserPassword failed")

	authOut, err := client.AdminInitiateAuth(ctx, &cognitoidpsdk.AdminInitiateAuthInput{
		UserPoolId: aws.String(poolID),
		ClientId:   aws.String(appClientID),
		AuthFlow:   cognitotypes.AuthFlowTypeUserPasswordAuth,
		AuthParameters: map[string]string{
			"USERNAME": username,
			"PASSWORD": "PermPass456!",
		},
	})
	require.NoError(t, err, "AdminInitiateAuth failed")
	require.NotNil(t, authOut.AuthenticationResult)

	accessToken = aws.ToString(authOut.AuthenticationResult.AccessToken)
	require.NotEmpty(t, accessToken)

	return accessToken, poolID, username
}

// TestIntegration_CognitoIDP_DeviceLifecycle drives ConfirmDevice followed by
// AdminListDevices/AdminGetDevice through the real SDK, asserting the device round-trips with
// real (non-empty) fields.
func TestIntegration_CognitoIDP_DeviceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCognitoIDPClient(t)
	ctx := t.Context()

	accessToken, poolID, username := signedInCognitoUser(t, client)

	deviceKey := "us-east-1_" + uuid.NewString()
	deviceName := "integration-test-device"

	confirmOut, err := client.ConfirmDevice(ctx, &cognitoidpsdk.ConfirmDeviceInput{
		AccessToken: aws.String(accessToken),
		DeviceKey:   aws.String(deviceKey),
		DeviceName:  aws.String(deviceName),
		DeviceSecretVerifierConfig: &cognitotypes.DeviceSecretVerifierConfigType{
			PasswordVerifier: aws.String("dGVzdC12ZXJpZmllcg=="),
			Salt:             aws.String("dGVzdC1zYWx0"),
		},
	})
	require.NoError(t, err, "ConfirmDevice should succeed")
	_ = confirmOut // UserConfirmationNecessary is a bool; nothing further to assert on it directly.

	listOut, err := client.AdminListDevices(ctx, &cognitoidpsdk.AdminListDevicesInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String(username),
	})
	require.NoError(t, err, "AdminListDevices should succeed")
	require.NotEmpty(t, listOut.Devices, "expected the confirmed device to appear in AdminListDevices")

	found := false
	for _, d := range listOut.Devices {
		if aws.ToString(d.DeviceKey) == deviceKey {
			found = true
			require.NotNil(t, d.DeviceCreateDate, "device create date should be set")
			assert.False(t, d.DeviceCreateDate.IsZero(), "device create date should be a real timestamp")
		}
	}
	assert.True(t, found, "confirmed device should be present in AdminListDevices")

	getOut, err := client.AdminGetDevice(ctx, &cognitoidpsdk.AdminGetDeviceInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String(username),
		DeviceKey:  aws.String(deviceKey),
	})
	require.NoError(t, err, "AdminGetDevice should succeed")
	require.NotNil(t, getOut.Device)
	assert.Equal(t, deviceKey, aws.ToString(getOut.Device.DeviceKey))
	require.NotNil(t, getOut.Device.DeviceCreateDate, "device create date should round-trip")
}

// TestIntegration_CognitoIDP_StartWebAuthnRegistration verifies StartWebAuthnRegistration
// returns real, non-empty passkey creation options (rp, user, and a random challenge) rather
// than a stubbed/empty document.
func TestIntegration_CognitoIDP_StartWebAuthnRegistration(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCognitoIDPClient(t)
	ctx := t.Context()

	accessToken, _, _ := signedInCognitoUser(t, client)

	out, err := client.StartWebAuthnRegistration(ctx, &cognitoidpsdk.StartWebAuthnRegistrationInput{
		AccessToken: aws.String(accessToken),
	})
	require.NoError(t, err, "StartWebAuthnRegistration should succeed")
	require.NotNil(t, out.CredentialCreationOptions, "expected real credential creation options, not empty")

	var opts map[string]any
	require.NoError(t, out.CredentialCreationOptions.UnmarshalSmithyDocument(&opts))

	challenge, ok := opts["challenge"].(string)
	require.True(t, ok, "expected a 'challenge' field in the credential creation options")
	assert.NotEmpty(t, challenge, "challenge should be a real, non-empty random value")

	rp, ok := opts["rp"].(map[string]any)
	require.True(t, ok, "expected an 'rp' field in the credential creation options")
	assert.NotEmpty(t, rp["id"], "rp.id should be populated")

	user, ok := opts["user"].(map[string]any)
	require.True(t, ok, "expected a 'user' field in the credential creation options")
	assert.NotEmpty(t, user["id"], "user.id should be populated")
}
