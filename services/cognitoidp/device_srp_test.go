package cognitoidp_test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"math/big"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const deviceTestPassword = "Pass1234!"

type deviceFixture struct {
	client   *cognitoidpsdk.Client
	poolID   string
	clientID string
}

func newDeviceFixture(t *testing.T, cfg *types.DeviceConfigurationType) deviceFixture {
	t.Helper()

	f, _ := newDeviceFixtureWithHandler(t, cfg)

	return f
}

func newDeviceFixtureWithHandler(
	t *testing.T,
	cfg *types.DeviceConfigurationType,
) (deviceFixture, *cognitoidp.Handler) {
	t.Helper()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("dev-pool"), DeviceConfiguration: cfg,
	})
	require.NoError(t, err)

	poolID := aws.ToString(pool.UserPool.Id)

	app, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("dev-client"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{
			types.ExplicitAuthFlowsTypeAllowUserSrpAuth, types.ExplicitAuthFlowsTypeAllowUserPasswordAuth,
			types.ExplicitAuthFlowsTypeAllowRefreshTokenAuth,
		},
	})
	require.NoError(t, err)

	_, err = client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("dev-user"),
	})
	require.NoError(t, err)

	_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID), Username: aws.String("dev-user"),
		Password: aws.String(deviceTestPassword), Permanent: true,
	})
	require.NoError(t, err)

	return deviceFixture{client: client, poolID: poolID, clientID: aws.ToString(app.UserPoolClient.ClientId)}, h
}

// srpSignIn runs USER_SRP_AUTH up to the last RespondToAuthChallenge, optionally naming a device.
func (f deviceFixture) srpSignIn(t *testing.T, deviceKey string) *cognitoidpsdk.RespondToAuthChallengeOutput {
	t.Helper()

	srp := newSRPTestClient(t)

	init, err := f.client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		AuthFlow: types.AuthFlowTypeUserSrpAuth, ClientId: aws.String(f.clientID),
		AuthParameters: map[string]string{"USERNAME": "dev-user", "SRP_A": srp.srpA()},
	})
	require.NoError(t, err)

	responses := srp.challengeResponses(t, f.poolID, deviceTestPassword, init.ChallengeParameters)
	if deviceKey != "" {
		responses["DEVICE_KEY"] = deviceKey
	}

	out, err := f.client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ChallengeName: types.ChallengeNameTypePasswordVerifier, ClientId: aws.String(f.clientID),
		Session: init.Session, ChallengeResponses: responses,
	})
	require.NoError(t, err)

	return out
}

// registerDevice mirrors amazon-cognito-identity-js generateHashDevice + ConfirmDevice.
func (f deviceFixture) registerDevice(t *testing.T, accessToken string, md *types.NewDeviceMetadataType) string {
	t.Helper()

	pw := make([]byte, 40)
	_, err := rand.Read(pw)
	require.NoError(t, err)

	devicePassword := base64.StdEncoding.EncodeToString(pw)

	saltBytes := make([]byte, 16)
	_, err = rand.Read(saltBytes)
	require.NoError(t, err)

	salt := new(big.Int).SetBytes(saltBytes)
	inner := sha256.Sum256([]byte(aws.ToString(md.DeviceGroupKey) + aws.ToString(md.DeviceKey) + ":" + devicePassword))
	x := new(big.Int).SetBytes(func() []byte {
		d := sha256.Sum256(append(testSRPPadHex(salt), inner[:]...))

		return d[:]
	}())
	verifier := new(big.Int).Exp(testSRPG(), x, testSRPN())

	_, err = f.client.ConfirmDevice(t.Context(), &cognitoidpsdk.ConfirmDeviceInput{
		AccessToken: aws.String(accessToken), DeviceKey: md.DeviceKey, DeviceName: aws.String("laptop"),
		DeviceSecretVerifierConfig: &types.DeviceSecretVerifierConfigType{
			PasswordVerifier: aws.String(base64.StdEncoding.EncodeToString(testSRPPadHex(verifier))),
			Salt:             aws.String(base64.StdEncoding.EncodeToString(testSRPPadHex(salt))),
		},
	})
	require.NoError(t, err)

	return devicePassword
}

func (f deviceFixture) deviceSignIn(
	t *testing.T,
	challenge *cognitoidpsdk.RespondToAuthChallengeOutput,
	md *types.NewDeviceMetadataType,
	devicePassword string,
) (*cognitoidpsdk.RespondToAuthChallengeOutput, error) {
	t.Helper()

	require.Equal(t, types.ChallengeNameTypeDeviceSrpAuth, challenge.ChallengeName)

	srp := newSRPTestClient(t)

	verifierChallenge, err := f.client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ChallengeName: types.ChallengeNameTypeDeviceSrpAuth, ClientId: aws.String(f.clientID),
		Session: challenge.Session,
		ChallengeResponses: map[string]string{
			"USERNAME": "dev-user", "DEVICE_KEY": aws.ToString(md.DeviceKey), "SRP_A": srp.srpA(),
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChallengeNameTypeDevicePasswordVerifier, verifierChallenge.ChallengeName)

	responses := srp.claim(t, aws.ToString(md.DeviceGroupKey), aws.ToString(md.DeviceKey), devicePassword,
		verifierChallenge.ChallengeParameters)
	responses["USERNAME"] = "dev-user"
	responses["DEVICE_KEY"] = aws.ToString(md.DeviceKey)

	return f.client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ChallengeName: types.ChallengeNameTypeDevicePasswordVerifier, ClientId: aws.String(f.clientID),
		Session: verifierChallenge.Session, ChallengeResponses: responses,
	})
}

func TestRealClient_DeviceSRPAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wrongPassword bool
	}{
		{"success", false},
		{"wrong_device_password", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDeviceFixture(t, &types.DeviceConfigurationType{ChallengeRequiredOnNewDevice: true})

			first := f.srpSignIn(t, "")
			require.NotNil(t, first.AuthenticationResult)
			md := first.AuthenticationResult.NewDeviceMetadata
			require.NotNil(t, md)
			assert.NotEmpty(t, aws.ToString(md.DeviceKey))
			assert.NotEmpty(t, aws.ToString(md.DeviceGroupKey))

			devicePassword := f.registerDevice(t, aws.ToString(first.AuthenticationResult.AccessToken), md)

			second := f.srpSignIn(t, aws.ToString(md.DeviceKey))
			require.Nil(t, second.AuthenticationResult)

			if tt.wrongPassword {
				_, err := f.deviceSignIn(t, second, md, "not-the-device-password")
				require.Error(t, err)

				return
			}

			done, err := f.deviceSignIn(t, second, md, devicePassword)
			require.NoError(t, err)
			require.NotNil(t, done.AuthenticationResult)
			assert.NotEmpty(t, aws.ToString(done.AuthenticationResult.AccessToken))
			assert.Nil(t, done.AuthenticationResult.NewDeviceMetadata)

			listed, err := f.client.ListDevices(t.Context(), &cognitoidpsdk.ListDevicesInput{
				AccessToken: done.AuthenticationResult.AccessToken,
			})
			require.NoError(t, err)
			require.Len(t, listed.Devices, 1)
			assert.Equal(t, aws.ToString(md.DeviceKey), aws.ToString(listed.Devices[0].DeviceKey))
		})
	}
}

func TestRealClient_NewDeviceMetadataGating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg     *types.DeviceConfigurationType
		name    string
		wantNew bool
	}{
		{nil, "tracking_off", false},
		{&types.DeviceConfigurationType{}, "tracking_on", true},
		{&types.DeviceConfigurationType{DeviceOnlyRememberedOnUserPrompt: true}, "prompt_only", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDeviceFixture(t, tt.cfg)
			out := f.srpSignIn(t, "")
			require.NotNil(t, out.AuthenticationResult)
			assert.Equal(t, tt.wantNew, out.AuthenticationResult.NewDeviceMetadata != nil)

			refreshed, err := f.client.GetTokensFromRefreshToken(
				t.Context(),
				&cognitoidpsdk.GetTokensFromRefreshTokenInput{
					ClientId: aws.String(f.clientID), RefreshToken: out.AuthenticationResult.RefreshToken,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantNew, refreshed.AuthenticationResult.NewDeviceMetadata != nil)
		})
	}
}

func TestDeviceSRPAuth_SkipsMFAForRememberedDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantChallenge types.ChallengeNameType
		challengeNew  bool
		wantTokens    bool
	}{
		{name: "challenge_required_skips_mfa", challengeNew: true, wantTokens: true},
		{
			name: "challenge_not_required_keeps_mfa", challengeNew: false,
			wantTokens: false, wantChallenge: types.ChallengeNameTypeSmsMfa,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, h := newDeviceFixtureWithHandler(
				t,
				&types.DeviceConfigurationType{ChallengeRequiredOnNewDevice: tt.challengeNew},
			)

			first := f.srpSignIn(t, "")
			md := first.AuthenticationResult.NewDeviceMetadata
			require.NotNil(t, md)

			devicePassword := f.registerDevice(t, aws.ToString(first.AuthenticationResult.AccessToken), md)

			_, err := f.client.UpdateUserPool(t.Context(), &cognitoidpsdk.UpdateUserPoolInput{
				UserPoolId: aws.String(f.poolID), MfaConfiguration: types.UserPoolMfaTypeOn,
				DeviceConfiguration: &types.DeviceConfigurationType{ChallengeRequiredOnNewDevice: tt.challengeNew},
			})
			require.NoError(t, err)
			require.NoError(t, h.Backend.AdminSetUserMFAPreference(f.poolID, "dev-user", true, false, "SMS_MFA"))

			done, err := f.deviceSignIn(t, f.srpSignIn(t, aws.ToString(md.DeviceKey)), md, devicePassword)
			require.NoError(t, err)

			if tt.wantTokens {
				require.NotNil(t, done.AuthenticationResult)
				assert.NotEmpty(t, aws.ToString(done.AuthenticationResult.AccessToken))

				return
			}

			assert.Nil(t, done.AuthenticationResult)
			assert.Equal(t, tt.wantChallenge, done.ChallengeName)
		})
	}
}
