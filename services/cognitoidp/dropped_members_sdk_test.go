package cognitoidp_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestSDK_UserPoolSettingsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create_then_describe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			created, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
				PoolName:              aws.String("settings"),
				AdminCreateUserConfig: &types.AdminCreateUserConfigType{AllowAdminCreateUserOnly: true},
				DeviceConfiguration:   &types.DeviceConfigurationType{ChallengeRequiredOnNewDevice: true},
				UserPoolAddOns: &types.UserPoolAddOnsType{
					AdvancedSecurityMode: types.AdvancedSecurityModeTypeAudit,
				},
				UsernameAttributes:       []types.UsernameAttributeType{types.UsernameAttributeTypeEmail},
				AliasAttributes:          []types.AliasAttributeType{types.AliasAttributeTypePhoneNumber},
				UsernameConfiguration:    &types.UsernameConfigurationType{CaseSensitive: aws.Bool(false)},
				EmailVerificationSubject: aws.String("subj"),
				SmsAuthenticationMessage: aws.String("code {####}"),
				UserPoolTier:             types.UserPoolTierTypePlus,
				VerificationMessageTemplate: &types.VerificationMessageTemplateType{
					DefaultEmailOption: types.DefaultEmailOptionTypeConfirmWithLink,
				},
			})
			require.NoError(t, err)

			got, err := client.DescribeUserPool(
				ctx,
				&cognitoidpsdk.DescribeUserPoolInput{UserPoolId: created.UserPool.Id},
			)
			require.NoError(t, err)

			pool := got.UserPool
			assert.True(t, pool.AdminCreateUserConfig.AllowAdminCreateUserOnly)
			assert.True(t, pool.DeviceConfiguration.ChallengeRequiredOnNewDevice)
			assert.Equal(t, types.AdvancedSecurityModeTypeAudit, pool.UserPoolAddOns.AdvancedSecurityMode)
			assert.Equal(t, []types.UsernameAttributeType{types.UsernameAttributeTypeEmail}, pool.UsernameAttributes)
			assert.Equal(t, []types.AliasAttributeType{types.AliasAttributeTypePhoneNumber}, pool.AliasAttributes)
			require.NotNil(t, pool.UsernameConfiguration)
			assert.False(t, aws.ToBool(pool.UsernameConfiguration.CaseSensitive))
			assert.Equal(t, "subj", aws.ToString(pool.EmailVerificationSubject))
			assert.Equal(t, "code {####}", aws.ToString(pool.SmsAuthenticationMessage))
			assert.Equal(t, types.UserPoolTierTypePlus, pool.UserPoolTier)
			assert.Equal(
				t,
				types.DefaultEmailOptionTypeConfirmWithLink,
				pool.VerificationMessageTemplate.DefaultEmailOption,
			)
		})
	}
}

func TestSDK_UpdateUserPoolAppliesMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantTier   types.UserPoolTierType
		updateTier types.UserPoolTierType
	}{
		{name: "tier_set", updateTier: types.UserPoolTierTypePlus, wantTier: types.UserPoolTierTypePlus},
		{name: "tier_omitted_resets", wantTier: types.UserPoolTierTypeEssentials},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			created, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
				PoolName:                 aws.String("before"),
				UserPoolTier:             types.UserPoolTierTypePlus,
				EmailVerificationMessage: aws.String("old {####}"),
				UsernameAttributes:       []types.UsernameAttributeType{types.UsernameAttributeTypeEmail},
			})
			require.NoError(t, err)

			_, err = client.UpdateUserPool(ctx, &cognitoidpsdk.UpdateUserPoolInput{
				UserPoolId:   created.UserPool.Id,
				PoolName:     aws.String("after"),
				UserPoolTier: tt.updateTier,
				SmsConfiguration: &types.SmsConfigurationType{
					SnsCallerArn: aws.String("arn:aws:iam::000000000000:role/sns"),
				},
				UserAttributeUpdateSettings: &types.UserAttributeUpdateSettingsType{
					AttributesRequireVerificationBeforeUpdate: []types.VerifiedAttributeType{
						types.VerifiedAttributeTypeEmail,
					},
				},
				UserPoolTags: map[string]string{"env": "dev"},
			})
			require.NoError(t, err)

			got, err := client.DescribeUserPool(
				ctx,
				&cognitoidpsdk.DescribeUserPoolInput{UserPoolId: created.UserPool.Id},
			)
			require.NoError(t, err)

			pool := got.UserPool
			assert.Equal(t, "after", aws.ToString(pool.Name))
			assert.Equal(t, tt.wantTier, pool.UserPoolTier)
			assert.Equal(t, "arn:aws:iam::000000000000:role/sns", aws.ToString(pool.SmsConfiguration.SnsCallerArn))
			assert.Equal(t, []types.VerifiedAttributeType{types.VerifiedAttributeTypeEmail},
				pool.UserAttributeUpdateSettings.AttributesRequireVerificationBeforeUpdate)
			assert.Nil(t, pool.EmailVerificationMessage, "omitted members reset on update")
			assert.Equal(t, []types.UsernameAttributeType{types.UsernameAttributeTypeEmail}, pool.UsernameAttributes,
				"create-only members survive update")

			tags, err := client.ListTagsForResource(ctx, &cognitoidpsdk.ListTagsForResourceInput{ResourceArn: pool.Arn})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "dev"}, tags.Tags)
		})
	}
}

func TestSDK_SignUpAdminOnlyPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		adminOnly bool
		wantErr   bool
	}{
		{name: "admin_only_rejects", adminOnly: true, wantErr: true},
		{name: "default_allows"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
				PoolName:              aws.String("p"),
				AdminCreateUserConfig: &types.AdminCreateUserConfigType{AllowAdminCreateUserOnly: tt.adminOnly},
			})
			require.NoError(t, err)

			app, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
				UserPoolId: pool.UserPool.Id, ClientName: aws.String("c"),
			})
			require.NoError(t, err)

			_, err = client.SignUp(ctx, &cognitoidpsdk.SignUpInput{
				ClientId: app.UserPoolClient.ClientId, Username: aws.String("u"), Password: aws.String("Passw0rd!x"),
			})

			if tt.wantErr {
				var na *types.NotAuthorizedException
				require.ErrorAs(t, err, &na)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_UserPoolClientMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		customSecret string
		generate     bool
		propagate    bool
		wantErr      bool
	}{
		{name: "custom_secret", customSecret: "my-own-secret-value-123", propagate: true},
		{name: "generate_and_custom_conflict", generate: true, customSecret: "x", wantErr: true},
		{name: "propagate_without_secret", propagate: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
			require.NoError(t, err)

			in := &cognitoidpsdk.CreateUserPoolClientInput{
				UserPoolId:          pool.UserPool.Id,
				ClientName:          aws.String("c"),
				GenerateSecret:      tt.generate,
				AuthSessionValidity: aws.Int32(5),
				AnalyticsConfiguration: &types.AnalyticsConfigurationType{
					ApplicationId: aws.String("app"), UserDataShared: true,
				},
				RefreshTokenRotation: &types.RefreshTokenRotationType{
					Feature: types.FeatureTypeEnabled, RetryGracePeriodSeconds: aws.Int32(10),
				},
			}
			if tt.customSecret != "" {
				in.ClientSecret = aws.String(tt.customSecret)
			}

			if tt.propagate {
				in.EnablePropagateAdditionalUserContextData = aws.Bool(true)
			}

			created, err := client.CreateUserPoolClient(ctx, in)
			if tt.wantErr {
				var ip *types.InvalidParameterException
				require.ErrorAs(t, err, &ip)

				return
			}

			require.NoError(t, err)

			got, err := client.DescribeUserPoolClient(ctx, &cognitoidpsdk.DescribeUserPoolClientInput{
				UserPoolId: pool.UserPool.Id, ClientId: created.UserPoolClient.ClientId,
			})
			require.NoError(t, err)

			c := got.UserPoolClient
			assert.Equal(t, tt.customSecret, aws.ToString(c.ClientSecret))
			assert.Equal(t, int32(5), aws.ToInt32(c.AuthSessionValidity))
			assert.True(t, aws.ToBool(c.EnablePropagateAdditionalUserContextData))
			assert.Equal(t, "app", aws.ToString(c.AnalyticsConfiguration.ApplicationId))
			assert.Equal(t, types.FeatureTypeEnabled, c.RefreshTokenRotation.Feature)

			updated, err := client.UpdateUserPoolClient(ctx, &cognitoidpsdk.UpdateUserPoolClientInput{
				UserPoolId: pool.UserPool.Id, ClientId: created.UserPoolClient.ClientId,
				AuthSessionValidity: aws.Int32(7),
			})
			require.NoError(t, err)
			assert.Equal(t, int32(7), aws.ToInt32(updated.UserPoolClient.AuthSessionValidity))
			assert.Nil(t, updated.UserPoolClient.RefreshTokenRotation, "omitted members reset on update")
		})
	}
}

func TestSDK_ListUsersAttributesToGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		get    []string
		want   []string
		subset bool
	}{
		{name: "subset", get: []string{"email"}, want: []string{"email"}},
		{name: "none_requested", get: nil, want: []string{"email", "given_name", "sub"}, subset: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
			require.NoError(t, err)

			_, err = client.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
				UserPoolId: pool.UserPool.Id, Username: aws.String("u"),
				UserAttributes: []types.AttributeType{
					{Name: aws.String("email"), Value: aws.String("u@example.com")},
					{Name: aws.String("given_name"), Value: aws.String("U")},
				},
			})
			require.NoError(t, err)

			out, err := client.ListUsers(ctx, &cognitoidpsdk.ListUsersInput{
				UserPoolId: pool.UserPool.Id, AttributesToGet: tt.get,
			})
			require.NoError(t, err)
			require.Len(t, out.Users, 1)

			var names []string
			for _, a := range out.Users[0].Attributes {
				names = append(names, aws.ToString(a.Name))
			}

			if tt.subset {
				assert.Subset(t, names, tt.want)

				return
			}

			assert.ElementsMatch(t, tt.want, names)
		})
	}
}

func TestSDK_SetUserMFAPreferenceEmail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantPref string
		wantList []string
		enabled  bool
	}{
		{name: "enabled_preferred", enabled: true, wantList: []string{"EMAIL_OTP"}, wantPref: "EMAIL_OTP"},
		{name: "disabled", enabled: false, wantList: nil, wantPref: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
			require.NoError(t, err)

			_, err = client.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
				UserPoolId: pool.UserPool.Id, Username: aws.String("u"),
			})
			require.NoError(t, err)

			_, err = client.AdminSetUserMFAPreference(ctx, &cognitoidpsdk.AdminSetUserMFAPreferenceInput{
				UserPoolId: pool.UserPool.Id, Username: aws.String("u"),
				EmailMfaSettings: &types.EmailMfaSettingsType{Enabled: tt.enabled, PreferredMfa: tt.enabled},
			})
			require.NoError(t, err)

			got, err := client.AdminGetUser(ctx, &cognitoidpsdk.AdminGetUserInput{
				UserPoolId: pool.UserPool.Id, Username: aws.String("u"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantList, got.UserMFASettingList)
			assert.Equal(t, tt.wantPref, aws.ToString(got.PreferredMfaSetting))
		})
	}
}

func TestSDK_SetUserPoolMfaConfigWebAuthn(t *testing.T) {
	t.Parallel()

	client := newTestCognitoIDPClient(t, newTestHandler(t))
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
	require.NoError(t, err)

	_, err = client.SetUserPoolMfaConfig(ctx, &cognitoidpsdk.SetUserPoolMfaConfigInput{
		UserPoolId: pool.UserPool.Id,
		WebAuthnConfiguration: &types.WebAuthnConfigurationType{
			RelyingPartyId:      aws.String("auth.example.com"),
			FactorConfiguration: types.WebAuthnFactorConfigurationTypeMultiFactorWithUserVerification,
		},
	})
	require.NoError(t, err)

	got, err := client.GetUserPoolMfaConfig(ctx, &cognitoidpsdk.GetUserPoolMfaConfigInput{UserPoolId: pool.UserPool.Id})
	require.NoError(t, err)
	require.NotNil(t, got.WebAuthnConfiguration)
	assert.Equal(t, "auth.example.com", aws.ToString(got.WebAuthnConfiguration.RelyingPartyId))
	assert.Equal(t, types.WebAuthnFactorConfigurationTypeMultiFactorWithUserVerification,
		got.WebAuthnConfiguration.FactorConfiguration)
}

func TestSDK_SetUICustomizationImageFile(t *testing.T) {
	t.Parallel()

	client := newTestCognitoIDPClient(t, newTestHandler(t))
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
	require.NoError(t, err)

	out, err := client.SetUICustomization(ctx, &cognitoidpsdk.SetUICustomizationInput{
		UserPoolId: pool.UserPool.Id, CSS: aws.String(".label-customizable{color:red}"),
		ImageFile: []byte("\x89PNG-bytes"),
	})
	require.NoError(t, err)
	assert.Equal(t, ".label-customizable{color:red}", aws.ToString(out.UICustomization.CSS))
}

func secretHash(username, clientID, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(username + clientID))

	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func TestSDK_GetTokensFromRefreshToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rotation    *types.RefreshTokenRotationType
		secretSent  string
		wantRotated bool
		wantErr     bool
	}{
		{name: "rotation_enabled", rotation: &types.RefreshTokenRotationType{Feature: types.FeatureTypeEnabled},
			secretSent: "s3cret-value-1234567", wantRotated: true},
		{name: "rotation_disabled", rotation: &types.RefreshTokenRotationType{Feature: types.FeatureTypeDisabled},
			secretSent: "s3cret-value-1234567"},
		{name: "rotation_unset", secretSent: "s3cret-value-1234567"},
		{name: "wrong_secret", secretSent: "nope", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
			require.NoError(t, err)

			const secret = "s3cret-value-1234567"

			app, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
				UserPoolId: pool.UserPool.Id, ClientName: aws.String("c"), ClientSecret: aws.String(secret),
				ExplicitAuthFlows:    []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
				RefreshTokenRotation: tt.rotation,
			})
			require.NoError(t, err)

			clientID := aws.ToString(app.UserPoolClient.ClientId)

			_, err = client.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
				UserPoolId: pool.UserPool.Id, Username: aws.String("u"), MessageAction: types.MessageActionTypeSuppress,
			})
			require.NoError(t, err)

			_, err = client.AdminSetUserPassword(ctx, &cognitoidpsdk.AdminSetUserPasswordInput{
				UserPoolId: pool.UserPool.Id, Username: aws.String("u"), Permanent: true,
				Password: aws.String("Passw0rd!x"),
			})
			require.NoError(t, err)

			auth, err := client.InitiateAuth(ctx, &cognitoidpsdk.InitiateAuthInput{
				ClientId: aws.String(clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
				AuthParameters: map[string]string{
					"USERNAME": "u", "PASSWORD": "Passw0rd!x", "SECRET_HASH": secretHash("u", clientID, secret),
				},
			})
			require.NoError(t, err)

			refresh := aws.ToString(auth.AuthenticationResult.RefreshToken)
			require.NotEmpty(t, refresh)

			got, err := client.GetTokensFromRefreshToken(ctx, &cognitoidpsdk.GetTokensFromRefreshTokenInput{
				ClientId: aws.String(
					clientID,
				), RefreshToken: aws.String(refresh), ClientSecret: aws.String(tt.secretSent),
			})
			if tt.wantErr {
				var na *types.NotAuthorizedException
				require.ErrorAs(t, err, &na)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(got.AuthenticationResult.AccessToken))
			assert.Equal(t, tt.wantRotated, aws.ToString(got.AuthenticationResult.RefreshToken) != "")

			_, err = client.GetTokensFromRefreshToken(ctx, &cognitoidpsdk.GetTokensFromRefreshTokenInput{
				ClientId: aws.String(clientID), RefreshToken: aws.String(refresh), ClientSecret: aws.String(secret),
			})
			assert.Equal(
				t,
				tt.wantRotated,
				err != nil,
				"old refresh token is rotated out only when rotation is enabled",
			)
		})
	}
}

func TestSDK_RevokeTokenChecksClientSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		secret  string
		wantErr bool
	}{
		{name: "right_secret", secret: "s3cret-value-1234567"},
		{name: "wrong_secret", secret: "nope", wantErr: true},
		{name: "missing_secret", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
			require.NoError(t, err)

			app, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
				UserPoolId:   pool.UserPool.Id,
				ClientName:   aws.String("c"),
				ClientSecret: aws.String("s3cret-value-1234567"),
			})
			require.NoError(t, err)

			in := &cognitoidpsdk.RevokeTokenInput{ClientId: app.UserPoolClient.ClientId, Token: aws.String("unknown")}
			if tt.secret != "" {
				in.ClientSecret = aws.String(tt.secret)
			}

			_, err = client.RevokeToken(ctx, in)
			if tt.wantErr {
				var ua *types.UnauthorizedException
				require.ErrorAs(t, err, &ua)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_SignUpPassesClientMetadataAndValidationData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		viaAdm bool
	}{
		{name: "sign_up"},
		{name: "admin_create_user", viaAdm: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			inv := &fakeInvoker{}
			h.Backend.SetLambdaTriggerInvoker(inv)

			client := newTestCognitoIDPClient(t, h)
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
				PoolName: aws.String("p"),
				LambdaConfig: &types.LambdaConfigType{
					PreSignUp: aws.String("arn:aws:lambda:us-east-1:000000000000:function:pre"),
				},
			})
			require.NoError(t, err)

			app, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
				UserPoolId: pool.UserPool.Id, ClientName: aws.String("c"),
			})
			require.NoError(t, err)

			validation := []types.AttributeType{{Name: aws.String("captcha"), Value: aws.String("ok")}}
			meta := map[string]string{"origin": "test"}

			if tt.viaAdm {
				_, err = client.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
					UserPoolId:     pool.UserPool.Id,
					Username:       aws.String("u"),
					ValidationData: validation,
					ClientMetadata: meta,
				})
			} else {
				_, err = client.SignUp(ctx, &cognitoidpsdk.SignUpInput{
					ClientId:       app.UserPoolClient.ClientId,
					Username:       aws.String("u"),
					Password:       aws.String("Passw0rd!x"),
					ValidationData: validation,
					ClientMetadata: meta,
				})
			}

			require.NoError(t, err)
			require.Equal(t, 1, inv.callCount())

			req, ok := inv.lastCall().event["request"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, map[string]any{"captcha": "ok"}, req["validationData"])
			assert.Equal(t, map[string]any{"origin": "test"}, req["clientMetadata"])
		})
	}
}

func TestPersistence_PoolAndClientSettingsRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := newTestBackend()

	pool, err := b.CreateUserPoolWithOpts("p", cognitoidp.UserPoolOptions{Settings: cognitoidp.PoolSettings{
		UserPoolTier:          "PLUS",
		AdminCreateUserConfig: map[string]any{"AllowAdminCreateUserOnly": true},
		UsernameAttributes:    []string{"email"},
	}})
	require.NoError(t, err)

	client, err := b.CreateUserPoolClientWithOpts(pool.ID, "c", cognitoidp.UserPoolClientOptions{
		AuthSessionValidity:  7,
		RefreshTokenRotation: map[string]any{"Feature": "ENABLED"},
	})
	require.NoError(t, err)

	restored := newTestBackend()
	require.NoError(t, restored.Restore(ctx, b.Snapshot(ctx)))

	gotPool, err := restored.DescribeUserPool(pool.ID)
	require.NoError(t, err)
	assert.Equal(t, "PLUS", gotPool.Settings.UserPoolTier)
	assert.Equal(t, true, gotPool.Settings.AdminCreateUserConfig["AllowAdminCreateUserOnly"])
	assert.Equal(t, []string{"email"}, gotPool.Settings.UsernameAttributes)

	gotClient, err := restored.DescribeUserPoolClient(pool.ID, client.ClientID)
	require.NoError(t, err)
	assert.Equal(t, int32(7), gotClient.AuthSessionValidity)
	assert.Equal(t, "ENABLED", gotClient.RefreshTokenRotation["Feature"])
}
