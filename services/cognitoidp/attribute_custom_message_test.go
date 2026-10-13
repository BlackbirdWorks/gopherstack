package cognitoidp_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

type attrFixture struct {
	h           *cognitoidp.Handler
	inv         *fakeInvoker
	client      *cognitoidpsdk.Client
	poolID      string
	clientID    string
	accessToken string
}

func newAttrFixture(t *testing.T, requireVerify bool) attrFixture {
	t.Helper()

	h := newTestHandler(t)
	inv := &fakeInvoker{respond: func(_ string, _ map[string]any) (map[string]any, error) {
		return map[string]any{"emailMessage": "code ####", "emailSubject": "subj"}, nil
	}}
	h.Backend.SetLambdaTriggerInvoker(inv)

	client := newTestCognitoIDPClient(t, h)

	in := &cognitoidpsdk.CreateUserPoolInput{
		PoolName:               aws.String("attr-pool"),
		AutoVerifiedAttributes: []types.VerifiedAttributeType{types.VerifiedAttributeTypeEmail},
		LambdaConfig: &types.LambdaConfigType{
			CustomMessage: aws.String("arn:aws:lambda:us-east-1:000000000000:function:cm"),
		},
	}
	if requireVerify {
		in.UserAttributeUpdateSettings = &types.UserAttributeUpdateSettingsType{
			AttributesRequireVerificationBeforeUpdate: []types.VerifiedAttributeType{types.VerifiedAttributeTypeEmail},
		}
	}

	pool, err := client.CreateUserPool(t.Context(), in)
	require.NoError(t, err)

	poolID := aws.ToString(pool.UserPool.Id)

	app, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("c"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
	})
	require.NoError(t, err)

	_, err = client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("u"),
		UserAttributes: []types.AttributeType{
			{Name: aws.String("email"), Value: aws.String("old@example.com")},
			{Name: aws.String("email_verified"), Value: aws.String("true")},
		},
	})
	require.NoError(t, err)

	_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(
			poolID,
		),
		Username:  aws.String("u"),
		Password:  aws.String(deviceTestPassword),
		Permanent: true,
	})
	require.NoError(t, err)

	clientID := aws.ToString(app.UserPoolClient.ClientId)

	auth, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		AuthFlow: types.AuthFlowTypeUserPasswordAuth, ClientId: aws.String(clientID),
		AuthParameters: map[string]string{"USERNAME": "u", "PASSWORD": deviceTestPassword},
	})
	require.NoError(t, err)

	return attrFixture{
		h: h, inv: inv, client: client, poolID: poolID, clientID: clientID,
		accessToken: aws.ToString(auth.AuthenticationResult.AccessToken),
	}
}

func (f attrFixture) userAttrs(t *testing.T) map[string]string {
	t.Helper()

	u, err := f.client.AdminGetUser(t.Context(), &cognitoidpsdk.AdminGetUserInput{
		UserPoolId: aws.String(f.poolID), Username: aws.String("u"),
	})
	require.NoError(t, err)

	out := map[string]string{}
	for _, a := range u.UserAttributes {
		out[aws.ToString(a.Name)] = aws.ToString(a.Value)
	}

	return out
}

func TestUpdateUserAttributes_CustomMessageAndVerification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantEmailNow  string
		requireVerify bool
	}{
		{name: "auto_verify_resets_flag", requireVerify: false, wantEmailNow: "new@example.com"},
		{name: "verify_before_update_holds_value", requireVerify: true, wantEmailNow: "old@example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newAttrFixture(t, tt.requireVerify)

			rec := doCognitoRequest(t, f.h, "UpdateUserAttributes", map[string]any{
				"AccessToken":    f.accessToken,
				"UserAttributes": []map[string]string{{"Name": "email", "Value": "new@example.com"}},
				"ClientMetadata": map[string]string{"tenant": "acme"},
			})
			require.Equal(t, 200, rec.Code, rec.Body.String())

			var resp struct {
				CodeDeliveryDetailsList []map[string]string `json:"CodeDeliveryDetailsList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.CodeDeliveryDetailsList, 1)

			d := resp.CodeDeliveryDetailsList[0]
			assert.Equal(t, "EMAIL", d["DeliveryMedium"])
			assert.Equal(t, "email", d["AttributeName"])
			assert.Equal(t, "subj", d["CustomMessageSubject"])
			assert.Equal(t, "code "+d["ConfirmationCode"], d["CustomMessage"])

			assert.Equal(
				t,
				"acme",
				metadataOf(t, f.inv, "CustomMessage_UpdateUserAttribute", "clientMetadata")["tenant"],
			)

			attrs := f.userAttrs(t)
			assert.Equal(t, tt.wantEmailNow, attrs["email"])
			assert.Equal(t, map[bool]string{true: "true", false: "false"}[tt.requireVerify], attrs["email_verified"])

			_, err := f.client.VerifyUserAttribute(t.Context(), &cognitoidpsdk.VerifyUserAttributeInput{
				AccessToken: aws.String(
					f.accessToken,
				),
				AttributeName: aws.String("email"),
				Code:          aws.String(d["ConfirmationCode"]),
			})
			require.NoError(t, err)

			attrs = f.userAttrs(t)
			assert.Equal(t, "new@example.com", attrs["email"])
			assert.Equal(t, "true", attrs["email_verified"])
		})
	}
}

func TestUpdateUserAttributes_VerifiedTrueSkipsMessage(t *testing.T) {
	t.Parallel()

	f := newAttrFixture(t, true)

	out, err := f.client.UpdateUserAttributes(t.Context(), &cognitoidpsdk.UpdateUserAttributesInput{
		AccessToken: aws.String(f.accessToken),
		UserAttributes: []types.AttributeType{
			{Name: aws.String("email"), Value: aws.String("skip@example.com")},
			{Name: aws.String("email_verified"), Value: aws.String("true")},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, out.CodeDeliveryDetailsList)
	assert.Equal(t, "skip@example.com", f.userAttrs(t)["email"])
}

func TestAdminUpdateUserAttributes_FiresCustomMessage(t *testing.T) {
	t.Parallel()

	f := newAttrFixture(t, false)

	_, err := f.client.AdminUpdateUserAttributes(t.Context(), &cognitoidpsdk.AdminUpdateUserAttributesInput{
		UserPoolId: aws.String(f.poolID), Username: aws.String("u"),
		UserAttributes: []types.AttributeType{{Name: aws.String("email"), Value: aws.String("admin@example.com")}},
		ClientMetadata: map[string]string{"by": "admin"},
	})
	require.NoError(t, err)

	assert.Equal(t, "admin", metadataOf(t, f.inv, "CustomMessage_UpdateUserAttribute", "clientMetadata")["by"])
	assert.Equal(t, "false", f.userAttrs(t)["email_verified"])
}

func TestGetUserAttributeVerificationCode_FiresCustomMessage(t *testing.T) {
	t.Parallel()

	f := newAttrFixture(t, false)

	rec := doCognitoRequest(t, f.h, "GetUserAttributeVerificationCode", map[string]any{
		"AccessToken": f.accessToken, "AttributeName": "email",
		"ClientMetadata": map[string]string{"k": "v"},
	})
	require.Equal(t, 200, rec.Code, rec.Body.String())

	var resp struct {
		CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "subj", resp.CodeDeliveryDetails["CustomMessageSubject"])
	assert.Equal(t, "v", metadataOf(t, f.inv, "CustomMessage_VerifyUserAttribute", "clientMetadata")["k"])
}

func TestAdminResetUserPassword_FiresCustomMessageAndIssuesCode(t *testing.T) {
	t.Parallel()

	f := newAttrFixture(t, false)

	_, err := f.client.AdminResetUserPassword(t.Context(), &cognitoidpsdk.AdminResetUserPasswordInput{
		UserPoolId: aws.String(f.poolID), Username: aws.String("u"), ClientMetadata: map[string]string{"why": "audit"},
	})
	require.NoError(t, err)

	assert.Equal(t, "audit", metadataOf(t, f.inv, "CustomMessage_ForgotPassword", "clientMetadata")["why"])

	u, err := f.h.Backend.AdminGetUser(f.poolID, "u")
	require.NoError(t, err)
	assert.NotEmpty(t, u.ConfirmCode)
}
