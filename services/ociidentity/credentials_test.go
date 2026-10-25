package ociidentity_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIKeys(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	u := e.mkUser(t, "keyed")

	kps := make([]keyPair, 4)
	for i := range kps {
		kps[i] = newKeyPair(t)
	}

	upload := func(pem string) (identity.UploadApiKeyResponse, error) {
		return e.client.UploadApiKey(ctx, identity.UploadApiKeyRequest{
			UserId: u.Id, Key: new(pem),
		})
	}

	r, err := upload(kps[0].pubPEM)
	require.NoError(t, err)
	assert.Equal(t, kps[0].fprint, *r.Fingerprint)
	assert.Equal(t, testTenancy+"/"+*u.Id+"/"+kps[0].fprint, *r.KeyId)
	assert.Equal(t, identity.ApiKeyLifecycleStateActive, r.LifecycleState)

	_, err = upload(kps[0].pubPEM)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = upload("not a pem")
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	_, err = upload(kps[1].pubPEM)
	require.NoError(t, err)

	_, err = upload(kps[2].pubPEM)
	require.NoError(t, err)

	_, err = upload(kps[3].pubPEM)
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

	lst, err := e.client.ListApiKeys(ctx, identity.ListApiKeysRequest{UserId: u.Id})
	require.NoError(t, err)
	require.Len(t, lst.Items, 3)
	assert.Equal(t, kps[0].pubPEM, *lst.Items[0].KeyValue)

	_, err = e.client.DeleteApiKey(ctx, identity.DeleteApiKeyRequest{UserId: u.Id, Fingerprint: new(kps[1].fprint)})
	require.NoError(t, err)

	_, err = e.client.DeleteApiKey(ctx, identity.DeleteApiKeyRequest{UserId: u.Id, Fingerprint: new(kps[1].fprint)})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = upload(kps[3].pubPEM)
	require.NoError(t, err)

	_, err = e.client.UploadApiKey(ctx, identity.UploadApiKeyRequest{
		UserId: new("ocid1.user.oc1..none"), Key: new(kps[0].pubPEM),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestSecretCredentials(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	u := e.mkUser(t, "secrets")

	t.Run("auth tokens", func(t *testing.T) {
		t.Parallel()

		c1, err := e.client.CreateAuthToken(ctx, identity.CreateAuthTokenRequest{
			UserId: u.Id, Description: new("ci"),
		})
		require.NoError(t, err)
		require.NotNil(t, c1.Token)
		assert.Len(t, *c1.Token, 20)

		_, err = e.client.CreateAuthToken(ctx, identity.CreateAuthTokenRequest{
			UserId: u.Id, Description: new("two"),
		})
		require.NoError(t, err)

		_, err = e.client.CreateAuthToken(ctx, identity.CreateAuthTokenRequest{
			UserId: u.Id, Description: new("three"),
		})
		assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

		lst, err := e.client.ListAuthTokens(ctx, identity.ListAuthTokensRequest{UserId: u.Id})
		require.NoError(t, err)
		require.Len(t, lst.Items, 2)

		for _, it := range lst.Items {
			assert.Nil(t, it.Token, "the secret is shown once")
		}

		up, err := e.client.UpdateAuthToken(ctx, identity.UpdateAuthTokenRequest{
			UserId:      u.Id,
			AuthTokenId: c1.Id,
			Description: new("renamed"),
		})
		require.NoError(t, err)
		assert.Equal(t, "renamed", *up.Description)
		assert.Nil(t, up.Token)

		_, err = e.client.DeleteAuthToken(ctx, identity.DeleteAuthTokenRequest{UserId: u.Id, AuthTokenId: c1.Id})
		require.NoError(t, err)

		_, err = e.client.DeleteAuthToken(ctx, identity.DeleteAuthTokenRequest{UserId: u.Id, AuthTokenId: c1.Id})
		assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

		_, err = e.client.CreateAuthToken(ctx, identity.CreateAuthTokenRequest{
			UserId: u.Id, Description: new(" "),
		})
		assert.Equal(t, "MissingParameter", svcErr(t, err).GetCode())
	})

	t.Run("customer secret keys", func(t *testing.T) {
		t.Parallel()

		c1, err := e.client.CreateCustomerSecretKey(ctx, identity.CreateCustomerSecretKeyRequest{
			UserId:      u.Id,
			DisplayName: new("s3"),
		})
		require.NoError(t, err)
		require.NotNil(t, c1.Key)
		assert.Len(t, *c1.Key, 44)
		assert.Len(t, *c1.Id, 40)

		lst, err := e.client.ListCustomerSecretKeys(ctx, identity.ListCustomerSecretKeysRequest{UserId: u.Id})
		require.NoError(t, err)
		require.Len(t, lst.Items, 1)
		assert.Equal(t, "s3", *lst.Items[0].DisplayName)

		up, err := e.client.UpdateCustomerSecretKey(ctx, identity.UpdateCustomerSecretKeyRequest{
			UserId: u.Id, CustomerSecretKeyId: c1.Id,
			DisplayName: new("s3b"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s3b", *up.DisplayName)

		_, err = e.client.CreateCustomerSecretKey(ctx, identity.CreateCustomerSecretKeyRequest{
			UserId:      u.Id,
			DisplayName: new("two"),
		})
		require.NoError(t, err)

		_, err = e.client.CreateCustomerSecretKey(ctx, identity.CreateCustomerSecretKeyRequest{
			UserId:      u.Id,
			DisplayName: new("three"),
		})
		assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())

		_, err = e.client.DeleteCustomerSecretKey(ctx, identity.DeleteCustomerSecretKeyRequest{
			UserId: u.Id, CustomerSecretKeyId: c1.Id,
		})
		require.NoError(t, err)
	})

	t.Run("smtp credentials", func(t *testing.T) {
		t.Parallel()

		c1, err := e.client.CreateSmtpCredential(ctx, identity.CreateSmtpCredentialRequest{
			UserId: u.Id, Description: new("mail"),
		})
		require.NoError(t, err)
		require.NotNil(t, c1.Password)
		assert.Contains(t, *c1.Username, *u.Id+"@"+testTenancy)

		lst, err := e.client.ListSmtpCredentials(ctx, identity.ListSmtpCredentialsRequest{UserId: u.Id})
		require.NoError(t, err)
		require.Len(t, lst.Items, 1)
		assert.Equal(t, *c1.Username, *lst.Items[0].Username)

		_, err = e.client.UpdateSmtpCredential(ctx, identity.UpdateSmtpCredentialRequest{
			UserId: u.Id, SmtpCredentialId: c1.Id,
			Description: new("mail2"),
		})
		require.NoError(t, err)

		_, err = e.client.DeleteSmtpCredential(
			ctx,
			identity.DeleteSmtpCredentialRequest{UserId: u.Id, SmtpCredentialId: c1.Id},
		)
		require.NoError(t, err)

		_, err = e.client.CreateSmtpCredential(ctx, identity.CreateSmtpCredentialRequest{
			UserId:      new("ocid1.user.oc1..none"),
			Description: new("x"),
		})
		assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
	})

	t.Run("ui password", func(t *testing.T) {
		t.Parallel()

		other := e.mkUser(t, "console")

		_, err := e.client.GetUserUIPasswordInformation(
			ctx,
			identity.GetUserUIPasswordInformationRequest{UserId: other.Id},
		)
		assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

		r1, err := e.client.CreateOrResetUIPassword(ctx, identity.CreateOrResetUIPasswordRequest{UserId: other.Id})
		require.NoError(t, err)
		require.NotNil(t, r1.Password)
		assert.Len(t, *r1.Password, 16)

		r2, err := e.client.CreateOrResetUIPassword(ctx, identity.CreateOrResetUIPasswordRequest{UserId: other.Id})
		require.NoError(t, err)
		assert.NotEqual(t, *r1.Password, *r2.Password)

		info, err := e.client.GetUserUIPasswordInformation(
			ctx,
			identity.GetUserUIPasswordInformationRequest{UserId: other.Id},
		)
		require.NoError(t, err)
		assert.Equal(t, *other.Id, *info.UserId)
		assert.Equal(t, identity.UiPasswordInformationLifecycleStateActive, info.LifecycleState)
	})
}
