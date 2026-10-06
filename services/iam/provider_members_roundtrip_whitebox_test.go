package iam

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPEMKey = "-----BEGIN PRIVATE KEY-----\nMIIBVQIBADANBgkqhkiG9w0BAQEF\n-----END PRIVATE KEY-----\n"

func TestSAMLProvider_EncryptionTagsRoundTrip(t *testing.T) {
	t.Parallel()

	const meta = `<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"/>`

	tests := []struct {
		name        string
		createMode  types.AssertionEncryptionModeType
		updateMode  types.AssertionEncryptionModeType
		wantMode    types.AssertionEncryptionModeType
		addOnUpdate bool
		wantKeys    int
	}{
		{
			name:       "create_with_key",
			createMode: types.AssertionEncryptionModeTypeAllowed,
			wantMode:   "Allowed",
			wantKeys:   1,
		},
		{
			name: "update_mode_and_add_key", createMode: types.AssertionEncryptionModeTypeAllowed,
			updateMode: types.AssertionEncryptionModeTypeRequired, wantMode: "Required", addOnUpdate: true, wantKeys: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newSigningCertTestClient(t, NewHandler(NewInMemoryBackend()))

			created, err := client.CreateSAMLProvider(ctx, &iamsdk.CreateSAMLProviderInput{
				Name:                    aws.String("idp"),
				SAMLMetadataDocument:    aws.String(meta),
				AssertionEncryptionMode: tt.createMode,
				AddPrivateKey:           aws.String(testPEMKey),
				Tags:                    []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
			})
			require.NoError(t, err)

			if tt.addOnUpdate || tt.updateMode != "" {
				upd := &iamsdk.UpdateSAMLProviderInput{
					SAMLProviderArn:         created.SAMLProviderArn,
					AssertionEncryptionMode: tt.updateMode,
				}
				if tt.addOnUpdate {
					upd.AddPrivateKey = aws.String(testPEMKey)
				}

				_, err = client.UpdateSAMLProvider(ctx, upd)
				require.NoError(t, err)
			}

			got, err := client.GetSAMLProvider(
				ctx,
				&iamsdk.GetSAMLProviderInput{SAMLProviderArn: created.SAMLProviderArn},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantMode, got.AssertionEncryptionMode)
			assert.NotEmpty(t, aws.ToString(got.SAMLProviderUUID))
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			require.Len(t, got.PrivateKeyList, tt.wantKeys)

			if tt.wantKeys == 2 {
				removed := aws.ToString(got.PrivateKeyList[0].KeyId)
				_, err = client.UpdateSAMLProvider(ctx, &iamsdk.UpdateSAMLProviderInput{
					SAMLProviderArn: created.SAMLProviderArn, RemovePrivateKey: aws.String(removed),
				})
				require.NoError(t, err)

				got, err = client.GetSAMLProvider(
					ctx,
					&iamsdk.GetSAMLProviderInput{SAMLProviderArn: created.SAMLProviderArn},
				)
				require.NoError(t, err)
				require.Len(t, got.PrivateKeyList, 1)
				assert.NotEqual(t, removed, aws.ToString(got.PrivateKeyList[0].KeyId))
			}
		})
	}
}

func TestSAMLProvider_EncryptionRejectsInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key  *string
		name string
		mode types.AssertionEncryptionModeType
	}{
		{name: "bad_mode", mode: "Sometimes"},
		{name: "bad_pem", key: aws.String("not a pem")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSigningCertTestClient(t, NewHandler(NewInMemoryBackend()))
			_, err := client.CreateSAMLProvider(t.Context(), &iamsdk.CreateSAMLProviderInput{
				Name:                    aws.String("idp"),
				SAMLMetadataDocument:    aws.String(`<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"/>`),
				AssertionEncryptionMode: tt.mode,
				AddPrivateKey:           tt.key,
			})
			require.Error(t, err)
		})
	}
}

func TestOIDCProvider_GetEchoesTags(t *testing.T) {
	t.Parallel()

	client := newSigningCertTestClient(t, NewHandler(NewInMemoryBackend()))
	created, err := client.CreateOpenIDConnectProvider(t.Context(), &iamsdk.CreateOpenIDConnectProviderInput{
		Url:            aws.String("https://oidc.example.com"),
		ThumbprintList: []string{"0123456789abcdef0123456789abcdef01234567"},
		Tags:           []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	require.NoError(t, err)

	got, err := client.GetOpenIDConnectProvider(t.Context(), &iamsdk.GetOpenIDConnectProviderInput{
		OpenIDConnectProviderArn: created.OpenIDConnectProviderArn,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "dev", aws.ToString(got.Tags[0].Value))
}

func TestServiceSpecificCredential_CredentialAgeDays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		days    *int32
		name    string
		wantExp bool
		wantErr bool
	}{
		{name: "none", days: nil},
		{name: "thirty", days: aws.Int32(30), wantExp: true},
		{name: "zero_rejected", days: aws.Int32(0), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newSigningCertTestClient(t, NewHandler(NewInMemoryBackend()))
			_, err := client.CreateUser(ctx, &iamsdk.CreateUserInput{UserName: aws.String("u1")})
			require.NoError(t, err)

			out, err := client.CreateServiceSpecificCredential(ctx, &iamsdk.CreateServiceSpecificCredentialInput{
				UserName: aws.String(
					"u1",
				), ServiceName: aws.String("bedrock.amazonaws.com"), CredentialAgeDays: tt.days,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			list, err := client.ListServiceSpecificCredentials(ctx, &iamsdk.ListServiceSpecificCredentialsInput{
				UserName: aws.String("u1"),
			})
			require.NoError(t, err)
			require.Len(t, list.ServiceSpecificCredentials, 1)

			exps := []*time.Time{
				out.ServiceSpecificCredential.ExpirationDate,
				list.ServiceSpecificCredentials[0].ExpirationDate,
			}

			for _, exp := range exps {
				if !tt.wantExp {
					assert.Nil(t, exp)

					continue
				}

				require.NotNil(t, exp)
				assert.WithinDuration(t, *exp, time.Now().Add(30*24*time.Hour), time.Minute)
			}
		})
	}
}
