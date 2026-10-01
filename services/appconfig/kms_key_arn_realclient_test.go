package appconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
)

type kmsSiblings struct{ h *kmsbackend.Handler }

func (s *kmsSiblings) GetKMSHandler() service.Registerable { return s.h }

// TestRealClient_KmsKeyArnResolved proves KmsKeyArn is resolved from KmsKeyIdentifier on profiles and hosted versions.
func TestRealClient_KmsKeyArnResolved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		identifier func(keyID, keyArn string) string
		wantArn    func(keyArn string) string
		name       string
	}{
		{func(id, _ string) string { return id }, func(a string) string { return a }, "key_id"},
		{func(_, a string) string { return a }, func(a string) string { return a }, "key_arn"},
		{func(_, _ string) string { return "alias/appcfg" }, func(a string) string { return a }, "alias"},
		{func(_, _ string) string { return "no-such-key" }, func(string) string { return "" }, "unknown_key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kms := kmsbackend.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			key, err := kms.CreateKey(t.Context(), &kmsbackend.CreateKeyInput{})
			require.NoError(t, err)
			require.NoError(t, kms.CreateAlias(t.Context(), &kmsbackend.CreateAliasInput{
				AliasName: "alias/appcfg", TargetKeyID: key.KeyMetadata.KeyID,
			}))

			backend := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
			backend.SetAppConfig(&kmsSiblings{h: kmsbackend.NewHandler(kms)})
			client := newTestAppConfigClient(t, appconfig.NewHandler(backend))
			ctx := t.Context()

			app, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{Name: aws.String("a")})
			require.NoError(t, err)

			ident := tt.identifier(key.KeyMetadata.KeyID, key.KeyMetadata.Arn)
			want := tt.wantArn(key.KeyMetadata.Arn)

			created, err := client.CreateConfigurationProfile(ctx, &appconfigsdk.CreateConfigurationProfileInput{
				ApplicationId:    app.Id,
				Name:             aws.String("p"),
				LocationUri:      aws.String("hosted"),
				KmsKeyIdentifier: aws.String(ident),
			})
			require.NoError(t, err)
			assert.Equal(t, want, aws.ToString(created.KmsKeyArn))
			assert.Equal(t, ident, aws.ToString(created.KmsKeyIdentifier))

			got, err := client.GetConfigurationProfile(ctx, &appconfigsdk.GetConfigurationProfileInput{
				ApplicationId: app.Id, ConfigurationProfileId: created.Id,
			})
			require.NoError(t, err)
			assert.Equal(t, want, aws.ToString(got.KmsKeyArn))

			verIn := &appconfigsdk.CreateHostedConfigurationVersionInput{
				ApplicationId: app.Id, ConfigurationProfileId: created.Id,
				Content: []byte("{}"), ContentType: aws.String("application/json"),
			}
			ver, err := client.CreateHostedConfigurationVersion(ctx, verIn)
			require.NoError(t, err)
			assert.Equal(t, want, aws.ToString(ver.KmsKeyArn))

			list, err := client.ListHostedConfigurationVersions(ctx, &appconfigsdk.ListHostedConfigurationVersionsInput{
				ApplicationId: app.Id, ConfigurationProfileId: created.Id,
			})
			require.NoError(t, err)
			require.Len(t, list.Items, 1)
			assert.Equal(t, want, aws.ToString(list.Items[0].KmsKeyArn))
		})
	}
}
