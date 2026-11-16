package bedrockagentcorecontrol_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
)

func syncConfig(url string, creds int) *types.SynchronizationConfiguration {
	cfg := &types.FromUrlSynchronizationConfiguration{Url: aws.String(url)}
	for range creds {
		cfg.CredentialProviderConfigurations = append(cfg.CredentialProviderConfigurations,
			types.RegistryRecordCredentialProviderConfiguration{
				CredentialProviderType: types.RegistryRecordCredentialProviderTypeIam,
				CredentialProvider: &types.RegistryRecordCredentialProviderUnionMemberIamCredentialProvider{
					Value: types.RegistryRecordIamCredentialProvider{Service: aws.String("execute-api")},
				},
			})
	}

	return &types.SynchronizationConfiguration{FromUrl: cfg}
}

func TestRegistryRecordSynchronization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		url     string
		wantErr string
		creds   int
		wantRec bool
	}{
		{name: "recorded", url: "https://mcp.example.com/server", wantRec: true},
		{name: "http rejected", url: "http://mcp.example.com/server", wantErr: "ValidationException"},
		{
			name: "two credentials rejected", url: "https://mcp.example.com/server", creds: 2,
			wantErr: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ctx := t.Context()

			reg, err := f.client.CreateRegistry(ctx, &sdk.CreateRegistryInput{Name: aws.String("catalog")})
			require.NoError(t, err)

			regID := regIDOf(aws.ToString(reg.RegistryArn))

			rec, err := f.client.CreateRegistryRecord(ctx, &sdk.CreateRegistryRecordInput{
				RegistryId: aws.String(regID), Name: aws.String("srv"), DescriptorType: types.DescriptorTypeMcp,
				SynchronizationType:          types.SynchronizationTypeUrl,
				SynchronizationConfiguration: syncConfig(tt.url, tt.creds),
			})
			if tt.wantErr != "" {
				assert.Equal(t, tt.wantErr, apiCode(err))

				return
			}

			require.NoError(t, err)

			recID := regIDOf(aws.ToString(rec.RecordArn))
			got, ok := f.backend.RecordSynchronization(recID)
			require.Equal(t, tt.wantRec, ok)
			assert.Equal(t, tt.url, got["url"])
			assert.Equal(t, "configuration", got["reason"])

			_, err = f.client.UpdateRegistryRecord(ctx, &sdk.UpdateRegistryRecordInput{
				RegistryId: aws.String(regID), RecordId: aws.String(recID), TriggerSynchronization: aws.Bool(true),
			})
			require.NoError(t, err)

			got, _ = f.backend.RecordSynchronization(recID)
			assert.Equal(t, "trigger", got["reason"])

			peer := bedrockagentcorecontrol.NewInMemoryBackend(testAccount, "us-east-1")
			require.NoError(t, peer.Restore(ctx, f.backend.Snapshot(ctx)))

			again, ok := peer.RecordSynchronization(recID)
			require.True(t, ok)
			assert.Equal(t, "trigger", again["reason"])
		})
	}
}

func TestRegistryRecordTriggerNeedsConfiguration(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	reg, err := f.client.CreateRegistry(ctx, &sdk.CreateRegistryInput{Name: aws.String("catalog")})
	require.NoError(t, err)

	regID := regIDOf(aws.ToString(reg.RegistryArn))

	rec, err := f.client.CreateRegistryRecord(ctx, &sdk.CreateRegistryRecordInput{
		RegistryId: aws.String(regID), Name: aws.String("plain"), DescriptorType: types.DescriptorTypeCustom,
	})
	require.NoError(t, err)

	_, err = f.client.UpdateRegistryRecord(ctx, &sdk.UpdateRegistryRecordInput{
		RegistryId: aws.String(regID), RecordId: aws.String(regIDOf(aws.ToString(rec.RecordArn))),
		TriggerSynchronization: aws.Bool(true),
	})
	assert.Equal(t, "ValidationException", apiCode(err))
}
