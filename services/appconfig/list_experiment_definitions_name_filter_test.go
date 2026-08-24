package appconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListExperimentDefinitions_NameFormFilterWithoutApplicationIdentifier
// drives ListExperimentDefinitions through the real aws-sdk-go-v2 client
// with a name-form ConfigurationProfileIdentifier/EnvironmentIdentifier and
// no ApplicationIdentifier. ListExperimentDefinitionsInput documents all
// three identifiers as independently resolvable "ID or name"
// (api_op_ListExperimentDefinitions.go) -- there is no stated dependency
// requiring ApplicationIdentifier to resolve the other two by name. Before
// this fix, a name-form configuration_profile_identifier/
// environment_identifier filter was compared literally against the ID
// field only when application_identifier was absent, so it silently
// matched nothing (see the "PARTIAL (unrelated, pre-existing)" note this
// pass corrects in PARITY.md).
func TestListExperimentDefinitions_NameFormFilterWithoutApplicationIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestAppConfigClient(t, h)
	ctx := t.Context()

	app, err := client.CreateApplication(ctx, &appconfigsdk.CreateApplicationInput{
		Name: aws.String("name-filter-app"),
	})
	require.NoError(t, err)

	env, err := client.CreateEnvironment(ctx, &appconfigsdk.CreateEnvironmentInput{
		ApplicationId: app.Id,
		Name:          aws.String("name-filter-env"),
	})
	require.NoError(t, err)

	prof, err := client.CreateConfigurationProfile(ctx, &appconfigsdk.CreateConfigurationProfileInput{
		ApplicationId: app.Id,
		Name:          aws.String("name-filter-profile"),
		LocationUri:   aws.String("hosted"),
		Type:          aws.String("AWS.AppConfig.FeatureFlags"),
	})
	require.NoError(t, err)

	def, err := client.CreateExperimentDefinition(ctx, &appconfigsdk.CreateExperimentDefinitionInput{
		ApplicationIdentifier:          app.Id,
		Name:                           aws.String("name-filter-def"),
		AudienceRule:                   aws.String("true"),
		FlagKey:                        aws.String("flag1"),
		EnvironmentIdentifier:          env.Id,
		ConfigurationProfileIdentifier: prof.Id,
		Control: &types.TreatmentInput{
			FlagValue: &types.FlagValue{Enabled: false},
			Weight:    50,
		},
		Treatments: []types.TreatmentInput{
			{FlagValue: &types.FlagValue{Enabled: true}, Weight: 50},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, def.Id)

	tests := []struct {
		input   *appconfigsdk.ListExperimentDefinitionsInput
		name    string
		wantLen int
	}{
		{
			name: "profile by name no app id",
			input: &appconfigsdk.ListExperimentDefinitionsInput{
				ConfigurationProfileIdentifier: aws.String("name-filter-profile"),
			},
			wantLen: 1,
		},
		{
			name: "env by name no app id",
			input: &appconfigsdk.ListExperimentDefinitionsInput{
				EnvironmentIdentifier: aws.String("name-filter-env"),
			},
			wantLen: 1,
		},
		{
			name: "non-matching name still no matches",
			input: &appconfigsdk.ListExperimentDefinitionsInput{
				ConfigurationProfileIdentifier: aws.String("no-such-profile"),
			},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, listErr := client.ListExperimentDefinitions(ctx, tt.input)
			require.NoError(t, listErr)
			require.Len(t, out.Items, tt.wantLen)

			if tt.wantLen > 0 {
				assert.Equal(t, aws.ToString(def.Id), aws.ToString(out.Items[0].Id))
			}
		})
	}
}
