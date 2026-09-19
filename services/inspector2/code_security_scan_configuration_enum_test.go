package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_CreateCodeSecurityScanConfiguration_EnumValidation drives
// CreateCodeSecurityScanConfiguration through a real aws-sdk-go-v2 client with
// invalid enum values on ScopeSettings.ProjectSelectionScope,
// PeriodicScanConfiguration.Frequency, and
// ContinuousIntegrationScanConfiguration.SupportedEvents -- each real,
// documented, enum-constrained (types.ProjectSelectionScope/
// PeriodicScanFrequency/ContinuousIntegrationScanEvent, inspector2 SDK
// enums.go) -- and asserts ValidationException.
func TestRealClient_CreateCodeSecurityScanConfiguration_EnumValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		config  *types.CodeSecurityScanConfiguration
		scope   *types.ScopeSettings
		name    string
		wantErr string
	}{
		{
			name:  "invalid project selection scope",
			scope: &types.ScopeSettings{ProjectSelectionScope: "SPECIFIC"},
			config: &types.CodeSecurityScanConfiguration{
				RuleSetCategories: []types.RuleSetCategory{types.RuleSetCategorySast},
			},
			wantErr: "ValidationException",
		},
		{
			name: "invalid periodic scan frequency",
			config: &types.CodeSecurityScanConfiguration{
				RuleSetCategories:         []types.RuleSetCategory{types.RuleSetCategorySast},
				PeriodicScanConfiguration: &types.PeriodicScanConfiguration{Frequency: "DAILY"},
			},
			wantErr: "ValidationException",
		},
		{
			name: "invalid continuous integration supported event",
			config: &types.CodeSecurityScanConfiguration{
				RuleSetCategories: []types.RuleSetCategory{types.RuleSetCategorySast},
				ContinuousIntegrationScanConfiguration: &types.ContinuousIntegrationScanConfiguration{
					SupportedEvents: []types.ContinuousIntegrationScanEvent{"MERGE"},
				},
			},
			wantErr: "ValidationException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, client := newRealClient(t)

			_, err := client.CreateCodeSecurityScanConfiguration(
				t.Context(), &inspector2sdk.CreateCodeSecurityScanConfigurationInput{
					Name:          aws.String("code-scan-config"),
					Level:         types.ConfigurationLevelAccount,
					Configuration: tc.config,
					ScopeSettings: tc.scope,
				},
			)

			require.Error(t, err)
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// TestRealClient_CreateCodeSecurityScanConfiguration_ValidEnums confirms the
// real, legal enum values still round-trip successfully.
func TestRealClient_CreateCodeSecurityScanConfiguration_ValidEnums(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)

	out, err := client.CreateCodeSecurityScanConfiguration(
		t.Context(), &inspector2sdk.CreateCodeSecurityScanConfigurationInput{
			Name:  aws.String("code-scan-config"),
			Level: types.ConfigurationLevelAccount,
			Configuration: &types.CodeSecurityScanConfiguration{
				RuleSetCategories: []types.RuleSetCategory{types.RuleSetCategorySast},
				PeriodicScanConfiguration: &types.PeriodicScanConfiguration{
					Frequency: types.PeriodicScanFrequencyWeekly,
				},
				ContinuousIntegrationScanConfiguration: &types.ContinuousIntegrationScanConfiguration{
					SupportedEvents: []types.ContinuousIntegrationScanEvent{
						types.ContinuousIntegrationScanEventPullRequest,
					},
				},
			},
			ScopeSettings: &types.ScopeSettings{ProjectSelectionScope: types.ProjectSelectionScopeAll},
		},
	)

	require.NoError(t, err)
	require.NotNil(t, out.ScanConfigurationArn)
}
