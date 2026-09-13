package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// TestHostedConfigurationVersionContentSurvivesRestore reproduces
// gopherstack-zxdex: HostedConfigurationVersion.Content carried json:"-",
// so store.Table's snapshotJSON (which marshals the struct directly, since
// hostedConfigVersions registers straight on the registry with no DTO
// twin) dropped it from every persisted snapshot. Content is this
// resource's payload -- GetHostedConfigurationVersion serves it as the
// response body -- so a restart silently emptied every version created
// before it.
func TestHostedConfigurationVersionContentSurvivesRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		content     []byte
	}{
		{
			name:        "feature flag json",
			contentType: "application/json",
			content: []byte(
				`{"version":"1","flags":{"launch-banner":{"name":"launch-banner"}},` +
					`"values":{"launch-banner":{"enabled":true}}}`,
			),
		},
		{
			name:        "arbitrary non-json bytes",
			contentType: "application/octet-stream",
			content:     []byte{0x00, 0x01, 0xFF, 0xFE, 'h', 'i', 0x00, 0x7F},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

			app, err := original.CreateApplication("content-app", "", nil)
			require.NoError(t, err)

			profile, err := original.CreateConfigurationProfile(
				app.ID, "content-profile", "", "hosted", "AWS.Freeform", "", "", nil, nil,
			)
			require.NoError(t, err)

			hcv, err := original.CreateHostedConfigurationVersion(
				app.ID, profile.ID, tt.contentType, "", "", tt.content, nil,
			)
			require.NoError(t, err)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			got, err := fresh.GetHostedConfigurationVersion(app.ID, profile.ID, hcv.VersionNumber)
			require.NoError(t, err)
			assert.Equal(t, tt.content, got.Content, "payload must survive Snapshot/Restore byte-for-byte")
			assert.Equal(t, tt.contentType, got.ContentType)
		})
	}
}

// TestDeployedConfigurationContentSurvivesRestore verifies that
// GetConfiguration -- which derives its response from the deployed
// HostedConfigurationVersion via deployedConfigVersionLocked -- keeps
// serving real content after a Snapshot/Restore round trip, not just the
// direct Get path above.
func TestDeployedConfigurationContentSurvivesRestore(t *testing.T) {
	t.Parallel()

	content := []byte(`{"feature":"on"}`)

	original := appconfig.NewInMemoryBackend("123456789012", "us-east-1")

	app, err := original.CreateApplication("deploy-content-app", "", nil)
	require.NoError(t, err)

	env, err := original.CreateEnvironment(app.ID, "deploy-content-env", "", nil, nil)
	require.NoError(t, err)

	profile, err := original.CreateConfigurationProfile(
		app.ID, "deploy-content-profile", "", "hosted", "AWS.Freeform", "", "", nil, nil,
	)
	require.NoError(t, err)

	_, err = original.CreateHostedConfigurationVersion(
		app.ID, profile.ID, "application/json", "", "", content, nil,
	)
	require.NoError(t, err)

	strategy, err := original.CreateDeploymentStrategy(
		"deploy-content-strat", "", 0, 0, 100, "LINEAR", "NONE", nil,
	)
	require.NoError(t, err)

	_, err = original.StartDeployment(app.ID, env.ID, profile.ID, strategy.ID, "1", "", nil, nil, nil)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	got, err := fresh.GetConfiguration(app.ID, env.ID, profile.ID)
	require.NoError(t, err)
	assert.Equal(t, content, got.Content, "GetConfiguration must derive real content after restore")
}
