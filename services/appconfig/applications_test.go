package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestBackend_GetApplication_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetApplication("nonexistent")
	require.Error(t, err)
}

func TestBackend_ListApplications_Empty(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	apps, _ := b.ListApplications("", 0)
	assert.Empty(t, apps)
}

func TestBackend_DeleteApplication_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.DeleteApplication("nonexistent")
	require.Error(t, err)
}

// TestBackend_DeleteApplication_CascadesExperimentDefinitions verifies that
// DeleteApplication removes every ExperimentDefinition scoped to it (and,
// transitively, their runs and tags) rather than leaving ghost rows behind
// -- the same cascade-cleanup precedent already set for environments/
// configProfiles/hostedConfigVersions/deployments/ExtensionAssociations.
func TestBackend_DeleteApplication_CascadesExperimentDefinitions(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	appID, envID, profileID := seedExperimentApp(t, b)

	def, err := b.CreateExperimentDefinition(
		appID, "cascade-app-def", envID, profileID, "flag1", "true", "", "", "",
		experimentTreatment(false, 100), []appconfig.Treatment{*experimentTreatment(true, 100)},
		nil,
	)
	require.NoError(t, err)

	run, err := b.StartExperimentRun(appID, def.ID, "", nil, nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteApplication(appID))

	_, err = b.GetExperimentDefinition(appID, def.ID)
	require.Error(t, err, "experiment definition must not survive its application's deletion")

	_, err = b.GetExperimentRun(appID, def.ID, run.Run)
	require.Error(t, err, "experiment run must not survive its application's deletion")
}

func TestBackend_appConfigPaginate_EdgeCases(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	secret := b.PaginationSecret()

	// Create 4 apps.
	for _, name := range []string{"a", "b", "c", "d"} {
		_, err := b.CreateApplication(name, "")
		require.NoError(t, err)
	}

	tests := []struct {
		name          string
		nextToken     string
		maxResults    int
		wantCount     int
		wantNextToken bool
	}{
		{
			name:       "zero_max_returns_all",
			maxResults: 0,
			wantCount:  4,
		},
		{
			name:          "first_page",
			maxResults:    2,
			wantCount:     2,
			wantNextToken: true,
		},
		{
			name:       "second_page",
			maxResults: 2,
			nextToken:  page.EncodeHMACToken(2, secret),
			wantCount:  2,
		},
		{
			name:       "token_beyond_end",
			maxResults: 2,
			nextToken:  page.EncodeHMACToken(50, secret),
			wantCount:  0,
		},
		{
			name:          "invalid_token_treated_as_start",
			maxResults:    2,
			nextToken:     "bogus",
			wantCount:     2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			apps, outToken := b.ListApplications(tt.nextToken, tt.maxResults)
			assert.Len(t, apps, tt.wantCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, outToken)
			} else {
				assert.Empty(t, outToken)
			}
		})
	}
}
