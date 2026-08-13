package cloudwatchlogs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validOpenSearchResourceConfig returns an OpenSearchResourceConfig
// satisfying its three required members (DataSourceRoleArn,
// DashboardViewerPrincipals, RetentionDays -- validateOpenSearchResourceConfig,
// validators.go).
func validOpenSearchResourceConfig() *cloudwatchlogs.OpenSearchResourceConfig {
	days := int32(30)

	return &cloudwatchlogs.OpenSearchResourceConfig{
		DataSourceRoleArn:         "arn:aws:iam::123456789012:role/cwl-opensearch",
		DashboardViewerPrincipals: []string{"arn:aws:iam::123456789012:user/viewer"},
		RetentionDays:             &days,
	}
}

func TestIntegration_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend)
		name   string
	}{
		{
			name: "put_get_list_delete",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				ig, err := b.PutIntegration("my-opensearch", "OPENSEARCH", validOpenSearchResourceConfig())
				require.NoError(t, err)
				assert.Equal(t, "my-opensearch", ig.Name)
				assert.Equal(t, "ACTIVE", ig.Status)
				require.NotNil(t, ig.OpenSearchResourceConfig)
				assert.Equal(
					t,
					"arn:aws:iam::123456789012:role/cwl-opensearch",
					ig.OpenSearchResourceConfig.DataSourceRoleArn,
				)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				got, err := b.GetIntegration("my-opensearch")
				require.NoError(t, err)
				assert.Equal(t, "OPENSEARCH", got.Type)

				igs := b.ListIntegrations()
				require.Len(t, igs, 1)

				err = b.DeleteIntegration("my-opensearch")
				require.NoError(t, err)

				_, err = b.GetIntegration("my-opensearch")
				require.ErrorIs(t, err, cloudwatchlogs.ErrIntegrationNotFound)
			},
		},
		{
			name: "list_multiple_sorted",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIntegration("z-integration", "OPENSEARCH", validOpenSearchResourceConfig())
				require.NoError(t, err)
				_, err = b.PutIntegration("a-integration", "OPENSEARCH", validOpenSearchResourceConfig())
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				igs := b.ListIntegrations()
				require.Len(t, igs, 2)
				assert.Equal(t, "a-integration", igs[0].Name)
				assert.Equal(t, "z-integration", igs[1].Name)
			},
		},
		{
			name: "get_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.GetIntegration("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrIntegrationNotFound)
			},
		},
		{
			name: "delete_not_found_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				err := b.DeleteIntegration("ghost")
				require.ErrorIs(t, err, cloudwatchlogs.ErrIntegrationNotFound)
			},
		},
		{
			name: "put_empty_name_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIntegration("", "OPENSEARCH", validOpenSearchResourceConfig())
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
		{
			name: "put_missing_resource_config_errors",
			setup: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend) {
				t.Helper()
				_, err := b.PutIntegration("no-config", "OPENSEARCH", nil)
				require.ErrorIs(t, err, cloudwatchlogs.ErrValidation)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			if tt.setup != nil {
				tt.setup(t, b)
			}
			if tt.verify != nil {
				tt.verify(t, b)
			}
		})
	}
}
