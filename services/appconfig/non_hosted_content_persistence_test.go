package appconfig_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

type staticContentReader struct {
	contentType string
	content     []byte
}

func (r staticContentReader) ReadConfiguration(context.Context, string, string, string) ([]byte, string, error) {
	return r.content, r.contentType, nil
}

func TestNonHostedDeployedContentSurvivesRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		location    string
		content     []byte
	}{
		{
			name: "ssm parameter json", location: "ssm-parameter://cfg",
			contentType: "application/json", content: []byte(`{"a":1}`),
		},
		{name: "default content type", location: "ssm-parameter://raw", content: []byte("plain")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			orig := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
			orig.SetConfigurationContentReader(staticContentReader{content: tt.content, contentType: tt.contentType})

			app, err := orig.CreateApplication("nh-app", "", nil)
			require.NoError(t, err)

			env, err := orig.CreateEnvironment(app.ID, "nh-env", "", nil, nil)
			require.NoError(t, err)

			profile, err := orig.CreateConfigurationProfile(
				app.ID, "nh-profile", "", tt.location, "AWS.Freeform",
				"arn:aws:iam::123456789012:role/r", "", nil, nil,
			)
			require.NoError(t, err)

			strategy, err := orig.CreateDeploymentStrategy("nh-strat", "", 0, 0, 100, "LINEAR", "NONE", nil)
			require.NoError(t, err)

			_, err = orig.StartDeployment(app.ID, env.ID, profile.ID, strategy.ID, "1", "", nil, nil, nil)
			require.NoError(t, err)

			wantContent, wantType, _, err := orig.CurrentDeployedConfiguration(app.ID, env.ID, profile.ID)
			require.NoError(t, err)
			require.Equal(t, wantContent, tt.content)

			fresh := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), orig.Snapshot(t.Context())))

			got, gotType, _, err := fresh.CurrentDeployedConfiguration(app.ID, env.ID, profile.ID)
			require.NoError(t, err)
			assert.Equal(t, wantContent, got)
			assert.Equal(t, wantType, gotType)
		})
	}
}
