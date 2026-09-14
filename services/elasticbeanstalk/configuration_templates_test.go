package elasticbeanstalk_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

// TestInMemoryBackend_UpdateConfigurationTemplate_BumpsDateUpdated verifies that
// UpdateConfigurationTemplate advances DateUpdated on every mutation, not just
// at creation time.
func TestInMemoryBackend_UpdateConfigurationTemplate_BumpsDateUpdated(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreateApplication(context.Background(), "app3", "", nil)
	require.NoError(t, err)
	tmpl, err := b.CreateConfigurationTemplate(context.Background(), "app3", "tmpl1", "orig", "", nil)
	require.NoError(t, err)
	created := tmpl.DateUpdated

	time.Sleep(time.Second)

	updated, err := b.UpdateConfigurationTemplate(context.Background(), "app3", "tmpl1", "new desc")
	require.NoError(t, err)
	assert.NotEqual(t, created, updated.DateUpdated)
}

// TestInMemoryBackend_CreateConfigurationTemplate_SeedsFromEnvironment verifies that
// EnvironmentId seeds a new configuration template from an existing environment's
// live option settings and solution stack (CreateConfigurationTemplateInput.EnvironmentId).
func TestInMemoryBackend_CreateConfigurationTemplate_SeedsFromEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		requestedStack    string
		requestedOverride []elasticbeanstalk.OptionSetting
		wantStack         string
		wantSettings      []elasticbeanstalk.OptionSetting
	}{
		{
			name:      "seeds stack and settings",
			wantStack: "64bit Amazon Linux 2 v5.8.0 running Go 1",
			wantSettings: []elasticbeanstalk.OptionSetting{
				{Namespace: "aws:autoscaling:launchconfiguration", OptionName: "InstanceType", Value: "t3.micro"},
			},
		},
		{
			name:           "requested solution stack wins over the seeded one",
			requestedStack: "64bit Amazon Linux 2 v5.9.0 running Go 1",
			wantStack:      "64bit Amazon Linux 2 v5.9.0 running Go 1",
			wantSettings: []elasticbeanstalk.OptionSetting{
				{Namespace: "aws:autoscaling:launchconfiguration", OptionName: "InstanceType", Value: "t3.micro"},
			},
		},
		{
			name:      "explicit option settings override the seeded value",
			wantStack: "64bit Amazon Linux 2 v5.8.0 running Go 1",
			requestedOverride: []elasticbeanstalk.OptionSetting{
				{Namespace: "aws:autoscaling:launchconfiguration", OptionName: "InstanceType", Value: "t3.large"},
			},
			wantSettings: []elasticbeanstalk.OptionSetting{
				{Namespace: "aws:autoscaling:launchconfiguration", OptionName: "InstanceType", Value: "t3.large"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := newTestBackend()
			_, err := b.CreateApplication(ctx, "seed-app", "", nil)
			require.NoError(t, err)

			env, err := b.CreateEnvironment(
				ctx,
				"seed-app",
				"seed-env",
				"64bit Amazon Linux 2 v5.8.0 running Go 1",
				"",
				nil,
				elasticbeanstalk.CreateEnvironmentParams{
					OptionSettings: []elasticbeanstalk.OptionSetting{
						{
							Namespace:  "aws:autoscaling:launchconfiguration",
							OptionName: "InstanceType",
							Value:      "t3.micro",
						},
					},
				},
			)
			require.NoError(t, err)

			tmpl, err := b.CreateConfigurationTemplateWithParams(
				ctx, "seed-app", "seed-tmpl", "", tt.requestedStack, nil,
				elasticbeanstalk.ConfigurationTemplateParams{
					EnvironmentID:  env.EnvironmentID,
					OptionSettings: tt.requestedOverride,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStack, tmpl.SolutionStackName)
			assert.Equal(t, tt.wantSettings, tmpl.OptionSettings)
		})
	}
}

// TestInMemoryBackend_CreateConfigurationTemplate_SeedsFromSourceConfiguration verifies
// that SourceConfiguration seeds a new template from another template's settings
// (CreateConfigurationTemplateInput.SourceConfiguration).
func TestInMemoryBackend_CreateConfigurationTemplate_SeedsFromSourceConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("seeds from a template in the same application", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		b := newTestBackend()
		_, err := b.CreateApplication(ctx, "src-app", "", nil)
		require.NoError(t, err)

		_, err = b.CreateConfigurationTemplateWithParams(
			ctx, "src-app", "base-tmpl", "", "64bit Amazon Linux 2 v5.8.0 running Go 1", nil,
			elasticbeanstalk.ConfigurationTemplateParams{
				OptionSettings: []elasticbeanstalk.OptionSetting{
					{
						Namespace:  "aws:elasticbeanstalk:application:environment",
						OptionName: "LOG_LEVEL",
						Value:      "debug",
					},
				},
			},
		)
		require.NoError(t, err)

		tmpl, err := b.CreateConfigurationTemplateWithParams(
			ctx, "src-app", "derived-tmpl", "", "", nil,
			elasticbeanstalk.ConfigurationTemplateParams{SourceTemplateName: "base-tmpl"},
		)
		require.NoError(t, err)
		assert.Equal(t, "64bit Amazon Linux 2 v5.8.0 running Go 1", tmpl.SolutionStackName)
		assert.Equal(t, []elasticbeanstalk.OptionSetting{
			{Namespace: "aws:elasticbeanstalk:application:environment", OptionName: "LOG_LEVEL", Value: "debug"},
		}, tmpl.OptionSettings)
	})

	t.Run("mismatched requested solution stack is rejected", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		b := newTestBackend()
		_, err := b.CreateApplication(ctx, "src-app2", "", nil)
		require.NoError(t, err)

		_, err = b.CreateConfigurationTemplateWithParams(
			ctx, "src-app2", "base-tmpl", "", "64bit Amazon Linux 2 v5.8.0 running Go 1", nil,
			elasticbeanstalk.ConfigurationTemplateParams{},
		)
		require.NoError(t, err)

		_, err = b.CreateConfigurationTemplateWithParams(
			ctx, "src-app2", "derived-tmpl", "", "64bit Amazon Linux 2 v5.9.0 running Go 1", nil,
			elasticbeanstalk.ConfigurationTemplateParams{SourceTemplateName: "base-tmpl"},
		)
		require.ErrorIs(t, err, elasticbeanstalk.ErrInvalidParameter)
	})

	t.Run("nonexistent source template is rejected", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		b := newTestBackend()
		_, err := b.CreateApplication(ctx, "src-app3", "", nil)
		require.NoError(t, err)

		_, err = b.CreateConfigurationTemplateWithParams(
			ctx, "src-app3", "derived-tmpl", "", "", nil,
			elasticbeanstalk.ConfigurationTemplateParams{SourceTemplateName: "missing-tmpl"},
		)
		require.ErrorIs(t, err, elasticbeanstalk.ErrNotFound)
	})
}

// TestInMemoryBackend_CreateConfigurationTemplate_EnvironmentIDNotFound verifies
// EnvironmentId naming a nonexistent environment is rejected rather than silently
// producing an unseeded template.
func TestInMemoryBackend_CreateConfigurationTemplate_EnvironmentIDNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend()
	_, err := b.CreateApplication(ctx, "env-missing-app", "", nil)
	require.NoError(t, err)

	_, err = b.CreateConfigurationTemplateWithParams(
		ctx, "env-missing-app", "derived-tmpl", "", "", nil,
		elasticbeanstalk.ConfigurationTemplateParams{EnvironmentID: "e-00000000"},
	)
	require.ErrorIs(t, err, elasticbeanstalk.ErrNotFound)
}
