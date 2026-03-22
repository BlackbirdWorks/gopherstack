package athena_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

type athenaConfigProvider struct {
	settings athena.Settings
}

func (c athenaConfigProvider) GetAthenaSettings() athena.Settings {
	return c.settings
}

// TestAthenaProvider_Init_WithConfigProvider verifies that Provider.Init picks up
// JanitorInterval and ExecutionTTL from the ConfigProvider.
func TestAthenaProvider_Init_WithConfigProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		settings     athena.Settings
		wantInterval time.Duration
		wantTTL      time.Duration
	}{
		{
			name:         "custom_settings_propagated",
			settings:     athena.Settings{JanitorInterval: 5 * time.Minute, ExecutionTTL: 48 * time.Hour},
			wantInterval: 5 * time.Minute,
			wantTTL:      48 * time.Hour,
		},
		{
			name:         "zero_uses_defaults",
			settings:     athena.Settings{},
			wantInterval: athena.DefaultJanitorInterval,
			wantTTL:      athena.DefaultExecutionTTL,
		},
		{
			name:         "no_config_provider",
			settings:     athena.Settings{},
			wantInterval: athena.DefaultJanitorInterval,
			wantTTL:      athena.DefaultExecutionTTL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &athena.Provider{}
			ctx := &service.AppContext{Logger: slog.Default()}

			if tt.name != "no_config_provider" {
				ctx.Config = athenaConfigProvider{settings: tt.settings}
			}

			svc, err := p.Init(ctx)
			require.NoError(t, err)

			h, ok := svc.(*athena.Handler)
			require.True(t, ok)
			assert.Equal(t, tt.wantInterval, h.GetJanitorInterval())
			assert.Equal(t, tt.wantTTL, h.GetJanitorExecutionTTL())
		})
	}
}
