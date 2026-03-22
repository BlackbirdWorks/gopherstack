package ses_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ses"
)

type sesConfigProvider struct{ settings ses.Settings }

func (c sesConfigProvider) GetSESSettings() ses.Settings { return c.settings }

func TestSESProvider_Init_WithConfigProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		config       any
		name         string
		wantInterval time.Duration
		wantEmailTTL time.Duration
	}{
		{
			name:         "custom_settings",
			config:       sesConfigProvider{ses.Settings{JanitorInterval: 5 * time.Minute, EmailTTL: 48 * time.Hour}},
			wantInterval: 5 * time.Minute,
			wantEmailTTL: 48 * time.Hour,
		},
		{
			name:         "no_config_provider",
			config:       nil,
			wantInterval: ses.DefaultJanitorInterval,
			wantEmailTTL: ses.DefaultEmailTTL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, err := (&ses.Provider{}).Init(&service.AppContext{Config: tt.config, Logger: slog.Default()})
			require.NoError(t, err)

			h, ok := svc.(*ses.Handler)
			require.True(t, ok)
			assert.Equal(t, tt.wantInterval, h.GetJanitorInterval())
			assert.Equal(t, tt.wantEmailTTL, h.GetEmailTTL())
		})
	}
}
