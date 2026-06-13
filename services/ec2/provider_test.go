package ec2_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ec2"
)

type ec2ConfigProvider struct{ settings ec2.Settings }

func (c ec2ConfigProvider) GetEC2Settings() ec2.Settings { return c.settings }

// TestEC2Provider_Init_WithConfigProvider verifies that Provider.Init picks up
// JanitorInterval, TerminatedTTL and CancelledSpotTTL from the ConfigProvider.
func TestEC2Provider_Init_WithConfigProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		config               any
		name                 string
		wantInterval         time.Duration
		wantTerminatedTTL    time.Duration
		wantCancelledSpotTTL time.Duration
	}{
		{
			name: "custom_settings_propagated",
			config: ec2ConfigProvider{
				ec2.Settings{
					JanitorInterval:  5 * time.Minute,
					TerminatedTTL:    2 * time.Hour,
					CancelledSpotTTL: 12 * time.Hour,
				},
			},
			wantInterval:         5 * time.Minute,
			wantTerminatedTTL:    2 * time.Hour,
			wantCancelledSpotTTL: 12 * time.Hour,
		},
		{
			name:                 "no_config_provider",
			config:               nil,
			wantInterval:         ec2.DefaultJanitorInterval,
			wantTerminatedTTL:    ec2.DefaultTerminatedTTL,
			wantCancelledSpotTTL: ec2.DefaultCancelledSpotTTL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, err := (&ec2.Provider{}).Init(
				&service.AppContext{Config: tt.config, Logger: slog.Default()},
			)
			require.NoError(t, err)

			h, ok := svc.(*ec2.Handler)
			require.True(t, ok)
			t.Cleanup(func() { h.Shutdown(context.Background()) })
			assert.Equal(t, tt.wantInterval, h.GetJanitorInterval())
			assert.Equal(t, tt.wantTerminatedTTL, h.GetJanitorTerminatedTTL())
			assert.Equal(t, tt.wantCancelledSpotTTL, h.GetJanitorCancelledSpotTTL())
		})
	}
}
