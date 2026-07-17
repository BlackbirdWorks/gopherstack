package xray_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/xray"
)

type xrayConfigProvider struct{ settings xray.Settings }

func (c xrayConfigProvider) GetXRaySettings() xray.Settings { return c.settings }

func TestXRayProvider_Init_WithConfigProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		config       any
		name         string
		wantInterval time.Duration
		wantTraceTTL time.Duration
	}{
		{
			name:         "custom_settings",
			config:       xrayConfigProvider{xray.Settings{JanitorInterval: 5 * time.Minute, TraceTTL: 2 * time.Hour}},
			wantInterval: 5 * time.Minute,
			wantTraceTTL: 2 * time.Hour,
		},
		{
			name:         "no_config_provider",
			config:       nil,
			wantInterval: xray.DefaultJanitorInterval,
			wantTraceTTL: xray.DefaultTraceTTL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, err := (&xray.Provider{}).Init(&service.AppContext{Config: tt.config, Logger: slog.Default()})
			require.NoError(t, err)

			h, ok := svc.(*xray.Handler)
			require.True(t, ok)
			assert.Equal(t, tt.wantInterval, h.GetJanitorInterval())
			assert.Equal(t, tt.wantTraceTTL, h.GetJanitorTraceTTL())
		})
	}
}

// TestErrNilAppContext verifies the provider nil guard.
func TestErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &xray.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, xray.ErrNilAppContext)
}

// TestProviderInit verifies normal provider init.
func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &xray.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}
