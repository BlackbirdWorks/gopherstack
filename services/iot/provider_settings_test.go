package iot_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

type iotSettingsConfig struct{ settings iot.Settings }

func (c iotSettingsConfig) GetIoTSettings() iot.Settings { return c.settings }

func TestProviderInit_MQTTPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  any
		name string
		want int
	}{
		{name: "no_config_defaults", cfg: nil, want: 1883},
		{name: "configured_port", cfg: iotSettingsConfig{iot.Settings{MQTTPort: 41883}}, want: 41883},
		{name: "zero_means_ephemeral", cfg: iotSettingsConfig{iot.Settings{MQTTPort: 0}}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, err := (&iot.Provider{}).Init(&service.AppContext{JanitorCtx: t.Context(), Config: tt.cfg})
			require.NoError(t, err)

			h, ok := svc.(*iot.Handler)
			require.True(t, ok)

			bk, ok := h.Backend.(*iot.InMemoryBackend)
			require.True(t, ok)
			assert.Equal(t, tt.want, bk.MQTTPort())
		})
	}
}
