package bedrockagentcore_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

type fakeConfig struct{ settings bedrockagentcore.Settings }

func (c fakeConfig) GetAgentCoreSettings() bedrockagentcore.Settings { return c.settings }

func (fakeConfig) GetAgentCoreEndpointURL() string { return "" }

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx  *service.AppContext
		name string
	}{
		{name: "no config provider", ctx: &service.AppContext{Logger: slog.Default()}},
		{
			name: "engine none",
			ctx: &service.AppContext{
				Logger: slog.Default(),
				Config: fakeConfig{settings: bedrockagentcore.Settings{Engine: bedrockagentcore.EngineNone}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reg, err := (&bedrockagentcore.Provider{}).Init(tt.ctx)
			require.NoError(t, err)

			h, ok := reg.(*bedrockagentcore.Handler)
			require.True(t, ok)
			t.Cleanup(h.Backend.Close)

			assert.False(t, h.Backend.EngineEnabled(), "no container engine without an enabled runtime")
			assert.Equal(t, "BedrockAgentCore", h.Name())
		})
	}
}
