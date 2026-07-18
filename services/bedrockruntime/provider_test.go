package bedrockruntime_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

// mockProviderConfig implements config.Provider for testing.
type mockProviderConfig struct {
	accountID string
	region    string
}

func (m *mockProviderConfig) GetGlobalConfig() *config.GlobalConfig {
	const noLatency = 0
	const noJanitorTimeout = 0
	const noAutoPurgeTTL = 0

	return config.NewGlobalConfig(m.accountID, m.region, noLatency, noJanitorTimeout, false, noAutoPurgeTTL)
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &bedrockruntime.Provider{}
	assert.Equal(t, "BedrockRuntime", p.Name())
}

func TestProvider_Init_NilContext(t *testing.T) {
	t.Parallel()

	p := &bedrockruntime.Provider{}
	reg, err := p.Init(nil)

	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestProvider_Init_WithConfig(t *testing.T) {
	t.Parallel()

	p := &bedrockruntime.Provider{}
	reg, err := p.Init(&service.AppContext{})

	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestProvider_Init_FullConfig(t *testing.T) {
	t.Parallel()

	p := &bedrockruntime.Provider{}
	ctx := &service.AppContext{Config: &mockProviderConfig{accountID: "111122223333", region: "eu-west-1"}}
	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)

	h, ok := reg.(*bedrockruntime.Handler)
	require.True(t, ok)
	assert.Equal(t, "eu-west-1", h.Backend.Region())
}
