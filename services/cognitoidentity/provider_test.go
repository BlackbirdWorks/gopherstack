package cognitoidentity_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

// mockConfig implements config.Provider for testing.
type mockConfig struct {
	cfg config.GlobalConfig
}

func (m *mockConfig) GetGlobalConfig() config.GlobalConfig { return m.cfg }

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &cognitoidentity.Provider{}
	assert.Equal(t, "CognitoIdentity", p.Name())
}

func TestProvider_Init_NilContext(t *testing.T) {
	t.Parallel()

	p := &cognitoidentity.Provider{}
	reg, err := p.Init(nil)

	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestProvider_Init_WithContext(t *testing.T) {
	t.Parallel()

	p := &cognitoidentity.Provider{}
	ctx := &service.AppContext{
		Config: &mockConfig{
			cfg: config.GlobalConfig{
				AccountID: "123456789012",
				Region:    "eu-west-1",
			},
		},
	}

	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestProvider_Init_ContextWithoutConfigProvider(t *testing.T) {
	t.Parallel()

	p := &cognitoidentity.Provider{}
	ctx := &service.AppContext{
		Config: "not-a-config-provider",
	}

	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)
}
