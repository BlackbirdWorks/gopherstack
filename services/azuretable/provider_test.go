package azuretable_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

func TestProvider_Init_NilAppContext(t *testing.T) {
	t.Parallel()

	p := &azuretable.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, azuretable.ErrNilAppContext)
}

func TestProvider_Init_ReturnsHandler(t *testing.T) {
	t.Parallel()

	p := &azuretable.Provider{}
	reg, err := p.Init(&service.AppContext{})

	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "AzureTable", reg.Name())
}

type fakeConfigProvider struct{ settings azuretable.Settings }

func (f fakeConfigProvider) GetAzureTableSettings() azuretable.Settings { return f.settings }

func TestProvider_Init_UsesConfigProviderSettings(t *testing.T) {
	t.Parallel()

	p := &azuretable.Provider{}
	reg, err := p.Init(&service.AppContext{Config: fakeConfigProvider{settings: azuretable.Settings{Port: 12345}}})

	require.NoError(t, err)

	h, ok := reg.(*azuretable.Handler)
	require.True(t, ok)
	assert.Equal(t, 12345, h.Port)
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &azuretable.Provider{}
	assert.Equal(t, "AzureTable", p.Name())
}

func TestDefaultSettings(t *testing.T) {
	t.Parallel()

	s := azuretable.DefaultSettings()
	assert.Equal(t, azuretable.DefaultPort, s.Port)
}
