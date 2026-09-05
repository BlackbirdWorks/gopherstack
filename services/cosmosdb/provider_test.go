package cosmosdb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func TestProvider_Init_NilAppContext(t *testing.T) {
	t.Parallel()

	p := &cosmosdb.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, cosmosdb.ErrNilAppContext)
}

func TestProvider_Init_ReturnsHandler(t *testing.T) {
	t.Parallel()

	p := &cosmosdb.Provider{}
	reg, err := p.Init(&service.AppContext{})

	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "CosmosDB", reg.Name())
}

type fakeConfigProvider struct{ settings cosmosdb.Settings }

func (f fakeConfigProvider) GetCosmosDBSettings() cosmosdb.Settings { return f.settings }

func TestProvider_Init_UsesConfigProviderSettings(t *testing.T) {
	t.Parallel()

	p := &cosmosdb.Provider{}
	reg, err := p.Init(&service.AppContext{
		Config: fakeConfigProvider{settings: cosmosdb.Settings{Port: 12345, MasterKey: "abc", ValidateAuth: true}},
	})

	require.NoError(t, err)

	h, ok := reg.(*cosmosdb.Handler)
	require.True(t, ok)
	assert.Equal(t, 12345, h.Port)
	assert.Equal(t, "abc", h.MasterKey)
	assert.True(t, h.ValidateAuth)
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &cosmosdb.Provider{}
	assert.Equal(t, "CosmosDB", p.Name())
}

func TestDefaultSettings(t *testing.T) {
	t.Parallel()

	s := cosmosdb.DefaultSettings()
	assert.Equal(t, cosmosdb.DefaultPort, s.Port)
	assert.Equal(t, cosmosdb.DefaultMasterKey, s.MasterKey)
	assert.False(t, s.ValidateAuth)
}
