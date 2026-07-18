package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestProvider_NameAndInit(t *testing.T) {
	t.Parallel()

	p := appconfig.Provider{}
	assert.Equal(t, "AppConfig", p.Name())

	result, err := p.Init(nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
}
