package cognitoidp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &cognitoidp.Provider{}
	assert.Equal(t, "CognitoIDP", p.Name())

	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &cognitoidp.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, cognitoidp.ErrNilAppContext)
}
