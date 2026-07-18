package amplify_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &amplify.Provider{}
	assert.Equal(t, "Amplify", p.Name())
}

func TestProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &amplify.Provider{}
	reg, err := p.Init(nil)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

func TestProvider_Init_WithCtx(t *testing.T) {
	t.Parallel()

	p := &amplify.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}
