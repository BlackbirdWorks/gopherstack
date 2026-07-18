package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &appsync.Provider{}
	assert.Equal(t, "AppSync", p.Name())
}

func TestProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &appsync.Provider{}
	svc, err := p.Init(nil)
	require.NoError(t, err)
	require.NotNil(t, svc)
}

func TestProvider_Init_WithCtx(t *testing.T) {
	t.Parallel()

	p := &appsync.Provider{}
	ctx := &service.AppContext{}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
}
