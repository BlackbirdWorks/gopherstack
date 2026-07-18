package codepipeline_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestCPProvider_Name(t *testing.T) {
	t.Parallel()

	p := &codepipeline.Provider{}
	assert.Equal(t, "CodePipeline", p.Name())
}

func TestCPProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codepipeline.Provider{}
	_, err := p.Init(nil)
	// CodePipeline requires a non-nil context
	require.Error(t, err)
}

func TestCPProvider_Init_WithCtx(t *testing.T) {
	t.Parallel()

	p := &codepipeline.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

// decodeBody parses JSON from response body into a map.

func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	var p codepipeline.Provider

	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codepipeline.ErrNilAppContext)
}
