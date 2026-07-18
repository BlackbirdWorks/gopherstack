package eks_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// TestProviderInit_NilCtx verifies that Init returns ErrNilAppContext when ctx is nil.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &eks.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, eks.ErrNilAppContext)
}

// TestProviderInitAndName verifies Provider.Name and that Init succeeds with a valid context.
func TestProviderInitAndName(t *testing.T) {
	t.Parallel()

	p := &eks.Provider{}
	assert.Equal(t, "EKS", p.Name())

	ctx := &service.AppContext{JanitorCtx: t.Context()}
	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)
}

// TestErrNilAppContext verifies the ErrNilAppContext sentinel.
func TestErrNilAppContext(t *testing.T) {
	t.Parallel()

	require.Error(t, eks.ErrNilAppContext)
	assert.NotEmpty(t, eks.ErrNilAppContext.Error())
}
