package ssm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &ssm.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &ssm.Provider{}
	ctx := &service.AppContext{JanitorCtx: t.Context()}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ ssm.StorageBackend = (*ssm.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies the number of supported operations.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := ssm.NewHandler(ssm.NewInMemoryBackend())
	assert.Len(t, h.GetSupportedOperations(), 152)
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	h := ssm.NewHandler(ssm.NewInMemoryBackend())
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}
