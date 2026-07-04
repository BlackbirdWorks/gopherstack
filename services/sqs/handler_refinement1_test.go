package sqs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &sqs.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, sqs.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &sqs.Provider{}
	reg, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ sqs.StorageBackend = (*sqs.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies 24 operations are supported.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)
	assert.Len(t, h.GetSupportedOperations(), 23)
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}
