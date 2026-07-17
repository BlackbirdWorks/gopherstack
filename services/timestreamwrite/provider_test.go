package timestreamwrite_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// TestProvider_ErrNilAppContext verifies the provider nil guard.
func TestProvider_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &timestreamwrite.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, timestreamwrite.ErrNilAppContext)
}

// TestProvider_Init verifies normal provider init.
func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &timestreamwrite.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestStorageBackendInterface verifies InMemoryBackend implements StorageBackend.
func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ timestreamwrite.StorageBackend = (*timestreamwrite.InMemoryBackend)(nil)
}
