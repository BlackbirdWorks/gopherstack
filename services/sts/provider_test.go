package sts_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &sts.Provider{}
	assert.Equal(t, "STS", p.Name())
}

// TestProvider_Init covers Init across a nil AppContext and two valid AppContext shapes.
func TestProvider_Init(t *testing.T) {
	t.Parallel()

	t.Run("nil_app_context_errors", func(t *testing.T) {
		t.Parallel()

		p := &sts.Provider{}
		_, err := p.Init(nil)

		require.Error(t, err)
		assert.ErrorIs(t, err, sts.ErrNilAppContext)
	})

	t.Run("with_logger", func(t *testing.T) {
		t.Parallel()

		p := &sts.Provider{}
		appCtx := &service.AppContext{
			Logger: logger.NewTestLogger(),
		}

		reg, err := p.Init(appCtx)
		require.NoError(t, err)
		assert.NotNil(t, reg)
		assert.Equal(t, "STS", reg.Name())
	})

	t.Run("with_janitor_ctx", func(t *testing.T) {
		t.Parallel()

		p := &sts.Provider{}
		reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
		require.NoError(t, err)
		assert.NotNil(t, reg)
	})
}

// TestStorageBackendInterface verifies the InMemoryBackend satisfies StorageBackend
// (a compile-time assertion exercised at test time).
func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ sts.StorageBackend = (*sts.InMemoryBackend)(nil)
}
