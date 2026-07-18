package sagemakerruntime_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
)

// --- Provider tests ---

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &sagemakerruntime.Provider{}
	assert.Equal(t, "SageMakerRuntime", p.Name())

	backend := sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1")
	h := sagemakerruntime.NewHandler(backend)

	assert.NotNil(t, h)
	assert.Equal(t, "SageMakerRuntime", h.Name())
	assert.Equal(t, "us-east-1", backend.Region())
}

func TestProvider_InitFull(t *testing.T) {
	t.Parallel()

	ctx := &service.AppContext{}
	p := &sagemakerruntime.Provider{}
	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "SageMakerRuntime", reg.Name())
}
