package sagemaker_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &sagemaker.Provider{}

	assert.Equal(t, "SageMaker", p.Name())

	backend := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
	h := sagemaker.NewHandler(backend)

	assert.NotNil(t, h)
	assert.Equal(t, "SageMaker", h.Name())
	assert.Equal(t, "us-east-1", backend.Region())
}

func TestProvider_InitFull(t *testing.T) {
	t.Parallel()

	ctx := &service.AppContext{}
	p := &sagemaker.Provider{}
	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "SageMaker", reg.Name())
}
