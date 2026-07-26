package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestBackend_CreateDeploymentStrategy(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	s, err := b.CreateDeploymentStrategy("strat", "desc", 0, 0, 100, "LINEAR", "NONE", nil)
	require.NoError(t, err)
	assert.Equal(t, "strat", s.Name)
	assert.NotEmpty(t, s.ID)

	strategies, _ := b.ListDeploymentStrategies("", 0)
	assert.Len(t, strategies, 1)
}

func TestBackend_UpdateDeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.UpdateDeploymentStrategy("nonexistent", "name", new(""), 0, 0, 0)
	require.Error(t, err)
}

func TestBackend_DeleteDeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.DeleteDeploymentStrategy("nonexistent")
	require.Error(t, err)
}

func TestBackend_GetDeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetDeploymentStrategy("nonexistent")
	require.Error(t, err)
}
