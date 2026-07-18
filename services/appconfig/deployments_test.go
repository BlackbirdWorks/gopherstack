package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestBackend_GetDeployment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetDeployment("app-1", "env-1", 1)
	require.Error(t, err)
}

func TestBackend_StopDeployment_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.StopDeployment("app-1", "env-1", 1)
	require.Error(t, err)
}
