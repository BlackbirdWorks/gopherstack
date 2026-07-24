package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestBackend_HostedConfigVersion_ProfileNotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	app, err := b.CreateApplication("app", "")
	require.NoError(t, err)

	_, err = b.CreateHostedConfigurationVersion(
		app.ID,
		"nonexistent-profile",
		"application/json",
		"",
		"",
		[]byte("{}"),
		nil,
	)
	require.Error(t, err)
}

func TestBackend_GetHostedConfigVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetHostedConfigurationVersion("app-1", "prof-1", 1)
	require.Error(t, err)
}

func TestBackend_DeleteHostedConfigVersion_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.DeleteHostedConfigurationVersion("app-1", "prof-1", 1)
	require.Error(t, err)
}
