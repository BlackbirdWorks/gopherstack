package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestBackend_GetConfigurationProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetConfigurationProfile("app-1", "prof-1")
	require.Error(t, err)
}

func TestBackend_ListConfigurationProfiles_AppNotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, _, err := b.ListConfigurationProfiles("nonexistent", "", 0)
	require.Error(t, err)
}

func TestBackend_UpdateConfigurationProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.UpdateConfigurationProfile("app-1", "prof-1", new("name"), new(""), nil, nil)
	require.Error(t, err)
}

func TestBackend_DeleteConfigurationProfile_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.DeleteConfigurationProfile("app-1", "prof-1")
	require.Error(t, err)
}
