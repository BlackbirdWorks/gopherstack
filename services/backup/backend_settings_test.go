package backup_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestGlobalSettingsBackend(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	b.UpdateGlobalSettings(map[string]string{"isCrossAccountBackupEnabled": "true"})
	settings, _ := b.DescribeGlobalSettings()
	assert.Equal(t, "true", settings["isCrossAccountBackupEnabled"])
}
