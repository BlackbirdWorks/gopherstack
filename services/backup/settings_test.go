package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestGlobalSettingsBackend(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	b.UpdateGlobalSettings(map[string]string{"isCrossAccountBackupEnabled": "true"})
	settings, _ := b.DescribeGlobalSettings()
	assert.Equal(t, "true", settings["isCrossAccountBackupEnabled"])
}

// TestGlobalSettingsLastUpdateTimeEpoch verifies that DescribeGlobalSettings'
// LastUpdateTime is serialized as epoch seconds, matching real AWS behavior.
func TestGlobalSettingsLastUpdateTimeEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "last_update_time_is_epoch_seconds"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newHandler(t)

			// Update settings so LastUpdateTime is set.
			doRequest(t, h, http.MethodPut, "/global-settings",
				`{"GlobalSettings":{"isCrossAccountBackupEnabled":"true"}}`)

			resp := doRequest(t, h, http.MethodGet, "/global-settings", "")
			require.Equal(t, http.StatusOK, resp.Code)

			var data map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &data))

			lastUpdate, exists := data["LastUpdateTime"]
			require.True(t, exists, "LastUpdateTime must be present")
			_, isFloat := lastUpdate.(float64)
			assert.True(t, isFloat,
				"LastUpdateTime must be epoch seconds (float64), got %T: %v", lastUpdate, lastUpdate)
		})
	}
}

func TestGlobalSettings(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	t.Run("describe returns empty initially", func(t *testing.T) {
		t.Parallel()
		h2, _ := newHandler(t)
		resp := doRequest(t, h2, http.MethodGet, "/global-settings", "")
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "GlobalSettings")
	})

	t.Run("update and describe", func(t *testing.T) {
		t.Parallel()
		h2, _ := newHandler(t)
		_ = doRequest(t, h2, http.MethodPut, "/global-settings",
			`{"GlobalSettings":{"isCrossAccountBackupEnabled":"true"}}`)
		resp := doRequest(t, h2, http.MethodGet, "/global-settings", "")
		assert.Contains(t, resp.Body.String(), "isCrossAccountBackupEnabled")
	})
	_ = h
}

func TestRegionSettings(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	t.Run("describe returns defaults", func(t *testing.T) {
		t.Parallel()
		h2, _ := newHandler(t)
		resp := doRequest(t, h2, http.MethodGet, "/account-settings", "")
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "ResourceTypeOptInPreference")
	})
	_ = h
}
