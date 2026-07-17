package appconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestHandler_GetAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/settings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var settings appconfig.AccountSettings
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &settings))
}

func TestHandler_UpdateAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Update deletion protection.
	body := []byte(`{"DeletionProtection":{"Enabled":true,"ProtectionPeriodInMinutes":30}}`)
	rec := doRequest(t, h, http.MethodPatch, "/settings", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var settings appconfig.AccountSettings
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &settings))
	require.NotNil(t, settings.DeletionProtection)
	require.NotNil(t, settings.DeletionProtection.Enabled)
	assert.True(t, *settings.DeletionProtection.Enabled)

	// GetAccountSettings should reflect the update.
	rec = doRequest(t, h, http.MethodGet, "/settings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &settings))
	require.NotNil(t, settings.DeletionProtection)
	assert.True(t, *settings.DeletionProtection.Enabled)
}
