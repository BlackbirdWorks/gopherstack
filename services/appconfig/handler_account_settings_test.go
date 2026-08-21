package appconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// TestVendedMetricsViaSDKClient proves GetAccountSettingsOutput/
// UpdateAccountSettingsOutput's real VendedMetrics member (appconfig@v1.48.4
// api_op_GetAccountSettings.go, alongside DeletionProtection) is no longer
// silently discarded on UpdateAccountSettings input and never emitted on
// GetAccountSettings output.
func TestVendedMetricsViaSDKClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestAppConfigClient(t, h)

	updateOut, err := client.UpdateAccountSettings(t.Context(), &appconfigsdk.UpdateAccountSettingsInput{
		VendedMetrics: &types.VendedMetricsSettings{Enabled: aws.Bool(true)},
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.VendedMetrics)
	assert.True(t, aws.ToBool(updateOut.VendedMetrics.Enabled))

	getOut, err := client.GetAccountSettings(t.Context(), &appconfigsdk.GetAccountSettingsInput{})
	require.NoError(t, err)
	require.NotNil(t, getOut.VendedMetrics)
	assert.True(t, aws.ToBool(getOut.VendedMetrics.Enabled))
}

func TestHandler_GetAccountSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/settings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var settings appconfig.AccountSettings
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &settings))
}

// TestHandler_UpdateAccountSettings_DeletionProtectionFieldsSurviveIndependentUpdates
// guards gopherstack-c8ge: DeletionProtectionSettings' Enabled and
// ProtectionPeriodInMinutes are both independently-optional pointers on the
// real UpdateAccountSettingsInput. Updating one field must not wipe the
// other field set by a previous, unrelated Update call -- a single update
// proves nothing here, since there's no Create op to compare a narrower
// payload against.
func TestHandler_UpdateAccountSettings_DeletionProtectionFieldsSurviveIndependentUpdates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Update A: set both fields.
	rec := doRequest(t, h, http.MethodPatch, "/settings",
		[]byte(`{"DeletionProtection":{"Enabled":true,"ProtectionPeriodInMinutes":120}}`))
	require.Equal(t, http.StatusOK, rec.Code)

	// Update B: change only Enabled, omitting ProtectionPeriodInMinutes.
	rec = doRequest(t, h, http.MethodPatch, "/settings",
		[]byte(`{"DeletionProtection":{"Enabled":false}}`))
	require.Equal(t, http.StatusOK, rec.Code)

	var settings appconfig.AccountSettings
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &settings))
	require.NotNil(t, settings.DeletionProtection)
	require.NotNil(t, settings.DeletionProtection.Enabled)
	assert.False(t, *settings.DeletionProtection.Enabled, "B's own field must apply")
	require.NotNil(t, settings.DeletionProtection.ProtectionPeriodInMinutes,
		"A's ProtectionPeriodInMinutes must survive an Update that never mentioned it")
	assert.EqualValues(t, 120, *settings.DeletionProtection.ProtectionPeriodInMinutes)

	// GetAccountSettings must reflect the same merged state.
	rec = doRequest(t, h, http.MethodGet, "/settings", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &settings))
	require.NotNil(t, settings.DeletionProtection.ProtectionPeriodInMinutes)
	assert.EqualValues(t, 120, *settings.DeletionProtection.ProtectionPeriodInMinutes)
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
