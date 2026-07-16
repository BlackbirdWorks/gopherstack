package ssm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceSetting_GetPutReset(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Get default
	rec := doRequest(
		t,
		h,
		"GetServiceSetting",
		`{"SettingId":"/ssm/parameter-store/default-parameter-tier"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Default")

	// Update
	updateBody := `{"SettingId":"/ssm/parameter-store/default-parameter-tier","SettingValue":"Advanced"}`
	rec = doRequest(t, h, "UpdateServiceSetting", updateBody)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get updated
	rec = doRequest(
		t,
		h,
		"GetServiceSetting",
		`{"SettingId":"/ssm/parameter-store/default-parameter-tier"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "Advanced")

	// Reset
	rec = doRequest(
		t,
		h,
		"ResetServiceSetting",
		`{"SettingId":"/ssm/parameter-store/default-parameter-tier"}`,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}
func TestFull_ServiceSetting_GetUpdateReset(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	settingID := "/ssm/parameter-store/high-throughput-enabled"

	code, out := mustPost(t, h, "GetServiceSetting", map[string]any{"SettingId": settingID})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["ServiceSetting"])

	code, _ = mustPost(t, h, "UpdateServiceSetting", map[string]any{
		"SettingId":    settingID,
		"SettingValue": "true",
	})
	assert.Equal(t, http.StatusOK, code)

	code, _ = mustPost(t, h, "ResetServiceSetting", map[string]any{"SettingId": settingID})
	assert.Equal(t, http.StatusOK, code)
}
func TestServiceSettings_LifecycleRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		settingID  string
		setValue   string
		wantCustom bool
	}{
		{
			name:       "parameter_store_advanced_tier",
			settingID:  "/ssm/parameter-store/default-parameter-tier",
			setValue:   "Advanced",
			wantCustom: true,
		},
		{
			name:       "session_manager_timeout",
			settingID:  "/ssm/session-manager/max-session-duration",
			setValue:   "60",
			wantCustom: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			// Default state.
			body, _ := json.Marshal(map[string]any{"SettingId": tt.settingID})
			rec := doRequest(t, h, "GetServiceSetting", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "Default")

			// Update.
			body, _ = json.Marshal(map[string]any{
				"SettingId":    tt.settingID,
				"SettingValue": tt.setValue,
			})
			rec = doRequest(t, h, "UpdateServiceSetting", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			// Get updated.
			body, _ = json.Marshal(map[string]any{"SettingId": tt.settingID})
			rec = doRequest(t, h, "GetServiceSetting", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.setValue)
			assert.Contains(t, rec.Body.String(), "Customized")

			// Reset.
			body, _ = json.Marshal(map[string]any{"SettingId": tt.settingID})
			rec = doRequest(t, h, "ResetServiceSetting", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			// Default again.
			body, _ = json.Marshal(map[string]any{"SettingId": tt.settingID})
			rec = doRequest(t, h, "GetServiceSetting", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "Default")
		})
	}
}
