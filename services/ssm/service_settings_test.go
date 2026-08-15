package ssm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
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
	h := newHandler()

	settingID := "/ssm/parameter-store/high-throughput-enabled"

	code, out := postJSON(t, h, "GetServiceSetting", map[string]any{"SettingId": settingID})
	assert.Equal(t, http.StatusOK, code)
	assert.NotNil(t, out["ServiceSetting"])

	code, _ = postJSON(t, h, "UpdateServiceSetting", map[string]any{
		"SettingId":    settingID,
		"SettingValue": "true",
	})
	assert.Equal(t, http.StatusOK, code)

	code, _ = postJSON(t, h, "ResetServiceSetting", map[string]any{"SettingId": settingID})
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

// TestServiceSetting_ARNAndLastModifiedDate asserts the real ServiceSetting
// members (types/types.go:5818) gopherstack previously had no Go struct
// fields for at all: ARN (doc-comment-confirmed shape:
// "arn:aws:ssm:<region>:<account>:servicesetting<settingId>",
// api_op_UpdateServiceSetting.go:46) and LastModifiedDate, an epoch-seconds
// DateTime populated once a setting has actually been customized.
func TestServiceSetting_ARNAndLastModifiedDate(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	settingID := "/ssm/parameter-store/high-throughput-enabled"

	def, err := client.GetServiceSetting(t.Context(), &ssmsdk.GetServiceSettingInput{
		SettingId: aws.String(settingID),
	})
	require.NoError(t, err)
	require.NotNil(t, def.ServiceSetting)
	assert.Equal(
		t, "arn:aws:ssm:us-east-1:123456789012:servicesetting/ssm/parameter-store/high-throughput-enabled",
		aws.ToString(def.ServiceSetting.ARN),
	)
	assert.Nil(t, def.ServiceSetting.LastModifiedDate, "Default status must not carry a modification time")

	_, err = client.UpdateServiceSetting(t.Context(), &ssmsdk.UpdateServiceSettingInput{
		SettingId:    aws.String(settingID),
		SettingValue: aws.String("true"),
	})
	require.NoError(t, err)

	updated, err := client.GetServiceSetting(t.Context(), &ssmsdk.GetServiceSettingInput{
		SettingId: aws.String(settingID),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.ServiceSetting.LastModifiedDate)
	assert.False(t, updated.ServiceSetting.LastModifiedDate.IsZero())
}
