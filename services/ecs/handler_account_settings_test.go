package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestAccountSettings_AllValidNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	validNames := []string{
		"containerInstanceLongArnFormat",
		"serviceLongArnFormat",
		"taskLongArnFormat",
		"awsvpcTrunking",
		"containerInsights",
		"dualStackIPv6",
		"fargateTaskRetirementWaitPeriod",
		"tagResourceAuthorization",
		"guardDutyActivate",
	}

	for _, name := range validNames {
		resp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
			"name":  name,
			"value": "enabled",
		})
		assert.Equal(t, http.StatusOK, resp.Code, "PutAccountSetting %s", name)
	}

	listResp := doECSRequest(t, h, "ListAccountSettings", map[string]any{})
	require.Equal(t, http.StatusOK, listResp.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	settings := listOut["settings"].([]any)
	assert.GreaterOrEqual(t, len(settings), len(validNames))
}

func TestAccountSettings_PutDefault_vs_Put(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// PutAccountSettingDefault sets for all principals
	defResp := doECSRequest(t, h, "PutAccountSettingDefault", map[string]any{
		"name":  "containerInsights",
		"value": "enabled",
	})
	require.Equal(t, http.StatusOK, defResp.Code)

	// PutAccountSetting for a specific principal
	putResp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":         "containerInsights",
		"value":        "disabled",
		"principalArn": "arn:aws:iam::000000000000:user/testuser",
	})
	require.Equal(t, http.StatusOK, putResp.Code)

	// ListAccountSettings with name filter
	listResp := doECSRequest(t, h, "ListAccountSettings", map[string]any{
		"name": "containerInsights",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	settings := listOut["settings"].([]any)
	assert.GreaterOrEqual(t, len(settings), 1)
}

func TestAccountSettings_Delete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":         "awsvpcTrunking",
		"value":        "enabled",
		"principalArn": "arn:aws:iam::000000000000:user/deluser",
	})

	delResp := doECSRequest(t, h, "DeleteAccountSetting", map[string]any{
		"name":         "awsvpcTrunking",
		"principalArn": "arn:aws:iam::000000000000:user/deluser",
	})
	require.Equal(t, http.StatusOK, delResp.Code)

	listResp := doECSRequest(t, h, "ListAccountSettings", map[string]any{
		"name":         "awsvpcTrunking",
		"principalArn": "arn:aws:iam::000000000000:user/deluser",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	settings := listOut["settings"].([]any)
	assert.Empty(t, settings)
}

func TestAccountSettings_OverwriteExisting(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "tagResourceAuthorization",
		"value": "enabled",
	})
	doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "tagResourceAuthorization",
		"value": "disabled",
	})

	listResp := doECSRequest(t, h, "ListAccountSettings", map[string]any{
		"name": "tagResourceAuthorization",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	settings := listOut["settings"].([]any)
	require.NotEmpty(t, settings)
	// Last write wins
	found := false
	for _, s := range settings {
		setting := s.(map[string]any)
		if setting["name"] == "tagResourceAuthorization" {
			assert.Equal(t, "disabled", setting["value"])
			found = true
		}
	}
	assert.True(t, found)
}

func TestAccountSettings_TaskLongArnFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	putResp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "taskLongArnFormat",
		"value": "enabled",
	})
	require.Equal(t, http.StatusOK, putResp.Code)

	var putOut map[string]any
	require.NoError(t, json.Unmarshal(putResp.Body.Bytes(), &putOut))
	setting := putOut["setting"].(map[string]any)
	assert.Equal(t, "taskLongArnFormat", setting["name"])
	assert.Equal(t, "enabled", setting["value"])
}

func TestAccountSettings_ContainerInstanceLongArnFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	putResp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "containerInstanceLongArnFormat",
		"value": "enabled",
	})
	require.Equal(t, http.StatusOK, putResp.Code)

	var putOut map[string]any
	require.NoError(t, json.Unmarshal(putResp.Body.Bytes(), &putOut))
	setting := putOut["setting"].(map[string]any)
	assert.Equal(t, "containerInstanceLongArnFormat", setting["name"])
	assert.Equal(t, "enabled", setting["value"])
}

func TestAccountSettings_FargateSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	fargateSettings := []string{
		"fargateTaskRetirementWaitPeriod",
		"fargateFIPSMode",
	}

	for _, name := range fargateSettings {
		putResp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
			"name":  name,
			"value": "enabled",
		})
		require.Equal(t, http.StatusOK, putResp.Code, "PutAccountSetting %s", name)

		var putOut map[string]any
		require.NoError(t, json.Unmarshal(putResp.Body.Bytes(), &putOut))
		setting := putOut["setting"].(map[string]any)
		assert.Equal(t, name, setting["name"])
	}
}

func TestAccountSettings_ServiceLongArnFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	putResp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "serviceLongArnFormat",
		"value": "enabled",
	})
	require.Equal(t, http.StatusOK, putResp.Code)

	listResp := doECSRequest(t, h, "ListAccountSettings", map[string]any{
		"name": "serviceLongArnFormat",
	})
	require.Equal(t, http.StatusOK, listResp.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	settings := listOut["settings"].([]any)
	require.NotEmpty(t, settings)
	assert.Equal(t, "enabled", settings[0].(map[string]any)["value"])
}

func TestECS_DeleteAccountSetting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		seedKey  string
		seedName string
		wantCode int
	}{
		{
			name:     "delete existing setting",
			seedName: "serviceLongArnFormat",
			seedKey:  ":serviceLongArnFormat",
			input:    map[string]any{"name": "serviceLongArnFormat"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing name returns 400",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "non-existent setting returns 400",
			input:    map[string]any{"name": "nonExistentSetting"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

			if tt.seedName != "" {
				backend.AddAccountSettingInternal(tt.seedKey, &ecs.AccountSetting{
					Name:  tt.seedName,
					Value: "enabled",
				})
			}

			h := ecs.NewHandler(backend)
			rec := doECSRequest(t, h, "DeleteAccountSetting", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			setting, ok := resp["setting"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.seedName, setting["name"])
		})
	}
}

func TestECS_ListAccountSettings(t *testing.T) {
	t.Parallel()

	t.Run("empty list", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doECSRequest(t, h, "ListAccountSettings", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		settings, ok := resp["settings"].([]any)
		require.True(t, ok)
		assert.Empty(t, settings)
	})

	t.Run("lists all after put", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		// Seed two account settings.
		putRec := doECSRequest(t, h, "PutAccountSetting", map[string]any{
			"name": "containerInsights", "value": "enabled",
		})
		require.Equal(t, http.StatusOK, putRec.Code)

		putRec2 := doECSRequest(t, h, "PutAccountSetting", map[string]any{
			"name": "serviceLongArnFormat", "value": "enabled",
		})
		require.Equal(t, http.StatusOK, putRec2.Code)

		// List and verify.
		rec := doECSRequest(t, h, "ListAccountSettings", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		settings, ok := resp["settings"].([]any)
		require.True(t, ok)
		assert.Len(t, settings, 2)

		// Verify field shape of the first setting.
		first := settings[0].(map[string]any)
		assert.NotEmpty(t, first["name"])
		assert.NotEmpty(t, first["value"])
	})
}

func TestECS_PutAccountSetting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     map[string]any
		name      string
		wantName  string
		wantValue string
		wantCode  int
	}{
		{
			name:      "creates setting",
			input:     map[string]any{"name": "containerInstanceLongArnFormat", "value": "enabled"},
			wantCode:  http.StatusOK,
			wantName:  "containerInstanceLongArnFormat",
			wantValue: "enabled",
		},
		{
			name:     "missing name returns 400",
			input:    map[string]any{"value": "enabled"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing value returns 400",
			input:    map[string]any{"name": "containerInstanceLongArnFormat"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "PutAccountSetting", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			setting, ok := resp["setting"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantName, setting["name"])
			assert.Equal(t, tt.wantValue, setting["value"])
		})
	}
}

func TestECS_PutAccountSettingDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     map[string]any
		name      string
		wantName  string
		wantValue string
		wantCode  int
	}{
		{
			name:      "creates default setting",
			input:     map[string]any{"name": "taskLongArnFormat", "value": "enabled"},
			wantCode:  http.StatusOK,
			wantName:  "taskLongArnFormat",
			wantValue: "enabled",
		},
		{
			name:     "missing name returns 400",
			input:    map[string]any{"value": "enabled"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "PutAccountSettingDefault", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			setting, ok := resp["setting"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantName, setting["name"])
			assert.Equal(t, tt.wantValue, setting["value"])
		})
	}
}
