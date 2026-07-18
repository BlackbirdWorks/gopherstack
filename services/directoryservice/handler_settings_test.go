package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update then describe"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// UpdateSettings
			rec1 := doRequest(t, h, "UpdateSettings", map[string]any{
				"DirectoryId": dirID,
				"Settings":    []any{map[string]any{"Name": "TLS_1_0", "Value": "Disable"}},
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			assert.Equal(t, dirID, r1["DirectoryId"])

			// DescribeSettings
			rec2 := doRequest(t, h, "DescribeSettings", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			settings, _ := r2["SettingEntries"].([]any)
			require.Len(t, settings, 1)
			setting := settings[0].(map[string]any)
			assert.Equal(t, "TLS_1_0", setting["Name"])

			_ = tc
		})
	}
}

func TestUpdateDirectorySetup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update then describe"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// UpdateDirectorySetup
			rec1 := doRequest(t, h, "UpdateDirectorySetup", map[string]any{
				"DirectoryId":                dirID,
				"UpdateType":                 "OS",
				"CreateSnapshotBeforeUpdate": true,
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// DescribeUpdateDirectory
			rec2 := doRequest(t, h, "DescribeUpdateDirectory", map[string]any{
				"DirectoryId": dirID,
				"UpdateType":  "OS",
			})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			entries, _ := r2["UpdateDirectoryInfo"].([]any)
			assert.Len(t, entries, 1)

			_ = tc
		})
	}
}

func TestDirectoryDataAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "enable describe disable cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Describe before enable
			rec1 := doRequest(t, h, "DescribeDirectoryDataAccess", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			assert.Equal(t, "Disabled", r1["DirectoryDataAccessStatus"])

			// Enable
			rec2 := doRequest(t, h, "EnableDirectoryDataAccess", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)

			// Describe after enable
			rec3 := doRequest(t, h, "DescribeDirectoryDataAccess", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			assert.Equal(t, "Enabled", r3["DirectoryDataAccessStatus"])

			// Disable
			rec4 := doRequest(t, h, "DisableDirectoryDataAccess", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec4.Code)

			_ = tc
		})
	}
}
