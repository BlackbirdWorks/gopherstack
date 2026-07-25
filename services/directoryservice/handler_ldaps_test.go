package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLDAPS(t *testing.T) {
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

			// Enable
			rec1 := doRequest(t, h, "EnableLDAPS", map[string]any{
				"DirectoryId": dirID,
				"Type":        "Client",
			})
			assert.Equal(t, http.StatusOK, rec1.Code)

			// Describe
			rec2 := doRequest(t, h, "DescribeLDAPSSettings", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			settings, _ := r2["LDAPSSettingsInfo"].([]any)
			require.Len(t, settings, 1)
			setting := settings[0].(map[string]any)
			assert.Equal(t, "Enabled", setting["LDAPSStatus"])

			// Disable
			rec3 := doRequest(t, h, "DisableLDAPS", map[string]any{
				"DirectoryId": dirID,
				"Type":        "Client",
			})
			assert.Equal(t, http.StatusOK, rec3.Code)

			_ = tc
		})
	}
}

func TestEnableLDAPS_InvalidType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dirID := mustCreateSimpleAD(t, h, "corp.example.com")

	rec := doRequest(t, h, "EnableLDAPS", map[string]any{
		"DirectoryId": dirID,
		"Type":        "Server",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "InvalidParameterException", body["__type"])
}
