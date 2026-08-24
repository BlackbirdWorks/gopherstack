package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataSync_Smb covers the SMB location lifecycle.
func TestDataSync_Smb(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	agentArn := createTestAgent(t, h)

	// Create
	rec := doRequest(t, h, "CreateLocationSmb", map[string]any{
		"ServerHostname": "smb.example.com",
		"Subdirectory":   "/share/data",
		"Domain":         "CORP",
		"User":           "smbuser",
		"Password":       "smbpass",
		"MountOptions":   map[string]any{"Version": "SMB3"},
		"AgentArns":      []string{agentArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	// Describe
	rec = doRequest(t, h, "DescribeLocationSmb", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "smb://")
	assert.Equal(t, "CORP", descResp["Domain"])
	assert.Equal(t, "smbuser", descResp["User"])
	assert.Equal(t, "NTLM", descResp["AuthenticationType"])
	assert.NotNil(t, descResp["MountOptions"])
	// Password not returned
	assert.Nil(t, descResp["Password"])
	// The real DescribeLocationSmbOutput has no top-level ServerHostname
	// field (folded into LocationUri only).
	assert.Nil(t, descResp["ServerHostname"])

	// Update
	rec = doRequest(t, h, "UpdateLocationSmb", map[string]any{
		"LocationArn":  locArn,
		"Subdirectory": "/share/updated",
		"Domain":       "NEWDOMAIN",
		"User":         "newuser",
		"Password":     "newpass",
		"MountOptions": map[string]any{"Version": "AUTOMATIC"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeLocationSmb", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "NEWDOMAIN", descResp["Domain"])
	mo := descResp["MountOptions"].(map[string]any)
	assert.Equal(t, "AUTOMATIC", mo["Version"])

	// Missing ServerHostname
	rec = doRequest(t, h, "CreateLocationSmb", map[string]any{
		"User": "u", "Password": "p",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Not found
	rec = doRequest(t, h, "DescribeLocationSmb", map[string]any{
		"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_UpdateLocationSmb_ServerHostname covers gopherstack-2xhy:
// UpdateLocationSmbInput.ServerHostname (aws-sdk-go-v2/service/datasync
// v1.61.4 api_op_UpdateLocationSmb.go:117) must update the location's
// LocationUri, not get silently dropped.
func TestDataSync_UpdateLocationSmb_ServerHostname(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update  map[string]any
		name    string
		wantURI string
	}{
		{
			name:    "hostname alone",
			update:  map[string]any{"ServerHostname": "new.example.com"},
			wantURI: "smb://new.example.com/share/data",
		},
		{
			name:    "hostname absent",
			update:  map[string]any{"Subdirectory": "/share/updated"},
			wantURI: "smb://smb.example.com/share/updated",
		},
		{
			name: "hostname with domain",
			update: map[string]any{
				"ServerHostname": "combo.example.com",
				"Domain":         "COMBODOMAIN",
			},
			wantURI: "smb://combo.example.com/share/data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			agentArn := createTestAgent(t, h)

			rec := doRequest(t, h, "CreateLocationSmb", map[string]any{
				"ServerHostname": "smb.example.com",
				"Subdirectory":   "/share/data",
				"User":           "smbuser",
				"Password":       "smbpass",
				"AgentArns":      []string{agentArn},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			locArn, ok := createResp["LocationArn"].(string)
			require.True(t, ok)

			tt.update["LocationArn"] = locArn
			rec = doRequest(t, h, "UpdateLocationSmb", tt.update)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "DescribeLocationSmb", map[string]any{"LocationArn": locArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			assert.Equal(t, tt.wantURI, descResp["LocationUri"])

			if domain, isSet := tt.update["Domain"]; isSet {
				assert.Equal(t, domain, descResp["Domain"])
			}
		})
	}
}
