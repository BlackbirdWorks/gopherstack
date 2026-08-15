package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSharedDirectories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "share accept describe unshare"},
		{name: "share reject"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Share
			rec1 := doRequest(t, h, "ShareDirectory", map[string]any{
				"DirectoryId": dirID,
				"ShareMethod": "HANDSHAKE",
				"ShareTarget": map[string]any{"Id": "111111111111", "Type": "ACCOUNT"},
			})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			sharedDirID, _ := r1["SharedDirectoryId"].(string)
			assert.NotEmpty(t, sharedDirID)

			if tc.name == "share accept describe unshare" {
				// Accept
				rec2 := doRequest(t, h, "AcceptSharedDirectory", map[string]any{
					"SharedDirectoryId": sharedDirID,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
				var r2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
				sd, _ := r2["SharedDirectory"].(map[string]any)
				require.NotNil(t, sd, "AcceptSharedDirectoryOutput.SharedDirectory must be present")
				assert.Equal(t, sharedDirID, sd["SharedDirectoryId"])
				assert.Equal(t, dirID, sd["OwnerDirectoryId"])
				assert.Equal(t, "111111111111", sd["SharedAccountId"])
				assert.Equal(t, "HANDSHAKE", sd["ShareMethod"])
				assert.Equal(t, "Shared", sd["ShareStatus"])
				assert.NotEmpty(t, sd["OwnerAccountId"])
				assert.NotZero(t, sd["CreatedDateTime"])
				assert.NotZero(t, sd["LastUpdatedDateTime"])

				// Describe
				rec3 := doRequest(t, h, "DescribeSharedDirectories", map[string]any{
					"OwnerDirectoryId": dirID,
				})
				assert.Equal(t, http.StatusOK, rec3.Code)
				var r3 map[string]any
				require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
				dirs, _ := r3["SharedDirectories"].([]any)
				assert.Len(t, dirs, 1)

				// Unshare
				rec4 := doRequest(t, h, "UnshareDirectory", map[string]any{
					"DirectoryId":   dirID,
					"UnshareTarget": map[string]any{"Id": "111111111111", "Type": "ACCOUNT"},
				})
				assert.Equal(t, http.StatusOK, rec4.Code)
			} else {
				// Reject
				rec2 := doRequest(t, h, "RejectSharedDirectory", map[string]any{
					"SharedDirectoryId": sharedDirID,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
				var r2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
				assert.Equal(t, sharedDirID, r2["SharedDirectoryId"])
			}
		})
	}
}

// TestShareDirectory_StatusLifecycle guards two ShareStatus bugs in the
// shared-directory family:
//
//  1. ShareDirectory with the (default) HANDSHAKE method must start
//     PendingAcceptance, since AWS requires the consumer account to call
//     AcceptSharedDirectory before the share becomes Shared -- an
//     ORGANIZATIONS-method share needs no handshake and starts Shared
//     immediately.
//  2. RejectSharedDirectory must transition the share to the terminal
//     "Rejected" state (the successful outcome), not "RejectFailed" (which
//     is the AWS enum value for a FAILED reject attempt) -- the previous
//     code inverted this, marking every successful reject as if it had
//     failed.
func TestShareDirectory_StatusLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		shareMethod    string
		wantAfterShare string
		name           string
	}{
		{name: "HANDSHAKE starts PendingAcceptance", shareMethod: "HANDSHAKE", wantAfterShare: "PendingAcceptance"},
		{name: "ORGANIZATIONS starts Shared", shareMethod: "ORGANIZATIONS", wantAfterShare: "Shared"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			shareRec := doRequest(t, h, "ShareDirectory", map[string]any{
				"DirectoryId": dirID,
				"ShareMethod": tt.shareMethod,
				"ShareTarget": map[string]any{"Id": "222222222222", "Type": "ACCOUNT"},
			})
			require.Equal(t, http.StatusOK, shareRec.Code)

			descRec := doRequest(t, h, "DescribeSharedDirectories", map[string]any{"OwnerDirectoryId": dirID})
			resp := respBody(t, descRec)
			list, _ := resp["SharedDirectories"].([]any)
			require.Len(t, list, 1)
			entry, _ := list[0].(map[string]any)
			assert.Equal(t, tt.wantAfterShare, entry["ShareStatus"])
		})
	}

	t.Run("RejectSharedDirectory transitions to Rejected", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := mustCreateSimpleAD(t, h, "corp.example.com")

		shareRec := doRequest(t, h, "ShareDirectory", map[string]any{
			"DirectoryId": dirID,
			"ShareMethod": "HANDSHAKE",
			"ShareTarget": map[string]any{"Id": "222222222222", "Type": "ACCOUNT"},
		})
		require.Equal(t, http.StatusOK, shareRec.Code)
		var shareResp map[string]any
		require.NoError(t, json.Unmarshal(shareRec.Body.Bytes(), &shareResp))
		sharedID, _ := shareResp["SharedDirectoryId"].(string)
		require.NotEmpty(t, sharedID)

		rejectRec := doRequest(t, h, "RejectSharedDirectory", map[string]any{"SharedDirectoryId": sharedID})
		require.Equal(t, http.StatusOK, rejectRec.Code)

		descRec := doRequest(t, h, "DescribeSharedDirectories", map[string]any{"OwnerDirectoryId": dirID})
		resp := respBody(t, descRec)
		list, _ := resp["SharedDirectories"].([]any)
		require.Len(t, list, 1)
		entry, _ := list[0].(map[string]any)
		assert.Equal(t, "Rejected", entry["ShareStatus"])
	})
}
