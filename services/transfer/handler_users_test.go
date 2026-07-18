package transfer_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_ListUsersSshPublicKeyCountAndHomeDirectoryType verifies that ListUsers returns
// the correct SshPublicKeyCount and HomeDirectoryType for each user. Real AWS includes both
// fields in the list response.
func TestHandler_ListUsersSshPublicKeyCountAndHomeDirectoryType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createSrvRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createSrvRec.Code)

	var srvOut struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(t, json.Unmarshal(createSrvRec.Body.Bytes(), &srvOut))

	createUserRec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId":          srvOut.ServerID,
		"UserName":          "alice",
		"Role":              "arn:aws:iam::123456789012:role/TransferRole",
		"HomeDirectoryType": "LOGICAL",
	})
	require.Equal(t, http.StatusOK, createUserRec.Code, "CreateUser failed: %s", createUserRec.Body.String())

	importRec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         srvOut.ServerID,
		"UserName":         "alice",
		"SshPublicKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC test@test",
	})
	require.Equal(t, http.StatusOK, importRec.Code, "ImportSshPublicKey failed: %s", importRec.Body.String())

	listRec := doTransferRequest(t, h, "ListUsers", map[string]any{
		"ServerId": srvOut.ServerID,
	})
	require.Equal(t, http.StatusOK, listRec.Code, "ListUsers failed: %s", listRec.Body.String())

	var listOut struct {
		Users []struct {
			UserName          string `json:"UserName"`
			HomeDirectoryType string `json:"HomeDirectoryType"`
			SSHPublicKeyCount int    `json:"SshPublicKeyCount"`
		} `json:"Users"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Users, 1)

	assert.Equal(t, "LOGICAL", listOut.Users[0].HomeDirectoryType)
	assert.Equal(t, 1, listOut.Users[0].SSHPublicKeyCount)
}

// TestHandler_CreateUserOnNonexistentServer verifies 400 response.
func TestHandler_CreateUserOnNonexistentServer(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId": "s-doesnotexist",
		"UserName": "alice",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_UserARNFormat verifies DescribeUser returns the correct ARN format
// arn:aws:transfer:<region>:<account>:user/<serverId>/<userName>.
func TestHandler_UserARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	descRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "alice",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	user := descResp["User"].(map[string]any)

	// newTestHandler uses testAccountID = "123456789012" and testRegion = "us-east-1"
	expectedARN := fmt.Sprintf("arn:aws:transfer:us-east-1:123456789012:user/%s/alice", s.ServerID)
	assert.Equal(t, expectedARN, user["Arn"])
}

// Test 6: CreateUser with PosixProfile; DescribeUser echoes it.
func TestHandler_CreateUserPosixProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "posixuser",
		"Role":     "arn:aws:iam::000000000000:role/transfer",
		"PosixProfile": map[string]any{
			"Uid":           1000,
			"Gid":           1001,
			"SecondaryGids": []int64{2000, 2001},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "posixuser",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	user := descResp["User"].(map[string]any)
	posix := user["PosixProfile"].(map[string]any)

	assert.EqualValues(t, 1000, posix["Uid"])
	assert.EqualValues(t, 1001, posix["Gid"])
}

// Test 7: CreateUser with HomeDirectoryType=LOGICAL + mappings; DescribeUser echoes them.
func TestHandler_CreateUserHomeDirectoryLogical(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId":          s.ServerID,
		"UserName":          "logicaluser",
		"Role":              "arn:aws:iam::000000000000:role/transfer",
		"HomeDirectoryType": "LOGICAL",
		"HomeDirectoryMappings": []map[string]any{
			{"Entry": "/docs", "Target": "/bucket/docs"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "logicaluser",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	user := descResp["User"].(map[string]any)

	assert.Equal(t, "LOGICAL", user["HomeDirectoryType"])
	mappings := user["HomeDirectoryMappings"].([]any)
	require.Len(t, mappings, 1)
	m := mappings[0].(map[string]any)
	assert.Equal(t, "/docs", m["Entry"])
	assert.Equal(t, "/bucket/docs", m["Target"])
}

// Test 8: DescribeUser includes SshPublicKeys list after ImportSshPublicKey.
func TestHandler_DescribeUserIncludesSshPublicKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateUser(
		s.ServerID,
		"keyuser",
		"/home/keyuser",
		"arn:aws:iam::000000000000:role/transfer",
		nil,
	)
	require.NoError(t, err)

	// Import a real-looking SSH public key (ed25519 test key).
	importRec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         s.ServerID,
		"UserName":         "keyuser",
		"SshPublicKeyBody": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl test@example",
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	descRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "keyuser",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	user := descResp["User"].(map[string]any)
	keys, ok := user["SshPublicKeys"].([]any)
	require.True(t, ok, "SshPublicKeys must be present")
	assert.Len(t, keys, 1)
}

// Test 20: ListUsers with MaxResults=2 returns NextToken when more exist.
func TestHandler_ListUsersPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	// Create 3 users.
	for _, name := range []string{"alice", "bob", "carol"} {
		_, err = h.Backend.CreateUser(s.ServerID, name, "/home/"+name, "arn:aws:iam::000000000000:role/transfer", nil)
		require.NoError(t, err)
	}

	listRec := doTransferRequest(t, h, "ListUsers", map[string]any{
		"ServerId":   s.ServerID,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	users := listResp["Users"].([]any)
	assert.Len(t, users, 2)
	assert.NotEmpty(t, listResp["NextToken"], "NextToken must be set when more results exist")
}

// Test 30: UpdateUser PosixProfile can be updated via UpdateUser.
func TestHandler_UpdateUserPosixProfile(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)
	_, err = h.Backend.CreateUser(s.ServerID, "updateuser", "/home/u", "arn:aws:iam::000000000000:role/transfer", nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "UpdateUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "updateuser",
		"PosixProfile": map[string]any{
			"Uid": 9999,
			"Gid": 8888,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "updateuser",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	user := descResp["User"].(map[string]any)
	posix := user["PosixProfile"].(map[string]any)
	assert.EqualValues(t, 9999, posix["Uid"])
}

func TestHandler_UserCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create server
	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	// Create user
	createUserRec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId":      serverID,
		"UserName":      "alice",
		"HomeDirectory": "/alice",
		"Role":          "arn:aws:iam::123456789012:role/role",
	})
	assert.Equal(t, http.StatusOK, createUserRec.Code)

	var userResp map[string]any
	require.NoError(t, json.Unmarshal(createUserRec.Body.Bytes(), &userResp))
	assert.Equal(t, "alice", userResp["UserName"])

	// Describe user
	descUserRec := doTransferRequest(t, h, "DescribeUser", map[string]any{
		"ServerId": serverID,
		"UserName": "alice",
	})
	assert.Equal(t, http.StatusOK, descUserRec.Code)

	// List users
	listUsersRec := doTransferRequest(t, h, "ListUsers", map[string]any{
		"ServerId": serverID,
	})
	assert.Equal(t, http.StatusOK, listUsersRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listUsersRec.Body.Bytes(), &listResp))
	users := listResp["Users"].([]any)
	assert.Len(t, users, 1)

	// Update user
	updateUserRec := doTransferRequest(t, h, "UpdateUser", map[string]any{
		"ServerId":      serverID,
		"UserName":      "alice",
		"HomeDirectory": "/home/alice",
	})
	assert.Equal(t, http.StatusOK, updateUserRec.Code)

	// Delete user
	deleteUserRec := doTransferRequest(t, h, "DeleteUser", map[string]any{
		"ServerId": serverID,
		"UserName": "alice",
	})
	assert.Equal(t, http.StatusOK, deleteUserRec.Code)

	// List again - should be empty
	listUsersRec2 := doTransferRequest(t, h, "ListUsers", map[string]any{
		"ServerId": serverID,
	})
	assert.Equal(t, http.StatusOK, listUsersRec2.Code)

	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(listUsersRec2.Body.Bytes(), &listResp2))
	assert.Empty(t, listResp2["Users"])
}

func TestHandler_CreateUser_MissingFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing server id",
			body:     map[string]any{"UserName": "alice"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing username",
			body:     map[string]any{"ServerId": serverID},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "CreateUser", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListUsers_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"Domain": "S3", "EndpointType": "PUBLIC", "IdentityProviderType": "SERVICE_MANAGED",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	for _, username := range []string{"alice", "bob", "charlie"} {
		doTransferRequest(t, h, "CreateUser", map[string]any{
			"ServerId": serverID, "UserName": username, "Role": "arn:aws:iam::000000000000:role/test",
		})
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantCode      int
		wantMinCount  int
		wantNextToken bool
	}{
		{
			name:         "list all users",
			body:         map[string]any{"ServerId": serverID},
			wantCode:     http.StatusOK,
			wantMinCount: 3,
		},
		{
			name:          "list with maxResults=1",
			body:          map[string]any{"ServerId": serverID, "MaxResults": 1},
			wantCode:      http.StatusOK,
			wantMinCount:  1,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "ListUsers", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			users := resp["Users"].([]any)
			assert.GreaterOrEqual(t, len(users), tt.wantMinCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, resp["NextToken"])
			}
		})
	}
}

func TestHandler_DescribeDeleteUpdateUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "describe user",
			action:   "DescribeUser",
			body:     map[string]any{"UserName": "testuser"},
			wantCode: http.StatusOK,
		},
		{
			name:     "describe user missing username",
			action:   "DescribeUser",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update user",
			action:   "UpdateUser",
			body:     map[string]any{"UserName": "testuser"},
			wantCode: http.StatusOK,
		},
		{
			name:     "update user missing username",
			action:   "UpdateUser",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete user",
			action:   "DeleteUser",
			body:     map[string]any{"UserName": "testuser"},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete user missing id",
			action:   "DeleteUser",
			body:     map[string]any{"UserName": "testuser"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createServerRec := doTransferRequest(t, h, "CreateServer", map[string]any{
				"Domain": "S3", "EndpointType": "PUBLIC", "IdentityProviderType": "SERVICE_MANAGED",
			})
			require.Equal(t, http.StatusOK, createServerRec.Code)

			var serverResp map[string]any
			require.NoError(t, json.Unmarshal(createServerRec.Body.Bytes(), &serverResp))
			serverID := serverResp["ServerId"].(string)

			doTransferRequest(t, h, "CreateUser", map[string]any{
				"ServerId": serverID,
				"UserName": "testuser",
				"Role":     "arn:aws:iam::000000000000:role/test",
			})

			body := make(map[string]any, len(tt.body)+1)
			maps.Copy(body, tt.body)

			if _, hasServerID := body["ServerId"]; !hasServerID && tt.wantCode != http.StatusBadRequest {
				body["ServerId"] = serverID
			}

			rec := doTransferRequest(t, h, tt.action, body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
