package transfer_test

// handler_refinement2_test.go covers the 20+ correctness gaps addressed in the
// round-1 refinement pass.  Each test is focused on one specific AWS-accuracy
// requirement and is safe to run in parallel.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// --- DeleteServer correctness ---

// TestRefinement2_DeleteServerOnlineReturnsConflict verifies that DeleteServer
// returns a ConflictException (400) when the server is ONLINE.
func TestRefinement2_DeleteServerOnlineReturnsConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	// Server is ONLINE immediately after creation; delete should fail.
	delRec := doTransferRequest(t, h, "DeleteServer", map[string]any{"ServerId": serverID})
	assert.Equal(t, http.StatusBadRequest, delRec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &errResp))
	// The handler maps ErrConflict to ResourceExistsException.
	assert.Contains(t, errResp["__type"], "ResourceExistsException")
}

// TestRefinement2_DeleteServerBackendOnlineReturnsError verifies the backend-level
// ErrServerOnline sentinel.
func TestRefinement2_DeleteServerBackendOnlineReturnsError(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	err = b.DeleteServer(s.ServerID)
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrConflict)
	require.ErrorIs(t, err, transfer.ErrServerOnline)
}

// TestRefinement2_DeleteServerOfflineSucceeds verifies that stopping then deleting works.
func TestRefinement2_DeleteServerOfflineSucceeds(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.StopServer(s.ServerID))
	// Wait for async OFFLINE transition.
	for range 30 {
		got, _ := b.DescribeServer(s.ServerID)
		if got != nil && got.State == "OFFLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	require.NoError(t, b.DeleteServer(s.ServerID))
	assert.Equal(t, 0, transfer.ServerCount(b))
}

// TestRefinement2_DeleteServerAlsoDeletesSSHKeys verifies that SSH keys are removed
// when a user is deleted, and transitively when a server is deleted.
func TestRefinement2_DeleteServerAlsoDeletesSSHKeys(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	_, err = b.ImportSSHPublicKey(
		s.ServerID, "alice",
		"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl test@example",
	)
	require.NoError(t, err)
	assert.Equal(t, 1, transfer.SSHPublicKeyCount(b))

	// Stop and delete server.
	require.NoError(t, b.StopServer(s.ServerID))
	for range 30 {
		got, _ := b.DescribeServer(s.ServerID)
		if got != nil && got.State == "OFFLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	require.NoError(t, b.DeleteServer(s.ServerID))

	assert.Equal(t, 0, transfer.SSHPublicKeyCount(b))
}

// --- DeleteUser also removes SSH keys ---

// TestRefinement2_DeleteUserAlsoDeletesSSHKeys verifies that deleting a user
// removes all of that user's SSH public keys.
func TestRefinement2_DeleteUserAlsoDeletesSSHKeys(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	_, err = b.ImportSSHPublicKey(
		s.ServerID, "alice",
		"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl test@example",
	)
	require.NoError(t, err)
	assert.Equal(t, 1, transfer.SSHPublicKeyCount(b))

	require.NoError(t, b.DeleteUser(s.ServerID, "alice"))
	assert.Equal(t, 0, transfer.SSHPublicKeyCount(b))
}

// --- CreateUser on nonexistent server ---

// TestRefinement2_CreateUserOnNonexistentServer verifies ResourceNotFoundException.
func TestRefinement2_CreateUserOnNonexistentServer(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	_, err := b.CreateUser("s-doesnotexist", "alice", "/alice", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

// TestRefinement2_CreateUserOnNonexistentServerHTTP verifies 400 response.
func TestRefinement2_CreateUserOnNonexistentServerHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId": "s-doesnotexist",
		"UserName": "alice",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- StartServer/StopServer idempotency ---

// TestRefinement2_StartServerIdempotent verifies starting an ONLINE server is a no-op.
func TestRefinement2_StartServerIdempotent(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "ONLINE", s.State)

	// Starting an already-ONLINE server should not error.
	require.NoError(t, b.StartServer(s.ServerID))

	got, err := b.DescribeServer(s.ServerID)
	require.NoError(t, err)
	assert.Equal(t, "ONLINE", got.State)
}

// TestRefinement2_StopServerIdempotent verifies stopping an OFFLINE server is a no-op.
func TestRefinement2_StopServerIdempotent(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.StopServer(s.ServerID))
	for range 30 {
		got, _ := b.DescribeServer(s.ServerID)
		if got != nil && got.State == "OFFLINE" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Stopping an already-OFFLINE server should not error.
	require.NoError(t, b.StopServer(s.ServerID))

	got, err := b.DescribeServer(s.ServerID)
	require.NoError(t, err)
	assert.Equal(t, "OFFLINE", got.State)
}

// --- CreateAccess duplicate ExternalId ---

// TestRefinement2_CreateAccessDuplicateExternalID verifies ResourceExistsException.
func TestRefinement2_CreateAccessDuplicateExternalID(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAccess(s.ServerID, "S-1-5-21-9999", "", "", nil)
	require.NoError(t, err)

	// Second CreateAccess with same ExternalId should fail.
	_, err = b.CreateAccess(s.ServerID, "S-1-5-21-9999", "", "", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrConflict)
	require.ErrorIs(t, err, transfer.ErrAccessAlreadyExists)
}

// TestRefinement2_CreateAccessDuplicateExternalIDHTTP verifies 400 response.
func TestRefinement2_CreateAccessDuplicateExternalIDHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":   s.ServerID,
		"ExternalId": "S-1-5-21-9999",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":   s.ServerID,
		"ExternalId": "S-1-5-21-9999",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ImportSSHPublicKey 50-key limit ---

// TestRefinement2_ImportSSHPublicKey51stKeyReturnsError verifies the 50-key limit.
func TestRefinement2_ImportSSHPublicKey51stKeyReturnsError(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	// Import 50 distinct keys.
	for i := range 50 {
		// Generate a synthetic-but-distinct key body (not real, but unique).
		// We bypass SSH parsing by using the internal store directly via a
		// fake key body that looks unique.  The backend doesn't validate key
		// authenticity; it only checks for duplicates and the count limit.
		fakeKey := fmt.Sprintf("ssh-ed25519 AAAA%06d test%d@example", i, i)
		_, err = b.ImportSSHPublicKey(s.ServerID, "alice", fakeKey)
		require.NoError(t, err, "key %d should import successfully", i)
	}

	assert.Equal(t, 50, transfer.SSHPublicKeyCount(b))

	// 51st key should fail.
	_, err = b.ImportSSHPublicKey(s.ServerID, "alice", "ssh-ed25519 AAAA999999 extra@example")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrInvalidParameter)
}

// --- SendWorkflowStepState status validation ---

// TestRefinement2_SendWorkflowStepStateInvalidStatus verifies invalid status is rejected.
func TestRefinement2_SendWorkflowStepStateInvalidStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status string
		wantOK bool
	}{
		{status: "COMPLETE", wantOK: true},
		{status: "EXCEPTION", wantOK: true},
		{status: "SUCCESS", wantOK: false},   // old non-AWS value
		{status: "FAILURE", wantOK: false},   // old non-AWS value
		{status: "COMPLETED", wantOK: false}, // close but wrong
		{status: "", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			t.Parallel()

			b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			wf, err := b.CreateWorkflow("test", nil, nil, nil)
			require.NoError(t, err)

			exec, err := b.CreateExecution(wf.WorkflowID)
			require.NoError(t, err)

			err = b.SendWorkflowStepStateRecord(wf.WorkflowID, exec.ExecutionID, "tok-1", tt.status)
			if tt.wantOK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)
			}
		})
	}
}

// TestRefinement2_SendWorkflowStepStateInvalidStatusHTTP verifies HTTP 400.
func TestRefinement2_SendWorkflowStepStateInvalidStatusHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wf, err := h.Backend.CreateWorkflow("test", nil, nil, nil)
	require.NoError(t, err)

	exec, err := h.Backend.CreateExecution(wf.WorkflowID)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "SendWorkflowStepState", map[string]any{
		"WorkflowId":  wf.WorkflowID,
		"ExecutionId": exec.ExecutionID,
		"Token":       "tok-abc",
		"Status":      "INVALID_STATUS",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement2_SendWorkflowStepStateExceptionStatus verifies EXCEPTION is valid.
func TestRefinement2_SendWorkflowStepStateExceptionStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wf, err := h.Backend.CreateWorkflow("test", nil, nil, nil)
	require.NoError(t, err)

	exec, err := h.Backend.CreateExecution(wf.WorkflowID)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "SendWorkflowStepState", map[string]any{
		"WorkflowId":  wf.WorkflowID,
		"ExecutionId": exec.ExecutionID,
		"Token":       "tok-abc",
		"Status":      "EXCEPTION",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	updatedExec, err := h.Backend.DescribeExecution(wf.WorkflowID, exec.ExecutionID)
	require.NoError(t, err)
	assert.Equal(t, "EXCEPTION", updatedExec.Status)
}

// --- ListServers response fields ---

// TestRefinement2_ListServersIncludesIdentityProviderType verifies ListServers
// returns IdentityProviderType, EndpointType, Domain, and UserCount per server.
func TestRefinement2_ListServersIncludesIdentityProviderType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a server with a specific IdentityProviderType.
	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"IdentityProviderType": "SERVICE_MANAGED",
		"EndpointType":         "PUBLIC",
		"Domain":               "S3",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	serverID := createResp["ServerId"].(string)

	// Add a user to check UserCount.
	_, err := h.Backend.CreateUser(serverID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	listRec := doTransferRequest(t, h, "ListServers", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	servers := listResp["Servers"].([]any)
	require.Len(t, servers, 1)

	item := servers[0].(map[string]any)
	assert.Equal(t, serverID, item["ServerId"])
	assert.Equal(t, "SERVICE_MANAGED", item["IdentityProviderType"])
	assert.Equal(t, "PUBLIC", item["EndpointType"])
	assert.Equal(t, "S3", item["Domain"])
	assert.EqualValues(t, 1, item["UserCount"])
}

// --- DescribeServer UserCount ---

// TestRefinement2_DescribeServerUserCount verifies UserCount is populated.
func TestRefinement2_DescribeServerUserCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	// No users yet.
	descRec := doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": s.ServerID})
	require.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server := descResp["Server"].(map[string]any)
	assert.EqualValues(t, 0, server["UserCount"])

	// Add two users.
	_, err = h.Backend.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)
	_, err = h.Backend.CreateUser(s.ServerID, "bob", "/bob", "", nil)
	require.NoError(t, err)

	descRec = doTransferRequest(t, h, "DescribeServer", map[string]any{"ServerId": s.ServerID})
	require.Equal(t, http.StatusOK, descRec.Code)
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	server = descResp["Server"].(map[string]any)
	assert.EqualValues(t, 2, server["UserCount"])
}

// --- TagResource + ListTagsForResource round-trip ---

// TestRefinement2_TagResourceListTagsRoundTripServerARN verifies TagResource and
// ListTagsForResource work with server ARNs.
func TestRefinement2_TagResourceListTagsRoundTripServerARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	// Build the server ARN as the AWS SDK would.
	arnStr := fmt.Sprintf("arn:aws:transfer:us-east-1:000000000000:server/%s", s.ServerID)

	tagRec := doTransferRequest(t, h, "TagResource", map[string]any{
		"Arn": arnStr,
		"Tags": []map[string]any{
			{"Key": "Env", "Value": "prod"},
			{"Key": "Team", "Value": "infra"},
		},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doTransferRequest(t, h, "ListTagsForResource", map[string]any{
		"Arn": arnStr,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	tags := listResp["Tags"].([]any)
	tagMap := make(map[string]string)
	for _, t := range tags {
		entry := t.(map[string]any)
		tagMap[entry["Key"].(string)] = entry["Value"].(string)
	}

	assert.Equal(t, "prod", tagMap["Env"])
	assert.Equal(t, "infra", tagMap["Team"])
}

// TestRefinement2_UntagResourceRoundTrip verifies UntagResource removes specific tags.
func TestRefinement2_UntagResourceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	arnStr := fmt.Sprintf("arn:aws:transfer:us-east-1:000000000000:server/%s", s.ServerID)

	doTransferRequest(t, h, "TagResource", map[string]any{
		"Arn": arnStr,
		"Tags": []map[string]any{
			{"Key": "Env", "Value": "prod"},
			{"Key": "Team", "Value": "infra"},
		},
	})

	untagRec := doTransferRequest(t, h, "UntagResource", map[string]any{
		"Arn":     arnStr,
		"TagKeys": []string{"Team"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec := doTransferRequest(t, h, "ListTagsForResource", map[string]any{"Arn": arnStr})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	tags := listResp["Tags"].([]any)
	tagMap := make(map[string]string)
	for _, tg := range tags {
		entry := tg.(map[string]any)
		tagMap[entry["Key"].(string)] = entry["Value"].(string)
	}

	assert.Equal(t, "prod", tagMap["Env"])
	assert.NotContains(t, tagMap, "Team")
}

// --- User ARN format ---

// TestRefinement2_UserARNFormat verifies DescribeUser returns the correct ARN format
// arn:aws:transfer:<region>:<account>:user/<serverId>/<userName>.
func TestRefinement2_UserARNFormat(t *testing.T) {
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

// --- ListServers sort order ---

// TestRefinement2_ListServersSortedByServerID verifies ListServers returns servers
// sorted by ServerId in ascending order (deterministic).
func TestRefinement2_ListServersSortedByServerID(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")

	// Create multiple servers and collect their IDs.
	for range 5 {
		_, err := b.CreateServer(nil, nil)
		require.NoError(t, err)
	}

	servers := b.ListServers()
	require.Len(t, servers, 5)

	for i := 1; i < len(servers); i++ {
		assert.LessOrEqual(t, servers[i-1].ServerID, servers[i].ServerID,
			"servers not sorted at index %d", i)
	}
}

// --- CreateServer defaults ---

// TestRefinement2_CreateServerDefaultsIdentityProviderType verifies that
// IdentityProviderType defaults to SERVICE_MANAGED when not provided.
func TestRefinement2_CreateServerDefaultsIdentityProviderType(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, "SERVICE_MANAGED", s.IdentityProviderType)
}

// TestRefinement2_CreateServerDefaultsEndpointType verifies EndpointType defaults to PUBLIC.
func TestRefinement2_CreateServerDefaultsEndpointType(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, "PUBLIC", s.EndpointType)
}

// TestRefinement2_CreateServerDefaultsDomain verifies Domain defaults to S3.
func TestRefinement2_CreateServerDefaultsDomain(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, "S3", s.Domain)
}

// --- ServerUserCount interface method ---

// TestRefinement2_ServerUserCountMethod verifies the new ServerUserCount method.
func TestRefinement2_ServerUserCountMethod(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 0, b.ServerUserCount(s.ServerID))

	_, err = b.CreateUser(s.ServerID, "alice", "/alice", "", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, b.ServerUserCount(s.ServerID))

	// Non-existent server returns 0.
	assert.Equal(t, 0, b.ServerUserCount("s-doesnotexist"))
}
