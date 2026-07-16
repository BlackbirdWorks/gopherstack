package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ImportSSHPublicKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	// Create a user first.
	userRec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "testuser-ssh",
		"Role":     "arn:aws:iam::123456789012:role/TransferUserRole",
	})
	require.Equal(t, http.StatusOK, userRec.Code)

	rec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         s.ServerID,
		"UserName":         "testuser-ssh",
		"SshPublicKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC5 user-key",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteSSHPublicKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId": s.ServerID,
		"UserName": "testuser-del-ssh",
		"Role":     "arn:aws:iam::123456789012:role/TransferUserRole",
	})

	importRec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         s.ServerID,
		"UserName":         "testuser-del-ssh",
		"SshPublicKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC6 del-user-key",
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	var importResp map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importResp))
	sshKeyID := importResp["SshPublicKeyId"].(string)

	rec := doTransferRequest(t, h, "DeleteSshPublicKey", map[string]any{
		"ServerId":       s.ServerID,
		"UserName":       "testuser-del-ssh",
		"SshPublicKeyId": sshKeyID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// Test 9: ImportSshPublicKey with duplicate body returns ResourceExistsException.
func TestHandler_ImportSshPublicKeyDuplicateBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)
	_, err = h.Backend.CreateUser(s.ServerID, "dupuser", "/", "arn:aws:iam::000000000000:role/transfer", nil)
	require.NoError(t, err)

	keyBody := "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl dup@test"

	firstRec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         s.ServerID,
		"UserName":         "dupuser",
		"SshPublicKeyBody": keyBody,
	})
	require.Equal(t, http.StatusOK, firstRec.Code)

	// Second import of the same body must fail.
	secondRec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         s.ServerID,
		"UserName":         "dupuser",
		"SshPublicKeyBody": keyBody,
	})
	assert.Equal(t, http.StatusBadRequest, secondRec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(secondRec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceExistsException", errResp["__type"])
}
