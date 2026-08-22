package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testSSHHostKeyBody is a syntactically valid (but not cryptographically
// meaningful) ed25519 SSH host key body, sufficient for ImportHostKey which
// does not validate key authenticity.
const testSSHHostKeyBody = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl " +
	"test@example"

const testHostKeyEd25519 = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GkZS test@host"

func TestHandler_DescribeHostKeyIncludesFingerprint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	serverRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, serverRec.Code)
	var serverResp map[string]any
	require.NoError(t, json.Unmarshal(serverRec.Body.Bytes(), &serverResp))
	serverID := serverResp["ServerId"].(string)

	importRec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    serverID,
		"HostKeyBody": testHostKeyEd25519,
		"Description": "audit2 host key",
		"Tags":        []map[string]string{{"Key": "purpose", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, importRec.Code)
	var importResp map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importResp))
	hostKeyID := importResp["HostKeyId"].(string)

	rec := doTransferRequest(t, h, "DescribeHostKey", map[string]any{
		"ServerId":  serverID,
		"HostKeyId": hostKeyID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	hk := resp["HostKey"].(map[string]any)

	fp, hasFp := hk["HostKeyFingerprint"].(string)
	assert.True(t, hasFp, "HostKeyFingerprint must be present in DescribeHostKey response")
	assert.Contains(t, fp, "SHA256:", "HostKeyFingerprint must start with SHA256:")

	dateImported, hasDate := hk["DateImported"].(float64)
	assert.True(t, hasDate, "DateImported must be present in DescribeHostKey response as an epoch-seconds JSON number")
	assert.Positive(t, dateImported)

	arn, hasArn := hk["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in DescribeHostKey response")
	assert.Contains(t, arn, hostKeyID, "Arn must contain HostKeyId")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")

	tags, hasTags := hk["Tags"].([]any)
	assert.True(t, hasTags, "Tags must be present in DescribeHostKey response")
	assert.Len(t, tags, 1)
}

func TestHandler_ListHostKeysIncludesFingerprintAndArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	serverRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, serverRec.Code)
	var serverResp map[string]any
	require.NoError(t, json.Unmarshal(serverRec.Body.Bytes(), &serverResp))
	serverID := serverResp["ServerId"].(string)

	doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    serverID,
		"HostKeyBody": testHostKeyEd25519,
		"Description": "list test key",
	})

	rec := doTransferRequest(t, h, "ListHostKeys", map[string]any{
		"ServerId": serverID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	hostKeys := resp["HostKeys"].([]any)
	require.NotEmpty(t, hostKeys)

	item := hostKeys[0].(map[string]any)

	fp, hasFp := item["Fingerprint"].(string)
	assert.True(t, hasFp, "Fingerprint must be present in ListHostKeys items (ListedHostKey's real member name)")
	assert.Contains(t, fp, "SHA256:", "Fingerprint must start with SHA256:")

	dateImported, hasDate := item["DateImported"].(float64)
	assert.True(t, hasDate, "DateImported must be present in ListHostKeys items as an epoch-seconds JSON number")
	assert.Positive(t, dateImported)

	arn, hasArn := item["Arn"].(string)
	assert.True(t, hasArn, "Arn must be present in ListHostKeys items")
	assert.Contains(t, arn, "arn:aws:transfer:", "Arn must start with arn:aws:transfer:")
}

func TestHandler_ImportHostKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    s.ServerID,
		"HostKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC test-key",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListHostKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	rec := doTransferRequest(t, h, "ListHostKeys", map[string]any{
		"ServerId": s.ServerID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DescribeHostKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	importRec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    s.ServerID,
		"HostKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC2 desc-key",
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	var importResp map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importResp))
	hostKeyID := importResp["HostKeyId"].(string)

	rec := doTransferRequest(t, h, "DescribeHostKey", map[string]any{
		"ServerId":  s.ServerID,
		"HostKeyId": hostKeyID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteHostKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	importRec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    s.ServerID,
		"HostKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC3 del-key",
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	var importResp map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importResp))
	hostKeyID := importResp["HostKeyId"].(string)

	rec := doTransferRequest(t, h, "DeleteHostKey", map[string]any{
		"ServerId":  s.ServerID,
		"HostKeyId": hostKeyID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateHostKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	importRec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
		"ServerId":    s.ServerID,
		"HostKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC4 upd-key",
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	var importResp map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importResp))
	hostKeyID := importResp["HostKeyId"].(string)

	rec := doTransferRequest(t, h, "UpdateHostKey", map[string]any{
		"ServerId":    s.ServerID,
		"HostKeyId":   hostKeyID,
		"Description": "updated host key",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
