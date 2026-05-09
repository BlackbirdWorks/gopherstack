package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func doIAMAction(t *testing.T, h *iam.Handler, action string, params map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := iamRequest(action, params)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func TestIAM_SSHPublicKeys(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Create a user first
	doIAMAction(t, h, "CreateUser", map[string]string{"UserName": "ssh-user"})

	// UploadSSHPublicKey
	sshPubKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC... test@example.com"
	rec := doIAMAction(t, h, "UploadSSHPublicKey", map[string]string{
		"UserName":     "ssh-user",
		"SSHPublicKeyBody": sshPubKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var uploadResp struct {
		XMLName     xml.Name `xml:"UploadSSHPublicKeyResponse"`
		SSHPublicKey struct {
			SSHPublicKeyID string `xml:"SSHPublicKeyId"`
		} `xml:"UploadSSHPublicKeyResult>SSHPublicKey"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &uploadResp))
	keyID := uploadResp.SSHPublicKey.SSHPublicKeyID
	require.NotEmpty(t, keyID)

	// GetSSHPublicKey
	rec = doIAMAction(t, h, "GetSSHPublicKey", map[string]string{
		"UserName":       "ssh-user",
		"SSHPublicKeyId": keyID,
		"Encoding":       "SSH",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateSSHPublicKey
	rec = doIAMAction(t, h, "UpdateSSHPublicKey", map[string]string{
		"UserName":       "ssh-user",
		"SSHPublicKeyId": keyID,
		"Status":         "Inactive",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteSSHPublicKey
	rec = doIAMAction(t, h, "DeleteSSHPublicKey", map[string]string{
		"UserName":       "ssh-user",
		"SSHPublicKeyId": keyID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
