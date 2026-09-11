package iam_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

const testSSHKeyBody = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC test-dispatch-key"

// newTestSSHKeyBody returns a real authorized_keys-format SSH public key,
// required for GetSSHPublicKey's Encoding=SSH conversion path
// (convertSSHPublicKeyEncoding, ssh_keys.go:163) to succeed.
func newTestSSHKeyBody(t *testing.T) string {
	t.Helper()

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	sshPub, err := ssh.NewPublicKey(&rsaKey.PublicKey)
	require.NoError(t, err)

	return string(ssh.MarshalAuthorizedKey(sshPub))
}

// TestUploadSSHPublicKey_Dispatch, TestGetSSHPublicKey_Dispatch,
// TestUpdateSSHPublicKey_Dispatch and TestDeleteSSHPublicKey_Dispatch pin the
// survivors of the gopherstack-f185i dispatch-table collision:
// iamSSHKeyUploadGetDispatch / iamSSHKeyListDeleteDispatch win over the now-
// deleted iamSSHKeyCompletenessDispatch duplicates. These four operations
// were behaviour-neutral -- the deleted entries called the same backend
// methods with the same parameters -- so these are regression guards against
// losing the dispatch entry entirely, not winner/loser distinctions.

func TestUploadSSHPublicKey_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend)
		name        string
		userName    string
		wantErrCode string
		wantCode    int
	}{
		{
			name: "success",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("ssh-upload-user", "/", "")
			},
			userName: "ssh-upload-user",
			wantCode: http.StatusOK,
		},
		{
			name:        "unknown_user_returns_nosuchentity",
			setup:       func(_ *iam.InMemoryBackend) {},
			userName:    "ssh-upload-ghost",
			wantCode:    http.StatusNotFound,
			wantErrCode: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(b)

			req := iamRequest("UploadSSHPublicKey", map[string]string{
				"UserName":         tt.userName,
				"SSHPublicKeyBody": testSSHKeyBody,
			})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "Active")
			}

			if tt.wantErrCode != "" {
				var errResp iam.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrCode, errResp.Error.Code)
			}
		})
	}
}

func TestGetSSHPublicKey_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, b *iam.InMemoryBackend) (userName, keyID, body string)
		name        string
		wantErrCode string
		wantCode    int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *iam.InMemoryBackend) (string, string, string) {
				t.Helper()

				_, _ = b.CreateUser("ssh-get-user", "/", "")
				body := newTestSSHKeyBody(t)
				key, _ := b.UploadSSHPublicKey("ssh-get-user", body)

				return "ssh-get-user", key.SSHPublicKeyID, body
			},
			wantCode: http.StatusOK,
		},
		{
			name: "unknown_key_returns_nosuchentity",
			setup: func(_ *testing.T, b *iam.InMemoryBackend) (string, string, string) {
				_, _ = b.CreateUser("ssh-get-user2", "/", "")

				return "ssh-get-user2", "APKAMISSING", ""
			},
			wantCode:    http.StatusNotFound,
			wantErrCode: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			userName, keyID, body := tt.setup(t, b)

			req := iamRequest("GetSSHPublicKey", map[string]string{
				"UserName":       userName,
				"SSHPublicKeyId": keyID,
				"Encoding":       "SSH",
			})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp getSSHPublicKeyResponseXML
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, body, resp.GetSSHPublicKeyResult.SSHPublicKey.SSHPublicKeyBody)
			}

			if tt.wantErrCode != "" {
				var errResp iam.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrCode, errResp.Error.Code)
			}
		})
	}
}

func TestUpdateSSHPublicKey_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend) (userName, keyID string)
		name        string
		wantErrCode string
		wantCode    int
	}{
		{
			name: "success",
			setup: func(b *iam.InMemoryBackend) (string, string) {
				_, _ = b.CreateUser("ssh-update-user", "/", "")
				key, _ := b.UploadSSHPublicKey("ssh-update-user", testSSHKeyBody)

				return "ssh-update-user", key.SSHPublicKeyID
			},
			wantCode: http.StatusOK,
		},
		{
			name: "unknown_key_returns_nosuchentity",
			setup: func(b *iam.InMemoryBackend) (string, string) {
				_, _ = b.CreateUser("ssh-update-user2", "/", "")

				return "ssh-update-user2", "APKAMISSING"
			},
			wantCode:    http.StatusNotFound,
			wantErrCode: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			userName, keyID := tt.setup(b)

			req := iamRequest("UpdateSSHPublicKey", map[string]string{
				"UserName":       userName,
				"SSHPublicKeyId": keyID,
				"Status":         "Inactive",
			})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				key, err := b.GetSSHPublicKey(userName, keyID)
				require.NoError(t, err)
				assert.Equal(t, "Inactive", key.Status)
			}

			if tt.wantErrCode != "" {
				var errResp iam.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrCode, errResp.Error.Code)
			}
		})
	}
}

func TestDeleteSSHPublicKey_Dispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend) (userName, keyID string)
		name        string
		wantErrCode string
		wantCode    int
	}{
		{
			name: "success",
			setup: func(b *iam.InMemoryBackend) (string, string) {
				_, _ = b.CreateUser("ssh-delete-user", "/", "")
				key, _ := b.UploadSSHPublicKey("ssh-delete-user", testSSHKeyBody)

				return "ssh-delete-user", key.SSHPublicKeyID
			},
			wantCode: http.StatusOK,
		},
		{
			name: "unknown_key_returns_nosuchentity",
			setup: func(b *iam.InMemoryBackend) (string, string) {
				_, _ = b.CreateUser("ssh-delete-user2", "/", "")

				return "ssh-delete-user2", "APKAMISSING"
			},
			wantCode:    http.StatusNotFound,
			wantErrCode: "NoSuchEntity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			userName, keyID := tt.setup(b)

			req := iamRequest("DeleteSSHPublicKey", map[string]string{
				"UserName":       userName,
				"SSHPublicKeyId": keyID,
			})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				_, err := b.GetSSHPublicKey(userName, keyID)
				require.Error(t, err)
			}

			if tt.wantErrCode != "" {
				var errResp iam.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantErrCode, errResp.Error.Code)
			}
		})
	}
}

// TestListSSHPublicKeys_Dispatch pins the actual distinguishing behaviour
// between the surviving iamSSHKeyListDeleteDispatch entry and the deleted
// iamSSHKeyCompletenessDispatch duplicate: the deleted entry hardcoded
// iamDefaultMaxItems (ignoring the client's MaxItems) and never echoed the
// continuation Marker in the response (listSSHPublicKeysResult.Marker was
// left as its zero value), breaking pagination per
// api_op_ListSSHPublicKeys.go:37-53's MaxItems/Marker contract.
func TestListSSHPublicKeys_Dispatch(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("ssh-list-dispatch-user", "/", "")

	for range 2 {
		_, err := b.UploadSSHPublicKey("ssh-list-dispatch-user", testSSHKeyBody)
		require.NoError(t, err)
	}

	req := iamRequest("ListSSHPublicKeys", map[string]string{
		"UserName": "ssh-list-dispatch-user",
		"MaxItems": "1",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Marker      string     `xml:"Marker"`
			Keys        []struct{} `xml:"SSHPublicKeys>member"`
			IsTruncated bool       `xml:"IsTruncated"`
		} `xml:"ListSSHPublicKeysResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Len(t, resp.Result.Keys, 1, "MaxItems=1 must be respected, not ignored in favor of the default page size")
	assert.True(t, resp.Result.IsTruncated)
	assert.NotEmpty(t, resp.Result.Marker, "IsTruncated=true must carry a continuation Marker")
}
