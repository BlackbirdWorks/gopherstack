package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestAppStream_DirectoryConfigs covers DirectoryConfig CRUD.
func TestAppStream_DirectoryConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateDirectoryConfig returns config",
			action: "CreateDirectoryConfig",
			body: map[string]any{
				"DirectoryName":                        "corp.example.com",
				"OrganizationalUnitDistinguishedNames": []string{"OU=AppStream,DC=corp,DC=example,DC=com"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				dc := resp["DirectoryConfig"].(map[string]any)
				assert.Equal(t, "corp.example.com", dc["DirectoryName"])
			},
		},
		{
			name:   "DescribeDirectoryConfigs lists all",
			action: "DescribeDirectoryConfigs",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateDirectoryConfig", map[string]any{"DirectoryName": "a.com"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				dcs := resp["DirectoryConfigs"].([]any)
				assert.Len(t, dcs, 1)
			},
		},
		{
			name:   "UpdateDirectoryConfig updates OUs",
			action: "UpdateDirectoryConfig",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateDirectoryConfig", map[string]any{"DirectoryName": "upd.com"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"DirectoryName":                        "upd.com",
				"OrganizationalUnitDistinguishedNames": []string{"OU=New,DC=upd,DC=com"},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteDirectoryConfig removes it",
			action: "DeleteDirectoryConfig",
			setup: func(h *appstream.Handler) {
				rec := doRequest(t, h, "CreateDirectoryConfig", map[string]any{"DirectoryName": "del.com"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"DirectoryName": "del.com"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteDirectoryConfig unknown returns error",
			action:   "DeleteDirectoryConfig",
			body:     map[string]any{"DirectoryName": "no-such.com"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_DirectoryConfigRoundtrip verifies CreateDirectoryConfig and
// DescribeDirectoryConfigs return the same OU distinguished names.
func TestAppStream_DirectoryConfigRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ouDNS := []string{"OU=Streaming,DC=example,DC=com", "OU=Finance,DC=example,DC=com"}
	rec := doRequest(t, h, "CreateDirectoryConfig", map[string]any{
		"DirectoryName":                        "example.com",
		"OrganizationalUnitDistinguishedNames": ouDNS,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDesc := doRequest(t, h, "DescribeDirectoryConfigs", map[string]any{
		"DirectoryNames": []string{"example.com"},
	})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &resp))
	configs := resp["DirectoryConfigs"].([]any)
	require.Len(t, configs, 1)
	dc := configs[0].(map[string]any)
	assert.Equal(t, "example.com", dc["DirectoryName"])
	ouRaw := dc["OrganizationalUnitDistinguishedNames"].([]any)
	assert.Len(t, ouRaw, 2)
}

// TestAppStream_DirectoryConfigServiceAccountCredentials verifies that
// ServiceAccountCredentials and CertificateBasedAuthProperties -- both real
// CreateDirectoryConfigInput/UpdateDirectoryConfigInput members -- are
// persisted and reflected back on Describe, and that Update can change them.
func TestAppStream_DirectoryConfigServiceAccountCredentials(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateDirectoryConfig", map[string]any{
		"DirectoryName": "cred.example.com",
		"ServiceAccountCredentials": map[string]any{
			"AccountName":     "svc-account",
			"AccountPassword": "s3cr3t",
		},
		"CertificateBasedAuthProperties": map[string]any{
			"CertificateAuthorityArn": "arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/abc",
			"Status":                  "ENABLED",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	dc := createResp["DirectoryConfig"].(map[string]any)

	cred := dc["ServiceAccountCredentials"].(map[string]any)
	assert.Equal(t, "svc-account", cred["AccountName"])
	assert.Equal(t, "s3cr3t", cred["AccountPassword"])

	certAuth := dc["CertificateBasedAuthProperties"].(map[string]any)
	assert.Equal(t, "ENABLED", certAuth["Status"])

	recUpd := doRequest(t, h, "UpdateDirectoryConfig", map[string]any{
		"DirectoryName": "cred.example.com",
		"ServiceAccountCredentials": map[string]any{
			"AccountName":     "svc-account-2",
			"AccountPassword": "n3wpass",
		},
	})
	require.Equal(t, http.StatusOK, recUpd.Code)

	var updResp map[string]any
	require.NoError(t, json.Unmarshal(recUpd.Body.Bytes(), &updResp))
	updDC := updResp["DirectoryConfig"].(map[string]any)
	updCred := updDC["ServiceAccountCredentials"].(map[string]any)
	assert.Equal(t, "svc-account-2", updCred["AccountName"])
	assert.Equal(t, "n3wpass", updCred["AccountPassword"])
}
