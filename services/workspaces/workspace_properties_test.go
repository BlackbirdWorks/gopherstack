package workspaces_test

import (
	"net/http"
	"testing"
)

// TestDirectoryModifyOps exercises the directory-scoped property-modify
// operations (certificate auth, SAML, self-service, streaming, workspace
// access, and workspace creation properties).
func TestDirectoryModifyOps(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "ModifyCertificateBasedAuthProperties",
			op:   "ModifyCertificateBasedAuthProperties",
			body: map[string]any{
				"DirectoryId": "d-cert",
				"CertificateBasedAuthProperties": map[string]any{
					"Status":                  "ENABLED",
					"CertificateAuthorityArn": "arn:aws:acm:us-east-1:123:ca/abc",
				},
			},
		},
		{
			name: "ModifySamlProperties",
			op:   "ModifySamlProperties",
			body: map[string]any{
				"DirectoryId": "d-saml",
				"SamlProperties": map[string]any{
					"Status":        "ENABLED",
					"UserAccessUrl": "https://saml.example.com",
				},
			},
		},
		{
			name: "ModifySelfservicePermissions",
			op:   "ModifySelfservicePermissions",
			body: map[string]any{
				"DirectoryId": "d-selfservice",
				"SelfservicePermissions": map[string]any{
					"RestartWorkspace": "ENABLED",
				},
			},
		},
		{
			name: "ModifyStreamingProperties",
			op:   "ModifyStreamingProperties",
			body: map[string]any{
				"DirectoryId": "d-streaming",
				"StreamingProperties": map[string]any{
					"StreamingExperiencePreferredProtocol": "TCP",
				},
			},
		},
		{
			name: "ModifyWorkspaceAccessProperties",
			op:   "ModifyWorkspaceAccessProperties",
			body: map[string]any{
				"DirectoryId": "d-access",
				"WorkspaceAccessProperties": map[string]any{
					"DeviceTypeWindows": "ALLOW",
				},
			},
		},
		{
			name: "ModifyWorkspaceCreationProperties",
			op:   "ModifyWorkspaceCreationProperties",
			body: map[string]any{
				"DirectoryId": "d-creation",
				"WorkspaceCreationProperties": map[string]any{
					"DefaultOu": "OU=Workspaces,DC=example,DC=com",
				},
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			rec := doTargetRequest(t, h, tc.op, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: expected 200, got %d: %s", tc.op, rec.Code, rec.Body)
			}
		})
	}
}
