package workspaces_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDirectoryModifyOps exercises the directory-scoped property-modify
// operations (certificate auth, SAML, self-service, streaming, workspace
// access, and workspace creation properties). The body key is "ResourceId"
// -- the real required member on every one of these Input structs, per
// aws-sdk-go-v2/service/workspaces v1.73.1 api_op_Modify*.go -- not
// "DirectoryId", which no real client sends for these six ops.
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
				"ResourceId": "d-cert",
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
				"ResourceId": "d-saml",
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
				"ResourceId": "d-selfservice",
				"SelfservicePermissions": map[string]any{
					"RestartWorkspace": "ENABLED",
				},
			},
		},
		{
			name: "ModifyStreamingProperties",
			op:   "ModifyStreamingProperties",
			body: map[string]any{
				"ResourceId": "d-streaming",
				"StreamingProperties": map[string]any{
					"StreamingExperiencePreferredProtocol": "TCP",
				},
			},
		},
		{
			name: "ModifyWorkspaceAccessProperties",
			op:   "ModifyWorkspaceAccessProperties",
			body: map[string]any{
				"ResourceId": "d-access",
				"WorkspaceAccessProperties": map[string]any{
					"DeviceTypeWindows": "ALLOW",
				},
			},
		},
		{
			name: "ModifyWorkspaceCreationProperties",
			op:   "ModifyWorkspaceCreationProperties",
			body: map[string]any{
				"ResourceId": "d-creation",
				"WorkspaceCreationProperties": map[string]any{
					"DefaultOu": "OU=Workspaces,DC=example,DC=com",
				},
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandlerWithBackend(t)

			// These ops require a registered directory (ResourceNotFoundException
			// otherwise) -- see TestDirectoryModifyOps_UnregisteredDirectory.
			// RegisterWorkspaceDirectory itself keys the directory ID under
			// "DirectoryId" (its own real required member); it just happens to
			// carry the same value as this test case's "ResourceId".
			doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
				"DirectoryId": tc.body["ResourceId"],
			})

			rec := doTargetRequest(t, h, tc.op, tc.body)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: expected 200, got %d: %s", tc.op, rec.Code, rec.Body)
			}
		})
	}
}

// TestDirectoryModifyOps_RejectsLegacyDirectoryIdKey proves the six
// directory-scoped property-modify ops read the resource identifier from
// "ResourceId" and no longer accept "DirectoryId" -- a real AWS client never
// sends "DirectoryId" for these ops (confirmed against
// awsAwsjson11_serializeOpDocumentModify*Input in the pinned SDK's
// serializers.go). Sending only the legacy key must behave exactly as if no
// identifier were sent at all: 404 ResourceNotFoundException, even though
// the directory referenced by the (ignored) "DirectoryId" key is
// registered. This fails if the handler ever reverts to reading
// "DirectoryId".
func TestDirectoryModifyOps_RejectsLegacyDirectoryIdKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "modifycertificatebasedauthproperties",
			op:   "ModifyCertificateBasedAuthProperties",
			body: map[string]any{
				"DirectoryId": "d-cert-legacy",
				"CertificateBasedAuthProperties": map[string]any{
					"Status": "ENABLED",
				},
			},
		},
		{
			name: "modifysamlproperties",
			op:   "ModifySamlProperties",
			body: map[string]any{
				"DirectoryId":    "d-saml-legacy",
				"SamlProperties": map[string]any{"Status": "ENABLED"},
			},
		},
		{
			name: "modifyselfservicepermissions",
			op:   "ModifySelfservicePermissions",
			body: map[string]any{
				"DirectoryId":            "d-selfservice-legacy",
				"SelfservicePermissions": map[string]any{"RestartWorkspace": "ENABLED"},
			},
		},
		{
			name: "modifystreamingproperties",
			op:   "ModifyStreamingProperties",
			body: map[string]any{
				"DirectoryId": "d-streaming-legacy",
				"StreamingProperties": map[string]any{
					"StreamingExperiencePreferredProtocol": "TCP",
				},
			},
		},
		{
			name: "modifyworkspaceaccessproperties",
			op:   "ModifyWorkspaceAccessProperties",
			body: map[string]any{
				"DirectoryId":               "d-access-legacy",
				"WorkspaceAccessProperties": map[string]any{"DeviceTypeWindows": "ALLOW"},
			},
		},
		{
			name: "modifyworkspacecreationproperties",
			op:   "ModifyWorkspaceCreationProperties",
			body: map[string]any{
				"DirectoryId": "d-creation-legacy",
				"WorkspaceCreationProperties": map[string]any{
					"DefaultOu": "OU=Workspaces,DC=example,DC=com",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandlerWithBackend(t)

			// Register the directory referenced by the legacy "DirectoryId"
			// key -- so a 404 can only mean the handler ignored that key,
			// not that the directory itself was unregistered.
			doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{
				"DirectoryId": tc.body["DirectoryId"],
			})

			rawBody, marshalErr := json.Marshal(tc.body)
			require.NoError(t, marshalErr)
			require.NotContains(
				t, string(rawBody), `"ResourceId"`,
				"test body must omit ResourceId to prove the legacy key alone is rejected",
			)

			rec := doTargetRequest(t, h, tc.op, tc.body)
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"%s: a body keyed \"DirectoryId\" (no \"ResourceId\") must 404, "+
					"proving the handler no longer reads the legacy key", tc.op)
		})
	}
}

// TestDirectoryModifyOps_UnregisteredDirectory verifies the directory-scoped
// property-modify operations reject a DirectoryId that was never registered
// via RegisterWorkspaceDirectory (previously silently accepted and even
// fabricated a settings row for the phantom ID -- ResourceNotFoundException
// is in every one of these operations' real error lists).
func TestDirectoryModifyOps_UnregisteredDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "ModifyEndpointEncryptionMode", op: "ModifyEndpointEncryptionMode",
			body: map[string]any{"DirectoryId": "d-unregistered", "EndpointEncryptionMode": "SAME_AS_REGION"},
		},
		{
			name: "ModifyCertificateBasedAuthProperties", op: "ModifyCertificateBasedAuthProperties",
			body: map[string]any{
				"ResourceId":                     "d-unregistered",
				"CertificateBasedAuthProperties": map[string]any{"Status": "ENABLED"},
			},
		},
		{
			name: "ModifySamlProperties", op: "ModifySamlProperties",
			body: map[string]any{
				"ResourceId":     "d-unregistered",
				"SamlProperties": map[string]any{"Status": "ENABLED"},
			},
		},
		{
			name: "ModifySelfservicePermissions", op: "ModifySelfservicePermissions",
			body: map[string]any{
				"ResourceId":             "d-unregistered",
				"SelfservicePermissions": map[string]any{"RestartWorkspace": "ENABLED"},
			},
		},
		{
			name: "ModifyStreamingProperties", op: "ModifyStreamingProperties",
			body: map[string]any{
				"ResourceId": "d-unregistered",
				"StreamingProperties": map[string]any{
					"StreamingExperiencePreferredProtocol": "TCP",
				},
			},
		},
		{
			name: "ModifyWorkspaceAccessProperties", op: "ModifyWorkspaceAccessProperties",
			body: map[string]any{
				"ResourceId":                "d-unregistered",
				"WorkspaceAccessProperties": map[string]any{"DeviceTypeWindows": "ALLOW"},
			},
		},
		{
			name: "ModifyWorkspaceCreationProperties", op: "ModifyWorkspaceCreationProperties",
			body: map[string]any{
				"ResourceId": "d-unregistered",
				"WorkspaceCreationProperties": map[string]any{
					"DefaultOu": "OU=Workspaces,DC=example,DC=com",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandlerWithBackend(t)

			rec := doTargetRequest(t, h, tc.op, tc.body)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s: expected 404, got %d: %s", tc.op, rec.Code, rec.Body)
			}
		})
	}
}
