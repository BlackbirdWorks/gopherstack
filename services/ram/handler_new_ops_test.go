package ram_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// --- CreatePermission ---

func TestHandler_CreatePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(*testing.T, *ram.Handler)
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"name":           "my-perm",
				"resourceType":   "ec2:Subnet",
				"policyTemplate": `{"Effect":"Allow","Action":["ec2:*"]}`,
			},
			wantStatus: http.StatusOK,
			wantBody:   "my-perm",
		},
		{
			name: "missing name",
			body: map[string]any{
				"resourceType":   "ec2:Subnet",
				"policyTemplate": `{}`,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resourceType",
			body: map[string]any{
				"name":           "perm-no-type",
				"policyTemplate": `{}`,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing policyTemplate",
			body: map[string]any{
				"name":         "perm-no-policy",
				"resourceType": "ec2:Subnet",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *ram.Handler) {
				t.Helper()
				_, err := h.Backend.CreatePermission("dup-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)
			},
			body: map[string]any{
				"name":           "dup-perm",
				"resourceType":   "ec2:Subnet",
				"policyTemplate": `{}`,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRAMRequest(t, h, "/createpermission", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// --- CreatePermissionVersion ---

func TestHandler_CreatePermissionVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				p, err := h.Backend.CreatePermission("version-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return p.ARN
			},
			wantStatus: http.StatusOK,
			wantBody:   "version-perm",
		},
		{
			name: "missing permissionArn",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:permission/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			permARN := tt.setup(t, h)

			body := map[string]any{
				"policyTemplate": `{"Effect":"Allow","Action":["ec2:DescribeSubnets"]}`,
			}
			if permARN != "" {
				body["permissionArn"] = permARN
			}

			rec := doRAMRequest(t, h, "/createpermissionversion", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// --- DeletePermission ---

func TestHandler_DeletePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				p, err := h.Backend.CreatePermission("del-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return p.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:permission/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing permissionArn",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			permARN := tt.setup(t, h)

			path := "/deletepermission"
			if permARN != "" {
				path += "?permissionArn=" + permARN
			}

			rec := doRAMRawRequest(t, h, http.MethodDelete, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- DeletePermissionVersion ---

func TestHandler_DeletePermissionVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, int32)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) (string, int32) {
				t.Helper()
				p, err := h.Backend.CreatePermission("delpv-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)
				// Create a second version so v1 is not the only version.
				_, err = h.Backend.CreatePermissionVersion(p.ARN, `{"v":"2"}`)
				require.NoError(t, err)

				return p.ARN, 2
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "cannot delete default version",
			setup: func(t *testing.T, h *ram.Handler) (string, int32) {
				t.Helper()
				p, err := h.Backend.CreatePermission("del-default-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return p.ARN, 1
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not found permission",
			setup: func(_ *testing.T, _ *ram.Handler) (string, int32) {
				return "arn:aws:ram:us-east-1:000000000000:permission/nonexistent", 1
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing permissionArn",
			setup: func(_ *testing.T, _ *ram.Handler) (string, int32) {
				return "", 1
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			permARN, version := tt.setup(t, h)

			path := "/deletepermissionversion"
			if permARN != "" {
				path += fmt.Sprintf("?permissionArn=%s&permissionVersion=%d", permARN, version)
			}

			rec := doRAMRawRequest(t, h, http.MethodDelete, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- GetPermission ---

func TestHandler_GetPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, *int32)
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success_default_version",
			setup: func(t *testing.T, h *ram.Handler) (string, *int32) {
				t.Helper()
				p, err := h.Backend.CreatePermission("get-perm", "ec2:Subnet", `{"allow":"all"}`, nil)
				require.NoError(t, err)

				return p.ARN, nil
			},
			wantStatus: http.StatusOK,
			wantBody:   "get-perm",
		},
		{
			name: "success_specific_version",
			setup: func(t *testing.T, h *ram.Handler) (string, *int32) {
				t.Helper()
				p, err := h.Backend.CreatePermission("get-perm-v", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)
				_, err = h.Backend.CreatePermissionVersion(p.ARN, `{"v":"2"}`)
				require.NoError(t, err)
				v := int32(2)

				return p.ARN, &v
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing permissionArn",
			setup: func(_ *testing.T, _ *ram.Handler) (string, *int32) {
				return "", nil
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) (string, *int32) {
				return "arn:aws:ram:us-east-1:000000000000:permission/nonexistent", nil
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			permARN, version := tt.setup(t, h)

			body := map[string]any{}
			if permARN != "" {
				body["permissionArn"] = permARN
			}

			if version != nil {
				body["permissionVersion"] = *version
			}

			rec := doRAMRequest(t, h, "/getpermission", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// --- AssociateResourceSharePermission ---

func TestHandler_AssociateResourceSharePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("perm-share", true, nil, nil, nil)
				require.NoError(t, err)
				p, err := h.Backend.CreatePermission("assoc-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return rs.ARN, p.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "resource share not found",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				p, err := h.Backend.CreatePermission("assoc-perm2", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent", p.ARN
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "permission not found",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("perm-share2", true, nil, nil, nil)
				require.NoError(t, err)

				return rs.ARN, "arn:aws:ram:us-east-1:000000000000:permission/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resourceShareArn",
			setup: func(_ *testing.T, _ *ram.Handler) (string, string) {
				return "", "arn:aws:ram:us-east-1:000000000000:permission/some-perm"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN, permARN := tt.setup(t, h)

			body := map[string]any{
				"permissionArn": permARN,
			}
			if shareARN != "" {
				body["resourceShareArn"] = shareARN
			}

			rec := doRAMRequest(t, h, "/associateresourcesharepermission", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- DisassociateResourceSharePermission ---

func TestHandler_DisassociateResourceSharePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("disassoc-perm-share", true, nil, nil, nil)
				require.NoError(t, err)
				p, err := h.Backend.CreatePermission("disassoc-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)
				err = h.Backend.AssociateResourceSharePermission(rs.ARN, p.ARN, false, nil)
				require.NoError(t, err)

				return rs.ARN, p.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "resource share not found",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				p, err := h.Backend.CreatePermission("disassoc-perm2", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent", p.ARN
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resourceShareArn",
			setup: func(_ *testing.T, _ *ram.Handler) (string, string) {
				return "", "arn:aws:ram:us-east-1:000000000000:permission/some-perm"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN, permARN := tt.setup(t, h)

			body := map[string]any{
				"permissionArn": permARN,
			}
			if shareARN != "" {
				body["resourceShareArn"] = shareARN
			}

			rec := doRAMRequest(t, h, "/disassociateresourcesharepermission", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- AcceptResourceShareInvitation ---

func TestHandler_AcceptResourceShareInvitation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("invite-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"invite-share",
					"111111111111",
					"222222222222",
				)

				return inv.InvitationARN
			},
			wantStatus: http.StatusOK,
			wantBody:   "ACCEPTED",
		},
		{
			name: "already accepted",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("accepted-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"accepted-share",
					"111111111111",
					"222222222222",
				)
				_, err = h.Backend.AcceptResourceShareInvitation(inv.InvitationARN)
				require.NoError(t, err)

				return inv.InvitationARN
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share-invitation/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing invitationArn",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			invARN := tt.setup(t, h)

			body := map[string]any{}
			if invARN != "" {
				body["resourceShareInvitationArn"] = invARN
			}

			rec := doRAMRequest(t, h, "/acceptresourceshareinvitation", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// --- GetResourceShareInvitations ---

func TestHandler_GetResourceShareInvitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, string)
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "list all",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("inv-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"inv-share",
					"111111111111",
					"222222222222",
				)

				return inv.InvitationARN, rs.ARN
			},
			wantStatus: http.StatusOK,
			wantBody:   "resourceShareInvitations",
		},
		{
			name: "filter by invitationArn",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("inv-share2", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(
					h.Backend.(*ram.InMemoryBackend),
					rs.ARN,
					"inv-share2",
					"111111111111",
					"222222222222",
				)

				return inv.InvitationARN, ""
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "empty",
			setup: func(_ *testing.T, _ *ram.Handler) (string, string) {
				return "", ""
			},
			wantStatus: http.StatusOK,
			wantBody:   "resourceShareInvitations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			invARN, shareARN := tt.setup(t, h)

			body := map[string]any{}
			if invARN != "" {
				body["resourceShareInvitationArns"] = []string{invARN}
			}

			if shareARN != "" {
				body["resourceShareArns"] = []string{shareARN}
			}

			rec := doRAMRequest(t, h, "/getresourceshareinvitations", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// --- GetResourcePolicies ---

func TestHandler_GetResourcePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "with resources",
			body: map[string]any{
				"resourceArns": []string{
					"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc123",
				},
			},
			wantStatus: http.StatusOK,
			wantBody:   "policies",
		},
		{
			name:       "empty",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantBody:   "policies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRequest(t, h, "/getresourcepolicies", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// --- Persistence tests for new maps ---

func TestRAM_PersistenceNewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*testing.T, *ram.InMemoryBackend)
		verify func(*testing.T, *ram.InMemoryBackend)
		name   string
	}{
		{
			name: "permissions_preserved",
			setup: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()
				_, err := b.CreatePermission("perm-snap", "ec2:Subnet", `{"v":"1"}`, map[string]string{"env": "test"})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()
				permARN := "arn:aws:ram:us-east-1:123456789012:permission/perm-snap"
				p, pv, err := b.GetPermission(permARN, nil)
				require.NoError(t, err)
				assert.Equal(t, "perm-snap", p.Name)
				assert.JSONEq(t, `{"v":"1"}`, pv.PolicyTemplate)
				assert.Equal(t, "test", p.Tags["env"])
			},
		},
		{
			name: "invitations_preserved",
			setup: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()
				inv := &ram.ResourceShareInvitation{
					InvitationARN:     "arn:aws:ram:us-east-1:123456789012:resource-share-invitation/test-inv",
					ResourceShareARN:  "arn:aws:ram:us-east-1:123456789012:resource-share/test-share",
					ResourceShareName: "test-share",
					SenderAccountID:   "111111111111",
					ReceiverAccountID: "222222222222",
					Status:            "PENDING",
					CreationTime:      time.Now(),
					LastUpdatedTime:   time.Now(),
				}
				b.AddInvitationInternal(inv)
			},
			verify: func(t *testing.T, b *ram.InMemoryBackend) {
				t.Helper()
				invs := b.GetResourceShareInvitations(
					[]string{"arn:aws:ram:us-east-1:123456789012:resource-share-invitation/test-inv"},
					nil,
				)
				require.Len(t, invs, 1)
				assert.Equal(t, "PENDING", invs[0].Status)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(t, b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := ram.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

// --- ExtractOperation for new ops ---

func TestHandler_ExtractOperation_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "accept invitation", path: "/acceptresourceshareinvitation", want: "AcceptResourceShareInvitation"},
		{name: "associate perm", path: "/associateresourcesharepermission", want: "AssociateResourceSharePermission"},
		{name: "create permission", path: "/createpermission", want: "CreatePermission"},
		{name: "create permission version", path: "/createpermissionversion", want: "CreatePermissionVersion"},
		{name: "delete permission", path: "/deletepermission", want: "DeletePermission"},
		{name: "delete permission version", path: "/deletepermissionversion", want: "DeletePermissionVersion"},
		{
			name: "disassociate perm",
			path: "/disassociateresourcesharepermission",
			want: "DisassociateResourceSharePermission",
		},
		{name: "get permission", path: "/getpermission", want: "GetPermission"},
		{name: "get resource policies", path: "/getresourcepolicies", want: "GetResourcePolicies"},
		{name: "get invitations", path: "/getresourceshareinvitations", want: "GetResourceShareInvitations"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRequest(t, h, tt.path, map[string]any{})

			// The operation routing should succeed (not a 404) - just verify the handler
			// processes the path without routing to "Unknown".
			assert.NotEqual(t, http.StatusNotFound, rec.Code)
		})
	}
}
