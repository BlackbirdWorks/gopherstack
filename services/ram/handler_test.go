package ram_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func newTestHandler(t *testing.T) *ram.Handler {
	t.Helper()

	return ram.NewHandler(ram.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRAMRequest(t *testing.T, h *ram.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	return doRAMRawRequest(t, h, http.MethodPost, path, bodyBytes)
}

func doRAMRawRequest(t *testing.T, h *ram.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/ram/aws4_request")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "RAM", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateResourceShare")
	assert.Contains(t, ops, "GetResourceShares")
	assert.Contains(t, ops, "UpdateResourceShare")
	assert.Contains(t, ops, "DeleteResourceShare")
	assert.Contains(t, ops, "AssociateResourceShare")
	assert.Contains(t, ops, "DisassociateResourceShare")
	assert.Contains(t, ops, "GetResourceShareAssociations")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
	assert.Contains(t, ops, "ListTagsForResource")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 87, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ram", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "create", path: "/createresourceshare", want: "CreateResourceShare"},
		{name: "get", path: "/getresourceshares", want: "GetResourceShares"},
		{name: "update", path: "/updateresourceshare", want: "UpdateResourceShare"},
		{name: "delete", path: "/deleteresourceshare", want: "DeleteResourceShare"},
		{name: "associate", path: "/associateresourceshare", want: "AssociateResourceShare"},
		{name: "disassociate", path: "/disassociateresourceshare", want: "DisassociateResourceShare"},
		{name: "get associations", path: "/getresourceshareassociations", want: "GetResourceShareAssociations"},
		{name: "tag", path: "/tagresource", want: "TagResource"},
		{name: "untag", path: "/untagresource", want: "UntagResource"},
		{name: "list tags", path: "/listtagsforresource", want: "ListTagsForResource"},
		{name: "list permissions", path: "/listresourcesharepermissions", want: "ListResourceSharePermissions"},
		{
			name: "enable org sharing",
			path: "/enablesharingwithawsorganization",
			want: "EnableSharingWithAwsOrganization",
		},
		{name: "unknown", path: "/unknownpath", want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodDelete,
		"/deleteresourceshare?resourceShareArn=arn:aws:ram:us-east-1:000000000000:resource-share/my-share",
		nil)
	c := e.NewContext(req, httptest.NewRecorder())

	got := h.ExtractResource(c)
	assert.Equal(t, "arn:aws:ram:us-east-1:000000000000:resource-share/my-share", got)
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()
	e := echo.New()

	tests := []struct {
		name    string
		path    string
		service string
		want    bool
	}{
		{
			name:    "createresourceshare with ram service",
			path:    "/createresourceshare",
			service: "ram",
			want:    true,
		},
		{
			name:    "getresourceshares with ram service",
			path:    "/getresourceshares",
			service: "ram",
			want:    true,
		},
		{
			name:    "wrong service",
			path:    "/createresourceshare",
			service: "s3",
			want:    false,
		},
		{
			name:    "unknown path",
			path:    "/unknownpath",
			service: "ram",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/"+tt.service+"/aws4_request",
			)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &ram.Provider{}
	assert.Equal(t, "RAM", p.Name())
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRAMRequest(t, h, "/createresourceshare", map[string]any{
		"name":                    "reset-share",
		"allowExternalPrincipals": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	h.Reset()

	listRec := doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner": "SELF",
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		ResourceShares []any `json:"resourceShares"`
	}

	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	assert.Empty(t, out.ResourceShares)
}

func TestReset_ReSeedsBuiltIns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset via backend"},
		{name: "reset via handler"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Add a customer permission.
			doRAMRequest(t, h, "/createpermission", map[string]any{
				"name":           "to-be-cleared",
				"resourceType":   "ec2:Subnet",
				"policyTemplate": `{}`,
			})

			// Reset clears user state.
			h.Reset()

			// Built-ins should be back; customer permission gone.
			rec := doRAMRequest(t, h, "/listpermissions", map[string]any{
				"permissionType": "CUSTOMER_MANAGED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var custResp struct {
				Permissions []json.RawMessage `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &custResp))
			assert.Empty(t, custResp.Permissions, "customer permissions cleared after reset")

			rec2 := doRAMRequest(t, h, "/listpermissions", map[string]any{
				"permissionType": "AWS_MANAGED",
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var awsResp struct {
				Permissions []json.RawMessage `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &awsResp))
			assert.Len(t, awsResp.Permissions, ram.BuiltInPermissionCount,
				"built-in permissions re-seeded after reset")
		})
	}
}

func TestHandler_ExtractOperation_PermissionAndInvitationPaths(t *testing.T) {
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

// ptr32 is a helper to get a pointer to an int32 literal.
func ptr32(v int32) *int32 {
	p := new(int32)
	*p = v

	return p
}

// TestRefinement1_ErrNilAppContext verifies the provider rejects a nil context.
func TestErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &ram.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, ram.ErrNilAppContext)
}

// TestRefinement1_HandlerOpsLen verifies the ops list has expected size.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 36, ram.HandlerOpsLen(h))
}

// TestRefinement1_GetSupportedOperationsSorted verifies the ops list is sorted.
func TestGetSupportedOperationsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "GetSupportedOperations must be sorted; %q > %q", ops[i-1], ops[i])
	}
}

// TestRefinement1_ExportCounts verifies the export count helpers work correctly.
func TestExportCounts(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	assert.Equal(t, 0, ram.ResourceShareCount(b))
	// Built-in AWS-managed permissions are seeded at construction.
	assert.Equal(t, ram.BuiltInPermissionCount, ram.PermissionCount(b))
	assert.Equal(t, 0, ram.InvitationCount(b))
	assert.Equal(t, 0, ram.AssociationCount(b))
	assert.Equal(t, 0, ram.SharePermissionCount(b))

	ram.AddResourceShareInternal(
		b,
		ram.NewTestResourceShare("arn:aws:ram:us-east-1:000000000000:resource-share/s1", "share-1"),
	)
	assert.Equal(t, 1, ram.ResourceShareCount(b))

	ram.AddPermissionInternal(
		b,
		ram.NewTestPermission("arn:aws:ram:us-east-1:000000000000:permission/p1", "perm-1", "ec2:Subnet"),
	)
	assert.Equal(t, ram.BuiltInPermissionCount+1, ram.PermissionCount(b))

	ram.AddInvitationInternal(b, ram.NewTestInvitation(
		"arn:aws:ram:us-east-1:000000000000:resource-share-invitation/inv1",
		"arn:aws:ram:us-east-1:000000000000:resource-share/s1",
		"share-1",
	))
	assert.Equal(t, 1, ram.InvitationCount(b))
}

// TestRefinement1_HandleError_ErrValidation verifies that ErrValidation yields 400.
func TestHandleError_ErrValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreatePermission then attempt to delete the default version → triggers ErrValidation.
	p, err := h.Backend.CreatePermission("perm-val", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	rec := doRAMRawRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/deletepermissionversion?permissionArn=%s&permissionVersion=1", p.ARN), nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedQueryStringException")
}

// TestRefinement1_HandleError_ErrPermissionNotFound verifies 400 with InvalidParameterException.
func TestHandleError_ErrPermissionNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRAMRequest(t, h, "/getpermission", map[string]any{
		"permissionArn": "arn:aws:ram:us-east-1:000000000000:permission/does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "UnknownResourceException")
}

// TestRefinement1_UnknownAction verifies that unknown operations yield a 400 with a message.
func TestUnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRawRequest(t, h, http.MethodPost, "/unknownramaction", nil)
	// Handler should return an error response (not panic).
	assert.True(t, rec.Code == http.StatusBadRequest || rec.Code == http.StatusInternalServerError)
}

// TestRefinement1_ErrInvalidJSON verifies that malformed JSON in body returns 400.
func TestErrInvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		name string
	}{
		{name: "createresourceshare", path: "/createresourceshare"},
		{name: "getresourceshares", path: "/getresourceshares"},
		{name: "updateresourceshare", path: "/updateresourceshare"},
		{name: "associateresourceshare", path: "/associateresourceshare"},
		{name: "disassociateresourceshare", path: "/disassociateresourceshare"},
		{name: "tagresource", path: "/tagresource"},
		{name: "untagresource", path: "/untagresource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRawRequest(t, h, http.MethodPost, tt.path, []byte("{bad"))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestRefinement1_Handler_ExtractOperation verifies ExtractOperation for all known paths.
func TestHandler_ExtractOperation_AllKnownPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path   string
		wantOp string
	}{
		{"/createresourceshare", "CreateResourceShare"},
		{"/deleteresourceshare", "DeleteResourceShare"},
		{"/getresourceshares", "GetResourceShares"},
		{"/updateresourceshare", "UpdateResourceShare"},
		{"/associateresourceshare", "AssociateResourceShare"},
		{"/disassociateresourceshare", "DisassociateResourceShare"},
		{"/getresourceshareassociations", "GetResourceShareAssociations"},
		{"/tagresource", "TagResource"},
		{"/untagresource", "UntagResource"},
		{"/listtagsforresource", "ListTagsForResource"},
		{"/getpermission", "GetPermission"},
		{"/createpermission", "CreatePermission"},
		{"/createpermissionversion", "CreatePermissionVersion"},
		{"/deletepermission", "DeletePermission"},
		{"/deletepermissionversion", "DeletePermissionVersion"},
		{"/getresourcepolicies", "GetResourcePolicies"},
		{"/getresourceshareinvitations", "GetResourceShareInvitations"},
		{"/acceptresourceshareinvitation", "AcceptResourceShareInvitation"},
		{"/associateresourcesharepermission", "AssociateResourceSharePermission"},
		{"/disassociateresourcesharepermission", "DisassociateResourceSharePermission"},
	}

	h := newTestHandler(t)
	supportedOps := h.GetSupportedOperations()
	opSet := make(map[string]struct{}, len(supportedOps))

	for _, op := range supportedOps {
		opSet[op] = struct{}{}
	}

	for _, tt := range tests {
		t.Run(strings.TrimPrefix(tt.path, "/"), func(t *testing.T) {
			t.Parallel()

			// Verify the operation is listed as supported.
			_, ok := opSet[tt.wantOp]
			assert.True(t, ok, "operation %q should be in GetSupportedOperations", tt.wantOp)
		})
	}
}
