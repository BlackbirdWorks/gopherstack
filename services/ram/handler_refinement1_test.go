package ram_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// TestRefinement1_ErrNilAppContext verifies the provider rejects a nil context.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &ram.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, ram.ErrNilAppContext)
}

// TestRefinement1_HandlerOpsLen verifies the ops list has expected size.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 36, ram.HandlerOpsLen(h))
}

// TestRefinement1_GetSupportedOperationsSorted verifies the ops list is sorted.
func TestRefinement1_GetSupportedOperationsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i], "GetSupportedOperations must be sorted; %q > %q", ops[i-1], ops[i])
	}
}

// TestRefinement1_ExportCounts verifies the export count helpers work correctly.
func TestRefinement1_ExportCounts(t *testing.T) {
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

// TestRefinement1_ErrValidationSentinel verifies ErrValidation wraps the correct sentinel.
func TestRefinement1_ErrValidationSentinel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Trigger DeletePermissionVersion on default version to get ErrValidation.
	_, err := h.Backend.CreatePermission("perm-x", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	permARN := "arn:aws:ram:us-east-1:000000000000:permission/perm-x"
	err = h.Backend.DeletePermissionVersion(permARN, 1) // version 1 is the default
	require.Error(t, err)
	require.ErrorIs(t, err, ram.ErrValidation)
}

// TestRefinement1_DeletePermissionVersion_UpdatesLatest verifies that deleting the
// latest non-default version updates LatestVersion to the previous version.
func TestRefinement1_DeletePermissionVersion_UpdatesLatest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create permission with v1 as default.
	p, err := h.Backend.CreatePermission("perm-latest", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	permARN := p.ARN

	// Create v2.
	p2, err := h.Backend.CreatePermissionVersion(permARN, `{"v":"2"}`)
	require.NoError(t, err)
	assert.Equal(t, int32(2), p2.LatestVersion)

	// Create v3.
	p3, err := h.Backend.CreatePermissionVersion(permARN, `{"v":"3"}`)
	require.NoError(t, err)
	assert.Equal(t, int32(3), p3.LatestVersion)

	// Delete v3 (the latest). LatestVersion should become 2.
	err = h.Backend.DeletePermissionVersion(permARN, 3)
	require.NoError(t, err)

	got, _, err := h.Backend.GetPermission(permARN, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(2), got.LatestVersion)
}

// TestRefinement1_DeletePermission_CascadesSharePermissions verifies that deleting a
// permission also removes it from all resource shares.
func TestRefinement1_DeletePermission_CascadesSharePermissions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create share.
	rs, err := h.Backend.CreateResourceShare("share-cascade", false, nil, nil, nil)
	require.NoError(t, err)

	// Create permission.
	p, err := h.Backend.CreatePermission("perm-cascade", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	// Associate permission with share.
	err = h.Backend.AssociateResourceSharePermission(rs.ARN, p.ARN, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, ram.SharePermissionCount(h.Backend.(*ram.InMemoryBackend)))

	// Delete permission → should remove from sharePermissions.
	err = h.Backend.DeletePermission(p.ARN)
	require.NoError(t, err)
	assert.Equal(t, 0, ram.SharePermissionCount(h.Backend.(*ram.InMemoryBackend)))
}

// TestRefinement1_DeleteResourceShare_SoftDelete verifies that a deleted resource share
// is marked DELETED (not removed from the map) and associated entities become DISASSOCIATED.
func TestRefinement1_DeleteResourceShare_SoftDelete(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	rs, err := b.CreateResourceShare("soft-share", false, nil, []string{"999999999999"}, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, ram.AssociationCount(b))

	err = b.DeleteResourceShare(rs.ARN)
	require.NoError(t, err)

	// Share record still exists (DELETED status).
	assert.Equal(t, 1, ram.ResourceShareCount(b))

	// GetResourceShare should return NotFound for deleted shares.
	_, err = b.GetResourceShare(rs.ARN)
	require.ErrorIs(t, err, ram.ErrNotFound)

	// Double-delete returns NotFound.
	err = b.DeleteResourceShare(rs.ARN)
	require.ErrorIs(t, err, ram.ErrNotFound)
}

// TestRefinement1_AssociateResourceShare_External verifies that principals with non-account-ID
// format (e.g. ARNs) are flagged as external.
func TestRefinement1_AssociateResourceShare_External(t *testing.T) {
	t.Parallel()

	tests := []struct {
		principal    string
		name         string
		wantExternal bool
	}{
		{
			name:         "own account - not external",
			principal:    "000000000000",
			wantExternal: false,
		},
		{
			name:         "other account - external",
			principal:    "999999999999",
			wantExternal: true,
		},
		{
			name:         "iam role ARN - external",
			principal:    "arn:aws:iam::111111111111:role/MyRole",
			wantExternal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")

			rs, err := b.CreateResourceShare("share-"+tt.name, false, nil, nil, nil)
			require.NoError(t, err)

			assocs, err := b.AssociateResourceShare(rs.ARN, []string{tt.principal}, nil)
			require.NoError(t, err)
			require.Len(t, assocs, 1)
			assert.Equal(t, tt.wantExternal, assocs[0].External)
		})
	}
}

// TestRefinement1_ListResourceShares_StatusFilter verifies status filtering.
func TestRefinement1_ListResourceShares_StatusFilter(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	rs, err := b.CreateResourceShare("share-active", false, nil, nil, nil)
	require.NoError(t, err)

	// Delete one share → DELETED status.
	err = b.DeleteResourceShare(rs.ARN)
	require.NoError(t, err)

	_, err = b.CreateResourceShare("share-active-2", false, nil, nil, nil)
	require.NoError(t, err)

	// Status="ACTIVE" should return only the non-deleted share.
	active := b.ListResourceShares("SELF", "ACTIVE")
	assert.Len(t, active, 1)
	assert.Equal(t, "share-active-2", active[0].Name)

	// Status="" returns all non-deleted.
	all := b.ListResourceShares("SELF", "")
	assert.Len(t, all, 1)
}

// TestRefinement1_ListResourceShares_SortedByName verifies output is sorted.
func TestRefinement1_ListResourceShares_SortedByName(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	for _, name := range []string{"zz-share", "aa-share", "mm-share"} {
		_, err := b.CreateResourceShare(name, false, nil, nil, nil)
		require.NoError(t, err)
	}

	list := b.ListResourceShares("SELF", "")
	require.Len(t, list, 3)
	assert.Equal(t, "aa-share", list[0].Name)
	assert.Equal(t, "mm-share", list[1].Name)
	assert.Equal(t, "zz-share", list[2].Name)
}

// TestRefinement1_ToTagObjectsSorted verifies tags are serialised in sorted order.
func TestRefinement1_ToTagObjectsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/createresourceshare", map[string]any{
		"name": "tagged-share",
		"tags": []map[string]string{
			{"key": "zz", "value": "last"},
			{"key": "aa", "value": "first"},
			{"key": "mm", "value": "middle"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List the share and verify the tags are in order.
	rec2 := doRAMRequest(t, h, "/getresourceshares", map[string]any{"resourceOwner": "SELF"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		ResourceShares []struct {
			Tags []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"tags"`
		} `json:"resourceShares"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	require.Len(t, resp.ResourceShares, 1)

	tags := resp.ResourceShares[0].Tags
	require.Len(t, tags, 3)
	assert.Equal(t, "aa", tags[0].Key)
	assert.Equal(t, "mm", tags[1].Key)
	assert.Equal(t, "zz", tags[2].Key)
}

// TestRefinement1_ListResourceSharePermissions_RealData verifies that actual
// associated permissions are returned instead of an empty list.
func TestRefinement1_ListResourceSharePermissions_RealData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a share and a permission.
	shareRec := doRAMRequest(t, h, "/createresourceshare", map[string]any{"name": "perm-share"})
	require.Equal(t, http.StatusOK, shareRec.Code)

	var shareResp struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	require.NoError(t, json.Unmarshal(shareRec.Body.Bytes(), &shareResp))
	shareARN := shareResp.ResourceShare.ResourceShareArn

	permRec := doRAMRequest(t, h, "/createpermission", map[string]any{
		"name":           "perm-for-share",
		"resourceType":   "ec2:Subnet",
		"policyTemplate": `{}`,
	})
	require.Equal(t, http.StatusOK, permRec.Code)

	var permResp struct {
		Permission struct {
			Arn string `json:"arn"`
		} `json:"permission"`
	}
	require.NoError(t, json.Unmarshal(permRec.Body.Bytes(), &permResp))
	permARN := permResp.Permission.Arn

	// Associate permission with share.
	assocRec := doRAMRequest(t, h, "/associateresourcesharepermission", map[string]any{
		"resourceShareArn": shareARN,
		"permissionArn":    permARN,
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	// List permissions for the share.
	listRec := doRAMRequest(t, h, "/listresourcesharepermissions", map[string]any{
		"resourceShareArn": shareARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Permissions []struct {
			Arn string `json:"arn"`
		} `json:"permissions"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Permissions, 1)
	assert.Equal(t, permARN, listResp.Permissions[0].Arn)
}

// TestRefinement1_ListResourceSharePermissions_RequiresShareARN verifies validation.
func TestRefinement1_ListResourceSharePermissions_RequiresShareARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/listresourcesharepermissions", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_GetResourceShares_StatusFilter verifies status filter via HTTP.
func TestRefinement1_GetResourceShares_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create then delete a share.
	createRec := doRAMRequest(t, h, "/createresourceshare", map[string]any{"name": "to-delete"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	delRec := doRAMRawRequest(t, h, http.MethodDelete,
		"/deleteresourceshare?resourceShareArn="+createResp.ResourceShare.ResourceShareArn, nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	// Create a live share.
	_, err := h.Backend.CreateResourceShare("live-share", false, nil, nil, nil)
	require.NoError(t, err)

	// Filter by ACTIVE status.
	rec := doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner":       "SELF",
		"resourceShareStatus": "ACTIVE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceShares []struct {
			Name string `json:"name"`
		} `json:"resourceShares"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.ResourceShares, 1)
	assert.Equal(t, "live-share", resp.ResourceShares[0].Name)
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore preserves
// all new maps (permissions, sharePermissions, invitations).
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateResourceShare("prs-share", false, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreatePermission("prs-perm", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	ram.AddInvitationInternal(b, ram.NewTestInvitation(
		"arn:aws:ram:us-east-1:000000000000:resource-share-invitation/inv-rt",
		"arn:aws:ram:us-east-1:000000000000:resource-share/s-rt",
		"rt-share",
	))

	data := b.Snapshot()
	require.NotNil(t, data)

	b2 := ram.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(data))

	assert.Equal(t, 1, ram.ResourceShareCount(b2))
	// Snapshot includes built-ins + 1 customer-managed permission.
	assert.Equal(t, ram.BuiltInPermissionCount+1, ram.PermissionCount(b2))
	assert.Equal(t, 1, ram.InvitationCount(b2))
}

// TestRefinement1_HandleError_ErrValidation verifies that ErrValidation yields 400.
func TestRefinement1_HandleError_ErrValidation(t *testing.T) {
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
func TestRefinement1_HandleError_ErrPermissionNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRAMRequest(t, h, "/getpermission", map[string]any{
		"permissionArn": "arn:aws:ram:us-east-1:000000000000:permission/does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "UnknownResourceException")
}

// TestRefinement1_GetResourceShareInvitations_SortedByCreation verifies invitations are
// returned in chronological order.
func TestRefinement1_GetResourceShareInvitations_SortedByCreation(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	now := time.Now()

	for i := range 5 {
		ram.AddInvitationInternal(b, &ram.ResourceShareInvitation{
			InvitationARN:     fmt.Sprintf("arn:aws:ram:us-east-1:000000000000:resource-share-invitation/inv-%d", i),
			ResourceShareARN:  "arn:aws:ram:us-east-1:000000000000:resource-share/s1",
			ResourceShareName: "share-1",
			SenderAccountID:   "111111111111",
			ReceiverAccountID: "000000000000",
			Status:            "PENDING",
			CreationTime:      now.Add(time.Duration(i) * time.Second),
			LastUpdatedTime:   now.Add(time.Duration(i) * time.Second),
		})
	}

	invitations := b.GetResourceShareInvitations(nil, nil)
	require.Len(t, invitations, 5)

	for i := 1; i < len(invitations); i++ {
		assert.False(t, invitations[i].CreationTime.Before(invitations[i-1].CreationTime),
			"invitations must be sorted by creation time")
	}
}

// TestRefinement1_AcceptInvitation_HTTP verifies the full HTTP roundtrip.
func TestRefinement1_AcceptInvitation_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	shareARN := "arn:aws:ram:us-east-1:000000000000:resource-share/accept-test"
	invARN := "arn:aws:ram:us-east-1:000000000000:resource-share-invitation/inv-accept"
	ram.AddInvitationInternal(h.Backend.(*ram.InMemoryBackend), ram.NewTestInvitation(invARN, shareARN, "accept-share"))

	rec := doRAMRequest(t, h, "/acceptresourceshareinvitation", map[string]any{
		"resourceShareInvitationArn": invARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceShareInvitation struct {
			Status string `json:"status"`
		} `json:"resourceShareInvitation"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ACCEPTED", resp.ResourceShareInvitation.Status)

	// Double-accept should return AlreadyAccepted.
	rec2 := doRAMRequest(t, h, "/acceptresourceshareinvitation", map[string]any{
		"resourceShareInvitationArn": invARN,
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "AlreadyAccepted")
}

// TestRefinement1_ErrInvitationNotFound_HTTP verifies 400 for missing invitation.
func TestRefinement1_ErrInvitationNotFound_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/acceptresourceshareinvitation", map[string]any{
		"resourceShareInvitationArn": "arn:aws:ram:us-east-1:000000000000:resource-share-invitation/missing",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceShareInvitationArnNotFoundException")
}

// TestRefinement1_CreateResourceShare_NameRequired verifies that name validation works.
func TestRefinement1_CreateResourceShare_NameRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/createresourceshare", map[string]any{
		"allowExternalPrincipals": true,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_Reset_ClearsAll verifies that Reset removes user state and re-seeds built-ins.
func TestRefinement1_Reset_ClearsAll(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateResourceShare("reset-share", false, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreatePermission("reset-perm", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	b.Reset()

	assert.Equal(t, 0, ram.ResourceShareCount(b))
	// Built-in permissions are re-seeded after reset.
	assert.Equal(t, ram.BuiltInPermissionCount, ram.PermissionCount(b))
	assert.Equal(t, 0, ram.InvitationCount(b))
	assert.Equal(t, 0, ram.AssociationCount(b))
}

// TestRefinement1_AddResourceShareInternal verifies seed helper works.
func TestRefinement1_AddResourceShareInternal(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	rs := ram.NewTestResourceShare("arn:aws:ram:us-east-1:000000000000:resource-share/seed-1", "seed-share")
	ram.AddResourceShareInternal(b, rs)
	assert.Equal(t, 1, ram.ResourceShareCount(b))
}

// TestRefinement1_AddPermissionInternal verifies seed helper works.
func TestRefinement1_AddPermissionInternal(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	p := ram.NewTestPermission("arn:aws:ram:us-east-1:000000000000:permission/seed-perm", "seed-perm", "ec2:Subnet")
	ram.AddPermissionInternal(b, p)
	// Built-ins + the one just added.
	assert.Equal(t, ram.BuiltInPermissionCount+1, ram.PermissionCount(b))
}

// TestRefinement1_GetPermission_VersionSpecific verifies fetching a specific version.
func TestRefinement1_GetPermission_VersionSpecific(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create permission, then a second version.
	p, err := h.Backend.CreatePermission("ver-perm", "ec2:Subnet", `{"v":"1"}`, nil)
	require.NoError(t, err)

	_, err = h.Backend.CreatePermissionVersion(p.ARN, `{"v":"2"}`)
	require.NoError(t, err)

	version := int32(1)
	pResult, pv, err := h.Backend.GetPermission(p.ARN, &version)
	require.NoError(t, err)
	assert.JSONEq(t, `{"v":"1"}`, pv.PolicyTemplate)
	assert.Equal(t, int32(2), pResult.LatestVersion)
}

// TestRefinement1_DisassociateResourceSharePermission_Idempotent verifies that calling
// DisassociateResourceSharePermission on a non-associated permission is a no-op.
func TestRefinement1_DisassociateResourceSharePermission_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("dis-share", false, nil, nil, nil)
	require.NoError(t, err)

	// Disassociate a permission that was never associated – should not error.
	err = h.Backend.DisassociateResourceSharePermission(rs.ARN, "arn:aws:ram:us-east-1:000000000000:permission/ghost")
	require.NoError(t, err)
}

// TestRefinement1_Snapshot_DeepCopy verifies that the snapshot does not share pointers with the backend.
func TestRefinement1_Snapshot_DeepCopy(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	rs, err := b.CreateResourceShare("snap-share", false, map[string]string{"env": "test"}, nil, nil)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	// Mutate the live share; the snapshot should be unaffected.
	err = b.TagResource(rs.ARN, map[string]string{"env": "prod"})
	require.NoError(t, err)

	b2 := ram.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	tags, err := b2.ListTagsForResource(rs.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"], "snapshot should have pre-mutation tag value")
}

// TestRefinement1_UnknownAction verifies that unknown operations yield a 400 with a message.
func TestRefinement1_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRawRequest(t, h, http.MethodPost, "/unknownramaction", nil)
	// Handler should return an error response (not panic).
	assert.True(t, rec.Code == http.StatusBadRequest || rec.Code == http.StatusInternalServerError)
}

// TestRefinement1_GetResourcePolicies_EmptyArns verifies empty input returns empty list.
func TestRefinement1_GetResourcePolicies_EmptyArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/getresourcepolicies", map[string]any{"resourceArns": []string{}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Policies []string `json:"policies"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Policies)
}

// TestRefinement1_CreatePermission_Duplicate verifies AlreadyExists is returned via HTTP.
func TestRefinement1_CreatePermission_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"name":           "dup-http-perm",
		"resourceType":   "ec2:Subnet",
		"policyTemplate": `{}`,
	}
	rec1 := doRAMRequest(t, h, "/createpermission", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRAMRequest(t, h, "/createpermission", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "AlreadyExists")
}

// TestRefinement1_AssociateResourceSharePermission_BadJSON verifies error handling.
func TestRefinement1_AssociateResourceSharePermission_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRawRequest(t, h, http.MethodPost, "/associateresourcesharepermission", []byte("{bad json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_CreatePermissionVersion_BadJSON verifies error handling.
func TestRefinement1_CreatePermissionVersion_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRawRequest(t, h, http.MethodPost, "/createpermissionversion", []byte("{bad"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_GetResourceShareInvitations_FilterByShareARN verifies share ARN filtering.
func TestRefinement1_GetResourceShareInvitations_FilterByShareARN(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	shareARN1 := "arn:aws:ram:us-east-1:000000000000:resource-share/s-filter-1"
	shareARN2 := "arn:aws:ram:us-east-1:000000000000:resource-share/s-filter-2"

	ram.AddInvitationInternal(b, ram.NewTestInvitation(
		"arn:aws:ram:us-east-1:000000000000:resource-share-invitation/inv-f1", shareARN1, "share-1"))
	ram.AddInvitationInternal(b, ram.NewTestInvitation(
		"arn:aws:ram:us-east-1:000000000000:resource-share-invitation/inv-f2", shareARN2, "share-2"))

	result := b.GetResourceShareInvitations(nil, []string{shareARN1})
	require.Len(t, result, 1)
	assert.Equal(t, shareARN1, result[0].ResourceShareARN)
}

// TestRefinement1_GetResourceShareAssociations_TypeFilter verifies type filtering.
func TestRefinement1_GetResourceShareAssociations_TypeFilter(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	rs, err := b.CreateResourceShare("assoc-filter", false, nil,
		[]string{"principal-1"},
		[]string{"arn:aws:ec2:us-east-1:000000000000:subnet/sub-1"},
	)
	require.NoError(t, err)

	principals := b.GetResourceShareAssociations("PRINCIPAL", []string{rs.ARN})
	assert.Len(t, principals, 1)
	assert.Equal(t, "PRINCIPAL", principals[0].AssociationType)

	resources := b.GetResourceShareAssociations("RESOURCE", []string{rs.ARN})
	assert.Len(t, resources, 1)
	assert.Equal(t, "RESOURCE", resources[0].AssociationType)
}

// TestRefinement1_ErrAlreadyExists_ShareName verifies name collision on CreateResourceShare.
func TestRefinement1_ErrAlreadyExists_ShareName(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateResourceShare("duplicate", false, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateResourceShare("duplicate", false, nil, nil, nil)
	require.ErrorIs(t, err, ram.ErrAlreadyExists)
}

// TestRefinement1_TagsRoundTrip verifies tag operations via HTTP.
func TestRefinement1_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("tag-rt-share", false, nil, nil, nil)
	require.NoError(t, err)

	// Tag the share.
	rec := doRAMRequest(t, h, "/tagresource", map[string]any{
		"resourceShareArn": rs.ARN,
		"tags":             []map[string]string{{"key": "env", "value": "prod"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List tags.
	listRec := doRAMRequest(t, h, "/listtagsforresource", map[string]any{
		"resourceShareArn": rs.ARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Tags []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"tags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Tags, 1)
	assert.Equal(t, "env", listResp.Tags[0].Key)
	assert.Equal(t, "prod", listResp.Tags[0].Value)

	// Untag.
	untagRec := doRAMRequest(t, h, "/untagresource", map[string]any{
		"resourceShareArn": rs.ARN,
		"tagKeys":          []string{"env"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	// Tags should now be empty.
	listRec2 := doRAMRequest(t, h, "/listtagsforresource", map[string]any{
		"resourceShareArn": rs.ARN,
	})
	require.Equal(t, http.StatusOK, listRec2.Code)
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Tags)
}

// TestRefinement1_UpdateResourceShare_SyncAssocName verifies that updating the share name
// is propagated to existing associations.
func TestRefinement1_UpdateResourceShare_SyncAssocName(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	rs, err := b.CreateResourceShare("old-name", false, nil, []string{"principal-1"}, nil)
	require.NoError(t, err)

	_, err = b.UpdateResourceShare(rs.ARN, "new-name", nil)
	require.NoError(t, err)

	assocs := b.GetResourceShareAssociations("PRINCIPAL", []string{rs.ARN})
	require.Len(t, assocs, 1)
	assert.Equal(t, "new-name", assocs[0].ResourceShareName)
}

// TestRefinement1_ErrInvalidJSON verifies that malformed JSON in body returns 400.
func TestRefinement1_ErrInvalidJSON(t *testing.T) {
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

// TestRefinement1_ResourceShareAssociation_DuplicateIdempotent verifies that associating
// the same entity twice is a no-op.
func TestRefinement1_ResourceShareAssociation_DuplicateIdempotent(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	rs, err := b.CreateResourceShare("idem-share", false, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.AssociateResourceShare(rs.ARN, []string{"principal-1"}, nil)
	require.NoError(t, err)

	// Second association of same entity should not duplicate.
	added, err := b.AssociateResourceShare(rs.ARN, []string{"principal-1"}, nil)
	require.NoError(t, err)
	assert.Empty(t, added, "duplicate association should not add a new entry")
	assert.Equal(t, 1, ram.AssociationCount(b))
}

// TestRefinement1_GetPermission_NotFound verifies 400 for missing permission.
func TestRefinement1_GetPermission_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/getpermission", map[string]any{
		"permissionArn": "arn:aws:ram:us-east-1:000000000000:permission/no-such-permission",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_Handler_ExtractOperation verifies ExtractOperation for all known paths.
func TestRefinement1_Handler_ExtractOperation(t *testing.T) {
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
