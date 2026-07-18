package ram_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestDeletePermission_AllowedAfterDisassociate(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	rs, err := b.CreateResourceShare("da-share", false, nil, nil, nil)
	require.NoError(t, err)

	p, err := b.CreatePermission("da-perm", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	err = b.AssociateResourceSharePermission(rs.ARN, p.ARN, false, nil)
	require.NoError(t, err)

	err = b.DisassociateResourceSharePermission(rs.ARN, p.ARN)
	require.NoError(t, err)

	err = b.DeletePermission(p.ARN)
	require.NoError(t, err)
}

// TestRefinement1_DeletePermission_CascadesSharePermissions verifies that deleting a
// permission also removes it from all resource shares.
func TestDeletePermission_RejectsWhenInUse(t *testing.T) {
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

	// Delete permission while in use → must fail with PermissionInUseException.
	err = h.Backend.DeletePermission(p.ARN)
	require.ErrorIs(t, err, ram.ErrPermissionInUse)

	// Disassociate first, then deletion must succeed.
	err = h.Backend.DisassociateResourceSharePermission(rs.ARN, p.ARN)
	require.NoError(t, err)
	err = h.Backend.DeletePermission(p.ARN)
	require.NoError(t, err)
}

// TestRefinement1_AddPermissionInternal verifies seed helper works.
func TestAddPermissionInternal(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	p := ram.NewTestPermission("arn:aws:ram:us-east-1:000000000000:permission/seed-perm", "seed-perm", "ec2:Subnet")
	ram.AddPermissionInternal(b, p)
	// Built-ins + the one just added.
	assert.Equal(t, ram.BuiltInPermissionCount+1, ram.PermissionCount(b))
}

// TestRefinement1_GetPermission_VersionSpecific verifies fetching a specific version.
func TestGetPermission_VersionSpecific(t *testing.T) {
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
