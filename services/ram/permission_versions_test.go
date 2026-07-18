package ram_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestDeletePermissionVersion_CascadesSharePermissions(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	p, err := b.CreatePermission("cascade-perm", "ec2:Subnet", `{"v":1}`, nil)
	require.NoError(t, err)

	_, err = b.CreatePermissionVersion(p.ARN, `{"v":2}`)
	require.NoError(t, err)

	rs, err := b.CreateResourceShare("cascade-share", false, nil, nil, nil)
	require.NoError(t, err)

	version := int32(2)
	err = b.AssociateResourceSharePermission(rs.ARN, p.ARN, false, &version)
	require.NoError(t, err)
	assert.Equal(t, 1, ram.SharePermissionCount(b))

	// Delete version 2 (which is currently associated) — share-permission link must be removed.
	err = b.DeletePermissionVersion(p.ARN, 2)
	require.NoError(t, err)
	assert.Equal(t, 0, ram.SharePermissionCount(b), "stale version association must be removed")
}

// TestRefinement1_ErrValidationSentinel verifies ErrValidation wraps the correct sentinel.
func TestErrValidationSentinel(t *testing.T) {
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
func TestDeletePermissionVersion_UpdatesLatest(t *testing.T) {
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
