package ram_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRAM_ListPermissions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// ListPermissions
	rec := doRAMRequest(t, h, "/permissions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListPermissionVersions
	rec = doRAMRequest(t, h, "/permissions/aws:aws:ram::aws:permission/AWSRAMDefaultPermissionVPC/versions", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}

func TestRAM_ListResources(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// ListResources (owner = SELF)
	rec := doRAMRequest(t, h, "/listresources", map[string]any{
		"resourceOwner": "SELF",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListPrincipals
	rec = doRAMRequest(t, h, "/listprincipals", map[string]any{
		"resourceOwner": "SELF",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRAM_InvitationOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateResourceShare("inv-share", true, nil, nil, nil)
	require.NoError(t, err)

	// RejectResourceShareInvitation (no invitations, exercises the code path)
	rec := doRAMRequest(t, h, "/rejectresourceshareinvitation", map[string]any{
		"resourceShareInvitationArn": "arn:aws:ram:us-east-1:123456789012:resource-share-invitation/nonexistent",
	})
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}
