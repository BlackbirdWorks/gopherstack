package ram_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// ---------------------------------------------------------------------------
// Finding 3/4: Auto-create invitations for external principals
// ---------------------------------------------------------------------------

func TestAccuracy2_AutoInvitation_AssociateExternalPrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		principal  string
		wantInvite bool
	}{
		{
			name:       "external account ID creates invitation",
			principal:  "999999999999",
			wantInvite: true,
		},
		{
			name:       "external IAM role ARN creates invitation",
			principal:  "arn:aws:iam::111111111111:role/MyRole",
			wantInvite: true,
		},
		{
			name:       "own account does not create invitation",
			principal:  "000000000000",
			wantInvite: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			rs, err := b.CreateResourceShare("invite-share", true, nil, nil, nil)
			require.NoError(t, err)

			before := ram.InvitationCount(b)

			_, err = b.AssociateResourceShare(rs.ARN, []string{tt.principal}, nil)
			require.NoError(t, err)

			after := ram.InvitationCount(b)

			if tt.wantInvite {
				assert.Equal(t, before+1, after, "expected invitation to be created for %s", tt.principal)
			} else {
				assert.Equal(t, before, after, "expected no invitation for own account")
			}
		})
	}
}

func TestAccuracy2_AutoInvitation_CreateResourceShareWithPrincipals(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	rs, err := b.CreateResourceShare(
		"invite-create-share",
		true,
		nil,
		[]string{"111111111111", "222222222222"},
		nil,
	)
	require.NoError(t, err)

	invs := b.GetResourceShareInvitations(nil, []string{rs.ARN})
	assert.Len(t, invs, 2, "two external principals should produce two invitations")

	for _, inv := range invs {
		assert.Equal(t, "PENDING", inv.Status)
		assert.Equal(t, rs.ARN, inv.ResourceShareARN)
	}
}

// ---------------------------------------------------------------------------
// Finding 7: AllowExternalPrincipals=false enforcement
// ---------------------------------------------------------------------------

func TestAccuracy2_AllowExternalPrincipals_FalseRejectsExternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		wantErr   bool
	}{
		{
			name:      "own account allowed when flag false",
			principal: "000000000000",
			wantErr:   false,
		},
		{
			name:      "external account ID rejected when flag false",
			principal: "999999999999",
			wantErr:   true,
		},
		{
			name:      "IAM role ARN rejected when flag false",
			principal: "arn:aws:iam::111111111111:role/MyRole",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			rs, err := b.CreateResourceShare("restrict-share", false, nil, nil, nil)
			require.NoError(t, err)

			_, err = b.AssociateResourceShare(rs.ARN, []string{tt.principal}, nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ram.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAccuracy2_AllowExternalPrincipals_FalseRejectsOnCreate(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateResourceShare(
		"restrict-create",
		false,
		nil,
		[]string{"999999999999"},
		nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ram.ErrValidation)
}

// TestAccuracy2_CreateResourceShare_RejectedExternalPrincipalLeavesNoOrphan verifies
// that a CreateResourceShare call rejected for an external principal (when
// AllowExternalPrincipals is false) does not leave a partially created resource
// share behind. Previously the share (and any associations for principals
// processed before the rejected one) were committed before validation ran,
// so a failed call still left orphaned state -- including reserving the
// share name, which made every retry with the same name fail with
// ResourceShareAlreadyExistsException.
func TestAccuracy2_CreateResourceShare_RejectedExternalPrincipalLeavesNoOrphan(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateResourceShare(
		"no-orphan-create",
		false,
		nil,
		[]string{"000000000000", "999999999999"},
		nil,
	)
	require.ErrorIs(t, err, ram.ErrValidation)

	shares := b.ListResourceShares("SELF", "")
	assert.Empty(t, shares, "rejected CreateResourceShare must not leave an orphaned share")

	// Retrying with the same name must succeed -- it would previously fail
	// with ResourceShareAlreadyExistsException because the first (failed)
	// call had already reserved the name.
	rs, err := b.CreateResourceShare("no-orphan-create", true, nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "no-orphan-create", rs.Name)
}

// TestAccuracy2_AssociateResourceShare_RejectedExternalPrincipalLeavesNoOrphan verifies
// that AssociateResourceShare does not commit associations for principals
// processed before a later external-principal rejection.
func TestAccuracy2_AssociateResourceShare_RejectedExternalPrincipalLeavesNoOrphan(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	rs, err := b.CreateResourceShare("assoc-no-orphan", false, nil, nil, nil)
	require.NoError(t, err)

	_, err = b.AssociateResourceShare(
		rs.ARN,
		[]string{"000000000000", "999999999999"},
		nil,
	)
	require.ErrorIs(t, err, ram.ErrValidation)

	assocs := b.GetResourceShareAssociations("PRINCIPAL", []string{rs.ARN})
	assert.Empty(t, assocs, "rejected AssociateResourceShare must not commit any associations")

	invs := b.GetResourceShareInvitations(nil, []string{rs.ARN})
	assert.Empty(t, invs, "rejected AssociateResourceShare must not create invitations")
}

// ---------------------------------------------------------------------------
// Finding 12/13: EXPIRED invitation status + correct error types
// ---------------------------------------------------------------------------

func TestAccuracy2_InvitationExpiredStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrType error
		name        string
		action      string // "accept" or "reject"
		invStatus   string
	}{
		{
			name:        "accept expired returns ExpiredException",
			action:      "accept",
			invStatus:   "EXPIRED",
			wantErrType: ram.ErrInvitationExpired,
		},
		{
			name:        "reject expired returns ExpiredException",
			action:      "reject",
			invStatus:   "EXPIRED",
			wantErrType: ram.ErrInvitationExpired,
		},
		{
			name:        "accept rejected returns AlreadyRejectedException",
			action:      "accept",
			invStatus:   "REJECTED",
			wantErrType: ram.ErrInvitationAlreadyRejected,
		},
		{
			name:        "reject accepted returns AlreadyAcceptedException",
			action:      "reject",
			invStatus:   "ACCEPTED",
			wantErrType: ram.ErrInvitationAlreadyAccepted,
		},
		{
			name:        "reject rejected returns AlreadyRejectedException",
			action:      "reject",
			invStatus:   "REJECTED",
			wantErrType: ram.ErrInvitationAlreadyRejected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			inv := ram.NewTestInvitation(
				"arn:aws:ram:us-east-1:000000000000:resource-share-invitation/test-inv",
				"arn:aws:ram:us-east-1:000000000000:resource-share/s1",
				"share-1",
			)
			inv.Status = tt.invStatus
			ram.AddInvitationInternal(b, inv)

			var err error
			if tt.action == "accept" {
				_, err = b.AcceptResourceShareInvitation(inv.InvitationARN)
			} else {
				_, err = b.RejectResourceShareInvitation(inv.InvitationARN)
			}

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErrType)
		})
	}
}

func TestAccuracy2_InvitationExpiredStatus_HTTPResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("exp-share", true, nil, nil, nil)
	require.NoError(t, err)

	inv := ram.CreateInvitation(
		h.Backend.(*ram.InMemoryBackend),
		rs.ARN, "exp-share", "111111111111", "000000000000",
	)
	inv.Status = "EXPIRED"
	ram.AddInvitationInternal(h.Backend.(*ram.InMemoryBackend), &ram.ResourceShareInvitation{
		InvitationARN:     inv.InvitationARN + "-expired",
		ResourceShareARN:  rs.ARN,
		ResourceShareName: "exp-share",
		SenderAccountID:   "111111111111",
		ReceiverAccountID: "000000000000",
		Status:            "EXPIRED",
	})

	// Accept an expired invitation should return 400 with ExpiredException.
	expiredARN := inv.InvitationARN + "-expired"
	rec := doRAMRequest(t, h, "/acceptresourceshareinvitation", map[string]any{
		"resourceShareInvitationArn": expiredARN,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceShareInvitationExpiredException")
}

// ---------------------------------------------------------------------------
// Finding 26: DeletePermissionVersion cascade cleans up sharePermissions
// ---------------------------------------------------------------------------

func TestAccuracy2_DeletePermissionVersion_CascadesSharePermissions(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Finding 27: DeletePermission rejects when permission is in use
// ---------------------------------------------------------------------------

func TestAccuracy2_DeletePermission_InUseRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("inuse-share", false, nil, nil, nil)
	require.NoError(t, err)

	p, err := h.Backend.CreatePermission("inuse-perm", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	err = h.Backend.AssociateResourceSharePermission(rs.ARN, p.ARN, false, nil)
	require.NoError(t, err)

	// HTTP delete should return 400 PermissionInUseException.
	rec := doRAMRequest(t, h, "/deletepermission?permissionArn="+p.ARN, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "PermissionInUseException")
}

func TestAccuracy2_DeletePermission_AllowedAfterDisassociate(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Finding 33: CreateResourceShare name validation
// ---------------------------------------------------------------------------

func TestAccuracy2_CreateResourceShare_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		shareName  string
		wantStatus int
	}{
		{
			name:       "valid name alphanumeric",
			shareName:  "my-share-01",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid name with dots",
			shareName:  "my.share.name",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid name with underscores",
			shareName:  "my_share_name",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid name with spaces",
			shareName:  "my share name",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid name with at-sign",
			shareName:  "my@share",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid empty name",
			shareName:  "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"name": tt.shareName}
			rec := doRAMRequest(t, h, "/createresourceshare", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Finding 39: ErrPermissionNotFound uses UnknownResourceException
// ---------------------------------------------------------------------------

func TestAccuracy2_PermissionNotFound_UsesUnknownResourceException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/getpermission", map[string]any{
		"permissionArn": "arn:aws:ram:us-east-1:000000000000:permission/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "UnknownResourceException", resp["__type"])
}

// ---------------------------------------------------------------------------
// Finding 41: strconv.Atoi for permissionVersion parsing
// ---------------------------------------------------------------------------

func TestAccuracy2_DeletePermissionVersion_RejectsNegativeVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		version    string
		wantStatus int
	}{
		{
			name:       "negative version rejected",
			version:    "-1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "zero version rejected",
			version:    "0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-integer rejected",
			version:    "abc",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			p, err := h.Backend.CreatePermission("ver-perm", "ec2:Subnet", `{}`, nil)
			require.NoError(t, err)

			rec := doRAMRequest(
				t,
				h,
				"/deletepermissionversion?permissionArn="+p.ARN+"&permissionVersion="+tt.version,
				nil,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Finding 17: ListResourceTypes expanded set with resourceRegionScope
// ---------------------------------------------------------------------------

func TestAccuracy2_ListResourceTypes_ExpandedSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/listresourcetypes", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceTypes []struct {
			ResourceType        string `json:"resourceType"`
			ServiceName         string `json:"serviceName"`
			ResourceRegionScope string `json:"resourceRegionScope"`
		} `json:"resourceTypes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Greater(t, len(resp.ResourceTypes), 4, "should list more than the original 4 types")

	found := map[string]bool{}
	for _, rt := range resp.ResourceTypes {
		found[rt.ResourceType] = true
		assert.NotEmpty(t, rt.ServiceName, "type %q must have serviceName", rt.ResourceType)
		assert.NotEmpty(t, rt.ResourceRegionScope, "type %q must have resourceRegionScope", rt.ResourceType)
		assert.Contains(
			t,
			[]string{"REGIONAL", "GLOBAL"},
			rt.ResourceRegionScope,
			"scope must be REGIONAL or GLOBAL",
		)
	}

	for _, requiredType := range []string{
		"ec2:Subnet",
		"ec2:TransitGateway",
		"route53resolver:ResolverRule",
		"license-manager:LicenseConfiguration",
		"codebuild:Project",
	} {
		assert.True(t, found[requiredType], "expected %q in ListResourceTypes", requiredType)
	}
}

// ---------------------------------------------------------------------------
// Finding 8: StatusMessage field on associationObject
// ---------------------------------------------------------------------------

func TestAccuracy2_AssociationStatusMessage_InResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("status-msg-share", false, nil, nil, nil)
	require.NoError(t, err)

	_, err = h.Backend.AssociateResourceShare(rs.ARN, nil,
		[]string{"arn:aws:ec2:us-east-1:000000000000:subnet/sub-1"})
	require.NoError(t, err)

	rec := doRAMRequest(t, h, "/getresourceshareassociations", map[string]any{
		"resourceShareArns": []string{rs.ARN},
		"associationType":   "RESOURCE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceShareAssociations []struct {
			Status        string `json:"status"`
			StatusMessage string `json:"statusMessage"`
		} `json:"resourceShareAssociations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.ResourceShareAssociations)

	// statusMessage is optional; verify the field exists in JSON (can be empty).
	// We check by re-parsing as raw map.
	var raw struct {
		ResourceShareAssociations []map[string]json.RawMessage `json:"resourceShareAssociations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	require.NotEmpty(t, raw.ResourceShareAssociations)
	// statusMessage should NOT appear when empty (omitempty).
	_, present := raw.ResourceShareAssociations[0]["statusMessage"]
	assert.False(t, present, "statusMessage should be omitted when empty")
}
