package ram_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestAutoInvitation_AssociateExternalPrincipal(t *testing.T) {
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

func TestAutoInvitation_CreateResourceShareWithPrincipals(t *testing.T) {
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

func TestInvitationExpiredStatus(t *testing.T) {
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

// TestRefinement1_GetResourceShareInvitations_SortedByCreation verifies invitations are
// returned in chronological order.
func TestGetResourceShareInvitations_SortedByCreation(t *testing.T) {
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

// TestRefinement1_GetResourceShareInvitations_FilterByShareARN verifies share ARN filtering.
func TestGetResourceShareInvitations_FilterByShareARN(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	shareARN1 := "arn:aws:ram:us-east-1:000000000000:resource-share/s-filter-1"
	shareARN2 := "arn:aws:ram:us-east-1:000000000000:resource-share/s-filter-2"

	ram.AddInvitationInternal(b, ram.NewTestInvitation(
		"arn:aws:ram:us-east-1:000000000000:resource-share-invitation/inv-f1", shareARN1, "share-1",
	))
	ram.AddInvitationInternal(b, ram.NewTestInvitation(
		"arn:aws:ram:us-east-1:000000000000:resource-share-invitation/inv-f2", shareARN2, "share-2",
	))

	result := b.GetResourceShareInvitations(nil, []string{shareARN1})
	require.Len(t, result, 1)
	assert.Equal(t, shareARN1, result[0].ResourceShareARN)
}
