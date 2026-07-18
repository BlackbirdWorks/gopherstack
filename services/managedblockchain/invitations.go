package managedblockchain

import (
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	// invitationStatusPending is the status for a pending invitation.
	invitationStatusPending = "PENDING"
	// invitationStatusRejected is the status for a rejected invitation.
	invitationStatusRejected = "REJECTED"
)

// invitationARN builds the ARN for a Managed Blockchain invitation.
func invitationARN(region, accountID, invitationID string) string {
	return arn.Build("managedblockchain", region, accountID, "invitations/"+invitationID)
}

// cloneInvitation returns a deep copy of an Invitation.
func cloneInvitation(inv *Invitation) *Invitation {
	cp := *inv

	if inv.NetworkSummary != nil {
		ns := *inv.NetworkSummary
		cp.NetworkSummary = &ns
	}

	return &cp
}

// ListInvitations returns all invitations sorted by invitation ID.
func (b *InMemoryBackend) ListInvitations() ([]*Invitation, error) {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	all := make([]*Invitation, 0, b.invitations.Len())

	for _, inv := range b.invitations.All() {
		all = append(all, cloneInvitation(inv))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].InvitationID < all[j].InvitationID
	})

	return all, nil
}

// RejectInvitation rejects an invitation by setting its status to REJECTED.
func (b *InMemoryBackend) RejectInvitation(invitationID string) error {
	b.mu.Lock("RejectInvitation")
	defer b.mu.Unlock()

	inv, exists := b.invitations.Get(invitationID)
	if !exists {
		return ErrInvitationNotFound
	}

	inv.Status = invitationStatusRejected

	return nil
}

// AddInvitationInternal adds an invitation directly to the backend (for testing and seeding).
func (b *InMemoryBackend) AddInvitationInternal(region, accountID, networkID, networkName string) *Invitation {
	b.mu.Lock("AddInvitationInternal")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	invitationID := uuid.NewString()

	var netSummary *InvitationNetworkSummary

	if n, exists := b.networks.Get(networkID); exists {
		netSummary = &InvitationNetworkSummary{
			ID:               n.ID,
			Arn:              n.Arn,
			Name:             n.Name,
			Description:      n.Description,
			Framework:        n.Framework,
			FrameworkVersion: n.FrameworkVersion,
			Status:           n.Status,
			CreationDate:     n.CreationDate,
		}
	} else {
		netSummary = &InvitationNetworkSummary{
			ID:   networkID,
			Name: networkName,
		}
	}

	inv := &Invitation{
		InvitationID:   invitationID,
		Arn:            invitationARN(region, accountID, invitationID),
		NetworkID:      networkID,
		NetworkName:    networkName,
		Status:         invitationStatusPending,
		CreationDate:   &now,
		NetworkSummary: netSummary,
	}

	b.invitations.Put(inv)

	return cloneInvitation(inv)
}
