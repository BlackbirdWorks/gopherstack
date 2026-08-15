package securityhub

import (
	"slices"
	"time"
)

func (b *InMemoryBackend) AcceptAdministratorInvitation(administratorID, invitationID string) error {
	b.mu.Lock("AcceptAdministratorInvitation")
	defer b.mu.Unlock()

	b.adminAccount = &AdminAccount{
		AccountId:    administratorID,
		InvitationId: invitationID,
		InvitedAt:    time.Now().UTC().Format(time.RFC3339),
		MemberStatus: statusEnabled,
	}

	return nil
}

func (b *InMemoryBackend) AcceptInvitation(masterID, invitationID string) error {
	return b.AcceptAdministratorInvitation(masterID, invitationID)
}

func (b *InMemoryBackend) DeclineInvitations(accountIDs []string) ([]map[string]any, []map[string]any) {
	b.mu.Lock("DeclineInvitations")
	defer b.mu.Unlock()

	var declined []map[string]any
	var unprocessed []map[string]any

	for _, inv := range b.invitations.All() {
		if slices.Contains(accountIDs, inv.AccountId) {
			inv.MemberStatus = "Resigned"
			declined = append(declined, map[string]any{
				keyAccountID:       inv.AccountId,
				"ProcessingResult": "SUCCESS", //nolint:goconst // existing issue.
			})
			b.invitations.Delete(inv.InvitationId)
		}
	}

	for _, id := range accountIDs {
		found := false

		for _, item := range declined {
			if item[keyAccountID] == id {
				found = true

				break
			}
		}

		if !found {
			unprocessed = append(unprocessed, map[string]any{
				keyAccountID:    id,
				keyErrorCode:    errCodeResourceNotFound,
				keyErrorMessage: "Invitation not found",
			})
		}
	}

	return declined, unprocessed
}

func (b *InMemoryBackend) DeleteInvitations(accountIDs []string) ([]map[string]any, []map[string]any) {
	b.mu.Lock("DeleteInvitations")
	defer b.mu.Unlock()

	var deleted []map[string]any
	var unprocessed []map[string]any

	for _, inv := range b.invitations.All() {
		if slices.Contains(accountIDs, inv.AccountId) {
			deleted = append(deleted, map[string]any{
				keyAccountID:       inv.AccountId,
				"ProcessingResult": "SUCCESS",
			})
			b.invitations.Delete(inv.InvitationId)
		}
	}

	for _, id := range accountIDs {
		found := false

		for _, item := range deleted {
			if item[keyAccountID] == id {
				found = true

				break
			}
		}

		if !found {
			unprocessed = append(unprocessed, map[string]any{
				keyAccountID:    id,
				keyErrorCode:    errCodeResourceNotFound,
				keyErrorMessage: "Invitation not found",
			})
		}
	}

	return deleted, unprocessed
}

func (b *InMemoryBackend) GetInvitationsCount() int {
	b.mu.RLock("GetInvitationsCount")
	defer b.mu.RUnlock()

	return b.invitations.Len()
}

func (b *InMemoryBackend) ListInvitations(nextToken string, maxResults int) ([]*Invitation, string) {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	snap := b.invitations.All()
	all := make([]*Invitation, 0, len(snap))

	for _, inv := range snap {
		cp := *inv
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) GetAdministratorAccount() (*AdminAccount, error) {
	b.mu.RLock("GetAdministratorAccount")
	defer b.mu.RUnlock()

	if b.adminAccount == nil {
		return nil, nil //nolint:nilnil // existing issue.
	}

	cp := *b.adminAccount

	return &cp, nil
}

func (b *InMemoryBackend) GetMasterAccount() (*AdminAccount, error) {
	return b.GetAdministratorAccount()
}

func (b *InMemoryBackend) DisassociateFromAdministratorAccount() error {
	b.mu.Lock("DisassociateFromAdministratorAccount")
	defer b.mu.Unlock()

	b.adminAccount = nil

	return nil
}

func (b *InMemoryBackend) DisassociateFromMasterAccount() error {
	return b.DisassociateFromAdministratorAccount()
}
