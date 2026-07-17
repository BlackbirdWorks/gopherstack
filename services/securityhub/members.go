package securityhub

import (
	"fmt"
	"time"
)

const msgMemberNotFound = "Member not found"

func (b *InMemoryBackend) CreateMembers(accounts []map[string]any) ([]*Member, []map[string]any) {
	b.mu.Lock("CreateMembers")
	defer b.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)

	var created []*Member
	var unprocessed []map[string]any

	for _, acc := range accounts {
		accountID, _ := acc["AccountId"].(string)
		email, _ := acc["Email"].(string)

		if accountID == "" {
			unprocessed = append(unprocessed, map[string]any{
				keyAccountID:    accountID,
				keyErrorCode:    errCodeInvalidInput,
				keyErrorMessage: "AccountId is required",
			})

			continue
		}

		if b.members.Has(accountID) {
			unprocessed = append(unprocessed, map[string]any{
				keyAccountID:    accountID,
				keyErrorCode:    "ResourceConflictException",
				keyErrorMessage: "Member account already exists",
			})

			continue
		}

		m := &Member{
			AccountId:       accountID,
			AdministratorId: b.accountID,
			MasterId:        b.accountID,
			Email:           email,
			MemberStatus:    "Created",
			UpdatedAt:       now,
		}
		b.members.Put(m)
		created = append(created, m)
	}

	return created, unprocessed
}

func (b *InMemoryBackend) DeleteMembers(accountIDs []string) ([]string, []map[string]any) {
	b.mu.Lock("DeleteMembers")
	defer b.mu.Unlock()

	var deleted []string
	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if b.members.Delete(id) {
			deleted = append(deleted, id)
		} else {
			unprocessed = append(unprocessed, map[string]any{
				keyAccountID:    id,
				keyErrorCode:    errCodeResourceNotFound,
				keyErrorMessage: msgMemberNotFound,
			})
		}
	}

	return deleted, unprocessed
}

func (b *InMemoryBackend) GetMembers(accountIDs []string) ([]*Member, []map[string]any) {
	b.mu.RLock("GetMembers")
	defer b.mu.RUnlock()

	var found []*Member
	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if m, ok := b.members.Get(id); ok {
			cp := *m
			found = append(found, &cp)
		} else {
			unprocessed = append(unprocessed, map[string]any{
				keyAccountID:    id,
				keyErrorCode:    errCodeResourceNotFound,
				keyErrorMessage: msgMemberNotFound,
			})
		}
	}

	return found, unprocessed
}

func (b *InMemoryBackend) InviteMembers(accountIDs []string) []map[string]any {
	b.mu.Lock("InviteMembers")
	defer b.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)

	var unprocessed []map[string]any

	for _, id := range accountIDs {
		// AWS requires the account to already exist as a member (via
		// CreateMembers) before it can be invited; accounts that were never
		// created go to UnprocessedAccounts instead of silently succeeding.
		m, ok := b.members.Get(id)
		if !ok {
			unprocessed = append(unprocessed, map[string]any{
				keyAccountID:    id,
				keyErrorCode:    errCodeResourceNotFound,
				keyErrorMessage: msgMemberNotFound,
			})

			continue
		}

		b.memberSeq++
		invitationID := fmt.Sprintf("%s-invite-%d", b.accountID, b.memberSeq)

		inv := &Invitation{
			AccountId:    id,
			InvitationId: invitationID,
			InvitedAt:    now,
			MemberStatus: "Invited",
		}
		b.invitations.Put(inv)

		m.MemberStatus = "Invited"
		m.InvitedAt = now
	}

	return unprocessed
}

func (b *InMemoryBackend) ListMembers(onlyAssociated bool, nextToken string, maxResults int) ([]*Member, string) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	var all []*Member

	for _, m := range b.members.All() {
		if onlyAssociated && m.MemberStatus != "Enabled" {
			continue
		}

		cp := *m
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) DisassociateMembers(accountIDs []string) error {
	b.mu.Lock("DisassociateMembers")
	defer b.mu.Unlock()

	for _, id := range accountIDs {
		if m, ok := b.members.Get(id); ok {
			m.MemberStatus = "Removed"
		}
	}

	return nil
}
