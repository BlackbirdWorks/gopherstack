package inspector2

import (
	"fmt"
	"time"
)

// AssociateMember adds a member account.
func (b *InMemoryBackend) AssociateMember(accountID string) error {
	b.mu.Lock("AssociateMember")
	defer b.mu.Unlock()

	if accountID == "" {
		return fmt.Errorf("%w: accountId is required", ErrValidation)
	}

	b.members.Put(&Member{
		AccountID:               accountID,
		DelegatedAdminAccountID: b.accountID,
		RelationshipStatus:      statusEnabled,
		UpdatedAt:               time.Now().UTC(),
	})

	return nil
}

// DisassociateMember removes a member account.
func (b *InMemoryBackend) DisassociateMember(accountID string) error {
	b.mu.Lock("DisassociateMember")
	defer b.mu.Unlock()

	if !b.members.Delete(accountID) {
		return ErrMemberNotFound
	}

	return nil
}

// GetMember returns a member account.
func (b *InMemoryBackend) GetMember(accountID string) (*Member, error) {
	b.mu.RLock("GetMember")
	defer b.mu.RUnlock()

	m, ok := b.members.Get(accountID)
	if !ok {
		return nil, ErrMemberNotFound
	}

	cp := *m

	return &cp, nil
}

// ListMembers returns all member accounts, optionally only associated ones.
func (b *InMemoryBackend) ListMembers(onlyAssociated bool) ([]*Member, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	result := make([]*Member, 0, b.members.Len())

	for _, m := range b.members.Snapshot() {
		if onlyAssociated && m.RelationshipStatus != statusEnabled {
			continue
		}

		cp := *m
		result = append(result, &cp)
	}

	return result, nil
}
