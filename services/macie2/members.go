package macie2

import (
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// CreateMember creates a new member account relationship.
func (b *InMemoryBackend) CreateMember(accountID, email string, tags map[string]string) error {
	b.mu.Lock("CreateMember")
	defer b.mu.Unlock()

	if b.members.Has(accountID) {
		return ErrMemberAlreadyExists
	}

	now := time.Now().UTC()
	b.members.Put(&Member{
		AccountID:              accountID,
		Arn:                    arn.Build("macie2", b.region, accountID, ""),
		AdministratorAccountID: b.accountID,
		Email:                  email,
		MasteredBy:             b.accountID,
		RelationshipStatus:     "CREATED",
		InvitedAt:              now,
		UpdatedAt:              now,
		Tags:                   maps.Clone(tags),
	})

	return nil
}

// GetMember returns a member account by account ID.
func (b *InMemoryBackend) GetMember(accountID string) (*Member, error) {
	b.mu.RLock("GetMember")
	defer b.mu.RUnlock()

	m, ok := b.members.Get(accountID)
	if !ok {
		return nil, ErrMemberNotFound
	}

	cp := *m
	cp.Tags = maps.Clone(m.Tags)

	return &cp, nil
}

// DeleteMember removes a member account.
func (b *InMemoryBackend) DeleteMember(accountID string) error {
	b.mu.Lock("DeleteMember")
	defer b.mu.Unlock()

	if !b.members.Delete(accountID) {
		return ErrMemberNotFound
	}

	return nil
}

// ListMembers returns all member accounts.
func (b *InMemoryBackend) ListMembers(onlyAssociated bool) ([]*Member, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	members := b.members.All()
	result := make([]*Member, 0, len(members))

	for _, m := range members {
		if onlyAssociated && m.RelationshipStatus == "DISASSOCIATED" {
			continue
		}

		cp := *m
		cp.Tags = maps.Clone(m.Tags)
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].AccountID < result[j].AccountID })

	return result, nil
}

// DisassociateMember sets a member's relationship status to DISASSOCIATED.
func (b *InMemoryBackend) DisassociateMember(accountID string) error {
	b.mu.Lock("DisassociateMember")
	defer b.mu.Unlock()

	m, ok := b.members.Get(accountID)
	if !ok {
		return ErrMemberNotFound
	}

	m.RelationshipStatus = "DISASSOCIATED"
	m.UpdatedAt = time.Now().UTC()

	return nil
}

// UpdateMemberSession updates the status of a member's Macie session.
func (b *InMemoryBackend) UpdateMemberSession(accountID, status string) error {
	b.mu.Lock("UpdateMemberSession")
	defer b.mu.Unlock()

	m, ok := b.members.Get(accountID)
	if !ok {
		return ErrMemberNotFound
	}

	if status != "" {
		m.RelationshipStatus = status
	}

	m.UpdatedAt = time.Now().UTC()

	return nil
}

// CreateInvitations creates invitations for the given account IDs.
func (b *InMemoryBackend) CreateInvitations(
	accountIDs []string, _ string, _ bool,
) ([]UnprocessedAccount, error) {
	b.mu.Lock("CreateInvitations")
	defer b.mu.Unlock()

	now := time.Now().UTC()

	for _, accountID := range accountIDs {
		id := uuid.New().String()
		b.invitations.Put(&Invitation{
			AccountID:          accountID,
			InvitationID:       id,
			InvitedAt:          now,
			RelationshipStatus: "INVITED",
		})
	}

	return nil, nil
}

// AcceptInvitation accepts a Macie invitation.
func (b *InMemoryBackend) AcceptInvitation(administratorAccountID, invitationID string) error {
	b.mu.Lock("AcceptInvitation")
	defer b.mu.Unlock()

	b.administrator = &AdministratorAccount{
		AccountID:          administratorAccountID,
		InvitationID:       invitationID,
		InvitedAt:          time.Now().UTC(),
		RelationshipStatus: statusEnabled,
	}

	return nil
}

// DeclineInvitations marks invitations from the given accounts as declined.
func (b *InMemoryBackend) DeclineInvitations(accountIDs []string) ([]UnprocessedAccount, error) {
	b.mu.Lock("DeclineInvitations")
	defer b.mu.Unlock()

	decline := make(map[string]bool, len(accountIDs))
	for _, id := range accountIDs {
		decline[id] = true
	}

	for _, inv := range b.invitations.All() {
		if decline[inv.AccountID] {
			inv.RelationshipStatus = "RESIGNED"
		}
	}

	return nil, nil
}

// DeleteInvitations removes invitations from the given accounts.
func (b *InMemoryBackend) DeleteInvitations(accountIDs []string) ([]UnprocessedAccount, error) {
	b.mu.Lock("DeleteInvitations")
	defer b.mu.Unlock()

	toDelete := make(map[string]bool, len(accountIDs))
	for _, id := range accountIDs {
		toDelete[id] = true
	}

	for _, inv := range b.invitations.All() {
		if toDelete[inv.AccountID] {
			b.invitations.Delete(inv.InvitationID)
		}
	}

	return nil, nil
}

// GetInvitationsCount returns the number of active invitations.
func (b *InMemoryBackend) GetInvitationsCount() (int64, error) {
	b.mu.RLock("GetInvitationsCount")
	defer b.mu.RUnlock()

	var count int64

	for _, inv := range b.invitations.All() {
		if inv.RelationshipStatus == "INVITED" {
			count++
		}
	}

	return count, nil
}

// ListInvitations returns all active invitations.
func (b *InMemoryBackend) ListInvitations() ([]*Invitation, error) {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	invitations := b.invitations.All()
	result := make([]*Invitation, 0, len(invitations))

	for _, inv := range invitations {
		cp := *inv
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].InvitationID < result[j].InvitationID })

	return result, nil
}
