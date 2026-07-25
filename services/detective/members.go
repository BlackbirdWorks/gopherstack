package detective

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

// CreateMembers creates or invites member accounts to a behavior graph.
func (b *InMemoryBackend) CreateMembers(
	graphARN string,
	accounts []Account,
	_ string,
) ([]*MemberDetail, []UnprocessedAccount, error) {
	if len(accounts) > maxCreateMembersPerBatch {
		return nil, nil, fmt.Errorf("%w: cannot specify more than %d accounts", ErrValidation, maxCreateMembersPerBatch)
	}

	for _, acc := range accounts {
		if !validateAccountID(acc.AccountID) {
			return nil, nil, fmt.Errorf("%w: account ID must be a 12-digit number", ErrValidation)
		}

		if acc.EmailAddress != "" && !validateEmail(acc.EmailAddress) {
			return nil, nil, fmt.Errorf("%w: invalid email address format", ErrValidation)
		}
	}

	b.mu.Lock("CreateMembers")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return nil, nil, ErrGraphNotFound
	}

	now := time.Now().UTC()

	var members []*MemberDetail
	var unprocessed []UnprocessedAccount

	for _, acc := range accounts {
		if acc.AccountID == b.accountID {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: acc.AccountID,
				Reason:    "Cannot invite the administrator account",
			})

			continue
		}

		// AWS documents that CreateMembers cannot re-process an account that
		// was already invited: it is reported back via UnprocessedAccounts,
		// not silently returned as a freshly-processed member.
		if b.members.Has(memberKey(graphARN, acc.AccountID)) {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: acc.AccountID,
				Reason:    "Account is already a member of the behavior graph",
			})

			continue
		}

		m := &storedMember{
			AccountID:       acc.AccountID,
			AdministratorID: b.accountID,
			EmailAddress:    acc.EmailAddress,
			GraphARN:        graphARN,
			InvitedTime:     now,
			Status:          memberStatusInvited,
			UpdatedTime:     now,
		}
		b.members.Put(m)
		cp := m.toMemberDetail(b.datasources[graphARN])
		members = append(members, &cp)
	}

	return members, unprocessed, nil
}

// DeleteMembers removes member accounts from a behavior graph.
func (b *InMemoryBackend) DeleteMembers(
	graphARN string,
	accountIDs []string,
) ([]string, []UnprocessedAccount, error) {
	b.mu.Lock("DeleteMembers")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return nil, nil, ErrGraphNotFound
	}

	deleted := make([]string, 0, len(accountIDs))
	unprocessed := make([]UnprocessedAccount, 0)

	for _, id := range accountIDs {
		key := memberKey(graphARN, id)
		if !b.members.Has(key) {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: id,
				Reason:    reasonMemberNotFoundInGraph,
			})

			continue
		}
		b.members.Delete(key)
		deleted = append(deleted, id)
	}

	return deleted, unprocessed, nil
}

// GetMembers returns member details for the given account IDs.
func (b *InMemoryBackend) GetMembers(
	graphARN string,
	accountIDs []string,
) ([]*MemberDetail, []UnprocessedAccount, error) {
	b.mu.RLock("GetMembers")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, nil, ErrGraphNotFound
	}

	var members []*MemberDetail
	var unprocessed []UnprocessedAccount

	for _, id := range accountIDs {
		if m, ok := b.members.Get(memberKey(graphARN, id)); ok {
			cp := m.toMemberDetail(b.datasources[graphARN])
			members = append(members, &cp)
		} else {
			unprocessed = append(unprocessed, UnprocessedAccount{
				AccountID: id,
				Reason:    reasonMemberNotFoundInGraph,
			})
		}
	}

	return members, unprocessed, nil
}

// ListMembers returns member accounts for a behavior graph.
func (b *InMemoryBackend) ListMembers(
	graphARN string,
	maxResults int32,
	nextToken string,
) ([]*MemberDetail, string, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	if !b.graphs.Has(graphARN) {
		return nil, "", ErrGraphNotFound
	}

	items := slices.Clone(b.membersByGraph.Get(graphARN))
	sort.Slice(items, func(i, j int) bool { return items[i].AccountID < items[j].AccountID })

	start, err := decodePageToken(nextToken)
	if err != nil {
		return nil, "", err
	}

	if start > len(items) {
		start = len(items)
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxMembersPerPage {
		limit = maxMembersPerPage
	}

	end := min(start+limit, len(items))

	result := make([]*MemberDetail, 0, end-start)
	for _, m := range items[start:end] {
		cp := m.toMemberDetail(b.datasources[graphARN])
		result = append(result, &cp)
	}

	var outToken string
	if end < len(items) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}

// AcceptInvitation accepts a graph invitation on behalf of the member account.
func (b *InMemoryBackend) AcceptInvitation(graphARN string) error {
	b.mu.Lock("AcceptInvitation")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	m, ok := b.members.Get(memberKey(graphARN, b.accountID))
	if !ok {
		return ErrMemberNotFound
	}

	if m.Status != memberStatusInvited {
		return fmt.Errorf("%w: member status must be INVITED", ErrValidation)
	}

	now := time.Now().UTC()
	m.Status = memberStatusEnabled
	m.UpdatedTime = now

	return nil
}

// RejectInvitation rejects a graph invitation on behalf of the member account.
func (b *InMemoryBackend) RejectInvitation(graphARN string) error {
	b.mu.Lock("RejectInvitation")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	m, ok := b.members.Get(memberKey(graphARN, b.accountID))
	if !ok {
		return ErrMemberNotFound
	}

	if m.Status != memberStatusInvited {
		return fmt.Errorf("%w: member status must be INVITED", ErrValidation)
	}

	b.members.Delete(memberKey(graphARN, b.accountID))

	return nil
}

// DisassociateMembership removes the calling account from a graph it belongs to.
func (b *InMemoryBackend) DisassociateMembership(graphARN string) error {
	b.mu.Lock("DisassociateMembership")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	m, ok := b.members.Get(memberKey(graphARN, b.accountID))
	if !ok {
		return ErrMemberNotFound
	}

	if m.Status != memberStatusEnabled {
		return fmt.Errorf("%w: member status must be ENABLED", ErrValidation)
	}

	b.members.Delete(memberKey(graphARN, b.accountID))

	return nil
}

// ListInvitations returns graphs where this account has an open or accepted invitation.
func (b *InMemoryBackend) ListInvitations(maxResults int32, nextToken string) ([]*MemberDetail, string, error) {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	var invitations []*MemberDetail
	for _, m := range b.members.All() {
		if m.AccountID == b.accountID && (m.Status == memberStatusInvited || m.Status == memberStatusEnabled) {
			cp := m.toMemberDetail(b.datasources[m.GraphARN])
			invitations = append(invitations, &cp)
		}
	}

	sort.Slice(invitations, func(i, j int) bool {
		return invitations[i].GraphARN < invitations[j].GraphARN
	})

	start, err := decodePageToken(nextToken)
	if err != nil {
		return nil, "", err
	}

	if start > len(invitations) {
		start = len(invitations)
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxInvitationsPerPage {
		limit = maxInvitationsPerPage
	}

	end := min(start+limit, len(invitations))
	result := invitations[start:end]

	var outToken string
	if end < len(invitations) {
		outToken = encodePageToken(end)
	}

	return result, outToken, nil
}
