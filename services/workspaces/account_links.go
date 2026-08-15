package workspaces

// accountLinkStatusPendingAcceptance is the real AccountLinkStatusEnum value
// for a newly created, not-yet-accepted invitation. The previous
// "PENDING_ACCEPTANCE" here was not a member of the real enum at all.
const accountLinkStatusPendingAcceptance = "PENDING_ACCEPTANCE_BY_TARGET_ACCOUNT"

// CreateAccountLinkInvitation creates an account link invitation.
func (b *InMemoryBackend) CreateAccountLinkInvitation(
	targetAccountID string,
) (*storedAccountLink, error) {
	b.mu.Lock("CreateAccountLinkInvitation")
	defer b.mu.Unlock()

	id := b.nextID("wsal-")
	link := &storedAccountLink{
		LinkID:          id,
		Status:          accountLinkStatusPendingAcceptance,
		SourceAccountID: b.accountID,
		TargetAccountID: targetAccountID,
	}
	b.accountLinks.Put(link)

	cp := *link

	return &cp, nil
}

// AcceptAccountLinkInvitation accepts an account link.
func (b *InMemoryBackend) AcceptAccountLinkInvitation(linkID string) (*storedAccountLink, error) {
	b.mu.Lock("AcceptAccountLinkInvitation")
	defer b.mu.Unlock()

	link, ok := b.accountLinks.Get(linkID)
	if !ok {
		return nil, errAccountLinkNotFound
	}

	link.Status = "LINKED"
	cp := *link

	return &cp, nil
}

// RejectAccountLinkInvitation rejects an account link.
func (b *InMemoryBackend) RejectAccountLinkInvitation(linkID string) (*storedAccountLink, error) {
	b.mu.Lock("RejectAccountLinkInvitation")
	defer b.mu.Unlock()

	link, ok := b.accountLinks.Get(linkID)
	if !ok {
		return nil, errAccountLinkNotFound
	}

	link.Status = "REJECTED"
	cp := *link

	return &cp, nil
}

// DeleteAccountLinkInvitation deletes a pending account link invitation.
// "DELETED" is not a member of the real AccountLinkStatusEnum, so the
// returned AccountLink keeps whatever status it already had (this backend
// only reaches this op while a link is still
// PENDING_ACCEPTANCE_BY_TARGET_ACCOUNT) rather than fabricating one.
func (b *InMemoryBackend) DeleteAccountLinkInvitation(linkID string) (*storedAccountLink, error) {
	b.mu.Lock("DeleteAccountLinkInvitation")
	defer b.mu.Unlock()

	link, ok := b.accountLinks.Get(linkID)
	if !ok {
		return nil, errAccountLinkNotFound
	}

	cp := *link
	b.accountLinks.Delete(linkID)

	return &cp, nil
}

// GetAccountLink retrieves an account link by ID.
func (b *InMemoryBackend) GetAccountLink(linkID string) (*storedAccountLink, error) {
	b.mu.RLock("GetAccountLink")
	defer b.mu.RUnlock()

	link, ok := b.accountLinks.Get(linkID)
	if !ok {
		return nil, errAccountLinkNotFound
	}

	cp := *link

	return &cp, nil
}

// ListAccountLinks returns account links, optionally filtered by status.
func (b *InMemoryBackend) ListAccountLinks(
	statusFilter string,
	_ int32,
	_ string,
) ([]*storedAccountLink, string, error) {
	b.mu.RLock("ListAccountLinks")
	defer b.mu.RUnlock()

	var result []*storedAccountLink

	for _, link := range b.accountLinks.All() {
		if statusFilter != "" && link.Status != statusFilter {
			continue
		}

		cp := *link
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedAccountLink{}
	}

	return result, "", nil
}
