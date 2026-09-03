package workspaces

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// accountLinkStatusPendingAcceptance is the real AccountLinkStatusEnum value
// for a newly created, not-yet-accepted invitation. The previous
// "PENDING_ACCEPTANCE" here was not a member of the real enum at all.
const accountLinkStatusPendingAcceptance = "PENDING_ACCEPTANCE_BY_TARGET_ACCOUNT"

// accountLinksPageSize is this backend's default page size for
// ListAccountLinks; real AWS doesn't document an exact default, so this is
// chosen generously (larger than any realistic per-account link count) so
// pagination only activates when a caller explicitly requests a smaller
// MaxResults.
const accountLinksPageSize = 100

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
	maxResults int32,
	nextToken string,
) ([]*storedAccountLink, string, error) {
	b.mu.RLock("ListAccountLinks")
	defer b.mu.RUnlock()

	all := b.accountLinks.All()

	sort.Slice(all, func(i, j int) bool { return all[i].LinkID < all[j].LinkID })

	result := make([]*storedAccountLink, 0, len(all))

	for _, link := range all {
		if statusFilter != "" && link.Status != statusFilter {
			continue
		}

		cp := *link
		result = append(result, &cp)
	}

	pg := page.New(result, nextToken, int(maxResults), accountLinksPageSize)

	return pg.Data, pg.Next, nil
}
