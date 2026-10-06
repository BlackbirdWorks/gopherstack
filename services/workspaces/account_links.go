package workspaces

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
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
	return b.CreateAccountLinkInvitationWithToken(targetAccountID, "")
}

// CreateAccountLinkInvitationWithToken is CreateAccountLinkInvitation honouring
// ClientToken: a live token replays the link it created.
func (b *InMemoryBackend) CreateAccountLinkInvitationWithToken(
	targetAccountID, clientToken string,
) (*storedAccountLink, error) {
	b.mu.Lock("CreateAccountLinkInvitation")
	defer b.mu.Unlock()

	fp := idemFingerprint(targetAccountID)

	prior, replay, err := b.idemReplay("CreateAccountLinkInvitation", clientToken, fp)
	if err != nil {
		return nil, err
	}

	if replay {
		if link, ok := b.accountLinks.Get(prior); ok {
			cp := *link

			return &cp, nil
		}
	}

	id := b.nextID("wsal-")
	b.idemRecord("CreateAccountLinkInvitation", clientToken, fp, id)
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
	return b.GetAccountLinkBy(linkID, "")
}

// GetAccountLinkBy looks a link up by LinkId or, when that is empty, by the
// LinkedAccountId of its other party. Exactly one of the two is required.
func (b *InMemoryBackend) GetAccountLinkBy(linkID, linkedAccountID string) (*storedAccountLink, error) {
	if (linkID == "") == (linkedAccountID == "") {
		return nil, awserr.New("specify exactly one of LinkId or LinkedAccountId", awserr.ErrInvalidParameter)
	}

	b.mu.RLock("GetAccountLink")
	defer b.mu.RUnlock()

	if linkID != "" {
		link, ok := b.accountLinks.Get(linkID)
		if !ok {
			return nil, errAccountLinkNotFound
		}

		cp := *link

		return &cp, nil
	}

	all := b.accountLinks.All()
	sort.Slice(all, func(i, j int) bool { return all[i].LinkID < all[j].LinkID })

	for _, link := range all {
		if link.SourceAccountID == linkedAccountID || link.TargetAccountID == linkedAccountID {
			cp := *link

			return &cp, nil
		}
	}

	return nil, errAccountLinkNotFound
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
