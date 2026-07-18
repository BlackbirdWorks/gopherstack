package omics

import (
	"fmt"
	"strings"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// Share
// ────────────────────────────────────────────────────────────────────────────

// CreateShare creates a new resource share.
func (b *InMemoryBackend) CreateShare(
	resourceARN, principalSubscriber, name string,
) (*Share, error) {
	b.mu.Lock("CreateShare")
	defer b.mu.Unlock()

	id := newID()
	now := time.Now().UTC()
	share := &Share{
		ShareID:             id,
		ResourceARN:         resourceARN,
		PrincipalSubscriber: principalSubscriber,
		Name:                name,
		Status:              "PENDING",
		CreationTime:        now,
	}
	b.shares.Put(share)

	result := *share

	return &result, nil
}

// AcceptShare accepts a share invitation.
func (b *InMemoryBackend) AcceptShare(shareID string) (*Share, error) {
	b.mu.Lock("AcceptShare")
	defer b.mu.Unlock()

	share, ok := b.shares.Get(shareID)
	if !ok {
		return nil, fmt.Errorf("%w: share %s not found", ErrNotFound, shareID)
	}

	share.Status = "ACTIVATING"
	now := time.Now().UTC()
	share.UpdateTime = &now
	result := *share

	return &result, nil
}

// DeleteShare deletes a share.
func (b *InMemoryBackend) DeleteShare(shareID string) (*Share, error) {
	b.mu.Lock("DeleteShare")
	defer b.mu.Unlock()

	share, ok := b.shares.Get(shareID)
	if !ok {
		return nil, fmt.Errorf("%w: share %s not found", ErrNotFound, shareID)
	}

	b.shares.Delete(shareID)
	result := *share
	result.Status = "DELETED"

	return &result, nil
}

// GetShare retrieves a share.
func (b *InMemoryBackend) GetShare(shareID string) (*Share, error) {
	b.mu.RLock("GetShare")
	defer b.mu.RUnlock()

	share, ok := b.shares.Get(shareID)
	if !ok {
		return nil, fmt.Errorf("%w: share %s not found", ErrNotFound, shareID)
	}

	result := *share

	return &result, nil
}

// ListShares lists shares by resource owner.
func (b *InMemoryBackend) ListShares(
	resourceOwner string,
	maxResults int,
	nextToken string,
) ([]*Share, string, error) {
	b.mu.RLock("ListShares")
	defer b.mu.RUnlock()

	all := b.shares.All()

	var ids []string

	for _, s := range all {
		isSelf := strings.Contains(s.ResourceARN, ":"+b.accountID+":")

		switch resourceOwner {
		case "SELF":
			if isSelf {
				ids = append(ids, s.ShareID)
			}
		case "OTHER":
			if !isSelf {
				ids = append(ids, s.ShareID)
			}
		default:
			ids = append(ids, s.ShareID)
		}
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.shares.Get)

	return result, outToken, nil
}
