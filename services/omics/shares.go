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

// GetShare retrieves a share, advancing ACTIVATING→ACTIVE on first poll,
// mirroring GetWorkflow/GetAnnotationStore/GetVariantStore's reap-on-read
// pattern. AcceptShare stamped Status ACTIVATING and nothing else in this
// backend ever advanced it -- a client polling GetShare for readiness never
// saw a terminal status. PENDING is left alone: it is the correct
// client-driven wait for AcceptShare/RejectShare, not a stall.
func (b *InMemoryBackend) GetShare(shareID string) (*Share, error) {
	b.mu.Lock("GetShare")
	defer b.mu.Unlock()

	share, ok := b.shares.Get(shareID)
	if !ok {
		return nil, fmt.Errorf("%w: share %s not found", ErrNotFound, shareID)
	}

	if share.Status == "ACTIVATING" {
		share.pollCount++
		if share.pollCount >= 1 {
			share.Status = "ACTIVE"
		}
	}

	result := *share

	return &result, nil
}

// shareResourceType derives the real AWS ShareResourceType (VARIANT_STORE/
// ANNOTATION_STORE/WORKFLOW) from a share's resource ARN, whose resource
// segment is built by arn.Build as "variantStore/…", "annotationStore/…", or
// "workflow/…" (see CreateVariantStore/CreateAnnotationStore/CreateWorkflow).
func shareResourceType(resourceARN string) string {
	switch {
	case strings.Contains(resourceARN, ":variantStore/"):
		return "VARIANT_STORE"
	case strings.Contains(resourceARN, ":annotationStore/"):
		return "ANNOTATION_STORE"
	case strings.Contains(resourceARN, ":workflow/"):
		return "WORKFLOW"
	default:
		return ""
	}
}

// ListShares lists shares by resource owner, optionally filtered by
// resourceArns/status/type (real AWS ListSharesInput body "filter",
// omics@v1.49.5 types/types.go:678).
func (b *InMemoryBackend) ListShares(
	resourceOwner string,
	filter *ShareFilter,
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
			if !isSelf {
				continue
			}
		case "OTHER":
			if isSelf {
				continue
			}
		}

		if !shareMatchesFilter(s.ResourceARN, s.Status, shareResourceType(s.ResourceARN), filter) {
			continue
		}

		ids = append(ids, s.ShareID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.shares.Get)

	return result, outToken, nil
}
