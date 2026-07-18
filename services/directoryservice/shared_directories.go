package directoryservice

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ShareDirectory shares a directory.
func (b *InMemoryBackend) ShareDirectory(
	ctx context.Context,
	directoryID, shareMethod, shareNotes, targetID string,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("ShareDirectory")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return "", ErrDirectoryNotFound
	}

	id := fmt.Sprintf("d-%s", uuid.NewString()[:10])
	now := time.Now().UTC()

	// HANDSHAKE shares require the consumer account to call
	// AcceptSharedDirectory before the share is active, so they start
	// PendingAcceptance; ORGANIZATIONS shares need no handshake and are
	// Shared immediately. Matches AWS's ShareStatus lifecycle.
	shareStatus := "Shared"
	if shareMethod == "HANDSHAKE" {
		shareStatus = "PendingAcceptance"
	}

	b.sharedDirectoryPut(&storedSharedDirectory{
		region:              region,
		SharedDirectoryID:   id,
		OwnerDirectoryID:    directoryID,
		OwnerAccountID:      b.accountID,
		SharedAccountID:     targetID,
		ShareMethod:         shareMethod,
		ShareStatus:         shareStatus,
		ShareNotes:          shareNotes,
		CreatedDateTime:     now,
		LastUpdatedDateTime: now,
	})

	return id, nil
}

// UnshareDirectory unshares a directory.
func (b *InMemoryBackend) UnshareDirectory(ctx context.Context, directoryID, targetID string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UnshareDirectory")
	defer b.mu.Unlock()

	for _, sd := range b.sharedDirectoriesInRegion(region) {
		if sd.OwnerDirectoryID == directoryID && sd.SharedAccountID == targetID {
			sd.ShareStatus = "Deleted"
			sd.LastUpdatedDateTime = time.Now().UTC()

			return sd.SharedDirectoryID, nil
		}
	}

	return "", ErrSharedDirectoryNotFound
}

// AcceptSharedDirectory accepts a shared directory.
func (b *InMemoryBackend) AcceptSharedDirectory(ctx context.Context, sharedDirectoryID string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("AcceptSharedDirectory")
	defer b.mu.Unlock()

	sd, ok := b.sharedDirectoryGet(region, sharedDirectoryID)
	if !ok {
		return "", ErrSharedDirectoryNotFound
	}

	sd.ShareStatus = "Shared"
	sd.LastUpdatedDateTime = time.Now().UTC()

	return sharedDirectoryID, nil
}

// RejectSharedDirectory rejects a shared directory.
func (b *InMemoryBackend) RejectSharedDirectory(ctx context.Context, sharedDirectoryID string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RejectSharedDirectory")
	defer b.mu.Unlock()

	sd, ok := b.sharedDirectoryGet(region, sharedDirectoryID)
	if !ok {
		return "", ErrSharedDirectoryNotFound
	}

	sd.ShareStatus = "Rejected"
	sd.LastUpdatedDateTime = time.Now().UTC()

	return sharedDirectoryID, nil
}

// DescribeSharedDirectories returns shared directories for an owner directory.
func (b *InMemoryBackend) DescribeSharedDirectories(
	ctx context.Context,
	ownerDirID string,
	sharedDirIDs []string,
	limit int32,
	nextToken string,
) ([]SharedDirInfo, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeSharedDirectories")
	defer b.mu.RUnlock()

	filterSet := make(map[string]bool, len(sharedDirIDs))
	for _, id := range sharedDirIDs {
		filterSet[id] = true
	}

	var ids []string
	for _, sd := range b.sharedDirectoriesInRegion(region) {
		if ownerDirID != "" && sd.OwnerDirectoryID != ownerDirID {
			continue
		}
		if len(filterSet) > 0 && !filterSet[sd.SharedDirectoryID] {
			continue
		}
		ids = append(ids, sd.SharedDirectoryID)
	}
	sort.Strings(ids)

	start := 0
	if nextToken != "" {
		for i, id := range ids {
			if id == nextToken {
				start = i

				break
			}
		}
	}

	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(ids))
	result := make([]SharedDirInfo, 0, end-start)
	for _, id := range ids[start:end] {
		sd, _ := b.sharedDirectoryGet(region, id)
		result = append(result, SharedDirInfo{
			SharedDirectoryID:   sd.SharedDirectoryID,
			OwnerDirectoryID:    sd.OwnerDirectoryID,
			OwnerAccountID:      sd.OwnerAccountID,
			SharedAccountID:     sd.SharedAccountID,
			ShareMethod:         sd.ShareMethod,
			ShareStatus:         sd.ShareStatus,
			ShareNotes:          sd.ShareNotes,
			CreatedDateTime:     sd.CreatedDateTime,
			LastUpdatedDateTime: sd.LastUpdatedDateTime,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}
