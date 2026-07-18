package directoryservice

import (
	"context"
	"slices"
	"sort"
	"time"
)

// CreateLogSubscription creates a log subscription.
func (b *InMemoryBackend) CreateLogSubscription(ctx context.Context, directoryID, logGroupName string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateLogSubscription")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	if _, exists := b.logSubscriptionGet(region, directoryID, logGroupName); exists {
		return ErrAliasAlreadyExists
	}

	b.logSubscriptionPut(&storedLogSubscription{
		region:                      region,
		DirectoryID:                 directoryID,
		LogGroupName:                logGroupName,
		SubscriptionCreatedDateTime: time.Now().UTC(),
	})

	return nil
}

// DeleteLogSubscription deletes a log subscription.
func (b *InMemoryBackend) DeleteLogSubscription(ctx context.Context, directoryID string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteLogSubscription")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return ErrDirectoryNotFound
	}

	for _, sub := range slices.Clone(b.logSubscriptionsInRegion(region)) {
		if sub.DirectoryID == directoryID {
			b.logSubscriptions.Delete(regionKey(region, logSubscriptionID(sub.DirectoryID, sub.LogGroupName)))
		}
	}

	return nil
}

// ListLogSubscriptions returns log subscriptions.
func (b *InMemoryBackend) ListLogSubscriptions(
	ctx context.Context,
	directoryID string,
	limit int32,
	nextToken string, //nolint:revive // existing issue.
) ([]LogSubscription, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListLogSubscriptions")
	defer b.mu.RUnlock()

	var all []storedLogSubscription
	for _, sub := range b.logSubscriptionsInRegion(region) {
		if directoryID != "" && sub.DirectoryID != directoryID {
			continue
		}
		all = append(all, *sub)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].DirectoryID == all[j].DirectoryID {
			return all[i].LogGroupName < all[j].LogGroupName
		}

		return all[i].DirectoryID < all[j].DirectoryID
	})

	start := 0
	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}

	end := min(start+pageSize, len(all))
	result := make([]LogSubscription, 0, end-start)
	for _, sub := range all[start:end] {
		result = append(result, LogSubscription{
			DirectoryID:  sub.DirectoryID,
			LogGroupName: sub.LogGroupName,
			CreatedTime:  sub.SubscriptionCreatedDateTime,
		})
	}

	var outToken string
	if end < len(all) {
		outToken = all[end].DirectoryID + ":" + all[end].LogGroupName
	}

	return result, outToken, nil
}
