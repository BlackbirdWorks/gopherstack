package directoryservice

import (
	"context"
	"sort"
	"time"
)

// CreateTrust creates a trust relationship.
func (b *InMemoryBackend) CreateTrust(
	ctx context.Context,
	directoryID, remoteDomainName, _, trustDirection, trustType, selectiveAuth string,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateTrust")
	defer b.mu.Unlock()

	if _, ok := b.directoryGet(region, directoryID); !ok {
		return "", ErrDirectoryNotFound
	}

	if selectiveAuth == "" {
		selectiveAuth = string(SelectiveAuthDisabled)
	}

	// Real AWS auto-verifies every direction except "One-Way: Incoming" (nothing to verify
	// against locally); terraform's resourceTrustCreate waits on waitTrustVerified for the rest.
	trustState := "Created"
	if trustDirection != "One-Way: Incoming" {
		trustState = "Verified"
	}

	id := newHexID("t-")
	now := time.Now().UTC()
	b.trustPut(&storedTrust{
		region:               region,
		TrustID:              id,
		DirectoryID:          directoryID,
		RemoteDomainName:     remoteDomainName,
		TrustDirection:       trustDirection,
		TrustType:            trustType,
		TrustState:           trustState,
		SelectiveAuth:        selectiveAuth,
		CreatedDateTime:      now,
		LastUpdatedDateTime:  now,
		StateLastUpdatedTime: now,
	})

	// Real AWS sets up DNS conditional forwarding for a trust; terraform's trust Read looks one up.
	// Stored directly here (not via CreateConditionalForwarder) to avoid deadlocking on b.mu.
	if _, exists := b.conditionalForwarderGet(region, directoryID, remoteDomainName); !exists {
		b.conditionalForwarderPut(&storedConditionalForwarder{
			region:           region,
			DirectoryID:      directoryID,
			RemoteDomainName: remoteDomainName,
			ReplicationScope: "Domain",
		})
	}

	return id, nil
}

// DeleteTrust deletes a trust relationship.
func (b *InMemoryBackend) DeleteTrust(ctx context.Context, trustID string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteTrust")
	defer b.mu.Unlock()

	trust, ok := b.trustGet(region, trustID)
	if !ok {
		return "", ErrTrustNotFound
	}

	b.trustDelete(region, trustID)
	b.conditionalForwarderDelete(region, trust.DirectoryID, trust.RemoteDomainName)

	return trustID, nil
}

// DescribeTrusts returns trusts for a directory.
func (b *InMemoryBackend) DescribeTrusts(
	ctx context.Context,
	directoryID string,
	trustIDs []string,
	limit int32,
	nextToken string,
) ([]TrustInfo, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeTrusts")
	defer b.mu.RUnlock()

	filterSet := make(map[string]bool, len(trustIDs))
	for _, id := range trustIDs {
		filterSet[id] = true
	}

	var ids []string
	for _, t := range b.trustsInRegion(region) {
		if directoryID != "" && t.DirectoryID != directoryID {
			continue
		}
		if len(filterSet) > 0 && !filterSet[t.TrustID] {
			continue
		}
		ids = append(ids, t.TrustID)
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
	result := make([]TrustInfo, 0, end-start)
	for _, id := range ids[start:end] {
		t, _ := b.trustGet(region, id)
		result = append(result, TrustInfo{
			TrustID:              t.TrustID,
			DirectoryID:          t.DirectoryID,
			RemoteDomainName:     t.RemoteDomainName,
			TrustDirection:       t.TrustDirection,
			TrustType:            t.TrustType,
			TrustState:           t.TrustState,
			SelectiveAuth:        t.SelectiveAuth,
			TrustStateReason:     t.TrustStateReason,
			CreatedDateTime:      t.CreatedDateTime,
			LastUpdatedDateTime:  t.LastUpdatedDateTime,
			StateLastUpdatedTime: t.StateLastUpdatedTime,
		})
	}

	var outToken string
	if end < len(ids) {
		outToken = ids[end]
	}

	return result, outToken, nil
}

// UpdateTrust updates a trust relationship.
func (b *InMemoryBackend) UpdateTrust(ctx context.Context, trustID, selectiveAuth string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateTrust")
	defer b.mu.Unlock()

	t, ok := b.trustGet(region, trustID)
	if !ok {
		return "", ErrTrustNotFound
	}

	if selectiveAuth != "" {
		t.SelectiveAuth = selectiveAuth
	}
	t.LastUpdatedDateTime = time.Now().UTC()

	return trustID, nil
}

// VerifyTrust verifies a trust relationship.
func (b *InMemoryBackend) VerifyTrust(ctx context.Context, trustID string) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("VerifyTrust")
	defer b.mu.Unlock()

	t, ok := b.trustGet(region, trustID)
	if !ok {
		return "", ErrTrustNotFound
	}

	t.TrustState = "Verified"
	t.LastUpdatedDateTime = time.Now().UTC()
	t.StateLastUpdatedTime = time.Now().UTC()

	return trustID, nil
}
