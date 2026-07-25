package s3control

import "fmt"

// CreateMultiRegionAccessPoint creates an async MRAP request and stores the MRAP instance.
func (b *InMemoryBackend) CreateMultiRegionAccessPoint(
	accountID, name, _ string,
) *MultiRegionAccessPointRequest {
	b.mu.Lock("CreateMultiRegionAccessPoint")
	defer b.mu.Unlock()

	token := b.newID("mrap-token")
	tokenARN := fmt.Sprintf(arnFmtMRAPToken, accountID, token)

	req := &MultiRegionAccessPointRequest{
		AccountID:       accountID,
		Token:           token,
		RequestTokenARN: tokenARN,
		Name:            name,
	}
	b.mrapRequests.Put(req)

	// Also store the real MRAP object so GetMultiRegionAccessPoint can retrieve it.
	alias := fmt.Sprintf("%s.mrap.accesspoint.s3-global.amazonaws.com", name)
	mrap := &MultiRegionAccessPoint{
		AccountID: accountID,
		Name:      name,
		Alias:     alias,
		Status:    "READY",
		CreatedAt: nowRFC3339(),
	}
	b.mraps.Put(mrap)

	cp := *req

	return &cp
}

// SetMRAPRegions stores the bucket-region list for an MRAP.
func (b *InMemoryBackend) SetMRAPRegions(accountID, name string, regions []string) error {
	b.mu.Lock("SetMRAPRegions")
	defer b.mu.Unlock()

	mrap, ok := b.mraps.Get(accountID + ":" + name)
	if !ok {
		return fmt.Errorf("%w: %s", errMRAPNotFound, name)
	}

	cp := make([]string, len(regions))
	copy(cp, regions)
	mrap.Regions = cp

	return nil
}

// GetMultiRegionAccessPoint retrieves an MRAP by name.
func (b *InMemoryBackend) GetMultiRegionAccessPoint(accountID, name string) (*MultiRegionAccessPoint, error) {
	b.mu.RLock("GetMultiRegionAccessPoint")
	defer b.mu.RUnlock()

	mrap, ok := b.mraps.Get(accountID + ":" + name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errMRAPNotFound, name)
	}

	cp := *mrap

	return &cp, nil
}

// DeleteMultiRegionAccessPoint removes an MRAP and cascade-cleans its route
// configuration (policy lives on the MultiRegionAccessPoint value itself via
// PutMultiRegionAccessPointPolicy, so it goes away with the row).
//
// LEAK FIX: this previously only checked b.mraps.Has(key) and returned nil
// without ever calling b.mraps.Delete(key) -- a deleted MRAP was never
// actually removed from the backing table. Both the synchronous DELETE
// /v20180820/mrap/instances/{Name} route and the async POST
// /v20180820/async-requests/mrap/delete route call this same method, so
// every DeleteMultiRegionAccessPoint call (sync or async) was a silent
// no-op: the MRAP stayed retrievable via GetMultiRegionAccessPoint /
// ListMultiRegionAccessPoints forever, and repeated create/delete cycles
// under new names accumulated unbounded ghost rows in b.mraps. No existing
// test caught this because the table-driven "delete_mrap" case only
// asserted err == nil, never that the resource was actually gone.
func (b *InMemoryBackend) DeleteMultiRegionAccessPoint(accountID, name string) error {
	b.mu.Lock("DeleteMultiRegionAccessPoint")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	if !b.mraps.Has(key) {
		return fmt.Errorf("%w: %s", errMRAPNotFound, name)
	}

	b.mraps.Delete(key)
	delete(b.mrapRoutes, key)

	return nil
}

// ListMultiRegionAccessPoints returns all MRAPs for an account.
func (b *InMemoryBackend) ListMultiRegionAccessPoints(accountID string) []*MultiRegionAccessPoint {
	b.mu.RLock("ListMultiRegionAccessPoints")
	defer b.mu.RUnlock()

	var out []*MultiRegionAccessPoint

	for _, v := range b.mraps.All() {
		if v.AccountID == accountID {
			cp := *v
			out = append(out, &cp)
		}
	}

	return out
}

// PutMultiRegionAccessPointPolicy stores a policy for an MRAP.
func (b *InMemoryBackend) PutMultiRegionAccessPointPolicy(accountID, name, policy string) error {
	b.mu.Lock("PutMultiRegionAccessPointPolicy")
	defer b.mu.Unlock()

	key := accountID + ":" + name
	mrap, ok := b.mraps.Get(key)
	if !ok {
		return ErrNotFound
	}

	mrap.Policy = policy

	return nil
}

// ---- MRAP ----

// DescribeMultiRegionAccessPointOperation returns the status of an MRAP async operation.
func (b *InMemoryBackend) DescribeMultiRegionAccessPointOperation(
	accountID, requestToken string,
) (*MultiRegionAccessPointRequest, error) {
	b.mu.RLock("DescribeMultiRegionAccessPointOperation")
	defer b.mu.RUnlock()

	// Try direct key lookup (bare token).
	if req, ok := b.mrapRequests.Get(accountID + ":" + requestToken); ok {
		return req, nil
	}
	// Fall back to ARN match.
	for _, req := range b.mrapRequests.All() {
		if req.AccountID == accountID && req.RequestTokenARN == requestToken {
			return req, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", errMRAPNotFound, requestToken)
}

// GetMultiRegionAccessPointPolicy returns the policy for an MRAP.
func (b *InMemoryBackend) GetMultiRegionAccessPointPolicy(accountID, name string) (string, error) {
	b.mu.RLock("GetMultiRegionAccessPointPolicy")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	mrapObj, ok := b.mraps.Get(key)
	if !ok {
		return "", fmt.Errorf("%w: %s", errMRAPNotFound, name)
	}

	return mrapObj.Policy, nil
}

// GetMultiRegionAccessPointPolicyStatus returns whether the MRAP policy is public.
func (b *InMemoryBackend) GetMultiRegionAccessPointPolicyStatus(
	accountID, name string,
) (bool, error) {
	b.mu.RLock("GetMultiRegionAccessPointPolicyStatus")
	defer b.mu.RUnlock()

	key := accountID + ":" + name
	mrapObj, ok := b.mraps.Get(key)
	if !ok {
		return false, fmt.Errorf("%w: %s", errMRAPNotFound, name)
	}

	return mrapObj.Policy != "", nil
}

// GetMultiRegionAccessPointRoutes returns the routing configuration for an MRAP.
func (b *InMemoryBackend) GetMultiRegionAccessPointRoutes(accountID, mrap string) (string, error) {
	b.mu.RLock("GetMultiRegionAccessPointRoutes")
	defer b.mu.RUnlock()

	key := accountID + ":" + mrap
	if !b.mraps.Has(key) {
		return "", fmt.Errorf("%w: %s", errMRAPNotFound, mrap)
	}

	return b.mrapRoutes[key], nil
}

// ---- MRAP Routes (submit) ----

// SubmitMultiRegionAccessPointRoutes stores routing configuration for an MRAP.
func (b *InMemoryBackend) SubmitMultiRegionAccessPointRoutes(accountID, mrap, routes string) error {
	b.mu.Lock("SubmitMultiRegionAccessPointRoutes")
	defer b.mu.Unlock()

	key := accountID + ":" + mrap
	b.mrapRoutes[key] = routes

	return nil
}
