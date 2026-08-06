package outposts

import "slices"

// seedAssetForOutpostLocked provisions exactly one COMPUTE asset for a
// newly-created Outpost. Real Outposts assets arrive via physical hardware
// installation, not a public CreateAsset API (there is none among these 43
// operations) -- this is a deliberate, documented choice so
// ListAssets/StartCapacityTask/GetOutpostInstanceTypes have real state to
// read and mutate rather than always-empty collections. See PARITY.md's
// "Asset seeding" note. Callers must hold b.mu.
func (b *InMemoryBackend) seedAssetForOutpostLocked(outpostID string) *Asset {
	a := &Asset{
		ID:        newAssetID(),
		OutpostID: outpostID,
		RackID:    "rack-" + outpostID,
		AssetType: AssetTypeCompute,
		ComputeAttributes: &ComputeAttributes{
			State: AssetStateActive,
		},
	}

	b.assets.Put(a)

	return a
}

// findAssetLocked resolves a bare Asset ID (no ARN form exists for assets).
// Callers must hold b.mu.
func (b *InMemoryBackend) findAssetLocked(assetID string) (*Asset, bool) {
	return b.assets.Get(assetID)
}

// assetFilter holds ListAssets' optional filters.
type assetFilter struct {
	assetTypes []string
	hostIDs    []string
	statuses   []string
}

func matchesAssetFilter(a *Asset, f assetFilter) bool {
	if len(f.assetTypes) > 0 && !containsStr(f.assetTypes, a.AssetType) {
		return false
	}

	if len(f.hostIDs) > 0 {
		if a.ComputeAttributes == nil || !containsStr(f.hostIDs, a.ComputeAttributes.HostID) {
			return false
		}
	}

	if len(f.statuses) > 0 {
		if a.ComputeAttributes == nil || !containsStr(f.statuses, a.ComputeAttributes.State) {
			return false
		}
	}

	return true
}

// ListAssets returns the assets belonging to outpostID, filtered by f, or a
// NotFoundException if the Outpost does not exist.
func (b *InMemoryBackend) ListAssets(outpostIdentifier string, f assetFilter) ([]*Asset, error) {
	b.mu.RLock("ListAssets")
	defer b.mu.RUnlock()

	o, ok := b.resolveOutpostLocked(outpostIdentifier)
	if !ok {
		return nil, notFoundError(resourceOutpost, outpostIdentifier)
	}

	all := b.assetsByOutpost.Get(o.ID)
	out := make([]*Asset, 0, len(all))

	for _, a := range all {
		if matchesAssetFilter(a, f) {
			// Clone before returning: these are the live, backend-owned
			// pointers, and scheduleCapacityTaskCompletion's
			// mergeInstanceTypeCapacity mutates
			// ComputeAttributes.InstanceTypeCapacities on them in place
			// after this call returns and the lock is released -- see
			// clone's doc comment.
			out = append(out, a.clone())
		}
	}

	return out, nil
}

// clone returns a deep copy of a, so the returned Asset shares no mutable
// memory with the backend's stored copy.
func (a *Asset) clone() *Asset {
	cp := *a
	cp.ComputeAttributes = a.ComputeAttributes.clone()

	return &cp
}

// clone returns a deep copy of ca, or nil if ca is nil.
func (ca *ComputeAttributes) clone() *ComputeAttributes {
	if ca == nil {
		return nil
	}

	cp := *ca
	cp.InstanceFamilies = cloneStrs(ca.InstanceFamilies)
	cp.InstanceTypeCapacities = append([]InstanceTypeCapacity(nil), ca.InstanceTypeCapacities...)

	return &cp
}

// assetInstanceFilter holds ListAssetInstances' optional filters.
type assetInstanceFilter struct {
	accountIDs    []string
	assetIDs      []string
	awsServices   []string
	instanceTypes []string
}

func matchesAssetInstanceFilter(ri *runningInstance, f assetInstanceFilter) bool {
	if len(f.accountIDs) > 0 && !containsStr(f.accountIDs, ri.AccountID) {
		return false
	}

	if len(f.assetIDs) > 0 && !containsStr(f.assetIDs, ri.AssetID) {
		return false
	}

	if len(f.awsServices) > 0 && !containsStr(f.awsServices, awsServiceNameEC2) {
		return false
	}

	if len(f.instanceTypes) > 0 && !containsStr(f.instanceTypes, ri.InstanceType) {
		return false
	}

	return true
}

// ListAssetInstances returns the EC2 instances placed on outpostIdentifier's
// assets -- real data recorded by services/ec2's RunInstances via
// ConsumeCapacity (capacity_ledger.go), not a fabricated result. An Outpost
// with no instances launched onto it (or before any ec2 RunInstances call
// has ever consumed its capacity) legitimately returns empty.
func (b *InMemoryBackend) ListAssetInstances(
	outpostIdentifier string, f assetInstanceFilter,
) ([]*runningInstance, error) {
	b.mu.RLock("ListAssetInstances")
	defer b.mu.RUnlock()

	o, ok := b.resolveOutpostLocked(outpostIdentifier)
	if !ok {
		return nil, notFoundError(resourceOutpost, outpostIdentifier)
	}

	all := b.runningInstancesByOutpost.Get(o.ID)
	out := make([]*runningInstance, 0, len(all))

	for _, ri := range all {
		if matchesAssetInstanceFilter(ri, f) {
			cp := *ri
			out = append(out, &cp)
		}
	}

	return out, nil
}

func containsStr(haystack []string, needle string) bool {
	return slices.Contains(haystack, needle)
}
