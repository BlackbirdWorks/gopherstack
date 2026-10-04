package ec2

import (
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
)

// CreateManagedPrefixList creates a new managed prefix list.
func (b *InMemoryBackend) CreateManagedPrefixList(
	name, addressFamily string,
	maxEntries int,
	entries []PrefixListEntry,
) (*ManagedPrefixList, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: PrefixListName is required", ErrInvalidParameter)
	}
	if addressFamily == "" {
		addressFamily = "IPv4"
	}

	b.mu.Lock("CreateManagedPrefixList")
	defer b.mu.Unlock()

	id := "pl-" + uuid.New().String()[:8]
	pl := &ManagedPrefixList{
		PrefixListID:   id,
		PrefixListName: name,
		PrefixListArn:  "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":prefix-list/" + id,
		AddressFamily:  addressFamily,
		State:          "create-complete",
		MaxEntries:     maxEntries,
		Version:        1,
		OwnerID:        b.AccountID,
		Entries:        slices.Clone(entries),
	}
	pl.recordVersion()
	b.managedPrefixLists.Put(pl)

	return copyManagedPrefixList(pl), nil
}

// prefixListMaxVersions bounds how many versions of entries are retained.
const prefixListMaxVersions = 100

// recordVersion snapshots the current entries under the current version,
// dropping the oldest versions beyond prefixListMaxVersions.
func (pl *ManagedPrefixList) recordVersion() {
	if pl.VersionEntries == nil {
		pl.VersionEntries = make(map[int64][]PrefixListEntry)
	}

	pl.VersionEntries[pl.Version] = slices.Clone(pl.Entries)

	for len(pl.VersionEntries) > prefixListMaxVersions {
		oldest := pl.Version

		for v := range pl.VersionEntries {
			oldest = min(oldest, v)
		}

		delete(pl.VersionEntries, oldest)
	}
}

// copyManagedPrefixList returns a copy safe to hand out of the lock; the
// version history stays internal.
func copyManagedPrefixList(pl *ManagedPrefixList) *ManagedPrefixList {
	cp := *pl
	cp.Entries = slices.Clone(pl.Entries)
	cp.VersionEntries = nil

	return &cp
}

// DeleteManagedPrefixList removes a managed prefix list.
func (b *InMemoryBackend) DeleteManagedPrefixList(id string) (*ManagedPrefixList, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteManagedPrefixList")
	defer b.mu.Unlock()

	pl, ok := b.managedPrefixLists.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}
	cp := *pl
	cp.State = "delete-complete"
	b.managedPrefixLists.Delete(id)
	delete(b.tags, id)

	return &cp, nil
}

// DescribeManagedPrefixLists returns managed prefix lists, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeManagedPrefixLists(ids []string) []*ManagedPrefixList {
	b.mu.RLock("DescribeManagedPrefixLists")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*ManagedPrefixList
	for _, pl := range b.managedPrefixLists.All() {
		if len(filter) > 0 && !filter[pl.PrefixListID] {
			continue
		}
		out = append(out, copyManagedPrefixList(pl))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PrefixListID < out[j].PrefixListID })

	return out
}

// GetManagedPrefixListEntries returns the entries for a prefix list.
func (b *InMemoryBackend) GetManagedPrefixListEntries(id string) ([]PrefixListEntry, error) {
	return b.GetManagedPrefixListEntriesAt(id, 0)
}

// GetManagedPrefixListEntriesAt returns the entries of a retained version
// (GetManagedPrefixListEntries.TargetVersion); 0 means the current version.
func (b *InMemoryBackend) GetManagedPrefixListEntriesAt(id string, targetVersion int64) ([]PrefixListEntry, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetManagedPrefixListEntries")
	defer b.mu.RUnlock()

	pl, ok := b.managedPrefixLists.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}

	if targetVersion == 0 || targetVersion == pl.Version {
		return slices.Clone(pl.Entries), nil
	}

	entries, found := pl.VersionEntries[targetVersion]
	if !found {
		return nil, fmt.Errorf("%w: prefix list %s has no version %d", ErrInvalidParameter, id, targetVersion)
	}

	return slices.Clone(entries), nil
}

// ModifyManagedPrefixList modifies a managed prefix list.
func (b *InMemoryBackend) ModifyManagedPrefixList(
	id string,
	addEntries, removeEntries []PrefixListEntry,
) (*ManagedPrefixList, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyManagedPrefixList")
	defer b.mu.Unlock()

	pl, ok := b.managedPrefixLists.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}

	// Remove entries
	if len(removeEntries) > 0 {
		removeCIDRs := make(map[string]bool, len(removeEntries))
		for _, e := range removeEntries {
			removeCIDRs[e.Cidr] = true
		}
		var kept []PrefixListEntry
		for _, e := range pl.Entries {
			if !removeCIDRs[e.Cidr] {
				kept = append(kept, e)
			}
		}
		pl.Entries = kept
	}

	// Add entries
	pl.Entries = append(pl.Entries, addEntries...)
	pl.Version++
	pl.State = "modify-complete"
	pl.recordVersion()

	return copyManagedPrefixList(pl), nil
}

// RestoreManagedPrefixListVersion restores a previous version's entries as a new version.
func (b *InMemoryBackend) RestoreManagedPrefixListVersion(id string, version int64) (*ManagedPrefixList, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreManagedPrefixListVersion")
	defer b.mu.Unlock()

	pl, ok := b.managedPrefixLists.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}

	entries, found := pl.VersionEntries[version]
	if !found {
		return nil, fmt.Errorf("%w: prefix list %s has no version %d", ErrInvalidParameter, id, version)
	}

	pl.Entries = slices.Clone(entries)
	pl.Version++
	pl.State = "restore-complete"
	pl.recordVersion()

	return copyManagedPrefixList(pl), nil
}

// ---- ClientVpnEndpoint ----

// ClientVpnEndpointOptions holds the optional advanced Client VPN endpoint
// fields available via CreateClientVpnEndpointWithOptions and
// ModifyClientVpnEndpointWithOptions.
type ClientVpnEndpointOptions struct {
	SplitTunnel *bool
	// DisconnectOnSessionTimeout is a *bool tri-state matching
	// SplitTunnel's shape: nil means "not specified" (defaults to true, the
	// documented default), so an explicit false can still be told apart
	// from omission.
	DisconnectOnSessionTimeout *bool
	ServerCertificateArn       string
	TransportProtocol          string
	VpcID                      string
	SelfServicePortalURL       string
	// TransitGatewayID associates the endpoint with a Transit Gateway instead
	// of a VPC (TransitGatewayConfiguration.TransitGatewayId on the wire).
	// When set, CreateClientVpnEndpointWithOptions creates a pending
	// TransitGatewayClientVpnAttachment for it.
	TransitGatewayID      string
	EndpointIPAddressType string
	TrafficIPAddressType  string
	SecurityGroupIDs      []string
	VpnPort               int32
	SessionTimeoutHours   int32
}
