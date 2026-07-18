package ec2

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// CreateManagedPrefixList creates a new managed prefix list.
func (b *InMemoryBackend) CreateManagedPrefixList(
	name, addressFamily string,
	maxEntries int,
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
	}
	b.managedPrefixLists.Put(pl)

	return pl, nil
}

// DeleteManagedPrefixList removes a managed prefix list.
func (b *InMemoryBackend) DeleteManagedPrefixList(id string) error {
	if id == "" {
		return fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteManagedPrefixList")
	defer b.mu.Unlock()

	if _, ok := b.managedPrefixLists.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}
	b.managedPrefixLists.Delete(id)

	return nil
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
		cp := *pl
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PrefixListID < out[j].PrefixListID })

	return out
}

// GetManagedPrefixListEntries returns the entries for a prefix list.
func (b *InMemoryBackend) GetManagedPrefixListEntries(id string) ([]PrefixListEntry, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetManagedPrefixListEntries")
	defer b.mu.RUnlock()

	pl, ok := b.managedPrefixLists.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}

	out := make([]PrefixListEntry, len(pl.Entries))
	copy(out, pl.Entries)

	return out, nil
}

// ModifyManagedPrefixList modifies a managed prefix list.
func (b *InMemoryBackend) ModifyManagedPrefixList(
	id string,
	addEntries, removeEntries []PrefixListEntry,
) error {
	if id == "" {
		return fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyManagedPrefixList")
	defer b.mu.Unlock()

	pl, ok := b.managedPrefixLists.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
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

	return nil
}

// RestoreManagedPrefixListVersion restores a previous version of a prefix list.
func (b *InMemoryBackend) RestoreManagedPrefixListVersion(id string, version int64) error {
	if id == "" {
		return fmt.Errorf("%w: PrefixListId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RestoreManagedPrefixListVersion")
	defer b.mu.Unlock()

	pl, ok := b.managedPrefixLists.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrManagedPrefixListNotFound, id)
	}
	pl.Version = version
	pl.State = "restore-complete"

	return nil
}

// ---- ClientVpnEndpoint ----

// ClientVpnEndpointOptions holds the optional advanced Client VPN endpoint
// fields available via CreateClientVpnEndpointWithOptions and
// ModifyClientVpnEndpointWithOptions.
type ClientVpnEndpointOptions struct {
	SplitTunnel          *bool
	ServerCertificateArn string
	TransportProtocol    string
	VpcID                string
	SelfServicePortalURL string
	SecurityGroupIDs     []string
	VpnPort              int32
	SessionTimeoutHours  int32
}
