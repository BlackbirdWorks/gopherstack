package lightsail

// This file backs family U (7 ops: CreateDomain, DeleteDomain, GetDomain,
// GetDomains, CreateDomainEntry, DeleteDomainEntry, UpdateDomainEntry).
// Domain is CONFIRMED a GLOBAL resource (Domain.Arn's own SDK doc-comment
// example, PARITY.md 4.7/5.1) -- its ARN uses globalARN (store.go), not
// regionalARN.

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	opTypeCreateDomain      = "CreateDomain"
	opTypeDeleteDomain      = "DeleteDomain"
	opTypeCreateDomainEntry = "CreateDomainEntry"
	opTypeDeleteDomainEntry = "DeleteDomainEntry"
	opTypeUpdateDomainEntry = "UpdateDomainEntry"
)

// CreateDomain creates a new Domain (a GLOBAL resource).
func (b *InMemoryBackend) CreateDomain(name string, userTags map[string]string) (*Operation, error) {
	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	if err := b.registerNameLocked(ResourceTypeDomain, name); err != nil {
		return nil, err
	}

	d := &Domain{
		Name: name, Arn: b.globalARN(ResourceTypeDomain, newUUID()), SupportCode: newSupportCode(),
		CreatedAt: nowUTC(), Location: ResourceLocation{RegionName: domainGlobalRegion},
		Tags: tags.New("lightsail.domain." + name + ".tags"),
	}
	d.Tags.Merge(userTags)
	b.domains.Put(d)

	ops := b.newOperationsLocked(opTypeCreateDomain, ResourceTypeDomain, []string{name})

	return &ops[0], nil
}

// DeleteDomain deletes the named domain.
func (b *InMemoryBackend) DeleteDomain(name string) (*Operation, error) {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	d, ok := b.domains.Get(name)
	if !ok {
		return nil, notFoundError("Domain", name)
	}

	if d.Tags != nil {
		d.Tags.Close()
	}

	b.domains.Delete(name)
	b.unregisterNameLocked(name)

	ops := b.newOperationsLocked(opTypeDeleteDomain, ResourceTypeDomain, []string{name})

	return &ops[0], nil
}

// GetDomain returns the named domain.
func (b *InMemoryBackend) GetDomain(name string) (*Domain, error) {
	b.mu.RLock("GetDomain")
	defer b.mu.RUnlock()

	d, ok := b.domains.Get(name)
	if !ok {
		return nil, notFoundError("Domain", name)
	}

	return d.clone(), nil
}

// GetDomains returns every domain, paginated.
func (b *InMemoryBackend) GetDomains(token string) (page.Page[*Domain], error) {
	b.mu.RLock("GetDomains")
	defer b.mu.RUnlock()

	all := b.domains.All()
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	out := make([]*Domain, len(all))
	for i, v := range all {
		out[i] = v.clone()
	}

	return paginateGeneric(out, token)
}

// CreateDomainEntry adds a DNS record entry to the named domain.
func (b *InMemoryBackend) CreateDomainEntry(domainName string, entry DomainEntry) (*Operation, error) {
	b.mu.Lock("CreateDomainEntry")
	defer b.mu.Unlock()

	d, ok := b.domains.Get(domainName)
	if !ok {
		return nil, notFoundError("Domain", domainName)
	}

	entry.ID = newUUID()
	d.Entries = append(d.Entries, entry)

	ops := b.newOperationsLocked(opTypeCreateDomainEntry, ResourceTypeDomain, []string{domainName})

	return &ops[0], nil
}

// DeleteDomainEntry removes the DNS record entry matching entry's
// Name/Type/Target from the named domain.
func (b *InMemoryBackend) DeleteDomainEntry(domainName string, entry DomainEntry) (*Operation, error) {
	b.mu.Lock("DeleteDomainEntry")
	defer b.mu.Unlock()

	d, ok := b.domains.Get(domainName)
	if !ok {
		return nil, notFoundError("Domain", domainName)
	}

	out := make([]DomainEntry, 0, len(d.Entries))

	for _, e := range d.Entries {
		if e.ID == entry.ID || (e.Name == entry.Name && e.Type == entry.Type && e.Target == entry.Target) {
			continue
		}

		out = append(out, e)
	}

	d.Entries = out

	ops := b.newOperationsLocked(opTypeDeleteDomainEntry, ResourceTypeDomain, []string{domainName})

	return &ops[0], nil
}

// UpdateDomainEntry updates (or, if unmatched, appends) the DNS record
// entry with entry's Id on the named domain.
func (b *InMemoryBackend) UpdateDomainEntry(domainName string, entry DomainEntry) ([]Operation, error) {
	b.mu.Lock("UpdateDomainEntry")
	defer b.mu.Unlock()

	d, ok := b.domains.Get(domainName)
	if !ok {
		return nil, notFoundError("Domain", domainName)
	}

	found := false

	for i, e := range d.Entries {
		if e.ID == entry.ID {
			if entry.ID == "" {
				entry.ID = newUUID()
			}

			d.Entries[i] = entry
			found = true

			break
		}
	}

	if !found {
		if entry.ID == "" {
			entry.ID = newUUID()
		}

		d.Entries = append(d.Entries, entry)
	}

	return b.newOperationsLocked(opTypeUpdateDomainEntry, ResourceTypeDomain, []string{domainName}), nil
}
