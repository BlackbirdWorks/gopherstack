package codeartifact

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateDomain creates a new CodeArtifact domain.
func (b *InMemoryBackend) CreateDomain(
	ctx context.Context, name, encryptionKey string, kv map[string]string,
) (*Domain, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateDomain")
	defer b.mu.Unlock()

	if b.domains.Has(regionKey(region, name)) {
		return nil, fmt.Errorf("%w: domain %s already exists", ErrAlreadyExists, name)
	}

	domainARN := arn.Build("codeartifact", region, b.accountID, "domain/"+name)
	t := tags.New("codeartifact.domain." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	d := &Domain{
		Name:          name,
		ARN:           domainARN,
		EncryptionKey: encryptionKey,
		Owner:         b.accountID,
		Region:        region,
		Status:        "Active",
		S3BucketARN:   "arn:aws:s3:::assets-" + uuid.NewString()[:8],
		CreatedTime:   time.Now().UTC(),
		Tags:          t,
	}
	b.domains.Put(d)
	cp := *d

	return &cp, nil
}

// DescribeDomain returns a domain by name.
func (b *InMemoryBackend) DescribeDomain(ctx context.Context, name string) (*Domain, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	d, ok := b.domains.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, name)
	}
	cp := *d

	return &cp, nil
}

// ListDomains returns all domains sorted by name.
func (b *InMemoryBackend) ListDomains(ctx context.Context) []*Domain {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListDomains")
	defer b.mu.RUnlock()

	entries := b.domainsByRegion.Get(region)
	list := make([]*Domain, 0, len(entries))
	for _, d := range entries {
		cp := *d
		list = append(list, &cp)
	}
	slices.SortFunc(list, func(a, b *Domain) int {
		return strings.Compare(a.Name, b.Name)
	})

	return list
}

// DeleteDomain deletes a domain by name, cascade-deleting all its repositories,
// packages, package versions, external connections, policies, and Tags.
func (b *InMemoryBackend) DeleteDomain(ctx context.Context, name string) (*Domain, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	d, ok := b.domains.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, name)
	}
	cp := *d

	repos := slices.Clone(b.repositoriesByRegion.Get(region))
	pkgs := slices.Clone(b.packagesByRegion.Get(region))
	pvs := slices.Clone(b.packageVersionsByRegion.Get(region))
	externalConnections := b.externalConnectionsStore(region)

	// Cascade: delete all repositories in this domain plus their dependents.
	for _, r := range repos {
		if r.DomainName != name {
			continue
		}

		for _, p := range pkgs {
			if p.DomainName == r.DomainName && p.Repository == r.Name {
				b.packages.Delete(regionKey(region, packageKey(
					p.DomainName, p.Repository, p.Format, p.Namespace, p.Name,
				)))
			}
		}
		for _, pv := range pvs {
			if pv.DomainName == r.DomainName && pv.Repository == r.Name {
				b.packageVersions.Delete(regionKey(region, packageVersionKey(
					pv.DomainName, pv.Repository, pv.Format, pv.Namespace, pv.PackageName, pv.Version,
				)))
			}
		}

		key := repoKey(r.DomainName, r.Name)
		delete(externalConnections, key)
		b.repositoryPolicies.Delete(regionKey(region, key))
		r.Tags.Close()
		b.repositories.Delete(regionKey(region, key))
	}

	b.domainPolicies.Delete(regionKey(region, name))
	b.domains.Delete(regionKey(region, name))
	d.Tags.Close()

	return &cp, nil
}

// --- Domain permissions policy methods ---

// GetDomainPermissionsPolicy retrieves the permissions policy for a domain.
// Returns ErrNotFound if the domain does not exist or if no policy has been set.
func (b *InMemoryBackend) GetDomainPermissionsPolicy(
	ctx context.Context, domainName string,
) (*DomainPermissionsPolicy, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetDomainPermissionsPolicy")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	pol, ok := b.domainPolicies.Get(regionKey(region, domainName))
	if !ok {
		return nil, fmt.Errorf("%w: no permissions policy found for domain %s", ErrNotFound, domainName)
	}
	cp := *pol

	return &cp, nil
}

// PutDomainPermissionsPolicy stores a permissions policy for a domain.
func (b *InMemoryBackend) PutDomainPermissionsPolicy(
	ctx context.Context, domainName, document string,
) (*DomainPermissionsPolicy, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("PutDomainPermissionsPolicy")
	defer b.mu.Unlock()

	d, ok := b.domains.Get(regionKey(region, domainName))
	if !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	pol := &DomainPermissionsPolicy{
		Document:    document,
		Revision:    uuid.NewString()[:8],
		ResourceARN: d.ARN,
		region:      region,
		domainName:  domainName,
	}
	b.domainPolicies.Put(pol)
	cp := *pol

	return &cp, nil
}

// DeleteDomainPermissionsPolicy removes the permissions policy from a domain.
// Returns ErrNotFound if the domain does not exist or if no policy has been set.
func (b *InMemoryBackend) DeleteDomainPermissionsPolicy(
	ctx context.Context, domainName string,
) (*DomainPermissionsPolicy, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteDomainPermissionsPolicy")
	defer b.mu.Unlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	pol, ok := b.domainPolicies.Get(regionKey(region, domainName))
	if !ok {
		return nil, fmt.Errorf("%w: no permissions policy found for domain %s", ErrNotFound, domainName)
	}
	cp := *pol
	b.domainPolicies.Delete(regionKey(region, domainName))

	return &cp, nil
}
