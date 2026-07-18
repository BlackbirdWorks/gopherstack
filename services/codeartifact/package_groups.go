package codeartifact

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// --- Package group methods ---

// packageGroupKey returns the map key for a package group.
func packageGroupKey(domainName, pattern string) string {
	return domainName + "/" + pattern
}

// CreatePackageGroup creates a new CodeArtifact package group.
func (b *InMemoryBackend) CreatePackageGroup(
	ctx context.Context,
	domainName, pattern, description, contactInfo string,
	kv map[string]string,
) (*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreatePackageGroup")
	defer b.mu.Unlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	key := packageGroupKey(domainName, pattern)
	if b.packageGroups.Has(regionKey(region, key)) {
		return nil, fmt.Errorf(
			"%w: package group %s already exists in domain %s",
			ErrAlreadyExists,
			pattern,
			domainName,
		)
	}

	pgARN := arn.Build("codeartifact", region, b.accountID, "package-group/"+domainName+pattern)
	t := tags.New("codeartifact.package-group." + key + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	pg := &PackageGroup{
		ARN:         pgARN,
		DomainName:  domainName,
		DomainOwner: b.accountID,
		Pattern:     pattern,
		Description: description,
		ContactInfo: contactInfo,
		CreatedTime: time.Now().UTC(),
		Tags:        t,
		region:      region,
	}
	b.packageGroups.Put(pg)
	cp := *pg

	return &cp, nil
}

// DescribePackageGroup returns a package group by domain and pattern.
func (b *InMemoryBackend) DescribePackageGroup(ctx context.Context, domainName, pattern string) (*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribePackageGroup")
	defer b.mu.RUnlock()

	pg, ok := b.packageGroups.Get(regionKey(region, packageGroupKey(domainName, pattern)))
	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found in domain %s", ErrNotFound, pattern, domainName)
	}
	cp := *pg

	return &cp, nil
}

// DeletePackageGroup deletes a package group by domain and pattern.
func (b *InMemoryBackend) DeletePackageGroup(ctx context.Context, domainName, pattern string) (*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePackageGroup")
	defer b.mu.Unlock()

	key := packageGroupKey(domainName, pattern)
	pg, ok := b.packageGroups.Get(regionKey(region, key))
	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found in domain %s", ErrNotFound, pattern, domainName)
	}
	cp := *pg
	b.packageGroups.Delete(regionKey(region, key))
	pg.Tags.Close()

	return &cp, nil
}

// GetAssociatedPackageGroup returns the most specific package group associated with a package.
func (b *InMemoryBackend) GetAssociatedPackageGroup(
	ctx context.Context,
	domainName, format, namespace, name string,
) (*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetAssociatedPackageGroup")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	_ = format
	_ = namespace
	_ = name

	// Return nil if no group matches (not an error per AWS API).
	return nil, nil //nolint:nilnil // AWS returns no error when no group matches
}

// ListPackageGroups returns all package groups in a domain, optionally filtered by prefix.
func (b *InMemoryBackend) ListPackageGroups(ctx context.Context, domainName, prefix string) ([]*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPackageGroups")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	entries := b.packageGroupsByRegion.Get(region)
	result := make([]*PackageGroup, 0, len(entries))

	for _, pg := range entries {
		if pg.DomainName != domainName {
			continue
		}

		if prefix != "" && !strings.HasPrefix(pg.Pattern, prefix) {
			continue
		}

		cp := *pg
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Pattern < result[j].Pattern
	})

	return result, nil
}

// ListSubPackageGroups returns sub-package groups of a given package group pattern.
func (b *InMemoryBackend) ListSubPackageGroups(
	ctx context.Context,
	domainName, pattern string,
) ([]*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListSubPackageGroups")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	entries := b.packageGroupsByRegion.Get(region)
	result := make([]*PackageGroup, 0, len(entries))

	parentRoot := strings.TrimSuffix(pattern, "*")

	for _, pg := range entries {
		if pg.DomainName != domainName {
			continue
		}

		if pg.Pattern == pattern || !strings.HasPrefix(pg.Pattern, parentRoot) {
			continue
		}

		cp := *pg
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Pattern < result[j].Pattern
	})

	return result, nil
}

// UpdatePackageGroup updates description or contact info of a package group.
func (b *InMemoryBackend) UpdatePackageGroup(
	ctx context.Context,
	domainName, pattern, description, contactInfo string,
) (*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdatePackageGroup")
	defer b.mu.Unlock()

	key := packageGroupKey(domainName, pattern)
	pg, ok := b.packageGroups.Get(regionKey(region, key))

	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found", ErrNotFound, pattern)
	}

	if description != "" {
		pg.Description = description
	}

	if contactInfo != "" {
		pg.ContactInfo = contactInfo
	}

	cp := *pg

	return &cp, nil
}

// UpdatePackageGroupOriginConfiguration is a stub that accepts origin config changes.
func (b *InMemoryBackend) UpdatePackageGroupOriginConfiguration(
	ctx context.Context,
	domainName, pattern string,
) (*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("UpdatePackageGroupOriginConfiguration")
	defer b.mu.RUnlock()

	key := packageGroupKey(domainName, pattern)
	pg, ok := b.packageGroups.Get(regionKey(region, key))

	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found", ErrNotFound, pattern)
	}

	cp := *pg

	return &cp, nil
}

// ListAllowedRepositoriesForGroup is a stub returning allowed repositories for a package group.
func (b *InMemoryBackend) ListAllowedRepositoriesForGroup(
	ctx context.Context,
	domainName, pattern string,
) ([]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAllowedRepositoriesForGroup")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	_ = pattern

	return []string{}, nil
}

// ListAssociatedPackages lists packages associated with a package group.
func (b *InMemoryBackend) ListAssociatedPackages(ctx context.Context, domainName, pattern string) ([]*Package, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAssociatedPackages")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	_ = pattern

	return []*Package{}, nil
}
