package codeartifact

import (
	"context"
	"fmt"
	"slices"
	"sort"
)

// --- Package methods ---

// packageKey returns the map key for a package.
func packageKey(domainName, repoName, format, namespace, name string) string {
	return domainName + "/" + repoName + "/" + format + "/" + namespace + "/" + name
}

// DescribePackage returns a package by domain, repository, format, namespace, and name.
// If the package does not already exist in the store, a stub entry is created on the fly so
// that callers (e.g. Terraform providers) can always retrieve metadata about packages that
// were published directly to the repository.
func (b *InMemoryBackend) DescribePackage(
	ctx context.Context, domainName, repoName, format, namespace, name string,
) (*Package, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DescribePackage")
	defer b.mu.Unlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, repoName))) {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	key := packageKey(domainName, repoName, format, namespace, name)
	pkg, ok := b.packages.Get(regionKey(region, key))
	if !ok {
		// Auto-create a stub package entry.
		pkg = &Package{
			DomainName:  domainName,
			DomainOwner: b.accountID,
			Repository:  repoName,
			Format:      format,
			Namespace:   namespace,
			Name:        name,
			region:      region,
		}
		b.packages.Put(pkg)
	}
	cp := *pkg

	return &cp, nil
}

// DeletePackage deletes a package and all its versions from a repository.
func (b *InMemoryBackend) DeletePackage(
	ctx context.Context, domainName, repoName, format, namespace, name string,
) (*Package, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePackage")
	defer b.mu.Unlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, repoName))) {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	key := packageKey(domainName, repoName, format, namespace, name)
	pkg, ok := b.packages.Get(regionKey(region, key))
	if !ok {
		return nil, fmt.Errorf("%w: package %s not found", ErrNotFound, name)
	}
	cp := *pkg
	b.packages.Delete(regionKey(region, key))

	// Remove all associated package versions.
	for _, pv := range slices.Clone(b.packageVersionsByRegion.Get(region)) {
		if pv.DomainName == domainName && pv.Repository == repoName && pv.Format == format &&
			pv.Namespace == namespace && pv.PackageName == name {
			b.packageVersions.Delete(regionKey(region, packageVersionKey(
				pv.DomainName, pv.Repository, pv.Format, pv.Namespace, pv.PackageName, pv.Version,
			)))
		}
	}

	return &cp, nil
}

// ListPackages lists packages in a repository.
func (b *InMemoryBackend) ListPackages(
	ctx context.Context, domainName, repoName, format, namespace string,
) ([]*Package, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPackages")
	defer b.mu.RUnlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, repoName))) {
		return nil, fmt.Errorf("%w: repository %s/%s not found", ErrNotFound, domainName, repoName)
	}

	result := make([]*Package, 0)

	for _, pv := range b.packageVersionsByRegion.Get(region) {
		if pv.DomainName != domainName || pv.Repository != repoName {
			continue
		}

		if format != "" && pv.Format != format {
			continue
		}

		if namespace != "" && pv.Namespace != namespace {
			continue
		}

		// Deduplicate by package name.
		found := false

		for _, existing := range result {
			if existing.Name == pv.PackageName && existing.Format == pv.Format && existing.Namespace == pv.Namespace {
				found = true

				break
			}
		}

		if !found {
			result = append(result, &Package{
				DomainName:  domainName,
				DomainOwner: b.accountID,
				Repository:  repoName,
				Format:      pv.Format,
				Namespace:   pv.Namespace,
				Name:        pv.PackageName,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// PutPackageOriginConfiguration sets a package's publish/upstream origin restrictions
// and persists the (possibly newly created) package record. publish/upstream default to
// "ALLOW" when the caller passes an empty string, matching an unconfigured package.
func (b *InMemoryBackend) PutPackageOriginConfiguration(
	ctx context.Context,
	domainName, repoName, format, namespace, name, publish, upstream string,
) (*Package, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("PutPackageOriginConfiguration")
	defer b.mu.Unlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, repoName))) {
		return nil, fmt.Errorf("%w: repository %s/%s not found", ErrNotFound, domainName, repoName)
	}

	if publish == "" {
		publish = "ALLOW"
	}
	if upstream == "" {
		upstream = "ALLOW"
	}

	key := packageKey(domainName, repoName, format, namespace, name)
	pkg, ok := b.packages.Get(regionKey(region, key))
	if !ok {
		pkg = &Package{
			DomainName:  domainName,
			DomainOwner: b.accountID,
			Repository:  repoName,
			Format:      format,
			Namespace:   namespace,
			Name:        name,
			region:      region,
		}
	}
	pkg.OriginConfigPublish = publish
	pkg.OriginConfigUpstream = upstream
	b.packages.Put(pkg)

	cp := *pkg

	return &cp, nil
}
