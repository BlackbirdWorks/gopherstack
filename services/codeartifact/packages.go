package codeartifact

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// originConfigOrDefault returns v, or "ALLOW" if v is unset -- matching
// PackageOriginRestrictions's real default before PutPackageOriginConfiguration
// is ever called (see Package.OriginConfigPublish's doc comment).
func originConfigOrDefault(v string) string {
	if v == "" {
		return "ALLOW"
	}

	return v
}

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

// listPackagesFilters holds every query-bound ListPackagesInput filter
// (serializers.go's awsRestjson1_serializeOpHttpBindingsListPackagesInput):
// format/namespace/packagePrefix narrow which PackageVersions count as a
// distinct package, publish/upstream narrow by the package's own
// PackageOriginConfiguration.
type listPackagesFilters struct {
	format        string
	namespace     string
	packagePrefix string
	publish       string
	upstream      string
}

func (f listPackagesFilters) matchesVersion(pv *PackageVersion) bool {
	if f.format != "" && pv.Format != f.format {
		return false
	}
	if f.namespace != "" && pv.Namespace != f.namespace {
		return false
	}

	return f.packagePrefix == "" || strings.HasPrefix(pv.PackageName, f.packagePrefix)
}

// matchesOrigin checks publish/upstream against pkg's origin configuration.
// PackageOriginConfiguration defaults to ALLOW/ALLOW until explicitly set
// (see Package.OriginConfigPublish's doc comment), so an empty stored value
// still needs to match a "publish"/"upstream" filter of ALLOW.
func (f listPackagesFilters) matchesOrigin(pkg *Package) bool {
	if f.publish != "" && originConfigOrDefault(pkg.OriginConfigPublish) != f.publish {
		return false
	}

	return f.upstream == "" || originConfigOrDefault(pkg.OriginConfigUpstream) == f.upstream
}

func containsPackage(packages []*Package, pv *PackageVersion) bool {
	for _, existing := range packages {
		if existing.Name == pv.PackageName && existing.Format == pv.Format && existing.Namespace == pv.Namespace {
			return true
		}
	}

	return false
}

// packageWithOrigin builds a Package summary for pv, filling in its real
// stored origin configuration (if PutPackageOriginConfiguration was ever
// called for it) rather than leaving OriginConfigPublish/Upstream blank.
func (b *InMemoryBackend) packageWithOrigin(region, domainName, repoName string, pv *PackageVersion) *Package {
	pkg := &Package{
		DomainName:  domainName,
		DomainOwner: b.accountID,
		Repository:  repoName,
		Format:      pv.Format,
		Namespace:   pv.Namespace,
		Name:        pv.PackageName,
	}

	key := regionKey(region, packageKey(domainName, repoName, pv.Format, pv.Namespace, pv.PackageName))
	if stored, ok := b.packages.Get(key); ok {
		pkg.OriginConfigPublish = stored.OriginConfigPublish
		pkg.OriginConfigUpstream = stored.OriginConfigUpstream
	}

	return pkg
}

// ListPackages lists packages in a repository, applying every
// listPackagesFilters entry.
func (b *InMemoryBackend) ListPackages(
	ctx context.Context, domainName, repoName, format, namespace, packagePrefix, publish, upstream string,
) ([]*Package, error) {
	region := getRegion(ctx, b.region)
	filters := listPackagesFilters{
		format: format, namespace: namespace, packagePrefix: packagePrefix, publish: publish, upstream: upstream,
	}

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
		if !filters.matchesVersion(pv) || containsPackage(result, pv) {
			continue
		}

		pkg := b.packageWithOrigin(region, domainName, repoName, pv)
		if !filters.matchesOrigin(pkg) {
			continue
		}

		result = append(result, pkg)
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

	// AllowPublish/AllowUpstream (types.PackageOriginRestrictions' enums) share
	// the same "ALLOW" string value as PackageGroupOriginRestrictionMode, so
	// restrictionModeAllow (declared in package_groups.go) is reused here too.
	if publish == "" {
		publish = restrictionModeAllow
	}
	if upstream == "" {
		upstream = restrictionModeAllow
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
