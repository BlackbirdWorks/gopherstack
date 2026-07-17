package codeartifact

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// --- Package version methods ---

// packageVersionKey returns the map key for a package version.
func packageVersionKey(domainName, repoName, format, namespace, name, version string) string {
	return packageKey(domainName, repoName, format, namespace, name) + "/" + version
}

// DescribePackageVersion returns a specific version of a package.
// As with DescribePackage, stub entries are created on demand.
func (b *InMemoryBackend) DescribePackageVersion(
	ctx context.Context,
	domainName, repoName, format, namespace, name, version string,
) (*PackageVersion, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DescribePackageVersion")
	defer b.mu.Unlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, repoName))) {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	vKey := packageVersionKey(domainName, repoName, format, namespace, name, version)
	pv, ok := b.packageVersions.Get(regionKey(region, vKey))
	if !ok {
		// Auto-create a stub version entry.
		pv = &PackageVersion{
			DomainName:  domainName,
			Repository:  repoName,
			Format:      format,
			Namespace:   namespace,
			PackageName: name,
			Version:     version,
			Status:      "Published",
			PublishedAt: time.Now().UTC(),
			Revision:    uuid.NewString()[:8],
			region:      region,
		}
		b.packageVersions.Put(pv)

		// Ensure the parent package record exists too.
		pKey := packageKey(domainName, repoName, format, namespace, name)
		if !b.packages.Has(regionKey(region, pKey)) {
			b.packages.Put(&Package{
				DomainName:  domainName,
				DomainOwner: b.accountID,
				Repository:  repoName,
				Format:      format,
				Namespace:   namespace,
				Name:        name,
				region:      region,
			})
		}
	}
	cp := *pv

	return &cp, nil
}

// DeletePackageVersions deletes specified versions of a package and returns a
// map of version→errorCode for any versions that could not be deleted.
func (b *InMemoryBackend) DeletePackageVersions(
	ctx context.Context,
	domainName, repoName, format, namespace, name string,
	versions []string,
) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePackageVersions")
	defer b.mu.Unlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, repoName))) {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	failed := make(map[string]string)
	for _, v := range versions {
		vKey := packageVersionKey(domainName, repoName, format, namespace, name, v)
		if !b.packageVersions.Has(regionKey(region, vKey)) {
			failed[v] = "RESOURCE_NOT_FOUND"

			continue
		}
		b.packageVersions.Delete(regionKey(region, vKey))
	}

	return failed, nil
}

// CopyPackageVersions copies specified package versions from a source repository
// to a destination repository in the same domain.
func (b *InMemoryBackend) CopyPackageVersions(
	ctx context.Context,
	domainName, srcRepo, dstRepo, format, namespace, name string,
	versions []string,
) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CopyPackageVersions")
	defer b.mu.Unlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, srcRepo))) {
		return nil, fmt.Errorf("%w: source repository %s not found in domain %s", ErrNotFound, srcRepo, domainName)
	}
	if !b.repositories.Has(regionKey(region, repoKey(domainName, dstRepo))) {
		return nil, fmt.Errorf("%w: destination repository %s not found in domain %s", ErrNotFound, dstRepo, domainName)
	}

	failed := make(map[string]string)
	for _, v := range versions {
		srcKey := packageVersionKey(domainName, srcRepo, format, namespace, name, v)
		src, ok := b.packageVersions.Get(regionKey(region, srcKey))
		if !ok {
			failed[v] = "RESOURCE_NOT_FOUND"

			continue
		}
		dstKey := packageVersionKey(domainName, dstRepo, format, namespace, name, v)
		if b.packageVersions.Has(regionKey(region, dstKey)) {
			failed[v] = "ALREADY_EXISTS"

			continue
		}
		copied := *src
		copied.Repository = dstRepo
		copied.region = region
		b.packageVersions.Put(&copied)
		// Ensure destination package record exists.
		dstPkgKey := packageKey(domainName, dstRepo, format, namespace, name)
		if !b.packages.Has(regionKey(region, dstPkgKey)) {
			b.packages.Put(&Package{
				DomainName:  domainName,
				DomainOwner: b.accountID,
				Repository:  dstRepo,
				Format:      format,
				Namespace:   namespace,
				Name:        name,
				region:      region,
			})
		}
	}

	return failed, nil
}

// DisposePackageVersions moves specified versions of a package to the Disposed status.
func (b *InMemoryBackend) DisposePackageVersions(
	ctx context.Context,
	domainName, repoName, format, namespace, name string,
	versions []string,
) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DisposePackageVersions")
	defer b.mu.Unlock()

	results := make(map[string]string, len(versions))

	for _, v := range versions {
		key := packageVersionKey(domainName, repoName, format, namespace, name, v)
		if pv, ok := b.packageVersions.Get(regionKey(region, key)); ok {
			pv.Status = "Disposed"
			results[v] = "SUCCESS"
		} else {
			results[v] = "NOT_FOUND"
		}
	}

	return results, nil
}

// ListPackageVersions lists all versions of a package in a repository.
func (b *InMemoryBackend) ListPackageVersions(
	ctx context.Context,
	domainName, repoName, format, namespace, name string,
) ([]*PackageVersion, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPackageVersions")
	defer b.mu.RUnlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, repoName))) {
		return nil, fmt.Errorf("%w: repository %s/%s not found", ErrNotFound, domainName, repoName)
	}

	entries := b.packageVersionsByRegion.Get(region)
	result := make([]*PackageVersion, 0, len(entries))

	for _, pv := range entries {
		if pv.DomainName != domainName || pv.Repository != repoName {
			continue
		}

		if format != "" && pv.Format != format {
			continue
		}

		if namespace != "" && pv.Namespace != namespace {
			continue
		}

		if name != "" && pv.PackageName != name {
			continue
		}

		cp := *pv
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Version < result[j].Version
	})

	return result, nil
}

// ListPackageVersionAssets lists the assets actually uploaded to a package version via
// PublishPackageVersion.
func (b *InMemoryBackend) ListPackageVersionAssets(
	ctx context.Context,
	domainName, repoName, format, namespace, name, version string,
) ([]AssetInfo, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPackageVersionAssets")
	defer b.mu.RUnlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)
	pv, ok := b.packageVersions.Get(regionKey(region, key))
	if !ok {
		return nil, fmt.Errorf("%w: package version not found", ErrNotFound)
	}

	return slices.Clone(pv.Assets), nil
}

// ListPackageVersionDependencies is a stub returning empty dependencies.
func (b *InMemoryBackend) ListPackageVersionDependencies(
	ctx context.Context,
	domainName, repoName, format, namespace, name, version string,
) ([]map[string]any, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPackageVersionDependencies")
	defer b.mu.RUnlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)
	if !b.packageVersions.Has(regionKey(region, key)) {
		return nil, fmt.Errorf("%w: package version not found", ErrNotFound)
	}

	return []map[string]any{}, nil
}

// GetPackageVersionAsset returns the content of an asset previously uploaded via
// PublishPackageVersion.
func (b *InMemoryBackend) GetPackageVersionAsset(
	ctx context.Context,
	domainName, repoName, format, namespace, name, version, asset string,
) ([]byte, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetPackageVersionAsset")
	defer b.mu.RUnlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)
	pv, ok := b.packageVersions.Get(regionKey(region, key))
	if !ok {
		return nil, fmt.Errorf("%w: package version not found", ErrNotFound)
	}

	for _, a := range pv.Assets {
		if a.Name == asset {
			return slices.Clone(a.Content), nil
		}
	}

	return nil, fmt.Errorf("%w: asset %s not found", ErrNotFound, asset)
}

// GetPackageVersionReadme is a stub that returns empty README content.
func (b *InMemoryBackend) GetPackageVersionReadme(
	ctx context.Context,
	domainName, repoName, format, namespace, name, version string,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetPackageVersionReadme")
	defer b.mu.RUnlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)
	if !b.packageVersions.Has(regionKey(region, key)) {
		return "", fmt.Errorf("%w: package version not found", ErrNotFound)
	}

	return "", nil
}

// PublishPackageVersion creates or updates a package version in the backend and
// upserts the uploaded asset (by name) into its Assets list. Unlike
// DescribePackageVersion's auto-create fallback, this is the real entry point AWS
// clients use to create a version, so it validates the repository exists first.
func (b *InMemoryBackend) PublishPackageVersion(
	ctx context.Context,
	domainName, repoName, format, namespace, name, version string,
	asset AssetInfo,
) (*PackageVersion, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("PublishPackageVersion")
	defer b.mu.Unlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, repoName))) {
		return nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)

	pv, ok := b.packageVersions.Get(regionKey(region, key))
	if !ok {
		pv = &PackageVersion{
			DomainName:  domainName,
			Repository:  repoName,
			Format:      format,
			Namespace:   namespace,
			PackageName: name,
			Version:     version,
			Status:      "Published",
			Revision:    uuid.NewString()[:8],
			PublishedAt: time.Now().UTC(),
			region:      region,
		}
		b.packageVersions.Put(pv)
	}

	if asset.Name != "" {
		upsertAsset(pv, asset)
	}

	pKey := packageKey(domainName, repoName, format, namespace, name)
	if !b.packages.Has(regionKey(region, pKey)) {
		b.packages.Put(&Package{
			DomainName:  domainName,
			DomainOwner: b.accountID,
			Repository:  repoName,
			Format:      format,
			Namespace:   namespace,
			Name:        name,
			region:      region,
		})
	}

	cp := *pv

	return &cp, nil
}

// upsertAsset replaces the named asset if it already exists on pv (re-publishing the
// same file, e.g. a corrected POM), or appends it otherwise.
func upsertAsset(pv *PackageVersion, asset AssetInfo) {
	for i := range pv.Assets {
		if pv.Assets[i].Name == asset.Name {
			pv.Assets[i] = asset

			return
		}
	}

	pv.Assets = append(pv.Assets, asset)
}

// UpdatePackageVersionsStatus updates the status of specified package versions.
func (b *InMemoryBackend) UpdatePackageVersionsStatus(
	ctx context.Context,
	domainName, repoName, format, namespace, name, targetStatus string,
	versions []string,
) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdatePackageVersionsStatus")
	defer b.mu.Unlock()

	results := make(map[string]string, len(versions))

	for _, v := range versions {
		key := packageVersionKey(domainName, repoName, format, namespace, name, v)
		if pv, ok := b.packageVersions.Get(regionKey(region, key)); ok {
			pv.Status = targetStatus
			results[v] = "SUCCESS"
		} else {
			results[v] = "NOT_FOUND"
		}
	}

	return results, nil
}
