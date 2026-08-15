package codeartifact

import (
	"context"
	"encoding/json"
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

// PackageVersionOutcome mirrors the wire shape of types.SuccessfulPackageVersionInfo
// (revision/status) -- the per-version value real AWS returns in the
// successfulVersions map of DeletePackageVersions/CopyPackageVersions/
// DisposePackageVersions/UpdatePackageVersionsStatus's outputs, all four of
// which key both successfulVersions and failedVersions by version string
// (a JSON *object*, not the array this backend used to build -- see
// deserializers.go's ...PackageVersionErrorMap/...SuccessfulPackageVersionInfoMap).
type PackageVersionOutcome struct {
	Revision string
	Status   string
}

// PackageVersionErrorCode values, mirroring types.PackageVersionErrorCode's
// two variants this backend can produce.
const (
	packageVersionErrorNotFound      = "NOT_FOUND"
	packageVersionErrorAlreadyExists = "ALREADY_EXISTS"
)

// DeletePackageVersions deletes specified versions of a package and returns
// per-version outcomes: successful (revision/status as of just before
// deletion) and failed (real PackageVersionErrorCode values, e.g. NOT_FOUND).
func (b *InMemoryBackend) DeletePackageVersions(
	ctx context.Context,
	domainName, repoName, format, namespace, name string,
	versions []string,
) (map[string]PackageVersionOutcome, map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePackageVersions")
	defer b.mu.Unlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, repoName))) {
		return nil, nil, fmt.Errorf("%w: repository %s not found in domain %s", ErrNotFound, repoName, domainName)
	}

	successful := make(map[string]PackageVersionOutcome)
	failed := make(map[string]string)
	for _, v := range versions {
		vKey := packageVersionKey(domainName, repoName, format, namespace, name, v)
		pv, ok := b.packageVersions.Get(regionKey(region, vKey))
		if !ok {
			failed[v] = packageVersionErrorNotFound

			continue
		}
		successful[v] = PackageVersionOutcome{Revision: pv.Revision, Status: "Deleted"}
		b.packageVersions.Delete(regionKey(region, vKey))
	}

	return successful, failed, nil
}

// CopyPackageVersions copies specified package versions from a source repository
// to a destination repository in the same domain.
func (b *InMemoryBackend) CopyPackageVersions(
	ctx context.Context,
	domainName, srcRepo, dstRepo, format, namespace, name string,
	versions []string,
) (map[string]PackageVersionOutcome, map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CopyPackageVersions")
	defer b.mu.Unlock()

	if !b.repositories.Has(regionKey(region, repoKey(domainName, srcRepo))) {
		return nil, nil, fmt.Errorf("%w: source repository %s not found in domain %s", ErrNotFound, srcRepo, domainName)
	}
	if !b.repositories.Has(regionKey(region, repoKey(domainName, dstRepo))) {
		return nil, nil, fmt.Errorf(
			"%w: destination repository %s not found in domain %s", ErrNotFound, dstRepo, domainName,
		)
	}

	successful := make(map[string]PackageVersionOutcome)
	failed := make(map[string]string)
	for _, v := range versions {
		srcKey := packageVersionKey(domainName, srcRepo, format, namespace, name, v)
		src, ok := b.packageVersions.Get(regionKey(region, srcKey))
		if !ok {
			failed[v] = packageVersionErrorNotFound

			continue
		}
		dstKey := packageVersionKey(domainName, dstRepo, format, namespace, name, v)
		if b.packageVersions.Has(regionKey(region, dstKey)) {
			failed[v] = packageVersionErrorAlreadyExists

			continue
		}
		copied := *src
		copied.Repository = dstRepo
		copied.region = region
		b.packageVersions.Put(&copied)
		successful[v] = PackageVersionOutcome{Revision: copied.Revision, Status: copied.Status}
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

	return successful, failed, nil
}

// DisposePackageVersions moves specified versions of a package to the Disposed status.
func (b *InMemoryBackend) DisposePackageVersions(
	ctx context.Context,
	domainName, repoName, format, namespace, name string,
	versions []string,
) (map[string]PackageVersionOutcome, map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DisposePackageVersions")
	defer b.mu.Unlock()

	successful := make(map[string]PackageVersionOutcome, len(versions))
	failed := make(map[string]string)

	for _, v := range versions {
		key := packageVersionKey(domainName, repoName, format, namespace, name, v)
		if pv, ok := b.packageVersions.Get(regionKey(region, key)); ok {
			pv.Status = "Disposed"
			successful[v] = PackageVersionOutcome{Revision: pv.Revision, Status: "Disposed"}
		} else {
			failed[v] = packageVersionErrorNotFound
		}
	}

	return successful, failed, nil
}

// ListPackageVersions lists versions of a package, optionally filtered by
// status (real ListPackageVersionsInput.Status, serializers.go's
// SetQuery("status")) and reordered by publish time (real
// ListPackageVersionsInput.SortBy, which has exactly one enum value,
// PUBLISHED_TIME -- serializers.go's SetQuery("sortBy")). OriginType is a
// real filter member too but this backend has no per-version origin concept
// to source it from -- disclosed in PARITY.md rather than fabricated.
func (b *InMemoryBackend) ListPackageVersions(
	ctx context.Context,
	domainName, repoName, format, namespace, name, status, sortBy string,
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

		if status != "" && pv.Status != status {
			continue
		}

		cp := *pv
		result = append(result, &cp)
	}

	if sortBy == "PUBLISHED_TIME" {
		sort.Slice(result, func(i, j int) bool {
			return result[i].PublishedAt.Before(result[j].PublishedAt)
		})
	} else {
		sort.Slice(result, func(i, j int) bool {
			return result[i].Version < result[j].Version
		})
	}

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

// PackageDependencyInfo is a single dependency extracted from a package
// version's published metadata, mirroring aws-sdk-go-v2
// types.PackageDependency.
type PackageDependencyInfo struct {
	DependencyType     string
	Namespace          string
	PackageName        string
	VersionRequirement string
}

// npmPackageJSON models the subset of an npm package.json this backend
// parses to serve real (not stubbed) GetPackageVersionReadme/
// ListPackageVersionDependencies content -- used only when the caller's
// published asset list includes a file literally named "package.json" (the
// metadata file real npm clients/CodeArtifact extract this data from when
// publishing a tarball). This backend's single-asset-per-call publish model
// doesn't unpack full tarballs, so formats/publishes that don't include a
// standalone "package.json" asset still get empty readme/dependencies --
// see PARITY.md's gaps.
type npmPackageJSON struct {
	Dependencies         map[string]string `json:"dependencies"`
	DevDependencies      map[string]string `json:"devDependencies"`
	PeerDependencies     map[string]string `json:"peerDependencies"`
	OptionalDependencies map[string]string `json:"optionalDependencies"`
	Readme               string            `json:"readme"`
}

// findPackageJSONMetadata returns the parsed "package.json" asset among
// assets, or nil if none was published or it doesn't parse as JSON.
func findPackageJSONMetadata(assets []AssetInfo) *npmPackageJSON {
	for _, a := range assets {
		if a.Name != "package.json" {
			continue
		}

		var meta npmPackageJSON
		if err := json.Unmarshal(a.Content, &meta); err != nil {
			return nil
		}

		return &meta
	}

	return nil
}

// npmDependencyTypes pairs each npm package.json dependency map with its
// real AWS dependencyType string (verified against aws-sdk-go-v2
// types.PackageDependency's doc comment: "npm: regular, dev, peer,
// optional").
//
//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var npmDependencyTypeAccessors = []struct {
	get     func(*npmPackageJSON) map[string]string
	depType string
}{
	{depType: "regular", get: func(m *npmPackageJSON) map[string]string { return m.Dependencies }},
	{depType: "dev", get: func(m *npmPackageJSON) map[string]string { return m.DevDependencies }},
	{depType: "peer", get: func(m *npmPackageJSON) map[string]string { return m.PeerDependencies }},
	{depType: "optional", get: func(m *npmPackageJSON) map[string]string { return m.OptionalDependencies }},
}

// dependenciesFromNpmMetadata flattens meta's dependency maps into
// PackageDependencyInfo entries, sorted by (dependencyType, packageName) for
// deterministic output.
func dependenciesFromNpmMetadata(meta *npmPackageJSON) []PackageDependencyInfo {
	result := make([]PackageDependencyInfo, 0)

	for _, accessor := range npmDependencyTypeAccessors {
		names := make([]string, 0, len(accessor.get(meta)))
		for name := range accessor.get(meta) {
			names = append(names, name)
		}

		sort.Strings(names)

		for _, name := range names {
			result = append(result, PackageDependencyInfo{
				DependencyType:     accessor.depType,
				PackageName:        name,
				VersionRequirement: accessor.get(meta)[name],
			})
		}
	}

	return result
}

// ListPackageVersionDependencies returns dependencies parsed from a
// published "package.json" asset (npm convention), if one exists -- see
// npmPackageJSON's doc comment for scope. Otherwise returns an empty list,
// same as real AWS would for a package version whose metadata carries no
// dependencies.
func (b *InMemoryBackend) ListPackageVersionDependencies(
	ctx context.Context,
	domainName, repoName, format, namespace, name, version string,
) ([]PackageDependencyInfo, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPackageVersionDependencies")
	defer b.mu.RUnlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)

	pv, ok := b.packageVersions.Get(regionKey(region, key))
	if !ok {
		return nil, fmt.Errorf("%w: package version not found", ErrNotFound)
	}

	meta := findPackageJSONMetadata(pv.Assets)
	if meta == nil {
		return []PackageDependencyInfo{}, nil
	}

	return dependenciesFromNpmMetadata(meta), nil
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

// GetPackageVersionReadme returns the "readme" field parsed from a
// published "package.json" asset (npm convention), if one exists -- see
// npmPackageJSON's doc comment for scope. Otherwise returns "", same as
// real AWS would for a package version whose metadata carries no readme.
// The returned PackageVersion lets callers build the full
// GetPackageVersionReadmeOutput wire shape (format/namespace/package/
// version/versionRevision) without a second lookup.
func (b *InMemoryBackend) GetPackageVersionReadme(
	ctx context.Context,
	domainName, repoName, format, namespace, name, version string,
) (string, *PackageVersion, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetPackageVersionReadme")
	defer b.mu.RUnlock()

	key := packageVersionKey(domainName, repoName, format, namespace, name, version)

	found, ok := b.packageVersions.Get(regionKey(region, key))
	if !ok {
		return "", nil, fmt.Errorf("%w: package version not found", ErrNotFound)
	}

	cp := *found

	var readme string
	if meta := findPackageJSONMetadata(found.Assets); meta != nil {
		readme = meta.Readme
	}

	return readme, &cp, nil
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
) (map[string]PackageVersionOutcome, map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdatePackageVersionsStatus")
	defer b.mu.Unlock()

	successful := make(map[string]PackageVersionOutcome, len(versions))
	failed := make(map[string]string)

	for _, v := range versions {
		key := packageVersionKey(domainName, repoName, format, namespace, name, v)
		if pv, ok := b.packageVersions.Get(regionKey(region, key)); ok {
			pv.Status = targetStatus
			successful[v] = PackageVersionOutcome{Revision: pv.Revision, Status: targetStatus}
		} else {
			failed[v] = packageVersionErrorNotFound
		}
	}

	return successful, failed, nil
}
