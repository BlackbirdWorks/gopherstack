package elasticsearch

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"
)

// CreatePackage creates a new Elasticsearch package (e.g., a dictionary file).
// PackageSource (S3BucketName + S3Key) is a required member of
// CreatePackageInput in the real API (types.CreatePackageInput.PackageSource
// has no default), so a missing/incomplete source is rejected exactly like a
// missing name or an invalid type.
func (b *InMemoryBackend) CreatePackage(
	ctx context.Context, name, packageType, description string, source PackageSource,
) (*Package, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: PackageName is required", ErrValidation)
	}

	if !validPackageTypes[packageType] {
		return nil, fmt.Errorf(
			"%w: PackageType must be TXT-DICTIONARY, got %q",
			ErrValidation,
			packageType,
		)
	}

	if source.S3BucketName == "" || source.S3Key == "" {
		return nil, fmt.Errorf("%w: PackageSource.S3BucketName and PackageSource.S3Key are required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("CreatePackage")
	defer b.mu.Unlock()

	packagesByName := b.packagesByNameStore(region)
	if _, exists := packagesByName[name]; exists {
		return nil, fmt.Errorf("%w: package %s already exists", ErrDomainAlreadyExists, name)
	}

	id := fmt.Sprintf("F%010d", b.nextIDLocked())
	now := time.Now()
	pkg := &Package{
		ID:            id,
		Name:          name,
		PackageType:   packageType,
		Description:   description,
		Status:        "AVAILABLE",
		PackageSource: source,
		CreatedAt:     now,
		LastUpdatedAt: now,
		Versions:      []PackageVersion{{Number: 1, CreatedAt: now}},
		region:        region,
	}
	b.packagePut(pkg)
	packagesByName[name] = id

	cp := *pkg

	return &cp, nil
}

// AssociatePackage associates an Elasticsearch package with a domain.
func (b *InMemoryBackend) AssociatePackage(ctx context.Context, packageID, domainName string) (*DomainPackage, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("AssociatePackage")
	defer b.mu.Unlock()

	pkg, exists := b.packageGet(region, packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	if _, exists = b.domainGet(region, domainName); !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	assocs := b.packageAssociationsStore(region)
	if slices.Contains(assocs[packageID], domainName) {
		return nil, fmt.Errorf(
			"%w: package %s is already associated with domain %s",
			ErrPackageAlreadyAssociated, packageID, domainName,
		)
	}

	assocs[packageID] = append(assocs[packageID], domainName)

	meta := b.packageAssociationMetaStore(region)
	if meta[packageID] == nil {
		meta[packageID] = make(map[string]PackageAssociation)
	}

	meta[packageID][domainName] = PackageAssociation{
		LastUpdated:    b.clock(),
		PackageVersion: availablePackageVersion(pkg),
	}

	return b.domainPackageLocked(region, pkg, domainName), nil
}

// domainPackageLocked builds the details of one association; associations
// restored without details fall back to the package's current version.
func (b *InMemoryBackend) domainPackageLocked(region string, pkg *Package, domainName string) *DomainPackage {
	meta, ok := b.packageAssociationMetaStoreRO(region)[pkg.ID][domainName]
	if !ok {
		meta = PackageAssociation{LastUpdated: pkg.LastUpdatedAt, PackageVersion: availablePackageVersion(pkg)}
	}

	return &DomainPackage{
		Package:        clonePackage(pkg),
		DomainName:     domainName,
		LastUpdated:    meta.LastUpdated,
		PackageVersion: meta.PackageVersion,
		ReferencePath:  packageReferencePath(pkg.ID),
	}
}

// packageReferencePath is the node-relative path a package is mounted at
// (the "analyzers/F111111111" form used as synonyms_path).
func packageReferencePath(packageID string) string { return "analyzers/" + packageID }

// DeletePackage removes a package by ID.
func (b *InMemoryBackend) DeletePackage(ctx context.Context, packageID string) (*Package, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeletePackage")
	defer b.mu.Unlock()

	pkg, exists := b.packageGet(region, packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	cp := *pkg
	delete(b.packagesByNameStore(region), pkg.Name)
	b.packageDelete(region, packageID)
	delete(b.packageAssociationsStore(region), packageID)
	delete(b.packageAssociationMetaStore(region), packageID)

	return &cp, nil
}

// DescribePackages returns packages matching the given IDs, or all packages if the list is empty.
func (b *InMemoryBackend) DescribePackages(ctx context.Context, packageIDs []string) []*Package {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribePackages")
	defer b.mu.RUnlock()

	if len(packageIDs) == 0 {
		packages := b.packagesInRegion(region)
		result := make([]*Package, 0, len(packages))
		for _, pkg := range packages {
			cp := *pkg
			result = append(result, &cp)
		}

		return result
	}

	result := make([]*Package, 0, len(packageIDs))
	for _, id := range packageIDs {
		if pkg, exists := b.packageGet(region, id); exists {
			cp := *pkg
			result = append(result, &cp)
		}
	}

	return result
}

// DissociatePackage removes a package association from a domain and returns
// the association as it stood.
func (b *InMemoryBackend) DissociatePackage(ctx context.Context, packageID, domainName string) (*DomainPackage, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DissociatePackage")
	defer b.mu.Unlock()

	pkg, exists := b.packageGet(region, packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	if _, exists = b.domainGet(region, domainName); !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	out := b.domainPackageLocked(region, pkg, domainName)

	associations := b.packageAssociationsStore(region)
	if idx := slices.Index(associations[packageID], domainName); idx >= 0 {
		associations[packageID] = slices.Delete(associations[packageID], idx, idx+1)
	}

	delete(b.packageAssociationMetaStore(region)[packageID], domainName)

	return out, nil
}

// GetPackageVersionHistory returns the package's versions, newest first.
func (b *InMemoryBackend) GetPackageVersionHistory(ctx context.Context, packageID string) ([]PackageVersion, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("GetPackageVersionHistory")
	defer b.mu.RUnlock()

	pkg, exists := b.packageGet(region, packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	versions := packageVersions(pkg)
	out := make([]PackageVersion, 0, len(versions))

	for _, v := range slices.Backward(versions) {
		out = append(out, v)
	}

	return out, nil
}

// packageVersions returns the package's versions, treating a package restored
// without version data as a single version 1.
func packageVersions(p *Package) []PackageVersion {
	if len(p.Versions) == 0 {
		return []PackageVersion{{Number: 1, CreatedAt: p.CreatedAt}}
	}

	return p.Versions
}

// availablePackageVersion returns the latest version label, such as "v2".
func availablePackageVersion(p *Package) string {
	versions := packageVersions(p)

	return packageVersionLabel(versions[len(versions)-1].Number)
}

func packageVersionLabel(n int) string {
	return "v" + strconv.Itoa(n)
}

// clonePackage copies p including its version slice.
func clonePackage(p *Package) *Package {
	cp := *p
	cp.Versions = slices.Clone(p.Versions)

	return &cp
}

// ListDomainsForPackage returns the domains associated with a package.
func (b *InMemoryBackend) ListDomainsForPackage(ctx context.Context, packageID string) ([]*DomainPackage, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListDomainsForPackage")
	defer b.mu.RUnlock()

	pkg, exists := b.packageGet(region, packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	assocs := b.packageAssociationsStoreRO(region)[packageID]
	result := make([]*DomainPackage, 0, len(assocs))

	for _, domainName := range assocs {
		result = append(result, b.domainPackageLocked(region, pkg, domainName))
	}

	return result, nil
}

// ListPackagesForDomain returns the packages associated with a domain.
func (b *InMemoryBackend) ListPackagesForDomain(ctx context.Context, domainName string) ([]*DomainPackage, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("ListPackagesForDomain")
	defer b.mu.RUnlock()

	if _, exists := b.domainGet(region, domainName); !exists {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	var result []*DomainPackage

	for packageID, assocs := range b.packageAssociationsStoreRO(region) {
		if !slices.Contains(assocs, domainName) {
			continue
		}

		if pkg, exists := b.packageGet(region, packageID); exists {
			result = append(result, b.domainPackageLocked(region, pkg, domainName))
		}
	}

	return result, nil
}

// UpdatePackage updates a package description.
func (b *InMemoryBackend) UpdatePackage(
	ctx context.Context, packageID, description, commitMessage string, source PackageSource,
) (*Package, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("UpdatePackage")
	defer b.mu.Unlock()

	pkg, exists := b.packageGet(region, packageID)
	if !exists {
		return nil, fmt.Errorf("%w: package %s not found", ErrPackageNotFound, packageID)
	}

	pkg.Description = description
	pkg.PackageSource = source
	pkg.LastUpdatedAt = time.Now()

	versions := packageVersions(pkg)
	pkg.Versions = append(slices.Clone(versions), PackageVersion{
		Number:        versions[len(versions)-1].Number + 1,
		CommitMessage: commitMessage,
		CreatedAt:     pkg.LastUpdatedAt,
	})

	if len(pkg.Versions) > maxPackageVersions {
		pkg.Versions = slices.Clone(pkg.Versions[len(pkg.Versions)-maxPackageVersions:])
	}

	return clonePackage(pkg), nil
}
