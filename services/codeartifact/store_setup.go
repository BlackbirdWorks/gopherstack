package codeartifact

// Code in this file supports Phase 3.3 of the datalayer refactor: seven
// resource collections that were previously nested by region (outer key =
// region, e.g. map[string]map[string]*Domain) are each replaced by a single
// flat *store.Table[T], keyed by the composite "region|id" string (see
// regionKey in backend.go), with a companion *store.Index grouping entries
// by region for the per-region scans the nested maps used to answer
// directly -- the region-qualified-table pattern services/emr uses.
//
// domains and repositories already carried a real, wire-visible Region
// field, so each is a "clean" table -- registered directly on b.registry, no
// DTO wrapper needed (see persistence.go).
//
// packageGroups, packages, and packageVersions have no wire-visible region
// field, so each gained an unexported region field purely for this
// composite key. repositoryPolicies and domainPolicies are missing not just
// region but their entire parent identity (RepositoryPermissionsPolicy has
// no DomainName/RepoName field at all; DomainPermissionsPolicy has no
// DomainName field), so each gained region plus the missing identity
// field(s). All five are "dirty" tables: store.New only, deliberately NOT
// store.Register-ed onto b.registry (so b.registry.SnapshotAll never tries
// to marshal them directly, which would silently drop the hidden fields);
// persistence.go instead builds an ephemeral DTO registry for them, and
// InMemoryBackend.Reset (backend.go) resets each of the five explicitly.
//
// externalConnections is deliberately NOT converted: its value,
// []ExternalConnection, is a slice of plain structs with no identity of its
// own, so there is nothing for store.Table to key on. It remains a plain
// region-nested map, unchanged by this refactor (see externalConnectionsStore
// in backend.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func domainTableKeyFn(v *Domain) string       { return regionKey(v.Region, v.Name) }
func domainRegionIndexKeyFn(v *Domain) string { return v.Region }

func repositoryTableKeyFn(v *Repository) string {
	return regionKey(v.Region, repoKey(v.DomainName, v.Name))
}
func repositoryRegionIndexKeyFn(v *Repository) string { return v.Region }

func packageGroupTableKeyFn(v *PackageGroup) string {
	return regionKey(v.region, packageGroupKey(v.DomainName, v.Pattern))
}
func packageGroupRegionIndexKeyFn(v *PackageGroup) string { return v.region }

func packageTableKeyFn(v *Package) string {
	return regionKey(v.region, packageKey(v.DomainName, v.Repository, v.Format, v.Namespace, v.Name))
}
func packageRegionIndexKeyFn(v *Package) string { return v.region }

func packageVersionTableKeyFn(v *PackageVersion) string {
	return regionKey(v.region, packageVersionKey(
		v.DomainName, v.Repository, v.Format, v.Namespace, v.PackageName, v.Version,
	))
}
func packageVersionRegionIndexKeyFn(v *PackageVersion) string { return v.region }

func repositoryPolicyTableKeyFn(v *RepositoryPermissionsPolicy) string {
	return regionKey(v.region, repoKey(v.domainName, v.repoName))
}

func domainPolicyTableKeyFn(v *DomainPermissionsPolicy) string {
	return regionKey(v.region, v.domainName)
}

// registerAllTables registers every converted resource collection on
// b.registry (the "clean" tables) or builds it standalone (the "dirty"
// tables -- see the file doc comment above) exactly once. It must be called
// during construction only (immediately after b.registry is created -- see
// NewInMemoryBackend), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through b.registry.ResetAll() plus
// the dirty tables' own Reset() calls instead (see InMemoryBackend.Reset in
// backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.domains = store.Register(b.registry, "domains", store.New(domainTableKeyFn))
	b.domainsByRegion = b.domains.AddIndex("byRegion", domainRegionIndexKeyFn)

	b.repositories = store.Register(b.registry, "repositories", store.New(repositoryTableKeyFn))
	b.repositoriesByRegion = b.repositories.AddIndex("byRegion", repositoryRegionIndexKeyFn)

	b.packageGroups = store.New(packageGroupTableKeyFn)
	b.packageGroupsByRegion = b.packageGroups.AddIndex("byRegion", packageGroupRegionIndexKeyFn)

	b.packages = store.New(packageTableKeyFn)
	b.packagesByRegion = b.packages.AddIndex("byRegion", packageRegionIndexKeyFn)

	b.packageVersions = store.New(packageVersionTableKeyFn)
	b.packageVersionsByRegion = b.packageVersions.AddIndex("byRegion", packageVersionRegionIndexKeyFn)

	b.repositoryPolicies = store.New(repositoryPolicyTableKeyFn)
	b.domainPolicies = store.New(domainPolicyTableKeyFn)
}
