package efs

// Code in this file supports Phase 3.3 of the datalayer refactor: four
// resource collections that were previously nested by region (outer key =
// region, e.g. map[string]map[string]*FileSystem) are each replaced by a
// single flat *store.Table[T], keyed by the composite "region|id" string (see
// regionKey below), with a companion *store.Index grouping entries by region
// for per-region scans -- the region-qualified-table pattern services/emr,
// services/mwaa, and services/neptune use. It otherwise follows pkgs/store's
// package doc.
//
// fileSystemsByARN, mountTargetsByARN, accessPointsByARN, and
// accessPointsByClientToken are a second layer on top of that: derived-cache
// *store.Table values (the services/ecs taskDefByArn pattern) providing O(1),
// region-scoped lookup of the *same* FileSystem/MountTarget/AccessPoint
// objects by their own ARN (or, for access points, ClientToken) instead of by
// primary ID. They are deliberately NOT registered on b.registry -- they hold
// no state of their own, so persisting them independently would be
// redundant; Restore rebuilds them from the just-restored primary tables
// instead (see rebuildDerivedIndexes in persistence.go), exactly as
// InMemoryBackend.Reset resets them directly rather than through
// registry.ResetAll().
//
// lifecyclePolicies (map[string]map[string][]LifecyclePolicy),
// backupPolicies/fileSystemPolicies (map[string]map[string]string), and the
// performance indexes creationTokenIdx/mtSubnetIdx/apByFS are deliberately
// NOT converted: store.Table requires a *V value with its own identity, but
// each entry in these maps is a bare slice/string/struct{} with no
// identifier of its own. They remain plain nested maps, unchanged by this
// refactor (see the *Store helpers in backend.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionKey builds a composite store.Table key for a resource whose primary
// identity (fsID/mtID/apID/ARN/ClientToken) is only unique within its own
// region, mirroring the two-level map[region]map[id]*T nesting it replaces.
func regionKey(region, id string) string { return region + "|" + id }

func fileSystemKeyFn(v *FileSystem) string            { return regionKey(v.Region, v.FileSystemID) }
func fileSystemRegionIndexKeyFn(v *FileSystem) string { return v.Region }
func fileSystemByARNKeyFn(v *FileSystem) string       { return regionKey(v.Region, v.FileSystemArn) }

func mountTargetKeyFn(v *MountTarget) string            { return regionKey(v.region, v.MountTargetID) }
func mountTargetRegionIndexKeyFn(v *MountTarget) string { return v.region }
func mountTargetByARNKeyFn(v *MountTarget) string       { return regionKey(v.region, v.MountTargetArn) }

func accessPointKeyFn(v *AccessPoint) string            { return regionKey(v.region, v.AccessPointID) }
func accessPointRegionIndexKeyFn(v *AccessPoint) string { return v.region }
func accessPointByARNKeyFn(v *AccessPoint) string       { return regionKey(v.region, v.AccessPointArn) }

// accessPointByClientTokenKeyFn keys the ClientToken derived cache. It is
// only ever Put for access points with a non-empty ClientToken (mirroring
// the pre-conversion "if req.ClientToken != \"\"" guard around the old
// apClientTokenStore assignment in CreateAccessPoint), so a lookup key of ""
// is never queried.
func accessPointByClientTokenKeyFn(v *AccessPoint) string { return regionKey(v.region, v.ClientToken) }

// replicationConfigKeyFn and replicationConfigRegionIndexKeyFn key off
// SourceFileSystemRegion rather than an unexported region field:
// ReplicationConfiguration already carries its owning region visibly in JSON
// (set to the create-time region in CreateReplicationConfiguration), so --
// unlike MountTarget/AccessPoint -- no hidden field or persistence DTO is
// needed for it (matches services/neptune's GlobalCluster precedent, the one
// converted table that needed no region-carrying wrapper).
func replicationConfigKeyFn(v *ReplicationConfiguration) string {
	return regionKey(v.SourceFileSystemRegion, v.SourceFileSystemID)
}

func replicationConfigRegionIndexKeyFn(v *ReplicationConfiguration) string {
	return v.SourceFileSystemRegion
}

// registerAllTables registers every converted resource collection on
// b.registry exactly once, and constructs the four unregistered derived-cache
// tables. It must be called during construction only (immediately after
// b.registry is created -- see NewInMemoryBackend), never on every Reset() --
// store.Register panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() (plus the four derived caches' own .Reset()) instead;
// see InMemoryBackend.Reset in backend.go.
func registerAllTables(b *InMemoryBackend) {
	b.fileSystems = store.Register(b.registry, "fileSystems", store.New(fileSystemKeyFn))
	b.fileSystemsByRegion = b.fileSystems.AddIndex("byRegion", fileSystemRegionIndexKeyFn)

	b.mountTargets = store.Register(b.registry, "mountTargets", store.New(mountTargetKeyFn))
	b.mountTargetsByRegion = b.mountTargets.AddIndex("byRegion", mountTargetRegionIndexKeyFn)

	b.accessPoints = store.Register(b.registry, "accessPoints", store.New(accessPointKeyFn))
	b.accessPointsByRegion = b.accessPoints.AddIndex("byRegion", accessPointRegionIndexKeyFn)

	b.replicationConfigs = store.Register(b.registry, "replicationConfigs", store.New(replicationConfigKeyFn))
	b.replicationConfigsByRegion = b.replicationConfigs.AddIndex("byRegion", replicationConfigRegionIndexKeyFn)

	// Unregistered derived-cache tables: reset/rebuilt manually, never part of
	// the persisted "tables" blob (see Reset in backend.go, Restore in persistence.go).
	b.fileSystemsByARN = store.New(fileSystemByARNKeyFn)
	b.mountTargetsByARN = store.New(mountTargetByARNKeyFn)
	b.accessPointsByARN = store.New(accessPointByARNKeyFn)
	b.accessPointsByClientToken = store.New(accessPointByClientTokenKeyFn)
}
