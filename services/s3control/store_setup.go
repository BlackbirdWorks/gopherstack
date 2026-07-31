package s3control

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry -- following the pattern
// established by services/ec2 (commit 12e611a4, data-driven registration
// slice), services/sqs (commit 0f09d77c, DTO-registry), and services/ses
// (commit e9af4cc7, json:"-" identity field + DTO for identity-less types).
// See pkgs/store's package doc for the underlying primitive.
//
// Tables split into two groups:
//
//   - "Clean" tables key off fields the value type already carries (usually
//     AccountID plus a resource ID/name), so they are registered on
//     b.registry here and persistence.go drives them through
//     b.registry.SnapshotAll() / RestoreAll() directly.
//   - "Dirty" tables key off a field with no natural home on the value
//     type: MultiRegionAccessPointRequest carried its bare async-request
//     token only embedded inside a full ARN (RequestTokenARN), and
//     PublicAccessBlock -- shared by the account-level "configs" table and
//     the per-access-point "accessPointPABs" table -- carried no access
//     point name at all. Both gained an identity-only field tagged
//     json:"-" purely so store.Table's keyFn can derive a key from the
//     value -- see the doc comments on those fields in store.go. They are
//     NOT registered on b.registry: persistence.go instead builds a
//     throwaway DTO store.Registry (mirroring the services/sqs pilot) whose
//     DTO types carry the identity as a real JSON field, so Snapshot/Restore
//     never depends on a json:"-" field to round-trip correctly.
//
// Every map[string]*T resource field this backend had is a flat map keyed by
// a composite string ("accountID:resourceID" or bare accountID); none were
// nested (map[string]map[string]*T), so no store.Index is needed here --
// unlike services/ses's eventDestinations.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func publicAccessBlockKeyFn(v *PublicAccessBlock) string { return v.AccountID }

func accessGrantsInstanceKeyFn(v *AccessGrantsInstance) string { return v.AccountID }

func accessGrantKeyFn(v *AccessGrant) string { return v.AccountID + ":" + v.AccessGrantID }

func accessGrantsLocationKeyFn(v *AccessGrantsLocation) string {
	return v.AccountID + ":" + v.AccessGrantsLocationID
}

func accessPointKeyFn(v *AccessPoint) string { return v.AccountID + ":" + v.Name }

func objectLambdaAccessPointKeyFn(v *ObjectLambdaAccessPoint) string {
	return v.AccountID + ":" + v.Name
}

// outpostsBucketKeyFn is keyed by bucket Name ALONE, not "AccountID:Name"
// like every sibling keyFn above -- see the CreateBucket doc comment in
// bucket.go (gopherstack-eje5) for why: unlike every other Create* op in this
// service, the real CreateBucketInput has no AccountId member at all, so
// there is no request-derived account value to partition by at the one
// point (creation) that establishes a bucket's identity.
func outpostsBucketKeyFn(v *OutpostsBucket) string { return v.Name }

func batchJobKeyFn(v *BatchJob) string { return v.AccountID + ":" + v.JobID }

func mrapKeyFn(v *MultiRegionAccessPoint) string { return v.AccountID + ":" + v.Name }

func storageLensGroupKeyFn(v *StorageLensGroup) string { return v.AccountID + ":" + v.Name }

// mrapRequestKeyFn is the "dirty" keyFn for the mrapRequests Table -- see the
// file doc comment above and MultiRegionAccessPointRequest.Token's doc
// comment in store.go.
func mrapRequestKeyFn(v *MultiRegionAccessPointRequest) string { return v.AccountID + ":" + v.Token }

// accessPointPABKeyFn is the "dirty" keyFn for the accessPointPABs Table --
// see the file doc comment above and PublicAccessBlock.APName's doc comment
// in store.go. It is a distinct *store.Table[PublicAccessBlock] from the
// "configs" table (account-level, keyed by AccountID alone via
// publicAccessBlockKeyFn); APName is always empty for configs entries.
func accessPointPABKeyFn(v *PublicAccessBlock) string { return v.AccountID + ":" + v.APName }

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. "Clean" tables are registered on
// b.registry; "dirty" tables (mrapRequests, accessPointPABs) are
// constructed alongside them but deliberately NOT registered -- see the
// file doc comment above. It must be called during construction only, never
// on every Reset(): store.Register panics on a duplicate name, so runtime
// resets go through resetTablesLocked (store.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.configs = store.Register(b.registry, "configs", store.New(publicAccessBlockKeyFn))
	b.accessGrantsInstances = store.Register(
		b.registry, "accessGrantsInstances", store.New(accessGrantsInstanceKeyFn),
	)
	b.accessGrants = store.Register(b.registry, "accessGrants", store.New(accessGrantKeyFn))
	b.accessGrantsLocations = store.Register(
		b.registry, "accessGrantsLocations", store.New(accessGrantsLocationKeyFn),
	)
	b.accessPoints = store.Register(b.registry, "accessPoints", store.New(accessPointKeyFn))
	b.objectLambdaAccessPoints = store.Register(
		b.registry, "objectLambdaAccessPoints", store.New(objectLambdaAccessPointKeyFn),
	)
	b.outpostsBuckets = store.Register(b.registry, "outpostsBuckets", store.New(outpostsBucketKeyFn))
	b.batchJobs = store.Register(b.registry, "batchJobs", store.New(batchJobKeyFn))
	b.mraps = store.Register(b.registry, "mraps", store.New(mrapKeyFn))
	b.storageLensGroups = store.Register(b.registry, "storageLensGroups", store.New(storageLensGroupKeyFn))

	b.mrapRequests = store.New(mrapRequestKeyFn)
	b.accessPointPABs = store.New(accessPointPABKeyFn)
}
