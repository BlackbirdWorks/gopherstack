package glacier

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (and map[compositeKey]*T) resource field on InMemoryBackend
// is registered exactly once, here, as a *store.Table[T] on b.registry. See
// pkgs/store's package doc and the services/ec2 (commit 12e611a4) /
// services/sqs (commit 0f09d77c) / services/apigateway conversions this
// follows.
//
// Jobs and MultipartUploads were previously nested per-vault maps
// (map[vaultKey]map[string]*T). store.Table has no notion of nesting, so
// each becomes a single flat table keyed by a composite "<vaultARN>#<id>"
// string (mirroring apigateway's "<restAPIID>#<childID>" pattern), with a
// secondary [store.Index] grouping by vault ARN for the "all jobs/uploads of
// vault X" lookups the nested maps used to answer directly. Both Job and
// MultipartUpload already carry VaultARN as a real, wire-visible field (used
// in their AWS response DTOs), so unlike apigateway's "dirty" tables no new
// json:"-" field or DTO-registry was needed here -- every registered table
// below is "clean" and round-trips through store.Registry.SnapshotAll /
// RestoreAll directly.
//
// Archives are deliberately NOT converted to their own store.Table: every
// access site scopes them by vault, and Archive itself carries no natural
// cross-vault identity field to key a flat table by (unlike Job/
// MultipartUpload's VaultARN). They stay inline as a map[string]*Archive
// field on Vault -- see the Vault doc comment in models.go.
//
// The following fields are deliberately plain maps, not store.Table at all:
//   - multipartParts (uploadKey -> []MultipartPart): slice-valued, not *T,
//     and MultipartPart has no identity field of its own.
//   - provisionedCapacity (accountID -> []*ProvisionedCapacity):
//     slice-valued, not *T.
//   - dataRetrievalPolicies (accountID -> string): value has no identity
//     field of its own to key a Table by.
//   - archiveData (archiveID -> []byte): slice-valued, not *T.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// acctRegionKey builds the composite "<accountID>/<region>" key used by the
// vaults table's secondary "byAccountRegion" index, replacing the
// hand-rolled accountID -> region -> vaultName -> struct{} nested map
// ListVaults used before this conversion.
func acctRegionKey(accountID, region string) string { return accountID + "/" + region }

func vaultKeyFn(v *Vault) string           { return v.VaultARN }
func vaultAcctRegionKeyFn(v *Vault) string { return acctRegionKey(v.AccountID, v.Region) }

// jobKey/jobKeyFn build the composite "<vaultARN>#<jobID>" key shared by
// every job. Access sites call jobKey directly (via the vaultARN helper in
// backend.go) so the exact same key is used for Put/Get/Delete.
func jobKey(vArn, jobID string) string { return vArn + "#" + jobID }
func jobKeyFn(v *Job) string           { return jobKey(v.VaultARN, v.JobID) }
func jobVaultIndexKeyFn(v *Job) string { return v.VaultARN }

// multipartUploadKey/multipartUploadKeyFn mirror jobKey/jobKeyFn for
// MultipartUpload.
func multipartUploadKey(vArn, uploadID string) string { return vArn + "#" + uploadID }
func multipartUploadKeyFn(v *MultipartUpload) string {
	return multipartUploadKey(v.VaultARN, v.MultipartUploadID)
}
func multipartUploadVaultIndexKeyFn(v *MultipartUpload) string { return v.VaultARN }

// vaultLockKeyFn keys VaultLock by its owning vault's ARN. There is at most
// one lock per vault, so no secondary index is needed.
func vaultLockKeyFn(v *VaultLock) string { return v.VaultARN }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created), never on every Reset() --
// store.Register panics on a duplicate name, so runtime resets go through
// registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call (plus any secondary store.AddIndex) to the concrete field and value
// type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.vaults = store.Register(b.registry, "vaults", store.New(vaultKeyFn))
		b.vaultsByAccountRegion = b.vaults.AddIndex("byAccountRegion", vaultAcctRegionKeyFn)
	},
	func(b *InMemoryBackend) {
		b.jobs = store.Register(b.registry, "jobs", store.New(jobKeyFn))
		b.jobsByVault = b.jobs.AddIndex("byVault", jobVaultIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.multipartUploads = store.Register(b.registry, "multipartUploads", store.New(multipartUploadKeyFn))
		b.multipartUploadsByVault = b.multipartUploads.AddIndex("byVault", multipartUploadVaultIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.vaultLocks = store.Register(b.registry, "vaultLocks", store.New(vaultLockKeyFn))
	},
}
