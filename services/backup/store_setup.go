package backup

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/ec2
// (commit 12e611a4, data-driven registration slice), services/sqs (commit
// 0f09d77c, DTO-registry pilot), and services/ses (commit e9af4cc7,
// identity-less json:"-" key fields). See pkgs/store's package doc for the
// underlying primitive.
//
// Every table is registered on b.registry, and persistence.go drives all of
// them through b.registry.SnapshotAll() / RestoreAll() directly (gopherstack-7au:
// recoveryPoints, copyJobs, vaultAccessPolicies, vaultLockConfigs,
// vaultNotifications, restoreJobs, reportJobs, scanJobs, tieringConfigs, and
// protectedResources held real user state but were never registered, so they
// were silently lost across a restart). selections and restoreTestingSelections
// key off a composite "<parentID>#<childID>" string built from fields the
// value type already carries (Selection.BackupPlanID/SelectionID,
// RestoreTestingSelection.RestoreTestingPlanName/RestoreTestingSelectionName),
// replacing the nested map[string]map[string]*T the backend used to hold;
// each also carries a secondary store.Index grouping by the parent key,
// replacing the nested map's direct lookup ("selections of plan X").
// recoveryPoints similarly carries a secondary store.Index grouping by
// BackupVaultName, replacing the nested map[string]map[string]*RecoveryPoint's
// vault-scoped lookup.
//
// VaultAccessPolicy, VaultLockConfig, and VaultNotificationConfig carried no
// field identifying which vault they belonged to (the vault name lived only
// in the map key) -- each gained a `VaultName string `json:"vaultName"`` field
// so store.Table's keyFn can derive a key from the value and it round-trips
// through persistence, mirroring services/ses's IdentityRecord/
// ConfigurationSet precedent.
//
// mpaApprovals, globalSettings and recoveryPointIndexStatus
// (map[string]string) and regionSettings (a single *RegionSettings pointer,
// not a collection) are deliberately left plain fields rather than
// store.Table: their values are not *T, so they do not fit store.Table's
// keyed-by-identity-value shape (mirroring ses's "policies" map). They are
// still real user state, so persistence.go carries all four as explicit
// backendSnapshot fields (gopherstack-y5b4) rather than via b.registry.
//
// vaultARNIndex, planARNIndex, planIDIndex, frameworkARNIndex and
// reportPlanARNIndex are derived lookup caches, not source state -- they are
// rebuilt from the restored tables by rebuildARNIndexes (persistence.go)
// rather than persisted.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func vaultKeyFn(v *Vault) string { return v.BackupVaultName }

func planKeyFn(v *Plan) string { return v.BackupPlanName }

func jobKeyFn(v *Job) string { return v.BackupJobID }

// selectionKey builds the composite "<planID>#<selectionID>" key shared by
// every backup selection nested under a plan. Access sites use this
// directly so the exact same key is used for Put/Get/Has/Delete.
func selectionKey(planID, selectionID string) string { return planID + "#" + selectionID }

func selectionKeyFn(v *Selection) string { return selectionKey(v.BackupPlanID, v.SelectionID) }

func frameworkKeyFn(v *Framework) string { return v.FrameworkName }

func legalHoldKeyFn(v *LegalHold) string { return v.LegalHoldID }

func reportPlanKeyFn(v *ReportPlan) string { return v.ReportPlanName }

func restoreAccessVaultKeyFn(v *RestoreAccessVault) string { return v.RestoreAccessBackupVaultName }

func restoreTestingPlanKeyFn(v *RestoreTestingPlan) string { return v.RestoreTestingPlanName }

// restoreTestingSelectionKey builds the composite "<planName>#<selectionName>"
// key shared by every restore testing selection nested under a plan.
func restoreTestingSelectionKey(planName, selectionName string) string {
	return planName + "#" + selectionName
}

func restoreTestingSelectionKeyFn(v *RestoreTestingSelection) string {
	return restoreTestingSelectionKey(v.RestoreTestingPlanName, v.RestoreTestingSelectionName)
}

// recoveryPointKey builds the composite "<vaultName>#<recoveryPointArn>" key
// shared by every recovery point nested under a vault.
func recoveryPointKey(vaultName, recoveryPointArn string) string {
	return vaultName + "#" + recoveryPointArn
}

func recoveryPointKeyFn(v *RecoveryPoint) string {
	return recoveryPointKey(v.BackupVaultName, v.RecoveryPointArn)
}

func copyJobKeyFn(v *CopyJob) string { return v.CopyJobID }

func vaultAccessPolicyKeyFn(v *VaultAccessPolicy) string { return v.VaultName }

func vaultLockConfigKeyFn(v *VaultLockConfig) string { return v.VaultName }

func vaultNotificationKeyFn(v *VaultNotificationConfig) string { return v.VaultName }

func restoreJobKeyFn(v *RestoreJob) string { return v.RestoreJobID }

func reportJobKeyFn(v *ReportJob) string { return v.ReportJobID }

func scanJobKeyFn(v *ScanJob) string { return v.ScanJobID }

func tieringConfigKeyFn(v *TieringConfiguration) string { return v.TieringConfigurationName }

func protectedResourceKeyFn(v *ProtectedResource) string { return v.ResourceArn }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through registry.ResetAll() instead.
func registerAllTables(b *InMemoryBackend) {
	b.vaults = store.Register(b.registry, "vaults", store.New(vaultKeyFn))
	b.plans = store.Register(b.registry, "plans", store.New(planKeyFn))
	b.jobs = store.Register(b.registry, "jobs", store.New(jobKeyFn))

	b.selections = store.Register(b.registry, "selections", store.New(selectionKeyFn))
	b.selectionsByPlan = b.selections.AddIndex(
		"byPlan", func(v *Selection) string { return v.BackupPlanID },
	)

	b.frameworks = store.Register(b.registry, "frameworks", store.New(frameworkKeyFn))
	b.legalHolds = store.Register(b.registry, "legalHolds", store.New(legalHoldKeyFn))
	b.reportPlans = store.Register(b.registry, "reportPlans", store.New(reportPlanKeyFn))
	b.restoreAccessVaults = store.Register(
		b.registry, "restoreAccessVaults", store.New(restoreAccessVaultKeyFn),
	)
	b.restoreTestingPlans = store.Register(
		b.registry, "restoreTestingPlans", store.New(restoreTestingPlanKeyFn),
	)

	b.restoreTestingSelections = store.Register(
		b.registry, "restoreTestingSelections", store.New(restoreTestingSelectionKeyFn),
	)
	b.restoreTestingSelectionsByPlan = b.restoreTestingSelections.AddIndex(
		"byPlan", func(v *RestoreTestingSelection) string { return v.RestoreTestingPlanName },
	)

	b.recoveryPoints = store.Register(b.registry, "recoveryPoints", store.New(recoveryPointKeyFn))
	b.recoveryPointsByVault = b.recoveryPoints.AddIndex(
		"byVault", func(v *RecoveryPoint) string { return v.BackupVaultName },
	)
	b.copyJobs = store.Register(b.registry, "copyJobs", store.New(copyJobKeyFn))
	b.vaultAccessPolicies = store.Register(
		b.registry, "vaultAccessPolicies", store.New(vaultAccessPolicyKeyFn),
	)
	b.vaultLockConfigs = store.Register(b.registry, "vaultLockConfigs", store.New(vaultLockConfigKeyFn))
	b.vaultNotifications = store.Register(
		b.registry, "vaultNotifications", store.New(vaultNotificationKeyFn),
	)
	b.restoreJobs = store.Register(b.registry, "restoreJobs", store.New(restoreJobKeyFn))
	b.reportJobs = store.Register(b.registry, "reportJobs", store.New(reportJobKeyFn))
	b.scanJobs = store.Register(b.registry, "scanJobs", store.New(scanJobKeyFn))
	b.tieringConfigs = store.Register(b.registry, "tieringConfigs", store.New(tieringConfigKeyFn))
	b.protectedResources = store.Register(
		b.registry, "protectedResources", store.New(protectedResourceKeyFn),
	)
}
