package backup

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/ec2
// (commit 12e611a4, data-driven registration slice), services/sqs (commit
// 0f09d77c, DTO-registry pilot), and services/ses (commit e9af4cc7,
// identity-less json:"-" key fields). See pkgs/store's package doc for the
// underlying primitive.
//
// Tables split into two groups:
//
//   - PERSISTED tables (vaults, plans, jobs, selections, frameworks,
//     legalHolds, reportPlans, restoreAccessVaults, restoreTestingPlans,
//     restoreTestingSelections) are registered on b.registry here, and
//     persistence.go drives them through b.registry.SnapshotAll() /
//     RestoreAll() directly. selections and restoreTestingSelections key off
//     a composite "<parentID>#<childID>" string built from fields the value
//     type already carries (Selection.BackupPlanID/SelectionID,
//     RestoreTestingSelection.RestoreTestingPlanName/
//     RestoreTestingSelectionName), replacing the nested
//     map[string]map[string]*T the backend used to hold; each also carries a
//     secondary store.Index grouping by the parent key, replacing the
//     nested map's direct lookup ("selections of plan X").
//
//   - VOLATILE tables (recoveryPoints, copyJobs, vaultAccessPolicies,
//     vaultLockConfigs, vaultNotifications, restoreJobs, reportJobs,
//     scanJobs, tieringConfigs, protectedResources) were NEVER part of
//     backendSnapshot before this refactor (see persistence.go's git
//     history) -- converting their storage to store.Table must not newly
//     start persisting them, so they are constructed with store.New but
//     deliberately NOT registered on b.registry, matching the
//     "constructed-but-excluded" pattern services/ses uses for its
//     emailsByID derived cache. recoveryPoints additionally carries a
//     secondary store.Index grouping by BackupVaultName, replacing the
//     nested map[string]map[string]*RecoveryPoint's vault-scoped lookup.
//
// VaultAccessPolicy, VaultLockConfig, and VaultNotificationConfig carried no
// field identifying which vault they belonged to (the vault name lived only
// in the map key) -- each gained a `VaultName string `json:"-"`` field
// purely so store.Table's keyFn can derive a key from the value, mirroring
// services/ses's IdentityRecord/ConfigurationSet precedent. The json:"-" tag
// is safe here specifically because none of these three tables are
// persisted (see above); a persisted identity-less type would need a DTO
// wrapper instead, as ses's Snapshot/Restore does for its own dirty tables.
//
// mpaApprovals (map[string]string, vaultName -> mpaApprovalTeamArn) and the
// remaining raw indexes/settings maps (vaultARNIndex, planARNIndex,
// planIDIndex, frameworkARNIndex, reportPlanARNIndex, globalSettings,
// recoveryPointLifecycle, recoveryPointIndexStatus, restoreValidations) are
// deliberately left plain maps: their values are plain strings, not *T, so
// they do not fit store.Table's keyed-by-identity-value shape (mirroring
// ses's "policies" map, left raw for the same reason). regionSettings is a
// single *RegionSettings pointer, not a collection, and is unaffected by
// this refactor.
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

func tieringConfigKeyFn(v *TieringConfiguration) string { return v.BackupVaultName }

func protectedResourceKeyFn(v *ProtectedResource) string { return v.ResourceArn }

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. PERSISTED tables are registered on
// b.registry; VOLATILE tables and their indexes are constructed alongside
// them but deliberately NOT registered -- see the file doc comment above.
// It must be called during construction only, never on every Reset(): store.Register
// panics on a duplicate name, so runtime resets go through Reset()'s
// individual Table.Reset() calls instead.
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

	b.recoveryPoints = store.New(recoveryPointKeyFn)
	b.recoveryPointsByVault = b.recoveryPoints.AddIndex(
		"byVault", func(v *RecoveryPoint) string { return v.BackupVaultName },
	)
	b.copyJobs = store.New(copyJobKeyFn)
	b.vaultAccessPolicies = store.New(vaultAccessPolicyKeyFn)
	b.vaultLockConfigs = store.New(vaultLockConfigKeyFn)
	b.vaultNotifications = store.New(vaultNotificationKeyFn)
	b.restoreJobs = store.New(restoreJobKeyFn)
	b.reportJobs = store.New(reportJobKeyFn)
	b.scanJobs = store.New(scanJobKeyFn)
	b.tieringConfigs = store.New(tieringConfigKeyFn)
	b.protectedResources = store.New(protectedResourceKeyFn)
}
