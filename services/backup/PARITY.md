---
service: backup
sdk_module: aws-sdk-go-v2/service/backup@v1.54.8
last_audit_commit: d56dc525
last_audit_date: 2026-07-12
overall: A            # ~450 LOC of genuine fixes found and applied; several routing families still open (see gaps)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  StartBackupJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "job now actually completes -- see families.BackupJob"}
  StopBackupJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unroutable; real path is POST /backup-jobs/{id}, not /backup-jobs/{id}/stop-backup-job"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unroutable; real path is POST /untag/{arn}, not DELETE /tags/{arn}"}
  DisassociateBackupVaultMpaApprovalTeam: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was unroutable; real path is POST (with ?delete) on the same /mpaApprovalTeam path as Associate"}
  GetRecoveryPointIndexDetails: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was unroutable (no route emitted this op at all); fixed path + vaultName wiring (was hardcoded \"\")"}
  UpdateRecoveryPointIndexSettings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was unroutable; fixed path + vaultName wiring"}
  UpdateRecoveryPointLifecycle: {wire: ok, errors: ok, state: ok, persist: partial, note: "was unroutable AND a disguised no-op (wrote to a side map nobody read); now mutates RecoveryPoint.Lifecycle/CalculatedLifecycle directly. RecoveryPoint table is VOLATILE (not persisted) -- see families.RecoveryPoint"}
  CreateRestoreAccessBackupVault: {wire: ok, errors: ok, state: ok, persist: ok, note: "method was POST, real AWS is PUT"}
  GetLegalHold: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unroutable (no GET case in parseLegalHoldRoute at all); error body now uses errResp so the SDK deserializes ResourceNotFoundException instead of UnknownError"}
  ListLegalHolds: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unroutable"}
  ListRecoveryPointsByLegalHold: {wire: partial, errors: ok, state: gap, note: "was unroutable, now routed; backend still returns [] unconditionally (legal-hold->RP association never tracked) -- pre-existing gap, not newly introduced"}
  GetRestoreTestingInferredMetadata: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was unroutable (/restore-testing/inferred-metadata doesn't share the /restore-testing/plans prefix)"}
  DescribeRestoreJob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was a disguised no-op: unknown job IDs returned a fabricated 200 COMPLETED body instead of 404 ResourceNotFoundException"}
  DescribeReportJob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fabricated-200 bug as DescribeRestoreJob, fixed"}
  DescribeScanJob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fabricated-200 bug, fixed"}
  DescribeProtectedResource: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fabricated-200 bug, fixed; see tests for the never-backed-up-resource case"}
  StartScanJob: {wire: ok, errors: n/a, state: ok, persist: n/a, note: "body field was BackupVaultArn (doesn't exist on the wire); real input is BackupVaultName. Now resolved to an ARN via DescribeBackupVault before storing"}
families:
  BackupVault: {status: ok, note: "CRUD, AccessPolicy, Notifications, Lock all verified against real paths/methods and already correct. mpaApprovalTeam Associate was already correct; Disassociate was unroutable (fixed)."}
  BackupPlan: {status: ok, note: "CRUD + versions + selections verified against real paths; already correct."}
  BackupSelection: {status: ok, note: "verified against real paths; already correct."}
  BackupJob: {status: ok, note: "StartBackupJob created jobs that stayed in CREATED forever -- CompleteBackupJob (state transition + recovery-point creation) existed as dead code, never called from anywhere. Janitor now advances CREATED jobs to COMPLETED on each sweep tick (mirrors services/batch's advanceJobs pattern), so StartBackupJob itself stays synchronous/CREATED (matching AWS) while background completion actually happens. StopBackupJob was unroutable (fixed)."}
  RecoveryPoint: {status: ok, note: "list/describe/delete/disassociate/restore-metadata routes were already correct. Index details/settings and lifecycle-update ops were entirely unroutable (no route emitted these op names) and, once reached directly, passed an empty vaultName to the backend. UpdateRecoveryPointLifecycle's write additionally went to a write-only side map (recoveryPointLifecycle) that nothing ever read -- DescribeRecoveryPoint never reflected the update. Fixed: real routes added, vaultName wiring fixed, backend now mutates RecoveryPoint.Lifecycle/CalculatedLifecycle directly, CalculatedLifecycle is now computed (MoveToColdStorageAfterDays/DeleteAfterDays -> timestamps from CreationDate) and serialized as epoch-seconds (was previously never populated, so the Go-default RFC3339 encoding bug on *time.Time was latent/undetected)."}
  Tags: {status: ok, note: "TagResource/ListTags correct. UntagResource was routed as DELETE /tags/{arn}; real AWS uses POST /untag/{arn} -- completely different path, so every real SDK client's UntagResource call 404'd. Fixed."}
  Framework: {status: ok, note: "CRUD verified against real paths; already correct."}
  ReportPlan: {status: ok, note: "CRUD verified against real paths; already correct."}
  RestoreTestingPlan: {status: ok, note: "CRUD + selections verified against real paths; already correct. GetRestoreTestingInferredMetadata was unroutable (fixed)."}
  LegalHold: {status: ok, note: "CreateLegalHold/CancelLegalHold were routed; GetLegalHold/ListLegalHolds/ListRecoveryPointsByLegalHold were never routed at all despite full handler code existing. Fixed."}
  RouteMatcher: {status: ok, note: "matchesBackupPath (the RouteMatcher gate -- see pkgs/service/registry.go, this is the ONLY thing that decides whether a request ever reaches this service's Handler) was missing /resources, /restore-jobs, /report-jobs(audit), /scan/jobs+/scan/job, /global-settings, /account-settings, /supported-resource-types, /tiering-configurations(prefix), /indexes/recovery-point, and /untag entirely. Unit tests never caught this because doREST() in the test helpers calls h.Handler() directly, bypassing RouteMatcher -- exactly the blind spot flagged in parity-principles.md #3. Fixed by adding the missing prefixes/exacts."}
  ReportJob/ScanJob/RegionSettings/TieringConfiguration/ProtectedResource-nested: {status: partial, note: "see gaps below -- path text was wrong (not just missing from RouteMatcher) for these families; fixed the ones needed to make StartReportJob/StartScanJob/DescribeRegionSettings/ListRecoveryPointsByResource/ListRestoreJobsByProtectedResource reachable at all, since those sit inside the top-priority families. TieringConfiguration's *data model* (keyed by TieringConfigurationName with nested BackupVaultName+ResourceSelection, vs gopherstack's current keyed-by-vault-name model) was left untouched -- out of scope for this pass, see gaps."}
gaps:
  - "TieringConfiguration backend data model doesn't match AWS: real API keys tiering configs by TieringConfigurationName (CreateTieringConfigurationInput.TieringConfiguration has BackupVaultName + ResourceSelection nested inside), gopherstack keys by vault name and has no TieringConfigurationName/ResourceSelection concept at all. Routing constant (pathTieringConf=\"/backup-vault-tiering\") is also NOT AWS's real path (\"/tiering-configurations\", \"/tiering-configurations/{Name}\"). Needs a backend redesign, not a routing patch -- deliberately left alone this pass."
  - "RestoreAccessVault List/Revoke real paths are NESTED under the source air-gapped vault (GET/DELETE /logically-air-gapped-backup-vaults/{BackupVaultName}/restore-access-backup-vaults[/{RestoreAccessBackupVaultArn}]), not the flat /restore-access-backup-vaults collection gopherstack currently uses for List/Revoke. Create (flat /restore-access-backup-vaults, now PUT) is fixed; List/Revoke are still unroutable against a real SDK client."
  - "ListRecoveryPointsByLegalHold backend always returns an empty slice -- legal-hold-to-recovery-point association is never tracked anywhere (CreateLegalHold takes no resource selectors in this emulator). Now at least routable; the empty-list behavior is a pre-existing simplification, not something this pass introduced or fixed."
  - "DescribeBackupVault omits MpaApprovalTeamArn/MpaSessionArn/LatestMpaApprovalTeamUpdate/EncryptionKeyType even though AssociateBackupVaultMpaApprovalTeam state is tracked (b.mpaApprovals) -- minor completeness gap, not a functional break."
deferred:
  - "CopyJob family (StartCopyJob/ListCopyJobs/DescribeCopyJob) -- routing verified correct against real SDK paths, but response-shape/error-code depth not re-audited this pass."
  - "RestoreJob family beyond DescribeRestoreJob (StartRestoreJob, ListRestoreJobs, GetRestoreJobMetadata, PutRestoreValidationResult) -- routing verified correct, response shapes not deep-audited."
  - "Framework/ReportPlan controls/settings nested shapes (FrameworkControl.ControlScope, ReportSetting) -- not wire-diffed against SDK this pass."
  - "Restore testing selection ProtectedResourceType/Conditions deep shape -- not wire-diffed this pass."
leaks: {status: clean, note: "Janitor's new advanceCreatedJobs takes the backend RLock, copies job IDs, releases, then calls CompleteBackupJob per ID (which takes its own Lock) -- no lock held across the loop, no goroutine leak. -race clean."}
---

## Notes

- **Protocol**: restjson1. Timestamps are epoch-seconds JSON numbers via the
  handler's local `epochSeconds()` helper (equivalent to `pkgs/awstime.Epoch`,
  just not routed through that package -- functionally correct, a style-only
  divergence from the catalog's preferred helper).
- **RouteMatcher is the real gate, not parseBackupPath.** `matchesBackupPath`
  (`Handler.RouteMatcher()`) decides whether `pkgs/service/registry.go` ever
  calls this service's `Handler()` at all. `parseBackupPath` can have perfectly
  correct logic for a path family and it will STILL never run if that family's
  prefix/exact isn't also listed in `matchesBackupPath`. This was the single
  biggest source of bugs this audit (see families.RouteMatcher above) and is
  invisible to this package's own unit tests because `doREST()`/`doBatch1Request()`
  call `h.Handler()(c)` directly, skipping the matcher entirely. **Any future
  audit that adds a new top-level path constant MUST also add it to
  `matchesBackupPath`'s `prefixes`/`exacts` slices, or it will silently never
  receive real traffic.**
- **AWS Backup's UntagResource path is `/untag/{ResourceArn}` (POST), not
  `/tags/{ResourceArn}` (DELETE).** Easy to get wrong by analogy with other
  services that DO use DELETE /tags/{arn} for untag -- Backup doesn't.
- **StopBackupJob shares its path with DescribeBackupJob**: `GET
  /backup-jobs/{BackupJobId}` describes, `POST /backup-jobs/{BackupJobId}`
  stops. There is no `.../stop-backup-job` suffix on the wire.
- **StartReportJob's path parameter is `ReportPlanName`, not a generic ID**:
  `POST /audit/report-jobs/{ReportPlanName}`. The same path shape
  (`/audit/report-jobs/{name}`) is reused for `DescribeReportJob` (GET, name =
  ReportJobId) -- distinguish purely by HTTP method.
- **StartScanJob's real path is the singular `/scan/job`** (PUT), separate
  from the plural `/scan/jobs` (GET list) and `/scan/jobs/{ScanJobId}` (GET
  describe). Do not conflate the two -- they really are different URIs in the
  smithy model.
- **DescribeRegionSettings/UpdateRegionSettings bind to `/account-settings`**,
  not `/region-settings`, despite the operation names. Confirmed directly from
  `serializers.go`'s `SplitURI` call for both ops.
- **CalculatedLifecycle must be epoch-seconds, not Go's default RFC3339.**
  Before this pass `CalculatedLifecycle` was always nil (UpdateRecoveryPointLifecycle
  never actually set it), so a latent bug -- passing the raw `*CalculatedLifecycle`
  struct straight to `c.JSON()`, which serializes `*time.Time` fields as RFC3339
  strings under Go's default `encoding/json` behavior -- was never observed by
  any test or client. Now that the field is actually populated, this is fixed
  via `calculatedLifecycleToJSON()`. **Trap for the next auditor**: any backend
  field of type `*time.Time` that isn't currently populated is a landmine --
  check its JSON serialization path even if it "looks unused."
- **Unit tests are not parity proof (again)**: every routing bug in this
  service was invisible to `go test ./services/backup/...` before this audit,
  because the test helpers call `h.Handler()(c)` directly and skip
  `RouteMatcher`/`matchesBackupPath` entirely. A green test suite here says
  nothing about whether a real `aws-sdk-go-v2` client could reach the
  operation at all.
