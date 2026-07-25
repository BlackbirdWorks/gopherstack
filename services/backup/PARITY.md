---
service: backup
sdk_module: aws-sdk-go-v2/service/backup@v1.59.0
last_audit_commit: 621eeacb
last_audit_date: 2026-07-25
overall: A            # all 4 prior gaps closed with real fixes + tests; all 4 prior deferred items field-diffed and closed; a service-wide error-code/HTTP-status bug found and fixed (see notes)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  StartBackupJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "job now actually completes -- see families.BackupJob"}
  StopBackupJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unroutable; real path is POST /backup-jobs/{id}, not /backup-jobs/{id}/stop-backup-job"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unroutable; real path is POST /untag/{arn}, not DELETE /tags/{arn}"}
  DisassociateBackupVaultMpaApprovalTeam: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was unroutable; real path is POST (with ?delete) on the same /mpaApprovalTeam path as Associate; responseCode 204 (was 200, fixed this pass)"}
  AssociateBackupVaultMpaApprovalTeam: {wire: ok, errors: ok, state: ok, persist: n/a, note: "responseCode 204 confirmed via botocore model's explicit http.responseCode -- was 200, fixed this pass"}
  GetRecoveryPointIndexDetails: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was unroutable (no route emitted this op at all); fixed path + vaultName wiring (was hardcoded \"\")"}
  UpdateRecoveryPointIndexSettings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was unroutable; fixed path + vaultName wiring"}
  UpdateRecoveryPointLifecycle: {wire: ok, errors: ok, state: ok, persist: partial, note: "was unroutable AND a disguised no-op (wrote to a side map nobody read); now mutates RecoveryPoint.Lifecycle/CalculatedLifecycle directly. RecoveryPoint table is VOLATILE (not persisted) -- see families.RecoveryPoint"}
  CreateRestoreAccessBackupVault: {wire: ok, errors: ok, state: ok, persist: ok, note: "method was POST, real AWS is PUT; SourceBackupVaultArn is now resolved against real vaults (ResourceNotFoundException if unresolvable) -- was previously stored verbatim with no validation"}
  ListRestoreAccessBackupVaults: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GAP CLOSED this pass -- was unroutable (op existed only as dead handler code on the flat /restore-access-backup-vaults collection, which is NOT the real path). Real path is GET /logically-air-gapped-backup-vaults/{BackupVaultName}/restore-access-backup-vaults, always scoped to one source vault; there is no list-all. Backend now tracks SourceBackupVaultName per restore-access vault and filters by it."}
  RevokeRestoreAccessBackupVault: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GAP CLOSED this pass -- same unroutable/wrong-path issue as List; real path is DELETE .../logically-air-gapped-backup-vaults/{name}/restore-access-backup-vaults/{arn}. Revoking a restore-access vault whose source vault name doesn't match the path is now correctly rejected (ResourceNotFoundException) instead of silently succeeding cross-vault."}
  GetLegalHold: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unroutable (no GET case in parseLegalHoldRoute at all); error body now uses errResp so the SDK deserializes ResourceNotFoundException instead of UnknownError"}
  ListLegalHolds: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unroutable"}
  CreateLegalHold: {wire: ok, errors: ok, state: ok, persist: ok, note: "GAP CLOSED this pass -- CreateLegalHoldInput.RecoveryPointSelection (DateRange/ResourceIdentifiers/VaultNames) was entirely absent from the model/wire parsing; now accepted and stored on the hold"}
  ListRecoveryPointsByLegalHold: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GAP CLOSED this pass -- backend previously returned [] unconditionally (association never tracked). CreateLegalHold now accepts a RecoveryPointSelection (VaultNames/ResourceIdentifiers/DateRange, matching real types.RecoveryPointSelection) and List now actually filters tracked recovery points against it. Wire response also fixed from a bare RecoveryPointArn to the real RecoveryPointMember shape (BackupVaultName/RecoveryPointArn/ResourceArn/ResourceType)."}
  DescribeBackupVault: {wire: ok, errors: ok, state: ok, persist: ok, note: "GAP CLOSED this pass -- now returns EncryptionKeyType (derived: CUSTOMER_MANAGED_KMS_KEY iff EncryptionKeyArn set, else AWS_OWNED_KMS_KEY) and MpaApprovalTeamArn (from b.mpaApprovals, already tracked but never surfaced). MpaSessionArn/LatestMpaApprovalTeamUpdate remain absent -- this backend has no MPA-session-approval-workflow state to source them from (see gaps)."}
  CreateTieringConfiguration: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GAP CLOSED this pass -- see families.TieringConfiguration for the full redesign"}
  DeleteTieringConfiguration: {wire: ok, errors: ok, state: ok, persist: n/a, note: "see families.TieringConfiguration"}
  GetTieringConfiguration: {wire: ok, errors: ok, state: ok, persist: n/a, note: "see families.TieringConfiguration"}
  ListTieringConfigurations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "see families.TieringConfiguration"}
  UpdateTieringConfiguration: {wire: ok, errors: ok, state: ok, persist: n/a, note: "see families.TieringConfiguration"}
  StartCopyJob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "DEFERRED ITEM CLOSED this pass -- SourceBackupVaultName (a NAME on the real wire) was passed straight into SourceBackupVaultArn with zero resolution/validation (silent data corruption for any real client); now resolved against real vaults (ResourceNotFoundException if either source name or destination ARN don't resolve), and the job now actually materializes a RecoveryPoint in the destination vault (previously a disguised no-op -- CopyJobId was returned but nothing was ever copied). DestinationRecoveryPointArn is now tracked and surfaced via DescribeCopyJob."}
  DescribeCopyJob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "wire response was missing AccountId/ResourceType/IamRoleArn (tracked in the model but silently dropped) and DestinationRecoveryPointArn (not tracked at all); both fixed"}
  ListCopyJobs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same missing-field fix as DescribeCopyJob, via the same copyJobToJSON helper"}
  StartRestoreJob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "DEFERRED ITEM CLOSED this pass -- RecoveryPointArn/IamRoleArn/Metadata are all required on the real wire and were previously unvalidated (a request missing all three silently 'succeeded'). Now validated (MissingParameterValueException). Also now enriches ResourceArn/BackupVaultName/BackupVaultArn/BackupSizeInBytes from the tracked source recovery point when known, and synthesizes CreatedResourceArn (real AWS provisions an actual new resource; this emulator cannot, so it fabricates a plausible ARN) -- both were entirely absent before."}
  DescribeRestoreJob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was a disguised no-op: unknown job IDs returned a fabricated 200 COMPLETED body instead of 404 ResourceNotFoundException (fixed prior pass). This pass: response wire shape extended with AccountId/BackupVaultArn/CreatedResourceArn/ValidationStatus/ValidationStatusMessage -- previously silently dropped or (for ValidationStatus) never wired at all, see PutRestoreValidationResult"}
  PutRestoreValidationResult: {wire: ok, errors: ok, state: ok, persist: n/a, note: "DISGUISED NO-OP FIXED this pass -- wrote ValidationStatus into a side map (b.restoreValidations) that NOTHING ever read; DescribeRestoreJob never reflected a validation result no matter how many times this was called. Side map deleted entirely; result now mutates the RestoreJob record directly (ValidationStatus + ValidationStatusMessage), and an unknown RestoreJobId now correctly returns ResourceNotFoundException instead of silently no-op'ing. responseCode 204 confirmed correct (unchanged)."}
  GetRestoreJobMetadata: {wire: ok, errors: ok, state: ok, persist: n/a, note: "unknown job ID silently returned an empty metadata map with 200 instead of ResourceNotFoundException; fixed"}
  DescribeReportJob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fabricated-200 bug as DescribeRestoreJob, fixed"}
  DescribeScanJob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fabricated-200 bug, fixed"}
  StartScanJob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "body field was BackupVaultArn (doesn't exist on the wire); real input is BackupVaultName. Now resolved to an ARN via DescribeBackupVault before storing. This pass: responseCode fixed from 200 to 201 (confirmed via botocore model)."}
  DescribeProtectedResource: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fabricated-200 bug, fixed; see tests for the never-backed-up-resource case"}
  GetRestoreTestingInferredMetadata: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was unroutable (/restore-testing/inferred-metadata doesn't share the /restore-testing/plans prefix)"}
  CreateRestoreTestingPlan: {wire: ok, errors: ok, state: ok, persist: ok, note: "responseCode fixed from 200 to 201 (confirmed via botocore model)"}
  DeleteRestoreTestingPlan: {wire: ok, errors: ok, state: ok, persist: ok, note: "responseCode fixed from 200 to 204 (confirmed via botocore model)"}
  CreateRestoreTestingSelection: {wire: ok, errors: ok, state: ok, persist: ok, note: "DEFERRED ITEM CLOSED this pass -- IamRoleArn (required on the real wire) was entirely absent from the model and unvalidated; ProtectedResourceType map[string]any-free-form ControlInputParameters-style bugs did NOT apply here (this family never had that bug), but ProtectedResourceArns/ProtectedResourceConditions (StringEquals/StringNotEquals []KeyValue)/RestoreMetadataOverrides/ValidationWindowHours were all missing from the model and wire parsing. All added, field-diffed against types.RestoreTestingSelectionForCreate. responseCode fixed from 200 to 201."}
  UpdateRestoreTestingSelection: {wire: ok, errors: ok, state: ok, persist: ok, note: "same field additions as Create; ProtectedResourceType is correctly left untouched on Update now (immutable per types.RestoreTestingSelectionForUpdate -- the prior implementation let it be silently changed, which real AWS does not allow)"}
  DeleteRestoreTestingSelection: {wire: ok, errors: ok, state: ok, persist: ok, note: "responseCode fixed from 200 to 204 (confirmed via botocore model)"}
  DisassociateRecoveryPointFromParent: {wire: ok, errors: ok, state: ok, persist: n/a, note: "responseCode fixed from 200 to 204 (confirmed via botocore model)"}
  CancelLegalHold: {wire: ok, errors: ok, state: ok, persist: ok, note: "responseCode fixed from 200 to 201 -- unusual for a DELETE but confirmed directly against the botocore service model's explicit http.responseCode"}
  CreateFramework: {wire: ok, errors: ok, state: ok, persist: ok, note: "DEFERRED ITEM CLOSED this pass -- ControlInputParameters was modeled/parsed as map[string]string; real wire shape is []ControlInputParameter ({ParameterName,ParameterValue} pairs). ControlScope was modeled as a free-form map[string]any; real wire shape is a struct {ComplianceResourceIds,ComplianceResourceTypes,Tags}. Both were WRONG SHAPES that would fail against any real aws-sdk-go-v2 client sending the real request shape, or silently mis-decode responses. Fixed, field-diffed against types.FrameworkControl/types.ControlScope."}
  UpdateFramework: {wire: ok, errors: ok, state: ok, persist: ok, note: "GAP CLOSED this pass -- FrameworkControls was not even accepted by UpdateFramework (real UpdateFrameworkInput.FrameworkControls lets you replace a framework's controls); now supported, omitted-field-means-unchanged"}
  CreateReportPlan: {wire: ok, errors: ok, state: ok, persist: ok, note: "ReportDeliveryChannel was missing S3KeyPrefix; ReportSetting was missing Accounts/OrganizationUnits/Regions/NumberOfFrameworks. All added, field-diffed against types.ReportDeliveryChannel/types.ReportSetting."}
  UpdateReportPlan: {wire: ok, errors: ok, state: ok, persist: ok, note: "GAP CLOSED this pass -- ReportDeliveryChannel/ReportSetting were not accepted by UpdateReportPlan at all (only description); real UpdateReportPlanInput accepts both. Now supported, omitted-field-means-unchanged."}
  GetPITRMalwareScanResults: {wire: partial, errors: ok, state: ok, persist: n/a, note: "NEW this pass (GET /scan/pitr-malware-scan-results, confirmed from serializers.go's awsRestjson1_serializeOpGetPITRMalwareScanResults path literal; all 4 input members -- BackupVaultName/MalwareScanner/RecoveryPointArn/ScanEndTime -- are query-string params per awsRestjson1_serializeOpHttpBindingsGetPITRMalwareScanResultsInput, not path segments or a JSON body, field-diffed against GetPITRMalwareScanResultsInput/Output and types.ScanResultInfo/ScanResultStatus). Real state validated: BackupVaultName resolved via DescribeBackupVault, RecoveryPointArn validated against that vault via DescribeRecoveryPoint -- both genuinely fail (400 ResourceNotFoundException, matching this service's uniform 400-for-not-found convention -- see errors.go) for an unknown vault or recovery point, not accepted verbatim. No malware scanning engine exists in this backend (GuardDuty malware-protection integration is out of scope/unmodeled), so ScanResult.ScanResultStatus is always the SDK's own 'UNKNOWN' enum value -- never a fabricated NO_THREATS_FOUND/THREATS_FOUND verdict, infected-file count, or threat name. ScanId/ScanMode/LastScanJobTime (all optional output members) are omitted entirely rather than populated with an invented ID/mode/timestamp. wire: partial reflects that these three optional members are never populated (by design, not oversight) rather than a genuine wire-shape defect -- ScanEndTime (required) and ScanResult (required) are both correctly present and accurate."}
families:
  BackupVault: {status: ok, note: "CRUD, AccessPolicy, Notifications, Lock all verified against real paths/methods and already correct. mpaApprovalTeam Associate/Disassociate both fixed to responseCode 204 this pass (see ops). DescribeBackupVault field-diffed and extended (EncryptionKeyType, MpaApprovalTeamArn) this pass."}
  BackupPlan: {status: ok, note: "CRUD + versions + selections verified against real paths; already correct."}
  BackupSelection: {status: ok, note: "verified against real paths; already correct."}
  BackupJob: {status: ok, note: "StartBackupJob created jobs that stayed in CREATED forever -- CompleteBackupJob (state transition + recovery-point creation) existed as dead code, never called from anywhere. Janitor now advances CREATED jobs to COMPLETED on each sweep tick (mirrors services/batch's advanceJobs pattern), so StartBackupJob itself stays synchronous/CREATED (matching AWS) while background completion actually happens. StopBackupJob was unroutable (fixed)."}
  RecoveryPoint: {status: ok, note: "list/describe/delete/disassociate/restore-metadata routes were already correct. Index details/settings and lifecycle-update ops were entirely unroutable (no route emitted these op names) and, once reached directly, passed an empty vaultName to the backend. UpdateRecoveryPointLifecycle's write additionally went to a write-only side map (recoveryPointLifecycle) that nothing ever read -- DescribeRecoveryPoint never reflected the update. Fixed: real routes added, vaultName wiring fixed, backend now mutates RecoveryPoint.Lifecycle/CalculatedLifecycle directly, CalculatedLifecycle is now computed (MoveToColdStorageAfterDays/DeleteAfterDays -> timestamps from CreationDate) and serialized as epoch-seconds. This pass: AddRecoveryPoint (public helper used by CopyJob/RestoreJob enrichment and tests) was found to never populate BackupVaultArn from the vault it's added to -- fixed."}
  Tags: {status: ok, note: "TagResource/ListTags correct. UntagResource was routed as DELETE /tags/{arn}; real AWS uses POST /untag/{arn} -- completely different path, so every real SDK client's UntagResource call 404'd. Fixed."}
  Framework: {status: ok, note: "CRUD verified against real paths. DEFERRED ITEM CLOSED this pass: FrameworkControl.ControlInputParameters/ControlScope were wrong wire shapes (map instead of array/struct); UpdateFramework didn't accept FrameworkControls at all. Both fixed and field-diffed against types.FrameworkControl/types.ControlScope -- see ops.CreateFramework/UpdateFramework."}
  ReportPlan: {status: ok, note: "CRUD verified against real paths. DEFERRED ITEM CLOSED this pass: ReportDeliveryChannel/ReportSetting were missing fields (S3KeyPrefix; Accounts/OrganizationUnits/Regions/NumberOfFrameworks) and UpdateReportPlan didn't accept either at all. Both fixed -- see ops.CreateReportPlan/UpdateReportPlan."}
  RestoreTestingPlan: {status: ok, note: "CRUD + selections verified against real paths. GetRestoreTestingInferredMetadata was unroutable (fixed prior pass). This pass: responseCodes fixed (Create 201, Delete 204) across plan+selection ops; RestoreTestingSelection DEFERRED ITEM CLOSED -- see ops.CreateRestoreTestingSelection."}
  LegalHold: {status: ok, note: "CreateLegalHold/CancelLegalHold were routed; GetLegalHold/ListLegalHolds/ListRecoveryPointsByLegalHold were never routed at all despite full handler code existing. Fixed prior pass. This pass: CreateLegalHold now accepts RecoveryPointSelection and ListRecoveryPointsByLegalHold actually filters by it (GAP CLOSED, was unconditional empty list); CancelLegalHold responseCode fixed 200->201."}
  RouteMatcher: {status: ok, note: "matchesBackupPath (the RouteMatcher gate -- see pkgs/service/registry.go, this is the ONLY thing that decides whether a request ever reaches this service's Handler) was missing /resources, /restore-jobs, /report-jobs(audit), /scan/jobs+/scan/job, /global-settings, /account-settings, /supported-resource-types, /tiering-configurations(prefix), /indexes/recovery-point, and /untag entirely. Fixed prior pass. This pass: /tiering-configurations path text itself was ALSO wrong (was \"/backup-vault-tiering\", a gopherstack-invented path -- fixed as part of the TieringConfiguration redesign) and /logically-air-gapped-backup-vaults now additionally routes the nested restore-access-vault sub-paths (already covered by the existing prefix, no RouteMatcher change needed there)."}
  TieringConfiguration: {status: ok, note: "GAP CLOSED this pass -- full backend redesign. Real API keys tiering configs by TieringConfigurationName (CreateTieringConfigurationInput.TieringConfiguration nests BackupVaultName+ResourceSelection inside), gopherstack previously keyed by vault name with no TieringConfigurationName/ResourceSelection concept at all -- a completely different (invented) data model. Routing path was also wrong (\"/backup-vault-tiering\" instead of the real \"/tiering-configurations\", \"/tiering-configurations/{Name}\"). Redesigned: TieringConfiguration now keyed by TieringConfigurationName, ResourceSelection ([]{ResourceType,Resources,TieringDownSettingsInDays}) validated (60-36500 day range, matching AWS docs), routing fixed (Create is PUT on the bare collection -- name lives in the body, not the URL -- Get/Update/Delete address by name in the path), CreatorRequestId idempotency added. Field-diffed against types.TieringConfiguration/TieringConfigurationInputForCreate/-ForUpdate/TieringConfigurationsListMember."}
  RestoreAccessVault: {status: ok, note: "GAP CLOSED this pass -- List/Revoke were routed against the WRONG (flat, invented) /restore-access-backup-vaults collection; real paths nest both under the source air-gapped vault (/logically-air-gapped-backup-vaults/{BackupVaultName}/restore-access-backup-vaults[/{arn}]), scoped per-source-vault (there is no list-all/revoke-any-vault op in the real API). Backend now tracks SourceBackupVaultName (resolved from the ARN at Create time) and both List and Revoke correctly scope/reject by it. Create's SourceBackupVaultArn is now validated against real vaults instead of stored verbatim."}
  CopyJob: {status: ok, note: "DEFERRED ITEM CLOSED this pass -- StartCopyJob's SourceBackupVaultName (wire: a NAME) was stored directly into the ARN field with zero resolution, and the 'copy' never actually created anything in the destination vault (CopyJobId was returned but DescribeRecoveryPoint against the destination vault would never see it -- a disguised no-op per parity-principles.md #2). Now: source name and destination ARN are both resolved/validated against real vaults, and a real RecoveryPoint is materialized in the destination vault with a tracked DestinationRecoveryPointArn. DescribeCopyJob/ListCopyJobs wire responses extended to surface AccountId/ResourceType/IamRoleArn/DestinationRecoveryPointArn (previously tracked-but-dropped or not tracked at all)."}
  RestoreJob: {status: ok, note: "DEFERRED ITEM CLOSED this pass -- StartRestoreJob accepted requests missing all of RecoveryPointArn/IamRoleArn/Metadata (all required on the real wire) with no validation. PutRestoreValidationResult was a disguised no-op (wrote to a side map, b.restoreValidations, that DescribeRestoreJob never read -- deleted the side map, wired the result directly onto the RestoreJob record). StartRestoreJob now also enriches from the tracked source recovery point and synthesizes CreatedResourceArn. DescribeRestoreJob/ListRestoreJobs wire responses extended with AccountId/BackupVaultArn/CreatedResourceArn/ValidationStatus/ValidationStatusMessage."}
gaps: []
  # All 4 gaps from the 2026-07-12 audit are now closed with real fixes + tests:
  #   TieringConfiguration data model -> families.TieringConfiguration
  #   RestoreAccessVault List/Revoke paths -> families.RestoreAccessVault
  #   ListRecoveryPointsByLegalHold empty-list -> ops.ListRecoveryPointsByLegalHold
  #   DescribeBackupVault missing MPA/EncryptionKeyType fields -> ops.DescribeBackupVault
  # New residual gap found and left open this pass (see below).
residual_gaps:
  - "DescribeBackupVault still omits MpaSessionArn and LatestMpaApprovalTeamUpdate. This backend's AssociateBackupVaultMpaApprovalTeam only ever stores an MpaApprovalTeamArn string (b.mpaApprovals map[string]string) -- there is no modeled MPA-session-approval workflow (session creation, approval status, expiry) anywhere in this service to source MpaSessionArn/LatestMpaApprovalTeamUpdate from. Populating them would mean fabricating session/approval state that isn't backed by any real API call in this emulator (CreateRestoreAccessBackupVault is MPA-adjacent but doesn't create an approval-team *session*) -- left genuinely open rather than invented. Real fix needs a broader MPA-session model, out of scope for a single-pass field-diff."
  - "ListBackupPlanVersions and ExportBackupPlanTemplate (backup_plans.go) silently swallow backend not-found errors and return an empty-but-200 response instead of propagating ResourceNotFoundException -- found while auditing this service's not-found-error-wrapping conventions (see notes below), NOT part of the original 4 gaps/4 deferred scope for this pass, left open."
  - "GetPITRMalwareScanResults has no malware scanning engine backing it (this emulator does not integrate with GuardDuty malware protection). ScanResultStatus is always 'UNKNOWN' and ScanId/ScanMode/LastScanJobTime are always absent -- an honest, documented limitation (see ops.GetPITRMalwareScanResults), not a hidden gap. Also: recovery points are not checked for continuous-backup/PITR eligibility (this backend has no EnableContinuousBackup-style flag on RecoveryPoint) -- a recovery point that would not actually support PITR in real AWS is still accepted here as long as it exists."
deferred: []
  # All 4 deferred items from the 2026-07-12 audit are now closed with real
  # fixes + tests (see the matching families/ops entries above):
  #   CopyJob response-shape/error-code depth -> families.CopyJob
  #   RestoreJob family beyond DescribeRestoreJob -> families.RestoreJob
  #   Framework/ReportPlan nested shapes -> families.Framework / families.ReportPlan
  #   RestoreTestingSelection deep shape -> ops.CreateRestoreTestingSelection
leaks: {status: clean, note: "Janitor's advanceCreatedJobs takes the backend RLock, copies job IDs, releases, then calls CompleteBackupJob per ID (which takes its own Lock) -- no lock held across the loop, no goroutine leak. This pass added no new goroutines/tickers; all new Lock()/RLock() call sites (TieringConfiguration CRUD, RestoreAccessVault List/Revoke, CopyJob/RestoreJob enrichment via findRecoveryPointByArn, PutRestoreValidationResult) follow the existing single-Lock-per-call, defer-Unlock, no-nested-backend-lock-calls pattern -- verified by reading each new method body. -race clean (go test -race -count=1)."}
---

## Notes

- **Protocol**: restjson1. Timestamps are epoch-seconds JSON numbers via the
  handler's local `epochSeconds()` helper (equivalent to `pkgs/awstime.Epoch`,
  just not routed through that package -- functionally correct, a style-only
  divergence from the catalog's preferred helper).
- **AWS Backup does NOT use 404/409 for not-found/conflict -- it's 400 for
  everything client-fault.** This was the single biggest finding this pass,
  verified against botocore's `backup/2018-11-15/service-2.json` (the
  authoritative AWS service model): none of Backup's client-fault exceptions
  (`ResourceNotFoundException`, `AlreadyExistsException`,
  `InvalidParameterValueException`, `MissingParameterValueException`,
  `InvalidRequestException`, `LimitExceededException`, `ConflictException`)
  carry an explicit `httpStatusCode` override in the model, so the restJson1
  protocol default of **400** applies uniformly to all of them. Compare e.g.
  Lambda's `ResourceNotFoundException`, which the same botocore model
  encodes with an explicit `"httpStatusCode": 404` -- Backup's has no such
  override. This service's `handleError()` previously returned 404 for
  not-found and 409 for already-exists, matching the *common* REST-JSON
  convention but not Backup's actual behavior. Fixed centrally in
  `handleError()`, plus ~90 direct (non-`handleError`-routed) call sites that
  hardcoded the same wrong assumption.
- **`ValidationException` is not a real AWS Backup error code** -- it does
  not appear anywhere in the service model. It was a gopherstack invention
  used at ~90 call sites. Deleted; replaced with the real generic codes
  `MissingParameterValueException` (message names a required field) and
  `InvalidParameterValueException` (everything else), both confirmed present
  in the real per-operation error lists. `errors.go`'s `ErrValidation`
  sentinel's label was updated to `InvalidParameterValueException`
  accordingly; a new `ErrInvalidRequest` sentinel (`InvalidRequestException`)
  was added for genuine state-conflict validation failures (e.g. deleting a
  non-empty or locked vault), distinct from malformed-parameter failures.
- **Several success responseCodes were wrong** (found via the same botocore
  model, which encodes explicit `http.responseCode` for the operations that
  deviate from the restJson1 default of 200): `AssociateBackupVaultMpaApprovalTeam`
  /`DisassociateBackupVaultMpaApprovalTeam`/`DisassociateRecoveryPointFromParent`
  /`DeleteRestoreTestingPlan`/`DeleteRestoreTestingSelection` are 204, not 200;
  `CreateRestoreTestingPlan`/`CreateRestoreTestingSelection`/`StartScanJob` are
  201, not 200; `CancelLegalHold` is (unusually, for a DELETE) 201, not
  200/204. All fixed. **Trap for the next auditor**: don't assume 200 for
  every 2xx response in this service -- check the botocore model's
  `operations.<Op>.http.responseCode` field explicitly per op.
- **Local "not found" error sentinels that don't wrap the shared `ErrNotFound`
  are a live bug class in this service, not just a style inconsistency.**
  Several ops (`GetTieringConfiguration`/`DeleteTieringConfiguration` before
  this pass, `DescribeRestoreJob`/`PutRestoreValidationResult` before this
  pass) used a locally-defined `errors.New("xxx not found")` sentinel instead
  of wrapping the shared `ErrNotFound`. Where the handler calls `h.handleError`
  (the normal path), an unwrapped local sentinel falls through to the
  `default` case and returns `500 InternalFailure` instead of the correct
  `400 ResourceNotFoundException` -- this was a real, live bug for
  `PutRestoreValidationResult`'s not-found path before this pass's fix (it
  hadn't been wired to call `handleError` at all yet, so it was latent, but
  would have surfaced the instant that wiring was added without also fixing
  the sentinel). Fixed the two op families above by wrapping `ErrNotFound`
  directly and removing the now-orphaned local sentinels
  (`errTieringConfigNotFound`, `errRestoreJobNotFound`). **Two more local
  sentinels of this shape remain** (`errRecoveryPointNotFound` appears
  unused/dead, `errBackupPlanNotFoundB1` is used by `ListBackupPlanVersions`/
  `ExportBackupPlanTemplate`, which don't call `handleError` at all --  they
  silently swallow the error into an empty-200 response instead, a *different*
  bug, see residual_gaps) -- not touched this pass, out of the 4+4 scope, but
  worth a dedicated look.
- **RouteMatcher is the real gate, not parseBackupPath.** `matchesBackupPath`
  (`Handler.RouteMatcher()`) decides whether `pkgs/service/registry.go` ever
  calls this service's `Handler()` at all. `parseBackupPath` can have perfectly
  correct logic for a path family and it will STILL never run if that family's
  prefix/exact isn't also listed in `matchesBackupPath`. This was the single
  biggest source of bugs in the 2026-07-12 audit and is invisible to this
  package's own unit tests because `doREST()`/`doBatch1Request()` call
  `h.Handler()(c)` directly, skipping the matcher entirely. **Any future
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
  Any backend field of type `*time.Time` that isn't currently populated is a
  landmine -- check its JSON serialization path even if it "looks unused."
  (Historical: this bit `CalculatedLifecycle` in the 2026-07-12 audit.)
- **StartCopyJob's SourceBackupVaultName vs DestinationBackupVaultArn
  asymmetry is real, not a typo** -- confirmed directly against
  `StartCopyJobInput` in the SDK: the source is addressed by NAME, the
  destination by ARN. Getting this backwards (treating both as ARNs, or both
  as names) is an easy mistake; this backend had exactly that bug
  (SourceBackupVaultName was stored straight into an ARN-typed field with no
  resolution) before this pass.
- **A "job started successfully" response with a real-looking ID is not
  proof the operation actually did anything** (parity-principles.md #2's
  "disguised stub" warning, generalized): `StartCopyJob` returned a
  populated `CopyJob` with COMPLETED state, but no recovery point was ever
  created in the destination vault -- any client polling
  `ListRecoveryPointsByBackupVault` on the destination after a "successful"
  copy would see nothing. `PutRestoreValidationResult` returned 204 (success)
  every time, but the result was written to a map nothing ever read. Both
  looked correct in isolation (right status code, right response shape) and
  both required tracing the write through to confirm nothing downstream
  actually consumed it.
- **Unit tests are not parity proof (again)**: every routing bug in this
  service in the 2026-07-12 audit was invisible to `go test ./services/backup/...`
  because the test helpers call `h.Handler()(c)` directly and skip
  `RouteMatcher`/`matchesBackupPath` entirely. A green test suite here says
  nothing about whether a real `aws-sdk-go-v2` client could reach the
  operation at all. This pass's error-code/HTTP-status fixes were verified
  against the authoritative botocore service model (not just re-reading this
  package's own code), which is the only way the wrong-404/wrong-409/
  wrong-responseCode/fake-`ValidationException` findings were caught --
  they were all internally self-consistent (this service's own tests
  asserted the wrong values right back at it) until compared against
  ground truth.

- **GetPITRMalwareScanResults is the one Backup GET operation whose entire
  input lives in the query string, not the path.** Every other GET in this
  service addresses its target with a URI path segment
  (`/backup-vaults/{name}`, `/legal-holds/{id}`, ...); this one has a fixed
  literal path (`/scan/pitr-malware-scan-results`, no `{...}` segments at
  all) and binds `BackupVaultName`/`MalwareScanner`/`RecoveryPointArn`/
  `ScanEndTime` as query parameters instead (confirmed against
  `awsRestjson1_serializeOpHttpBindingsGetPITRMalwareScanResultsInput`).
  `handleGetPITRMalwareScanResults` reads `c.Request().URL.Query()`
  directly rather than using `route.resource`. There is no real malware
  scanner behind this handler (see gaps) -- `ScanResultStatus` is always
  `"UNKNOWN"`, one of the three real enum values, meaning exactly
  "no determination available," not a disguised clean/infected claim.
