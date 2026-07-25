service: quicksight
sdk_module: aws-sdk-go-v2/service/quicksight@v1.112.0
last_audit_commit: 9dea467e
last_audit_date: 2026-07-22
overall: A            # full-surface pass: all named gaps fixed, every deferred family
                      # confirmed real (no stubs), 13 banned complexity nolints removed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateDataSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "was fabricating IngestionArn/IngestionId=\"auto\" for every ImportMode; fixed to only report an ingestion (a real, describable backend Ingestion record) when ImportMode is SPICE, matching CreateDataSetOutput's documented semantics"}
  DescribeDataSet: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now mirrors CreateDataSet -- when the resulting ImportMode is SPICE, UpdateDataSet creates a real, describable storedIngestion and reports IngestionArn/IngestionId in the response; omitted for DIRECT_QUERY. See TestQuickSight_DataSets/UpdateDataSet_on_{SPICE,DIRECT_QUERY}_dataset_*"}
  DeleteDataSet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataSets: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchDataSets: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDataSetPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataSetPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataSources: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchDataSources: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDataSourcePermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataSourcePermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateIngestion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Arn was hand-formatted with a hardcoded \"aws\" partition instead of pkgs/arn.Build; fixed -- also brings GovCloud/China region parity in line with every other resource type in this backend"}
  DescribeIngestion: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelIngestion: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now rejects cancelling an ingestion already in a terminal state (COMPLETED/FAILED/CANCELLED) with ErrIngestionNotCancellable (ConflictException, 409) instead of silently overwriting its status; the SDK doc comment gives no explicit error name for this case, so ConflictException was chosen to match this backend's existing errConflictException convention (see ErrIngestionAlreadyExists). See TestQuickSight_CancelIngestion_CompletedAutoIngestion"}
  ListIngestions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDashboard: {wire: ok, errors: ok, state: ok, persist: ok, note: "Status/CreationStatus was the invalid ResourceStatus literal \"CREATED\"; fixed to CREATION_SUCCESSFUL (the only enum value SDK clients round-trip through types.ResourceStatus)"}
  DescribeDashboard: {wire: ok, errors: ok, state: ok, persist: ok, note: "dashboardToMap's PublishedVersionNumber was reading d.VersionNumber, not d.PublishedVersionNumber -- so calling UpdateDashboardPublishedVersion never showed up in Describe/List; fixed"}
  UpdateDashboard: {wire: ok, errors: ok, state: ok, persist: ok, note: "response was missing CreationStatus entirely (UpdateDashboardOutput has one) and the backend never transitioned Status on update; fixed both"}
  DeleteDashboard: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDashboards: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDashboardVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "synthesized version Status also carried the invalid \"CREATED\" literal; fixed alongside CreateDashboard"}
  SearchDashboards: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDashboardPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDashboardPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDashboardPublishedVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDashboardLinks: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDashboardDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceStatus field reuses Dashboard.Status, fixed by the same CREATED->CREATION_SUCCESSFUL change"}
  CreateAnalysis: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAnalysis: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAnalysis: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAnalysis: {wire: ok, errors: ok, state: ok, persist: ok, note: "soft-delete (Status=DELETED) vs hard-delete on forceDeleteWithoutRecovery correctly mirrors RestoreAnalysis existing as a real op"}
  ListAnalyses: {wire: ok, errors: ok, state: ok, persist: ok}
  RestoreAnalysis: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchAnalyses: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAnalysisPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAnalysisPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  ListNamespaces: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified this pass: group.go's DeleteGroup already deletes every groupMembers row under that group's key prefix (was already fixed by the time of this audit, despite the stale gap note from the prior pass) -- locked with TestQuickSight_GroupMemberships/DeleteGroup_also_removes_its_memberships"}
  ListGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateGroupMembership: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeGroupMembership: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGroupMembership: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroupMemberships: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUser: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "left ghost groupMembers rows referencing the deleted user forever (ListGroupMemberships/ListUserGroups kept surfacing them); fixed -- removeUserFromAllGroups() now runs on delete"}
  DeleteUserByPrincipalId: {wire: ok, errors: ok, state: ok, persist: ok, note: "same ghost-membership bug as DeleteUser, same fix"}
  ListUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUserGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now checks InMemoryBackend.arnExists(resourceARN) (a data-driven scan over every independently-taggable resource family's live ARNs) before writing, returning ErrTaggableResourceNotFound (ResourceNotFoundException, 404) for an ARN this backend doesn't hold. Same fix applied to UntagResource/ListTagsForResource. See TestQuickSight_Tags_UnknownARN"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  # Every family below was audited this pass by (1) reading handler_dispatch.go's
  # exhaustive per-op routing comments, which enumerate exactly which backend method
  # backs every op in every family and confirm none are canned/stub responses (the one
  # true exception, UpdateApplicationWithTokenExchangeGrant, is a genuinely void-result
  # op per its SDK doc comment -- no Describe/Get op exists for it, so there is no state
  # to fabricate), and (2) spot-checking wire shapes for each family's core
  # Create/Describe/List op against aws-sdk-go-v2/service/quicksight/types. One real gap
  # was found and fixed (Folder.SharingModel, below); no other missing/incorrect fields
  # were found in the families spot-checked in full depth (Folder, VPCConnection,
  # CustomPermissions, Brand, AccountLevel, Embed). Families not independently
  # field-by-field diffed against the SDK this pass (Template, Theme, Topic,
  # IAMPolicyAssignment, RefreshSchedule, OAuthClientApplication, ActionConnector,
  # IdentityPropagationConfig, AssetBundle, Automation, DashboardSnapshotJob, Flow,
  # SelfUpgrade) are marked ok on the strength of the no-stub confirmation plus their
  # existing test coverage (handler_<family>_test.go, all green); a residual field-level
  # gap analogous to Folder.SharingModel is possible but not known.
  Folder: {status: ok, note: "CRUD + membership + permissions real (folders.go, handler_folders.go); found+fixed a genuine gap this pass: Folder.SharingModel was never tracked/returned (real DescribeFolderOutput.Folder.SharingModel silently dropped) -- CreateFolder now accepts SharingModel, defaults to ACCOUNT per CreateFolderInput's doc comment when omitted, and folderToMap returns it. See TestQuickSight_FolderCRUD/DescribeFolder_returns_folder and .../CreateFolder_omitted_SharingModel_defaults_to_ACCOUNT"}
  Template: {status: ok, note: "CRUD + versions/aliases/permissions real (templates.go, handler_templates.go); classifyTemplateAlias decomposed from a flagged nolint this pass, behavior preserved verbatim including DeleteTemplateAlias's id-not-alias quirk (locked in handler_paths_test.go)"}
  Theme: {status: ok, note: "CRUD + versions/aliases/permissions real (themes.go, handler_themes.go); classifyThemeAlias decomposed from a flagged nolint this pass, same DeleteThemeAlias id-not-alias quirk preserved and locked"}
  Topic: {status: ok, note: "CRUD + permissions + refresh schedules/reviewed answers real (topics.go, handler_topics.go); classifyTopicPaths decomposed from a flagged nolint this pass, behavior preserved verbatim"}
  VPCConnection: {status: ok, note: "CRUD real (vpcconnections.go); spot-checked against types.VPCConnection -- NetworkInterfaces (AWS-populated once the VPC connection succeeds) is not modeled, a safe omission (no fabrication) consistent with this backend having no real VPC provisioning to report on, not a fabricated field"}
  IAMPolicyAssignment: {status: ok, note: "CRUD + list-for-user real (iampolicyassignments.go, handler_iampolicyassignments.go)"}
  CustomPermissions: {status: ok, note: "CRUD + role membership + role/user custom-permission sub-families real (custompermissions.go, handler_custompermissions.go); spot-checked against types.CustomPermissions -- fields match exactly"}
  RefreshSchedule: {status: ok, note: "DataSet refresh-schedule + refresh-properties CRUD real (refreshschedule.go, handler_refreshschedule.go); classifyDataSetSubRes/SubResID decomposed from classifyDataSetPaths's flagged nolint this pass, behavior preserved verbatim"}
  AccountLevel: {status: ok, note: "large family: customizations, settings, subscription, IP restriction, key registration, public sharing, Q personalization/search config, SPICE capacity, default Q Business app, token-exchange grant, identity context, PredictQAResults (account.go, handler_account.go) -- all real; spot-checked AccountSettings/AccountInfo against SDK types, fields match; dispatchAccountConfig's flat switch decomposed into a sync.OnceValue map[op]handler-method table this pass to remove its cyclop nolint"}
  Embed: {status: ok, note: "GenerateEmbedUrlFor*, GetSessionEmbedUrl, GetDashboardEmbedUrl, GenerateIdentityContext (embedurl.go) -- all real: every op validates the referenced namespace/user/dashboard actually exists before minting a URL/token, and each URL/token is freshly generated per call (matching real AWS's single-use, time-limited embed URLs) rather than a canned constant"}
  Brand: {status: ok, note: "CRUD + assignment + published-version real (brands.go, handler_brands.go); spot-checked against types.BrandDetail, fields match"}
  OAuthClientApplication: {status: ok, note: "CRUD real (oauth.go, handler_oauth.go)"}
  ActionConnector: {status: ok, note: "CRUD + search + permissions real (actionconnector.go, handler_actionconnector.go)"}
  IdentityPropagationConfig: {status: ok, note: "list/update/delete real (identitypropagation.go, handler_identitypropagation.go)"}
  AssetBundle: {status: ok, note: "export/import job lifecycle real (assetbundle.go, handler_assetbundle.go)"}
  Automation: {status: ok, note: "StartAutomationJob/DescribeAutomationJob real (automation.go, handler_automation.go)"}
  DashboardSnapshotJob: {status: ok, note: "StartDashboardSnapshotJob(Schedule)/Describe*Result real (dashboardsnapshot.go, handler_assetbundle.go); classifyDashboardSubRes/SubResID/SubSubRes decomposed from classifyDashboardPaths's flagged nolint this pass, behavior preserved verbatim"}
  Flow: {status: ok, note: "ListFlows/SearchFlows/GetFlowMetadata/permissions real (flow.go, handler_flow.go); no CreateFlow in the real SDK either (flows are console/Quick-Suite-authored), so SeedFlow test helper is the only way to seed fixtures -- matches real AWS, not a gap"}
  SelfUpgrade: {status: ok, note: "config + request list/update real (selfupgrade.go, handler_selfupgrade.go); classifyNsSelfUpgradeConfig/Requests/UpdateSelfUpgrade decomposed from classifyNsWithSubRes's flagged nolint this pass"}
gaps: []
  # All 5 previously-named gaps fixed this pass (UpdateDataSet ingestion reporting,
  # CancelIngestion terminal-status handling, Tag/Untag/ListTags ARN existence check --
  # DeleteGroup's groupMembers cleanup was re-verified as already fixed, not a live gap).
  # One new gap found+fixed during the deferred-family audit: Folder.SharingModel was
  # never tracked/returned; see families.Folder above.
deferred: []
  # All 19 previously-deferred families audited this pass; see families above. None
  # remain deferred.
leaks: {status: clean, note: "no goroutines/timers/janitors found in this service -- it's a synchronous in-memory backend behind a single coarse lockmetrics.RWMutex. DeleteUser's groupMembers cleanup (fixed prior pass) and DeleteGroup's groupMembers cleanup (re-verified this pass, already correct) both cascade-clean group membership rows on delete. DeleteFolder cascade-cleans folderMembers rows the same way. No ghost rows found in any family audited this pass."}

---

## Notes

Protocol: **REST-JSON (restjson1)**, not action-header dispatch -- routing is by HTTP
method + URL path (`classifyRequest` in handler.go), unlike most gopherstack services
that dispatch on an `X-Amz-Target`-style op header. `GetSupportedOperations()` still
enumerates the full op catalog for chaos-injection wiring.

Timestamps: all `CreatedTime`/`LastUpdatedTime` fields go over the wire as
`.Unix()` epoch-seconds numbers (correct for this JSON protocol) rather than via
`pkgs/awstime.Epoch` -- functionally equivalent, but worth normalizing to the shared
helper in a future pass for consistency with other services' bug history.

`Status`/`CreationStatus`/`UpdateStatus`/`ResourceStatus` fields all share the real
SDK's seven-value `types.ResourceStatus` enum: `CREATION_IN_PROGRESS`,
`CREATION_SUCCESSFUL`, `CREATION_FAILED`, `UPDATE_IN_PROGRESS`, `UPDATE_SUCCESSFUL`,
`UPDATE_FAILED`, `DELETED`. This backend only ever synthesizes the terminal-success
values (`*_SUCCESSFUL`) or `DELETED`, never the `_IN_PROGRESS`/`_FAILED` states, which
is fine for parity (this backend has no async failure modes) but means a client
polling for `CREATION_IN_PROGRESS` to flip will never observe it -- everything is
synchronously done. Before this pass, Dashboard alone used the invalid literal
`"CREATED"` for this field family; that's fixed now, so **all** resource types
consistently use only real enum values. Don't reintroduce a bespoke "CREATED" string.

`CreateDataSetOutput`/`UpdateDataSetOutput` both document `IngestionArn`/`IngestionId`
as "triggered as a result of dataset creation if the import mode is SPICE" -- i.e.
these fields are conditional on ImportMode, not unconditional. Before this pass,
`CreateDataSet` fabricated `IngestionArn: "{arn}/ingestion/auto"` /
`IngestionId: "auto"` unconditionally, for every import mode, without ever creating a
backing `Ingestion` record -- a classic disguised-no-op (see parity-principles.md
rule 1: fabricated IDs that skip real state). A client calling
`DescribeIngestion(dataSetId, "auto")` right after `CreateDataSet` would get a 404
despite the create response claiming that ingestion existed. Fixed by having
`CreateDataSet` create a real `storedIngestion` (status `COMPLETED`, since this
backend has no async pipeline) when, and only when, `ImportMode == "SPICE"`, and
omitting `IngestionArn`/`IngestionId` entirely for `DIRECT_QUERY`.

ARN construction: every resource type in this backend builds ARNs via
`pkgs/arn.Build` (partition derived from region -- GovCloud/China/ISO-correct)
**except** `CreateIngestion`, which used to hand-format
`fmt.Sprintf("arn:aws:quicksight:%s:%s:dataset/%s/ingestion/%s", ...)` with a
hardcoded `"aws"` partition. Fixed to use `arn.Build` like every other resource.
Grep for `fmt.Sprintf("arn:` before adding new resource types to catch regressions.

`dashboardToMap()` (handler.go) is the single place that flattens a `*Dashboard` for
`DescribeDashboard`/`ListDashboards` JSON responses; it had a copy-paste bug reading
`d.VersionNumber` into the `PublishedVersionNumber` wire key instead of
`d.PublishedVersionNumber` -- the two fields diverge as soon as
`UpdateDashboardPublishedVersion` is called with anything other than the latest
version, or `UpdateDashboard` bumps `VersionNumber` without a matching publish. Fixed.

Group membership storage (`b.groupMembers map[string]bool`, not a `store.Table`) is
keyed `"{accountID}/{namespace}/{groupName}/{memberName}"` with no escaping -- this is
safe only because namespace/group/member names are assumed not to contain literal
`/` characters, consistent with every other composite key in this file (`userKey`,
`dataSourceKey`, etc.). Don't add resource names with `/` without revisiting all of
these key builders.
