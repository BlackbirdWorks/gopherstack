service: quicksight
sdk_module: aws-sdk-go-v2/service/quicksight@v1.112.0
last_audit_commit: 5256fdde
last_audit_date: 2026-07-12
overall: A            # genuine fixes found this pass (fresh audit, first PARITY.md)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateDataSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "was fabricating IngestionArn/IngestionId=\"auto\" for every ImportMode; fixed to only report an ingestion (a real, describable backend Ingestion record) when ImportMode is SPICE, matching CreateDataSetOutput's documented semantics"}
  DescribeDataSet: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataSet: {wire: partial, errors: ok, state: ok, persist: ok, note: "real UpdateDataSetOutput also carries IngestionArn/IngestionId when the update itself triggers a new SPICE ingestion (e.g. import-mode or schema change); this backend never triggers/reports one on update -- omission is safe (no fabrication) but incomplete, see gaps"}
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
  CancelIngestion: {wire: partial, errors: ok, state: ok, persist: ok, note: "unconditionally overwrites IngestionStatus to CANCELLED even if already COMPLETED/FAILED/CANCELLED; real AWS likely rejects cancelling a terminal-state ingestion -- not fixed this pass, exact error semantics unverified against SDK doc comments, see gaps"}
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
  DeleteGroup: {wire: partial, errors: ok, state: partial, persist: ok, note: "does not clean up groupMembers rows for the deleted group (mirrors the DeleteUser gap fixed this pass, but for the group side); ListGroupMemberships on a re-created group of the same name would resurface stale members -- gaps"}
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
  TagResource: {wire: ok, errors: partial, state: ok, persist: ok, note: "accepts tags for any ARN string with no check that the resource actually exists; real AWS returns ResourceNotFoundException for an unknown resource ARN -- not fixed this pass (see gaps; same leniency exists for UntagResource/ListTagsForResource)"}
  UntagResource: {wire: ok, errors: partial, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: partial, state: ok, persist: ok}
families:
  Folder: {status: deferred, note: "not audited this pass -- CRUD + membership + permissions surface exists (backend_folders.go, handler_folders.go)"}
  Template: {status: deferred, note: "not audited this pass -- includes versions/aliases/permissions (backend_templates.go, handler_templates.go)"}
  Theme: {status: deferred, note: "not audited this pass -- includes versions/aliases/permissions (backend_themes.go, handler_themes.go)"}
  Topic: {status: deferred, note: "not audited this pass -- includes refresh schedules/reviewed answers (backend_topics.go, handler_topics.go)"}
  VPCConnection: {status: deferred, note: "not audited this pass (backend_vpcconnections.go)"}
  IAMPolicyAssignment: {status: deferred, note: "not audited this pass (backend_iampolicyassignments.go)"}
  CustomPermissions: {status: deferred, note: "not audited this pass, includes role-membership + role/user custom-permission sub-families (backend_custompermissions.go)"}
  RefreshSchedule: {status: deferred, note: "DataSet refresh-schedule + refresh-properties CRUD not audited this pass (backend_refreshschedule.go)"}
  AccountLevel: {status: deferred, note: "large family: customizations, settings, subscription, IP restriction, key registration, public sharing, Q personalization/search config, SPICE capacity, default Q Business app, token-exchange grant, identity context, PredictQAResults (backend_account.go, handler_account.go) -- not audited this pass"}
  Embed: {status: deferred, note: "GenerateEmbedUrlFor*, GetSessionEmbedUrl (backend_embedurl.go) -- not audited this pass"}
  Brand: {status: deferred, note: "not audited this pass (backend_brands.go)"}
  OAuthClientApplication: {status: deferred, note: "not audited this pass (backend_oauth.go)"}
  ActionConnector: {status: deferred, note: "not audited this pass (backend_actionconnector.go)"}
  IdentityPropagationConfig: {status: deferred, note: "not audited this pass (backend_identitypropagation.go)"}
  AssetBundle: {status: deferred, note: "export/import job lifecycle not audited this pass (backend_assetbundle.go)"}
  Automation: {status: deferred, note: "StartAutomationJob/DescribeAutomationJob not audited this pass (backend_automation.go)"}
  DashboardSnapshotJob: {status: deferred, note: "StartDashboardSnapshotJob(Schedule)/Describe*Result not audited this pass (backend_dashboardsnapshot.go)"}
  Flow: {status: deferred, note: "ListFlows/SearchFlows/GetFlowMetadata/permissions not audited this pass (backend_flow.go)"}
  SelfUpgrade: {status: deferred, note: "not audited this pass (backend_selfupgrade.go)"}
gaps:
  - "UpdateDataSet never triggers/reports a new SPICE ingestion (IngestionArn/IngestionId always omitted from UpdateDataSetOutput, even when import mode or schema effectively changes) -- omission is safe (no fabrication) but incomplete vs real AWS"
  - "CancelIngestion unconditionally sets IngestionStatus=CANCELLED regardless of current status; real AWS behavior for cancelling an already-terminal ingestion is unverified from SDK doc comments alone"
  - "DeleteGroup does not clean up groupMembers rows for that group (same class of bug as the DeleteUser ghost-membership issue fixed this pass, but on the group side) -- ListGroupMemberships on a same-named recreated group would resurface stale members"
  - "TagResource/UntagResource/ListTagsForResource accept any ARN string with no existence check against real backend resources; real AWS returns ResourceNotFoundException for unknown resource ARNs"
  - "Large swaths of the surface (see families: deferred above) were not audited this pass -- scope was capped to the highest-traffic families named in the audit brief (DataSet, DataSource, Dashboard, Analysis, User/Group, Ingestion, Tags)"
deferred:
  - Folder
  - Template
  - Theme
  - Topic
  - VPCConnection
  - IAMPolicyAssignment
  - CustomPermissions (+ role membership, role/user custom permission)
  - RefreshSchedule (DataSet refresh schedules/properties)
  - AccountLevel (customizations, settings, subscription, IP restriction, key registration, public sharing, Q config, SPICE capacity config, default Q Business app, token-exchange grant, identity context, PredictQAResults)
  - Embed (GenerateEmbedUrlFor*, GetSessionEmbedUrl)
  - Brand
  - OAuthClientApplication
  - ActionConnector
  - IdentityPropagationConfig
  - AssetBundle (export/import jobs)
  - Automation
  - DashboardSnapshotJob
  - Flow
  - SelfUpgrade
leaks: {status: clean, note: "no goroutines/timers/janitors found in this service -- it's a synchronous in-memory backend behind a single coarse lockmetrics.RWMutex; the one map-cleanup leak found (DeleteUser leaving ghost groupMembers rows) was fixed this pass. A sibling leak (DeleteGroup not cleaning groupMembers) is filed under gaps, not fixed, to stay within this pass's DataSet/DataSource/Dashboard/Analysis/User-Group/Ingestion/Tags scope."}

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
