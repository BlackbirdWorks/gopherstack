---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: ssm
sdk_module: aws-sdk-go-v2/service/ssm@v1.73.4
last_audit_commit: 02bc086d
last_audit_date: 2026-07-24
overall: A                 # parity-3 PHASE 2 (2026-07-24): closed every remaining gap from the
                            # 2026-07-23 A- pass. State Manager associations (CreateAssociationInput
                            # AND UpdateAssociationInput AND CreateAssociationBatchRequestEntry) now
                            # carry all 11 previously-missing fields (bd gopherstack-ouvq, closed).
                            # OpsCenter (CreateOpsItemInput/UpdateOpsItemInput) now carries all 7
                            # previously-missing Change-Manager fields (bd gopherstack-iq4m, closed).
                            # PatchBaseline.ApprovedPatchesEnableNonSecurity converted bool -> *bool
                            # across Create/Update/PatchBaseline so UpdatePatchBaseline can explicitly
                            # turn the flag off, not just on. NoChangeNotification/ExpirationNotification
                            # parameter policies are now actually evaluated by a new janitor sweep and
                            # emit real events through an injectable ParameterPolicyNotifier (see
                            # families.parameter-store and Notes) -- the EventBridge-side adapter
                            # (services/eventbridge/ssm_integration.go) is implemented and proven via
                            # a cross-package test, only the cli.go injection line remains (see
                            # Notes: "cli.go wiring still needed"). ListCloudConnectors/
                            # ValidateCloudConnector's MaxResults bound is no longer a guess -- AWS
                            # published the real API reference pages (API_ListCloudConnectors.html /
                            # API_ValidateCloudConnector.html) sometime between the 2026-07-23 and
                            # 2026-07-24 passes; re-checked and confirmed present this pass (Minimum 0/
                            # Maximum 10 for List, Minimum 0/Maximum 75 for Validate), now enforced
                            # with a real ValidationException instead of silently accepting any value.
                            # ValidateCloudConnector's inability to make a real outbound Azure call
                            # remains a genuine, structural sandbox impossibility (see gaps) -- not
                            # closed, and cannot be. Prior pass's headline epoch-seconds fix and
                            # Sessions/PatchBaselines/MaintenanceWindows re-verification carried
                            # forward unchanged (files not touched this pass).
                            # --- history below: parity-sweep-3 (2026-07-23) pass notes, preserved ---
                            # THIS PASS (parity-sweep-3, worker=ssm): closed all 5 previously-deferred
                            # families and re-verified the 4 previously-open gaps. Headline finding:
                            # a systemic wire-shape bug affecting ~9 structs across 6 files -- every
                            # raw `time.Time`/`*time.Time` JSON field (AssociationExecution.ExecutionDate,
                            # MaintenanceWindowExecution*.StartTime/EndTime x4 structs,
                            # InstanceInformation/NodeInfo/InstanceAssociationStatusInfo.RegistrationDate/
                            # ExecutionDate, InstancePatchState.OperationStartTime,
                            # PatchComplianceData.InstalledTime, ResourceDataSync.SyncCreatedTime/
                            # LastSyncTime, InventoryDeletion.DeletionStartTime) was serializing as a
                            # Go-default RFC3339Nano *string*, but real AWS SSM (awsjson1.1) always
                            # encodes DateTime as an epoch-seconds JSON *number*
                            # (smithytime.ParseEpochSeconds, confirmed directly in aws-sdk-go-v2's
                            # deserializers.go for every one of these fields) -- a real aws-sdk-go-v2
                            # client would fail to deserialize these responses. Fixed by converting
                            # every affected field to float64 (UnixTimeFloat), this package's existing,
                            # already-correct convention for every other timestamp. Also: Sessions
                            # (deferred) fully re-verified and fixed (see families.sessions); Patch
                            # baselines and Maintenance windows re-verified with real field-diff fixes;
                            # State Manager associations and OpsCenter spot-checked with one real fix
                            # each (OpsItem.Priority) but NOT fully field-diffed -- see families and
                            # gaps below for exactly what remains open. Every ops: row carried over
                            # from the 2026-07-11 audit (last_audit_commit 2d2b1b9b) whose backing
                            # files were not touched this pass is trusted unchanged per protocol.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  PutParameter: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — see Notes: hierarchy-level limit, labeled-oldest-version eviction guard, Intelligent-Tiering auto-upgrade, Policies-require-Advanced-tier"}
  GetParameter: {wire: ok, errors: ok, state: ok, persist: ok, note: "selector suffix (:version/:label), SecureString decrypt, ARN population all proven correct"}
  GetParameters: {wire: ok, errors: ok, state: ok, persist: ok, note: "unresolvable names/labels/decrypt failures correctly become InvalidParameters entries, not a hard error"}
  GetParameterHistory: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults 1-50 default 50 (matches AWS), label backfill via parameterLabelsStore proven correct, pagination via opaque index token"}
  DeleteParameter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteParameters: {wire: ok, errors: ok, state: ok, persist: ok}
  GetParametersByPath: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults 1-10 default 10 (matches AWS), recursive/non-recursive prefix matching, ParameterFilters proven correct"}
  DescribeParameters: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults 1-50 default 50 (matches AWS)"}
  LabelParameterVersion: {wire: fixed, errors: ok, state: ok, persist: ok, note: "10-label-per-version cap (appendLabelsWithLimit) and move-label-between-versions semantics proven correct; (2026-08-14, gopherstack-7185) FIXED -- LabelParameterVersionOutputFull serialized an invented AddedLabels field with no counterpart in aws-sdk-go-v2/service/ssm@v1.73.4's LabelParameterVersionOutput (InvalidLabels + ParameterVersion only, confirmed against api_op_LabelParameterVersion.go and the awsAwsjson11_deserializeOpDocumentLabelParameterVersionOutput case switch). Several existing tests asserted AddedLabels' presence, entrenching the wrong shape; corrected to verify actually-attached labels via GetParameterHistory instead."}
  UnlabelParameterVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — DocumentDescription response was leaking the full Content body (see Notes)"}
  GetDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — explicit $DEFAULT selector was conflated with $LATEST (see Notes)"}
  UpdateDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — same Content-leak as CreateDocument; version cap (maxDocumentVersionCap=1000) proven correct"}
  DescribeDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — Content leak, AND the DocumentVersion selector was previously ignored entirely (always described the latest version)"}
  DeleteDocument: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDocuments: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDocumentVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDocumentPermission: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDocumentPermission: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDocumentDefaultVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "verifies requested version exists in documentVersionsStore before pinning"}
  SendCommand: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCommands: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCommandInvocations: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCommandInvocation: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelCommand: {wire: ok, errors: ok, state: ok, persist: ok}
  PutInventory: {wire: ok, errors: ok, state: ok, persist: ok, note: "merge-by-TypeName semantics proven correct"}
  GetInventory: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInventorySchema: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static built-in AWS: /Custom: schema catalog, matches real SSM's documented inventory types"}
  DeleteInventory: {wire: ok, errors: ok, state: ok, persist: ok, note: "records a real DeletionId job consumed by DescribeInventoryDeletions"}
  CreateActivation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteActivation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeActivations: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-a250): input was a literal struct{}; real DescribeActivationsInput (api_op_DescribeActivations.go) has optional Filters/MaxResults/NextToken, all discarded on every request. Now filters by FilterKey (ActivationIds/DefaultInstanceName/IamRole, the only 3 real DescribeActivationsFilterKeys — unrecognized keys accept-and-echo, mirroring ListNodes) and paginates via the shared paginateSlice helper (store.go). TestDescribeActivations_FiltersAndPagination (empty_struct_inputs_test.go), hand-verified failing against unfixed code."}
  DeleteAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePatchBaseline: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (parity-sweep-3) — was missing ApprovalRules/GlobalFilters/Sources/RejectedPatchesAction/AvailableSecurityUpdatesComplianceStatus/ApprovedPatchesEnableNonSecurity entirely (confirmed against aws-sdk-go-v2 v1.73.4's api_op_CreatePatchBaseline.go); all now round-trip for real. FIXED phase-2 — ApprovedPatchesEnableNonSecurity converted bool -> *bool (confirmed *bool in CreatePatchBaselineInput/UpdatePatchBaselineInput/PatchBaseline via go doc)."}
  DeletePatchBaseline: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPatchBaseline: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — PatchGroups (patch groups currently registered with this baseline) was entirely unpopulated; now derived from the reverse patchGroup->baselineID map, excluding the synthetic default/default-<OS> bookkeeping keys"}
  UpdatePatchBaseline: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (parity-sweep-3) — same missing-fields gap as CreatePatchBaseline, now merges them. FIXED phase-2 — ApprovedPatchesEnableNonSecurity is now *bool so an explicit false is distinguishable from omitted and can actually turn the flag back off (previously could only ever turn it on)."}
  StartSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — StartSessionInput previously accepted 4 gopherstack-invented fields (OutputS3BucketName/OutputS3KeyPrefix/CloudWatchLogGroupName/CloudWatchOutputEnabled) not present anywhere in the real SDK's StartSessionInput (confirmed: only Target/DocumentName/Parameters/Reason exist) — deleted per parity-principles' invented-field rule. Session.OutputUrl (real field name SessionManagerOutputUrl, members CloudWatchOutputUrl/S3OutputUrl) is documented \"Reserved for future use\" in the SDK and is correctly never populated now. Session.AccessType now defaults to \"Standard\" (was entirely absent)."}
  TerminateSession: {wire: ok, errors: ok, state: ok, persist: ok}
  ResumeSession: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSessions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — 3 real bugs: (1) State ('Active'/'History', types.SessionState) was compared directly against a session's own Status ('Connected'/'Terminated', types.SessionStatus) — two different enums — so a real client's State filter could never match; added sessionStateMatchesFilter bucketing. (2) Filters ([]SessionFilter, Target/Owner/SessionId/AccessType/Status/InvokedAfter/InvokedBefore) was accepted on the wire and silently discarded. (3) MaxResults/NextToken pagination was entirely missing (always returned every session). SessionFilter's wire keys are lowercase \"key\"/\"value\" — confirmed a deliberate AWS quirk via serializers.go, not a copy-paste bug to '''fix'''."}
  GetConnectionStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass — the not-connected case returned \"notConnected\" (camelCase); real AWS's ConnectionStatus enum is all-lowercase \"notconnected\" (confirmed types/enums.go). The connected case (\"connected\") was already correct."}
  GetAccessToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — previously a pure stub returning a fabricated TokenValue/AccessRequestId regardless of input, matching neither the real request shape (AccessRequestId, required) nor response shape (AccessRequestStatus + Credentials{AccessKeyId,SecretAccessKey,SessionToken,ExpirationTime} — confirmed api_op_GetAccessToken.go). Now looks up a real AccessRequest created by StartAccessRequest and mints mock Credentials only when Approved."}
  StartAccessRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — previously ignored Reason/Targets/Tags entirely and returned a random ID with no backing state. Now validates Reason+Targets are present (both required in the real SDK) and persists a real AccessRequest (new *store.Table[AccessRequest], services/ssm/store_setup.go), auto-approved since gopherstack has no approver workflow to model — documented rather than left as a silent no-op."}
  CreateOpsItem: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (parity-sweep-3) — Priority (confirmed present in api_op_CreateOpsItem.go) was entirely absent; now round-trips. FIXED phase-2 (bd gopherstack-iq4m, closed) — AccountId/ActualStartTime/ActualEndTime/Notifications/PlannedStartTime/PlannedEndTime/RelatedOpsItems all now round-trip, confirmed against api_op_CreateOpsItem.go. OperationalDataToDelete (an UpdateOpsItem-only field, out of the bd issue's field list) deliberately left out of scope — documented in models_ops_items.go."}
  UpdateOpsItem: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (parity-sweep-3) — Priority now round-trips (see CreateOpsItem). FIXED phase-2 (bd gopherstack-iq4m, closed) — same 7 fields as CreateOpsItem now round-trip via UpdateOpsItem, confirmed against api_op_UpdateOpsItem.go."}
  CreateAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED phase-2 (bd gopherstack-ouvq, closed) — ApplyOnlyAtCronInterval/ComplianceSeverity/MaxConcurrency/MaxErrors/OutputLocation/ScheduleExpression/SyncCompliance/CalendarNames/AssociationDispatchAssumeRole/AutomationTargetParameterName/Duration all now round-trip, confirmed against api_op_CreateAssociation.go. Name/Targets/Parameters/DocumentVersion/AssociationName/InstanceID were already correct (parity-sweep-3)."}
  UpdateAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED phase-2 — same 10 optional fields as CreateAssociation (all but Name, which UpdateAssociationInput doesn't carry) now round-trip, confirmed against api_op_UpdateAssociation.go. Previously only AssociationName/DocumentVersion/Parameters/Targets were settable."}
  CreateAssociationBatch: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED phase-2 — CreateAssociationBatchRequestEntry carries the same extended fields as CreateAssociationInput, confirmed against types.CreateAssociationBatchRequestEntry."}
  CreateCloudConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass — see Notes: implemented from scratch, previously entirely unimplemented (excluded from sdk_completeness_test.go rather than stubbed)"}
  DeleteCloudConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  GetCloudConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  ListCloudConnectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW (parity-sweep-3) — SubscriptionId/TenantId filters incl. documented \"NONE\" tenant-level-only value, opaque index-token pagination matching DescribeParameters. FIXED phase-2 — MaxResults bound was a guess (50, aliased to defaultDescribeMaxResults); AWS has since published the real bound (Minimum 0, Maximum 10, confirmed 2026-07-24 via API_ListCloudConnectors.html) — now enforced with ValidationException outside [0,10] instead of silently accepting any positive value."}
  UpdateCloudConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  ValidateCloudConnector: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NEW (parity-sweep-3) — see Notes: no real Azure tenant to call out to, so findings are deterministically derived from the connector's own stored configuration rather than a fabricated always-success stub (this specific limitation remains a genuine sandbox impossibility, see gaps). FIXED phase-2 — MaxResults bound was a guess (50); AWS has since published the real bound (Minimum 0, Maximum 75, confirmed 2026-07-24 via API_ValidateCloudConnector.html) — now enforced with ValidationException outside [0,75]."}
  RegisterTaskWithMaintenanceWindow: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — Targets (the managed nodes/window-targets a task applies to; confirmed required-in-practice for Run Command tasks per api_op_RegisterTaskWithMaintenanceWindow.go) was accepted on the wire but silently discarded; now round-trips through Register/Update/Describe."}
  UpdateMaintenanceWindowTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — same Targets gap as RegisterTaskWithMaintenanceWindow"}
  CreateMaintenanceWindow: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — StartDate/EndDate/ScheduleTimezone/ScheduleOffset were confirmed present in api_op_CreateMaintenanceWindow.go but entirely absent from this package; now round-trip (stored as-is, not evaluated against Schedule — see gaps)."}
  UpdateMaintenanceWindow: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — same StartDate/EndDate/ScheduleTimezone/ScheduleOffset gap, plus AllowUnassociatedTargets was previously create-only (confirmed updatable in api_op_UpdateMaintenanceWindow.go)."}
  DescribeAssociationExecutions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — see Notes: AssociationExecution.ExecutionDate was a raw time.Time (RFC3339 string on the wire); real AWS DateTime fields in this awsjson1.1 API are epoch-seconds numbers"}
  DescribeMaintenanceWindowExecutions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass — same epoch-seconds bug, MaintenanceWindowExecution.StartTime/EndTime"}
  DescribeMaintenanceWindowExecutionTasks: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass — same epoch-seconds bug, MaintenanceWindowExecutionTask.StartTime"}
  DescribeMaintenanceWindowExecutionTaskInvocations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass — same epoch-seconds bug, MaintenanceWindowExecutionTaskInvocation.StartTime"}
  GetMaintenanceWindowExecution: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass — same epoch-seconds bug"}
  GetMaintenanceWindowExecutionTask: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass — same epoch-seconds bug"}
  GetMaintenanceWindowExecutionTaskInvocation: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass — same epoch-seconds bug"}
  DescribeInstanceInformation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — same epoch-seconds bug, InstanceInformation.RegistrationDate. FIXED (gopherstack-a250): input was a literal struct{}; real DescribeInstanceInformationInput (api_op_DescribeInstanceInformation.go) has optional Filters/InstanceInformationFilterList/MaxResults/NextToken, all discarded. Now filters on the attributes InstanceInformation actually tracks (InstanceIds/ActivationIds/AgentVersion/PingStatus/PlatformTypes) and paginates. TestDescribeInstanceInformation_FilterAndPagination, hand-verified failing against unfixed code."}
  DescribeInstanceAssociationsStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass — same epoch-seconds bug, InstanceAssociationStatusInfo.ExecutionDate"}
  DescribeInstancePatchStates: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — same epoch-seconds bug, InstancePatchState.OperationStartTime"}
  DescribeInstancePatches: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — same epoch-seconds bug, PatchComplianceData.InstalledTime"}
  ListResourceDataSync: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED prior pass — same epoch-seconds bug, ResourceDataSync.SyncCreatedTime/LastSyncTime. FIXED this pass (gopherstack-4ggy): ResourceDataSyncItem.SyncSource (types.ResourceDataSyncSourceWithState) now echoed back per item, populated by UpdateResourceDataSync's fix below (was previously nil for every sync). FIXED (gopherstack-a250): input was a literal struct{}; real ListResourceDataSyncInput (api_op_ListResourceDataSync.go) has optional SyncType/MaxResults/NextToken, all discarded. Now filters by SyncType (an exact field match, real backing state) and paginates. TestListResourceDataSync_FilterAndPagination, hand-verified failing against unfixed code."}
  UpdateResourceDataSync: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "gopherstack-4ggy: SyncSource AND SyncType (both required UpdateResourceDataSyncInput members alongside SyncName -- api_op_UpdateResourceDataSync.go:36-54) were dropped entirely; the handler read only SyncName and silently returned success on an empty one instead of erroring, and never errored on an unknown sync name either. Now both required, SyncSource's own SourceType/SourceRegions validated when present (validateResourceDataSyncSource, validators.go), and stored/echoed on the ResourceDataSync (see ListResourceDataSync). Also fixed while wiring the not-found path: ErrResourceDataSyncNotFound had NO case in classifySSMErrorExtended (handler.go) at all, so both this op's and DeleteResourceDataSync's not-found path fell through to a 500 InternalServerError -- an existing test (TestDeleteResourceDataSync_Handler_NotFound) literally asserted the 500 as expected behavior under the name non_existent_sync_returns_500, now corrected to this service's uniform 400 convention. ErrResourceDataSyncExists (CreateResourceDataSync's duplicate-name case) had the same missing-mapping bug, fixed alongside since it's the same class of gap one line away."}
  StartChangeRequestExecution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "gopherstack-4ggy: Runbooks (a required StartChangeRequestExecutionInput member, api_op_StartChangeRequestExecution.go:37-51) was dropped entirely -- request only read the top-level DocumentName (the change template document) and built automation steps from IT directly, when the actual Automation runbook(s) to execute live in Runbooks[].DocumentName instead. Now required (each entry's own DocumentName required per validateRunbook, validators.go), steps built from Runbooks[0].DocumentName (this backend's AutomationExecution models one step list; real AWS runs each Runbook as its own workflow -- an accepted simplification, not attempted to fully multi-runbook this pass), and the full Runbooks list echoed back on AutomationExecution.Runbooks (new field, types.AutomationExecution.Runbooks, types.go:761/943) for both GetAutomationExecution and DescribeAutomationExecutions. Runbook itself models only DocumentName/DocumentVersion/MaxConcurrency/MaxErrors/Parameters -- TargetLocations/TargetMaps/TargetParameterName/Targets deliberately unmodeled, matching the same shallow-scalar simplification StartAutomationExecutionInput already makes for its own Targets/TargetLocations/TargetParameterName (pre-existing convention, not new scope)."}
  DescribeInventoryDeletions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — same epoch-seconds bug, InventoryDeletion.DeletionStartTime"}
  ListNodes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "2026-08-13 (gopherstack-6uag): input was a literal struct{}, same surface pattern as ListNodesSummary's pre-fix bug (gopherstack-m53b) but a different case on inspection -- ListNodesInput (api_op_ListNodes.go:31-53) has no required members (Filters/MaxResults/NextToken/SyncName all optional), unlike ListNodesSummaryInput's required Aggregators, so this was never the required-field-ignored/fabricated-response-key stub class. It was still a real bug: the struct{} silently discarded all four real optional fields from every request, so Filters never filtered and MaxResults/NextToken never paginated. Fixed by giving ListNodesInput real fields, applying Filters via the shared filterNodes (extracted from ListNodesSummary's own fix, no behavior change there), and paginating via this service's established parseNextToken convention (50-item default, matching DescribeOpsItems). Reading the whole operation found a second, more severe bug: the real ListNodesOutput element (types.Node, types/types.go:4087-4106) is CaptureTime/Id/NodeType/Owner/Region, with PlatformType/AgentVersion nested three levels down under NodeType.Instance (types.InstanceInfo, types/types.go:2693-2747) -- this backend instead serialized NodeInfo directly under top-level InstanceId/PlatformType/AgentVersion/RegistrationDate keys, none of which exist on the real wire, and RegistrationDate doesn't correspond to any real field at all (renamed the wire-facing struct's field to CaptureTime, the real epoch-seconds member). New wire types Node/NodeType/NodeInstanceInfo/NodeOwnerInfo added; NodeInfo keeps its old field set as a purely internal domain struct, converted to Node by nodeToWire at response time. Owner is always nil: no account/OU tracking exists. Proven via TestFleetManager_ListNodes_FromActivations (rewritten to drive the real SDK client and assert the nested NodeType.Instance.PlatformType location instead of a raw top-level map key, which would have passed against the bug), new TestFleetManager_ListNodes_Filters, and TestEpochSecondsWireShape_Node (renamed from _NodeInfo) -- all three hand-verified to fail against the pre-fix code."}
  ListNodesSummary: {wire: ok, errors: ok, state: ok, persist: n/a, note: "gopherstack-m53b (required-member sweep pass 4): input was a literal struct{} (api_op_ListNodesSummary.go:31-62 shows Aggregators is a required []types.NodeAggregator, Filters/MaxResults/NextToken/SyncName optional) and the backend ignored its own parameter entirely, returning a fixed synthetic {\"NodeCount\": activationCount} regardless of what was requested — the fabricated \"NodeCount\" key does not exist on the real wire either (real Summary is []map[string]string with no fixed key schema). Op WAS reachable (JSON-RPC 1.1 dispatch keys off the X-Amz-Target header, not the input shape) — confirmed with the sdkshape script and by reading handler.go's ssmDispatchTable/jsonOp, so this was a backend-logic bug, not a routing bug. Fixed: Aggregators is now required (InvalidAggregatorException, one of this op's own declared exceptions per deserializeOpErrorListNodesSummary — not the generic ValidationException most other ssm ops use) and actually drives real per-attribute grouping (aggregateNodes in instances.go) over managed nodes derived from the activations store, with Filters applied (matchesNodeFilter) before grouping. This backend only tracks InstanceId/PlatformType/AgentVersion per node (see NodeInfo) — the other five NodeAttributeName/NodeFilterKey values (PlatformName/PlatformVersion/Region/ResourceType/SourceType/AvailabilityZone/...) have no backing state and are honestly left as \"\" rather than fabricated; nested NodeAggregator.Aggregators (multi-level grouping) are accepted on the wire but not applied. Proven via Test_SDKRoundTrip_ListNodesSummary_GroupsByAggregator/TestListNodesSummary_Filters/TestListNodesSummary_MissingAggregators (list_nodes_summary_test.go) and TestFleetManager_ListNodesSummary_NodeCount (activations_test.go, converted to drive the real SDK client) — all fail against the unfixed backend. TestStubOps_SimpleCalls's bare-{}-body manifest (maintenance_window_lifecycle_test.go) had ListNodesSummary removed per parity-principles.md's de-stub-hygiene rule, since an empty body is no longer valid input."}
families:
  cloud-connectors: {status: ok, note: "NEW this pass — aws-sdk-go-v2 bumped v1.69.5 to v1.71.0 (see sdk_module) added CreateCloudConnector/DeleteCloudConnector/GetCloudConnector/ListCloudConnectors/UpdateCloudConnector/ValidateCloudConnector (Azure-only third-party cloud environment connectors). Implemented as a real *store.Table[CloudConnector]-backed resource (services/ssm/cloud_connector.go): required-field validation (ConfigConnectorArn/DisplayName/RoleArn/Configuration.AzureConfiguration.{ApplicationId,TenantId}) on Create, ResourceNotFoundException (the SDK's generic not-found type — no CloudConnector-specific error exists) on Get/Delete/Update/Validate of an unknown ID, tag integration via the existing generic miscResourceTags fallback path (ResourceTypeForTagging enum confirms \"CloudConnector\" is a valid AddTagsToResource/ListTagsForResource resource type, and that path was already resource-type-agnostic), and full Snapshot/Restore persistence via the existing store.Registry generic mechanism (store_setup.go's getOrCreateTable/tableAccessorsByPrefix — no persistence.go changes needed). Wire shapes verified against aws-sdk-go-v2/service/ssm@v1.73.4's serializers.go/deserializers.go directly (not the SDK's own doc comments): CreatedAt/UpdatedAt are epoch-seconds JSON numbers, matching this package's existing UnixTimeFloat convention, NOT ISO8601 strings; Configuration is a one-member Azure-only union wire-wrapped by member name (\"AzureConfiguration\")."}

  parameter-store: {status: ok, note: "FIXED (parity-sweep-3, PutParameter): 15-level hierarchy limit (HierarchyLevelLimitExceededException, previously unenforced), labeled-oldest-version eviction guard (ParameterMaxVersionLimitExceeded, previously silently evicted labeled versions and leaked their parameterLabels entries forever), Intelligent-Tiering auto-upgrade-to-Advanced on >4KiB value or Policies attached (previously hard-rejected instead of auto-selecting Advanced, defeating the entire point of Intelligent-Tiering), Policies-require-Advanced-tier (previously any tier accepted policies). Tier value-size limits (4096 Standard / 8192 Advanced), AllowedPattern regex validation, SecureString KMS encrypt/decrypt round-trip via per-instance AES-256 key, parameter selector suffix (:version/:label) parsing were all already correct. FIXED phase-2 (2026-07-24) — NoChangeNotification/ExpirationNotification policies were stored and round-tripped but never evaluated; now a new janitor sweep (sweepParameterPolicyNotifications, parameter_policy_notifications.go) evaluates every parameter's Policies each tick and reports newly-due policies through an injectable ParameterPolicyNotifier, with per-policy-instance dedupe (never refires until the parameter is re-written, matching AWS's documented LastModifiedTime-reset semantics for NoChangeNotification) and cascade cleanup on delete (no ghost dedupe rows). The EventBridge-side adapter is implemented for real (services/eventbridge/ssm_integration.go, publishes source=\"aws.ssm\"/detail-type=\"Parameter Store Policy Action\"/detail={\"parameter-name\",\"policy-type\"} — confirmed via sysman-paramstore-cwe.html) and proven by TestNotifyParameterPolicyAction using an EventBridge Archive with a matching EventPattern as an independent wire-shape observer. Only the cli.go line wiring InMemoryBackend.SetParameterPolicyNotifier(ebBackend) remains — see Notes."
  documents: {status: ok, note: "FIXED this pass (this AND prior pass): CreateDocument/UpdateDocument/DescribeDocument content-leak and $DEFAULT/$LATEST conflation (prior pass, see below). THIS pass (bd gopherstack-1hg, now closed): the version-cap eviction (maxDocumentVersionCap=1000, FIFO-trimmed on every UpdateDocument) could silently evict the version pinned as DefaultVersion, orphaning the $DEFAULT selector after 1000+ updates. Fixed via evictOldestDocumentVersions, which now skips the DefaultVersion-pinned entry when trimming (mirrors PutParameter's labeled-version eviction guard) — the store may retain one entry beyond the cap in that case, an accepted tradeoff for never orphaning $DEFAULT. — Prior-pass notes: CreateDocument/UpdateDocument/DescribeDocument were all returning the internal Document struct (which carries Content) as their metadata-only response — added a DocumentDescription wire type (matches AWS's real DocumentDescription, no Content field) and a Document.toDocumentDescription() converter. Also: GetDocument/DescribeDocument's DocumentVersion selector conflated explicit \"$DEFAULT\" with \"$LATEST\"/omitted, always serving the latest version's content/metadata even when a caller explicitly asked for $DEFAULT after UpdateDocumentDefaultVersion pinned an older version. Left the omitted-DocumentVersion behavior as latest (unchanged) since AWS's own API/CLI reference docs do not state a default and an existing, deliberately-written test (document_test.go TestInMemoryBackend_Snapshot_IncludesDocumentsAndCommands) depends on that behavior — only the unambiguous explicit-$DEFAULT case was fixed. Document version cap (1000) and content-hash-free JSON/YAML round-trip were already correct."
  command-execution: {status: ok, note: "no goroutines/timers in command_exec.go or automation_exec.go — command progression is driven synchronously plus the single ctx-cancel-aware janitor sweep (janitor.go), not per-command background workers. Nothing to leak."}
  automation-executions: {status: ok, note: "gopherstack-gt9o: modeled AutomationExecution/AutomationExecutionMetadata/StepExecution's WarningMessage *string field (confirmed at aws-sdk-go-v2/service/ssm@v1.73.4 types/types.go:803,969,6079), but deliberately left it permanently unset (json omitempty). Reasoning: WarningMessage is output-only, set by real SSM's engine for a non-critical issue detected mid-run; this emulator's automation run (automation_exec.go completeAutomationLocked) unconditionally drives every step to Success with no partial-failure/degraded/timeout path — automationStatusFailed is declared in store.go but never assigned anywhere. There is no genuine condition to derive a warning from, so inventing one would be a fabricated string a real client could surface to an operator. Same precedent as apigatewayv2's failOnWarnings (validated but a documented no-op). Test coverage (automations_test.go TestAutomationExecution_WarningMessageAbsentFromWire) asserts the raw response body genuinely omits the key (not merely empty-string) across GetAutomationExecution/DescribeAutomationExecutions/DescribeAutomationStepExecutions; neuter-tested by dropping omitempty, confirming all three subtests fail, then restoring. FIXED (gopherstack-a250): DescribeAutomationExecutions' input was a literal struct{}; real DescribeAutomationExecutionsInput (api_op_DescribeAutomationExecutions.go) has an optional Filters member (plus MaxResults/NextToken), all discarded. Now filters on ExecutionId/ExecutionStatus (exact match) and DocumentNamePrefix (prefix match, the three attributes AutomationExecution actually tracks) and paginates. TestDescribeAutomationExecutions_FilterAndPagination, hand-verified failing against unfixed code."}
  sessions: {status: ok, note: "FULLY RE-VERIFIED and FIXED this pass (previously deferred) — see the per-op notes above (StartSession/DescribeSessions/GetConnectionStatus/GetAccessToken/StartAccessRequest) for the 6 real bugs found and fixed: invented StartSessionInput fields, State/Status enum confusion, missing Filters/pagination, wrong ConnectionStatus casing, and GetAccessToken/StartAccessRequest being non-functional stubs. TerminateSession/ResumeSession/evictExcessTerminatedSessionsLocked were already correct (re-confirmed, no changes). New AccessRequest resource (services/ssm/models_sessions.go, sessions.go) is a real *store.Table[AccessRequest]-backed resource with full Snapshot/Restore persistence via the existing store_setup.go mechanism."}
  patch-baselines: {status: ok, note: "FULLY RE-VERIFIED and FIXED (parity-sweep-3, split out of the previously-deferred 'patch-maintenance-associations-inventory' family) — see CreatePatchBaseline/UpdatePatchBaseline/GetPatchBaseline notes above. DeletePatchBaseline, DescribePatchBaselines (OS/name-prefix filters + pagination), RegisterPatchBaselineForPatchGroup/DeregisterPatchBaselineForPatchGroup, GetDefaultPatchBaseline/RegisterDefaultPatchBaseline, DescribePatchGroups/DescribePatchGroupState/DescribePatchProperties, DescribeEffectivePatchesForPatchBaseline, and GetDeployablePatchSnapshotForInstance were all re-diffed against the SDK and confirmed already-correct — no changes needed there. FIXED phase-2 — ApprovedPatchesEnableNonSecurity bool->*bool (see CreatePatchBaseline/UpdatePatchBaseline notes and Notes section)."}
  maintenance-windows: {status: ok, note: "FULLY RE-VERIFIED and FIXED this pass (split out of the previously-deferred combined family) — see RegisterTaskWithMaintenanceWindow/UpdateMaintenanceWindowTask/CreateMaintenanceWindow/UpdateMaintenanceWindow and the DescribeMaintenanceWindowExecution*/GetMaintenanceWindowExecution* epoch-seconds notes above. RegisterTargetWithMaintenanceWindow/DeregisterTargetFromMaintenanceWindow/UpdateMaintenanceWindowTarget/DeregisterTaskFromMaintenanceWindow/DescribeMaintenanceWindows/DescribeMaintenanceWindowTargets/DescribeMaintenanceWindowTasks/DescribeMaintenanceWindowsForTarget/DescribeMaintenanceWindowSchedule/CancelMaintenanceWindowExecution/DeleteMaintenanceWindow re-diffed and confirmed already-correct."}
  state-manager-associations: {status: ok, note: "SPOT-CHECKED (parity-sweep-3, split out of the previously-deferred combined family) — AssociationExecution.ExecutionDate epoch-seconds bug fixed (DescribeAssociationExecutions). FULLY FIELD-DIFFED phase-2 (bd gopherstack-ouvq, closed) — CreateAssociationInput/UpdateAssociationInput/CreateAssociationBatchRequestEntry were missing ApplyOnlyAtCronInterval/ComplianceSeverity/MaxConcurrency/MaxErrors/OutputLocation/ScheduleExpression/SyncCompliance/CalendarNames/AssociationDispatchAssumeRole/AutomationTargetParameterName/Duration, confirmed against api_op_CreateAssociation.go/api_op_UpdateAssociation.go/types.CreateAssociationBatchRequestEntry; all 11 now round-trip through Create/CreateBatch/Update and are covered by wire-shape-asserting tests (associations_test.go). DeleteAssociation/DescribeAssociation/UpdateAssociationStatus/ListAssociationVersions/StartAssociationsOnce/DescribeAssociationExecutionTargets re-confirmed already-correct, no changes needed. CORRECTION (gopherstack-a250): ListAssociations WAS wrong — input was a literal struct{}; real ListAssociationsInput (api_op_ListAssociations.go) has optional AssociationFilterList/MaxResults/NextToken, all discarded, and the response never carried NextToken either (a dead, unused ListAssociationsOutputFull type already had the right shape). Now filters on InstanceId/Name/AssociationId/AssociationName/AssociationStatusName (the attributes Association actually tracks) and paginates; backend return type switched to ListAssociationsOutputFull. TestListAssociations_FilterAndPagination, hand-verified failing against unfixed code."}
  ops-center: {status: ok, note: "SPOT-CHECKED (parity-sweep-3, split out of the previously-deferred combined family) — Priority confirmed missing and fixed. FULLY FIELD-DIFFED phase-2 (bd gopherstack-iq4m, closed) — CreateOpsItemInput/UpdateOpsItemInput were missing AccountId/ActualStartTime/ActualEndTime/Notifications/PlannedStartTime/PlannedEndTime/RelatedOpsItems (mostly Change-Manager /aws/changerequest-oriented), confirmed against api_op_CreateOpsItem.go/api_op_UpdateOpsItem.go; all 7 now round-trip and are covered by wire-shape-asserting tests (ops_items_test.go). UpdateOpsItemInput.OperationalDataToDelete (confirmed present but outside the bd issue's field list) deliberately left out of scope, documented in models_ops_items.go. GetOpsItem/DeleteOpsItem/DescribeOpsItems (filters+pagination)/AssociateOpsItemRelatedItem/DisassociateOpsItemRelatedItem/ListOpsItemRelatedItems/ListOpsItemEvents/CreateOpsMetadata/GetOpsMetadata/DeleteOpsMetadata re-confirmed already-correct, no changes needed. CORRECTION (gopherstack-7rq1): UpdateOpsMetadata was NOT actually correct -- UpdateOpsMetadataInput's Metadata field carried json tag \"Metadata\", but the real UpdateOpsMetadataRequest member (ssm/2014-11-06/service-2.json) is \"MetadataToUpdate\" (CreateOpsMetadataRequest genuinely does use \"Metadata\", which is presumably how this got missed). A real client's update payload was silently dropped by json.Unmarshal every time, making UpdateOpsMetadata a complete no-op; the existing test asserting HTTP 200 with a body keyed \"Metadata\" passed despite this. Fixed the json tag; TestOpsMetadata_FullCRUD's Update step now sends the real wire key and asserts the update actually lands. CORRECTION (gopherstack-a250): ListOpsMetadata was NOT actually correct either -- input was a literal struct{}; real ListOpsMetadataInput (api_op_ListOpsMetadata.go) has optional Filters/MaxResults/NextToken, all discarded. Now filters by Key==\"ResourceId\" (the only OpsMetadata attribute with real backing state; other keys accept-and-echo) and paginates. TestListOpsMetadata_FilterAndPagination, hand-verified failing against unfixed code. GetOpsSummary's Aggregators/Filters/MaxResults/NextToken/ResultAttributes/SyncName (also a literal struct{}) deliberately left unwired: this backend's GetOpsSummary always returns one fixed AWS:OpsItem/Count entity, not a queryable multi-type OpsData dataset these members could honestly filter or aggregate over -- documented in models_ops_items.go rather than fabricating query semantics."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "NoChangeNotification/ExpirationNotification are now fully EVALUATED (see families.parameter-store and Notes: 'Parameter policy notifications') — a new janitor sweep computes due-ness and calls an injectable ParameterPolicyNotifier, and the real EventBridge-side adapter (services/eventbridge/ssm_integration.go) is implemented and proven by a cross-package test (TestNotifyParameterPolicyAction). The ONE remaining piece, deliberately left undone because this agent was instructed not to edit cli.go, is the single wiring call — `ssmBackend.SetParameterPolicyNotifier(eventbridgeBackend)` (mirroring the existing SetEventBridgeIntegration/SetSQSIntegration/SetGlueIntegration wiring block in cli.go around wireStepFunctionsServiceIntegrations) — that actually injects the real notifier into the running SSM backend at startup. Until that line lands, PutParameter/the janitor behave exactly as before from an external caller's perspective (b.parameterPolicyNotifier is nil, so the sweep is a safe no-op) — see cli_wiring_note in the pass receipt."
  - "ValidateCloudConnector cannot make a real outbound call to Azure (gopherstack has no Azure tenant), so its ValidationFindings are derived deterministically from the connector's own stored Configuration (tenant/subscription IDs) rather than reflecting real third-party connectivity/permission state. This is an inherent sandbox constraint (same category as KMS being locally emulated instead of a real HSM call), not a wire/state bug — re-confirmed phase-2, still genuinely impossible for the same reason (no Azure credentials/tenant/egress available to the emulator, and reaching out to a live Azure tenant from an AWS emulator's request handler would be inappropriate even if it were possible) — documented here so a future reader doesn't mistake the mocked findings for verified AWS behavior."
  - "CreateMaintenanceWindow/UpdateMaintenanceWindow's new StartDate/EndDate/ScheduleTimezone/ScheduleOffset fields are stored and round-tripped verbatim but not evaluated — DescribeMaintenanceWindowSchedule/DescribeMaintenanceWindowExecutions do not yet factor StartDate/EndDate into whether a window is currently active, or ScheduleOffset into the computed next-run time. Untouched this pass — out of scope (not one of this pass's assigned gaps)."
deferred: []              # phase-2 (2026-07-24): closed CreateAssociationInput/UpdateAssociationInput/
                           # CreateAssociationBatchRequestEntry field gaps (bd gopherstack-ouvq),
                           # CreateOpsItemInput/UpdateOpsItemInput field gaps (bd gopherstack-iq4m),
                           # PatchBaseline.ApprovedPatchesEnableNonSecurity bool->*bool, ListCloudConnectors/
                           # ValidateCloudConnector MaxResults bounds (now AWS-published), and implemented
                           # NoChangeNotification/ExpirationNotification evaluation+emission end-to-end
                           # except the single cli.go injection line. Remaining open items are proven
                           # impossibilities (ValidateCloudConnector's Azure call) or genuinely out of this
                           # pass's assigned scope (MaintenanceWindow schedule evaluation).
leaks: {status: clean, note: "Janitor (janitor.go) is the only background goroutine, ctx.Done()-aware, single Run() loop shared across all sweeps (parameters/commands/sessions). PutParameter's history cap now also deletes the corresponding parameterLabels[version] entries on eviction (previously left as an unbounded-growth leak: labels attached to since-evicted versions stayed in the map forever with no key ever removed). THIS PASS: new AccessRequest store (services/ssm/sessions.go) follows the same pattern as patchBaselines/opsItems/documents — a user-managed resource with no automatic janitor sweep, not a leak (consistent with existing precedent for resources the caller is expected to explicitly delete). No new goroutines/tickers/timers introduced this pass; the epoch-seconds timestamp fix (see overall note) touched only struct field types and their few call-site assignments, no new state or locking."}
---

## Notes

SSM speaks the **json-1.1 protocol** (`AmazonSSM.<Op>` `X-Amz-Target`, `application/x-amz-json-1.1`
content type) — confirmed via `handler.go`'s `classifySSMError`/`handleError` using
`service.JSONErrorResponse` with a bare `{"Type":..., "Message":...}` body, not XML.

### Empty-struct-input candidates: fixed (gopherstack-a250, closing the gopherstack-6uag follow-up)

The 7 ops flagged by the previous pass — `DescribeActivations`, `ListResourceDataSync`,
`DescribeInstanceInformation`, `ListAssociations`, `DescribeAutomationExecutions`,
`ListOpsMetadata`, `GetOpsSummary` — were re-verified against the pinned `ssm@v1.73.4` SDK and 6
of the 7 fixed: each real `*Input` has optional `Filters`/`MaxResults`/`NextToken`-class members a
literal `struct{}` input discarded on every request. See the per-op notes above (and
`families.state-manager-associations`/`ops-center`/`automation-executions`) for exactly what each
now filters on and its SDK citation. `GetOpsSummary` is the one exception, deliberately left
unwired — see its note under `families.ops-center` above: this backend's `GetOpsSummary` returns
one fixed synthetic entity, not a queryable dataset its `Aggregators`/`Filters`/`ResultAttributes`
could honestly project across. All 6 fixes are proven by real-`aws-sdk-go-v2`-client tests in
`empty_struct_inputs_test.go`, each hand-verified to fail against the pre-fix code.

### Real bug: Intelligent-Tiering was rejecting the exact case it exists for

`resolveTier` treated `Intelligent-Tiering` identically to `Standard` for the 4096-byte size
check — a `PutParameter` with `Tier: "Intelligent-Tiering"` and a 5000-byte value returned
`ValidationException` instead of succeeding. This defeats the entire purpose of the tier: per AWS
docs (confirmed via websearch against the GitHub aws-sdk-net issue tracker and the AWS
"Managing tiers" user guide), Intelligent-Tiering auto-promotes to Advanced whenever the request
needs a capability Standard doesn't support — either a value over 4 KiB, or parameter policies
attached — rather than failing. Fixed: `resolveTier` now upgrades `tier` to `"Advanced"` in that
case (and still enforces the 8 KiB Advanced ceiling on top). An explicit `Tier: "Standard"` still
hard-fails on the same conditions, since the caller opted out of auto-selection by naming a
concrete tier. Confirmed via websearch that Policies (Expiration/ExpirationNotification/
NoChangeNotification) are Advanced-tier-only — Standard rejects them outright (this AWS constraint
was previously entirely unenforced; any tier could carry a Policies string). Three existing tests
in `parity_emr_test.go` (`TestParityEMR_ParameterExpiration_JanitorEvicts`) attached an Expiration
policy without ever setting `Tier`, i.e. exercised Standard+Policies — updated those to
`Tier: "Advanced"` since that combination is what real AWS requires; the janitor-eviction behavior
under test is otherwise untouched.

### Real bug: labeled parameter versions could be silently evicted

`PutParameter` caps stored history at 100 versions (`maxHistoryCap`), evicting the oldest entry on
overflow. AWS's actual behavior (confirmed via websearch of the
`ParameterMaxVersionLimitExceeded` exception docs) is that this eviction is refused — and the
whole `PutParameter` call fails with `ParameterMaxVersionLimitExceeded` — when the version about to
be evicted has a label attached, specifically so a labeled ("prod", "release-42", etc.) version is
never silently destroyed out from under a consumer pinned to that label. The emulator previously
evicted unconditionally. Fixed with a pre-mutation check (oldest history entry's
`parameterLabelsStore` entry) that aborts the whole write before any state changes if the oldest
version is labeled. Also closed a companion leak: `parameterLabels[name][version]` entries for
already-evicted versions were never deleted, so a parameter updated thousands of times would
accumulate stale label-map entries forever; eviction now deletes them.

### Real bug: parameter name hierarchy depth was never validated

AWS caps a parameter name at 15 "/"-delimited levels (confirmed via the `PutParameter` API
reference's own worked example: `/L1/.../L14/name` is valid, one more level throws
`HierarchyLevelLimitExceededException`). `validateParameterName` checked length, double-slashes,
reserved prefixes, and the name-charset regex, but never counted hierarchy depth. Added
`parameterHierarchyLevels`/`maxParamHierarchyLevels` and the new `ErrHierarchyLevelLimitExceeded`
sentinel, wired into `classifySSMError`.

### Real bug: DescribeDocument/CreateDocument/UpdateDocument leaked Content in a metadata-only response

AWS's real `DocumentDescription` structure (returned by all three ops) has **no `Content` field**
— confirmed by grepping `aws-sdk-go-v2/service/ssm/types/types.go` for `DocumentDescription
struct`. Only `GetDocument` returns document content; the metadata ops deliberately omit it (likely
so a `ListDocuments`-adjacent describe call doesn't have to re-transmit a potentially large
document body). This emulator's `CreateDocumentOutput`/`UpdateDocumentOutput`/
`DescribeDocumentOutput` all embedded the full internal `Document` struct — which does carry
`Content` (no `omitempty`) for `GetDocument`'s own use — so every describe/create/update response
included the entire document body. A conformant SDK client ignores unknown response fields, so this
wasn't client-breaking, but it is a real wire-shape deviation (and a needless
content-in-metadata-response leak) per the audit's wire-shape-accuracy bar. Fixed by introducing a
separate `DocumentDescription` type (mirrors `Document` minus `Content`) and a
`Document.toDocumentDescription()` converter; the three ops now return that type. Covered by a new
JSON-serialization assertion (`Test_DescribeDocument_OmitsContentAndHonorsVersionSelector`) since a
Go zero-value-string field is indistinguishable from an absent field in a struct-level comparison
— only marshaling and checking the wire bytes actually catches this class of bug.

### Real bug: explicit "$DEFAULT" document version was conflated with "$LATEST"

`GetDocument` and `DescribeDocument` both special-cased `""`, `"$LATEST"`, and `"$DEFAULT"`
identically, always serving the document's latest content/metadata. But `$DEFAULT` is a distinct,
explicit selector — pinned independently via `UpdateDocumentDefaultVersion` — that can diverge from
`$LATEST` (create v1, `UpdateDocument` to v2, never repoint the default: v1 is still `$DEFAULT`,
v2 is `$LATEST`). A caller explicitly asking for `$DEFAULT` in that state got v2's content instead
of v1's. Fixed via a shared `resolveDocumentVersionSelector(doc, requested)` helper used by both
ops; `DescribeDocument` additionally now looks up the resolved version's own
`DocumentVersion`/`DocumentFormat`/`Status` from `documentVersionsStore` instead of always
reporting the top-level (latest) document's fields.

**Deliberately NOT changed**: what an *omitted* `DocumentVersion` resolves to. AWS's API reference,
CLI reference, and user guide (all checked via WebFetch this pass) do not state whether omitting
the parameter is equivalent to `$DEFAULT` or `$LATEST` — evidence was genuinely ambiguous — and an
existing test (`document_test.go`'s `TestInMemoryBackend_Snapshot_IncludesDocumentsAndCommands` /
`document_survives_round_trip`) explicitly asserts that an omitted version returns the *latest*
content after an `UpdateDocument`. Changing that risked a real regression on weak secondary
evidence, so omitted-version behavior is left exactly as before (== `$LATEST`); only the
unambiguous explicit-`$DEFAULT` case was fixed.

### Already-correct traps (do not re-flag)

- `GetParametersByPath` (`MaxResults` 1-10, default 10) and `DescribeParameters`/
  `GetParameterHistory` (`MaxResults` 1-50, default 50) look asymmetric but are correct — these are
  AWS's actual, independently-documented per-op limits, not a copy-paste inconsistency.
- `resolveTier`'s explicit-`Standard`-tier hard-fail on `Policies` is intentional per AWS
  (`Standard tier parameters ... can't be configured to use parameter policies`) — do not "fix" it
  to silently upgrade the tier the way `Intelligent-Tiering` does; only `Intelligent-Tiering` gets
  auto-promotion, `Standard` is an explicit opt-out of that.
- `PutParameter`'s `Intelligent-Tiering` tier is echoed back verbatim in the response (`Tier:
  "Intelligent-Tiering"`) when no promotion is needed — it does **not** resolve to the concrete
  `"Standard"` tier in the wire response. The `ParameterTier` enum in
  `aws-sdk-go-v2/service/ssm/types/enums.go` lists `Intelligent-Tiering` as a first-class value
  distinct from `Standard`/`Advanced`, confirming AWS reports what was requested, not the
  internally-selected concrete tier, except when a promotion actually occurs.
- `DeleteInventory` succeeding with `removed=0` for a `TypeName` with no stored items is correct,
  not a missing not-found check — AWS's `DeleteInventory` operates on a type across the whole
  fleet and a zero-item deletion is a valid, successful job (see `DeletionSummary.TotalCount`), not
  an error. The unused `ErrInventoryNotFound`/`ErrDocumentVersionNotFound` (duplicate of
  `ErrInvalidDocumentVersion`)/`ErrExecutionPreviewNotFound`/`ErrResourcePolicyNotFound` sentinels
  declared in `backend_ops.go`/`backend_batch2.go` are dead code from an earlier pass, not evidence
  of missing error handling — the operations that would use them either don't need a not-found path
  (see DeleteInventory above) or already return a differently-named sentinel with the same string.

### New feature this pass: Cloud Connectors (aws-sdk-go-v2 v1.69.5 → v1.73.4 added surface)

The re-audit protocol's "check the SDK module for ops added since sdk_version" step turned up 6
brand-new operations (`CreateCloudConnector`, `DeleteCloudConnector`, `GetCloudConnector`,
`ListCloudConnectors`, `UpdateCloudConnector`, `ValidateCloudConnector`) that the prior dependency
bump (`e51c0de9`) had silently carved out of `sdk_completeness_test.go`'s exclusion list rather
than stubbing or implementing — i.e. genuinely unimplemented surface, not a disguised stub. Per
parity-principles.md rule 1 ("if an op genuinely can't be implemented yet, say so explicitly —
never a half-working stub"), the alternative to implementing it would have been to leave it
excluded and documented; since Cloud Connectors turned out to be a well-scoped, single-union
(Azure-only) CRUD resource with no cross-service dependency, it was implemented for real instead
(`services/ssm/cloud_connector.go`, ~410 LOC + `cloud_connector_test.go`, ~340 LOC). All wire
shapes (field names, the epoch-seconds `CreatedAt`/`UpdatedAt` DateTime shape, the
`{"AzureConfiguration": {...}}` union-by-member-name wrapping, `CloudConnectorSummary`'s narrower
field set vs. the full `CloudConnector`/`GetCloudConnectorOutput`) were read directly out of
`aws-sdk-go-v2/service/ssm@v1.73.4`'s generated `serializers.go`/`deserializers.go`, not inferred
from Go doc comments. `ResourceNotFoundException` (a generic SDK error type, not a
CloudConnector-specific one) was chosen for the not-found case since no dedicated error type exists
for this resource. See `gaps` for the two known limitations (unconfirmed pagination bound; findings
are derived from stored config rather than a real Azure connectivity check) that were consciously
left as-is rather than guessed at with false confidence.

### parity-sweep-3 (2026-07-23): systemic epoch-seconds timestamp bug (the headline finding)

SSM speaks awsjson1.1. Every `DateTime`-shaped field in
`aws-sdk-go-v2/service/ssm@v1.73.4`'s generated `deserializers.go` is deserialized via
`smithytime.ParseEpochSeconds(f64)` from a `json.Number` — confirmed by grepping every one of the
~15 `case "<FieldName>":` blocks for the affected fields (see below), every single one hit the
`case json.Number: ... ParseEpochSeconds` branch and explicitly rejects anything else
(`default: return fmt.Errorf("expected DateTime to be a JSON Number, got %T instead", value)`).
This package's own convention for timestamps (`CreatedDate`, `ModifiedDate`, `StartDate`, `EndDate`
on Parameter/Document/PatchBaseline/MaintenanceWindow/etc.) already does this correctly via the
`UnixTimeFloat(t time.Time) float64` helper (`models.go`). But 9 structs across 6 files had instead
declared the field as a raw `time.Time` or `*time.Time`, which Go's `encoding/json` marshals as an
RFC3339Nano **string** by default (e.g. `"ExecutionDate":"2026-07-23T00:00:00Z"`) — a real
`aws-sdk-go-v2` client parsing gopherstack's response for any of these fields would hard-fail with
exactly the deserializer error quoted above. This is the exact bug class the audit brief flagged as
having recurred in sagemaker/glue, and it had silently spread across a third service. Fixed by
converting every field to `float64` + `UnixTimeFloat` at each population site:

- `AssociationExecution.ExecutionDate` (`models_associations.go`, `associations.go`) — DescribeAssociationExecutions
- `MaintenanceWindowExecution.StartTime`/`EndTime`, `MaintenanceWindowExecutionTask.StartTime`,
  `MaintenanceWindowExecutionTaskInvocation.StartTime`, and the 3 `Get*OutputFull` variants of the
  same shapes (`models_maintenance_window.go`, `maintenance_window.go`) — the whole
  DescribeMaintenanceWindowExecution*/GetMaintenanceWindowExecution* op family
- `InstanceInformation.RegistrationDate`, `Node.CaptureTime` (renamed from `NodeInfo.RegistrationDate`
  when `ListNodes` was fixed for real, gopherstack-6uag, see below),
  `InstanceAssociationStatusInfo.ExecutionDate`, `InstancePatchState.OperationStartTime`,
  `PatchComplianceData.InstalledTime` (`models_instances.go`, `instances.go`, `patch_inventory.go`) —
  DescribeInstanceInformation/ListNodes/DescribeInstanceAssociationsStatus/DescribeInstancePatchStates/DescribeInstancePatches
- `ResourceDataSync.SyncCreatedTime`/`LastSyncTime` (`models_activations.go`, `activations.go`) — ListResourceDataSync
- `InventoryDeletion.DeletionStartTime` (`patch_inventory.go`, `inventory.go`) — DescribeInventoryDeletions

Two call sites (`instances.go`'s `RegistrationDate`/`ExecutionDate` population) had been doing a
pointless `time.Unix(int64(x.CreatedDate), 0).UTC()` round-trip from an *already-float64* source
field just to satisfy the (wrong) `time.Time` field type — simplified to a direct assignment now
that the field type matches the source. Locked in by a new dedicated test file,
`epoch_seconds_wire_shape_test.go`, asserting byte-for-byte that each affected field marshals as a
bare JSON number (not a quoted string) — a Go zero-value-string vs. absent-field comparison
wouldn't catch this class of bug, same rationale as the earlier `DocumentDescription` content-leak
fix's dedicated marshal-byte test.

### parity-sweep-3: Session Manager (previously deferred, now fully re-verified)

Field-diffed every session op against `aws-sdk-go-v2/service/ssm@v1.73.4`'s `api_op_*.go` files.
Six real bugs, all fixed (see the `ops:` notes for `StartSession`/`DescribeSessions`/
`GetConnectionStatus`/`GetAccessToken`/`StartAccessRequest` above for the specifics). The most
significant: `GetAccessToken`/`StartAccessRequest` were pure stubs that didn't implement the
just-in-time node access workflow at all — `GetAccessToken` returned an ad-hoc `TokenValue` field
that doesn't exist anywhere in the real `GetAccessTokenOutput` shape (`AccessRequestStatus` +
`Credentials{AccessKeyId,SecretAccessKey,SessionToken,ExpirationTime}`), and `StartAccessRequest`
never stored the request it claimed to create, so a `GetAccessToken` call against a real
`AccessRequestId` had no state to look up even in principle. Implemented as a real
`*store.Table[AccessRequest]`-backed resource, auto-approved (documented — gopherstack has no
approver workflow to model, and leaving every request permanently "Pending" would make
`GetAccessToken` a dead end for every caller).

### parity-sweep-3: Patch baselines & Maintenance windows (previously deferred, now fully re-verified)

Patch baselines: `CreatePatchBaselineInput`/`PatchBaseline` were missing `ApprovalRules`
(auto-approval rules), `GlobalFilters`, `Sources` (custom Linux repos), `RejectedPatchesAction`,
`AvailableSecurityUpdatesComplianceStatus`, and `ApprovedPatchesEnableNonSecurity` — six fields
confirmed present in `api_op_CreatePatchBaseline.go` and entirely absent from this package. Added
as real, persisted, round-tripped fields (not evaluated against actual patch matching — same
scoping as the pre-existing `ApprovedPatches`/`RejectedPatches` handling, which was already
correctly scoped to storage-not-evaluation). Also: `GetPatchBaselineOutput.PatchGroups` (the patch
groups currently registered with a baseline) was entirely unpopulated — confirmed unique to
`GetPatchBaselineOutput` (absent from `UpdatePatchBaselineOutput`) via
`api_op_UpdatePatchBaseline.go`, now derived from the reverse `patchGroup->baselineID` map.

Maintenance windows: `RegisterTaskWithMaintenanceWindowInput.Targets` (the managed nodes/window-
targets a task runs against — required in practice for Run Command-type tasks per
`api_op_RegisterTaskWithMaintenanceWindow.go`) was accepted on the wire and silently discarded,
meaning a registered task could never actually record what it targets. Fixed through
Register/Update/Describe. `CreateMaintenanceWindowInput`/`UpdateMaintenanceWindowInput` were also
missing `StartDate`/`EndDate`/`ScheduleTimezone`/`ScheduleOffset` (confirmed present in both
`api_op_CreateMaintenanceWindow.go` and `api_op_UpdateMaintenanceWindow.go`), and
`AllowUnassociatedTargets` was previously create-only despite being documented as updatable — all
now round-trip (stored, not yet factored into schedule-execution logic — see `gaps`).

### parity-sweep-3: State Manager associations & OpsCenter (previously deferred, spot-checked only)

Ran out of scope budget to fully field-diff these two op-by-op after the epoch-seconds fix (which
itself touched `DescribeAssociationExecutions`) and the Session/PatchBaseline/MaintenanceWindow
work above consumed the pass. What was verified: `CreateAssociationInput` is missing ~10 fields
(`ApplyOnlyAtCronInterval`, `ComplianceSeverity`, `MaxConcurrency`, `MaxErrors`, `OutputLocation`,
`ScheduleExpression`, `SyncCompliance`, `CalendarNames`, `AssociationDispatchAssumeRole`,
`AutomationTargetParameterName`, `Duration`) confirmed absent against
`api_op_CreateAssociation.go` (bd: gopherstack-ouvq); `CreateOpsItemInput`/`UpdateOpsItemInput` are
missing `AccountId`/`ActualStartTime`/`ActualEndTime`/`Notifications`/`PlannedStartTime`/
`PlannedEndTime`/`RelatedOpsItems` (mostly Change-Manager `/aws/changerequest`-oriented) confirmed
absent against `api_op_CreateOpsItem.go`/`api_op_UpdateOpsItem.go` (bd: gopherstack-iq4m) — `Priority`
was the one field from that list fixed this pass since it's simple and broadly useful outside
Change Manager specifically. Per the audit brief's instruction, these are recorded honestly as
`partial`/open gaps with bd issues filed, not reclassified to `ok`.

### parity-3 phase-2 (2026-07-24): closing all remaining gaps from the 2026-07-23 A- pass

This pass closed all six items in the PARITY.md `gaps:` list as of 2026-07-23, four for real
(State Manager associations, OpsCenter, PatchBaseline pointer semantics, ListCloudConnectors/
ValidateCloudConnector MaxResults bounds) and one to the maximum extent this pass's constraints
allow (parameter policy notifications — implemented end-to-end except a single cli.go injection
line this agent was explicitly instructed not to touch). The sixth (ValidateCloudConnector's
inability to call a real Azure tenant) was re-confirmed as a genuine, unclosable sandbox
constraint.

**State Manager associations** (bd gopherstack-ouvq, closed): `CreateAssociationInput` was missing
11 fields confirmed present in `api_op_CreateAssociation.go` — `ApplyOnlyAtCronInterval`,
`AssociationDispatchAssumeRole`, `AutomationTargetParameterName`, `CalendarNames`,
`ComplianceSeverity`, `Duration`, `MaxConcurrency`, `MaxErrors`, `OutputLocation`,
`ScheduleExpression`, `SyncCompliance`. All 11 were added to `CreateAssociationInput`,
`CreateAssociationBatchRequestEntry` (confirmed the same 11 fields exist on
`types.CreateAssociationBatchRequestEntry`), `UpdateAssociationInput` (confirmed the same fields,
minus `Name`, on `api_op_UpdateAssociation.go` — UpdateAssociation was previously not even
mentioned in the gap, but leaving it unable to touch fields CreateAssociation could set would have
been a new, self-inflicted asymmetry), and the stored `Association`/`AssociationDescription` type
so they round-trip through Describe/List. Two new wire types were added to model
`OutputLocation`: `InstanceAssociationOutputLocation` (the `S3Location` wrapper) and
`S3OutputLocation` (`OutputS3BucketName`/`OutputS3KeyPrefix`/`OutputS3Region`) — field names and
nesting confirmed against `types.InstanceAssociationOutputLocation`/`types.S3OutputLocation`.
`UpdateAssociation`'s cyclomatic complexity crossed the package's cyclop limit (15) once the new
fields were wired in (17); split into `applyAssociationCoreUpdates`/`applyAssociationExtendedUpdates`
rather than adding a `//nolint:cyclop` per this campaign's hard constraint against banned nolints.

**OpsCenter** (bd gopherstack-iq4m, closed): `CreateOpsItemInput`/`UpdateOpsItemInput` were missing
`AccountId`, `ActualStartTime`, `ActualEndTime`, `Notifications`, `PlannedStartTime`,
`PlannedEndTime`, `RelatedOpsItems` — confirmed present on both `api_op_CreateOpsItem.go` and
`api_op_UpdateOpsItem.go`. All 7 added to `CreateOpsItemInput`, `UpdateOpsItemInput`, and the
stored `OpsItem` type (two new small wire types: `OpsItemNotification{Arn}`,
`RelatedOpsItemRef{OpsItemId}` — named `RelatedOpsItemRef` rather than reusing the existing
`OpsItemRelatedItem` type, since that's a different, unrelated resource: the associate/disassociate
"related item" feature keyed by `AssociationId`/`AssociationType`/`ResourceType`/`ResourceUri`, not
the `RelatedOpsItems` field's simple `{OpsItemId}` list). `ActualStartTime`/`ActualEndTime`/
`PlannedStartTime`/`PlannedEndTime` are modeled as `*float64` (this package's `UnixTimeFloat`
epoch-seconds convention, matching the systemic fix from the 2026-07-23 pass) rather than a bare
`float64`, since these are genuinely optional (Change-Manager-only in real AWS) and a bare
`float64` couldn't distinguish "not applicable" from epoch-zero.
`UpdateOpsItemInput.OperationalDataToDelete` (confirmed present in `api_op_UpdateOpsItem.go` but
NOT one of the 7 fields bd gopherstack-iq4m tracked) was deliberately left unimplemented — adding it
would have been scope creep beyond the specific field list this pass was asked to close; documented
in a comment in `models_ops_items.go` so it isn't mistaken for an oversight. `UpdateOpsItem`'s
cyclomatic complexity also crossed the cyclop limit once wired in; split into
`applyOpsItemCoreUpdates`/`applyOpsItemChangeManagerUpdates`.

**PatchBaseline.ApprovedPatchesEnableNonSecurity**: confirmed via `go doc` that
`CreatePatchBaselineInput`/`UpdatePatchBaselineInput`/`PatchBaseline` (aliased as
`GetPatchBaselineOutput`) all declare this field `*bool`, not `bool`. Converted across all three;
`CreatePatchBaseline`'s assignment was already a straight field copy (works unchanged for either
type), `UpdatePatchBaseline`'s `if input.ApprovedPatchesEnableNonSecurity { ... }` (which could only
ever read `true`) became `if input.ApprovedPatchesEnableNonSecurity != nil { ... }`, now able to
merge an explicit `false`. Locked in by a new table test,
`TestUpdatePatchBaseline_ApprovedPatchesEnableNonSecurityPointerSemantics`, covering all four
true/false x omitted/explicit combinations.

**ListCloudConnectors/ValidateCloudConnector MaxResults bounds**: the 2026-07-23 pass recorded this
as a legitimate proven-impossibility (checked both the SDK's Go doc comments and a public API
reference search at the time; neither had a published bound for this brand-new, July-2026-added API
family). Re-checked this pass via `WebFetch` against
`https://docs.aws.amazon.com/systems-manager/latest/APIReference/API_ListCloudConnectors.html` and
`.../API_ValidateCloudConnector.html` directly (not a search-engine summary, which surfaced
unreliable noise from an unrelated AWS IoT service also named "ListCloudConnectors" before the
direct fetch): both pages are now live and state exact bounds — `ListCloudConnectorsInput.MaxResults`
"Valid Range: Minimum value of 0. Maximum value of 10.", `ValidateCloudConnectorInput.MaxResults`
"Minimum value of 0. Maximum value of 75." These pages were apparently published sometime between
the 2026-07-23 and 2026-07-24 audit passes — the prior pass's "not yet published" finding was
accurate as of when it was made. Replaced the shared `defaultCloudConnectorMaxResults=50` guess
with two operation-specific constants (`listCloudConnectorsMaxResults=10`,
`validateCloudConnectorMaxResults=75`) and, matching this package's existing convention for
range-constrained `MaxResults` fields (`GetParametersByPath`, `DescribeParameters`), replaced the
previous silent accept-any-positive-value handling with a real `ValidationException` for any value
outside `[0, bound]` — including honoring the documented minimum of literal `0` (previously any
value `<= 0` silently fell back to the default instead of being treated as a valid explicit
request for zero items). Locked in by a new table test, `Test_CloudConnector_MaxResultsBounds`.

**ValidateCloudConnector's Azure-call limitation** (gap, not closed, genuinely can't be):
re-confirmed this pass. gopherstack has no Azure tenant, no Azure credentials, and — as a locally
running AWS-API emulator — no business making an unbounded outbound HTTPS call to a third-party
cloud provider from inside a request handler even if credentials were somehow available (no such
egress path exists in the test/dev sandbox this runs in, and doing so would be a meaningful
architectural and security departure from how every other emulated AWS service in this codebase
behaves — all state is local and deterministic). `ValidateCloudConnector`'s `ValidationFindings`
continue to be derived deterministically from the connector's own stored `Configuration`, which is
the same category of limitation as KMS being a local AES-256 emulation rather than a real HSM call.

### parity-3 phase-2 (2026-07-24): Parameter policy notifications (NoChangeNotification / ExpirationNotification)

Real AWS SSM enforces parameter policies via periodic background scans (confirmed via the
parameter-store-policies user guide: "Parameter Store enforces parameter policies by using
asynchronous, periodic scans") and, for `ExpirationNotification`/`NoChangeNotification`
specifically, publishes an EventBridge event when a policy becomes due (confirmed via
`sysman-paramstore-cwe.html`'s "Parameter policy" event pattern example): `source: "aws.ssm"`,
`detail-type: "Parameter Store Policy Action"`, `detail: {"parameter-name": <name>,
"policy-type": "Expiration"|"ExpirationNotification"|"NoChangeNotification"}`. Prior to this pass,
gopherstack stored and round-tripped the `Policies` string but never evaluated
`ExpirationNotification`/`NoChangeNotification` at all (only `Expiration` was enforced, via the
pre-existing janitor sweep that deletes the parameter outright).

Implemented as a new janitor ticker, `sweepParameterPolicyNotifications`
(`parameter_policy_notifications.go`, wired into `janitor.go`'s `Run`/`SweepOnce`), which on each
tick:
1. Parses every parameter's `Policies` JSON into a generic `parameterStorePolicy{Type, Version,
   Attributes}` shape (rather than the pre-existing `Expiration`-only `parameterExpirationPolicy`).
2. Computes a due-time for each `ExpirationNotification` (`Before` `Unit` ahead of the parameter's
   `Expiration` policy timestamp, if any — an `ExpirationNotification` with no `Expiration` policy
   on the same parameter has nothing to count down to and never fires) and `NoChangeNotification`
   (`After` `Unit` after `LastModifiedDate`) policy, supporting both AWS's documented `Unit` values
   (`Days`, `Hours`).
3. Reports each newly-due policy instance through an injectable `ParameterPolicyNotifier` interface
   (`NotifyParameterPolicyAction(ctx, parameterName, policyType) error`) — the same
   injected-cross-service-hook pattern `services/stepfunctions/asl.EventBridgeIntegration` uses,
   deliberately keeping this package free of any direct dependency on `services/eventbridge`.
4. Dedupes per (parameter name, policy Type+Attributes) so a policy instance notifies at most once
   until the parameter is next written — `PutParameter` always resets `LastModifiedDate` and
   wholesale-replaces `Policies` (confirmed via the user guide: "If you add a new policy to a
   parameter that already has policies, Systems Manager overwrites the policies attached to the
   parameter"; and specifically for `NoChangeNotification`: "If you change or edit a parameter, the
   system resets the notification time period"), so `PutParameter`/`DeleteParameter`/
   `DeleteParameters`/the existing expiry sweep all now call
   `clearParameterPolicyNotificationStateLocked` to invalidate/cascade-clean this dedupe state —
   proven by `TestSSMJanitor_ParameterPolicyNotifications`'s `put_parameter_resets_dedupe_state` and
   `delete_then_recreate_leaves_no_ghost_dedupe_state` cases.
5. A `nil` notifier (the default, until wired) makes the whole sweep a cheap no-op that evaluates
   and marks nothing — so a policy that becomes due before a real notifier is injected is still
   reported once the notifier lands, rather than being silently lost to premature dedup-marking.

New backend state: `notifiedParameterPolicies map[string]map[string]map[string]struct{}` (region ->
parameter name -> dedupe key), added to `store.go` (init in `NewInMemoryBackend`/`Reset`) and fully
wired into `persistence.go`'s `Snapshot`/`Restore` (own JSON field, `initSnapshotPatchOpsFields`-style
nil-init on restore of an older snapshot).

**The real EventBridge-side adapter is implemented, not deferred**: `services/eventbridge/
ssm_integration.go` adds `(*eventbridge.InMemoryBackend).NotifyParameterPolicyAction`, implementing
`ssm.ParameterPolicyNotifier` directly on the EventBridge backend (mirroring
`services/eventbridge/sfn_integration.go`'s `SFNPutEvents` — a provider-package adapter satisfying a
consumer package's interface by name, so no wrapper struct is needed), translating the notification
into a real `PutEvents` call with the exact documented wire shape. Proven end-to-end by
`TestNotifyParameterPolicyAction` (`services/eventbridge/put_events_test.go`), which uses an
EventBridge Archive with an `EventPattern` matching the exact `source`/`detail-type`/`policy-type`
values as an independent observer — the archive's `EventCount` only increments if the published
event genuinely matches that wire shape, not merely "PutEvents returned no error". A compile-time
assertion (`var _ ssm.ParameterPolicyNotifier = (*eventbridge.InMemoryBackend)(nil)`) locks in that
the adapter satisfies the interface.

**cli.go wiring still needed** (the one deliberately-undone piece, per this agent's explicit
instruction not to touch cli.go): a single call, following the existing pattern already in
`wireStepFunctionsServiceIntegrations` (e.g. `sfnBk.SetEventBridgeIntegration(ebBk)`), needs to be
added wherever the SSM and EventBridge backends are both resolved from the service registry:

```go
if ssmH, ok := ssmReg.(*ssmbackend.Handler); ok {
    if ssmBk, ok := ssmH.Backend.(*ssmbackend.InMemoryBackend); ok {
        if ebH, ok := ebReg.(*ebbackend.Handler); ok {
            if ebBk, ok := ebH.Backend.(*ebbackend.InMemoryBackend); ok {
                ssmBk.SetParameterPolicyNotifier(ebBk)
            }
        }
    }
}
```

Until this lands, `SetParameterPolicyNotifier` is never called in the running binary, so
`b.parameterPolicyNotifier` stays `nil` and the janitor sweep remains a no-op in production exactly
as it was before this pass — this pass changes nothing observable for a real client until that one
line is added, by design (no risk of a half-wired feature misbehaving in the interim).
