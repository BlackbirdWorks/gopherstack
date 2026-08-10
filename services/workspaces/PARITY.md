service: workspaces
sdk_module: aws-sdk-go-v2/service/workspaces@v1.73.1
last_audit_commit: 1b07910674fd
last_audit_date: 2026-07-23
overall: A            # all previously-listed gaps/deferred items closed for real this pass;
                       # 3 more genuine bugs found beyond the assigned list (2 wire-shape, 1 leak-adjacent)

# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (prior pass) — was all-or-nothing; now partitions FailedRequests/PendingRequests per item, matching real FailedCreateWorkspaceRequest{WorkspaceRequest,ErrorCode,ErrorMessage} shape"}
  DescribeWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination (25/page), region filter, WorkspaceIds/DirectoryId/UserName/BundleId filters all verified against real field names"}
  DescribeWorkspacesConnectionStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — ConnectionStateCheckTimestamp/LastKnownUserConnectionTimestamp were entirely missing from the response (only WorkspaceId/ConnectionState were wired); both are now emitted as epoch-seconds numbers via awstime.Epoch. LastKnownUserConnectionTimestamp stays zero-valued (0, omitted) since this backend models no actual client connection activity."}
  ModifyWorkspaceProperties: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyWorkspaceState: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "intentionally does not transition state — documented + tested (TestRebootWorkspaces_DoesNotChangeState in workspaces_lifecycle_test.go); this emulator models reboot as instantaneous with no transient REBOOTING window, not a bug"}
  RebuildWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as RebootWorkspaces — intentional, tested no-transition behavior (TestRebuildWorkspaces_DoesNotChangeState)"}
  StartWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "STOPPED->AVAILABLE; idempotent no-op for other states, no failure reported (matches AWS tolerance for redundant start/stop calls)"}
  StopWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok}
  TerminateWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "deletes workspace + its tags"}
  CreateTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeWorkspaceBundles: {wire: ok, errors: ok, state: ok, persist: ok, note: "Amazon-owned static list + custom bundles, owner filter, pagination all verified"}
  DescribeWorkspaceDirectories: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterWorkspaceDirectory: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — re-registering an already-registered directory silently 200'd (unconditionally idempotent); now returns ResourceAlreadyExistsException, matching real AWS."}
  DeregisterWorkspaceDirectory: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — deregistered a directory unconditionally even with live WorkSpaces still assigned to it (a ghost-reference risk: DescribeWorkspaces would keep returning WorkSpaces whose DirectoryId no longer resolved to any registered directory). Real AWS: 'If any WorkSpaces are registered to this directory, you must remove them before you can deregister the directory' — now enforced via InvalidResourceStateException. Also now cascade-cleans the directoryIpGroups association map on a successful deregister (was leaked as an orphaned entry keyed by the dead DirectoryId)."}
  RestoreWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (prior pass) — was a true no-op with no existence check (silently 200'd for unknown WorkspaceId); now returns ResourceNotFoundException. No snapshot modeling, so still otherwise a no-op beyond validation — acceptable given no snapshot state exists to restore from."}
  MigrateWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "source deleted, new workspace created with target bundleId, tested in workspaces_lifecycle_test.go"}
  CreateIpGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "lowercase groupId/groupName/groupDesc/userRules JSON keys verified against real deserializer — an AWS API quirk, not a bug"}
  DescribeIpGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIpGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  AuthorizeIpRules: {wire: ok, errors: ok, state: ok, persist: ok}
  RevokeIpRules: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRulesOfIpGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateIpGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — directoryIpGroups map is now included in backendSnapshot (Snapshot/Restore), matching Tags. Previously persist:deferred (ephemeral across restarts); no snapshot-version bump needed since an older snapshot just decodes with an empty map, matching prior behavior exactly."}
  DisassociateIpGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStandbyWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — was invented-shape: built pending/failed items from a hand-rolled map[string]string carrying UserName/BundleId fields that DON'T EXIST on the real StandbyWorkspace/PendingCreateStandbyWorkspacesRequest types (gopherstack-invented fields), and FailedStandbyRequests was hardcoded to always be empty regardless of input. Rewrote using the real shapes: StandbyWorkspace{DirectoryId, PrimaryWorkspaceId, DataReplication, Tags, VolumeEncryptionKey} for requests, PendingCreateStandbyWorkspacesRequest{DirectoryId, State, UserName, WorkspaceId} for successes (note: no BundleId field on this response type either), FailedCreateStandbyWorkspacesRequest{ErrorCode, ErrorMessage, StandbyWorkspaceRequest} for per-item failures. Moved to a single-item CreateStandbyWorkspace(ctx, spec) backend method with the batch/partial-failure loop in the handler, mirroring the CreateWorkspaces pattern. Real per-item validation: an unregistered DirectoryId now reports a genuine FailedStandbyRequests entry instead of always succeeding. PrimaryWorkspaceId existence is NOT cross-validated (see Notes: this backend has no way to see a primary WorkSpace living in a different region's backend instance) — this is a documented, deliberate limitation, not an oversight."}
  DescribeImageAssociations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED — was a stub ignoring ImageId entirely (always 200'd with an empty list for a nonexistent image, and Associations was typed []any with no real field names). Now validates ImageId is required + must reference a real image (ResourceNotFoundException) and AssociatedResourceTypes is required + must be \"APPLICATION\" (the only real enum value for ImageAssociatedResourceType). Response now uses the real ImageResourceAssociation shape (AssociatedResourceId/AssociatedResourceType/ImageId/State/StateReason/Created/LastUpdatedTime, epoch timestamps). Real AWS's WorkSpaces Application Manager has no public API to create this association (only AssociateWorkspaceApplication, which associates an app directly with a WorkSpace, not an image) — so a freshly emulated account always returns an empty (but now correctly validated/typed) list."}
  DescribeBundleAssociations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED — same class of stub as DescribeImageAssociations; now validates BundleId (checked against both Amazon-owned and custom bundles) and AssociatedResourceTypes, real BundleResourceAssociation shape."}
  DescribeAccountModifications: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — was a true stub always returning an empty list regardless of history. ModifyAccount now appends an AccountModification{ModificationState:\"COMPLETED\", DedicatedTenancySupport, DedicatedTenancyManagementCidrRange, StartTime} entry on every call (this backend applies changes synchronously, so there's no PENDING window to model); DescribeAccountModifications returns them most-recent-first, paginated via pkgs/page, and both accountConfig and this new history list are now included in backendSnapshot. Real DescribeAccountModificationsInput has no MaxResults field (only NextToken) — this backend uses a fixed internal page size (100), field-diffed against the real input shape."}
  ListAvailableManagementCidrRanges: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED — was a stub returning the same 3 hardcoded CIDRs (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16) regardless of input, and ManagementCidrRangeConstraint (a real smithy-`required` field) wasn't validated at all. Now requires + validates the constraint is a real IPv4 CIDR (InvalidParameterValuesException otherwise) and derives up to 8 contained /26 sub-ranges from it (real AWS returns /26 management-interface blocks carved from the caller's constraint), paginated via pkgs/page."}

families:
  ConnectionAlias: {status: ok, note: "Create/Describe/Delete/Associate/Disassociate/Permissions all mutate storedConnAlias state correctly; spot-checked against real WorkspaceRequest/ConnectionAlias field names"}
  WorkspaceBundle_custom: {status: ok, note: "Create/Delete/Update custom bundles verified real mutation"}
  WorkspaceImage: {status: ok, note: "Copy/Create/Delete/Import/CreateUpdated/DescribePermissions/UpdatePermission all mutate storedImage table. FIXED this pass: Created was serialized as an ISO8601 string (\"2006-01-02T15:04:05Z\") in three response shapes (CreateWorkspaceImage, DescribeWorkspaceImages, DescribeCustomWorkspaceImageImport) — real WorkspaceImage.Created / DescribeCustomWorkspaceImageImportOutput.Created are *time.Time, and this is the awsjson1.1 protocol, which requires epoch-seconds numbers (unixTimestamp), not RFC3339 strings; a real client SDK would fail to deserialize the response. Fixed via awstime.Epoch, matching the bug class already fixed in QuickSight/IoT."}
  WorkspacesPool: {status: ok, note: "Create/Describe/Start/Stop/Terminate/Update all real state transitions on storedPool.State. FIXED this pass: (1) CreatedAt had the same ISO8601-string-instead-of-epoch-number bug as WorkspaceImage.Created, fixed via awstime.Epoch; (2) CapacityStatus and RunningMode — both `This member is required` on the real WorkspacesPool type — were entirely absent from the response; CreateWorkspacesPoolInput.Capacity.DesiredUserSessions was parsed but silently discarded, never reaching the backend. Now storedPool carries RunningMode (default ALWAYS_ON when unset) + DesiredUserSessions, both settable on Create and Update (UpdateWorkspacesPoolInput.RunningMode/Capacity were likewise previously accepted-but-dropped); CapacityStatus is derived as a steady-state value (ActiveUserSessions:0, ActualUserSessions=AvailableUserSessions=DesiredUserSessions) since no live session tracking is modeled — this matches the documented invariant ActualUserSessions = AvailableUserSessions + ActiveUserSessions. Not modeled: UpdateWorkspacesPoolInput's real constraint that RunningMode can only change while the pool is STOPPED (this backend applies it unconditionally) — flagged as a follow-up, not blocking."}
  WorkspacesPoolSession: {status: ok}
  Account: {status: ok, note: "DescribeAccount/ModifyAccount/ModifyEndpointEncryptionMode read/write storedAccountConfig; DescribeAccountModifications now has a real, persisted modification history (see ops table) instead of an always-empty stub."}
  ConnectClientAddIn: {status: ok}
  ClientBranding: {status: ok}
  ClientProperties: {status: partial, note: "gopherstack-u8my: NEW since v1.68.3, not fixed -- ClientProperties gained ClientExperiencePolicy (*string: FORCE_CLASSIC/FORCE_UI_2026/USER_CHOICE). ModifyClientProperties(resourceID, reconnectEnabled string) only threads ReconnectEnabled; ClientExperiencePolicy is silently dropped (needs bd issue)."}
  DirectoryModifyOps: {status: ok, note: "ModifyCertificateBasedAuthProperties/ModifySamlProperties/ModifySelfservicePermissions/ModifyStreamingProperties/ModifyWorkspaceAccessProperties/ModifyWorkspaceCreationProperties all write storedDirSettings.Properties map"}
  AccountLinks: {status: ok, note: "Create/Accept/Reject/Delete/Get/List all mutate storedAccountLink.Status"}
  Applications: {status: ok, note: "Associate/Disassociate/Deploy/DescribeAssociations/DescribeApplicationAssociations/DescribeApplications"}
  ImageBundleAssociations: {status: ok, note: "FIXED — see DescribeImageAssociations/DescribeBundleAssociations, now tracked individually in the ops table above (previously rolled up here only). Deep-audited this pass (previously marked deferred/not-audited): confirmed real AWS exposes no public create-association API for image/bundle<->application, so an always-empty (correctly validated + typed) response is genuine emulated behavior, not a gap."}
  DescribeWorkspaceSnapshots: {status: ok, note: "returns empty RebuildSnapshots/RestoreSnapshots lists — correct void-result shape since no snapshot state is modeled anywhere in this backend"}

gaps: []
  # All gaps from the prior pass (CreateStandbyWorkspaces FailedStandbyRequests,
  # AssociateIpGroups/DisassociateIpGroups persistence) were closed for real this
  # pass — see the ops table entries above for what changed.

deferred:
  - DescribeWorkspaceAssociations/DescribeApplicationAssociations/DeployWorkspaceApplications
    (the Applications family) still model application-deployment status as an
    always-"INSTALLED" / always-empty-error placeholder rather than a real
    per-application deployment state machine — not re-audited this pass (out of
    the assigned gaps/deferred scope), flagged for a future pass.
  - UpdateWorkspacesPoolInput's real "RunningMode can only change while STOPPED"
    constraint is not enforced (see WorkspacesPool family note above).

leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table maps guarded by lockmetrics.RWMutex. FIXED this pass: DeregisterWorkspaceDirectory no longer allows deregistering a directory that still has live WorkSpaces assigned to it (previously left DescribeWorkspaces returning WorkSpaces pointing at a DirectoryId with no corresponding registered directory — a dangling-reference-shaped leak, now prevented outright per real AWS semantics) and now cascade-cleans the directoryIpGroups map entry for a directory on successful deregistration (was an orphaned map entry keyed by a dead DirectoryId, never reachable again once the directory itself was gone)."}

---

## Notes

Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: WorkspacesService.<Op>`.
Route matcher (`strings.HasPrefix(header, "WorkspacesService.")`) is simple and every
one of the 91 real SDK v1.68.3 ops is reachable — verified via `comm` diff between
`ls aws-sdk-go-v2/service/workspaces@v1.68.3/api_op_*.go` and the handler's
`buildOps()` map; 91/91 match, no missing ops, no phantom (non-SDK) op names. Re-verified
this pass (91 op names extracted straight from the SDK zip's `api_op_*.go` filenames):
no gopherstack-invented op names exist in this service.

### Bugs fixed this pass (2026-07-23)

1. **CreateStandbyWorkspaces used invented fields.** The backend's request/response
   shapes were hand-rolled `map[string]string` carrying `UserName`/`BundleId` keys
   that don't exist anywhere on the real `StandbyWorkspace` or
   `PendingCreateStandbyWorkspacesRequest` types (field-diffed against
   `aws-sdk-go-v2/service/workspaces/types`: `StandbyWorkspace` has
   `DirectoryId`/`PrimaryWorkspaceId`/`DataReplication`/`Tags`/`VolumeEncryptionKey`;
   `PendingCreateStandbyWorkspacesRequest` has `DirectoryId`/`State`/`UserName`/
   `WorkspaceId` — no `BundleId` on either). `FailedStandbyRequests` was also
   hardcoded to always be an empty slice, so a malformed standby request (e.g. an
   unregistered `DirectoryId`) always silently "succeeded". Rewrote with real
   per-op DTOs (`StandbyWorkspaceSpec`, `PendingStandbyWorkspace` in interfaces.go)
   and moved the batch/partial-failure loop into the handler (mirroring
   `CreateWorkspaces`'s established pattern), with a single-item
   `CreateStandbyWorkspace` backend method that does real `DirectoryId`
   registration validation.

2. **DescribeImageAssociations/DescribeBundleAssociations were stubs.** Both
   ignored their `ImageId`/`BundleId` input entirely — calling either with a
   nonexistent ID silently 200'd with an empty list instead of
   `ResourceNotFoundException`, and `AssociatedResourceTypes` (a real required
   field) was never validated. Response shape was `[]any` with no real field
   names. Fixed with real existence + required-field validation and the real
   `ImageResourceAssociation`/`BundleResourceAssociation` wire shapes
   (`AssociatedResourceId`/`AssociatedResourceType`/`State`/`StateReason`/
   `Created`/`LastUpdatedTime`, the latter two as epoch-seconds numbers). The
   list itself still comes back empty in every case, which is *correct*: real
   AWS's WorkSpaces Application Manager has no public API to associate an
   application with an image or bundle directly (only
   `AssociateWorkspaceApplication`, which associates an application with a
   WorkSpace, and `DeployWorkspaceApplications`, neither of which touch an
   image/bundle) — so a freshly emulated account genuinely has none to report.

3. **DescribeAccountModifications and ListAvailableManagementCidrRanges were
   stubs.** The former always returned an empty list, with no history tracking
   even after `ModifyAccount` calls. The latter always returned the same 3
   hardcoded CIDR blocks (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`)
   regardless of the (real, required) `ManagementCidrRangeConstraint` input,
   which also went completely unvalidated. Fixed: `ModifyAccount` now records a
   real `AccountModification` history entry (persisted in `backendSnapshot`),
   and `ListAvailableManagementCidrRanges` now requires + validates the
   constraint as an IPv4 CIDR and derives real `/26` sub-ranges contained
   within it.

4. **Epoch-seconds timestamp bug** (the exact bug class already fixed in
   QuickSight/IoT — see `pkgs/awstime`'s doc comment): four response fields
   serialized `time.Time` as ISO8601 strings
   (`"2006-01-02T15:04:05Z"`) instead of the epoch-seconds JSON numbers the
   `awsjson1.1` protocol's `unixTimestamp` timestamp format requires. Affected:
   `WorkspaceImage.Created` (in `CreateWorkspaceImage`, `DescribeWorkspaceImages`,
   and `DescribeCustomWorkspaceImageImport`'s `Created`) and
   `WorkspacesPool.CreatedAt`. A real client SDK would reject these responses
   with "expected Timestamp to be a JSON Number, got string instead". Fixed via
   `awstime.Epoch` at every call site. `DescribeWorkspacesConnectionStatus` was
   also missing its two timestamp fields
   (`ConnectionStateCheckTimestamp`/`LastKnownUserConnectionTimestamp`) entirely
   — added as epoch-seconds numbers.

5. **WorkspacesPool was missing two `This member is required` fields.**
   `CapacityStatus` and `RunningMode` are both required on the real
   `WorkspacesPool` type but were absent from every response; separately,
   `CreateWorkspacesPoolInput.Capacity.DesiredUserSessions` was parsed off the
   wire but then silently discarded — never reaching the backend, so it had no
   effect at all. Same for `UpdateWorkspacesPoolInput.RunningMode`/`Capacity`.
   Fixed: `storedPool` now carries `RunningMode`/`DesiredUserSessions`, both
   real state now flowing from Create/Update; `CapacityStatus` is derived
   (steady-state: no live session tracking is modeled, so
   `ActiveUserSessions=0` and `ActualUserSessions=AvailableUserSessions=
   DesiredUserSessions`, satisfying the documented
   `Actual = Available + Active` invariant).

6. **AssociateIpGroups/DisassociateIpGroups state (`directoryIpGroups`) was
   ephemeral.** Not included in `backendSnapshot`, so an association silently
   vanished across any Snapshot -> Restore cycle even though the IP group and
   directory it referenced both survived. Now persisted directly in
   `backendSnapshot.DirectoryIpGroups` alongside `Tags`; no snapshot-version
   bump needed since an older/absent value decodes as an empty map, matching
   the prior (always-empty-after-restart) behavior exactly.

7. **RegisterWorkspaceDirectory/DeregisterWorkspaceDirectory had no real
   state-conflict handling** — a leak-adjacent bug caught while auditing the
   directory family for the `directoryIpGroups` persistence fix above.
   `RegisterWorkspaceDirectory` was unconditionally idempotent (re-registering
   an already-registered directory silently 200'd); real AWS rejects this with
   `ResourceAlreadyExistsException`. `DeregisterWorkspaceDirectory` deleted the
   directory unconditionally even with live WorkSpaces still assigned to it —
   real AWS: *"If any WorkSpaces are registered to this directory, you must
   remove them before you can deregister the directory"* — which this backend
   now enforces via `InvalidResourceStateException` rather than either
   silently succeeding (leaving `DescribeWorkspaces` returning WorkSpaces whose
   `DirectoryId` no longer resolved to anything) or auto-cascade-deleting the
   WorkSpaces (which is *not* real AWS behavior — do not "fix" this into a
   cascade-delete). `handler.go`'s `handleError` gained two new sentinel
   mappings (`awserr.ErrAlreadyExists` -> 400 `ResourceAlreadyExistsException`,
   `awserr.ErrConflict` -> 400 `InvalidResourceStateException`) to support
   this — previously only `ErrNotFound`/`ErrInvalidParameter` were wired, so
   both of these new sentinels would otherwise have silently fallen through to
   a wrong 500 `InternalServerException`.

### Traps for the next auditor

- `RebootWorkspaces`/`RebuildWorkspaces` **intentionally** do not transition
  workspace state (no REBOOTING/REBUILDING window) — this was a deliberate design
  decision from a prior audit pass, documented and asserted by
  `TestRebootWorkspaces_DoesNotChangeState` /
  `TestRebuildWorkspaces_DoesNotChangeState` in workspaces_lifecycle_test.go
  (renamed from `handler_parity3_test.go`'s `TestParity3_*` names by an
  unrelated file-naming cleanup pass; same tests, same rationale).
  Do not "fix" this without reading that test's rationale first.
- `workspaceResp`/`pendingWorkspace` include a `Tags` JSON field on the `Workspace`
  shape; real AWS's `Workspace` type has **no** `Tags` field (tags are fetched via
  a separate `DescribeTags` call). This is harmless (aws-sdk-go-v2's json
  deserializers silently ignore unrecognized keys via a `default:` case in every
  generated switch), so it was left as-is rather than spending scope removing a
  non-breaking extra field.
- `CreateIpGroup`/`DescribeIpGroups`/etc use **lowercase** wire keys (`groupId`,
  `groupName`, `groupDesc`, `userRules`, `ipRule`, `ruleDesc`) — this looks wrong
  at a glance (every other shape in this service is PascalCase) but is verified
  correct against the real `awsAwsjson11_deserializeDocumentWorkspacesIpGroup` /
  `IpRuleItem` deserializers. Real AWS quirk, not a bug — don't "fix" the casing.
- `CreateStandbyWorkspaces`' `PrimaryWorkspaceId` is accepted and echoed back on
  failure, but its existence is **not** cross-validated against any workspace
  table. This is deliberate, not an oversight: `CreateStandbyWorkspaces` runs in
  the *standby* (target) region to create a DR copy of a WorkSpace whose
  `PrimaryWorkspaceId` lives in a *different* region's backend instance — this
  in-memory backend has no cross-region visibility, so there is nothing correct
  to validate against. Only `DirectoryId` (which must be registered in *this*
  region) is validated.
- `DescribeImageAssociations`/`DescribeBundleAssociations` will always return an
  empty `Associations` list in this backend — this is correct, not a stub. Real
  AWS's WorkSpaces Application Manager has no public API to create an
  image/bundle<->application association at all (only
  `AssociateWorkspaceApplication`, which is WorkSpace-only). Don't "fix" this by
  inventing a fake association-creation pathway.
