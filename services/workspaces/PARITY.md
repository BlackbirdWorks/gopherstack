service: workspaces
sdk_module: aws-sdk-go-v2/service/workspaces@v1.68.3
last_audit_commit: 1b07910674fd
last_audit_date: 2026-07-12
overall: A            # 3 genuine fixes found (1 systemic, 1 batch-semantics, 1 disguised no-op)

# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — was all-or-nothing; now partitions FailedRequests/PendingRequests per item, matching real FailedCreateWorkspaceRequest{WorkspaceRequest,ErrorCode,ErrorMessage} shape"}
  DescribeWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination (25/page), region filter, WorkspaceIds/DirectoryId/UserName/BundleId filters all verified against real field names"}
  DescribeWorkspacesConnectionStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyWorkspaceProperties: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyWorkspaceState: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "intentionally does not transition state — documented + tested in handler_parity3_test.go (item 16); this emulator models reboot as instantaneous with no transient REBOOTING window, not a bug"}
  RebuildWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as RebootWorkspaces — intentional, tested no-transition behavior"}
  StartWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "STOPPED->AVAILABLE; idempotent no-op for other states, no failure reported (matches AWS tolerance for redundant start/stop calls)"}
  StopWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok}
  TerminateWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "deletes workspace + its tags"}
  CreateTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeWorkspaceBundles: {wire: ok, errors: ok, state: ok, persist: ok, note: "Amazon-owned static list + custom bundles, owner filter, pagination all verified"}
  DescribeWorkspaceDirectories: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterWorkspaceDirectory: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterWorkspaceDirectory: {wire: ok, errors: ok, state: ok, persist: ok}
  RestoreWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — was a true no-op with no existence check (silently 200'd for unknown WorkspaceId); now returns ResourceNotFoundException. No snapshot modeling, so still otherwise a no-op beyond validation — acceptable given no snapshot state exists to restore from."}
  MigrateWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "source deleted, new workspace created with target bundleId, tested in handler_parity3_test.go item 17"}
  CreateIpGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "lowercase groupId/groupName/groupDesc/userRules JSON keys verified against real deserializer — an AWS API quirk, not a bug"}
  DescribeIpGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIpGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  AuthorizeIpRules: {wire: ok, errors: ok, state: ok, persist: ok}
  RevokeIpRules: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRulesOfIpGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateIpGroups: {wire: ok, errors: ok, state: ok, persist: deferred, note: "directoryIpGroups map intentionally ephemeral (pre-existing design, see backend.go field comment)"}
  DisassociateIpGroups: {wire: ok, errors: ok, state: ok, persist: deferred}

families:
  ConnectionAlias: {status: ok, note: "Create/Describe/Delete/Associate/Disassociate/Permissions all mutate storedConnAlias state correctly; spot-checked against real WorkspaceRequest/ConnectionAlias field names"}
  WorkspaceBundle_custom: {status: ok, note: "Create/Delete/Update custom bundles verified real mutation"}
  WorkspaceImage: {status: ok, note: "Copy/Create/Delete/Import/CreateUpdated/DescribePermissions/UpdatePermission all mutate storedImage table"}
  WorkspacesPool: {status: ok, note: "Create/Describe/Start/Stop/Terminate/Update all real state transitions on storedPool.State"}
  WorkspacesPoolSession: {status: ok}
  Account: {status: ok, note: "DescribeAccount/ModifyAccount/ModifyEndpointEncryptionMode read/write storedAccountConfig; DescribeAccountModifications returns empty list (no modification-history tracking, acceptable — matches steady-state accounts)"}
  ConnectClientAddIn: {status: ok}
  ClientBranding: {status: ok}
  ClientProperties: {status: ok}
  DirectoryModifyOps: {status: ok, note: "ModifyCertificateBasedAuthProperties/ModifySamlProperties/ModifySelfservicePermissions/ModifyStreamingProperties/ModifyWorkspaceAccessProperties/ModifyWorkspaceCreationProperties all write storedDirSettings.Properties map"}
  AccountLinks: {status: ok, note: "Create/Accept/Reject/Delete/Get/List all mutate storedAccountLink.Status"}
  Applications: {status: ok, note: "Associate/Disassociate/Deploy/DescribeAssociations/DescribeApplicationAssociations/DescribeApplications"}
  ImageBundleAssociations: {status: deferred, note: "DescribeImageAssociations/DescribeBundleAssociations not deep-audited this pass — low traffic, no evidence of stubbing found in spot check"}
  CreateStandbyWorkspaces: {status: ok, note: "creates real storedWorkspace entries in PENDING state; FailedStandbyRequests always empty (no per-item validation modeled) — deferred, see gaps"}
  DescribeWorkspaceSnapshots: {status: ok, note: "returns empty RebuildSnapshots/RestoreSnapshots lists — correct void-result shape since no snapshot state is modeled anywhere in this backend"}
  ListAvailableManagementCidrRanges: {status: deferred, note: "not deep-audited this pass"}

gaps:
  - CreateStandbyWorkspaces never reports FailedStandbyRequests (e.g. for a missing DirectoryId) — always returns an empty failure list even when a standby spec is malformed. Lower priority than the CreateWorkspaces fix since CreateStandbyWorkspaces is a Multi-Region DR feature with much lower call volume. (bd: file follow-up)
  - AssociateIpGroups/DisassociateIpGroups state (directoryIpGroups map) is not persisted across Snapshot/Restore — pre-existing, documented design choice (see backend.go InMemoryBackend field comment), not newly introduced.

deferred:
  - DescribeImageAssociations / DescribeBundleAssociations
  - ListAvailableManagementCidrRanges
  - DescribeAccountModifications (returns empty list; no modification-history tracking)

leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table maps guarded by lockmetrics.RWMutex"}

---

## Notes

Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: WorkspacesService.<Op>`.
Route matcher (`strings.HasPrefix(header, "WorkspacesService.")`) is simple and every
one of the 91 real SDK v1.68.3 ops is reachable — verified via `comm` diff between
`ls aws-sdk-go-v2/service/workspaces@v1.68.3/api_op_*.go` and the handler's
`buildOps()` map; 91/91 match, no missing ops, no phantom (non-SDK) op names.

### Bugs fixed this pass

1. **Systemic `awserr.Newf` argument-order bug** (backend.go, handler.go — 8 call
   sites). `pkgs/awserr.Newf(msg string, sentinel error, args ...any)` formats `msg`
   with `args`. Every call site in this service had the arguments in the wrong
   order: `awserr.Newf(errInvalidParameterValues, awserr.ErrInvalidParameter,
   "descriptive %s text", val)` — i.e. the *error code constant* (no format verbs)
   was passed as the format string, and the real format string + its args were
   passed as extra `args`, which `fmt.Sprintf` appends as a `%!(EXTRA
   type=value, ...)` suffix instead of substituting. Every InvalidParameterValuesException
   response body's `message` field was garbled (e.g.
   `"InvalidParameterValuesException%!(EXTRA string=directory %q is not
   registered, string=d-abc)"`) instead of describing the actual problem. HTTP
   status code and `__type` were unaffected (those come from `errors.Is` sentinel
   matching and a hardcoded constant in `handleError`, not from the message text),
   so this was invisible to status-code-only tests — only the message body was
   wrong. Fixed by swapping every call to `Newf(formatString, sentinel, args...)`
   or `New(literalMsg, sentinel)` when there were no format args at all.
   `pkgs/awserr` itself is correct and untouched (that's a shared file outside this
   service's scope); only the call sites here were wrong. No other service in the
   repo calls `awserr.Newf` at all, so this bug pattern is unique to workspaces.

2. **CreateWorkspaces was all-or-nothing**, aborting the whole batch with a
   top-level 400 on the first per-item runtime failure (e.g. an unregistered
   DirectoryId), instead of AWS's partial-failure batch semantics: report that one
   `WorkspaceRequest` in `FailedRequests` and still create the rest. Real
   `FailedCreateWorkspaceRequest` has shape `{ErrorCode, ErrorMessage,
   WorkspaceRequest}` — note this is a *different* shape from the other bulk ops'
   `FailedWorkspaceChangeRequest{WorkspaceId, ErrorCode, ErrorMessage}` (no
   WorkspaceId, since the WorkSpace was never created; instead the original
   request is echoed back). Added `workspaceRequestResp`/`failedCreateWorkspaceItem`
   types and `classifyCreateError` to build this correctly. Structural
   validation (`Workspaces` list empty/too-large, or a spec missing a
   smithy-`required` field like UserName/DirectoryId/BundleId) correctly remains a
   whole-request 400 — those are shape-level validation failures a real client SDK
   would catch before ever sending the request, distinct from runtime/semantic
   failures like an unregistered directory.

3. **RestoreWorkspace was a true no-op**: it never checked whether the given
   WorkspaceId existed, so calling it with a nonexistent ID silently returned 200
   instead of `ResourceNotFoundException`. This backend does not model WorkSpace
   snapshots, so the operation is still a no-op beyond validation — that's an
   accepted limitation (see gaps), but the existence check is real AWS behavior
   and now enforced.

### Traps for the next auditor

- `RebootWorkspaces`/`RebuildWorkspaces` **intentionally** do not transition
  workspace state (no REBOOTING/REBUILDING window) — this was a deliberate design
  decision from a prior audit pass, documented and asserted by
  `TestParity3_RebootWorkspaces_DoesNotChangeState` /
  `TestParity3_RebuildWorkspaces_DoesNotChangeState` in handler_parity3_test.go.
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
