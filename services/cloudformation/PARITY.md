---
service: cloudformation
sdk_module: aws-sdk-go-v2/service/cloudformation@v1.71.7
last_audit_commit: 6548cf87
last_audit_date: 2026-07-05
overall: A            # genuine fixes found and applied in the audited families; full
                       # ~42k-LOC surface not re-proven op-by-op this pass (see deferred)
ops:
  CreateStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "CAPABILITY_AUTO_EXPAND no longer wrongly satisfies the IAM-resource capability check (backend_parity.go requireIAMCapability)"}
  UpdateStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: missing UPDATE_FAILED stack event on template parse failure; added pre-flight export-in-use block (validateExportsStillInUse)"}
  DeleteStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now idempotent (no-op, not ErrStackNotFound) per AWS's unmodeled DeleteStack error surface; added export-in-use block (stackExportsInUse)"}
  DescribeStacks: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStacks: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was returning the full unpaginated event history every call, ignoring NextToken entirely; now uses pkgs/page like the other List* ops"}
  GetTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStackResources: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackResources: {wire: ok, errors: ok, state: ok, persist: ok}
  ListExports: {wire: ok, errors: ok, state: ok, persist: ok}
  ListImports: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateChangeSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed error code ChangeSetNotFoundException -> ChangeSetNotFound (SDK deserializer matches the un-suffixed code; see errors.go ChangeSetNotFoundException.ErrorCode())"}
  ExecuteChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: no longer executes a FAILED/UNAVAILABLE change set (added InvalidChangeSetStatus gate); on success now clears every other change set for the stack, matching documented AWS behaviour, not just the executed one; fixed ChangeSetNotFound code"}
  DeleteChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed ChangeSetNotFound code"}
  ListChangeSets: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeType: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  resource_provisioning: {status: ok, note: "topoSortResources (Kahn's algorithm, deterministic alphabetical tie-break) + provisionResources/rollbackCreateResources (reverse-order rollback, DeletionPolicy Retain/Snapshot honored) verified correct, no changes needed"}
  update_reconciliation: {status: ok, note: "updateResources/rollbackUpdateResources snapshot-and-restore semantics verified correct; deleteStaleResources runs only after all creates/updates succeed, matching AWS ordering"}
  exports_imports: {status: ok, note: "ADDED: delete-blocked-while-imported and update-blocked-while-imported (Export X cannot be deleted as it is in use by Y), the one concretely-named gap from the audit brief that was completely unimplemented before this pass"}
  error_code_mapping: {status: ok, note: "verified against aws-sdk-go-v2 deserializers.go: StackNotFoundException is modeled for exactly one op (ImportStacksToStackSet) — every other stack-lookup op correctly falls back to generic ValidationError, matching real AWS's query-protocol behaviour; this was already correct, not changed"}
  drift_detection: {status: ok, note: "DetectStackDrift / DescribeStackResourceDrifts / legacy SimulateDrift fallback reviewed, logic is internally consistent; NOT re-verified against AWS's real per-property drift diff algorithm this pass (see deferred)"}
  nested_stacks: {status: ok, note: "CreateNestedStack/DeleteNestedStack correctly reuse createStackLocked/deleteStackLocked under the parent's already-held lock (no double-lock/deadlock); ParentID wiring reviewed, looks correct"}
gaps:
  - "Top-level Transform is never parsed (Template struct has no Transform field), so CAPABILITY_AUTO_EXPAND is never required for macro-using templates even though Fn::Transform invocation itself works (bd: gopherstack-urm)"
  - "changeset_diff.go requiresRecreation() models only a curated subset of AWS resource types' replacement-forcing properties (documented in-code as intentional partial coverage, not a regression) — expanding this table is future work, not tracked separately from gopherstack-e5h"
deferred:
  - "StackSets: CreateStackSet/UpdateStackSet/DeleteStackSet/instances/operations/drift — not audited this pass (bd: gopherstack-e5h)"
  - "Generated Templates family — not audited this pass (bd: gopherstack-e5h)"
  - "Resource Scans family — not audited this pass (bd: gopherstack-e5h)"
  - "Type registry/management (RegisterType/ActivateType/PublishType/TestType/etc.) — not audited this pass (bd: gopherstack-e5h)"
  - "Stack Refactor family — not audited this pass (bd: gopherstack-e5h)"
  - "YAML short-form intrinsics (!Ref/!GetAtt/etc.) wire coverage — not re-verified this pass (bd: gopherstack-e5h)"
leaks: {status: clean, note: "no new goroutines/janitors introduced. ExecuteChangeSet's synchronous UpdateStack/CreateStack call remains synchronous (no unbounded goroutine fan-out). evictDeletedStacks (cap 1000) and addEvent (cap 1000/stack) bounds were already in place and untouched. ctx is threaded through all new/changed code paths (stackExportsInUse takes no ctx since it only reads in-memory state, consistent with sibling ListImports)."}
---

## Notes

Protocol: AWS query/XML (`Action=...` form POST, `<FooResponse>` root, `ResponseMetadata>RequestId`).
Errors always serialize as HTTP 400 with `<ErrorResponse><Error><Code>/<Message></Error></ErrorResponse>`
(see `xmlError` in handler.go) — this project doesn't vary HTTP status by error code, matching how
CloudFormation's query protocol reports client errors.

### Verified against the actual SDK model, not assumption

- `aws-sdk-go-v2/service/cloudformation@v1.71.7`'s `types/errors.go` models a `StackNotFoundException`
  with `ErrorCode() == "StackNotFoundException"`, but grepping `deserializers.go` shows it is only ever
  switched on for **ImportStacksToStackSet**. Every other operation that can fail with "stack doesn't
  exist" (CreateStack's implicit lookups, UpdateStack, DeleteStack, DescribeStacks, GetTemplate,
  DescribeStackEvents, ...) has no modeled not-found exception at all, so the real API surfaces those as
  a generic `ValidationError`. This codebase already did that correctly everywhere except it had
  `ErrStackNotFound` as a hard *error* for DeleteStack, which leads to the next point.
- `DeleteStack`'s SDK-generated `awsAwsquery_deserializeOpErrorDeleteStack` has **no error cases at all**
  besides `TokenAlreadyExistsException` — confirming DeleteStack is fire-and-forget/idempotent in real
  AWS (deleting a stack that doesn't exist, or was already deleted, is a silent success). The backend
  previously returned `ErrStackNotFound` for this case; fixed to a no-op.
- `ChangeSetNotFoundException.ErrorCode()` returns `"ChangeSetNotFound"` (no "Exception" suffix), and
  `deserializers.go`'s `case strings.EqualFold("ChangeSetNotFound", errorCode)` confirms the wire code the
  real client matches on. Three handlers (`DescribeChangeSet`/`ExecuteChangeSet`/`DeleteChangeSet`) were
  emitting `"ChangeSetNotFoundException"` — a code the SDK client would never recognize, falling through
  to a generic `smithy.GenericAPIError`. Fixed to the exact modeled code.
- `InvalidChangeSetStatusException.ErrorCode()` returns `"InvalidChangeSetStatus"`. `ExecuteChangeSet` had
  no status gate at all before this pass — it would happily "execute" a change set whose
  `ExecutionStatus` was `UNAVAILABLE` (e.g. one created with zero net changes, which AWS marks
  FAILED/UNAVAILABLE at creation time). Fixed to reject with this exact code, matching AWS's documented
  `ExecuteChangeSet operation failed. Only failed changesets should have an execution status of
  UNAVAILABLE.`-class behaviour.
- The `ExecuteChangeSet` API doc comment in the SDK source states: "When you execute a change set,
  CloudFormation deletes all other change sets associated with the stack because they aren't valid for
  the updated stack." The backend previously deleted only the just-executed change set. Fixed to clear
  the whole per-stack change-set map on success.
- Export-in-use protection ("Export X cannot be deleted as it is in use by Y") was completely absent —
  neither `DeleteStack` nor `UpdateStack` checked whether a stack's export was still referenced via
  `Fn::ImportValue` by another active stack before removing it. This is one of the concretely-named
  high-value gaps in the audit brief. Added `stackExportsInUse` (shared helper, reuses the same
  `collectImportValues` machinery `ListImports` already uses) wired into both `deleteStackLocked` and a
  new `validateExportsStillInUse` pre-flight check in `applyTemplateToStack`. The update-side check is a
  *pre-update* approximation (computed from the pre-update physical-ID snapshot, mirroring where
  `validateImportValues` already runs) rather than a full two-pass plan/apply — acceptable given
  CloudFormation's own pre-flight validation model, but note if a future change makes an export's value
  depend on a resource newly created *by the same update*, this check won't see that value change.
- `CAPABILITY_AUTO_EXPAND` was incorrectly accepted as satisfying the `AWS::IAM::*` capability
  requirement in `requireIAMCapability`. Per AWS docs, `CAPABILITY_AUTO_EXPAND` only authorizes
  macro/transform expansion (e.g. SAM) — it does not grant permission to create IAM resources declared
  directly in the template. Fixed to only accept `CAPABILITY_IAM`/`CAPABILITY_NAMED_IAM`.
- `DescribeStackEvents` ignored `NextToken` and `addEvent`'s own 1000-event-per-stack cap entirely,
  returning the full history in one response. Changed the `StorageBackend` interface method to
  `DescribeStackEvents(nameOrID, nextToken string) (page.Page[StackEvent], error)`, reusing
  `pkgs/page.New` exactly like `ListStacks`/`ListChangeSets`/`ListExports` already do, and now surfaces
  `NextToken` in the XML response. This changed the interface signature — all backend/handler test call
  sites in this package were updated to match (`p, err := b.DescribeStackEvents(name, token); events :=
  p.Data`).

### Traps for the next auditor (looks-wrong-but-correct)

- `createStackFromTemplate`/`applyTemplateToStack` deliberately leave a stack in `CREATE_FAILED` /
  `UPDATE_FAILED` (not `ROLLBACK_COMPLETE` / `UPDATE_ROLLBACK_COMPLETE`) when `ParseTemplate` itself fails
  (malformed JSON/YAML), while every other pre-flight validator (`ValidateParameters`,
  `validateIntrinsics`, `validateImportValues`, and now `validateExportsStillInUse`) transitions all the
  way through to `*_ROLLBACK_COMPLETE`. This asymmetry is intentional and mirrors two genuinely different
  AWS failure classes; don't "fix" it into uniformity. The only real bug here (now fixed) was that the
  `UPDATE_FAILED` branch was silently missing its stack-level `addEvent` call — `CREATE_FAILED`'s sibling
  branch already had it.
- `computeChanges`/`CreateChangeSet` marking a same-template change set `Status: FAILED,
  ExecutionStatus: "UNAVAILABLE"` is correct AWS behaviour (a change set with zero net changes), not a
  bug — a stale unit test (`TestBackend_ExecuteChangeSet/existing_stack`) previously exercised this exact
  case with `wantErr` unset only because `ExecuteChangeSet` had no status gate; it now uses
  `modifiedTemplate` (a real diff) and a new sibling case
  (`existing_stack_no_changes`) explicitly covers the FAILED/blocked path.
- `ExecuteChangeSet`'s two lock windows (state-check-and-flip, then unlocked `UpdateStack`/`CreateStack`
  call, then re-lock to finalize) are pre-existing and intentional — `UpdateStack`/`CreateStack` take
  `b.mu` themselves, so holding it across the call would deadlock. Not touched.
