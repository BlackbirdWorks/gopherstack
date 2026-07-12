---
service: cloudformation
sdk_module: aws-sdk-go-v2/service/cloudformation@v1.71.7
last_audit_commit: d6fae6df
last_audit_date: 2026-07-11
overall: A            # local surface unchanged since prior sweep (ce30166a..HEAD diff is empty for
                       # this service); this pass spot-audited 4 previously fully-deferred families
                       # (StackSets, Stack Refactor, Generated Templates, Resource Scans) and found +
                       # fixed 4 genuine bugs: DeleteStackSet idempotency, DescribeStackRefactor
                       # not-found handling, and an unsuffixed-wire-code repeat of the ChangeSetNotFound
                       # bug class across Generated Templates + Resource Scans (plus two disguised-stub
                       # List* handlers that discarded not-found errors). Remaining deferred families
                       # (Type registry, YAML short-form intrinsics) still not re-proven op-by-op.
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
  CreateStackSet: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStackSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStackSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now idempotent (no-op, not StackSetNotFoundException) — SDK's DeleteStackSet error deserializer models only {OperationInProgressException, StackSetNotEmptyException}, no not-found case, mirroring the already-fixed DeleteStack precedent"}
  DescribeStackSet: {wire: partial, errors: ok, state: ok, persist: ok, note: "only StackSetId/StackSetName/Status/Description returned; real DescribeStackSetResult.StackSet also has Parameters, Capabilities, Tags, StackSetARN, AdministrationRoleARN, ExecutionRoleName, PermissionModel, OrganizationalUnitIds, AutoDeployment, ManagedExecution, Regions — feature-completeness gap, not a wire-shape bug (existing fields serialize correctly), left as a gap (bd: gopherstack-e5h)"}
  ListStackSets: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStackInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "real per-account/region child stacks are provisioned (provisionStackInstance), not just recorded rows — verified correct"}
  DeleteStackInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "tears down provisioned child stacks via deleteStackLocked — verified correct"}
  UpdateStackInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStackInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DetectStackSetDrift: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStackSetOperations: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackSetOperation: {wire: ok, errors: partial, state: ok, persist: ok, note: "always returns OperationNotFoundException even when the StackSetName itself doesn't exist; SDK models StackSetNotFoundException as a distinct case for this op. Minor edge-case miscode, not fixed this pass (low value: both conditions require an unknown operation ID) — left as a gap (bd: gopherstack-e5h)"}
  StopStackSetOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStackSetOperationResults: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStackSetAutoDeploymentTargets: {wire: ok, errors: ok, state: ok, persist: ok}
  ImportStacksToStackSet: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStackRefactor: {wire: ok, errors: ok, state: ok, persist: ok, note: "SDK models zero errors for this op (fire-and-forget) — verified via deserializers.go, no changes needed"}
  DescribeStackRefactor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: unknown StackRefactorId previously returned 200 with an empty Status instead of StackRefactorNotFoundException, the one error this op does model (unlike its Create/Execute/List siblings, which are genuinely fire-and-forget per the SDK model)"}
  ExecuteStackRefactor: {wire: ok, errors: ok, state: ok, persist: ok, note: "SDK models zero errors for this op — no-op on unknown ID is correct, not a bug"}
  ListStackRefactors: {wire: ok, errors: ok, state: ok, persist: ok, note: "SDK models zero errors for this op"}
  ListStackRefactorActions: {wire: ok, errors: ok, state: ok, persist: ok, note: "SDK models zero errors for this op; empty list on unknown ID is correct"}
  CreateGeneratedTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGeneratedTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire code: GeneratedTemplateNotFoundException -> GeneratedTemplateNotFound (SDK's ErrorCode() is unsuffixed, same bug class as the earlier ChangeSetNotFound fix)"}
  DeleteGeneratedTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire code: GeneratedTemplateNotFoundException -> GeneratedTemplateNotFound"}
  DescribeGeneratedTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire code: GeneratedTemplateNotFoundException -> GeneratedTemplateNotFound"}
  GetGeneratedTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire code: GeneratedTemplateNotFoundException -> GeneratedTemplateNotFound"}
  ListGeneratedTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  StartResourceScan: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeResourceScan: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire code: ResourceScanNotFoundException -> ResourceScanNotFound (SDK's ErrorCode() is unsuffixed)"}
  ListResourceScans: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourceScanResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was silently discarding the not-found error (`_`) and returning 200 with an empty list for an unknown ResourceScanId; SDK models ResourceScanNotFound for this op. Now surfaces it with the correct unsuffixed code"}
  ListResourceScanRelatedResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same disguised-stub pattern as ListResourceScanResources — was discarding the not-found error; now surfaces ResourceScanNotFound"}
families:
  resource_provisioning: {status: ok, note: "topoSortResources (Kahn's algorithm, deterministic alphabetical tie-break) + provisionResources/rollbackCreateResources (reverse-order rollback, DeletionPolicy Retain/Snapshot honored) verified correct, no changes needed"}
  update_reconciliation: {status: ok, note: "updateResources/rollbackUpdateResources snapshot-and-restore semantics verified correct; deleteStaleResources runs only after all creates/updates succeed, matching AWS ordering"}
  exports_imports: {status: ok, note: "ADDED: delete-blocked-while-imported and update-blocked-while-imported (Export X cannot be deleted as it is in use by Y), the one concretely-named gap from the audit brief that was completely unimplemented before this pass"}
  error_code_mapping: {status: ok, note: "verified against aws-sdk-go-v2 deserializers.go: StackNotFoundException is modeled for exactly one op (ImportStacksToStackSet) — every other stack-lookup op correctly falls back to generic ValidationError, matching real AWS's query-protocol behaviour; this was already correct, not changed"}
  drift_detection: {status: ok, note: "DetectStackDrift / DescribeStackResourceDrifts / legacy SimulateDrift fallback reviewed, logic is internally consistent; NOT re-verified against AWS's real per-property drift diff algorithm this pass (see deferred)"}
  nested_stacks: {status: ok, note: "CreateNestedStack/DeleteNestedStack correctly reuse createStackLocked/deleteStackLocked under the parent's already-held lock (no double-lock/deadlock); ParentID wiring reviewed, looks correct"}
  stacksets: {status: ok, note: "spot-audited this pass (previously fully deferred): all 17 StackSet/instance/operation ops cross-checked against deserializers.go's modeled error switch per op. Found + fixed one genuine bug: DeleteStackSet was returning StackSetNotFoundException where AWS's operation model has no not-found case at all (idempotent, like DeleteStack). Everything else already matched. NOT a full re-audit of business logic (drift-diff accuracy, SERVICE_MANAGED/OU semantics, deployment-target math) — see gaps/deferred."}
  stack_refactor: {status: ok, note: "spot-audited this pass (previously fully deferred): all 5 ops cross-checked against deserializers.go. Found + fixed one genuine bug: DescribeStackRefactor silently returned 200/empty-Status for an unknown StackRefactorId instead of the StackRefactorNotFoundException the SDK models for this specific op (its 4 siblings are correctly fire-and-forget per the SDK's empty error models — left unchanged)."}
  generated_templates: {status: ok, note: "spot-audited this pass (previously fully deferred): all 6 ops cross-checked against deserializers.go. Found + fixed a repeat of the ChangeSetNotFound bug class: all 4 not-found-capable ops (Update/Delete/Describe/GetGeneratedTemplate) emitted the wrong wire code (\"GeneratedTemplateNotFoundException\" instead of the SDK-modeled unsuffixed \"GeneratedTemplateNotFound\")."}
  resource_scans: {status: ok, note: "spot-audited this pass (previously fully deferred): all 5 ops cross-checked against deserializers.go. Found + fixed two bugs: (1) same unsuffixed-wire-code bug as generated_templates on DescribeResourceScan; (2) ListResourceScanResources/ListResourceScanRelatedResources were disguised stubs — both discarded the backend's not-found error entirely and returned 200 with an empty list for any unknown ResourceScanId."}
gaps:
  - "Top-level Transform is never parsed (Template struct has no Transform field), so CAPABILITY_AUTO_EXPAND is never required for macro-using templates even though Fn::Transform invocation itself works (bd: gopherstack-urm)"
  - "changeset_diff.go requiresRecreation() models only a curated subset of AWS resource types' replacement-forcing properties (documented in-code as intentional partial coverage, not a regression) — expanding this table is future work, not tracked separately from gopherstack-e5h"
  - "DescribeStackSet returns only StackSetId/StackSetName/Status/Description; missing Parameters/Capabilities/Tags/StackSetARN/AdministrationRoleARN/ExecutionRoleName/PermissionModel/OrganizationalUnitIds/AutoDeployment/ManagedExecution/Regions fields that real AWS's DescribeStackSetResult.StackSet returns (bd: gopherstack-e5h)"
  - "DescribeStackSetOperation always returns OperationNotFoundException even when StackSetName itself doesn't exist (SDK models a distinct StackSetNotFoundException case); low-value edge case, not fixed (bd: gopherstack-e5h)"
deferred:
  - "StackSets business-logic depth: SERVICE_MANAGED/OU-based auto-deployment semantics, DetectStackSetDrift's actual per-instance drift diff accuracy, deployment-target math beyond the synthetic per-account-as-OU placeholder — not audited this pass, only wire/error shape was (bd: gopherstack-e5h)"
  - "Stack Refactor business-logic depth (ListStackRefactorActions' MOVE-only action modeling, real resource-mapping semantics) — not audited this pass, only wire/error shape was (bd: gopherstack-e5h)"
  - "Generated Templates family — not audited this pass (bd: gopherstack-e5h)"
  - "Resource Scans family — not audited this pass (bd: gopherstack-e5h)"
  - "Type registry/management (RegisterType/ActivateType/PublishType/TestType/etc.) — not audited this pass (bd: gopherstack-e5h)"
  - "YAML short-form intrinsics (!Ref/!GetAtt/etc.) wire coverage — not re-verified this pass (bd: gopherstack-e5h)"
leaks: {status: clean, note: "no new goroutines/janitors introduced this pass either. Both fixes (DeleteStackSet, DescribeStackRefactor) are pure control-flow changes (early nil-return / new error-return); no new allocations, locks, or ctx plumbing."}
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
- (2026-07-11 sweep) `DeleteStackSet`'s SDK-generated `awsAwsquery_deserializeOpErrorDeleteStackSet`
  models exactly `{OperationInProgressException, StackSetNotEmptyException}` — no not-found case, same
  pattern as the earlier `DeleteStack` finding above. The backend previously returned
  `ErrStackSetNotFound` (surfaced as `StackSetNotFoundException`) when deleting a StackSet name that
  never existed; fixed to a silent no-op, matching AWS's DeleteStack-style idempotency for this op.
- (2026-07-11 sweep) Spot-audited the previously fully-deferred Stack Refactor family
  (`CreateStackRefactor`/`DescribeStackRefactor`/`ExecuteStackRefactor`/`ListStackRefactors`/
  `ListStackRefactorActions`) against their deserializers. Four of the five have a genuinely *empty*
  modeled error switch (fire-and-forget, same class as `DeleteStack`) — their existing no-op-on-unknown-ID
  behaviour is correct. The exception is `DescribeStackRefactor`, whose deserializer models
  `StackRefactorNotFoundException` — the one op in this family that's NOT fire-and-forget. The backend
  was returning `("", nil)` for an unknown `StackRefactorId`, which the handler serialized as a 200 with
  an empty `<Status/>` — a disguised-stub pattern (real-looking op, but an unpopulated lookup silently
  "succeeding" per parity-principles.md rule 4). Fixed: added `ErrStackRefactorNotFound`, backend now
  returns it, handler maps it to `StackRefactorNotFoundException`.
- (2026-07-11 sweep) Spot-audited the previously fully-deferred Generated Templates and Resource Scans
  families. `GeneratedTemplateNotFoundException.ErrorCode()` and `ResourceScanNotFoundException.ErrorCode()`
  both return their **unsuffixed** wire code (`"GeneratedTemplateNotFound"` / `"ResourceScanNotFound"`) —
  the exact same bug class already found and fixed for `ChangeSetNotFound` in an earlier sweep, but it had
  independently recurred here. Four Generated Template handlers
  (`Update`/`Delete`/`Describe`/`GetGeneratedTemplate`) and one Resource Scan handler
  (`DescribeResourceScan`) were all emitting the `...Exception`-suffixed code, which
  `aws-sdk-go-v2` clients never match against the typed exception (falls back to a generic
  `smithy.GenericAPIError`). Fixed all five to the exact modeled code. Additionally,
  `ListResourceScanResources` and `ListResourceScanRelatedResources` were discarding
  `Backend.ListResourceScanResources`/`ListResourceScanRelatedResources`'s returned error with a bare
  `_`, so an unknown `ResourceScanId` silently produced a 200 with an empty list instead of
  `ResourceScanNotFound` — a disguised-stub pattern per parity-principles.md rule 4 (a `List*` op that
  looks real because it calls into backend logic, but the specific not-found branch was unreachable).
  Fixed both to surface the error.

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
