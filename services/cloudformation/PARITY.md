---
service: cloudformation
sdk_module: aws-sdk-go-v2/service/cloudformation@v1.76.1
last_audit_commit: 514ddad6
last_audit_date: 2026-07-23
overall: A            # This pass closed out all 4 documented gaps and independently re-verified/acted
                       # on all 6 documented deferred items (see gaps:/deferred: below for exact
                       # disposition of each -- some fixed, some reclassified to ok after
                       # re-verification, two genuinely still deferred with reasons). It also
                       # field-diffed the previously-unaudited Type Registry family (16 ops, none of
                       # which appeared in this doc's ops: table before) and found + fixed real bugs
                       # there too. Independently of the named gaps/deferred list, this pass found and
                       # fixed two significant previously-undocumented parity bugs: (1) 10 backend map
                       # fields (StackSet instances/operations, type configs/versions, drift detail,
                       # resource-scan items, custom-resource signals, hook progress) were NEVER wired
                       # into Snapshot/Restore at all -- silent data loss across every restart/restore
                       # for StackSets, type registry, drift detection, resource scans, and signaling;
                       # (2) CreateChangeSet never accepted or stored a Capabilities parameter, so
                       # ExecuteChangeSet always called UpdateStack/CreateStack with an empty
                       # StackOptions -- meaning ANY change set touching IAM resources could never
                       # actually be executed regardless of what capabilities the caller declared at
                       # CreateChangeSet time. Prior audit text retained below for history:
                       #
                       # local surface unchanged since prior sweep (ce30166a..HEAD diff is empty for
                       # this service); this pass spot-audited 4 previously fully-deferred families
                       # (StackSets, Stack Refactor, Generated Templates, Resource Scans) and found +
                       # fixed 4 genuine bugs: DeleteStackSet idempotency, DescribeStackRefactor
                       # not-found handling, and an unsuffixed-wire-code repeat of the ChangeSetNotFound
                       # bug class across Generated Templates + Resource Scans (plus two disguised-stub
                       # List* handlers that discarded not-found errors). Remaining deferred families
                       # (Type registry, YAML short-form intrinsics) still not re-proven op-by-op.
ops:
  CreateStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "CAPABILITY_AUTO_EXPAND no longer wrongly satisfies the IAM-resource capability check (backend_parity.go requireIAMCapability); this pass ALSO fixed the inverse gap -- top-level Transform is now parsed (Template.Transform) and requireAutoExpandCapability gates CAPABILITY_AUTO_EXPAND for macro/SAM-using templates, which was previously never enforced at all"}
  UpdateStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: missing UPDATE_FAILED stack event on template parse failure; added pre-flight export-in-use block (validateExportsStillInUse); same CAPABILITY_AUTO_EXPAND gate as CreateStack added this pass"}
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
  CreateChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now accepts and stores a Capabilities parameter (Capabilities.member.N form field, parseCapabilities) -- previously silently dropped, meaning capabilities declared at CreateChangeSet time were never usable at Execute time (see ExecuteChangeSet note); DescribeChangeSet's response now surfaces Capabilities too, matching the real DescribeChangeSetResult shape"}
  DescribeChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed error code ChangeSetNotFoundException -> ChangeSetNotFound (SDK deserializer matches the un-suffixed code; see errors.go ChangeSetNotFoundException.ErrorCode()); this pass added the missing Capabilities field to the response"}
  ExecuteChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: no longer executes a FAILED/UNAVAILABLE change set (added InvalidChangeSetStatus gate); on success now clears every other change set for the stack, matching documented AWS behaviour, not just the executed one; fixed ChangeSetNotFound code. THIS PASS fixed a significant additional bug: ExecuteChangeSet always called UpdateStack/CreateStack with an empty StackOptions{} (zero capabilities), because CreateChangeSet never stored Capabilities in the first place -- meaning ANY change set touching IAM resources could never actually be executed regardless of what capabilities the caller declared at CreateChangeSet time. Verified via TestChangeSet_Capabilities_ThreadedToExecute (execute now succeeds with CAPABILITY_IAM, fails with InsufficientCapabilities without it)"}
  DeleteChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed ChangeSetNotFound code"}
  ListChangeSets: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeType: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStackSet: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStackSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStackSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now idempotent (no-op, not StackSetNotFoundException) — SDK's DeleteStackSet error deserializer models only {OperationInProgressException, StackSetNotEmptyException}, no not-found case, mirroring the already-fixed DeleteStack precedent"}
  DescribeStackSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass (was the #1 named gap): full field set now returned, field-diffed against awsAwsquery_deserializeDocumentStackSet -- Parameters, Capabilities, Tags, StackSetARN, AdministrationRoleARN, ExecutionRoleName, PermissionModel, OrganizationalUnitIds, AutoDeployment{Enabled,RetainStacksOnAccountRemoval}, ManagedExecution{Active}. CreateStackSet/UpdateStackSet now accept these via a new StackSetOptions struct (signature change, all callers updated). Regions is intentionally NOT stored on StackSet -- it's computed live from stack instances each call (StackSetRegions) to avoid a second source of truth, mirroring the driftByStackID rationale below. Verified via TestStackSet_DescribeFieldCompleteness"}
  ListStackSets: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStackInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "real per-account/region child stacks are provisioned (provisionStackInstance), not just recorded rows — verified correct. gopherstack-g7b5: now also accepts DeploymentTargets.OrganizationalUnitIds.member.N (serializers.go's DeploymentTargets/OrganizationalUnitIdList encoders) and resolves each OU to its real member accounts via a wired Organizations backend (services/cloudformation/organizations_directory.go's OrganizationsDirectory interface, satisfied by organizations.InMemoryBackend.ResolveAccountIDsUnderParent, wired in cli.go's wireCloudFormationOrganizations). Requires PermissionModel=SERVICE_MANAGED and ActivateOrganizationsAccess; errors clearly otherwise rather than silently expanding to zero accounts. gopherstack-nirx: DeploymentTargets.AccountFilterType was documented as rejected but the field was never read by the handler (silently dropped, computing a union of Accounts and OU-resolved accounts regardless of the requested filter) — now handler_stack_sets.go's unsupportedAccountFilterType actually rejects INTERSECTION/DIFFERENCE/UNION with ValidationError; only unset/NONE (the union case) is honoured. See TestStackInstances_AccountFilterType"}
  DeleteStackInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "tears down provisioned child stacks via deleteStackLocked — verified correct. gopherstack-g7b5: also accepts DeploymentTargets.OrganizationalUnitIds, same resolution path as CreateStackInstances"}
  UpdateStackInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-g7b5: also accepts DeploymentTargets.OrganizationalUnitIds"}
  ListStackInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DetectStackSetDrift: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: was a disguised stub -- recorded a real SUCCEEDED operation but never actually ran per-instance drift comparison, so every stack instance's DriftStatus stayed NOT_CHECKED forever. Now runs the same compareStackResources logic DetectStackDrift uses against each instance's provisioned child stack and updates its DriftStatus (IN_SYNC/DRIFTED) in place. Verified via TestStackSetDrift_UpdatesInstanceDriftStatus (mutates a child-stack resource out of band, confirms DriftStatus flips to DRIFTED on re-detection)"}
  ListStackSetOperations: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackSetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: now returns StackSetNotFoundException when the StackSetName itself doesn't exist (SDK models this as a distinct case from OperationNotFoundException, which is now reserved for a known StackSet with an unknown OperationId). Verified via TestDescribeStackSetOperation_NotFoundErrorCodes"}
  StopStackSetOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStackSetOperationResults: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStackSetAutoDeploymentTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-g7b5: now groups by the real OrganizationalUnitId recorded on each SERVICE_MANAGED stack instance (see CreateStackInstances note) instead of always synthesizing one placeholder target per account; self-managed instances (no OU) still fall back to the per-account placeholder, matching real AWS semantics"}
  ImportStacksToStackSet: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStackRefactor: {wire: ok, errors: ok, state: ok, persist: ok, note: "SDK models zero errors for this op (fire-and-forget) — verified via deserializers.go, no changes needed"}
  DescribeStackRefactor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: unknown StackRefactorId previously returned 200 with an empty Status instead of StackRefactorNotFoundException, the one error this op does model (unlike its Create/Execute/List siblings, which are genuinely fire-and-forget per the SDK model)"}
  ExecuteStackRefactor: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass (gopherstack-g7b5): was a pure status-flip (`r.Status = \"EXECUTE_COMPLETE\"`) that never moved anything, a disguised no-op. CreateStackRefactor now parses ResourceMappings.member.N.{Source,Destination}.{StackName,LogicalResourceId} (verified against serializers.go's ResourceMapping/ResourceLocation encoders); Execute now validates every mapping (source/dest stack + source resource must exist) before mutating, then moves each StackResource entry between b.resources[stackID] maps under the destination's logical ID. Unknown refactor ID or a missing source resource now errors (StackRefactorNotFoundException / ValidationError) instead of silently no-oping. Verified via TestExecuteStackRefactor_MovesResourceBetweenStacks (reads both stacks back via DescribeStackResources)"}
  ListStackRefactors: {wire: ok, errors: ok, state: ok, persist: ok, note: "SDK models zero errors for this op"}
  ListStackRefactorActions: {wire: ok, errors: ok, state: ok, persist: ok, note: "now derives real MOVE actions from the stored ResourceMappings (previously always empty since CreateStackRefactor never parsed StackDefinitions/mappings at all)"}
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
  DescribeType: {wire: ok, errors: ok, state: ok, persist: ok}
  ActivateType: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass: field-diffed against awsAwsquery_deserializeOpErrorActivateType (models CFNRegistryException, TypeNotFoundException); was previously entirely absent from this ops table despite being routed and non-stub"}
  DeactivateType: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  RegisterType: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed (SDK models only CFNRegistryException for this op)"}
  DeregisterType: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: handler discarded Backend.DeregisterType's returned error entirely (`_ = h.Backend.DeregisterType(...)`), so an unknown Arn silently returned 200 instead of the TypeNotFoundException the SDK models for this op -- a disguised stub matching the same bug class as the earlier ListResourceScanResources fix. A stale test (TestTypeRegistry_DeregisterNotFound) literally had a comment noting this ('handler currently ignores DeregisterType error'); now asserts the real 400/TypeNotFoundException"}
  PublishType: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  SetTypeDefaultVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: same disguised-stub pattern as DeregisterType -- backend never returned an error for an unknown Arn (silent no-op) and the handler discarded the return value too, even though the SDK models TypeNotFoundException for this op. Backend now returns ErrTypeNotFound; handler propagates it. Verified via TestTypeRegistry_SetTypeDefaultVersionNotFound"}
  SetTypeConfiguration: {wire: ok, errors: partial, state: ok, persist: ok, note: "SDK models TypeNotFoundException for this op, but the backend intentionally accepts configuration for ANY type name without requiring prior RegisterType/ActivateType -- this matches real-world usage where first-party AWS types (e.g. AWS::S3::Bucket) can have extension configuration set without ever being explicitly registered in this emulator's type registry (which only models the RegisterType/ActivateType *custom-extension* flow, not the full built-in-type catalog). Left permissive rather than force a wrong not-found on a legitimate first-party-type call; not re-classified as a bug (bd: gopherstack-e5h)"}
  BatchDescribeTypeConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass (gopherstack-g7b5): the request's TypeConfigurationIdentifiers.member.N was a list of STRUCTS (Type/TypeArn/TypeConfigurationAlias/TypeConfigurationArn/TypeName -- serializers.go:7114/7085), not scalars as the old parseMemberList call assumed, so no identifier was ever actually parsed. Now parses the real struct shape and populates Errors (TypeNotFoundException per identifier with no matching type/config, api_op_BatchDescribeTypeConfigurations.go:47) and UnprocessedTypeConfigurations (identifiers with no TypeName/TypeConfigurationArn to resolve by, :55) for real, instead of leaving them always empty. Verified via TestHandler_BatchDescribeTypeConfigurations"}
  TestType: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed (SDK models only CFNRegistryException)"}
  ListTypes: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed (SDK models only CFNRegistryException)"}
  ListTypeVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  ListTypeRegistrations: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  DescribeTypeRegistration: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  RegisterPublisher: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  DescribePublisher: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
families:
  resource_provisioning: {status: ok, note: "topoSortResources (Kahn's algorithm, deterministic alphabetical tie-break) + provisionResources/rollbackCreateResources (reverse-order rollback, DeletionPolicy Retain/Snapshot honored) verified correct, no changes needed"}
  update_reconciliation: {status: ok, note: "updateResources/rollbackUpdateResources snapshot-and-restore semantics verified correct; deleteStaleResources runs only after all creates/updates succeed, matching AWS ordering"}
  exports_imports: {status: ok, note: "ADDED: delete-blocked-while-imported and update-blocked-while-imported (Export X cannot be deleted as it is in use by Y), the one concretely-named gap from the audit brief that was completely unimplemented before this pass"}
  error_code_mapping: {status: ok, note: "verified against aws-sdk-go-v2 deserializers.go: StackNotFoundException is modeled for exactly one op (ImportStacksToStackSet) — every other stack-lookup op correctly falls back to generic ValidationError, matching real AWS's query-protocol behaviour; this was already correct, not changed"}
  drift_detection: {status: ok, note: "DetectStackDrift / DescribeStackResourceDrifts / legacy SimulateDrift fallback reviewed, logic is internally consistent; NOT re-verified against AWS's real per-property drift diff algorithm this pass (see deferred)"}
  nested_stacks: {status: ok, note: "CreateNestedStack/DeleteNestedStack correctly reuse createStackLocked/deleteStackLocked under the parent's already-held lock (no double-lock/deadlock); ParentID wiring reviewed, looks correct"}
  stacksets: {status: ok, note: "spot-audited previously (all 17 StackSet/instance/operation ops cross-checked against deserializers.go's modeled error switch per op; DeleteStackSet idempotency fixed then). Prior pass fixed DetectStackSetDrift (was a disguised stub), DescribeStackSetOperation's error-code gap, and closed the DescribeStackSet field-completeness gap. Then (gopherstack-g7b5): services/organizations DOES have a real, queryable OU hierarchy (CreateOrganizationalUnit/ListAccountsForParent/ListOrganizationalUnitsForParent), so the SERVICE_MANAGED/OU-based deployment-target gap was honestly implementable and has been closed — see CreateStackInstances/DeleteStackInstances/UpdateStackInstances/ListStackSetAutoDeploymentTargets notes above. THIS PASS (gopherstack-nirx): AccountFilterType and AccountsUrl remain unimplemented (real edge cases) but AccountFilterType is now actually rejected when set to anything but NONE — the g7b5 pass's claim of explicit rejection was documentation-only, the field was never read by the handler and was silently dropped — see gaps."}
  stack_refactor: {status: ok, note: "spot-audited previously (all 5 ops cross-checked against deserializers.go; DescribeStackRefactor not-found handling fixed then). THIS PASS (gopherstack-g7b5): ExecuteStackRefactor now performs a real resource move — see its ops: note above. ListStackRefactorActions derives real MOVE actions from the stored mappings."}
  generated_templates: {status: ok, note: "spot-audited previously (all 6 ops cross-checked against deserializers.go; unsuffixed-wire-code bug class fixed then, same as ChangeSetNotFound). THIS PASS: independently re-verified all 4 not-found-capable ops still emit the correct unsuffixed GeneratedTemplateNotFound code (handler_generated_templates.go) -- confirms the family: ok classification from the prior pass; the deferred: bullet claiming this family was 'not audited' was stale leftover documentation from BEFORE that prior spot-audit ran and is removed this pass."}
  resource_scans: {status: ok, note: "spot-audited previously (all 5 ops cross-checked against deserializers.go; unsuffixed-wire-code + two disguised-stub List* bugs fixed then). THIS PASS: independently re-verified the fixed error codes are still correct (ResourceScanNotFound, generated_templates.go/handler_generated_templates.go) -- confirms family: ok; same stale deferred: bullet issue as generated_templates, removed."}
  type_registry: {status: ok, note: "NEW this pass: this family (16 ops: DescribeType plus 15 RegisterType/ActivateType/... management ops) had NO ops: table entries at all before this pass despite being fully routed and non-stub -- the deferred: bullet 'not audited this pass' was accurate for every prior pass. Field-diffed all 16 against deserializers.go's per-op modeled error switches. Found + fixed two disguised-stub bugs (DeregisterType, SetTypeDefaultVersion — see ops: above). SetTypeConfiguration/TestType/BatchDescribeTypeConfigurations/RegisterPublisher's non-error-returning backend methods were reviewed and left as-is with reasoning recorded per-op above (SetTypeConfiguration's permissiveness is intentional; BatchDescribeTypeConfigurations' missing Errors/UnprocessedTypeConfigurations fields is a real but low-value gap)."}
  yaml_short_form_intrinsics: {status: ok, note: "NEW this pass: previously deferred as 'not re-verified'. Independent verification found it was actually BROKEN, not merely unverified -- ParseTemplate/parseGenericTemplate called gopkg.in/yaml.v3's Unmarshal directly into typed structs / map[string]any, which silently discards any custom YAML tag and decodes only the tagged node's native scalar/seq/map content. `!Ref MyParam` decoded to the bare string \"MyParam\" instead of the long-form {\"Ref\": \"MyParam\"} every resolveValue-style consumer expects -- every YAML short-form intrinsic (!Ref, !GetAtt, !Sub, !Join, !Select, !Split, !Base64, !Cidr, !ImportValue, !GetAZs, !FindInMap, !And, !Or, !Not, !Equals, !If, !Condition, !Transform) silently degraded to a dead literal string rather than resolving or erroring. Fixed via a new yamlToJSON/normalizeYAMLNode pass that walks the raw *yaml.Node tree (preserving tag info) before the JSON round-trip. Verified via TestParseTemplate_YAMLShortFormIntrinsics (shape-level) and TestCreateStack_YAMLShortFormIntrinsics_Resolve (end-to-end: !Ref/!Sub actually resolve through CreateStack/DescribeStacks Outputs)."}
gaps:
  - "changeset_diff.go requiresRecreation() models only a curated subset of AWS resource types' replacement-forcing properties (documented in-code as intentional partial coverage, not a regression) — expanding this table is future work, not tracked separately from gopherstack-e5h"
  - "SetTypeConfiguration accepts configuration for any type name without requiring prior registration (intentional permissiveness for first-party AWS types — see ops: SetTypeConfiguration note); real AWS models TypeNotFoundException here but this emulator doesn't track the full built-in-type catalog (bd: gopherstack-e5h)"
  - "StackSets DeploymentTargets.AccountFilterType INTERSECTION/DIFFERENCE/UNION filtering and AccountsUrl are not implemented — only the unset/NONE case (union of Accounts and OU-resolved accounts) is honoured; other AccountFilterType values are now rejected explicitly with ValidationError (fixed gopherstack-nirx; previously silently dropped despite being documented as rejected — bd: gopherstack-g7b5, gopherstack-nirx)"
  - "ImportStacksToStackSet still doesn't tag imported instances with a real OU (no DeploymentTargets on that op in the SDK to source one from) — unaffected by the gopherstack-g7b5 OU work"
leaks: {status: clean, note: "no goroutines/janitors/tickers introduced this pass. All fixes are pure control-flow/data changes under the existing b.mu lock discipline (every new lock path already has its matching defer Unlock/RUnlock, verified by reading each new/changed method in full). The persistence fix (10 previously-unpersisted map fields) is the largest change this pass but is snapshot/restore-only -- no new background work, no new maps that need cascade-delete beyond what already existed (stackInstances/stackSetOperations were already correctly cascade-deleted by DeleteStackSet before this pass; this pass only fixed their Snapshot/Restore wiring, not their lifecycle)."}
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
- (2026-07-23 sweep) **Persistence gap (largest finding this pass):** `store_setup.go`'s
  `registerAllTables` doc comment lists 15 backend map fields deliberately left as plain maps (not
  `store.Table`-registered) because they're nested/one-to-many. Of those 15, only 4
  (`events`/`resources`/`changeSets`/`stackPolicies`) were actually wired into `backendSnapshot` in
  `persistence.go` — the other 10 (`stackInstances`, `stackSetOperations`, `typeConfigs`,
  `handlerProgress`, `signals`, `stackSetOpResults`, `typeVersions`, `resourceScanItems`,
  `resourceDriftStatus`, `resourceDriftDetail`) were never persisted at all. This is silent data loss on
  every restart/restore: StackSet instances/operations, the entire type registry's configuration/version
  history, per-resource drift detail, resource-scan findings, and custom-resource signals all vanished
  across a snapshot/restore cycle. Fixed by adding all 10 fields to `backendSnapshot` with the same
  nil-fallback-on-restore pattern the existing 4 use (`applyNilDefaults`, split out of `Restore` to keep
  it under golangci-lint's cyclop threshold). `driftByStackID` (a reverse index) is deliberately NOT
  persisted directly — like `stackIDIndex`, it's rebuilt from its persisted source table
  (`driftDetections`) in `Restore` (`rebuildDerivedIndexes`), so it can never drift out of sync with the
  data it indexes. Verified via `TestInMemoryBackend_SnapshotRestore_PlainMapFields`.
- (2026-07-23 sweep) **`CreateChangeSet` Capabilities gap:** `CreateChangeSet`'s signature never accepted
  a `Capabilities` parameter at all, despite the real `CreateChangeSetInput.Capabilities` field (and the
  fact that `DescribeChangeSet`'s real output also returns `Capabilities`). Because `ExecuteChangeSet`
  applies a change set by calling `UpdateStack`/`CreateStack` with `StackOptions{}` (zero capabilities),
  this meant ANY change set touching IAM resources could never actually be executed — `ExecuteChangeSet`
  would always hit `requireIAMCapability`'s `InsufficientCapabilities` gate inside `UpdateStack`/
  `CreateStack`, regardless of what capabilities the caller declared at `CreateChangeSet` time. Fixed:
  `ChangeSet` gained a `Capabilities []string` field, `CreateChangeSet` now accepts and stores it (wired
  from the `Capabilities.member.N` form field via the existing `parseCapabilities` helper), and
  `ExecuteChangeSet` now passes `StackOptions{Capabilities: cs.Capabilities}` instead of an empty
  `StackOptions{}`. `DescribeChangeSet`'s response also gained the `Capabilities` field it was missing.
  Verified via `TestChangeSet_Capabilities_ThreadedToExecute` (execute now succeeds with `CAPABILITY_IAM`
  on an IAM-touching template, fails with `InsufficientCapabilities` without it — both cases previously
  behaved identically to "without it" since the parameter was discarded).
- (2026-07-23 sweep) **`DetectStackSetDrift` was a disguised stub:** it recorded a real `SUCCEEDED`
  `StackSetOperation` (so it looked functional in any test that only checked the operation record) but
  never actually compared any stack instance's provisioned child-stack resources against its template —
  every instance's `DriftStatus` stayed `NOT_CHECKED` forever, no matter how many times drift detection
  ran. Fixed: added `detectStackInstanceDrift`, which reuses the exact same `compareStackResources` logic
  the standalone per-stack `DetectStackDrift` already used, resolving each instance's `StackID` back to
  its child stack via `stackIDIndex` and updating `DriftStatus` to `IN_SYNC`/`DRIFTED` in place. Verified
  via `TestStackSetDrift_UpdatesInstanceDriftStatus`, which simulates an out-of-band mutation on a child
  stack's resource (`RecordResourceMutation`) and confirms the instance's `DriftStatus` actually flips to
  `DRIFTED` on re-detection (it previously never would have, at any capability level).
- (2026-07-23 sweep) **YAML short-form intrinsics were silently broken, not merely unverified:**
  `ParseTemplate`'s YAML branch and `parseGenericTemplate` both called `gopkg.in/yaml.v3`'s `Unmarshal`
  directly into typed structs / a bare `map[string]any`. yaml.v3 silently discards any custom YAML tag it
  doesn't recognize as a built-in and decodes only the tagged node's native scalar/sequence/mapping
  content — confirmed via a standalone repro: `!Ref MyParam` decoded to the bare Go string `"MyParam"`,
  and `!Sub "${AWS::StackName}-bucket"` decoded to the raw unresolved string, with **no error and no
  indication anything was lost**. Every `resolveValue`-family consumer in this package expects the
  long-form map representation (`{"Ref": "MyParam"}`, `{"Fn::Sub": "..."}`), so every YAML short-form
  intrinsic — `!Ref`, `!GetAtt`, `!Sub`, `!Join`, `!Select`, `!Split`, `!Base64`, `!Cidr`, `!ImportValue`,
  `!GetAZs`, `!FindInMap`, `!And`, `!Or`, `!Not`, `!Equals`, `!If`, `!Condition`, `!Transform` — silently
  degraded to a dead literal string instead of resolving or erroring, for every template written in YAML
  short form (the common case for hand-written CloudFormation YAML). Fixed via `yamlToJSON`/
  `normalizeYAMLNode`/`normalizeYAMLNodeValue`, which walk the raw `*yaml.Node` tree (so tag information
  is never lost) and expand short-form tags into their long-form map representation before an ordinary
  JSON round-trip through the existing, already-correct JSON-path logic. `!GetAtt`'s dotted scalar short
  form (`LogicalId.Attribute`) is split into the long-form two-element list. Both `ParseTemplate` and
  `parseGenericTemplate` (used by the `Fn::ForEach` language-extension expander) now share this path, so
  a YAML template combining `Fn::ForEach` with short-form intrinsics resolves both correctly together.
  Verified via `TestParseTemplate_YAMLShortFormIntrinsics` (shape-level, all tag kinds) and
  `TestCreateStack_YAMLShortFormIntrinsics_Resolve` (end-to-end: `!Ref`/`!Sub` actually resolve through
  `CreateStack`/`DescribeStacks` `Outputs`, not just at the parse-tree level).

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
- (2026-07-23 sweep) `Template.UnmarshalYAML`/`TemplateResource.UnmarshalYAML` use the OLD-style
  `gopkg.in/yaml.v3` "obsolete unmarshaler" interface (`func(unmarshal func(any) error) error`), which
  yaml.v3 keeps for backward compat with yaml.v2 code — do NOT "modernize" these to the `*yaml.Node`-based
  interface without checking every call site; they're intentionally written this way to match the
  pre-existing `TemplateResource` pattern. Separately: an embed-based JSON alias trick (`type plain T;
  struct { Extra X; *plain }`) works fine for `UnmarshalJSON` but silently drops every promoted field on
  the YAML path — yaml.v3 does not auto-promote fields from an anonymously embedded pointer the way
  `encoding/json` does. `Template`'s `UnmarshalJSON`/`UnmarshalYAML` both decode into `templatePlain`
  (all fields listed explicitly, both `json` and `yaml` tags) for exactly this reason; don't refactor
  toward the embed trick even though it looks like less boilerplate. Both `Template.UnmarshalJSON` and
  `Template.UnmarshalYAML` are effectively dead code paths now that `ParseTemplate`'s YAML branch
  round-trips through `yamlToJSON` first (so only `UnmarshalJSON` actually runs at parse time) — left in
  place rather than deleted, since they're exported `yaml.Unmarshaler`/`json.Unmarshaler` implementations
  a caller outside this package's `ParseTemplate` could still reasonably invoke directly.
