---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: ssm
sdk_module: aws-sdk-go-v2/service/ssm@v1.71.0
last_audit_commit: 2d2b1b9b
last_audit_date: 2026-07-11
overall: B                 # This pass: local drift since the last audit was a single test-file
                            # change (sdk_completeness_test.go excluding the 6 new CloudConnector
                            # ops added by the aws-sdk-go-v2 bump from v1.69.5 to v1.71.0). Audited
                            # that new surface per protocol: CloudConnector (Create/Delete/Get/
                            # List/Update/Validate) was entirely unimplemented (excluded from the
                            # completeness check rather than stubbed) — implemented for real this
                            # pass (state, wire shapes verified against serializers.go/
                            # deserializers.go, errors, persistence, tagging integration). Every
                            # row carried over from the prior audit (last_audit_commit 647d2017,
                            # itself unreachable from current HEAD — a cross-service commit hash;
                            # baseline re-derived as ce30166a per the re-audit protocol, whose diff
                            # against HEAD touched only that one test file) is trusted unchanged.
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
  LabelParameterVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "10-label-per-version cap (appendLabelsWithLimit) and move-label-between-versions semantics proven correct"}
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
  DescribeActivations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAssociationBatch: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePatchBaseline: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePatchBaseline: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPatchBaseline: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCloudConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass — see Notes: implemented from scratch, previously entirely unimplemented (excluded from sdk_completeness_test.go rather than stubbed)"}
  DeleteCloudConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  GetCloudConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  ListCloudConnectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass — SubscriptionId/TenantId filters incl. documented \"NONE\" tenant-level-only value, opaque index-token pagination matching DescribeParameters"}
  UpdateCloudConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass"}
  ValidateCloudConnector: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NEW this pass — see Notes: no real Azure tenant to call out to, so findings are deterministically derived from the connector's own stored configuration rather than a fabricated always-success stub"}
families:
  cloud-connectors: {status: ok, note: "NEW this pass — aws-sdk-go-v2 bumped v1.69.5 to v1.71.0 (see sdk_module) added CreateCloudConnector/DeleteCloudConnector/GetCloudConnector/ListCloudConnectors/UpdateCloudConnector/ValidateCloudConnector (Azure-only third-party cloud environment connectors). Implemented as a real *store.Table[CloudConnector]-backed resource (services/ssm/cloud_connector.go): required-field validation (ConfigConnectorArn/DisplayName/RoleArn/Configuration.AzureConfiguration.{ApplicationId,TenantId}) on Create, ResourceNotFoundException (the SDK's generic not-found type — no CloudConnector-specific error exists) on Get/Delete/Update/Validate of an unknown ID, tag integration via the existing generic miscResourceTags fallback path (ResourceTypeForTagging enum confirms \"CloudConnector\" is a valid AddTagsToResource/ListTagsForResource resource type, and that path was already resource-type-agnostic), and full Snapshot/Restore persistence via the existing store.Registry generic mechanism (store_setup.go's getOrCreateTable/tableAccessorsByPrefix — no persistence.go changes needed). Wire shapes verified against aws-sdk-go-v2/service/ssm@v1.71.0's serializers.go/deserializers.go directly (not the SDK's own doc comments): CreatedAt/UpdatedAt are epoch-seconds JSON numbers, matching this package's existing UnixTimeFloat convention, NOT ISO8601 strings; Configuration is a one-member Azure-only union wire-wrapped by member name (\"AzureConfiguration\")."}

  parameter-store: {status: ok, note: "FIXED this pass (PutParameter): 15-level hierarchy limit (HierarchyLevelLimitExceededException, previously unenforced), labeled-oldest-version eviction guard (ParameterMaxVersionLimitExceeded, previously silently evicted labeled versions and leaked their parameterLabels entries forever), Intelligent-Tiering auto-upgrade-to-Advanced on >4KiB value or Policies attached (previously hard-rejected instead of auto-selecting Advanced, defeating the entire point of Intelligent-Tiering), Policies-require-Advanced-tier (previously any tier accepted policies). Tier value-size limits (4096 Standard / 8192 Advanced), AllowedPattern regex validation, SecureString KMS encrypt/decrypt round-trip via per-instance AES-256 key, parameter selector suffix (:version/:label) parsing were all already correct."
  documents: {status: ok, note: "FIXED this pass: CreateDocument/UpdateDocument/DescribeDocument were all returning the internal Document struct (which carries Content) as their metadata-only response — added a DocumentDescription wire type (matches AWS's real DocumentDescription, no Content field) and a Document.toDocumentDescription() converter. Also: GetDocument/DescribeDocument's DocumentVersion selector conflated explicit \"$DEFAULT\" with \"$LATEST\"/omitted, always serving the latest version's content/metadata even when a caller explicitly asked for $DEFAULT after UpdateDocumentDefaultVersion pinned an older version. Left the omitted-DocumentVersion behavior as latest (unchanged) since AWS's own API/CLI reference docs do not state a default and an existing, deliberately-written test (document_test.go TestInMemoryBackend_Snapshot_IncludesDocumentsAndCommands) depends on that behavior — only the unambiguous explicit-$DEFAULT case was fixed. Document version cap (1000) and content-hash-free JSON/YAML round-trip were already correct."
  command-execution: {status: ok, note: "no goroutines/timers in command_exec.go or automation_exec.go — command progression is driven synchronously plus the single ctx-cancel-aware janitor sweep (janitor.go), not per-command background workers. Nothing to leak."}
  sessions: {status: deferred, note: "StartSession/TerminateSession/ResumeSession not re-audited this pass beyond confirming they route through the janitor's terminated-session sweep (leak_test.go/janitor_test.go already cover this from a prior sweep); no wire/state changes made."}
  patch-maintenance-associations-inventory: {status: deferred, note: "spot-checked (Inventory family fully, PutParameter/PutInventory cross-reference) but not re-audited op-by-op this pass; prior sweeps (parity_batch7_test.go, parity_deepdive_test.go, batch2_accuracy_test.go) already cover patch baselines, patch groups/compliance, maintenance window tasks/targets, and state-manager associations in depth and no drift was found in the files backing them."
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Document version-cap eviction (maxDocumentVersionCap=1000) can, in a very long-lived document, evict the version currently pinned as DefaultVersion, orphaning the $DEFAULT selector (GetDocument/DescribeDocument would then return ErrInvalidDocumentVersion instead of re-pointing or falling back). Needs 1000+ UpdateDocument calls after pinning an old DefaultVersion — rare in practice, not fixed this pass (bd: gopherstack-1hg)"
  - "NoChangeNotification parameter policy is stored (Policies field round-trips) but never evaluated/acted on — no EventBridge event is emitted when a parameter goes unchanged past its configured window. ExpirationNotification has the same gap. Only Expiration is enforced (janitor sweep deletes on expiry). Out of scope for this pass — would require EventBridge cross-service wiring (shared-file, not fixed)."
  - "ListCloudConnectors/ValidateCloudConnector MaxResults default/cap (defaultCloudConnectorMaxResults=50, aliased to the existing defaultDescribeMaxResults) is a reasonable-default guess, not a value confirmed against real AWS docs — this brand-new API family (added in aws-sdk-go-v2 v1.70/1.71) has no documented bound in the SDK's Go doc comments or a public API reference at time of this pass. Revisit if AWS publishes the real bound."
  - "ValidateCloudConnector cannot make a real outbound call to Azure (gopherstack has no Azure tenant), so its ValidationFindings are derived deterministically from the connector's own stored Configuration (tenant/subscription IDs) rather than reflecting real third-party connectivity/permission state. This is the best achievable emulation for a cross-cloud connectivity check, not a wire/state bug — documented here so a future reader doesn't mistake the mocked findings for verified AWS behavior."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - Session Manager (StartSession/TerminateSession/ResumeSession) full op-by-op re-verification
  - Patch baselines / patch groups / compliance op-by-op re-verification (spot-checked only)
  - Maintenance windows (tasks/targets) op-by-op re-verification (spot-checked only)
  - State Manager associations op-by-op re-verification (spot-checked only)
  - OpsCenter (OpsItem/OpsMetadata) op-by-op re-verification (spot-checked only, errors already wired)
leaks: {status: clean, note: "Janitor (janitor.go) is the only background goroutine, ctx.Done()-aware, single Run() loop shared across all sweeps (parameters/commands/sessions). PutParameter's history cap now also deletes the corresponding parameterLabels[version] entries on eviction (previously left as an unbounded-growth leak: labels attached to since-evicted versions stayed in the map forever with no key ever removed). No new goroutines/tickers/timers introduced this pass."}
---

## Notes

SSM speaks the **json-1.1 protocol** (`AmazonSSM.<Op>` `X-Amz-Target`, `application/x-amz-json-1.1`
content type) — confirmed via `handler.go`'s `classifySSMError`/`handleError` using
`service.JSONErrorResponse` with a bare `{"Type":..., "Message":...}` body, not XML.

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

### New feature this pass: Cloud Connectors (aws-sdk-go-v2 v1.69.5 → v1.71.0 added surface)

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
`aws-sdk-go-v2/service/ssm@v1.71.0`'s generated `serializers.go`/`deserializers.go`, not inferred
from Go doc comments. `ResourceNotFoundException` (a generic SDK error type, not a
CloudConnector-specific one) was chosen for the not-found case since no dedicated error type exists
for this resource. See `gaps` for the two known limitations (unconfirmed pagination bound; findings
are derived from stored config rather than a real Azure connectivity check) that were consciously
left as-is rather than guessed at with false confidence.
