---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: ram
sdk_module: aws-sdk-go-v2/service/ram@v1.39.4   # version audited against
last_audit_commit: cfc26365a                    # HEAD when this manifest was written
last_audit_date: 2026-08-19
overall: A            # 2026-07-23: genuine fixes found (state-corruption bugs + wire-shape bugs)
                      # 2026-07-31: pkgs/sdkcheck reverse check found ListTagsForResource wrongly advertised/documented as a real SDK op (it isn't -- see its ops-block note); corrected, route left wired as internal test scaffolding. Grade held at A: unreachable by real traffic either way (RAM dispatches by request path, and no real client sends this path), and real tag-reading via GetResourceShares.Tags was already correct.
                      # 2026-08-19: wrapper-key/nested-shape sweep of all 34 SDK ops found and fixed 3 genuine bugs (CreatePermissionVersion/ListPermissionVersions had their Summary/Detail response shapes swapped; ListPermissionAssociations used the wrong key ("permissionArn" vs real "arn") and wrong type (number vs real string) for its AssociatedPermission items, the latter causing an actual SDK deserialization failure, not just a silent drop). All 3 fixed and proven by hand-revert + SDK-client round trip. Remaining 31 ops confirmed clean against their own deserializers. Grade held at A.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateResourceShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) - when no permissionArns are given and resourceArns are, now auto-associates the AWS-managed default permission for each resource type present (matches AWS: 'If you don't specify [permissionArns], the resource share is automatically associated with the default RAM-managed permission for each resource type included in the resource share')"}
  GetResourceShare: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceShares: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) - added the permissionArn/permissionVersion and tagFilters request filters (previously unimplemented, both present on the real GetResourceSharesInput); ResourceOwner is now enforced as required ('This member is required' on the real input, previously silently defaulted to empty)"}
  UpdateResourceShare: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourceShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "soft-deletes the share AND marks its associations DISASSOCIATED in place (kept in the associations slice); DisassociateResourceShare now uses the same pattern (fixed below), so the two are consistent again"}
  AssociateResourceShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) - dedup logic is now status-aware: only an ASSOCIATED row blocks re-association; a DISASSOCIATED row (from a prior DisassociateResourceShare) is reactivated in place instead of being ignored or duplicated. Also now auto-associates the default managed permission for any newly-introduced resource type not yet covered (AssociateResourceShare has no permissionArns parameter in the real API, so AWS always does this)"}
  DisassociateResourceShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) - previously hard-deleted matching rows from the associations slice; now marks them DISASSOCIATED in place, matching DeleteResourceShare's pattern. This closes the GetResourceShareAssociations(associationStatus=DISASSOCIATED) visibility gap and lets AssociateResourceShare reactivate a disassociated row (see above) instead of accumulating duplicates"}
  GetResourceShareAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "AssociationType is now enforced as required ('This member is required' on the real GetResourceShareAssociationsInput, previously silently defaulted to 'return every type')"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  # ListTagsForResource is intentionally NOT listed as an advertised SDK op
  # here. 2026-07-31 CORRECTION: the row that used to live at this position
  # ("wire: ok, ...") was inaccurate -- ListTagsForResource is not a real AWS
  # RAM SDK operation at all (verified against botocore's ram service-2.json:
  # only /tagresource and /untagresource exist; there is no
  # /listtagsforresource route). Caught by pkgs/sdkcheck's reverse check
  # (commit 12cfe14d5; gopherstack-vhw2 category A). Real clients read tags
  # back via GetResourceShares' ResourceShare.Tags field, which gopherstack
  # already populates correctly. RAM dispatches purely by request path via
  # ramGetListRoutes, so a real client can never send "/listtagsforresource"
  # and this route was already unreachable by real traffic; it stays wired as
  # internal test scaffolding, unadvertised. See handler.go's comment on
  # opListTagsForResource. Same resolution as EMR's ListTagsForResource and
  # CloudFront's GetFunctionAssociations/SetFunctionAssociations.
  AcceptResourceShareInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  RejectResourceShareInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceShareInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPendingInvitationResources: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePermission: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePermissionVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-08-19) - CreatePermissionVersionOutput.Permission is *types.ResourceSharePermissionDetail (api_op_CreatePermissionVersion.go:100), whose deserializer (deserializers.go:916) carries the policy-document 'permission' field via awsRestjson1_deserializeDocumentResourceSharePermissionDetail. gopherstack was building the response from the narrower Summary shape instead (toPermissionSummaryObject), which has no 'permission' case at all -- so a real client's output.Permission.Permission always decoded nil after CreatePermissionVersion. Switched to toPermissionDetailObject(p, pv). Proven via SDK-client round trip Test_SDKRoundTrip_CreatePermissionVersion_ReturnsPolicyDocument + hand-revert (confirmed the field decodes nil on revert, non-nil and correct on fix)."}
  DeletePermission: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePermissionVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPermission: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPermissionVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-08-19) - inverse of the CreatePermissionVersion bug above: ListPermissionVersionsOutput.Permissions is []types.ResourceSharePermissionSummary (api_op_ListPermissionVersions.go:75; deserializers.go:3821), which has no 'permission' policy-document field. gopherstack was building each item from the Detail shape (toPermissionDetailObject), leaking the full policy-document text under 'permission' for every version -- a field the real API never sends here. Switched to a new toPermissionVersionSummaryObject(p, pv) helper building the Summary shape with the version pinned. Proven via raw-body absence test Test_ListPermissionVersions_OmitsPolicyDocumentField (the typed SDK client can't observe a leaked field the real type doesn't declare, so a raw-body assertion is the correct instrument here) + hand-revert (confirmed the leak reappears verbatim on revert)."}
  ListPermissionAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-08-19) - ListPermissionAssociationsOutput.Permissions is []types.AssociatedPermission (api_op_ListPermissionAssociations.go:99; types/types.go:11-), whose wire key for the permission ARN is 'arn', not 'permissionArn', and whose PermissionVersion is a JSON string, not a number (deserializers.go's awsRestjson1_deserializeDocumentAssociatedPermission type-asserts permissionVersion to string). gopherstack emitted permissionArn (wrong key, so a real client's Arn always decoded nil) and a numeric permissionVersion (wrong type -- this is worse than a silent drop: it makes the real SDK client's ListPermissionAssociations call fail outright with 'deserialization failed ... expected String to be of type string, got json.Number instead'). Fixed both in permissionAssociationObject. Proven via SDK-client round trip Test_SDKRoundTrip_ListPermissionAssociations_ArnAndVersionShape + hand-revert (confirmed the exact decode error reproduces verbatim on revert)."}
  SetDefaultPermissionVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  PromotePermissionCreatedFromPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PromoteResourceShareCreatedFromPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "mock-simplified: real AWS asynchronously flips featureSet CREATED_FROM_POLICY -> PROMOTING_TO_STANDARD -> STANDARD; this backend has no featureSet state machine (CreateResourceShare always sets STANDARD) so the op is effectively a no-op validator. Acceptable since nothing here ever creates a CREATED_FROM_POLICY share (see deferred below)"}
  AssociateResourceSharePermission: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateResourceSharePermission: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) - now enforces AWS's documented rule ('You can remove a managed permission from a resource share only if there are currently no resources of the relevant resource type currently attached to the resource share') via OperationNotPermittedException; empty sharePermissions[shareARN] map entries are now pruned on last-permission removal"}
  ListResourceSharePermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  ReplacePermissionAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) - now honors the optional fromPermissionVersion request filter (previously parsed but discarded, replacing every share regardless of pinned version); records a real ReplacePermissionAssociationsWork item (persisted via a new store.Table) instead of fabricating a throwaway 'replace-work-<arn>' string"}
  ListReplacePermissionAssociationsWork: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) - was a permanently-empty stub; work items created by ReplacePermissionAssociations are now recorded and retrievable, with workIds/status filtering and pagination. Also fixed a wire-shape bug: the response list field must be 'replacePermissionAssociationsWorks' (plural) per the real deserializer -- the old code emitted the singular 'replacePermissionAssociationsWork' key (copy-pasted from the single-item ReplacePermissionAssociationsOutput shape), which a real SDK client would never populate from"}
  ListResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceOwner is now enforced as required (see GetResourceShares note)"}
  ListPrincipals: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceOwner is now enforced as required (see GetResourceShares note)"}
  ListResourceTypes: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "static table of shareable resource types, matches AWS's documented list"}
  ListSourceAssociations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED (2026-07-23) - wire-shape bug: response used a fabricated 'associations' key holding associationObject (principal/resource-association) shapes; the real deserializer reads 'sourceAssociations' holding AssociatedSource shapes (sourceId/sourceType/status/statusMessage/resourceShareArn). Fixed the shape; the list itself is correctly always empty -- confirmed by enumerating every api_op_*.go in the SDK module, there is no CreateSourceAssociation (or any) operation that could ever populate one via the RAM API, so an empty list is the only value this backend's public surface can ever produce, not a disguised stub"}
  GetResourcePolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableSharingWithAwsOrganization: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "no organization/delegated-admin model exists in this backend; op is a pure ReturnValue:true ack, matches how the AWS docs describe the call (idempotent enablement, no other side effects observable via the RAM API)"}
families:
  routing: {status: ok, note: "RouteMatcher / ExtractOperation path-prefix tables manually cross-checked against every op in GetSupportedOperations(); all prefix-collision cases (e.g. /listresourcesharepermissions vs /listresources, /createpermissionversion vs /createpermission, /associateresourcesharepermission vs /associateresourceshare) are already ordered longer-prefix-first correctly. No route-matcher bug found in this service."}
  wrapper_key_sweep_2026_08_19: {status: ok, note: "All 34 SDK ops swept (api_op_*.go count) against their own deserializers.go top-level-key switch AND their nested types' field-by-field switch (not generalized from siblings). 3 genuine bugs found and fixed (CreatePermissionVersion, ListPermissionVersions, ListPermissionAssociations -- see per-op notes above). 31 ops confirmed clean: ResourceShare/ResourceShareAssociation/ResourceShareInvitation/Principal/Resource/Tag/ServiceNameAndResourceType/AssociatedSource/ReplacePermissionAssociationsWork/ResourceSharePermissionSummary/ResourceSharePermissionDetail all verified field-for-field against their own deserializeDocument* function in deserializers.go@ram v1.39.4. No fabricated members found beyond the two Summary/Detail swaps. Layer-3 hunt (never-emitted members) was out of scope; resourceShareConfiguration/resourceGroupArn/receiverArn/resourceShareAssociations(on invitation) noted as genuine unfixed gaps below, not treated as bugs."}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore; versioned backendSnapshot (ramSnapshotVersion) with store.Registry-backed tables for resourceShares/permissions/invitations/replaceWorks plus raw sharePermissions/associations fields. The new replaceWorks table (ReplacePermissionAssociations work items) is registered like the other three 'clean' tables (identity-carrying ID field) and round-trips through the existing registry.SnapshotAll/RestoreAll machinery with no bespoke persistence.go changes needed. Confirmed via existing persistence_test.go coverage (unchanged, still green) -- did not add a dedicated persistence round-trip test for replaceWorks specifically since it's exercised through the same generic registry path as every other store.Table."}
gaps: []
deferred:
  - PromoteResourceShareCreatedFromPolicy's featureSet state machine (CREATED_FROM_POLICY -> PROMOTING_TO_STANDARD -> STANDARD) is not modeled; every share created here is already STANDARD so this hasn't caused observed drift, but if CREATED_FROM_POLICY share creation is ever added, this needs revisiting.
  - "CLOSED 2026-08-13: permissionSummaryObject/permissionDetailObject emitted a resourceRegionScope field that does not exist on the real ResourceSharePermissionSummary/ResourceSharePermissionDetail SDK types. Evidence: aws-sdk-go-v2/service/ram@v1.39.4, types/types.go:492-(Summary)/403-(Detail), checked 2026-08-13 -- exhaustive field lists are Arn/CreationTime/DefaultVersion/FeatureSet/IsResourceTypeDefault/LastUpdatedTime/Name/PermissionType/ResourceType/Status/Tags/Version (Summary, plus Permission on Detail), no ResourceRegionScope on either. That field exists only on types.Resource and types.ServiceNameAndResourceType (see handler_resources.go's legitimate use, TestResourceRegionScope_InListResources). Deleted the field from both wire structs; the internal Permission.ResourceRegionScope domain field (models.go) is untouched -- it backs real filtering logic, just was never a real member of these two wire shapes. Raw-body regression test: TestPermissionResponses_NoResourceRegionScopeField."
  - "DISCLOSED not fixed (2026-08-19 sweep, out of scope per sweep charter -- Layer 3 never-emitted members are only fixed if incidental): ResourceShare never emits resourceShareConfiguration (deserializers.go:8642+, types.ResourceShareConfiguration); Resource never emits resourceGroupArn (deserializers.go's awsRestjson1_deserializeDocumentResource); ResourceShareInvitation never emits receiverArn or resourceShareAssociations (deserializers.go's awsRestjson1_deserializeDocumentResourceShareInvitation). None of these surfaced incidentally while fixing the 3 genuine bugs this session, so left alone per the sweep's Layer-3-out-of-scope rule."
leaks: {status: clean, note: "no goroutines/janitors in this backend; all state is plain maps/slices (plus the new replaceWorks store.Table) behind the single lockmetrics.RWMutex, snapshotted/restored atomically under that lock. DisassociateResourceSharePermission now prunes an empty sharePermissions[shareARN] map entry when its last permission is removed, closing a minor unbounded-empty-map-entry accumulation path. DisassociateResourceShare/AssociateResourceShare no longer produce duplicate association rows for repeated disassociate/re-associate cycles on the same entity (see AssociateResourceShare note) -- previously this was bounded (hard-delete kept the slice from growing) but the status-aware reactivation is now also memory-neutral, reusing the existing row instead of allocating a new one."}
---

## Notes

Protocol: REST-JSON (restjson1), single-segment lowercase POST paths (e.g.
`/createresourceshare`, `/listresourcesharepermissions`). Timestamps are
epoch-seconds JSON numbers (`epochSeconds` helper), matching the SDK
deserializer's `smithytime.ParseEpochSeconds` for every `creationTime`/
`lastUpdatedTime`/`invitationTimestamp` field -- verified directly against
`deserializers.go` for `ResourceSharePermissionSummary`,
`ResourceSharePermissionDetail`, `ReplacePermissionAssociationsWork`, and
`AssociatedSource`. Confirmed no gopherstack-invented ops/fields exist:
`GetSupportedOperations()` (35 ops) was cross-checked one-for-one against
every `api_op_*.go` file in `aws-sdk-go-v2/service/ram@v1.36.1` -- no
extras, no missing ops.

**Bugs fixed this sweep (2026-07-23)**, closing all 7 gaps + partially
addressing the 1 deferred item recorded in the 2026-07-13 audit:

1. **DisassociateResourceShare hard-deleted instead of soft-deleting**
   (`share_associations.go`). Rewrote to mark matching rows
   `DISASSOCIATED` in place (like `DeleteResourceShare`) instead of
   removing them from `b.associations`. Paired with...

2. **AssociateResourceShare's dedup logic was not status-aware**
   (`share_associations.go`). Previously treated *any* existing row for
   `(shareARN, entity)` as "already associated", regardless of status --
   after fix #1 stopped hard-deleting, this would have silently no-op'd
   forever on any entity ever disassociated once. Now indexes existing
   rows into `active` (ASSOCIATED, blocks re-association) and `inactive`
   (any other status, reactivation candidate); `reactivateOrCreateLocked`
   flips a prior `DISASSOCIATED` row back to `ASSOCIATED` in place rather
   than appending a duplicate. Decomposed `AssociateResourceShare` into
   `indexAssociationsByEntityLocked` / `validateExternalPrincipalsLocked` /
   `associatePrincipalsLocked` / `associateResourcesLocked` to keep
   cognitive complexity under the gocognit threshold after the added logic.

3. **GetResourceShares missing permissionArn/permissionVersion/tagFilters
   filters** (`handler_resource_shares.go`). All three are present on the
   real `GetResourceSharesInput` and were silently ignored. Added
   `shareUsesPermission` (reuses `ListResourceSharePermissions`) and
   `tagsMatchFilters`, applied uniformly to both the `resourceShareArns`
   lookup path and the owner/status filter path.

4. **DisassociateResourceSharePermission didn't enforce the
   last-permission-for-resource-type rule** (`share_permissions.go`). Real
   AWS: "You can remove a managed permission from a resource share only if
   there are currently no resources of the relevant resource type
   currently attached to the resource share." Added
   `shareHasActiveResourceOfTypeLocked` and wired it in, returning
   `OperationNotPermittedException` when violated. Also now prunes an
   empty `sharePermissions[shareARN]` map entry when its last permission
   is removed (hygiene, not previously done).

5. **CreateResourceShare / AssociateResourceShare didn't auto-associate
   default managed permissions** (`share_permissions.go`,
   `handler_resource_shares.go`, `handler_share_associations.go`). Real
   AWS auto-attaches the default AWS-managed permission (e.g.
   `AWSRAMDefaultPermissionEC2Subnet`) for each resource type included in
   a share when `CreateResourceShare` is called with no `permissionArns`,
   and *always* for `AssociateResourceShare` (which has no `permissionArns`
   parameter in the real API at all). Added
   `InMemoryBackend.AutoAssociateDefaultPermissions`, called from both
   handlers after resource associations are created; idempotent, reuses
   the existing `awsBuiltInPermissions` seed data via
   `defaultPermissionForTypeLocked`.

6. **ListReplacePermissionAssociationsWork was a permanently-empty stub**
   (`share_permissions.go`, `handler_share_permissions.go`, `store.go`,
   `store_setup.go`, `models.go`). Added a `ReplacePermissionAssociationsWork`
   model type and a `store.Table`-backed `replaceWorks` field (registered
   like the other three "clean" tables), populated by
   `ReplacePermissionAssociations` and queryable by `ListReplacePermissionAssociationsWork`
   with `workIds`/`status` filtering + pagination. This mock performs the
   underlying association swap synchronously, so a work item's `Status` is
   always the terminal `COMPLETED` (not `IN_PROGRESS`) by the time it's
   stored -- there's no separate async completion step to fake. Also fixed
   a wire-shape bug found while implementing this: the list response's
   field must be `replacePermissionAssociationsWorks` (plural, per
   `deserializers.go`'s `awsRestjson1_deserializeOpDocumentListReplacePermissionAssociationsWorkOutput`),
   not the singular `replacePermissionAssociationsWork` key the old stub
   used (copy-pasted from the *single-item* `ReplacePermissionAssociationsOutput`
   shape, which correctly uses the singular key). `ReplacePermissionAssociations`
   now also honors the previously-parsed-but-discarded `fromPermissionVersion`
   request field, only replacing shares pinned to that specific version
   when given.

7. **Required-field validation gaps** (`handler_resource_shares.go`,
   `handler_resources.go`, `handler_principals.go`,
   `handler_share_associations.go`). `ResourceOwner` (`GetResourceShares`,
   `ListResources`, `ListPrincipals`) and `AssociationType`
   (`GetResourceShareAssociations`) are all `"This member is required"` on
   their real SDK input types but were silently treated as an
   empty-string default. Added explicit `errInvalidRequest` checks
   matching the pattern already used for other required fields
   (`resourceShareArn`, `name`, etc.) elsewhere in this service.

8. **ListSourceAssociations wire-shape bug** (`handler_resources.go`).
   Found while auditing item 6's neighbor: the response used a fabricated
   `associations` field holding `associationObject` (the
   principal/resource-association shape) instead of the real
   `sourceAssociations` field holding `AssociatedSource` objects
   (`sourceId`/`sourceType`/`status`/`statusMessage`/`resourceShareArn`).
   Fixed the shape. The list itself correctly stays always-empty: verified
   by enumerating every `api_op_*.go` in the SDK module that there is no
   RAM operation capable of ever creating a source association (they're
   populated by other AWS services acting behind the scenes, not via the
   RAM API), so this is the *only* value this backend's public surface can
   produce -- not a disguised stub. Reclassified from a documented "gap"
   to `ok` on this basis.

**Traps for the next auditor:**

- `ReplacePermissionAssociations`'s own response and
  `ListReplacePermissionAssociationsWork`'s response *use the same work
  item* and both report `Status: COMPLETED` (this mock swaps the
  association synchronously, so there's no real `IN_PROGRESS` window to
  model). If a future change adds a genuinely deferred/async op here,
  don't reflexively copy this pattern -- COMPLETED-on-creation is only
  correct because the underlying mutation has actually already happened
  by the time the work item is constructed.
- `AutoAssociateDefaultPermissions` is called from the *handler* layer
  (`handleCreateResourceShare`/`handleAssociateResourceShare`), not from
  inside the backend's `CreateResourceShare`/`AssociateResourceShare`
  methods. This is deliberate: those backend methods don't receive
  `permissionArns`, and `AutoAssociateDefaultPermissions` takes its own
  lock, so calling it from within an already-locked backend method would
  deadlock. Any future refactor that inlines this into the backend must
  either drop the re-lock or make the whole call chain lock-free-reentrant.
- `resourceTypeFromARN`'s `typeMap` and `awsBuiltInPermissions` are now
  consumed by three call sites (`ListResources`/`ListPrincipals` type
  derivation, `DisassociateResourceSharePermission`'s in-use check, and
  `AutoAssociateDefaultPermissions`'s default lookup) -- keep them as the
  single source of truth for resource-type <-> default-permission mapping
  rather than re-deriving it anywhere else.
- The `resourceRegionScope` field on permission summary/detail JSON
  objects is still not part of the real SDK shape for those types (only
  `Resource` and `ServiceNameAndResourceType` have it) but remains
  harmless noise since restjson1 deserializers ignore unrecognized fields
  -- left as a deferred note, don't "fix" this by removing it without
  checking whether tests depend on it.
- `DisassociateResourceSharePermission`'s in-use check only looks at the
  permission's own `ResourceType` against currently-`ASSOCIATED` `RESOURCE`
  associations on the share; it does not special-case a share that somehow
  has *two* permissions covering the same resource type (not achievable
  through this backend's own ops today, but if that ever becomes possible,
  revisit whether the rule should also cross-check other permissions
  before blocking).

## 2026-08-19 wrapper-key / nested-shape sweep

Enumerated all 34 `api_op_*.go` files in `aws-sdk-go-v2/service/ram@v1.39.4`
and cross-checked each one's own `deserializers.go` top-level-key switch and
its nested types' field-by-field switch against gopherstack's emitted JSON --
never generalizing one op's shape onto a same-looking sibling. Confirmed the
protocol from `api_client.go`'s `resolveHTTPSignerV4` plus the
`awsRestjson1_*` deserializer prefix (restjson1) -- this means casing
differences are real bugs here (unlike Query/XML services).

**3 genuine bugs found and fixed** (see per-op notes in the `ops:` block
above for full SDK `file:line` citations):

1. `CreatePermissionVersion` (`handler_permission_versions.go`) was building
   its response from `permissionSummaryObject` (`toPermissionSummaryObject`)
   when the real `CreatePermissionVersionOutput.Permission` is a
   `*types.ResourceSharePermissionDetail` -- the narrower Summary shape has
   no `permission` (policy-document) field at all, so a real client's
   `output.Permission.Permission` always decoded `nil`. Switched to
   `toPermissionDetailObject(p, pv)`.

2. `ListPermissionVersions` (same file) had the *inverse* bug: it built each
   item from `permissionDetailObject`, leaking the full policy-document text
   under a `permission` key the real `ListPermissionVersionsOutput.Permissions`
   (`[]types.ResourceSharePermissionSummary`) never sends for this op. Added
   `toPermissionVersionSummaryObject(p, pv)` and switched to it.

3. `ListPermissionAssociations` (`handler_share_permissions.go`) emitted its
   items with a fabricated `permissionArn` key (real key is `arn`, per
   `types.AssociatedPermission`) and a numeric `permissionVersion` (real
   type is a JSON string). The type mismatch is worse than a silent drop:
   the real SDK client's deserializer type-asserts `permissionVersion` to
   string and returns a hard decode error otherwise, so a real client's
   `ListPermissionAssociations` call would fail outright, not just come back
   with a missing field. Fixed both the key and the type.

**Every fix proven by hand-revert**: reverted the file to its pre-fix
content, re-ran the new test to confirm the exact predicted symptom
reproduced verbatim (nil-decoded field / leaked policy text in the raw body
/ the literal `"expected String to be of type string, got json.Number
instead"` SDK decode error), then restored the fix and diffed byte-identical
against the pre-revert version.

**New tests** (all in `services/ram/`, table-free since each targets one
specific wire-shape claim -- consistent with this file's existing
`Test_SDKRoundTrip_*` naming from sibling services like acm):

- `permission_version_shape_test.go`:
  `Test_SDKRoundTrip_CreatePermissionVersion_ReturnsPolicyDocument` (real
  `ramsdk.Client` round trip -- the real type has the field, so this is the
  correct instrument) and `Test_ListPermissionVersions_OmitsPolicyDocumentField`
  (raw-body absence assertion -- the real type has no such field for a typed
  client to observe either way, so absence can only be proven by inspecting
  the raw JSON).
- `permission_association_shape_test.go`:
  `Test_SDKRoundTrip_ListPermissionAssociations_ArnAndVersionShape` (real
  `ramsdk.Client` round trip; also incidentally proves the type-mismatch no
  longer causes a decode error).

**31 remaining ops confirmed clean** at both the wrapper-key and nested-shape
layers: `AcceptResourceShareInvitation`, `AssociateResourceShare`,
`AssociateResourceSharePermission`, `CreatePermission`, `CreateResourceShare`,
`DeletePermission`, `DeletePermissionVersion`, `DeleteResourceShare`,
`DisassociateResourceShare`, `DisassociateResourceSharePermission`,
`EnableSharingWithAwsOrganization`, `GetPermission`, `GetResourcePolicies`,
`GetResourceShareAssociations`, `GetResourceShareInvitations`,
`GetResourceShares`, `ListPendingInvitationResources`, `ListPermissions`,
`ListPrincipals`, `ListReplacePermissionAssociationsWork`,
`ListResourceSharePermissions`, `ListResourceTypes`, `ListResources`,
`ListSourceAssociations`, `PromotePermissionCreatedFromPolicy`,
`PromoteResourceShareCreatedFromPolicy`, `RejectResourceShareInvitation`,
`ReplacePermissionAssociations`, `SetDefaultPermissionVersion`,
`TagResource`/`UntagResource` (void outputs, correctly empty),
`UpdateResourceShare`.

No existing test asserted a wrong key for any of the 3 fixed ops (the
existing `TestListPermissionVersions_Pagination`/
`TestListPermissionAssociations_Pagination` tests only checked `version`
string values and slice length via `[]any`/anonymous structs, never the
leaked/missing `permission` field or the `arn`/`permissionArn` key) -- so
nothing needed correcting, only new coverage added.
