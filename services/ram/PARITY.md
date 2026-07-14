---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: ram
sdk_module: aws-sdk-go-v2/service/ram@v1.36.1   # version audited against
last_audit_commit: 8d42b940                     # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: A            # genuine fixes found (state-corruption bugs + wire-shape bugs)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateResourceShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED - previously left an orphaned share (and any principal associations processed before a rejected external principal) committed on validation failure; now validates all principals before any mutation"}
  GetResourceShare: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceShares: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED - the resourceShareArns lookup path previously ignored the name/resourceShareStatus filters and skipped pagination entirely; now applies both filters + pagination like the non-ARN path. Still missing permissionArn/permissionVersion filters (gap below)"}
  UpdateResourceShare: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourceShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "soft-deletes the share AND marks its associations DISASSOCIATED in place (kept in the associations slice) -- this is the correct pattern; see gap on DisassociateResourceShare below for the inconsistency"}
  AssociateResourceShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED - same partial-mutation bug as CreateResourceShare: principals/invitations processed before a rejected external principal were committed on error; now validates all non-duplicate principals before any mutation"}
  DisassociateResourceShare: {wire: ok, errors: ok, state: partial, persist: ok, note: "removes associations from the slice entirely (hard delete) instead of marking them DISASSOCIATED in place like DeleteResourceShare does -- see gap below"}
  GetResourceShareAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED - added the associationStatus request filter (present in the real API, silently ignored before); principal/resourceArn filters were already correct"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AcceptResourceShareInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  RejectResourceShareInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceShareInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPendingInvitationResources: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePermission: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePermissionVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePermission: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePermissionVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPermission: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPermissionVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPermissionAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  SetDefaultPermissionVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  PromotePermissionCreatedFromPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PromoteResourceShareCreatedFromPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "mock-simplified: real AWS asynchronously flips featureSet CREATED_FROM_POLICY -> PROMOTING_TO_STANDARD -> STANDARD; this backend has no featureSet state machine (CreateResourceShare always sets STANDARD) so the op is effectively a no-op validator. Acceptable since nothing here ever creates a CREATED_FROM_POLICY share"}
  AssociateResourceSharePermission: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateResourceSharePermission: {wire: partial, errors: partial, state: ok, persist: ok, note: "does not enforce AWS's 'cannot disassociate the last managed permission for a resource type still present in the share' rule (gap below); silently no-ops instead of erroring when the permission wasn't associated"}
  ListResourceSharePermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED - previously ignored the per-share associated permission version entirely (always reported the permission's current default version and hardcoded defaultVersion=true); now returns the version actually pinned to the share via AssociateResourceSharePermission's permissionVersion, with defaultVersion computed against it. Backend signature changed []*Permission -> []*ResourceSharePermissionDetail (Permission + Version pair)"}
  ReplacePermissionAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListReplacePermissionAssociationsWork: {wire: gap, errors: ok, state: gap, persist: n/a, note: "always returns an empty list; ReplacePermissionAssociations work items are not tracked anywhere so this op can never report on a real request (deferred, see gap below)"}
  ListResources: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPrincipals: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourceTypes: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "static table of shareable resource types, matches AWS's documented list"}
  ListSourceAssociations: {wire: gap, errors: ok, state: gap, persist: n/a, note: "always returns an empty list; source associations (RAM's cross-region/cross-share resource linkage) are not modeled at all (deferred)"}
  GetResourcePolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableSharingWithAwsOrganization: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "no organization/delegated-admin model exists in this backend; op is a pure ReturnValue:true ack, matches how the AWS docs describe the call (idempotent enablement, no other side effects observable via the RAM API)"}
families:
  routing: {status: ok, note: "RouteMatcher / ExtractOperation path-prefix tables manually cross-checked against every op in GetSupportedOperations(); all prefix-collision cases (e.g. /listresourcesharepermissions vs /listresources, /createpermissionversion vs /createpermission, /associateresourcesharepermission vs /associateresourceshare) are already ordered longer-prefix-first correctly. No route-matcher bug found in this service."}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore; versioned backendSnapshot (ramSnapshotVersion) with store.Registry-backed tables for resourceShares/permissions/invitations plus raw sharePermissions/associations fields. Confirmed via existing persistence_test.go round-trip coverage; updated the two call sites that read ListResourceSharePermissions' return type after this sweep's signature change."}
gaps:
  - DisassociateResourceShare hard-deletes matching associations from the in-memory slice instead of soft-deleting them (Status=DISASSOCIATED, kept in place) the way DeleteResourceShare does. This means GetResourceShareAssociations(associationStatus=DISASSOCIATED) can never show an entry produced via DisassociateResourceShare (only via DeleteResourceShare). Existing test TestRAM_Backend_DisassociateResourceShare_RemovesFromSlice pins the current (hard-delete) behavior intentionally, and AssociateResourceShare's re-association dedup logic (the `existing` set built from all associations regardless of status) would need a matching status-aware fix to allow re-associating a previously-disassociated entity without producing duplicate rows. Left unfixed this sweep due to blast radius; needs a bd issue to redesign both together.
  - GetResourceShares does not support the permissionArn/permissionVersion request filters that real AWS exposes (filter resource shares by an associated managed permission). Not implemented; would need to reuse the sharePermissions map.
  - DisassociateResourceSharePermission does not enforce AWS's rule that you cannot disassociate the last managed permission associated with a resource type still present in the share's resources. Requires cross-referencing sharePermissions against the resource types of active RESOURCE associations for the share.
  - CreateResourceShare / AssociateResourceShare do not auto-associate the default AWS-managed permission for a resource's type when no permissionArns are given (real AWS attaches e.g. AWSRAMDefaultPermissionEC2Subnet automatically). ListResourceSharePermissions returns empty for such shares even though real AWS would show the default permission. resourceTypeFromARN + awsBuiltInPermissions already contain enough data to build the resourceType -> default permission ARN mapping needed to close this gap.
  - ListReplacePermissionAssociationsWork and ListSourceAssociations are permanently-empty stubs (documented honestly in code comments, not disguised). ReplacePermissionAssociations work items and source associations are not tracked anywhere in the backend.
  - Several "This member is required" SDK input fields (ResourceOwner on GetResourceShares/ListResources/ListPrincipals, AssociationType on GetResourceShareAssociations) are not validated as required -- missing them is silently treated as an empty-string default rather than erroring. Consistent with this codebase's generally permissive input handling; not fixed this sweep.
  - permissionSummaryObject/permissionDetailObject emit a resourceRegionScope field that does not exist on the real ResourceSharePermissionSummary/ResourceSharePermissionDetail SDK types (harmless: the restjson1 deserializer ignores unrecognized fields, confirmed by reading deserializers.go). Not removed since it's a no-op field, not a bug.
deferred:
  - PromoteResourceShareCreatedFromPolicy's featureSet state machine (CREATED_FROM_POLICY -> PROMOTING_TO_STANDARD -> STANDARD) is not modeled; every share created here is already STANDARD so this hasn't caused observed drift, but if CREATED_FROM_POLICY share creation is ever added, this needs revisiting.
leaks: {status: clean, note: "no goroutines/janitors in this backend; all state is plain maps/slices behind the single lockmetrics.RWMutex, snapshotted/restored atomically under that lock."}
---

## Notes

Protocol: REST-JSON (restjson1), single-segment lowercase POST paths (e.g.
`/createresourceshare`, `/listresourcesharepermissions`). Timestamps are
epoch-seconds JSON numbers (`epochSeconds` helper), matching the SDK
deserializer's `smithytime.ParseEpochSeconds` for every `creationTime`/
`lastUpdatedTime`/`invitationTimestamp` field -- verified directly against
`deserializers.go` for `ResourceSharePermissionSummary` and
`ResourceSharePermissionDetail`.

**Bugs fixed this sweep (2026-07-13):**

1. **CreateResourceShare / AssociateResourceShare state corruption on
   rejected external principal** (`backend.go`). Both methods mutated
   backend state (`resourceShares.Put`, `b.associations` appends, invitation
   creation) *inside* the loop that validates `AllowExternalPrincipals`, so
   a request with e.g. `principals: [ownAccountID, externalID]` would commit
   the share (or the first principal's association/invitation) before
   failing on the second principal. The caller sees an error, but the
   backend keeps an orphaned resource share -- which also permanently blocks
   retrying with the same name (`ResourceShareAlreadyExistsException`).
   Fixed by validating every principal in a first pass before any mutation
   in both methods.

2. **ListResourceSharePermissions ignored the per-share permission version**
   (`backend.go`, `interfaces.go`, `handler.go`). AWS's
   `ResourceSharePermissionSummary.Version`/`.DefaultVersion` describe the
   version *pinned to that resource share* (set via
   `AssociateResourceSharePermission`'s `permissionVersion` parameter), not
   the permission's global default version. gopherstack's backend method
   threw away the version stored in `sharePermissions[shareARN][permARN]`
   and reported `p.DefaultVersion` / `defaultVersion: true` unconditionally.
   Fixed by changing `ListResourceSharePermissions` to return
   `[]*ResourceSharePermissionDetail{Permission, Version}` pairs, and adding
   `toResourceSharePermissionSummaryObject` to compute `defaultVersion` as
   `version == permission.DefaultVersion`.

3. **GetResourceShares `resourceShareArns` path bypassed name/status filters
   and pagination** (`handler.go`). When `resourceShareArns` was supplied,
   the handler looked shares up individually and returned them unfiltered
   and unpaginated, ignoring `name`/`resourceShareStatus` and never applying
   `maxResults`/`nextToken`. AWS combines all of these filters. Fixed by
   applying the same name/status filters as the non-ARN path and running the
   result through `ramPaginate` either way (refactored into
   `getResourceSharesByARN`/`getResourceSharesByFilter` to keep cognitive
   complexity under the gocognit threshold).

4. **GetResourceShareAssociations missing `associationStatus` filter**
   (`handler.go`). The real API's `GetResourceShareAssociationsInput` has an
   `AssociationStatus` field (wire key `associationStatus`) that the
   gopherstack request struct didn't have at all, so a caller polling for
   e.g. `DISASSOCIATED` entries got every status mixed together. Added the
   field and filter.

5. **Missing `status` field on permission wire objects** (`handler.go`).
   `ResourceSharePermissionSummary`/`ResourceSharePermissionDetail` both
   carry a `status` field (`ATTACHABLE`/`UNATTACHABLE`/`DELETING`/`DELETED`)
   that gopherstack never emitted, leaving `*Permission.Status` nil/empty on
   the client side. Added `status: "ATTACHABLE"` (the only steady state this
   backend models -- there's no async permission-deletion pipeline).

**Traps for the next auditor:**

- `DisassociateResourceShare` hard-deletes matching entries from
  `b.associations` (see gap above) while `DeleteResourceShare` soft-deletes
  (marks `DISASSOCIATED`, keeps the row). This looks inconsistent and *is*
  inconsistent with real AWS, but changing it safely requires also making
  `AssociateResourceShare`'s re-association dedup logic status-aware (it
  currently treats any row for `(shareARN, entity)` as "already associated"
  regardless of status). Don't fix one without the other.
- `resourceTypeFromARN`'s `typeMap` and `awsBuiltInPermissions` already
  contain everything needed to auto-attach default managed permissions on
  `CreateResourceShare`/`AssociateResourceShare` (see gap above) -- a future
  sweep implementing this should reuse them rather than re-deriving the
  resource-type-to-permission mapping.
- The `resourceRegionScope` field on permission summary/detail JSON objects
  is not part of the real SDK shape for those types (only `Resource` and
  `ServiceNameAndResourceType` have it) but is harmless noise since restjson1
  deserializers ignore unrecognized fields -- don't "fix" this by removing it
  without checking whether tests depend on it.
