---
service: resourcegroups
sdk_module: aws-sdk-go-v2/service/resourcegroups@v1.33.22
last_audit_commit: 343e1204   # HEAD when this audit started (parity-4 sweep)
last_audit_date: 2026-07-24
overall: A            # genuine wire-shape and behavior fixes found and fixed
ops:
  CreateGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Tags/ResourceQuery no longer nested inside Group; Owner tag renamed; now accepts Owner/DisplayName/Criticality at creation time via CreateGroupOption; Criticality range corrected to 1-10"}
  GetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Owner wire tag"}
  UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Owner wire tag, now includes ApplicationTag; now accepts Owner input field; Criticality range corrected to 1-10 (was 1-5)"}
  DeleteGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now echoes deleted Group (was empty envelope)"}
  ListGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: GroupIdentifiers now include DisplayName/Criticality/Owner; Filters now support the real owner/display-name/criticality GroupFilterName values; invented name-prefix filter removed"}
  GetGroupQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGroupQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGroupConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: removed fabricated GroupName field, added required Status field"}
  PutGroupConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GroupResources: {wire: ok, errors: ok, state: ok, persist: ok}
  UngroupResources: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroupResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: deprecated ResourceIdentifiers field now populated identically to Resources; QueryErrors field now present on the wire (always empty -- see gaps, CFN-stack queries not modeled)"}
  ListGroupingStatuses: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UpdatedAt now epoch-seconds, was RFC3339 string"}
  SearchResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: QueryErrors field now present on the wire (always empty -- see gaps, CFN-stack queries not modeled)"}
  GetTags: {wire: ok, errors: ok, state: ok, persist: ok}
  Tag: {wire: ok, errors: ok, state: ok, persist: ok}
  Untag: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccountSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAccountSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  StartTagSyncTask: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelTagSyncTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was setting fabricated CANCELLED status (not a valid TagSyncTaskStatus wire value); now deletes the task per AWS docs"}
  GetTagSyncTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CreatedAt now epoch-seconds, was ISO8601 string"}
  ListTagSyncTasks: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CreatedAt now epoch-seconds, was RFC3339 string (default time.Time marshal)"}
families:
  route_matcher: {status: ok, note: "verified every REST path/method (POST for all ops except GET/PUT/PATCH /resources/{Arn}/tags) against serializers.go opPath/request.Method -- exact match, no gaps"}
gaps:
  - "SearchResources/ListGroupResources' QueryErrors field is present on the wire but always empty: its documented ErrorCode values (CLOUDFORMATION_STACK_INACTIVE, CLOUDFORMATION_STACK_NOT_EXISTING, CLOUDFORMATION_STACK_UNASSUMABLE_ROLE, RESOURCE_TYPE_NOT_SUPPORTED) only ever arise for CLOUDFORMATION_STACK_1_0-based groups, which this emulator does not model (no CloudFormation backend integration). Genuinely cannot be finished without modeling CloudFormation stacks. (bd: gopherstack-rg-cfn-queryerrors)"
  - "ListGroupResourcesItem.Status (AWS::EC2::HostManagement pending-membership state, real types.ResourceStatus) is never populated; this emulator does not model AWS::EC2::HostManagement async grouping, so the field is legitimately always absent (matches real AWS behavior for any group NOT of that configuration type, but would be wrong for one that is). Genuinely cannot be finished without modeling async host-management grouping. (bd: gopherstack-rg-hostmgmt-status)"
deferred: []
leaks: {status: clean, note: "no goroutines/janitors; CancelTagSyncTask fix removes the only TTL-dependent eviction path's live producer (tagSyncTaskTTL eviction in ListTagSyncTasks is now effectively dead code since nothing sets a non-ACTIVE status, but is left as harmless defensive generic logic for a future ERROR-status producer)"}
---

## Notes

Protocol: **rest-json1**. Every op except `Tag` (PUT), `Untag` (PATCH), and `GetTags`
(GET) uses POST, including "list"/"search" verbs (`ListGroups` is `POST /groups-list`,
`SearchResources` is `POST /resources/search`, `UpdateGroupQuery` is
`POST /update-group-query` -- note: NOT PUT, despite the name). All of this was
verified directly against `serializers.go`'s `opPath`/`request.Method` in
aws-sdk-go-v2/service/resourcegroups@v1.33.22 and matches gopherstack's
`rgRESTPathOps` exactly; no route-matcher bugs found this sweep.

### Real bugs fixed this sweep

1. **`Owner` wire tag was `OwnerId`.** The real `types.Group`/`types.GroupIdentifier`
   JSON field is `Owner` (confirmed via `deserializers.go`'s `awsRestjson1_deserializeDocumentGroup`
   `case "Owner":`), not `OwnerId`. gopherstack additionally fabricated its value as the
   AWS account ID on every `CreateGroup` call; real `Owner` is an optional, free-form,
   caller-supplied string (person/email/team) with no relation to account ID, and is
   `nil`/absent unless the caller supplies it. Both the tag and the fabrication were
   fixed; `Owner` is now unset by default (see gaps: no input path exists to set it).

2. **`CreateGroupOutput` nested `Tags`/`ResourceQuery` inside the `Group` object.**
   The real `types.Group` shape has no `Tags` or `ResourceQuery` members -- both travel
   as separate top-level sibling fields of `CreateGroupOutput`
   (`{"Group":{...},"ResourceQuery":{...},"Tags":{...}}`). gopherstack was marshaling
   the internal `*Group` backend struct directly as the `"Group"` field, which (a) leaked
   `Tags`/`ResourceQuery` into the wrong place and (b) meant a real SDK client's
   `CreateGroupOutput.Tags` was always empty even though the caller passed tags on
   create -- real data loss for any real-SDK caller. Fixed by building the wire shape
   from a dedicated `getGroupBody` (now shared by CreateGroup/GetGroup/UpdateGroup) and
   adding a top-level `Tags` field.

3. **`GetGroupConfiguration` fabricated a `GroupName` field and omitted `Status`.**
   Real `types.GroupConfiguration` is `{Configuration, FailureReason,
   ProposedConfiguration, Status}` -- no `GroupName`. `Status` (`"UPDATE_COMPLETE"` once
   a configuration is set) was entirely missing from the response. Fixed by reusing the
   `groupConfigurationBody` type (already correct, used by `CreateGroup`'s inline
   `GroupConfiguration`) instead of a bespoke shape.

4. **`CancelTagSyncTask` transitioned to a fabricated `"CANCELLED"` status.**
   `TagSyncTaskStatus`'s only documented wire values are `ACTIVE` and `ERROR`
   (confirmed via `types/enums.go` and the AWS API reference); there is no `CANCELLED`
   value. AWS's own docs describe `CancelTagSyncTask` as taking "the TaskArn of the
   tag-sync task **you want to delete**", and the operation requires
   `resource-groups:DeleteGroup` permission. gopherstack instead kept the task alive for
   24h with an invalid enum value, which a prior audit pass (`handler_audit1_test.go`
   "issue #22") had deliberately locked in as expected behavior -- re-verified against
   AWS docs this sweep and reverted: `CancelTagSyncTask` now deletes the task outright,
   and `GetTagSyncTask`/`ListTagSyncTasks` no longer find it afterward.

5. **`TagSyncTask.CreatedAt` and `GroupingStatusItem.UpdatedAt` were RFC3339/ISO8601
   strings, not epoch-seconds numbers.** rest-json1 uses the `unixTimestamp` format
   (JSON number of seconds since epoch) for these fields (confirmed via
   `deserializers.go`'s `smithytime.ParseEpochSeconds` calls) -- the exact bug class
   flagged in `.claude/memories/parity-principles.md`. `GetTagSyncTask` was manually
   formatting an ISO8601 string; `ListTagSyncTasks`/`ListGroupingStatuses` were relying
   on `time.Time`'s default `encoding/json` marshaling (RFC3339). Fixed with
   `pkgs/awstime.Epoch` via new wire DTOs (`tagSyncTaskItem`, `groupingStatusItemWire`)
   that keep the backend's internal `time.Time` representation (used for persistence and
   pagination-token derivation) separate from the wire encoding.

6. **`DeleteGroup` returned an empty envelope.** Real `DeleteGroupOutput.Group` echoes
   back a full description of the just-deleted group. gopherstack discarded it. Fixed:
   `InMemoryBackend.DeleteGroup` now returns `(*Group, error)` (was `error`), and the
   handler echoes it via `getGroupBody`.

7. **`ListGroups`' `GroupIdentifiers` omitted `DisplayName`/`Criticality`/`Owner`.**
   Real `types.GroupIdentifier` carries all of `GroupName`, `GroupArn`, `Description`,
   `DisplayName`, `Criticality`, `Owner`. Only `GroupName`/`GroupArn`/`Description` were
   populated. Fixed (Owner remains unset per gap #2 above, same as everywhere else).

### Real bugs fixed this sweep (parity-3, 2026-07-24)

8. **`ListGroups` `Filters` supported an invented `"name-prefix"` filter name and was
   missing three real ones.** The real `types.GroupFilterName` enum (confirmed via
   `types/enums.go`'s `GroupFilterName.Values()`) is exactly
   `resource-type|configuration-type|owner|display-name|criticality` -- there is no
   `name-prefix` value. gopherstack had fabricated `name-prefix` and silently ignored
   `owner`/`display-name`/`criticality` filters (matching everything instead of
   filtering, since the `switch` had no `case` for them). Fixed: `name-prefix` and its
   handler/tests were deleted; `owner`/`display-name` (exact string match) and
   `criticality` (exact match against `strconv.Itoa(g.Criticality)`) are now
   implemented in `groupMatchesFilters`.

9. **`CreateGroup` had no input path for `Owner`/`DisplayName`/`Criticality`.** The real
   `CreateGroupInput` accepts all three directly (confirmed via `api_op_CreateGroup.go`);
   gopherstack only allowed setting them via a follow-up `UpdateGroup` call, so a
   real-SDK caller's `CreateGroupInput.Owner`/`DisplayName`/`Criticality` were silently
   dropped on create. Fixed: `InMemoryBackend.CreateGroup` gained a
   `...CreateGroupOption` variadic parameter (`WithOwner`/`WithDisplayName`/
   `WithCriticality`) -- chosen over widening the positional parameter list to avoid
   breaking the ~65 existing call sites that already pass exactly six positional
   arguments -- and the `CreateGroup` handler now threads `Owner`/`DisplayName`/
   `Criticality` from `CreateGroupInput` through to it.

10. **`UpdateGroup` had no input path for `Owner`.** The real `UpdateGroupInput` accepts
    `Owner` alongside `Description`/`DisplayName`/`Criticality` (confirmed via
    `api_op_UpdateGroup.go`); gopherstack's `UpdateGroup` never accepted or updated it.
    Fixed: `InMemoryBackend.UpdateGroup` gained an `owner string` parameter (leaves
    `Owner` unchanged when empty, matching the existing `displayName`/`criticality`
    convention), and the handler now reads `Owner` from `UpdateGroupInput`.

11. **`Criticality` validation used the wrong range (1-5 instead of 1-10).** Both
    `api_op_CreateGroup.go` and `api_op_UpdateGroup.go` document Criticality as "a scale
    of 1 to 10, with a rank of 1 being the most critical, and a rank of 10 being least
    critical" -- gopherstack's `UpdateGroup` rejected valid values 6-10 with a
    `BadRequestException`. Fixed via a shared `validateCriticality` helper (now also
    used by the new `CreateGroup` Criticality input) enforcing the correct 1-10 range.

12. **`ListGroupResourcesOutput` omitted the deprecated `ResourceIdentifiers` field.**
    The real API keeps `ResourceIdentifiers` populated alongside `Resources` for
    backward-compatible clients that still read the deprecated field (confirmed via
    `api_op_ListGroupResources.go`); gopherstack only populated `Resources`. Fixed:
    `ResourceIdentifiers` is now populated identically to `Resources`.

13. **`SearchResourcesOutput`/`ListGroupResourcesOutput` were missing the `QueryErrors`
    field entirely.** Both real output shapes carry a `QueryErrors []types.QueryError`
    member (confirmed via `api_op_SearchResources.go`/`api_op_ListGroupResources.go`);
    it was absent from gopherstack's wire structs. Added a `queryErrorWire` DTO and the
    field to both outputs (`omitempty`, so it serializes identically to real AWS for the
    common case of no errors). It remains always-empty here since
    `CLOUDFORMATION_STACK_1_0`-based groups (the only source of `QueryError`s) are not
    modeled -- see gaps.

### Wire-shape traps confirmed correct (do not re-flag)

- `ListGroupResourcesItem`'s wire shape (`Identifier`, optional `Status`) matches;
  `Status` (host-management pending-state) is legitimately omitted since gopherstack
  doesn't model `AWS::EC2::HostManagement` async grouping.
- `GroupResourcesOutput`/`UngroupResourcesOutput`'s `Pending` field reusing
  `GroupingFailedItem`'s shape (`ResourceArn`/`ErrorCode`/`ErrorMessage`) instead of the
  real `PendingResource` shape (`ResourceArn` only) is currently harmless: `Pending` is
  always an empty array (all grouping/ungrouping here is synchronous), so the extra
  fields never actually serialize. Would need fixing if async pending-state support is
  ever added.
- `ListGroupsFilter`'s "configuration-type"/"resource-type" filter *values* match real
  behavior; only the filter *names* diverge (see gaps).
