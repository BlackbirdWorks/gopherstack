---
service: resourcegroups
sdk_module: aws-sdk-go-v2/service/resourcegroups@v1.33.22
last_audit_commit: 343e1204   # HEAD when this audit started (parity-4 sweep)
last_audit_date: 2026-07-13
overall: A            # genuine wire-shape and behavior fixes found and fixed
ops:
  CreateGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Tags/ResourceQuery no longer nested inside Group; Owner tag renamed"}
  GetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Owner wire tag"}
  UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Owner wire tag, now includes ApplicationTag"}
  DeleteGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now echoes deleted Group (was empty envelope)"}
  ListGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: GroupIdentifiers now include DisplayName/Criticality/Owner"}
  GetGroupQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGroupQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGroupConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: removed fabricated GroupName field, added required Status field"}
  PutGroupConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GroupResources: {wire: ok, errors: ok, state: ok, persist: ok}
  UngroupResources: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroupResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "deprecated ResourceIdentifiers/QueryErrors fields not populated -- see gaps"}
  ListGroupingStatuses: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UpdatedAt now epoch-seconds, was RFC3339 string"}
  SearchResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "QueryErrors field never populated -- see gaps"}
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
  - "ListGroups Filters only supports 'resource-type', 'configuration-type', 'name-prefix'; real AWS GroupFilterName enum is resource-type|configuration-type|owner|display-name|criticality (no name-prefix). Filtering by owner/display-name/criticality silently matches everything instead of filtering; name-prefix is a gopherstack-only extension. (bd: gopherstack-rg-filters)"
  - "CreateGroup/UpdateGroup do not accept an Owner input field (real API supports it on both); Owner is therefore always empty in this emulator. CreateGroup also does not accept DisplayName/Criticality at creation time (only settable via a follow-up UpdateGroup). (bd: gopherstack-rg-owner-input)"
  - "SearchResources/ListGroupResources never populate QueryErrors (CLOUDFORMATION_STACK_* failure reporting) since CloudFormation-stack-based queries are not modeled. ListGroupResourcesOutput also omits the deprecated ResourceIdentifiers field (Resources is populated; most SDKs read Resources)."
  - "ListGroupResourcesItem/GroupConfigurationItem 'Status' (AWS::EC2::HostManagement pending-membership state) is never populated; only host-management-specific clients would notice."
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
