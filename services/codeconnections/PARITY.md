---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: codeconnections
sdk_module: aws-sdk-go-v2/service/codeconnections@v1.13.4   # version audited against; go.mod pin as of this pass (was stale at v1.10.22)
last_audit_commit: 749ff939+wt                    # HEAD when this manifest was PREVIOUSLY written; this pass's changes are uncommitted working-tree changes on top (git commands unavailable to this pass)
last_audit_date: 2026-08-10
overall: A            # true-parity pass: closed every gaps/deferred item from the prior audit, plus new wire/error-shape bugs found while field-diffing this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags field on CreateConnectionOutput correct. validProviderTypes has all 6 real ProviderType values. RESOLVED this pass (was a gap): duplicate ConnectionName is no longer rejected. CreateConnection's real error list -- confirmed by reading the operation's own deserializer, aws-sdk-go-v2/service/codeconnections@v1.13.4 deserializers.go's awsAwsjson10_deserializeOpErrorCreateConnection switch -- is exactly [LimitExceededException, ResourceNotFoundException, ResourceUnavailableException]; no ResourceAlreadyExistsException case exists, so a real client sending that error code would fall through to the generic/unmodelled branch. Sibling ops CreateRepositoryLink/CreateSyncConfiguration in the same service DO have a ResourceAlreadyExistsException case in their deserializers, showing the omission here is deliberate, not an SDK oversight. The direction of the prior bug: gopherstack was MORE RESTRICTIVE than real AWS, rejecting creates the real service accepts. The connectionsByName index (its only reader) was removed as dead weight."}
  GetConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConnections: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateHost: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags field on CreateHostOutput correct. RESOLVED this pass (was a gap): duplicate Name is no longer rejected. CreateHost's real error list, confirmed via its own deserializer (deserializers.go's awsAwsjson10_deserializeOpErrorCreateHost switch), is exactly [LimitExceededException] -- no ResourceAlreadyExistsException. Same direction of bug and same fix as CreateConnection above; the hostsByName index was removed as dead weight."}
  GetHost: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHosts: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHost: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: the in-use check (added in the 2026-07-13 pass) used ConflictException, but DeleteHost's real, complete error list (botocore codeconnections/2023-12-01/service-2.json) is exactly [ResourceNotFoundException, ResourceUnavailableException] -- ConflictException is not a possible error for this operation at all. Changed ErrResourceInUse's wire type to ResourceUnavailableException (its doc note also covers the sibling 'host cannot be deleted while VPC_CONFIG_INITIALIZING/VPC_CONFIG_DELETING' case, the same 'host not currently deletable' family)."}
  UpdateHost: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "duplicate-link check present (ResourceAlreadyExistsException IS in CreateRepositoryLink's real error list, confirmed via botocore -- unlike CreateConnection/CreateHost above)."}
  GetRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: response item no longer carries an invented Tags field (see gaps history below -- removed, not merely noted)."}
  DeleteRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "in-use check present; SyncConfigurationStillExistsException IS in DeleteRepositoryLink's real error list, confirmed via botocore."}
  ListRepositoryLinks: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: response items no longer carry an invented Tags field."}
  CreateSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "duplicate check present (ResourceAlreadyExistsException IS in CreateSyncConfiguration's real error list, confirmed via its deserializer). PullRequestComment (present in this service's pinned SDK types.SyncConfiguration/CreateSyncConfigurationInput) is accepted, stored, and round-tripped through Get/List/persistence. FIXED this pass: PublishDeploymentStatus/TriggerResourceUpdateOn/PullRequestComment were accepted with NO enum validation at all (types/enums.go: PublishDeploymentStatus/PullRequestComment={ENABLED,DISABLED}, TriggerResourceUpdateOn={ANY_CHANGE,FILE_CHANGE}) -- any string, including garbage, was silently stored and echoed back. Now validated against their real enum sets (empty string still allowed -- none of the three are required input members)."}
  GetSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "deletion also removes any syncBlockers rows for that resource+syncType (cascade fix from a prior pass) -- DeleteSyncConfiguration's real error list has no 'blockers still exist'-style exception, so deletion stays unconditional; only the orphaned children are cleaned up."}
  UpdateSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "PullRequestComment updatable (empty string preserves the existing value, matching the PublishDeploymentStatus/TriggerResourceUpdateOn convention). FIXED this pass: same missing enum validation as CreateSyncConfiguration above, same fix (empty string still means 'leave unchanged')."}
  ListSyncConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRepositorySyncStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "epoch-seconds StartedAt/Events[].Time unchanged from 2026-07-13 pass (already correct); RepositorySyncAttempt's real wire shape is only Events/StartedAt/Status (no revision fields), which this response already matched -- unlike ResourceSyncAttempt below."}
  GetResourceSyncStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "MAJOR wire-shape fix this pass: the real ResourceSyncAttempt type (used for both LatestSync and LatestSuccessfulSync) requires Events/InitialRevision/StartedAt/Status/Target/TargetRevision -- InitialRevision/Target/TargetRevision were entirely missing from the response struct (confirmed via aws-sdk-go-v2/service/codeconnections@v1.13.4's own deserializers.go awsAwsjson10_deserializeDocumentResourceSyncAttempt switch, which has explicit cases for all six). Also, the previously-deferred optional DesiredState/LatestSuccessfulSync top-level members are now populated: this backend does not simulate real git-repo content, so InitialRevision/TargetRevision/DesiredState are synthesized identically from the resource's SyncConfiguration (Branch/ConfigFile-as-Directory/OwnerId/ProviderType/RepositoryName), with a deterministic (not random) Sha derived via syntheticRevisionSha (sha1 of stable identity fields, hex-encoded -- SHA is unconstrained beyond min:1/max:255 on the wire, real git shas are simulated shape only). LatestSuccessfulSync is populated identically to LatestSync since every synthesized attempt is immediately SUCCEEDED (no partial/failed-attempt history is modeled)."}
  GetSyncBlockerSummary: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSyncBlocker: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRepositorySyncDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "RESOLVED this pass (was 'deferred: pagination'): confirmed via aws-sdk-go-v2/service/codeconnections@v1.13.4's ListRepositorySyncDefinitionsInput struct AND botocore's paginators-1.json (empty pagination config for this op) that the real INPUT has NO NextToken/MaxResults member at all, even though the real OUTPUT has an optional NextToken -- a real client has no way to ever request a further page for this specific operation. Added a NextToken field to the output wire shape for completeness (real member); it always stays nil/omitted since every definition is returned in one response, which is the only behavior a real client could ever observe anyway."}
  UpdateRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: response no longer carries an invented Tags field."}
families:
  RouteMatcher: {status: ok, note: "unchanged from 2026-07-13 pass; X-Amz-Target prefix and Content-Type verified byte-for-byte, no bug."}
  ConnectionStatus: {status: ok, note: "unchanged; CreateConnection sets AVAILABLE immediately, defensible emulation choice, do not 'fix' to PENDING."}
  HostStatus: {status: ok, note: "unchanged; CreateHost sets AVAILABLE immediately, defensible emulation choice."}
  SyncBlockerPersistence: {status: ok, note: "unchanged store.Table structure from 2026-07-13 pass; this pass's DeleteSyncConfiguration cascade-cleanup (see ops above) reuses the existing syncBlockersByResource index, no schema change, no snapshot version bump needed."}
  ErrValidationWireType: {status: ok, note: "FIXED this pass: ErrValidation's wire type was 'ValidationException', a gopherstack-INVENTED error code -- aws-sdk-go-v2/service/codeconnections@v1.13.4's types/errors.go has NO ValidationException type at all in its full modeled exception set (17 types: AccessDeniedException/ConcurrentModificationException/ConditionalCheckFailedException/ConflictException/InternalServerException/InvalidInputException/LimitExceededException/ResourceAlreadyExistsException/ResourceNotFoundException/ResourceUnavailableException/RetryLatestCommitFailedException/SyncBlockerDoesNotExistException/SyncConfigurationStillExistsException/ThrottlingException/UnsupportedOperationException/UnsupportedProviderTypeException/UpdateOutOfSyncException). Renamed to InvalidInputException, confirmed as the real type for malformed/missing-required-field input by cross-referencing every mutating op's real error list in botocore's codeconnections/2023-12-01/service-2.json (all list InvalidInputException for input validation; none list ValidationException). This affected every required-field check across every handler in this service (Handler.resolveErrorType's single switch case), not just one op."}
gaps: []
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table/Index behind lockmetrics.RWMutex, snapshotted via persistence.go. FIXED this pass: DeleteSyncConfiguration previously left orphaned syncBlockers rows behind forever (see GetResourceSyncStatus/DeleteSyncConfiguration ops notes) -- a real ghost-row leak, now cleaned up via a cascade delete keyed off the existing syncBlockersByResource index. No new goroutines or tables were introduced; the fix reuses existing indexes."}
---

## Notes

- **Protocol**: `application/x-amz-json-1.0` (awsjson1.0), single POST endpoint,
  `X-Amz-Target: CodeConnections_20231201.<Op>` -- unchanged from the 2026-07-13
  pass, re-verified against serializers.go, no bug.

- **Epoch-seconds timestamps**: unchanged from the 2026-07-13 pass (already
  fixed then via `pkgs/awstime.Epoch`); re-verified this pass while field-diffing
  `GetResourceSyncStatus`'s full response shape.

- **`ErrValidation` wire type was `ValidationException`, a gopherstack-INVENTED
  error code** -- the real SDK's `types/errors.go` has no such type in its full
  17-member modeled exception set. Renamed to `InvalidInputException`, the real
  type for input validation, confirmed against every mutating op's real error
  list in botocore's `codeconnections/2023-12-01/service-2.json`. This is a
  single fix in `Handler.resolveErrorType`'s switch (`handler.go`) plus the
  `ErrValidation` sentinel's message string (`errors.go`), but it affects the
  wire error type returned by every required-field check across every handler
  in this service.

- **`DeleteHost`'s in-use rejection used `ConflictException`**, a type not in
  DeleteHost's real, complete error list (`[ResourceNotFoundException,
  ResourceUnavailableException]` per botocore). Renamed to
  `ResourceUnavailableException` -- the real type, and the same one covering
  the doc-noted sibling case ("host cannot be deleted while
  VPC_CONFIG_INITIALIZING/VPC_CONFIG_DELETING").

- **`GetResourceSyncStatus`'s `ResourceSyncAttempt` response items were
  missing `InitialRevision`/`Target`/`TargetRevision`** entirely -- all three
  are required members of the real `ResourceSyncAttempt` type (confirmed via
  this service's own `deserializers.go`). Additionally, the previously-deferred
  optional `DesiredState`/`LatestSuccessfulSync` top-level output members were
  never populated. Both are now real: synthesized from the resource's
  `SyncConfiguration` (this backend does not simulate actual git-repo commit
  history), with a deterministic `Sha` derived via a stable hash of the
  configuration's identity fields (`syntheticRevisionSha`,
  `repository_sync.go`) rather than a fabricated/random one.

- **`RepositoryLinkInfo` response items carried an invented `Tags` field** --
  the real `RepositoryLinkInfo` wire type has no `Tags` member at all (tags
  for a repository link are retrievable only via `ListTagsForResource`). A
  previous audit pass found this but left it in place "to avoid risk breaking
  existing tests"; per this pass's mandate to delete gopherstack-invented
  fields not in the real SDK, it has been removed from
  `CreateRepositoryLink`/`GetRepositoryLink`/`UpdateRepositoryLink`/
  `ListRepositoryLinks` response items, and the one test that asserted on it
  (`TestRepositoryLinkTagsInListItem`) was rewritten
  (`TestRepositoryLinkNoTagsFieldInListItem`) to assert the field's absence
  and that the tags remain real state via `ListTagsForResource`.

- **`PullRequestComment`** (present in this service's pinned SDK
  `types.SyncConfiguration`/`CreateSyncConfigurationInput`/
  `UpdateSyncConfigurationInput` but previously entirely unimplemented) is now
  a real field: accepted on create/update, stored on `SyncConfiguration`,
  returned by Get/List, and round-tripped through the `syncConfigurations`
  DTO persistence table (no snapshot version bump needed -- old snapshots
  without the field simply decode it as `""`, matching the existing
  `PublishDeploymentStatus`/`TriggerResourceUpdateOn` precedent on the same
  struct).

- **`DeleteSyncConfiguration` leaked `syncBlockers` rows** (LEAKS finding):
  deletion only ever removed the `syncConfigurations` entry, never the
  `syncBlockers` rows indexed under the same resource+syncType key. Because
  `GetSyncBlockerSummary` requires the sync configuration to still exist, the
  orphaned blockers were invisible through the API immediately -- but they
  were not actually gone: recreating a sync configuration for the exact same
  `ResourceName`+`SyncType` would make the old, already-resolved blockers
  silently reappear via `GetSyncBlockerSummary`. Fixed with a cascade delete
  in `DeleteSyncConfiguration` (`sync_configurations.go`) reusing the existing
  `syncBlockersByResource` index; locked by
  `TestDeleteSyncConfiguration_CleansUpSyncBlockers`.

- **`CreateConnection` accepted a `HostArn` referencing a nonexistent host
  with zero validation.** Fixed: `CreateConnection`'s real, complete error
  list (`[LimitExceededException, ResourceNotFoundException,
  ResourceUnavailableException]` per botocore) confirms
  `ResourceNotFoundException` is the correct type for a missing host, the
  same type `GetHost`/`DeleteHost` already use. A previous audit pass left
  this gap open citing inability to confirm without live AWS access; the
  botocore error list resolves that uncertainty.

- **Ambiguity RESOLVED this pass**: `CreateConnection`/`CreateHost` used to
  reject a duplicate `ConnectionName`/`Name` with `ResourceAlreadyExistsException`,
  even though neither operation's real error list includes that exception. The
  operation's own deserializer (not just botocore's service-2.json) is the
  authoritative enumeration: `aws-sdk-go-v2/service/codeconnections@v1.13.4`
  `deserializers.go`'s `awsAwsjson10_deserializeOpErrorCreateConnection` switch
  recognizes only `LimitExceededException`/`ResourceNotFoundException`/
  `ResourceUnavailableException`, and `awsAwsjson10_deserializeOpErrorCreateHost`'s
  switch recognizes only `LimitExceededException` -- a real client sending
  `ResourceAlreadyExistsException` for either op falls through to the generic/
  unmodelled-error branch, meaning the real service cannot use that exception
  for either op. This is not an oversight: sibling ops in the very same
  service, `CreateRepositoryLink` and `CreateSyncConfiguration`, DO have a
  `ResourceAlreadyExistsException` case in their own deserializers, proving the
  modelers know how to add it when the constraint is enforced and chose not to
  here. Verdict: real AWS does not reject a duplicate name for these two ops
  (the "must be unique" doc text is not backed by an enforced API contract);
  gopherstack's prior behavior was MORE RESTRICTIVE than the real service,
  rejecting creates a real client would receive as 200s with distinct ARNs.
  Fixed by removing the duplicate-name check from `CreateConnection`/
  `CreateHost` (`connections.go`/`hosts.go`); the now-dead `connectionsByName`/
  `hostsByName` secondary indexes (their only reader) were removed with it
  (`store.go`/`store_setup.go`). `ErrAlreadyExists` remains in use for
  `CreateRepositoryLink`/`CreateSyncConfiguration`, where it is real.

- **`PublishDeploymentStatus`/`TriggerResourceUpdateOn`/`PullRequestComment`
  had zero enum validation** on `CreateSyncConfiguration`/
  `UpdateSyncConfiguration` -- any string, valid or not, was silently accepted
  and echoed back verbatim. The real enums (`types/enums.go`) are
  `PublishDeploymentStatus`/`PullRequestComment` = `{ENABLED, DISABLED}` and
  `TriggerResourceUpdateOn` = `{ANY_CHANGE, FILE_CHANGE}`. None of the three
  are required input members, so an omitted/empty value is still accepted (and
  for `UpdateSyncConfiguration`, empty string remains the existing
  "leave unchanged" sentinel). Fixed with `validEnabledDisabled`/
  `validTriggerResourceUpdateOn` in `models.go`, matching the
  `validProviderTypes`/`validSyncTypes` pattern already used elsewhere in this
  service.

- 2026-08-21 (gopherstack-r80d batch 23, required-OUTPUT-member cut): read all
  14 ops-with-required (15 required fields total, tied with
  codestarconnections/awsconfig as the largest remaining candidates after
  sagemaker) end to end against `aws-sdk-go-v2/service/codeconnections@
  v1.13.4`'s `api_op_*.go`, plus every nested domain struct one level deeper
  (`RepositoryLinkInfo`, `SyncConfiguration`, `SyncBlockerSummary`/
  `SyncBlocker`, `RepositorySyncDefinition`, `ResourceSyncAttempt`/
  `RepositorySyncAttempt`, `Revision`, `ResourceSyncEvent`/
  `RepositorySyncEvent`) against `types/types.go` directly -- this service's
  `GetResourceSyncStatus`/`GetRepositorySyncStatus` are the "one wrapper key"
  shape (`LatestSync` wraps a whole `ResourceSyncAttempt`/
  `RepositorySyncAttempt`), so the flat op-level count undercounts
  substantially; `handler_repository_sync.go`'s own doc comments record that a
  prior pass already closed this exact gap
  (`InitialRevision`/`Target`/`TargetRevision` "were previously missing
  entirely from this response shape"), confirmed still correctly wired this
  pass. 0 new bugs. One tagged-`omitempty`-on-a-required-member reviewed and
  ruled out: `repositorySyncDefinitionItem.Parent` (wire member of
  `RepositorySyncDefinition`, required) is tagged `omitempty`, but its only
  value source is `SyncConfiguration.ResourceName`, which
  `handleCreateSyncConfiguration`/`handleUpdateSyncConfiguration` both reject
  as empty via `ErrValidation` before any `SyncConfiguration` (and therefore
  any `RepositorySyncDefinition`) is ever stored -- the real SDK's own
  client-side validator only rejects a nil `ResourceName` pointer, not an
  empty string (`validateOpCreateSyncConfigurationInput`,
  `aws-sdk-go-v2/service/codeconnections@v1.13.4/validators.go:722-748`), so
  this backend is stricter than real AWS and the empty state is genuinely
  unreachable through it -- same "stricter than real AWS, unreachable" class
  `batch` (service) named for `QuotaShareCapacityLimit.CapacityUnit`.
  services/_REQUIRED_OUTPUT_CANDIDATES.md updated.
