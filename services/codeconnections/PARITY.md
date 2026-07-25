---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: codeconnections
sdk_module: aws-sdk-go-v2/service/codeconnections@v1.10.22   # version audited against
last_audit_commit: 749ff939+wt                    # HEAD when this manifest was PREVIOUSLY written; this pass's changes are uncommitted working-tree changes on top (git commands unavailable to this pass)
last_audit_date: 2026-07-23
overall: A            # true-parity pass: closed every gaps/deferred item from the prior audit, plus new wire/error-shape bugs found while field-diffing this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags field on CreateConnectionOutput correct. validProviderTypes has all 6 real ProviderType values. NEW this pass: a HostArn referencing a nonexistent host is now rejected with ResourceNotFoundException (confirmed via botocore codeconnections/2023-12-01/service-2.json: CreateConnection's real error list is exactly [LimitExceededException, ResourceNotFoundException, ResourceUnavailableException] -- ResourceNotFoundException is the correct real type for a missing host, the same type GetHost/DeleteHost use). Previously this was accepted with zero validation. NOTE (not actioned, see gaps): CreateConnection's real error list has NO ResourceAlreadyExistsException, yet this backend rejects duplicate ConnectionName with one -- left as-is (ambiguous signal, see gaps)."}
  GetConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConnections: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateHost: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags field on CreateHostOutput correct. NOTE (not actioned, see gaps): CreateHost's real error list is only [LimitExceededException] -- no ResourceAlreadyExistsException -- yet this backend rejects duplicate Name with one; same ambiguity as CreateConnection above, left as-is."}
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
  CreateSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "duplicate check present (ResourceAlreadyExistsException IS in CreateSyncConfiguration's real error list). FIXED this pass: PullRequestComment (present in this service's pinned SDK types.SyncConfiguration/CreateSyncConfigurationInput but previously entirely unimplemented) is now accepted, stored, and round-tripped through Get/List/persistence."}
  GetSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: PullRequestComment now included in the response."}
  DeleteSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass (leak): deletion now also removes any syncBlockers rows for that resource+syncType. Previously they were left as ghost rows in b.syncBlockers forever, invisible via the API (GetSyncBlockerSummary requires the sync configuration to still exist) but ready to silently resurface if a sync configuration for the same ResourceName+SyncType was ever recreated -- a ghost-data-resurrection bug, not just a memory leak. DeleteSyncConfiguration's real error list has no 'blockers still exist'-style exception, so deletion stays unconditional; only the orphaned children are now cleaned up."}
  UpdateSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: PullRequestComment can now be updated (empty string preserves the existing value, matching the PublishDeploymentStatus/TriggerResourceUpdateOn convention already used by this op)."}
  ListSyncConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: PullRequestComment now included in each list item."}
  GetRepositorySyncStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "epoch-seconds StartedAt/Events[].Time unchanged from 2026-07-13 pass (already correct); RepositorySyncAttempt's real wire shape is only Events/StartedAt/Status (no revision fields), which this response already matched -- unlike ResourceSyncAttempt below."}
  GetResourceSyncStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "MAJOR wire-shape fix this pass: the real ResourceSyncAttempt type (used for both LatestSync and LatestSuccessfulSync) requires Events/InitialRevision/StartedAt/Status/Target/TargetRevision -- InitialRevision/Target/TargetRevision were entirely missing from the response struct (confirmed via aws-sdk-go-v2/service/codeconnections@v1.10.22's own deserializers.go awsAwsjson10_deserializeDocumentResourceSyncAttempt switch, which has explicit cases for all six). Also, the previously-deferred optional DesiredState/LatestSuccessfulSync top-level members are now populated: this backend does not simulate real git-repo content, so InitialRevision/TargetRevision/DesiredState are synthesized identically from the resource's SyncConfiguration (Branch/ConfigFile-as-Directory/OwnerId/ProviderType/RepositoryName), with a deterministic (not random) Sha derived via syntheticRevisionSha (sha1 of stable identity fields, hex-encoded -- SHA is unconstrained beyond min:1/max:255 on the wire, real git shas are simulated shape only). LatestSuccessfulSync is populated identically to LatestSync since every synthesized attempt is immediately SUCCEEDED (no partial/failed-attempt history is modeled)."}
  GetSyncBlockerSummary: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSyncBlocker: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRepositorySyncDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "RESOLVED this pass (was 'deferred: pagination'): confirmed via aws-sdk-go-v2/service/codeconnections@v1.10.22's ListRepositorySyncDefinitionsInput struct AND botocore's paginators-1.json (empty pagination config for this op) that the real INPUT has NO NextToken/MaxResults member at all, even though the real OUTPUT has an optional NextToken -- a real client has no way to ever request a further page for this specific operation. Added a NextToken field to the output wire shape for completeness (real member); it always stays nil/omitted since every definition is returned in one response, which is the only behavior a real client could ever observe anyway."}
  UpdateRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: response no longer carries an invented Tags field."}
families:
  RouteMatcher: {status: ok, note: "unchanged from 2026-07-13 pass; X-Amz-Target prefix and Content-Type verified byte-for-byte, no bug."}
  ConnectionStatus: {status: ok, note: "unchanged; CreateConnection sets AVAILABLE immediately, defensible emulation choice, do not 'fix' to PENDING."}
  HostStatus: {status: ok, note: "unchanged; CreateHost sets AVAILABLE immediately, defensible emulation choice."}
  SyncBlockerPersistence: {status: ok, note: "unchanged store.Table structure from 2026-07-13 pass; this pass's DeleteSyncConfiguration cascade-cleanup (see ops above) reuses the existing syncBlockersByResource index, no schema change, no snapshot version bump needed."}
  ErrValidationWireType: {status: ok, note: "FIXED this pass: ErrValidation's wire type was 'ValidationException', a gopherstack-INVENTED error code -- aws-sdk-go-v2/service/codeconnections@v1.10.22's types/errors.go has NO ValidationException type at all in its full modeled exception set (17 types: AccessDeniedException/ConcurrentModificationException/ConditionalCheckFailedException/ConflictException/InternalServerException/InvalidInputException/LimitExceededException/ResourceAlreadyExistsException/ResourceNotFoundException/ResourceUnavailableException/RetryLatestCommitFailedException/SyncBlockerDoesNotExistException/SyncConfigurationStillExistsException/ThrottlingException/UnsupportedOperationException/UnsupportedProviderTypeException/UpdateOutOfSyncException). Renamed to InvalidInputException, confirmed as the real type for malformed/missing-required-field input by cross-referencing every mutating op's real error list in botocore's codeconnections/2023-12-01/service-2.json (all list InvalidInputException for input validation; none list ValidationException). This affected every required-field check across every handler in this service (Handler.resolveErrorType's single switch case), not just one op."}
gaps:
  - "CreateConnection/CreateHost reject duplicate ConnectionName/Name with ResourceAlreadyExistsException, but neither op's real error list (botocore codeconnections/2023-12-01/service-2.json) includes ResourceAlreadyExistsException at all (CreateConnection: [LimitExceededException, ResourceNotFoundException, ResourceUnavailableException]; CreateHost: [LimitExceededException] only) -- despite the Connection/Host struct doc comments both stating names 'must be unique in an Amazon Web Services account'. This is a genuine ambiguity in AWS's own published model (doc text implies enforcement, the per-op error list contradicts it) that cannot be resolved without live AWS access. Left unchanged this pass: the existing duplicate-rejection behavior has substantial test coverage (TestConnectionNameUniqueness, TestErrAlreadyExistsMapping, TestCreateHostNameUniqueness) and matches the codestarconnections precedent; ripping it out on a single ambiguous signal risked a worse regression than leaving it. (bd: file follow-up requiring live-AWS confirmation)"
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

- **Ambiguity found but NOT actioned**: `CreateConnection`/`CreateHost`
  reject duplicate names with `ResourceAlreadyExistsException`, but neither
  op's real error list includes that exception (see `gaps` above for detail).
  Left unchanged given the conflicting signal from the struct doc comments
  and existing test coverage; recorded as a gap requiring live-AWS
  confirmation rather than silently reclassified either way.
