---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: codeconnections
sdk_module: aws-sdk-go-v2/service/codeconnections@v1.10.22   # version audited against
last_audit_commit: 749ff939                       # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: A            # genuine wire/error-shape/state fixes found, same bug-class family as codestarconnections
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags field on CreateConnectionOutput already correct (unlike codestarconnections, which had lost it in a prior sweep). validProviderTypes was missing AzureDevOps -- real codeconnections ProviderType enum has 6 values (Bitbucket/GitHub/GitHubEnterpriseServer/GitLab/GitLabSelfManaged/AzureDevOps), one more than the older codestarconnections service; fixed."}
  GetConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConnections: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateHost: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags field on CreateHostOutput already correct."}
  GetHost: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHosts: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHost: {wire: ok, errors: ok, state: ok, persist: ok, note: "was missing the in-use check entirely (not a wrong-typed error like codestarconnections had -- no check at all): real DeleteHost's own doc comment states 'Before you delete a host, all connections associated to the host must be deleted.' Added connectionHasReferenceToHostLocked + ConflictException (ErrResourceInUse), same real type used for the analogous codestarconnections fix (no dedicated typed error is documented for this case either)."}
  UpdateHost: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "duplicate-link check was entirely missing (repeated CreateRepositoryLink calls for the same connection+owner+repo silently created N distinct links with fresh UUIDs each time). Added the same-connection+owner+repo duplicate check codestarconnections has, using ResourceAlreadyExistsException (reused the existing ErrAlreadyExists sentinel, which already carried this exact wire type)."}
  GetRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "was missing the in-use check entirely: any sync configurations still referencing the link were silently orphaned on delete. Added syncConfigHasReferenceToLinkLocked + SyncConfigurationStillExistsException (ErrSyncConfigStillExists), matching the real per-type doc text and the analogous codestarconnections fix."}
  ListRepositoryLinks: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "duplicate check was entirely missing: creating a second sync configuration for an existing ResourceName+SyncType silently overwrote the first one in place (via store.Table.Put on the same composite key) instead of being rejected. Added a Has-before-Put duplicate check returning ResourceAlreadyExistsException (ErrAlreadyExists), matching codestarconnections."}
  GetSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSyncConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRepositorySyncStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "StartedAt/Events[].Time fixed from RFC3339 strings to epoch-seconds JSON numbers (awstime.Epoch) -- confirmed via aws-sdk-go-v2's deserializers.go smithytime.ParseEpochSeconds(f64) calls at every StartedAt/Time case in this service's own deserializer, not inferred from codestarconnections."}
  GetResourceSyncStatus: {wire: partial, errors: ok, state: ok, persist: ok, note: "same StartedAt/Time epoch-seconds fix applied; DesiredState/LatestSuccessfulSync (types.Revision) optional output members are not populated -- would require simulating git-repo content/SHAs, out of scope this pass (see gaps, matches codestarconnections' identical deferred item)."}
  GetSyncBlockerSummary: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised no-op: always returned LatestBlockers: [] regardless of state (no SyncBlocker table existed at all). Added a real syncBlockers store.Table + byResource index (dirty-table pattern, same as repositoryLinks/syncConfigurations) so blockers created via the new CreateSyncBlocker internal/test helper are actually readable. CreatedAt/ResolvedAt fixed to epoch-seconds JSON numbers."}
  UpdateSyncBlocker: {wire: ok, errors: ok, state: ok, persist: ok, note: "MAJOR wire-shape bug fixed: response used to send a fabricated 'SyncBlockerSummary' (list) key with an always-empty LatestBlockers; real UpdateSyncBlockerOutput wire key is 'SyncBlocker' (singular object) plus top-level ResourceName/ParentResourceName -- confirmed via aws-sdk-go-v2's api_op_UpdateSyncBlocker.go UpdateSyncBlockerOutput struct, which has no SyncBlockerSummary field at all. Also fixed: any ID (even one that was never created) used to silently 'succeed' with an empty summary; real op documents SyncBlockerDoesNotExistException and does not resolve unknown IDs gracefully -- added ErrSyncBlockerNotFound backed by a real syncBlockers table lookup."}
  ListRepositorySyncDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised no-op (doc comment literally said 'stub sync definitions', body always returned []RepositorySyncDefinition{} regardless of existing sync configs, and silently discarded the syncType parameter via `_ = syncType`). Now derives real definitions from the repository link's SyncConfigurations (Branch/ConfigFile-as-Directory/ResourceName-as-Target+Parent, matching AWS docs' 'for CFN_STACK_SYNC the parent and target resource are the same'), same derivation codestarconnections uses."}
  UpdateRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  RouteMatcher: {status: ok, note: "X-Amz-Target prefix 'CodeConnections_20231201.' and Content-Type 'application/x-amz-json-1.0' both verified byte-for-byte against aws-sdk-go-v2/service/codeconnections@v1.10.22's serializers.go SetHeader calls for every op -- no bug. This is a DIFFERENT prefix/date from codestarconnections' 'CodeStar_connections_20191201.', as expected for the rebranded successor service."}
  ConnectionStatus: {status: ok, note: "CreateConnection sets AVAILABLE immediately (real AWS is PENDING until the console handshake). Verified correct emulated behavior per the same reasoning as codestarconnections: no API in this service can ever transition a connection out of PENDING, so leaving it PENDING would make emulated connections permanently unusable. Do not 'fix' this to PENDING."}
  HostStatus: {status: ok, note: "CreateHost sets AVAILABLE immediately too (real AWS is PENDING until console/VPC setup). Left unchanged -- same rationale as ConnectionStatus above; unlike codestarconnections' HostStatus (which stays PENDING and has an existing test asserting that), codeconnections has an existing test (implicit through GetHost checks) asserting Status: AVAILABLE right after CreateHost, so this was left as pre-existing behavior rather than flipped, since either choice is a defensible emulation trade-off and this service has no test asserting PENDING."}
  SyncBlocker persistence: {status: ok, note: "added syncBlockers as a third 'dirty' store.Table (alongside repositoryLinks/syncConfigurations) with its own region-qualifying field, composite byResource index, and DTO round-trip through persistence.go's ephemeral DTO registry. No snapshot version bump needed: store.Registry.RestoreAll resets any table whose name is absent from older snapshot data to empty rather than erroring, so old snapshots (with no 'syncBlockers' key) restore safely with zero blockers."}
gaps:
  - GetResourceSyncStatus does not populate optional DesiredState/LatestSuccessfulSync (types.Revision) fields -- would require simulating actual git repo content/SHAs, out of scope for this pass (bd: file follow-up, matches the identical codestarconnections gap)
  - CreateConnection with a HostArn referencing a nonexistent host is accepted without validation (real CreateConnection likely documents an unavailable-resource error for a bad host ARN, mirroring codestarconnections' identical deferred gap); left unfixed this pass because an existing test intentionally exercises an arbitrary un-created HostArn and the real trigger condition could not be confirmed without live AWS access (bd: file follow-up)
  - repositoryLinkItem (CreateRepositoryLink/GetRepositoryLink/UpdateRepositoryLink/ListRepositoryLinks response items) includes a "Tags" field, but the real RepositoryLinkInfo wire type (aws-sdk-go-v2/service/codeconnections/types.RepositoryLinkInfo) has no Tags member at all -- tags for a repository link are only ever returned via ListTagsForResource in the real API. This extra field is harmless to a real aws-sdk-go-v2 client (unrecognized JSON members are silently ignored by the deserializer) so it was left in place rather than risk breaking existing tests that assert on it; noted here so a future audit does not need to re-derive this finding (bd: file follow-up, low priority)
  - PullRequestComment (CreateSyncConfiguration/UpdateSyncConfiguration input, SyncConfiguration output) is present in this service's pinned SDK (aws-sdk-go-v2/service/codeconnections@v1.10.22 types.SyncConfiguration/CreateSyncConfigurationInput/UpdateSyncConfigurationInput) but not implemented here -- unlike codestarconnections, where the equivalent field was correctly deferred because it did NOT exist in that older service's pinned SDK version, this is a genuine missing field for codeconnections specifically (bd: file follow-up)
deferred:
  - ListRepositorySyncDefinitions pagination (NextToken) -- the real ListRepositorySyncDefinitionsOutput does support NextToken, but this was left unpaginated to match the established codestarconnections precedent for the same op; revisit both services together if this becomes a problem
leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table/Index behind lockmetrics.RWMutex, snapshotted via persistence.go. The new syncBlockers table follows the exact same lifecycle as the two pre-existing dirty tables (repositoryLinks/syncConfigurations): explicit Reset() call, ephemeral DTO round-trip in Snapshot/Restore, no goroutine involved."}
---

## Notes

- **Protocol**: `application/x-amz-json-1.0` (awsjson1.0), single POST endpoint,
  `X-Amz-Target: CodeConnections_20231201.<Op>` -- verified against every op's
  `SetHeader("X-Amz-Target")` call in aws-sdk-go-v2/service/codeconnections@v1.10.22's
  serializers.go. This is a distinct target prefix/date from the older
  codestarconnections service (`CodeStar_connections_20191201.`), as expected for a
  rebranded successor with its own endpoint.

- **Epoch-seconds timestamps** (same bug class as codestarconnections): every
  timestamp in this service's sync/blocker surface (GetRepositorySyncStatus
  StartedAt, sync event Time, GetResourceSyncStatus StartedAt, GetSyncBlockerSummary/
  UpdateSyncBlocker CreatedAt/ResolvedAt) was wire-serialized as an RFC3339 string via
  `time.Format(time.RFC3339)`. The real awsjson1.0 protocol requires epoch-seconds
  JSON numbers (`smithytime.ParseEpochSeconds(f64)` at every one of these fields in
  aws-sdk-go-v2/service/codeconnections@v1.10.22's own deserializers.go -- confirmed
  directly against this service's SDK, not inferred from codestarconnections). Fixed
  via `pkgs/awstime.Epoch`.

- **UpdateSyncBlocker wire shape** was the most significant bug found this pass, and
  it exists for a deeper reason than codestarconnections' equivalent bug: this
  service had NO SyncBlocker backing store at all. `GetSyncBlockerSummary` always
  returned an empty list and `UpdateSyncBlocker` was a literal stub that echoed back
  whatever ResourceName was in the request without validating the blocker ID existed.
  Added a full `syncBlockers` store.Table (region-qualified "dirty" table, third
  alongside repositoryLinks/syncConfigurations, with its own byResource composite
  index) plus a `CreateSyncBlocker` internal/test helper so the two Get/Update ops now
  read and mutate real state. Additionally fixed the wire shape itself: the response
  body key was `SyncBlockerSummary` (a list wrapper) instead of the real `SyncBlocker`
  (a single object -- the one blocker that was just resolved) plus top-level
  `ResourceName`/`ParentResourceName` -- confirmed via aws-sdk-go-v2's
  `UpdateSyncBlockerOutput` struct, which has no `SyncBlockerSummary` field at all. A
  real aws-sdk-go-v2 client's `out.SyncBlocker` would always have decoded as `nil`
  against the old response. Unknown/wrong-region blocker IDs now correctly return
  `SyncBlockerDoesNotExistException` instead of silently "succeeding".

- **Missing duplicate/in-use checks** (same underlying bug class as
  codestarconnections' error-type fixes, but here the checks were absent entirely
  rather than mistyped):
  - `DeleteHost` had no check for connections still referencing the host being
    deleted, contradicting the real op's own doc comment ("Before you delete a host,
    all connections associated to the host must be deleted."). Fixed with
    `ConflictException` (`ErrResourceInUse`) -- same real type codestarconnections
    uses for the identical case, since no dedicated typed error is documented for
    this specific dependency check.
  - `DeleteRepositoryLink` had no check for sync configurations still referencing the
    link. Fixed with `SyncConfigurationStillExistsException` (`ErrSyncConfigStillExists`).
  - `CreateRepositoryLink` had no duplicate check at all: calling it twice with the
    same ConnectionArn+OwnerId+RepositoryName silently created two distinct
    repository-link resources with different IDs. Fixed with
    `ResourceAlreadyExistsException` (reused the existing `ErrAlreadyExists`
    sentinel, which already carried this exact wire type from the
    connection/host-name duplicate checks).
  - `CreateSyncConfiguration` had no duplicate check either: calling it twice for the
    same ResourceName+SyncType silently overwrote the first configuration in place
    (the composite store key made the second `Put` clobber the first). Fixed the
    same way as `CreateRepositoryLink` above.

- **`ListRepositorySyncDefinitions`** was a disguised no-op: the struct doc comment
  literally said "stub sync definitions" and the handler always returned an empty
  array regardless of state, discarding `syncType` via `_ = syncType`. Now derives
  real definitions from the repository link's `SyncConfiguration`s (same derivation
  codestarconnections uses).

- **`AzureDevOps` provider type**: `validProviderTypes()` was missing it. Confirmed
  via aws-sdk-go-v2/service/codeconnections@v1.10.22's `types/enums.go`, which lists
  6 `ProviderType` values (Bitbucket/GitHub/GitHubEnterpriseServer/GitLab/
  GitLabSelfManaged/AzureDevOps) -- one more than the older codestarconnections
  service, which predates AzureDevOps support and has no such enum value. A previous
  audit pass had even added a test (`TestParity_CreateConnection_AllProviderTypes`)
  that asserted `AzureDevOps` should be *rejected* as invalid; rewritten to assert it
  is accepted, with a genuinely-invalid provider type (`NotARealProvider`) substituted
  as the negative case.

- **`Create*` response `Tags` fields**: `CreateConnectionOutput.Tags` and
  `CreateHostOutput.Tags` were already correct in this service (unlike
  codestarconnections, where a prior sweep had incorrectly removed them). No fix
  needed here; confirmed present in both the current handler code and the real SDK's
  `CreateConnectionOutput`/`CreateHostOutput` structs.
