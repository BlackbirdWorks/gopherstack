---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: codestarconnections
sdk_module: aws-sdk-go-v2/service/codestarconnections@v1.35.15   # version audited against
last_audit_commit: 3f6a5e93                       # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: A            # genuine wire/error-shape fixes found across most-used ops
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "restored Tags field in response (CreateConnectionOutput.Tags is real, distinct from Connection type which has no Tags) — a prior sweep had incorrectly removed it by conflating the two shapes"}
  GetConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConnections: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateHost: {wire: ok, errors: ok, state: ok, persist: ok, note: "restored Tags field in response, same rationale as CreateConnection"}
  GetHost: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHosts: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHost: {wire: ok, errors: ok, state: ok, persist: ok, note: "in-use error type changed from fabricated ResourceInUseException (does not exist in the real service) to ConflictException (real type, best documented fit; DeleteHost itself documents no specific typed error for this case)"}
  UpdateHost: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "duplicate-link error type fixed: was InvalidInputException, real op registers a dedicated ResourceAlreadyExistsException"}
  GetRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok, note: "in-use error type fixed: was fabricated ResourceInUseException, real op documents SyncConfigurationStillExistsException for this exact dependency check"}
  ListRepositoryLinks: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "duplicate-config error type fixed: was InvalidInputException, real op registers ResourceAlreadyExistsException"}
  GetSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSyncConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSyncConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRepositorySyncStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "StartedAt/Events[].Time fixed from RFC3339 strings to epoch-seconds JSON numbers (awstime.Epoch) — this is the awsjson1.0 protocol's unixTimestamp wire format, confirmed via aws-sdk-go-v2's ParseEpochSeconds(json.Number) deserializer"}
  GetResourceSyncStatus: {wire: partial, errors: ok, state: ok, persist: ok, note: "same StartedAt/Time epoch-seconds fix applied; DesiredState/LatestSuccessfulSync fields (real optional output members requiring simulated git-repo Revision state) are not populated — see gaps"}
  GetSyncBlockerSummary: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreatedAt/ResolvedAt fixed from RFC3339 strings to epoch-seconds JSON numbers"}
  UpdateSyncBlocker: {wire: ok, errors: ok, state: ok, persist: ok, note: "MAJOR wire-shape bug fixed: response used to send a fabricated 'SyncBlockerSummary' (list) key; real CreateSyncBlockerOutput/UpdateSyncBlockerOutput wire key is 'SyncBlocker' (singular object) — confirmed via aws-sdk-go-v2 deserializer, which never even looks at a SyncBlockerSummary key for this op. Also fixed: unknown/wrong-region blocker IDs used to silently return 200 with an empty list; real op documents SyncBlockerDoesNotExistException and does not resolve unknown IDs gracefully. CreatedAt/ResolvedAt also switched to epoch-seconds."}
  ListRepositorySyncDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised no-op (struct literally doc-commented 'is a stub definition', body always returned []RepositorySyncDefinition{} regardless of existing sync configs). Now derives real definitions from the repository link's SyncConfigurations (Branch/ConfigFile-as-Directory/ResourceName-as-Target+Parent, matching AWS docs' 'for CFN_STACK_SYNC the parent and target resource are the same')."}
  UpdateRepositoryLink: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  RouteMatcher: {status: ok, note: "X-Amz-Target prefix 'CodeStar_connections_20191201.' and Content-Type 'application/x-amz-json-1.0' both verified byte-for-byte against aws-sdk-go-v2's serializers.go SetHeader calls for every op — no bug (task brief's suggested 'com.amazonaws.codestar.connections.' long-prefix form does not exist in the real SDK)."}
  ConnectionStatus: {status: ok, note: "CreateConnection sets AVAILABLE immediately (not AWS's real PENDING-until-console-handshake). Verified this is the correct emulated behavior, not a bug: LocalStack's own CodeConnections docs example shows ConnectionStatus:AVAILABLE immediately after CreateConnection, and there is no API in this service that could ever transition a connection out of PENDING (unlike ACM's PENDING_VALIDATION, which has an autoValidate timer) — leaving it PENDING would make connections permanently unusable in the emulator. Do not 'fix' this to PENDING."}
  HostStatus: {status: ok, note: "CreateHost sets PENDING and stays there (no auto-transition, unlike ConnectionStatus) — matches existing test TestAudit2_Host_StatusAvailableOnCreate and is consistent with AWS's real HOST behavior (host setup genuinely requires console/agent installation); left unchanged."}
gaps:
  - GetResourceSyncStatus does not populate optional DesiredState/LatestSuccessfulSync (types.Revision) fields — would require simulating actual git repo content/SHAs, out of scope for this pass (bd: file follow-up)
  - CreateConnection with a HostArn referencing a nonexistent host is accepted without validation (real CreateConnection documents ResourceUnavailableException for a bad host ARN); left unfixed this pass because an existing test (TestAudit2_Connection_HostArnIncludedWhenSet) intentionally exercises an arbitrary un-created HostArn and the real trigger condition (existence vs. malformed-ARN-format) could not be confirmed without live AWS access (bd: file follow-up)
  - CreateConnection/CreateHost duplicate-name rejection (ErrAlreadyExists, InvalidInputException) has no direct confirmation in the real per-op error lists (which show only LimitExceededException/ResourceNotFoundException/ResourceUnavailableException for CreateConnection and only LimitExceededException for CreateHost) — left as-is since the Connection type doc explicitly states "Connection names must be unique in an Amazon Web Services account", and InvalidInputException is the most plausible untyped/common-error bucket; not changed for lack of stronger evidence
deferred:
  - PullRequestComment field (CreateSyncConfiguration/UpdateSyncConfiguration/SyncConfiguration) — present in current AWS API docs but NOT in the pinned aws-sdk-go-v2@v1.35.15 SDK's types/serializers/deserializers; correctly omitted to match the SDK version actually vendored by this repo
leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table/Index behind lockmetrics.RWMutex, snapshotted via persistence.go"}
---

## Notes

- Protocol is `application/x-amz-json-1.0` (awsjson1.0), single POST endpoint,
  `X-Amz-Target: CodeStar_connections_20191201.<Op>` — verified against every
  op's `SetHeader("X-Amz-Target")` call in aws-sdk-go-v2's generated
  serializers.go. No long `com.amazonaws.codestar.connections.` prefix exists
  in the real wire protocol for this service; do not "fix" the route matcher
  to add one.

- **Epoch-seconds timestamps** (bug class from parity-principles.md #2):
  every timestamp in this service's sync/blocker surface (GetRepositorySyncStatus
  StartedAt, sync event Time, GetResourceSyncStatus StartedAt, GetSyncBlockerSummary/
  UpdateSyncBlocker CreatedAt/ResolvedAt) was wire-serialized as an RFC3339
  string. The real awsjson1.0 protocol requires epoch-seconds JSON numbers
  (`smithytime.ParseEpochSeconds(json.Number)` in the SDK deserializer — it
  explicitly rejects strings with "expected Timestamp to be a JSON Number, got
  string instead"). Fixed via `pkgs/awstime.Epoch`. A previous audit had even
  added a test (`TestAudit2_GetRepositorySyncStatus_StartedAtFormat`) that
  *asserted* the wrong RFC3339 behavior — rewritten to assert a JSON number.

- **UpdateSyncBlocker wire shape** was the most significant bug found this
  pass: the response body key was `SyncBlockerSummary` (a list wrapper)
  instead of the real `SyncBlocker` (a single object — the one blocker that
  was just updated). A real aws-sdk-go-v2 client's `out.SyncBlocker` would
  always have decoded as `nil` against gopherstack's old response, silently
  breaking any caller reading the resolved blocker back from the API
  response (GetSyncBlockerSummary was unaffected — its shape was already
  correct).

- **Error-type fidelity**: `ResourceInUseException`, previously used for both
  "host has active connections" (DeleteHost) and "repository link has active
  sync configs" (DeleteRepositoryLink), does not exist anywhere in
  codestarconnections' real error catalog (verified against
  `aws-sdk-go-v2/service/codestarconnections/types/errors.go`'s exhaustive
  type list). Split into two real, evidence-backed types:
  `SyncConfigurationStillExistsException` for the repository-link case
  (explicitly documented for DeleteRepositoryLink) and `ConflictException`
  for the host case (no dedicated documented type exists for DeleteHost's
  dependency check, so the closest real, generic type was used instead of a
  fabricated name). Similarly, `CreateRepositoryLink`/`CreateSyncConfiguration`
  duplicate-resource errors were `InvalidInputException`; the real per-op
  error deserializers register a dedicated `ResourceAlreadyExistsException`
  for both, so a new `ErrResourceAlreadyExists` sentinel now carries that
  distinct type (kept separate from the connection/host-name `ErrAlreadyExists`
  sentinel, whose ops document no such dedicated type).

- **CreateConnection/CreateHost `Tags` field**: restored after determining a
  prior sweep (commit b1146508, "fix... tag response shape") had removed it
  by incorrectly generalizing from `GetConnection`/`GetHost`'s `Connection`/
  `Host` types (which genuinely have no `Tags` field in the real SDK) to
  `CreateConnectionOutput`/`CreateHostOutput` (which are distinct generated
  structs that DO have their own `Tags []types.Tag` field, confirmed via the
  compiled SDK's own deserializer code, not just docs). This is the one case
  this pass reversed a previous audit's explicit decision — see the updated
  test comments in `handler_audit2_test.go` for the full reasoning trail so a
  future audit does not flip it back without re-checking the SDK source.

- **`ListRepositorySyncDefinitions`** was a disguised no-op: the struct comment
  literally said "is a stub definition" and the handler always returned an
  empty array regardless of state, ignoring `syncType` entirely (`_ =
  syncType`). Now derives real definitions from the repository link's
  `SyncConfiguration`s.
