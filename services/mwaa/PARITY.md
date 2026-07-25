---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: mwaa
sdk_module: aws-sdk-go-v2/service/mwaa@v1.40.1   # version audited against (go.mod pins this)
last_audit_commit: e15f163e+uncommitted   # HEAD was e15f163e when this pass started; set the real hash at commit time
last_audit_date: 2026-07-23
overall: A                # zero gaps carried forward without re-verification; several new
                           # wire-shape bugs found independently and fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "NetworkConfiguration now enforced required with SubnetIds==2/SecurityGroupIds 1-5 (was previously optional/unbounded -- see Notes); EnvironmentClass now includes mw1.micro (was missing, rejecting a real value); WebserverAccessMode now includes PUBLIC_AND_PRIVATE (was missing); WorkerReplacementStrategy is no longer accepted/validated on Create (it was never a member of CreateEnvironmentInput -- see Notes); duplicate-name conflict remains ValidationException/400"}
  GetEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Environment response no longer echoes a fabricated top-level WorkerReplacementStrategy field (real Environment has no such member -- only LastUpdate.WorkerReplacementStrategy is real)"}
  UpdateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "WorkerReplacementStrategy enum values corrected to FORCED/GRACEFUL (was FORCED/TERMINATION_WITH_DRAIN -- the latter is fabricated, the real second value GRACEFUL was previously rejected); WebserverAccessMode now includes PUBLIC_AND_PRIVATE; NetworkConfiguration wire-shape fix from the prior pass (UpdateNetworkConfigurationInput has no SubnetIds) re-verified unchanged"}
  DeleteEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEnvironments: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified MaxResults/NextToken are httpQuery-bound (not body) against serializers.go -- matches"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCliToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "re-verified CliToken/WebServerHostname field names against CreateCliTokenOutput -- matches"}
  CreateWebLoginToken: {wire: partial, errors: ok, state: ok, persist: n/a, note: "AirflowIdentity/IamIdentity response fields still not populated (see gaps); WebToken/WebServerHostname field names re-verified against CreateWebLoginTokenOutput -- matches"}
  InvokeRestApi: {wire: partial, errors: ok, state: ok, persist: n/a, note: "now enforces the environment must be AVAILABLE (ResourceNotFoundException otherwise), matching CreateCliToken/CreateWebLoginToken -- the mock previously let InvokeRestApi succeed against a CREATING/DELETING/etc environment whose Airflow webserver doesn't exist yet; response is still always a synthesized 200 for an AVAILABLE env regardless of Path (see gaps)"}
  PublishMetrics: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  environment_lifecycle: {status: ok, note: "EnvironmentStatus constant fixed: gopherstack used the fabricated string \"UPDATE_ROLLING_BACK\" for a transient rollback state; the real aws-sdk-go-v2/service/mwaa/types.EnvironmentStatus enum value is \"ROLLING_BACK\". Also removed an entirely invented \"ERROR\" status (not in the real 12-value enum, was unused except in one test). CREATING/UPDATING/etc transiently promote to AVAILABLE on next GetEnvironment observation (promoteTransientStatus); this remains a deliberate mock simplification, not a stuck-forever bug"}
  errors: {status: ok, note: "error taxonomy unchanged from the prior pass (7 real exception types, confirmed again against types/errors.go); ErrEnvironmentAlreadyExists's Go error message text no longer contains the literal string \"AlreadyExistsException\" (it was leaking the fabricated exception name into the wire response's \"message\" field even though \"__type\" was already correctly ValidationException)"}
persistence: {status: ok, note: "Snapshot/Restore round-trips verified unaffected by this pass's field/validation changes (NetworkConfiguration, status constants, WorkerReplacementStrategy removal all covered by existing + new tests); no persistence.go edits were needed since none of the fixed fields had bespoke DTO mapping"}
gaps:
  - CreateWebLoginToken does not populate AirflowIdentity/IamIdentity (real AWS returns the calling IAM identity's username/ARN). No caller-identity extraction helper exists in pkgs/ or is derivable within services/mwaa alone (STS's assumed-role/session tracking lives in services/sts and is out of this audit's edit scope); populating these with fabricated values would violate the no-fabricated-data rule, so they are left absent rather than invented. Re-confirmed this pass: grepped pkgs/ for CallerIdentity/AssumedRoleUser/GetCallerIdentity/ExtractCallerIdentity helpers -- none exist. Candidate follow-up: a shared pkgs/ helper that derives caller identity from the SigV4 Authorization header, usable by any service that needs it.
  - InvokeRestApi always synthesizes a 200 success with an empty RestApiResponse for any AVAILABLE environment, regardless of the caller-supplied Path/Method; it never returns RestApiClientException/RestApiServerException (which real AWS returns when the underlying Airflow REST call itself 4xx/5xx's). Faithfully emulating arbitrary Airflow REST API behavior per-path is out of scope for this pass. This pass DID fix the adjacent, previously-missing AVAILABLE-status precondition (see ops.InvokeRestApi note) -- the always-200-for-available-envs limitation is the only remaining piece.
  - EnvironmentClass mw1.micro is now accepted (fixed this pass), but its AWS-documented special-case default/bounds for MaxWebservers/MinWebservers ("Defaults to 2 for all environment sizes except mw1.micro, which defaults to 1") are NOT modeled -- gopherstack applies the same default (2) and bounds (1-5) to mw1.micro as every other class. Confirmed via aws-sdk-go-v2/service/mwaa@v1.40.1/types/types.go's MaxWebservers/MinWebservers doc comments. Narrow, newly-discovered gap; not fixed this pass to keep the NetworkConfiguration/WorkerReplacementStrategy/status fixes (which have real client-facing impact) as the priority.
  - MethodNotAllowedException (405) is used for HTTP-verb mismatches on matched MWAA path prefixes (e.g. GET /clitoken/{name}). This exception name is not part of the real MWAA API model, but the code path is unreachable by any conformant aws-sdk-go-v2 client (which always sends the correct verb per operation) -- and the same pattern is used consistently across 15+ other gopherstack services (apigatewayv2, pinpoint, lambda, opensearch, etc.), so it was left as-is rather than special-cased here.
deferred:
  - Chaos/fault-injection interaction with this pass's status-constant and NetworkConfiguration-validation changes (not re-audited; ChaosOperations() surface is GetSupportedOperations() minus nothing new -- it shrank by one entry this pass since GetMetrics was removed, see Notes).
leaks: {status: clean, note: "no goroutines/janitors in this service; existing leak_test.go/isolation_test.go untouched and still green"}
---

## Notes

- **Protocol**: restjson1. Route prefixes unchanged from the prior pass, re-verified against
  aws-sdk-go-v2/service/mwaa@v1.40.1 serializers.go for every op: `/environments`
  (POST-less; GET=List), `/environments/{Name}` (GET/PUT/DELETE/PATCH =
  Get/Create/Delete/Update), `/clitoken/{Name}` (POST), `/webtoken/{Name}` (POST -- the
  real wire path is `/webtoken/`, NOT `/weblogintoken/` despite the operation being named
  CreateWebLoginToken), `/restapi/{Name}` (POST), `/tags/{ResourceArn}` (GET/POST/DELETE
  = List/Tag/Untag), `/metrics/environments/{EnvironmentName}` (POST=PublishMetrics; GET
  is intentionally unrouted, see the GetMetrics note below).

- **GetMetrics deleted from the wire surface** (was `GET /metrics/environments/{Name}`,
  advertised in `GetSupportedOperations()`/`ChaosOperations()` and dispatched by
  `handler.go`). Confirmed independently against
  aws-sdk-go-v2/service/mwaa@v1.40.1's exported `*mwaa.Client` methods: there is no
  `GetMetrics` method on the real SDK client at all -- only `PublishMetrics` exists on this
  path (documented "internal use only", used by the Airflow environment itself to push
  metrics to CloudWatch). The prior audit pass flagged this as an invented
  test-observability extension but left it wired up as if it were a real op, reasoning it
  was "harmless" since no real client would call it; that reasoning missed that
  `GetSupportedOperations()` feeds `ChaosOperations()` (presenting a fake op as
  fault-injectable) and is exactly the kind of drift `sdkcheck.CheckCompleteness` does NOT
  catch (it only verifies every *real* SDK method is accounted for, not that
  `GetSupportedOperations()` contains no extras). Fixed by removing the GET case from
  `extractMetricsOperation`/`dispatchMetrics` (GET now correctly falls through to
  `MethodNotAllowedException`/405, consistent with every other unsupported-verb-on-matched-path
  case in this handler) and deleting `handleGetMetrics`. The backend's
  `InMemoryBackend.GetMetrics` Go method is kept as internal, non-wire-exposed test
  introspection (tests assert `PublishMetrics`'s side effects by calling
  `h.Backend.GetMetrics(...)` directly, the same pattern as the `EnvironmentCount`/
  `MetricsCount` helpers in export_test.go) -- it is no longer presented as an AWS
  operation anywhere.

- **WorkerReplacementStrategy was fabricated on CreateEnvironment and on the Environment
  response shape.** Confirmed via aws-sdk-go-v2/service/mwaa@v1.40.1/api_op_CreateEnvironment.go's
  `CreateEnvironmentInput` struct (no `WorkerReplacementStrategy` member at all) and
  types/types.go's `Environment` struct (also no top-level `WorkerReplacementStrategy`
  member -- it exists ONLY on the nested `LastUpdate` struct, which real AWS uses to record
  just the most recent update call's setting, not a persistent environment-level value).
  gopherstack previously (a) accepted and validated `WorkerReplacementStrategy` in the
  Create request body, (b) copied it onto a fabricated top-level `Environment.WorkerReplacementStrategy`
  field on both Create and Update, and (c) emitted that fabricated field in every
  CreateEnvironment/GetEnvironment/UpdateEnvironment JSON response. Fixed by removing the
  field from `createEnvironmentRequest` and `Environment` entirely; it remains correctly
  present on `updateEnvironmentRequest` and `LastUpdate` (the only two real members).

- **WorkerReplacementStrategy's enum values were also wrong.** gopherstack accepted
  `FORCED`/`TERMINATION_WITH_DRAIN` and rejected `GRACEFUL`. The real
  `aws-sdk-go-v2/service/mwaa/types.WorkerReplacementStrategy` enum
  (types/enums.go) has exactly two values: `FORCED` and `GRACEFUL`.
  `TERMINATION_WITH_DRAIN` does not exist in the real API at all -- this was a double bug
  (accepting a fake value AND rejecting a real one). Fixed the constant and all test
  fixtures.

- **WebserverAccessMode was missing a real third value.** gopherstack's validator only
  accepted `PUBLIC_ONLY`/`PRIVATE_ONLY`. The real
  `aws-sdk-go-v2/service/mwaa/types.WebserverAccessMode` enum also has
  `PUBLIC_AND_PRIVATE`. This was a real functional bug (rejecting valid input, not just a
  permissive superset), fixed via a new shared `validateWebserverAccessMode` helper used by
  both Create and Update.

- **EnvironmentClass was missing `mw1.micro`.** gopherstack's `validEnvironmentClasses()`
  had small/medium/large/xlarge/2xlarge but not `mw1.micro`, which IS a documented valid
  value (aws-sdk-go-v2/service/mwaa@v1.40.1/types/types.go's EnvironmentClass field
  comment: "Valid values: mw1.micro, mw1.small, mw1.medium, mw1.large, mw1.xlarge, and
  mw1.2xlarge"). Fixed by adding it; see gaps for the still-unmodeled mw1.micro-specific
  webserver-count default.

- **EnvironmentStatus had a wrong value and an invented one.** gopherstack used
  `"UPDATE_ROLLING_BACK"` for the transient rollback status; the real
  `aws-sdk-go-v2/service/mwaa/types.EnvironmentStatus` enum value (types/enums.go) is
  `"ROLLING_BACK"` (no `UPDATE_` prefix). Also removed an `"ERROR"` status constant that
  does not exist anywhere in the real 12-value enum (`CREATING`, `CREATE_FAILED`,
  `AVAILABLE`, `UPDATING`, `DELETING`, `DELETED`, `UNAVAILABLE`, `UPDATE_FAILED`,
  `ROLLING_BACK`, `CREATING_SNAPSHOT`, `PENDING`, `MAINTENANCE`) and was unused except in
  one test's terminal-status list. `MAINTENANCE` remains unmodeled (gopherstack's mock
  never produces it, since there's no maintenance-window simulation) -- this is a
  pre-existing, low-risk simplification, not newly introduced.

- **NetworkConfiguration is now enforced required with real bounds on Create.**
  Confirmed via aws-sdk-go-v2/service/mwaa@v1.40.1/validators.go's generated
  `validateOpCreateEnvironmentInput` (client-side rejects a nil `NetworkConfiguration`
  before the request is even sent -- so real conformant clients can never omit it) and the
  live API docs for SubnetIds ("Fixed number: 2") / SecurityGroupIds ("1-5"), which have NO
  client-side validator (`validateNetworkConfigurationInput` does not exist in
  validators.go, unlike `validateUpdateNetworkConfigurationInput`) and so ARE genuinely
  reachable with a real client sending e.g. 1 subnet. The prior pass identified this gap
  but deferred it citing ~10+ tests relying on the lenient behavior; this pass did the
  test sweep (added a shared `testNetworkConfig()`/`newCreateReq()`/`seedEnv()`/
  `newIsoCreateReq()` fixture update covering ~80 call sites across both Go struct
  literals and HTTP JSON bodies) and landed the fix: `validateNetworkConfigCreate`
  requires non-nil, `len(SubnetIds) == 2`, `1 <= len(SecurityGroupIds) <= 5`.

- **InvokeRestApi now requires the environment to be AVAILABLE**, matching
  CreateCliToken/CreateWebLoginToken (the other two operations that reach into the
  environment's Airflow webserver process, which doesn't exist yet during
  CREATING/UPDATING/etc). Previously `InvokeRestAPI` only checked the environment existed,
  not its status, so it incorrectly succeeded against environments whose webserver isn't up.

- **The prior pass's standout bug** (UpdateEnvironment's NetworkConfiguration wire shape;
  see git history / prior manifest version) was re-verified unchanged and still correct
  this pass.

- **Error taxonomy**: unchanged from the prior pass -- MWAA's API model has exactly 7
  exception types (`AccessDeniedException`, `InternalServerException`,
  `ResourceNotFoundException`, `RestApiClientException`, `RestApiServerException`,
  `ServiceUnavailableException`, `ValidationException`), re-confirmed against
  `aws-sdk-go-v2/service/mwaa@v1.40.1/types/errors.go`. This pass additionally scrubbed
  `ErrEnvironmentAlreadyExists`'s Go error message text (previously
  `"AlreadyExistsException: environment already exists"`), which leaked the fabricated
  exception name into the wire response's `"message"` field even though `"__type"` was
  already correctly `ValidationException` -- now reads
  `"ValidationException: environment already exists"`.
