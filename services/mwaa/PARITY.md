---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: mwaa
sdk_module: aws-sdk-go-v2/service/mwaa@v1.43.4   # version audited against (go.mod pins this)
last_audit_commit: e15f163e+uncommitted   # HEAD was e15f163e when this pass started; set the real hash at commit time
last_audit_date: 2026-07-23
overall: A                # zero gaps carried forward without re-verification; several new
                           # wire-shape bugs found independently and fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "NetworkConfiguration now enforced required with SubnetIds==2/SecurityGroupIds 1-5 (was previously optional/unbounded -- see Notes); EnvironmentClass now includes mw1.micro (was missing, rejecting a real value); WebserverAccessMode now includes PUBLIC_AND_PRIVATE (was missing); WorkerReplacementStrategy is no longer accepted/validated on Create (it was never a member of CreateEnvironmentInput -- see Notes); duplicate-name conflict remains ValidationException/400; mw1.micro now defaults MaxWebservers/MinWebservers to 1 (was incorrectly defaulting to 2 like every other class) and rejects explicit values other than 1 (was incorrectly accepting the full 1-5 range) -- see Notes"}
  GetEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Environment response no longer echoes a fabricated top-level WorkerReplacementStrategy field (real Environment has no such member -- only LastUpdate.WorkerReplacementStrategy is real)"}
  UpdateEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "WorkerReplacementStrategy enum values corrected to FORCED/GRACEFUL (was FORCED/TERMINATION_WITH_DRAIN -- the latter is fabricated, the real second value GRACEFUL was previously rejected); WebserverAccessMode now includes PUBLIC_AND_PRIVATE; NetworkConfiguration wire-shape fix from the prior pass (UpdateNetworkConfigurationInput has no SubnetIds) re-verified unchanged; mw1.micro webserver-count restriction now enforced using the effective (request-or-persisted) EnvironmentClass; a rejected update (e.g. MinWorkers>MaxWorkers) no longer silently mutates the stored environment's other fields first -- see Notes"}
  DeleteEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEnvironments: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified MaxResults/NextToken are httpQuery-bound (not body) against serializers.go -- matches"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCliToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "re-verified CliToken/WebServerHostname field names against CreateCliTokenOutput -- matches"}
  CreateWebLoginToken: {wire: partial, errors: ok, state: ok, persist: n/a, note: "AirflowIdentity/IamIdentity response fields still not populated (see gaps -- re-investigated this pass, confirmed genuinely not derivable, not just an unwired accessor)"}
  InvokeRestApi: {wire: partial, errors: ok, state: ok, persist: n/a, note: "now enforces the environment must be AVAILABLE (ResourceNotFoundException otherwise), matching CreateCliToken/CreateWebLoginToken -- the mock previously let InvokeRestApi succeed against a CREATING/DELETING/etc environment whose Airflow webserver doesn't exist yet; response is still always a synthesized 200 for an AVAILABLE env regardless of Path (see gaps -- re-investigated this pass with botocore's service-2.json, see gap note for why this is more nuanced than a simple 404/405 miss)"}
  PublishMetrics: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  environment_lifecycle: {status: ok, note: "EnvironmentStatus constant fixed: gopherstack used the fabricated string \"UPDATE_ROLLING_BACK\" for a transient rollback state; the real aws-sdk-go-v2/service/mwaa/types.EnvironmentStatus enum value is \"ROLLING_BACK\". Also removed an entirely invented \"ERROR\" status (not in the real 12-value enum, was unused except in one test). CREATING/UPDATING/etc transiently promote to AVAILABLE on next GetEnvironment observation (promoteTransientStatus); this remains a deliberate mock simplification, not a stuck-forever bug"}
  errors: {status: ok, note: "error taxonomy unchanged from the prior pass (7 real exception types, confirmed again against types/errors.go); ErrEnvironmentAlreadyExists's Go error message text no longer contains the literal string \"AlreadyExistsException\" (it was leaking the fabricated exception name into the wire response's \"message\" field even though \"__type\" was already correctly ValidationException)"}
persistence: {status: ok, note: "Snapshot/Restore round-trips verified unaffected by this pass's field/validation changes (NetworkConfiguration, status constants, WorkerReplacementStrategy removal all covered by existing + new tests); no persistence.go edits were needed since none of the fixed fields had bespoke DTO mapping"}
gaps:
  - CreateWebLoginToken does not populate AirflowIdentity/IamIdentity (real AWS returns the calling IAM identity's username/ARN). Re-investigated end-to-end this pass rather than assuming the blocker (the codedeploy/mgn `siblingServices`/`GetXHandler()` pattern exists and *CLI already exposes GetSTSHandler()/GetIAMHandler() at cli.go:1043-1046). The actual missing piece is upstream of any mwaa-specific wiring: gopherstack has NO per-request caller-identity plumbing anywhere in the codebase. Confirmed by grepping every `ctxval.NewKey` call site (repo-wide): only two context keys exist at all, `pkgs/awsmeta` (Account/Region/Partition/RequestID -- no principal/ARN field) and `pkgs/logger`. `pkgs/httputils/sigv4.go`'s SigV4Validator parses the Authorization header's Credential (which contains the access-key-id) purely to verify the signature and explicitly discards it afterward -- its own doc comment states "the access-key-id in the request is informational only -- gopherstack is a single-tenant simulator". So even though `services/sts.GetCallerIdentity(accessKeyID, sessionToken)` exists and could resolve an access-key-id to an ARN, no access-key-id is ever threaded from the request into a handler's context anywhere in gopherstack today. Making IamIdentity real requires NEW cross-cutting plumbing (a context key carrying the parsed Credential access-key-id, populated by an Echo middleware, consumed via a services/mwaa/cross_service.go siblingServices accessor into STS) -- not a mwaa-local fix. AirflowIdentity is a second, independent gap on top of that: it requires mapping the resolved IAM principal to an Airflow RBAC username, which AWS derives from environment-specific IdP/role-mapping configuration that has no field anywhere in mwaa's Environment model (verified: models.go has no such member). Populating either field with a fabricated value would violate the no-fabricated-data rule, so both are left absent.
  - InvokeRestApi always synthesizes a 200 success with an empty RestApiResponse for any AVAILABLE environment, regardless of the caller-supplied Path/Method. Re-investigated this pass against botocore's mwaa/2020-07-01/service-2.json (not just the Go SDK): the operation's HTTP binding is `"responseCode": 200` for the success shape, so the AWS-transport-level 200 gopherstack already returns is not itself wrong -- real MWAA's InvokeRestApiOutput/RestApiClientException/RestApiServerException shapes ALL carry the same `RestApiStatusCode`/`RestApiResponse` pair (types/errors.go:94-150), meaning the *actual* downstream Airflow HTTP status (e.g. 404 for an unknown path, 405 for a wrong method) is meant to be surfaced as data inside that pair, not necessarily as a distinct SDK-visible exception -- and the SDK model does not document which of {success w/ non-2xx RestApiStatusCode, RestApiClientException, RestApiServerException} a given downstream failure maps to. Enumerating the real Apache Airflow REST API's actual path/method surface (which varies by AirflowVersion: /api/v1 for Airflow 2.x, /api/v2 for Airflow 3.x per the AWS user guide) to decide per-request which of those three shapes applies would mean inventing a route table gopherstack cannot verify -- exactly the fabrication class today's campaign already reverted once (an invented xray formula). Declined to implement path/method-based rejection for this reason; RestAPIStatusCode remains a fixed, documented mock simplification (see Notes) rather than a per-path guess.
  - MethodNotAllowedException (405) is used for HTTP-verb mismatches on matched MWAA path prefixes (e.g. GET /clitoken/{name}). This exception name is not part of the real MWAA API model, but the code path is unreachable by any conformant aws-sdk-go-v2 client (which always sends the correct verb per operation) -- and the same pattern is used consistently across 15+ other gopherstack services (apigatewayv2, pinpoint, lambda, opensearch, etc.), so it was left as-is rather than special-cased here.
deferred:
  - Chaos/fault-injection interaction with this pass's status-constant and NetworkConfiguration-validation changes (not re-audited; ChaosOperations() surface is GetSupportedOperations() minus nothing new -- it shrank by one entry this pass since GetMetrics was removed, see Notes).
leaks: {status: clean, note: "no goroutines/janitors in this service; existing leak_test.go/isolation_test.go untouched and still green"}
---

## Notes

**2026-08-15 (gopherstack-3gbe):** investigated whether MWAA shares Omics'
(gopherstack-keee) client-side host-prefix-rewrite reachability gap. It
does, and covers nearly this service's entire real surface: **12 of MWAA's
operations** carry a `req.URL.Host = "..." + req.URL.Host` rewrite from a
per-operation Smithy Finalize middleware, confirmed against the pinned
`mwaa@v1.43.4` module -- `api.` (8: CreateEnvironment
`api_op_CreateEnvironment.go:340`, GetEnvironment `:130`, DeleteEnvironment
`:126`, UpdateEnvironment `:312`, ListEnvironments `:219`, TagResource
`:135`, UntagResource `:133`, ListTagsForResource `:134`), `env.` (3:
CreateCliToken `api_op_CreateCliToken.go:134`, CreateWebLoginToken
`api_op_CreateWebLoginToken.go:142`, InvokeRestApi
`api_op_InvokeRestApi.go:159`), `ops.` (1: PublishMetrics
`api_op_PublishMetrics.go:140`) -- exactly matching gopherstack-3gbe's
filing (three literal prefixes using `.`, not `-`).

No routing/auth code needed changing. `Handler.RouteMatcher` (`handler.go:82`)
matches on `URL.Path` alone, gated on the SigV4 service name `"airflow"`
(already listed as SigV4-scoped and confirmed clean in
`services/_ROUTE_COLLISIONS.md`'s "hand-read this pass" section), and every
op already has a distinct path/method pair. The reachability gap is a pure
client-side DNS/dial failure, same as Omics -- confirmed live via
`host_prefix_reachability_test.go`'s before-fix test:
`dial tcp: lookup api.127.0.0.1 on 127.0.0.53:53: no such host`.

Before this pass, mwaa had **no real-SDK-client test at all** -- every
existing test drives the handler directly over a raw `httptest.Recorder`,
so the real-client reachability of this operation family had never been
exercised in either direction. Added
`host_prefix_reachability_test.go` following
`services/omics/host_prefix_reachability_test.go`'s before/after pattern
(real unmodified client fails to dial; a redial-to-the-real-listener
transport leaves the SDK's real, un-disabled rewrite intact on the wire and
the op succeeds with correctly decoded values), one representative op per
prefix. Gates green: build, vet, race, `go fix -diff` (no diff),
golangci-lint (0 findings; the one staticcheck SA1019 on the deliberately
deprecated-but-real `PublishMetrics` call is `//nolint:staticcheck`'d, same
convention as `services/directconnect/sdk_roundtrip_test.go`).

- **Protocol**: restjson1. Route prefixes unchanged from the prior pass, re-verified against
  aws-sdk-go-v2/service/mwaa@v1.43.4 serializers.go for every op: `/environments`
  (POST-less; GET=List), `/environments/{Name}` (GET/PUT/DELETE/PATCH =
  Get/Create/Delete/Update), `/clitoken/{Name}` (POST), `/webtoken/{Name}` (POST -- the
  real wire path is `/webtoken/`, NOT `/weblogintoken/` despite the operation being named
  CreateWebLoginToken), `/restapi/{Name}` (POST), `/tags/{ResourceArn}` (GET/POST/DELETE
  = List/Tag/Untag), `/metrics/environments/{EnvironmentName}` (POST=PublishMetrics; GET
  is intentionally unrouted, see the GetMetrics note below).

- **GetMetrics deleted from the wire surface** (was `GET /metrics/environments/{Name}`,
  advertised in `GetSupportedOperations()`/`ChaosOperations()` and dispatched by
  `handler.go`). Confirmed independently against
  aws-sdk-go-v2/service/mwaa@v1.43.4's exported `*mwaa.Client` methods: there is no
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
  response shape.** Confirmed via aws-sdk-go-v2/service/mwaa@v1.43.4/api_op_CreateEnvironment.go's
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
  value (aws-sdk-go-v2/service/mwaa@v1.43.4/types/types.go's EnvironmentClass field
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
  Confirmed via aws-sdk-go-v2/service/mwaa@v1.43.4/validators.go's generated
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
  `aws-sdk-go-v2/service/mwaa@v1.43.4/types/errors.go`. This pass additionally scrubbed
  `ErrEnvironmentAlreadyExists`'s Go error message text (previously
  `"AlreadyExistsException: environment already exists"`), which leaked the fabricated
  exception name into the wire response's `"message"` field even though `"__type"` was
  already correctly `ValidationException` -- now reads
  `"ValidationException: environment already exists"`.

- **mw1.micro's special-case MaxWebservers/MinWebservers default/bounds are now modeled.**
  `aws-sdk-go-v2/service/mwaa@v1.43.4/types/types.go`'s MaxWebservers/MinWebservers doc
  comments (identical text also in botocore's `mwaa/2020-07-01/service-2.json`, confirmed
  independently): "Valid values: For environments larger than mw1.micro, accepts values
  from 2 to 5. Defaults to 2 for all environment sizes except mw1.micro, which defaults to
  1." gopherstack previously applied the same default (2) and bounds (1-5) to every
  EnvironmentClass including mw1.micro -- more permissive than real AWS, which (per the
  quoted text explicitly scoping the 2-5 range to "environments larger than mw1.micro")
  only accepts 1 for that class. Fixed via a new `validateWebserversForClass` helper used
  by both CreateEnvironment (using the request's EnvironmentClass) and UpdateEnvironment
  (using the effective EnvironmentClass: the request's if it also changes class, else the
  persisted environment's). CreateEnvironment's default resolution now also picks 1 instead
  of 2 when EnvironmentClass resolves to mw1.micro and MaxWebservers/MinWebservers are
  unset. This does NOT model per-Environment reconfiguration edge cases beyond the
  documented default/range (e.g. AWS's exact behavior when downgrading EnvironmentClass to
  mw1.micro via Update while implicitly leaving a previously-set 2-5 value in place is not
  independently confirmed beyond "the effective class governs the check").

- **UpdateEnvironment no longer silently persists a rejected request's other fields.**
  `env` returned by the backend's `store.Table[Environment].Get` is the live stored
  pointer, not a copy. `UpdateEnvironment` previously called `applyUpdateScalars`/
  `applyUpdateS3Paths` (which mutate `env` in place) BEFORE checking
  `MinWorkers <= MaxWorkers`; a request that failed that check (or, before this pass, the
  mw1.micro webserver check) had already had its other fields (DagS3Path,
  ExecutionRoleArn, AirflowVersion, etc.) applied to the stored environment despite the API
  call returning an error to the caller -- a client retrying with valid worker counts would
  see fields it never successfully set. Fixed by computing the effective (post-update)
  MinWorkers/MaxWorkers from the request before any mutation and validating first;
  the mw1.micro webserver check added this pass follows the same pre-mutation-validation
  pattern.
