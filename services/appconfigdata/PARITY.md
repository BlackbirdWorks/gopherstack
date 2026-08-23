---
service: appconfigdata
sdk_module: aws-sdk-go-v2/service/appconfigdata@v1.26.4   # version audited against
last_audit_commit: 0aba172b526c53ba24aaf135c063a37ba136f7f5
last_audit_date: 2026-08-20
overall: A            # both ops re-verified field-by-field against v1.26.4's generated
                       # serializers.go/deserializers.go/types.go/errors.go/enums.go plus
                       # botocore 1.43.56's bundled service-2.json.gz -- zero new bugs found
ops:
  StartConfigurationSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "identifier max-length was 2048, real Identifier shape max is 128 -- fixed in a prior pass"}
  GetLatestConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "poll-interval echo and empty-blob-on-unchanged semantics already correct; this pass fixed the 204-vs-200 responseCode deviation (see below)"}
gaps:
  - services/appconfig is now bridged to appconfigdata (bd gopherstack-uiyi, closed;
    commit 41f3817bd): appconfig's InMemoryBackend.finalizeDeploymentLocked (every
    completion path -- synchronous zero-duration, the async reconciler, and restore-time
    finalization funnel through it) and revertDeployedConfigLocked (an AllowRevert
    StopDeployment) call publishDeployedConfigurationLocked, which pushes the deployed
    content into appconfigdata via PublishConfiguration, stamping ConfigVersion.DeploymentId
    with the real deployment number. cli.go's wireAppConfigDeployments wires
    appconfigdata's *InMemoryBackend straight in as appconfig's
    DeployedConfigurationPublisher, no adapter. A real StartDeployment now surfaces through
    StartConfigurationSession + GetLatestConfiguration polling, and
    StartConfigurationSession correctly 404s with ErrNoActiveDeployment until a deployment
    has completed, matching real AWS.
  - The bridge only covers AppConfig-hosted configuration profiles: publishDeployedConfigurationLocked
    skips any profile whose LocationURI isn't the "hosted" sentinel
    (services/appconfig/configuration.go:86, contentTypeHostedLocation at
    services/appconfig/store.go:26) -- the same restriction CurrentDeployedConfiguration
    already had. Profiles backed by SSM Parameter Store, SSM documents, S3, or Secrets
    Manager still never populate appconfigdata; SetConfiguration remains reachable only via
    the dashboard admin endpoints (cli.go:8293, dashboard/ui.go:1462/2195) with no
    deployment attribution for those location types.
deferred: []
leaks: {status: clean, note: "janitor.go SessionSweeper ticker is ctx-parented via worker.NewGroup and exits cleanly on ctx.Done() (g.Stop() joins on return); SweepExpiredSessions purges both idle/absolute-expired sessions and expired grace-token cache entries in the same pass, so neither table grows unbounded. Verified this pass with new janitor_test.go: TestJanitor_RunExitsOnContextCancel (goroutine actually exits within 500ms of cancel, not just 'looks ctx-parented by inspection') and TestJanitor_SweepsExpiredSessionsOnTick (the ticker actually invokes the sweep and evicts a live session, not just a direct SweepExpiredSessions() unit test)."}
---

## Notes

- **2026-08-20 wrapper-key/nested-shape sweep**: zero bugs found. Re-verified every
  wire-shape claim below directly against the pinned `aws-sdk-go-v2/service/appconfigdata
  @v1.26.4` source in the module cache (not sibling versions, not the handler's own code)
  plus botocore 1.43.56's bundled `appconfigdata/2021-11-11/service-2.json.gz` decompressed
  locally:
  - `api_op_StartConfigurationSession.go`: Input has exactly `ApplicationIdentifier`,
    `ConfigurationProfileIdentifier`, `EnvironmentIdentifier`,
    `RequiredMinimumPollIntervalInSeconds` (all flat JSON body fields per
    `awsRestjson1_serializeOpDocumentStartConfigurationSessionInput`,
    serializers.go:152-172 -- no HTTP bindings besides `Content-Type`). Output has exactly
    `InitialConfigurationToken` (deserializers.go:308-341). Matches gopherstack's
    `startSessionRequest`/`startSessionResponse` in models.go field-for-field.
  - `api_op_GetLatestConfiguration.go`: Input has exactly `ConfigurationToken`, bound to
    query param `configuration_token` (serializers.go:71-81,
    `encoder.SetQuery("configuration_token")`). Output has `Configuration []byte` as the
    httpPayload (`payload: Configuration` in service-2.json), plus `ContentType`,
    `NextPollConfigurationToken`, `NextPollIntervalInSeconds`, `VersionLabel` all bound to
    response HEADERS (deserializers.go:130-159): `Content-Type`,
    `Next-Poll-Configuration-Token`, `Next-Poll-Interval-In-Seconds`, `Version-Label` --
    exact matches to gopherstack's header constants in handler.go:31-38. The payload
    deserializer (`awsRestjson1_deserializeOpDocumentGetLatestConfigurationOutput`,
    deserializers.go:161-181) does no JSON decode at all -- it reads the raw body into
    `v.Configuration = buf.Bytes()` only `if buf.Len() > 0`, confirming the "raw blob, empty
    when unchanged" contract. gopherstack's `writeGetLatestConfigurationResponse`
    (handler.go:328-367) mirrors this: `c.Blob` for non-empty content, `c.NoContent(200)`
    (never 204, matching the model's fixed `responseCode: 200`) for empty, and
    Content-Type/ETag/Version-Label headers are only set on the non-empty path.
  - Diffed `api_op_*.go`, `types/types.go`, `types/errors.go`, `types/enums.go` between the
    module-cache's v1.23.20 (the version a prior pass's notes cited) and the pinned v1.26.4:
    zero wire-relevant differences -- only internal SDK middleware-stack plumbing changed
    (retry/logging/telemetry helper wiring), no Input/Output/error/enum field changes. The
    prior pass's per-field findings remain valid at the currently pinned version.
  - `BadRequestException.Details` union: single member `InvalidParametersMap` keyed
    `"InvalidParameters"` (deserializers.go:507-534,
    `BadRequestDetailsMemberInvalidParameters`), value `map[string]InvalidParameterDetail`
    where `InvalidParameterDetail{Problem InvalidParameterProblem}` -- exact match to
    gopherstack's `invalidParamsDetail{InvalidParameters map[string]invalidParamProblem}`.
    `BadRequestReason` has only `"InvalidParameters"` (enums.go); `InvalidParameterProblem`
    has exactly `Corrupted`, `Expired`, `PollIntervalNotSatisfied` (enums.go) -- both
    checked both directions (enum constant lists diffed against gopherstack's errors.go
    constants, and against every emission site in handler.go), no drift, no invented values.
  - `ResourceNotFoundException` carries `ResourceType` (5-value enum: `Application`,
    `ConfigurationProfile`, `Deployment`, `Environment`, `Configuration`) and
    `ReferencedBy map[string]string` (types/errors.go) -- both modeled in gopherstack's
    `awsResourceNotFoundBody`.
  - `Identifier` shape max 128 (service-2.json), `Token` shape pattern `\S{1,8192}`,
    `OptionalPollSeconds` min 15/max 86400 -- all three re-confirmed unchanged and already
    correctly enforced in gopherstack's `errors.go` constants.
  - No FABRICATED or MISSING members found on either op's Input/Output/error shapes; no
    wrong-nesting-level, wrong-JSON-type, or wrong-enum-value findings. The service's tiny
    two-op surface leaves little room for the (a)-(e) bug classes this sweep targets, and
    none were present.
  - One doc-only fix: `TestHandler_TokenExpired`'s godoc claimed "the handler returns 401"
    -- the test never calls the HTTP handler (it drives `InMemoryBackend` directly and
    asserts `ErrSessionNotFound`), and real AWS returns 400 for an expired token anyway
    (already correctly documented in errors.go's `ErrTokenExpired` comment). Corrected the
    comment; no behavior change. `services/appconfigdata/configuration_test.go:200-202`.
  - Structurally unverifiable boundary: `Configuration`'s content is an opaque
    client-supplied blob (SensitiveBlob shape) -- gopherstack's own change-detection
    (content-hash comparison) and grace-token replay logic determine emptiness, and no real
    AWS behavior exists to diff against beyond "empty when unchanged, full otherwise",
    which is what both the SDK doc comment and gopherstack's `configuration.go` already
    encode.
  - Provenance: prior stamp's `last_audit_commit` (`1283500...`) dated 2026-07-13 while
    `last_audit_date` claimed 2026-07-24 -- an 11-day gap with the sha predating the date,
    the documented tell for a stale/inconsistent stamp. Content itself re-verified accurate
    regardless (see above); stamp refreshed to current HEAD + today's date.

- Two ops only: StartConfigurationSession (POST /configurationsessions, restjson1,
  201-on-success) and GetLatestConfiguration (GET /configuration, ConfigurationToken bound
  as the `configuration_token` query param, response is a raw body blob + headers, not a
  JSON envelope). Both route/query bindings verified directly against
  aws-sdk-go-v2/service/appconfigdata@v1.23.20's generated serializers.go/deserializers.go
  (not just the handler's own code) -- confirmed exact matches:
  - Response headers: `Content-Type`, `Next-Poll-Configuration-Token`,
    `Next-Poll-Interval-In-Seconds`, `Version-Label` (NOT `X-Amzn-AppConfig-Version-Label`,
    an older/incorrect header name a previous pass had already caught and fixed).
  - Exception set (from service-2.json): BadRequestException(400),
    InternalServerException(500), ResourceNotFoundException(404), ThrottlingException(429).
    There is no PayloadTooLargeException for this service -- a stray
    `exceptionPayloadTooLarge` const existed in types.go, unused and describing a fictitious
    AWS exception type; removed in a prior pass.
  - BadRequestException.Reason enum: only "InvalidParameters" exists in the real model --
    matches badRequestReasonInvalidParameters.
  - InvalidParameterProblem enum: Corrupted, Expired, PollIntervalNotSatisfied -- exact
    3-value match with gopherstack's invalidParamProblem* constants.
  - ResourceNotFoundException.ResourceType enum: Application, ConfigurationProfile,
    Deployment, Environment, Configuration -- exact 5-value match (only Deployment is
    actually raised today; the rest exist for API-shape completeness).
  - Identifier shape: min 1, max **128** (re-verified this pass directly against botocore
    1.43.34's bundled `appconfigdata/2021-11-11/service-2.json.gz`, decompressed and
    inspected locally: `{"max": 128, "min": 1, "type": "string"}`).
  - Token shape: pattern `\S{1,8192}` (non-whitespace, 1-8192 chars) -- gopherstack's
    `<64-hex-random>.<16-hex-mac>` token format is well within bounds; server-side length/
    pattern enforcement isn't needed since malformed tokens already fail the MAC check and
    surface as BadRequestException/Corrupted.
  - OptionalPollSeconds (RequiredMinimumPollIntervalInSeconds) shape: min 15, max 86400 --
    already correct in gopherstack, no change needed.
  - StartConfigurationSession responseCode: fixed 201. GetLatestConfiguration responseCode:
    fixed 200 (see the 204-vs-200 fix below).

- **Bug fixed this pass**: `writeGetLatestConfigurationResponse` returned HTTP 204 (No
  Content) when the configuration was unchanged since the last poll. Re-verified directly
  against botocore's bundled service-2.json: `GetLatestConfiguration`'s `http` binding is
  `{"method": "GET", "requestUri": "/configuration", "responseCode": 200}` -- a *fixed*
  code, with no 204 variant documented anywhere in the model, even when the `Configuration`
  payload is empty. AWS's real behavior is 200 with an empty body, never 204. A previous
  audit pass had already caught this deviation and explicitly chose not to fix it (see prior
  revision of this file), reasoning that both aws-sdk-go-v2's deserializer
  (`response.StatusCode < 200 || >= 300`) and botocore's (`http.status_code >= 300`) accept
  any 2xx as success, so no real SDK client observes a difference. That reasoning is correct
  as far as it goes, but "TRUE AWS parity" means matching the documented wire shape exactly,
  not just what happens not to break the two reference SDKs -- a raw HTTP client, a
  different-language SDK, or a test harness asserting on status code directly would all see
  the deviation. Fixed `handleGetLatestConfigurationResponse` to always return
  `http.StatusOK` (with an empty body via `c.NoContent`, exactly as before, just with a
  different status). Updated ~16 test assertions across configuration_test.go and
  profile_test.go that previously asserted `http.StatusNoContent`; all now assert
  `http.StatusOK` plus an explicit empty-body check where relevant, so the "unchanged means
  no body" behavior remains fully covered even though the status code no longer
  distinguishes the two cases. sdk_flow_test.go's real-aws-sdk-go-v2-driven integration test
  did not need a status-code change (it never asserted on raw HTTP status), only a stale
  comment fix; it still passes unmodified because the SDK, as noted above, always treated
  200 and 204 as equivalent success.

- **Bug fixed in a prior pass**: `writeGetLatestConfigurationResponse` computed
  `Next-Poll-Interval-In-Seconds` as `max(defaultPollIntervalInSeconds, session-declared)`.
  Since the AWS-allowed minimum declared interval (15s) is below the service default (30s),
  every session declaring an interval in [15,29] silently got back "30" instead of its own
  declared value -- clients honoring the header would poll less aggressively than they
  asked to, and clients round-tripping the header value elsewhere would see a value that
  doesn't match what they requested. Fixed to use the declared value whenever
  `PollIntervalInSeconds > 0`, falling back to the default only when the client left it at
  0 (unset). Regression test: TestHandler_PollIntervalHonored covers both an above-default
  (60s) and a below-default (15s) declared interval.

- **The most important behavior for this service** -- GetLatestConfiguration returning a
  full `Configuration` blob on the first poll and an *empty* blob on subsequent polls when
  the underlying profile's content hash hasn't changed -- was already correctly implemented
  (configuration.go's `hash != sess.PreviousContentHash` check) and is exercised end-to-end
  via a real aws-sdk-go-v2 client in sdk_flow_test.go's TestSDKClient_FullSessionFlow. No
  changes needed here.

- Token lifecycle (opaque, single-use, rotated every successful poll, HMAC-signed with a
  process-lifetime signing key, ~5-minute grace window for idempotent retry of the
  just-rotated-away token, 24h absolute + 1h idle janitor-swept expiry) all verified against
  the "each token is valid for one call... valid for up to 24 hours" documentation and
  found already correct; no changes made to backend.go's token logic this pass.

- Persistence: Handler.Snapshot/Restore delegate to InMemoryBackend, which uses
  pkgs/store.Registry across three tables (profiles, sessions, graceTokens). Round-trip
  verified by persistence_test.go including the grace-token idempotent-retry path
  post-restore. The HMAC signing key is deliberately NOT persisted (regenerated per
  process start, matching the sibling AppConfig control-plane service's pagination-secret
  precedent) -- a restored session token will fail its MAC check and surface as
  ErrTokenCorrupted after a restart; this is a documented, accepted limitation, not a bug.

- Chaos/fault-injection (ChaosServiceName/ChaosOperations/ChaosRegions) is wired via the
  generic pkgs/chaos middleware keyed by service+operation name, not per-handler code --
  ThrottlingException is reachable through that path even though nothing in this package
  raises it directly.
