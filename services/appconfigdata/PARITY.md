---
service: appconfigdata
sdk_module: aws-sdk-go-v2/service/appconfigdata@v1.23.20   # version audited against
last_audit_commit: 128350087c039303f08b6d8113ec9f9ac4cbc4b9
last_audit_date: 2026-07-24
overall: A            # both ops field-diffed clean against the real SDK + botocore service-2.json; only remaining item is a documented cross-service wiring gap
ops:
  StartConfigurationSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "identifier max-length was 2048, real Identifier shape max is 128 -- fixed in a prior pass"}
  GetLatestConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "poll-interval echo and empty-blob-on-unchanged semantics already correct; this pass fixed the 204-vs-200 responseCode deviation (see below)"}
gaps:
  - appconfigdata's config content store (SetConfiguration) is entirely self-contained and
    is never populated by services/appconfig (the control-plane service that owns
    applications/environments/configuration-profiles/hosted-config-versions/deployments).
    SetConfiguration is reachable only via the internal dashboard admin endpoints
    (cli.go:6091, dashboard/ui.go:1345/2020), not from any real AppConfig deployment flow.
    Real AWS semantics: GetLatestConfiguration serves whatever the *active deployment* for
    the app/env/profile currently is; StartConfigurationSession 404s
    (ErrNoActiveDeployment) until one exists. gopherstack instead requires a manual/dashboard
    SetConfiguration call to seed content per app/env/profile key -- functionally similar
    per-session but with no link to services/appconfig's deployment lifecycle (no
    deployment-state transitions, no rollback-on-deploy, no version pinning to a specific
    DeploymentId even though ConfigVersion.DeploymentId exists as a field and is never
    populated). This is a cross-service wiring gap in services/appconfig + cli.go/dashboard,
    out of scope for an appconfigdata-only edit -- fixing it means wiring a
    Set<Config>-style accessor into cli.go's provider-init sequence (the same pattern used
    by wireIoTRules/wireAppSyncLambda for other control-plane/data-plane service pairs), and
    this pass's mandate explicitly forbids editing cli.go. Re-confirmed still open and
    already tracked: bd issue gopherstack-uiyi ("appconfigdata disconnected from appconfig
    control-plane"), open, priority 2.
deferred: []
leaks: {status: clean, note: "janitor.go SessionSweeper ticker is ctx-parented via worker.NewGroup and exits cleanly on ctx.Done() (g.Stop() joins on return); SweepExpiredSessions purges both idle/absolute-expired sessions and expired grace-token cache entries in the same pass, so neither table grows unbounded. Verified this pass with new janitor_test.go: TestJanitor_RunExitsOnContextCancel (goroutine actually exits within 500ms of cancel, not just 'looks ctx-parented by inspection') and TestJanitor_SweepsExpiredSessionsOnTick (the ticker actually invokes the sweep and evicts a live session, not just a direct SweepExpiredSessions() unit test)."}
---

## Notes

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
