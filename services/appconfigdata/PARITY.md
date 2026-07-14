---
service: appconfigdata
sdk_module: aws-sdk-go-v2/service/appconfigdata@v1.23.20   # version audited against
last_audit_commit: 128350087c039303f08b6d8113ec9f9ac4cbc4b9
last_audit_date: 2026-07-13
overall: B            # already-accurate on the core protocol; 3 concrete defects fixed
ops:
  StartConfigurationSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "identifier max-length was 2048, real Identifier shape max is 128 -- fixed"}
  GetLatestConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "poll-interval echo took max(default,declared) instead of declared-or-default -- fixed; empty-blob-on-unchanged semantics were already correct"}
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
    populated). This is a cross-service wiring gap in services/appconfig +
    cli.go/dashboard, out of scope for an appconfigdata-only edit. (bd: file a
    gopherstack-appconfig-appconfigdata-bridge issue)
  - GetLatestConfiguration returns HTTP 204 (No Content) when the configuration is
    unchanged and HTTP 200 when it has content. The real service-2.json models a *fixed*
    `responseCode: 200` for GetLatestConfiguration -- AWS always answers 200, with an empty
    body when nothing changed, never 204. This was NOT changed in this pass: both
    aws-sdk-go-v2 (`response.StatusCode < 200 || >= 300` in deserializers.go) and botocore
    (`http.status_code >= 300`) treat any 2xx as success, so no real SDK client observes a
    difference, and the existing test suite (including a real aws-sdk-go-v2-driven
    integration test, parity_pass1_test.go) has deep, deliberate coverage asserting 204.
    Flagging as a known wire-shape deviation from the literal AWS model rather than fixing,
    since fixing has zero functional benefit for any SDK and would be a large, purely
    cosmetic diff across ~15 test assertions. (bd: low-priority, cosmetic-only)
deferred: []
leaks: {status: clean, note: "janitor.go SessionSweeper ticker exits cleanly on ctx.Done(); no unbounded goroutines"}
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
    AWS exception type; removed.
  - BadRequestException.Reason enum: only "InvalidParameters" exists in the real model --
    matches badRequestReasonInvalidParameters.
  - InvalidParameterProblem enum: Corrupted, Expired, PollIntervalNotSatisfied -- exact
    3-value match with gopherstack's invalidParamProblem* constants.
  - ResourceNotFoundException.ResourceType enum: Application, ConfigurationProfile,
    Deployment, Environment, Configuration -- exact 5-value match (only Deployment is
    actually raised today; the rest exist for API-shape completeness).
  - Identifier shape: min 1, max **128** (verified via botocore's bundled service-2.json;
    gopherstack previously enforced 2048, which was never the real bound -- fixed, along
    with the two boundary tests that encoded the wrong number).
  - OptionalPollSeconds (RequiredMinimumPollIntervalInSeconds) shape: min 15, max 86400 --
    already correct in gopherstack, no change needed.

- **Bug fixed**: `writeGetLatestConfigurationResponse` computed
  `Next-Poll-Interval-In-Seconds` as `max(defaultPollIntervalInSeconds, session-declared)`.
  Since the AWS-allowed minimum declared interval (15s) is below the service default (30s),
  every session declaring an interval in [15,29] silently got back "30" instead of its own
  declared value -- clients honoring the header would poll less aggressively than they
  asked to, and clients round-tripping the header value elsewhere would see a value that
  doesn't match what they requested. Fixed to use the declared value whenever
  `PollIntervalInSeconds > 0`, falling back to the default only when the client left it at
  0 (unset). Regression test: TestHandler_PollIntervalHonored now covers both an
  above-default (60s) and a below-default (15s) declared interval.

- **The most important behavior for this service** -- GetLatestConfiguration returning a
  full `Configuration` blob on the first poll and an *empty* blob on subsequent polls when
  the underlying profile's content hash hasn't changed -- was already correctly implemented
  (backend.go's `hash != sess.PreviousContentHash` check) and is exercised end-to-end via a
  real aws-sdk-go-v2 client in parity_pass1_test.go's TestParity_SDKFullSessionFlow. No
  changes needed here.

- Token lifecycle (opaque, single-use, rotated every successful poll, HMAC-signed with a
  process-lifetime signing key, ~5-minute grace window for idempotent retry of the
  just-rotated-away token, 24h absolute + 1h idle janitor-swept expiry) all verified against
  the "each token is valid for one call... valid for up to 24 hours" documentation and
  found already correct; no changes made to backend.go's token logic.

- Persistence: Handler.Snapshot/Restore delegate to InMemoryBackend, which uses
  pkgs/store.Registry across three tables (profiles, sessions, graceTokens). Round-trip
  verified by persistence_test.go including the grace-token idempotent-retry path
  post-restore. The HMAC signing key is deliberately NOT persisted (regenerated per
  process start, matching the sibling AppConfig control-plane service's pagination-secret
  precedent) -- a restored session token will fail its MAC check and surface as
  ErrTokenCorrupted after a restart; this is a documented, accepted limitation, not a bug.
