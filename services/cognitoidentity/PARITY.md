---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: cognitoidentity
sdk_module: aws-sdk-go-v2/service/cognitoidentity@v1.36.4
last_audit_commit: 2d47b51d4
last_audit_date: 2026-07-29
overall: A                # error-taxonomy field-diff vs deserializers.go found 3 real gaps, all fixed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateIdentityPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed OpenIdConnectProviderARNs JSON key casing (prior pass); LimitExceededException deferred, see deferred[]"}
  DeleteIdentityPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades identities/roles/principalTags"}
  DescribeIdentityPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed OpenIdConnectProviderARNs JSON key casing (prior pass)"}
  ListIdentityPools: {wire: ok, errors: ok, state: ok, persist: ok, note: "name-cursor pagination verified"}
  UpdateIdentityPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed OpenIdConnectProviderARNs JSON key casing (prior pass); ConcurrentModificationException/LimitExceededException deferred"}
  GetId: {wire: ok, errors: ok, state: ok, persist: ok, note: "merges logins into existing identity per AWS semantics; ExternalServiceException/LimitExceededException deferred"}
  GetCredentialsForIdentity: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed Expiration epoch-seconds vs epoch-millis bug (prior pass); NEW this pass: InvalidIdentityPoolConfigurationException when the pool has no IAM role for the identity's auth state (real business-logic gap, not just an error-code omission -- GetCredentialsForIdentity previously handed out credentials for pools with zero role configuration)"}
  GetOpenIdToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "ExternalServiceException deferred"}
  SetIdentityPoolRoles: {wire: ok, errors: ok, state: ok, persist: ok, note: "partial-merge semantics (omitted keys preserved) — deliberately tested by prior Refinement2 pass, left as-is; ConcurrentModificationException deferred"}
  GetIdentityPoolRoles: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIdentities: {wire: ok, errors: ok, state: ok, persist: ok, note: "best-effort silent skip of missing IDs matches AWS"}
  DescribeIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "timestamps now routed through pkgs/awstime.Epoch (prior pass)"}
  GetOpenIdTokenForDeveloperIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "PrincipalTags input field accepted by SDK but not consumed (see gaps). NEW this pass: an explicit IdentityId was previously validated for existence but its Logins were silently dropped instead of being linked (a disguised stub per parity principle #4) -- now actually links them, and rejects a developer-provider login already claimed by a different identity with DeveloperUserAlreadyRegisteredException"}
  GetPrincipalTagAttributeMap: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIdentities: {wire: ok, errors: ok, state: ok, persist: ok, note: "timestamps now routed through pkgs/awstime.Epoch (prior pass)"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  LookupDeveloperIdentity: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NEW this pass: when both IdentityId and DeveloperUserIdentifier are supplied, they are now cross-validated and a ResourceConflictException is returned on mismatch, per the operation's own doc comment (\"If you supply both, DeveloperUserIdentifier will be matched against IdentityId... Otherwise, a ResourceConflictException is thrown\"); previously the DeveloperUserIdentifier argument was silently ignored whenever IdentityId was also set"}
  MergeDeveloperIdentities: {wire: ok, errors: ok, state: ok, persist: ok}
  SetPrincipalTagAttributeMap: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UnlinkDeveloperIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  UnlinkIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "ExternalServiceException deferred"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families: {}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "IMPOSSIBLE (re-investigated gopherstack-tqdj): GetOpenIdTokenForDeveloperIdentity accepts a PrincipalTags request field (SDK-modeled, real doc comment: 'Use this operation to configure attribute mappings for custom providers.') but the backend never stores/applies it. Investigated two candidate real consumption points before concluding this: (1) GetCredentialsForIdentityInput (confirmed against api_op_GetCredentialsForIdentity.go) has NO Token/tag-related parameter at all -- only IdentityId/CustomRoleArn/Logins -- so PrincipalTags cannot flow through that op's wire surface no matter what gopherstack does internally; real AWS's actual use of PrincipalTags is to set STS session tags on the role-assumption Cognito performs *internally* on GetCredentialsForIdentity, which has no client-visible wire representation gopherstack could honestly populate without also faking IAM/STS session-tag enforcement this codebase doesn't model anywhere (see the separate 'session Policy/PolicyArns... not enforced' deferred item above). (2) Considered whether the OIDC token itself could carry the tags as a real https://aws.amazon.com/tags claim (mirroring services/sts's own GetWebIdentityToken, which does this) for a caller that hands the token to STS's AssumeRoleWithWebIdentity directly -- STS's WebIdentityToken parser (token_validation.go) is genuinely claim-driven and signature-verification-free, so this is *technically* wireable. Not implemented this pass: it would require replacing the current placeholder token format (a static JWT-shaped header + random payload + literal 'signature', see GetOpenIdToken/GetOpenIdTokenForDeveloperIdentity in credentials.go) with a real base64url JSON payload, is a materially larger change than an error-type fix, and no test or documented use case in this codebase currently chains a cognitoidentity-issued token into sts.AssumeRoleWithWebIdentity to exercise it -- speculative cross-service plumbing without a concrete consumer was judged too large/uncertain a change for this pass. Left as an honestly-documented gap, not fabricated."
  - "IMPOSSIBLE (re-investigated gopherstack-tqdj): SetIdentityPoolRoles/GetOpenIdTokenForDeveloperIdentity TokenDuration are accepted/validated (0-86400s range) but not enforced against issued token lifetime. Same root cause as the PrincipalTags item above: the returned token is an opaque synthetic string (credentials.go), not a real JWT with an exp claim, and no operation in this codebase currently re-validates staleness of a previously issued cognitoidentity OpenID token. Embedding a real TokenDuration-derived exp claim would require the same token-format rework discussed above, for the same currently-hypothetical consumer -- not implemented this pass for the same reason."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - HTTP status code choice for NotAuthorizedException (403 here) vs AWS's actual per-exception status (SDK error-type resolution is body-driven, not status-code-driven, so this doesn't break aws-sdk-go-v2 clients; only relevant to tooling that inspects raw HTTP status).
  - "ALREADY COVERED BY CHAOS (verified gopherstack-tqdj): ConcurrentModificationException (SetIdentityPoolRoles, UpdateIdentityPool per deserializers.go) is not emulated: there is no optimistic-concurrency/version token in this backend's resource model to make a genuine concurrent-write collision detectable, and fabricating one that never fires (or fires on arbitrary heuristics) would be worse than omitting it. Would need a real revision-counter field added to IdentityPool/IdentityRoles to do properly -- out of scope for an error-taxonomy pass. Concretely verified this pass: cognitoidentity.Handler implements ChaosServiceName() -> \"cognito-identity\" and ChaosOperations() -> h.GetSupportedOperations() (handler.go), and pkgs/chaos.Middleware is wired globally via registry.Use(chaos.Middleware(faultStore)) in cli.go, matching purely on the request's SigV4 service name + X-Amz-Target operation + region and injecting an arbitrary caller-specified FaultError{Code, StatusCode} without touching backend state -- a fault rule such as {\"service\":\"cognito-identity\",\"operation\":\"UpdateIdentityPool\",\"error\":{\"code\":\"ConcurrentModificationException\",\"statusCode\":400}} deterministically returns that exact typed error to a real client with zero backend code changes."
  - "ALREADY COVERED BY CHAOS (verified gopherstack-tqdj): TooManyRequestsException (every op) and LimitExceededException (CreateIdentityPool/GetId/UpdateIdentityPool per deserializers.go) are throttling/account-quota conditions; this in-memory emulator has no request-rate tracking and AWS's actual per-account pool/identity quotas are account-specific soft limits, not fixed constants -- inventing an arbitrary hard-coded threshold would be a fabricated business rule, not a verified one. Left unimplemented, consistent with how other gopherstack services treat throttling. Same chaos mechanism as ConcurrentModificationException above makes both reachable on demand with zero backend code changes."
  - "ALREADY COVERED BY CHAOS (verified gopherstack-tqdj): ExternalServiceException (GetCredentialsForIdentity, GetId, GetOpenIdToken, UnlinkIdentity per deserializers.go) is AWS's wrapper for a real external identity provider (Facebook/Google/a linked Cognito user pool) rejecting a token. This backend validates login tokens against its own stored state, not a real external IdP, so there is no authentic backend-state trigger condition for this exception here. Same chaos mechanism as above makes it reachable on demand with zero backend code changes."
leaks: {status: clean, note: "no goroutines/janitors in this service; single lockmetrics.RWMutex guards all store.Table access"}
---

## Notes

- Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: AWSCognitoIdentityService.<Op>`
  dispatch. Confirmed target prefix and `Content-Type: application/x-amz-json-1.1` against the
  real serializer output (`serializers.go`).

- **Bug fixed — Expiration wire format (epoch seconds, not milliseconds).**
  `GetCredentialsForIdentity`'s `Credentials.Expiration` was serialized via
  `creds.Expiration.UnixMilli()` into an `int64` field. The real deserializer
  (`aws-sdk-go-v2/service/cognitoidentity` `deserializers.go`, case `"Expiration"`) calls
  `smithytime.ParseEpochSeconds(f64)`, i.e. the wire value is a JSON number of **seconds**
  since epoch (fractional allowed), exactly matching `pkgs/awstime.Epoch`'s contract. A
  prior "accuracy" pass (`accuracy_test.go` Gap 7) got this backwards and had asserted the
  *wrong* (millisecond) behavior as correct — the test has been corrected to assert
  epoch-seconds and to bound the value against a ~1-hour-from-now expiry. `DescribeIdentity`
  and `ListIdentities`' `CreationDate`/`LastModifiedDate` were already numerically equivalent
  (hand-rolled `UnixMilli()/1000.0`) but have been switched to `awstime.Epoch` for consistency
  and to remove the now-redundant `millisPerSecond` local constant, per the pkgs-reuse rule.

- **Bug fixed — `OpenIdConnectProviderARNs` JSON key casing.** The real wire key (confirmed in
  both `serializers.go` and `deserializers.go`) is `OpenIdConnectProviderARNs` (lowercase `d`
  in `Id`). The handler emitted `OpenIDConnectProviderARNs` (uppercase `ID`) on
  `CreateIdentityPool`/`DescribeIdentityPool`/`UpdateIdentityPool` responses. Go's
  `encoding/json.Unmarshal` is case-insensitive, so *incoming* requests using either casing
  still decoded correctly (that direction was not actually broken), but `json.Marshal` is
  case-exact, so a real aws-sdk-go-v2 client's deserializer switch (`case
  "OpenIdConnectProviderARNs":`) would never match our old key and would silently drop the
  field from every response. Fixed the three JSON tags (Go field identifiers unchanged); the Go
  field name itself is a private implementation detail and does not need to match the wire key.

- `SetIdentityPoolRoles` uses partial-merge semantics: a key omitted from the request's `Roles`
  map (or a nil `RoleMappings`) leaves the existing stored value untouched rather than clearing
  it. This looked like a possible "should be full-replace, like most AWS `Set*` ops" bug, but it
  is deliberately covered by three dedicated tests from an earlier audit pass
  (`TestInMemoryBackend_Refinement2_SetIdentityPoolRoles_MergePreservesExistingRole`,
  `TestHandler_Refinement2_SetIdentityPoolRoles_PreservesExistingRole`,
  `TestAccuracy_SetIdentityPoolRoles_RoleMappingsNilPreservesExisting`) — left as-is this pass;
  flagging here so a future auditor with a way to verify against real AWS doesn't waste time
  re-discovering the same fork in the road.

- Sentinel-error → HTTP-status mapping: `ResourceNotFoundException`/`ResourceConflictException`/
  `InvalidParameterException` → 400, `NotAuthorizedException` → 403. Verified the real SDK's
  client-side error-type resolution (`deserializers.go`) only checks `response.StatusCode < 200
  || >= 300` and then dispatches purely on the body's error-type string — the exact status code
  used here doesn't affect aws-sdk-go-v2 client behavior either way, so this was left alone
  (deferred, not a proven bug).

- Persistence: `Handler.Snapshot`/`Restore` delegate to `InMemoryBackend.Snapshot`/`Restore`
  (persistence.go), which round-trip all four `store.Table` collections (pools, identities,
  roles, principalTags) through region-qualified DTOs. Verified present and wired correctly;
  not modified this pass.

- Region isolation: every resource is keyed by composite `"region|id"` (`regionKey` in
  backend.go) via `store.Table`/`store.Index`, with per-request region resolved from
  `X-Amz-Region`/SigV4 via `regionContextKey`. Verified consistent across all ops.

## 2026-07-24 pass — error-taxonomy field-diff

Prior passes field-diffed wire *shapes* thoroughly but never field-diffed the *error
taxonomy* against `aws-sdk-go-v2/service/cognitoidentity`'s `deserializers.go`, which encodes
-- per operation, in its `awsAwsjson11_deserializeOpError<Op>` functions -- the exact set of
`strings.EqualFold("<ExceptionName>", errorCode)` cases the real client recognizes. Extracted
that table this pass (`awk` over `deserializers.go`) and diffed it against
`cognitoIdentitySentinelErrors` in `handler.go`, which only implemented 4 of the 11 modeled
exception types (`ResourceNotFoundException`/`ResourceConflictException`/
`InvalidParameterException`/`NotAuthorizedException`). Findings:

- **Bug fixed — generic-error fallback used the wrong wire type.** `resolveErrorType`'s
  catch-all returned Query/EC2-protocol-style `"InternalFailure"`, which does not match any
  case in *any* of cognitoidentity's 24 per-operation error switches (every one of them
  recognizes `"InternalErrorException"` instead, confirmed by grepping every
  `awsAwsjson11_deserializeOpError*` function). A real aws-sdk-go-v2 client hitting this path
  would fall through to an untyped smithy API error instead of a typed
  `*types.InternalErrorException`, breaking typed-exception-matching retry logic. Same bug
  class previously found and fixed in bedrockruntime (see that service's PARITY.md). Fixed:
  `resolveErrorType`'s fallback now returns `"InternalErrorException"`.

- **Bug fixed — `LookupDeveloperIdentity` silently ignored `DeveloperUserIdentifier` when
  `IdentityId` was also supplied.** The op's own doc comment in `api_op_LookupDeveloperIdentity.go`
  states: "Either IdentityID or DeveloperUserIdentifier must not be null. If you supply only
  one of these values, the other value will be searched... and returned... If you supply
  both, DeveloperUserIdentifier will be matched against IdentityID... Otherwise, a
  ResourceConflictException is thrown." The backend only ever branched on `identityID != ""`
  first and never even looked at `developerUserIdentifier` in that case -- so a caller
  supplying a mismatched pair got a silent (wrong) success instead of a conflict. Fixed by
  resolving both lookups independently and reconciling them (`reconcileLookupMatch`); added
  the new `ErrResourceConflict` sentinel (wire type `ResourceConflictException`, distinct
  from the pool-name-collision `ErrIdentityPoolAlreadyExists` which shares the same wire
  type per AWS, as multiple conditions can map to one exception type).

- **Bug fixed — `GetOpenIdTokenForDeveloperIdentity` dropped logins when `IdentityId` was
  explicit (disguised stub).** The op's doc comment says it "can also be used to... link new
  logins... to an existing... identity, by providing the existing IdentityId." The backend
  validated the identity existed but then discarded `logins` entirely instead of merging them
  -- looked like real logic (existence check + real state read) but silently no-opped the
  actual linking work, matching parity-principles.md's "disguised stub" pattern. Fixed with
  `linkDeveloperLogins`, which also implements the previously entirely-unimplemented
  `DeveloperUserAlreadyRegisteredException`: rejects linking a developer-provider login that's
  already registered to a *different* identity in the pool (checked via the pool's
  `DeveloperProviderName`, since `Logins` may also carry non-developer provider entries).

- **Bug fixed — `GetCredentialsForIdentity` never checked identity-pool role
  configuration.** Real AWS returns `InvalidIdentityPoolConfigurationException` when the pool
  has no IAM role for the identity's auth state; gopherstack happily minted synthetic
  credentials for pools with zero roles configured (or missing the specific auth/unauth role
  needed), which is a real, common, user-visible AWS error condition ("Invalid identity pool
  configuration. Check assigned IAM roles for this pool."). Fixed via `checkRoleConfigured`,
  called after login-token validation (so `NotAuthorizedException` still takes precedence for
  mismatched tokens, matching existing precedence). This is a real backend-state check
  (`b.rolesGet`), not a fabricated rule -- it required touching ~7 existing tests that
  previously called `GetCredentialsForIdentity` without ever configuring pool roles, which
  is not how a real AWS caller could ever have gotten credentials in the first place.

- **Deferred, with rationale recorded above:** `ConcurrentModificationException` (no
  optimistic-concurrency model to make authentic), `TooManyRequestsException`/
  `LimitExceededException` (no request-rate tracking; AWS's real limits are account-specific
  soft quotas, not constants safe to hard-code), `ExternalServiceException` (wraps a real
  external IdP's rejection; this backend has no real external IdP to fail). All three
  categories were deliberately *not* implemented with fabricated trigger conditions, per the
  no-stub/no-invented-business-rules principle -- an exception type with no real,
  state-driven trigger condition is worse than an honestly-documented gap.
