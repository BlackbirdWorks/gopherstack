---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: cognitoidentity
sdk_module: aws-sdk-go-v2/service/cognitoidentity@v1.33.20
last_audit_commit: 659c9617
last_audit_date: 2026-07-13
overall: A                # 2 genuine wire-shape bugs found and fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateIdentityPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed OpenIdConnectProviderARNs JSON key casing"}
  DeleteIdentityPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades identities/roles/principalTags"}
  DescribeIdentityPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed OpenIdConnectProviderARNs JSON key casing"}
  ListIdentityPools: {wire: ok, errors: ok, state: ok, persist: ok, note: "name-cursor pagination verified"}
  UpdateIdentityPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed OpenIdConnectProviderARNs JSON key casing"}
  GetId: {wire: ok, errors: ok, state: ok, persist: ok, note: "merges logins into existing identity per AWS semantics"}
  GetCredentialsForIdentity: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed Expiration epoch-seconds vs epoch-millis bug; synthetic creds are an accepted simplification"}
  GetOpenIdToken: {wire: ok, errors: ok, state: ok, persist: n/a}
  SetIdentityPoolRoles: {wire: ok, errors: ok, state: ok, persist: ok, note: "partial-merge semantics (omitted keys preserved) — deliberately tested by prior Refinement2 pass, left as-is"}
  GetIdentityPoolRoles: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIdentities: {wire: ok, errors: ok, state: ok, persist: ok, note: "best-effort silent skip of missing IDs matches AWS"}
  DescribeIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "timestamps now routed through pkgs/awstime.Epoch"}
  GetOpenIdTokenForDeveloperIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "PrincipalTags input field accepted by SDK but not consumed (see gaps)"}
  GetPrincipalTagAttributeMap: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIdentities: {wire: ok, errors: ok, state: ok, persist: ok, note: "timestamps now routed through pkgs/awstime.Epoch"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  LookupDeveloperIdentity: {wire: ok, errors: ok, state: ok, persist: n/a}
  MergeDeveloperIdentities: {wire: ok, errors: ok, state: ok, persist: ok}
  SetPrincipalTagAttributeMap: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UnlinkDeveloperIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  UnlinkIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families: {}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - GetOpenIdTokenForDeveloperIdentity accepts a PrincipalTags request field (SDK-modeled) but the backend never stores/applies it; our OpenID tokens are synthetic strings, not real JWTs with claims, so there is nowhere to embed the tags. Low priority (niche custom-provider attribute-mapping feature).
  - SetIdentityPoolRoles/GetOpenIdTokenForDeveloperIdentity TokenDuration are accepted/validated but not enforced against issued token lifetime (tokens are opaque synthetic strings, not real expiring JWTs).
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - HTTP status code choice for NotAuthorizedException (403 here) vs AWS's actual per-exception status (SDK error-type resolution is body-driven, not status-code-driven, so this doesn't break aws-sdk-go-v2 clients; only relevant to tooling that inspects raw HTTP status).
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
