---
service: rolesanywhere
sdk_module: aws-sdk-go-v2/service/rolesanywhere@v1.23.0
last_audit_commit: 59739a9e
last_audit_date: 2026-07-13
overall: A            # real fixes found and applied this pass
ops:
  CreateTrustAnchor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: enabled field was ignored, always created enabled; now honors request enabled (default true)"}
  GetTrustAnchor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes notificationSettings"}
  ListTrustAnchors: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes notificationSettings per item"}
  DeleteTrustAnchor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response was an empty envelope; now returns {trustAnchor: {...}} matching AWS DeleteTrustAnchorResponse"}
  UpdateTrustAnchor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes notificationSettings"}
  EnableTrustAnchor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes notificationSettings"}
  DisableTrustAnchor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes notificationSettings"}
  CreateProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes attributeMappings (empty at create)"}
  GetProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes attributeMappings"}
  ListProfiles: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes attributeMappings per item"}
  DeleteProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response was an empty envelope; now returns {profile: {...}} matching AWS DeleteProfileResponse"}
  UpdateProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes attributeMappings"}
  EnableProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes attributeMappings"}
  DisableProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now includes attributeMappings"}
  ImportCrl: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCrl: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCrls: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCrl: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCrl: {wire: ok, errors: ok, state: ok, persist: ok, note: "already correctly returned {crl: {...}} pre-audit; used as the reference pattern for the Delete*TrustAnchor/Profile fix"}
  EnableCrl: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableCrl: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSubject: {wire: ok, errors: ok, state: partial, persist: ok, note: "store is never populated (see gaps: no CreateSession)"}
  ListSubjects: {wire: ok, errors: ok, state: partial, persist: ok, note: "store is never populated (see gaps: no CreateSession)"}
  PutAttributeMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAttributeMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  PutNotificationSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  ResetNotificationSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: read resourceArn/tagKeys from query string; real wire shape is a JSON POST body -- op was a complete no-op against any real SDK client"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "resourceArn is a GET query param (verified against serializer, not the task brief's body assumption)"}
families:
  trustAnchor_crud: {status: ok, note: "route matcher + PATCH/POST/GET/DELETE methods verified against serializers.go opPath/Method for every op; all match"}
  profile_crud: {status: ok, note: "same verification as trustAnchor_crud"}
  crl_crud: {status: ok, note: "DeleteCrl was already correct; used as reference for the two Delete fixes"}
  tags: {status: ok, note: "UntagResource wire-shape bug fixed; TagResource/ListTagsForResource were already correct"}
gaps:
  - "GetSubject/ListSubjects: subjects store is never populated -- there is no CreateSession endpoint in this service (AWS Roles Anywhere's session-vending API is a separate mTLS-authenticated data-plane API, not SigV4/control-plane, and was out of scope for this audit). SubjectDetail's Credentials/InstanceProperties fields are also unmodeled. Would need its own audit pass if CreateSession is ever added to gopherstack."
  - "CreateProfile/UpdateProfile ignore the real acceptRoleSessionName field entirely (not modeled in the Profile struct); low impact since it only affects the separate CreateSession data plane, which gopherstack doesn't implement."
  - "TrustAnchorDetail/ProfileDetail's createdBy field (AWS account that created the resource) is not populated; cosmetic, single-account emulator."
  - "CreateTrustAnchor accepts a notificationSettings field in the real request (set notifications at creation time); gopherstack silently drops it -- callers must use the separate PutNotificationSettings op after create instead. Not fixed this pass (kept scope to the enabled-field bug, which was the higher-severity ignored-input gap)."
  - "No tag-count validation (TooManyTagsException) or AccessDeniedException paths -- these are generic AWS exceptions with no evidence they're actually exercised by common client flows against this service; not treated as a stub violation."
leaks: {status: clean, note: "no goroutines/janitors in this service; locking is via the shared lockmetrics.RWMutex per pkgs-catalog rule, single lock, no nesting introduced by this pass's added GetNotificationSettings/GetAttributeMappings calls (each backend call takes and releases the lock independently, no re-entrant locking)"}
---

## Notes

Protocol: restJson1. Verified route/method/path for every operation directly against
`aws-sdk-go-v2/service/rolesanywhere@v1.23.0`'s `serializers.go` (`SplitURI` opPath +
`request.Method` per op) -- all of gopherstack's `parseRESTPath`/`parseEntityPath`/
`parseTagPaths` switch cases matched the real SDK exactly (including the somewhat
surprising ones: `ListTagsForResource` is GET with `resourceArn` as a query param, not
POST with a body like Tag/UntagResource).

**Real bugs fixed this pass:**

1. **`UntagResource` wire-shape bug (handler.go)** -- the real AWS client serializes
   `UntagResourceInput` (`resourceArn`, `tagKeys`) as a JSON POST body (confirmed via
   `awsRestjson1_serializeOpDocumentUntagResourceInput` in the SDK's `serializers.go`),
   not query parameters. gopherstack's `handleUntagResource` was parsing `c.Request().
   URL.RawQuery` instead, so tagKeys/resourceArn were *always* empty against a real SDK
   call -- this made UntagResource a complete no-op for any real client, silently
   returning 200 without removing anything. `TagResource` and `ListTagsForResource` were
   already correct (body and query-param respectively, matching the SDK).

2. **`DeleteTrustAnchor`/`DeleteProfile` returned an empty envelope.** AWS's
   `DeleteTrustAnchorResponse`/`DeleteProfileResponse` both carry the deleted resource
   (confirmed via `awsRestjson1_deserializeOpDocumentDeleteTrustAnchorOutput` /
   `...DeleteProfileOutput` in the SDK). `DeleteCrl` already did this correctly in
   gopherstack (returns `{crl: {...}}`); TrustAnchor/Profile did not. Backend interface
   signatures changed from `Delete*(ctx, id) error` to `Delete*(ctx, id) (*T, error)`,
   snapshotting state immediately before removal (same pattern as the existing
   `DeleteCrl`).

3. **`CreateTrustAnchor` ignored the request's `enabled` field**, always creating trust
   anchors as enabled regardless of what the caller sent. `CreateTrustAnchorInput.
   Enabled` is a real, optional field (confirmed in the SDK's request struct and
   `awsRestjson1_serializeOpDocumentCreateTrustAnchorInput`). Backend now takes
   `enabled *bool` (nil defaults to true, matching `ImportCrl`'s existing enabled-default
   pattern).

4. **`notificationSettings`/`attributeMappings` were only visible via the dedicated
   `Put*`/`Reset*`/`Delete*Mapping` responses**, never via `Get`/`List`/`Create`/
   `Update`/`Enable`/`Disable`. AWS's `TrustAnchorDetail.notificationSettings` and
   `ProfileDetail.attributeMappings` are read on *every* detail response (confirmed
   field-by-field in `awsRestjson1_deserializeDocumentTrustAnchorDetail`/
   `...ProfileDetail`) -- settings/mappings that were stored via one op but invisible
   from every other read were a disguised-gap in the persisted state's visibility. Added
   `Handler.trustAnchorJSON`/`Handler.profileJSON` helpers that every trust-anchor/
   profile handler now routes through instead of the bare `trustAnchorToJSON`/
   `profileToJSON`.

**Traps for the next auditor (looks-wrong-but-correct):**

- Timestamps use `time.RFC3339` (via `Format(time.RFC3339)`), not the SDK's exact
  millisecond output format (`2006-01-02T15:04:05.999Z`, per `smithy-go/time.
  FormatDateTime`). This is NOT a bug: `smithytime.ParseDateTime` (the client-side
  parser) accepts both `time.RFC3339` and `time.RFC3339Nano` as fallbacks, so
  gopherstack's output round-trips fine through the real SDK client.
- `errBody` returns `{"__type": ..., "message": ...}` with no `X-Amzn-ErrorType` header.
  This is also NOT a bug: the SDK's error deserializer (`awsRestjson1_deserializeOpError*`)
  falls back to the JSON body's type field via `restjson.GetErrorInfo` when the header is
  absent.
- `go.Blob` fields (`crlData`) are plain `[]byte` in the Go struct/JSON tag; Go's
  `encoding/json` already base64-encodes `[]byte` on marshal, matching the SDK's
  base64-string wire representation with zero extra code.
