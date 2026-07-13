service: verifiedpermissions
sdk_module: aws-sdk-go-v2/service/verifiedpermissions@v1.31.4
last_audit_commit: b9f40060
last_audit_date: 2026-07-13
overall: A            # 6 genuine wire/logic bugs fixed op-by-op against the real SDK
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreatePolicyStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "extra (harmless) validationSettings field in response vs real SDK; real SDK also lacks CreatePolicyStoreOutput fields we don't emit -- none required-but-missing"}
  GetPolicyStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "missing optional CedarVersion field (SDK-optional, not required) -- deferred, see gaps"}
  ListPolicyStores: {wire: ok, errors: ok, state: ok, persist: ok, note: "PolicyStoreItem has extra (harmless) validationSettings/deletionProtection fields vs real SDK's leaner item shape"}
  UpdatePolicyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePolicyStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: deletion-protection conflict now wires as ConflictException (was ResourceConflictException, not a real AWS exception name)"}
  CreatePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "STATIC policies don't echo top-level principal/resource parsed from the Cedar statement's scope clause -- deferred, see gaps"}
  ListPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: filter.principal/resource now wire as the EntityReference union ({identifier:{...}} / {unspecified:true}), was a flat entityIdentifier the real SDK never sends that shape for"}
  UpdatePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePolicyTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicyTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicyTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePolicyTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePolicyTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  PutSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  IsAuthorized: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: determiningPolicies/errors were bare string arrays; real SDK deserializer requires objects ({policyId:...}/{errorDescription:...}) and would hard-fail parsing any non-empty response. Cedar evaluation itself is cedar-go (real engine, not simplified)."}
  IsAuthorizedWithToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "same determiningPolicies/errors fix as IsAuthorized. principalFromToken always uses the FIRST identity source in the store and ignores token issuer/aud matching -- documented simplification, see gaps"}
  BatchIsAuthorized: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: same determiningPolicies/errors object-array fix, plus each result's echoed request.principal/action/resource now nest as objects (BatchIsAuthorizedInputItem shape) instead of the flat internal AuthorizationRequest field names"}
  BatchIsAuthorizedWithToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fixes as BatchIsAuthorized"}
  BatchGetPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: (1) openIdConnectConfiguration.tokenSelection.identityTokenOnly now wires clientIds (was audiences, silently dropping client-ID restrictions on ID-token sources and never round-tripping accepted client IDs back to callers); (2) cognitoUserPoolConfiguration.issuer (required response field) now populated, derived from userPoolArn"}
  GetIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same identityTokenOnly/issuer fixes as CreateIdentitySource (shared JSON types)"}
  ListIdentitySources: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: wire 'filters' list (principalEntityType) was parsed into the input struct but never passed to the backend -- always returned every identity source regardless of filter"}
  UpdateIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same identityTokenOnly/issuer fixes"}
  DeleteIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: tag-count-exceeded now wires as TooManyTagsException (the only exception the real SDK declares for TagResource's tag limit); CreatePolicyStore's own tag-count overflow correctly stays ValidationException per its declared error set"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
gaps:                     # known divergences NOT fixed
  - "GetPolicy/ListPolicies/BatchGetPolicy: STATIC policy views don't echo top-level principal/resource parsed from the Cedar statement's scope clause (AWS parses the actual permit/forbid scope; gopherstack only populates principal/resource for TEMPLATE_LINKED policies). Fixing this needs Cedar AST scope introspection via cedar-go, out of scope for this pass."
  - "IsAuthorizedWithToken/BatchIsAuthorizedWithToken: principalFromToken always resolves the principal type from the FIRST identity source in the policy store and does not match the token's issuer/aud against a specific identity source, nor verify the JWT signature. Documented simplification of the identity-source-selection logic, not a wire-shape bug."
  - "GetPolicyStore/ListPolicyStores: optional CedarVersion field (Cedar v4 FAQ) never populated -- SDK-optional field, no client breakage, low priority."
deferred:                 # consciously not audited this pass (scope) -- next pass targets
  - CreatePolicyStore ClientToken idempotency semantics (real AWS treats a retried ClientToken with different params as ConflictException; gopherstack ignores ClientToken entirely)
leaks: {status: clean, note: "no goroutines/janitors in this service; InMemoryBackend uses a single lockmetrics.RWMutex, Snapshot/Restore fully exercised by existing persistence_test.go plus this pass's new tests"}

## Notes

Protocol: awsjson1.0 (`application/x-amz-json-1.0`, `X-Amz-Target: VerifiedPermissions.<Op>`),
correctly matched by `RouteMatcher`/`ExtractOperation` (targetPrefix check).

Timestamps (`createdDate`/`lastUpdatedDate`/schema dates) are ISO-8601 strings
(`smithytime.ParseDateTime` on the real client side), NOT epoch-seconds numbers --
gopherstack's `timeFormat = "2006-01-02T15:04:05.000Z"` + `.UTC().Format(...)` is correct
as-is. This is a "looks-wrong-but-correct" trap: don't reflexively reach for
`pkgs/awstime.Epoch` here, this service is one of the ISO8601-not-epoch awsjson1.0 services.

Six real bugs fixed this pass (all in `handler.go`/`backend.go`, wire-shape and error-code
level, no state-management bugs found):

1. **`determiningPolicies`/`errors` bare-string-array bug** (IsAuthorized, IsAuthorizedWithToken,
   BatchIsAuthorized, BatchIsAuthorizedWithToken) -- the real SDK's `DeterminingPolicyItem`/
   `EvaluationErrorItem` deserializers require each array element to be a JSON object
   (`{"policyId": "..."}` / `{"errorDescription": "..."}`); gopherstack emitted bare strings,
   which would make `awsAwsjson10_deserializeDocumentDeterminingPolicyItem` (or the error-item
   equivalent) hard-fail with `"unexpected JSON type"` on any non-empty response -- i.e. a real
   SDK client's `IsAuthorized` call would error out whenever a policy actually matched. Highest
   severity finding in this audit; this is the core value proposition of the service.
2. **BatchIsAuthorized(WithToken) `results[].request` flat-vs-nested bug** -- the echoed request
   used gopherstack's internal flat `AuthorizationRequest` JSON shape
   (`principalEntityType`/`actionType`/...) instead of the real SDK's `BatchIsAuthorizedInputItem`
   nested shape (`principal: {entityType, entityId}`, `action: {actionType, actionId}`, ...). A
   real client's per-item echoed request would silently deserialize to nil Principal/Action/Resource.
3. **OIDC `identityTokenOnly` field-name bug** -- the real SDK uses `clientIds` for
   `identityTokenOnly` and `audiences` for `accessTokenOnly` (different field names per union
   member); gopherstack used `audiences` for both. Requests configuring an ID-token identity
   source with client-ID restrictions silently lost that data (json field never matched), and
   responses never echoed it back correctly either.
4. **Missing `cognitoUserPoolConfiguration.issuer`** -- a required response field the real
   service derives from the user pool ARN (`https://cognito-idp.<region>.amazonaws.com/<poolId>`);
   gopherstack never populated it.
5. **`ListIdentitySources` filters silently ignored** -- the wire `filters` (principalEntityType)
   list was never threaded through to the backend; every call returned all identity sources.
6. **Wrong exception names**: `ConflictException` was wired as `"ResourceConflictException"`
   (not a real AWS Verified Permissions exception at all), and TagResource's tag-count overflow
   used `ValidationException` instead of the real SDK's `TooManyTagsException` (the only
   exception TagResource declares for that condition; CreatePolicyStore's own tag-count overflow
   correctly has no TooManyTagsException in its error model and keeps ValidationException).

Also fixed as part of the same audit: `ListPolicies`' `filter.principal`/`filter.resource` used
a flat `entityIdentifier` instead of the real SDK's `EntityReference` union
(`{"identifier": {...}}` / `{"unspecified": true}`), which silently no-op'd any principal/resource
filter a real client sent.

No disguised no-ops found beyond the above: policy/template/identity-source/schema state is real
(backed by `pkgs/store.Table`/`Index`), Cedar evaluation uses the real `cedar-go` engine (not a
stubbed decision), and `Snapshot`/`Restore` round-trip all five tables (including the "dirty"
schemas table via its DTO wrapper) correctly -- verified by existing `persistence_test.go` plus
this pass's new tests exercising the fixed wire shapes end-to-end through the HTTP handler.
