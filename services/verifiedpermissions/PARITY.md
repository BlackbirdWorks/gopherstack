service: verifiedpermissions
sdk_module: aws-sdk-go-v2/service/verifiedpermissions@v1.31.4
last_audit_commit: b9f40060
last_audit_date: 2026-07-23
overall: A            # this pass: 1 critical evaluation bug + ~12 wire-shape bugs fixed op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreatePolicyStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: dropped the invented validationSettings field from the response (real CreatePolicyStoreOutput has none); ClientToken idempotency now implemented (8h window, same-token/same-params replays, same-token/different-params -> ConflictException)"}
  GetPolicyStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: optional cedarVersion field now populated (always CEDAR_4 -- gopherstack's cedar-go engine implements Cedar 4)"}
  ListPolicyStores: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: dropped invented validationSettings/deletionProtection fields from PolicyStoreItem (real item shape is leaner: arn/createdDate/policyStoreId/description/lastUpdatedDate only)"}
  UpdatePolicyStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: added missing required createdDate field; dropped invented validationSettings field (real UpdatePolicyStoreOutput has neither)"}
  DeletePolicyStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade now also clears resourceTags for every deleted child resource + the store itself, and clears policySetCache/policySetDirty for the store (previously only arnIndex was cleaned, leaving ghost tag-map rows and an unbounded policy-set cache)"}
  CreatePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: CreatePolicyOutput now echoes effect/actions/principal/resource (STATIC: parsed from the policy's Cedar scope clause; TEMPLATE_LINKED: effect/actions from the referenced template's statement, principal/resource from the policy's own binding) -- these 4 real response fields were entirely missing before. ClientToken idempotency now implemented."}
  GetPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: same effect/actions/principal/resource fix as CreatePolicy -- STATIC policies now echo principal/resource/effect/actions parsed from their Cedar scope clause via a new Cedar-JSON-format scope parser (policy_scope.go), closing last pass's documented gap"}
  ListPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): ListPolicies' STATIC definition item was echoing the full Cedar statement text -- the real SDK's StaticPolicyDefinitionItem (unlike GetPolicy's StaticPolicyDefinitionDetail) carries ONLY description, never the statement. Also gained the same effect/actions/principal/resource top-level fields as CreatePolicy/GetPolicy. FIXED last pass: filter.principal/resource wire as the EntityReference union."}
  UpdatePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: same effect/actions/principal/resource fix as CreatePolicy"}
  DeletePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: now also clears the deleted policy's resourceTags entry (was leaking a ghost tag-map row after delete)"}
  CreatePolicyTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "ClientToken idempotency now implemented"}
  GetPolicyTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicyTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePolicyTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePolicyTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (state bug, not wire): the real SDK documents that DeletePolicyTemplate \"also deletes any policies that were created from the specified policy template\" -- gopherstack previously deleted only the template row, leaving every TEMPLATE_LINKED policy referencing it as a dangling reference (visible via GetPolicy/ListPolicies, silently dropped from Cedar evaluation). Now cascade-deletes those policies (row + arnIndex + resourceTags) and invalidates the store's compiled policy-set cache."}
  PutSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  IsAuthorized: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass, CRITICAL): buildCedarPolicySet only ever compiled STATIC policies into the evaluated Cedar PolicySet -- every TEMPLATE_LINKED policy was silently skipped during evaluation, meaning a template-linked permit policy could never actually ALLOW anything (the core value proposition of policy templates). Now every policy's effective statement is resolved (STATIC: its own statement; TEMPLATE_LINKED: the referenced template's statement with ?principal/?resource substituted) before compiling the policy set. Cedar evaluation itself remains the real cedar-go engine. Last pass's determiningPolicies/errors object-array fix carries forward correctly."}
  IsAuthorizedWithToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "IMPROVED: principalFromToken now matches the token's \"iss\" claim against each identity source's issuer (OIDC OpenIDIssuer, or the issuer AWS derives from a Cognito user pool ARN) and picks the matching source, falling back to the first source only when there's no iss claim or no match -- was previously always the first identity source regardless of the token's issuer. aud/client_id matching and JWT signature verification remain out of scope (documented simplification, not a wire-shape bug). Same TEMPLATE_LINKED evaluation fix as IsAuthorized applies here too."}
  BatchIsAuthorized: {wire: ok, errors: ok, state: ok, persist: ok, note: "same TEMPLATE_LINKED evaluation fix as IsAuthorized (shared buildCedarPolicySet)"}
  BatchIsAuthorizedWithToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "same TEMPLATE_LINKED evaluation + issuer-matching fixes"}
  BatchGetPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed BatchGetPolicyOutputItem correctly has NO top-level effect/actions/principal/resource fields (unlike GetPolicy/ListPolicies) -- verified against the real SDK type, left as-is"}
  CreateIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): CreateIdentitySourceOutput was echoing the full principalEntityType + configuration back -- the real output shape is minimal (identitySourceId/policyStoreId/timestamps only); those two fields don't exist on the real CreateIdentitySourceOutput type. ClientToken now wired to idempotency (was parsed but silently discarded before). Last pass: identityTokenOnly.clientIds + cognitoUserPoolConfiguration.issuer fixes carry forward."}
  GetIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass -- the fuller identitySourceOutput shape (with principalEntityType + configuration) IS correct here, matching the real GetIdentitySourceOutput"}
  ListIdentitySources: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass -- IdentitySourceItem's fuller shape also correctly includes principalEntityType + configuration"}
  UpdateIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): same over-eager-echo bug as CreateIdentitySource -- UpdateIdentitySourceOutput was echoing principalEntityType, a field the real UpdateIdentitySourceOutput doesn't have (minimal id/policyStoreId/timestamps shape, same as Create)"}
  DeleteIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: now also clears the deleted identity source's resourceTags entry"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
gaps:                     # known divergences NOT fixed
  - "IsAuthorizedWithToken/BatchIsAuthorizedWithToken: principalFromToken matches the token's \"iss\" claim against configured identity sources (improved this pass) but does not additionally match \"aud\"/\"client_id\" against the source's configured client IDs/audiences, nor verify the JWT signature. Documented simplification of the identity-source-selection logic, not a wire-shape bug."
deferred: []              # the one item deferred last pass (CreatePolicyStore ClientToken) is now implemented; see ops notes
leaks: {status: clean, note: "no goroutines/janitors in this service; InMemoryBackend uses a single lockmetrics.RWMutex. This pass fixed real ghost-row leaks: DeletePolicy/DeleteIdentitySource/DeletePolicyStore's cascade/DeletePolicyTemplate's new cascade all now clear resourceTags (previously only arnIndex was cleaned, so a tagged-then-deleted resource left its tag map entry behind forever); DeletePolicyStore also now clears policySetCache/policySetDirty for the deleted store. clientTokens (new: ClientToken idempotency state) is an ephemeral, never-persisted map matching the pattern already used by policySetCache/policySetDirty -- entries age out via the 8h idempotencyWindow check at lookup time (no janitor goroutine, consistent with this service's no-goroutines design), so it is a bounded, self-limiting resource under any bounded call pattern. Snapshot/Restore fully exercised by existing persistence_test.go plus this pass's new tests."}

## Notes

Protocol: awsjson1.0 (`application/x-amz-json-1.0`, `X-Amz-Target: VerifiedPermissions.<Op>`),
correctly matched by `RouteMatcher`/`ExtractOperation` (targetPrefix check).

Timestamps (`createdDate`/`lastUpdatedDate`/schema dates) are ISO-8601 strings
(`smithytime.ParseDateTime` on the real client side), NOT epoch-seconds numbers --
gopherstack's `timeFormat = "2006-01-02T15:04:05.000Z"` + `.UTC().Format(...)` is correct
as-is. This is a "looks-wrong-but-correct" trap: don't reflexively reach for
`pkgs/awstime.Epoch` here, this service is one of the ISO8601-not-epoch awsjson1.0 services.

## This pass's findings (2026-07-23 re-audit)

The prior pass (2026-07-13) marked every op `ok` and left three items as documented
"gaps" plus one "deferred". Re-auditing turned up one **critical evaluation bug** the
prior pass's op-by-op wire/error review didn't catch (it's a state/behavior bug, not a
wire-shape or error-code one), plus resolved all three gaps and the one deferred item,
plus found several more wire-shape bugs while implementing the gap fixes (field-diffing
CreatePolicy/UpdatePolicy/ListPolicies/CreateIdentitySource/UpdateIdentitySource/
ListPolicyStores/CreatePolicyStore/UpdatePolicyStore against the real SDK types surfaced
divergences the prior pass's per-op review missed).

### Critical: TEMPLATE_LINKED policies were never evaluated (fixed)

`buildCedarPolicySet` (authorization.go) only ever compiled `STATIC` policies into the
Cedar `PolicySet` used by `IsAuthorized`/`IsAuthorizedWithToken`/`BatchIsAuthorized*`:

```go
for _, p := range policies {
    if p.PolicyType != policyTypeStatic || p.Statement == "" {
        continue   // TEMPLATE_LINKED policies always hit this and were skipped
    }
    ...
}
```

A `TEMPLATE_LINKED` policy could be created, retrieved, listed, and deleted correctly,
but it never actually participated in an authorization decision -- a template-linked
`permit` policy could never cause `ALLOW`. Since policy templates exist specifically to
let one Cedar statement (with `?principal`/`?resource` placeholders) back many
concrete, callable policies, this silently broke the core value proposition of the
template feature for every template-linked policy in every policy store.

Fixed by adding `instantiateTemplate` (policy_scope.go): given a template-linked
policy's bound principal/resource, it substitutes the `?principal`/`?resource` tokens
in the referenced template's statement with concrete `EntityType::"id"` literals,
producing a normal, parseable Cedar statement. `buildCedarPolicySet` now resolves
every policy's effective statement this way (via `resolveStatementLocked`) regardless
of type, so template-linked policies are compiled into the evaluated policy set exactly
like static ones. A template whose placeholder isn't bound (e.g. an omitted
`?resource`) simply fails to parse and is skipped, matching the previous silent-skip
behavior for that one malformed-reference edge case rather than erroring the whole
evaluation.

### DeletePolicyTemplate didn't cascade-delete linked policies (fixed)

The real SDK documents: *"This operation also deletes any policies that were created
from the specified policy template. Those policies are immediately removed from all
future API responses..."* -- gopherstack's `DeletePolicyTemplate` deleted only the
template row, leaving every `TEMPLATE_LINKED` policy referencing it as a dangling
reference (still visible via `GetPolicy`/`ListPolicies`/`BatchGetPolicy`, and -- after
the fix above -- simply skipped during evaluation since `resolveStatementLocked`
treats a missing template as an empty statement). Fixed to cascade-delete every
`TEMPLATE_LINKED` policy referencing the deleted template (row + ARN index + tags),
leaving `STATIC` policies in the same store untouched, and invalidates the store's
compiled policy-set cache.

### STATIC policy scope introspection: GetPolicy/ListPolicies/CreatePolicy/UpdatePolicy (fixed)

The real SDK's `GetPolicyOutput`/`PolicyItem` (ListPolicies)/`CreatePolicyOutput`/
`UpdatePolicyOutput` all carry `effect`/`actions`/`principal`/`resource` fields parsed
from a policy's Cedar scope clause (`permit(principal == User::"alice", action ==
Action::"view", resource); `-> `effect: "Permit"`, `principal: {entityType: "User",
entityId: "alice"}`, `actions: [{actionType: "Action", actionId: "view"}]`).
gopherstack previously populated `principal`/`resource` only for `TEMPLATE_LINKED`
policies (from their explicit binding) and never populated `effect`/`actions` for
either type.

Fixed by adding a small Cedar-JSON-format scope parser (`policy_scope.go`):
`parseCedarScope` reparses a policy's Cedar statement via `cedar-go`'s own
`MarshalJSON` (the stable, spec'd [Cedar JSON policy
format](https://docs.cedarpolicy.com/policies/json-format.html)) and extracts the
`effect`/`principal`/`action`/`resource` scope clauses. An `==` or single-entity `in`
scope yields a concrete `EntityIdentifier`; an `is`/`is..in` (entity-type-only) or
unconstrained (`All`) scope yields nothing for that field, matching AWS's documented
"isn't included in the response when [it] isn't present in the policy content"
behavior. For `TEMPLATE_LINKED` policies, `effect`/`actions` are now derived the same
way from the *referenced template's* statement (with slots substituted), while
`principal`/`resource` continue to come from the policy's own explicit binding (not
re-parsed from the slot-bearing scope clause, which has no concrete entity to extract).

### ListPolicies' STATIC item was leaking the full Cedar statement (fixed)

The real SDK's `StaticPolicyDefinitionItem` (used by `ListPolicies`' `PolicyItem`) has
only a `description` field -- unlike `GetPolicy`/`BatchGetPolicy`'s
`StaticPolicyDefinitionDetail`, it does **not** include the Cedar statement text.
gopherstack's `ListPolicies` was echoing the full statement in every item (reusing the
same definition type as `GetPolicy`). Fixed by splitting `policyDefinitionOut` (full
detail, used by Get/BatchGet) from a new `policyDefinitionItemOut` (item shape, used by
List) that omits the statement for `STATIC` policies.

### Invented fields removed from PolicyStore responses (fixed)

Field-diffing all four policy-store response shapes against the real SDK found three
invented fields and one missing required one:
- `CreatePolicyStoreOutput` had no `validationSettings` at all -- gopherstack echoed it. Removed.
- `PolicyStoreItem` (`ListPolicyStores`) has no `validationSettings`/`deletionProtection`
  -- gopherstack echoed both. Removed.
- `UpdatePolicyStoreOutput` had no `validationSettings` -- removed -- but **is** missing
  gopherstack's previous response: it lacked the *required* `createdDate` field. Added.
- `GetPolicyStoreOutput`'s optional `cedarVersion` field (Cedar v4 FAQ) was never
  populated; now always `"CEDAR_4"` (gopherstack's `cedar-go` engine implements Cedar 4).

### Invented fields removed from CreateIdentitySource/UpdateIdentitySource (fixed)

`CreateIdentitySourceOutput` and `UpdateIdentitySourceOutput` are both minimal shapes
(id/policyStoreId/timestamps only) in the real SDK -- unlike `GetIdentitySource`/
`ListIdentitySources`' fuller item shape, neither echoes `principalEntityType` or
`configuration`. gopherstack returned the fuller shape from all four ops. Fixed by
giving Create/Update their own minimal `identitySourceIDsOutput` type, leaving
Get/List's fuller `identitySourceOutput` unchanged (verified correct against the real
SDK).

### ClientToken idempotency implemented (was deferred)

All four `Create*` ops that accept a `ClientToken` (`CreatePolicyStore`,
`CreatePolicy`, `CreatePolicyTemplate`, `CreateIdentitySource`) now implement the real
SDK's documented eight-hour idempotency window: a retry with the same `ClientToken`
and the same parameters replays the original resource (no duplicate created); a retry
with the same token but *different* parameters fails with `ConflictException`, per
each op's `ClientToken` doc ("If you retry the operation with the same ClientToken, but
with different parameters, the retry fails with an ConflictException error"). Backed by
a shared `checkClientToken`/`recordClientToken` pair (store.go) keyed by
`"<op>:<token>"`, storing a deterministic per-op parameter fingerprint; entries age out
lazily at the 8h window boundary rather than via a background janitor (consistent with
this service's no-goroutines design).

### IsAuthorizedWithToken: principal resolution now matches by issuer (improved)

`principalFromToken` previously always used the *first* identity source in the policy
store to resolve a token's principal type, regardless of which identity source the
token actually came from. With more than one identity source configured, a token from
the second (or later) source would be resolved against the first source's principal
type -- wrong whenever the two sources map to different Cedar entity types. Improved to
match the token's `iss` claim against each identity source's issuer (OIDC's own
`OpenIDIssuer`, or the issuer AWS derives from a Cognito user pool's ARN), falling back
to the first source only when there's no `iss` claim or no match. `aud`/`client_id`
matching and JWT signature verification remain out of scope (see gaps).

No other disguised no-ops found: policy/template/identity-source/schema state is real
(backed by `pkgs/store.Table`/`Index`), Cedar evaluation uses the real `cedar-go` engine
end-to-end (including template instantiation, after this pass's fix), and
`Snapshot`/`Restore` round-trip all five tables (including the "dirty" schemas table via
its DTO wrapper) correctly -- verified by existing `persistence_test.go` plus this
pass's new tests exercising every fix end-to-end through the HTTP handler.

### Prior pass (2026-07-13): six wire-shape/error-code bugs fixed

1. `determiningPolicies`/`errors` bare-string-array bug (IsAuthorized family) -- the real
   SDK's deserializers require each array element to be a JSON object
   (`{"policyId": "..."}` / `{"errorDescription": "..."}`).
2. `BatchIsAuthorized(WithToken)` `results[].request` flat-vs-nested bug -- now nests as
   `principal: {entityType, entityId}` / `action: {actionType, actionId}` / ....
3. OIDC `identityTokenOnly` field-name bug -- real SDK uses `clientIds` for
   `identityTokenOnly`, `audiences` for `accessTokenOnly` (different names per union member).
4. Missing `cognitoUserPoolConfiguration.issuer` (required response field, derived from
   the user pool ARN).
5. `ListIdentitySources` `filters` silently ignored (never threaded to the backend).
6. Wrong exception names: `ConflictException` was wired as `"ResourceConflictException"`;
   `TagResource`'s tag-count overflow used `ValidationException` instead of `TooManyTagsException`.

Also fixed that pass: `ListPolicies`' `filter.principal`/`filter.resource` used a flat
`entityIdentifier` instead of the real SDK's `EntityReference` union.
