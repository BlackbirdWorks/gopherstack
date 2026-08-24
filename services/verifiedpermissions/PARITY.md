service: verifiedpermissions
sdk_module: aws-sdk-go-v2/service/verifiedpermissions@v1.36.4
last_audit_commit: 92bc04738
last_audit_date: 2026-08-20
overall: A            # this pass (wrapper-key/nested-shape sweep, gopherstack-c733): field-diffed every response struct against the pinned SDK's types.go, with special attention to this service's union families (PolicyDefinition/Detail/Item, Configuration/Detail/Item, OpenIdConnectTokenSelection/Detail/Item, EntityReference) since this campaign hadn't yet stress-tested a union-heavy service. Found and fixed two real bugs: ListPolicyTemplates leaking a fabricated "statement" field (pattern: member generalized from GetPolicyTemplateOutput's wider sibling shape), and BatchGetPolicy mis-coding an unresolvable-alias failure as POLICY_STORE_NOT_FOUND instead of the real SDK's dedicated POLICY_STORE_ALIAS_NOT_FOUND value (right key, wrong value). Every union discriminator key/casing, every summary/full pair, and both three-way families verified correct against deserializers.go/serializers.go -- no other wire bugs found. No regressions in prior fixes (all 159 tests still pass).
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
  CreatePolicyTemplate: {wire: fixed, errors: ok, state: ok, persist: ok, note: "ClientToken idempotency now implemented. 2026-08-22 (gopherstack-tpu3): CreatePolicyTemplateInput.Name (api_op_CreatePolicyTemplate.go:94) was never read by this handler at all -- Name now threaded through to the backend and stored (also folded into the idempotency fingerprint, since a retried token with a different Name must not silently keep the old one). Not echoed on CreatePolicyTemplateOutput itself: the real Output carries no Name member (only CreatedDate/LastUpdatedDate/PolicyStoreId/PolicyTemplateId), confirmed against the SDK struct."}
  GetPolicyTemplate: {wire: fixed, errors: ok, state: ok, persist: ok, note: "2026-08-22 (gopherstack-tpu3): GetPolicyTemplateOutput requires a \"name\" key (deserializers.go:9615, awsAwsjson10_deserializeOpDocumentGetPolicyTemplateOutput) that this handler never emitted -- PolicyTemplate had no Name field at all. Fixed; see TestPolicyTemplate_NameRoundTrip."}
  ListPolicyTemplates: {wire: fixed, errors: ok, state: ok, persist: ok, note: "2026-08-22 (gopherstack-tpu3): types.PolicyTemplateItem requires \"name\" too (deserializers.go:7584, awsAwsjson10_deserializeDocumentPolicyTemplateItem) -- same gap as GetPolicyTemplate, now fixed. FIXED (2026-08-20): PolicyTemplateItem was also echoing the full Cedar statement text -- the real SDK's types.PolicyTemplateItem (types/types.go:2121) has no statement field at all (only createdDate/lastUpdatedDate/policyStoreId/policyTemplateId/description/name), unlike GetPolicyTemplateOutput's wider sibling shape which requires one. A typed client can't observe an over-emitted key, so proven with a raw-body absence assertion (TestListPolicyTemplates_ItemHasNoStatement). Correcting a stale note from the 2026-08-22 pass, which claimed the statement key was still emitted (\"harmless...left as-is\") -- verified 2026-08-23 (gopherstack-fg0u) against policyTemplateView (handler_policy_templates.go) and the passing test: the statement field was never present on that struct, so the 2026-08-20 fix's claim was and remains correct."}
  UpdatePolicyTemplate: {wire: fixed, errors: ok, state: ok, persist: ok, note: "2026-08-22 (gopherstack-tpu3): UpdatePolicyTemplateInput.Name (api_op_UpdatePolicyTemplate.go:104) was never read -- fixed, same empty-string-means-unchanged convention this handler already used for Description/Statement. Not echoed on UpdatePolicyTemplateOutput (no Name member there either, same as Create); visible on the next GetPolicyTemplate/ListPolicyTemplates instead."}
  DeletePolicyTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (state bug, not wire): the real SDK documents that DeletePolicyTemplate \"also deletes any policies that were created from the specified policy template\" -- gopherstack previously deleted only the template row, leaving every TEMPLATE_LINKED policy referencing it as a dangling reference (visible via GetPolicy/ListPolicies, silently dropped from Cedar evaluation). Now cascade-deletes those policies (row + arnIndex + resourceTags) and invalidates the store's compiled policy-set cache."}
  PutSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  IsAuthorized: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (this pass, CRITICAL): buildCedarPolicySet only ever compiled STATIC policies into the evaluated Cedar PolicySet -- every TEMPLATE_LINKED policy was silently skipped during evaluation, meaning a template-linked permit policy could never actually ALLOW anything (the core value proposition of policy templates). Now every policy's effective statement is resolved (STATIC: its own statement; TEMPLATE_LINKED: the referenced template's statement with ?principal/?resource substituted) before compiling the policy set. Cedar evaluation itself remains the real cedar-go engine. Last pass's determiningPolicies/errors object-array fix carries forward correctly. FIXED 2026-08-23: context/entities request members are now real -- see the gaps entry below."}
  IsAuthorizedWithToken: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): the response was missing the real SDK's \"principal\" field (IsAuthorizedWithTokenOutput.Principal, EntityIdentifier, optional) -- now echoed when a principal is resolved, omitted otherwise. FIXED (this pass, security-relevant): principalFromToken now also validates the token's aud/client_id claim against the matched identity source's configured client IDs/audiences (cognito-validation.html / oidc-validation.html) -- a token with a mismatched audience no longer resolves a principal from that source (fails closed to no-principal/DENY), closing a real over-authorization gap where a token minted for a different app but the same trusted issuer could previously be ALLOWed. A source with no client IDs/audiences configured keeps accepting any token (real AWS: validation is opt-in). JWT signature verification remains out of scope (needs the issuer's real signing keys); a malformed/unparseable token still fails closed to no-principal/DENY rather than erroring, matching the response schema's principal field being optional (not required), which implies graceful degradation rather than a dedicated exception. principalFromToken still matches the token's \"iss\" claim against each identity source's issuer (OIDC OpenIDIssuer, or the issuer AWS derives from a Cognito user pool ARN), falling back to the first source when there's no iss claim or no match. Same TEMPLATE_LINKED evaluation fix as IsAuthorized applies here too. FIXED 2026-08-23: context/entities now real, see the gaps entry below."}
  BatchIsAuthorized: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same TEMPLATE_LINKED evaluation fix as IsAuthorized (shared buildCedarPolicySet). FIXED 2026-08-23: top-level entities was wrong shape (bare array, not the real {entityList:[...]}/{cedarJson:...} union) and, along with each item's per-request context, was accepted then never threaded into evaluation at all -- see the gaps entry below."}
  BatchIsAuthorizedWithToken: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): response was missing the real SDK's top-level \"principal\" field (BatchIsAuthorizedWithTokenOutput.Principal); also, each result's echoed \"request\" was wrongly including a \"principal\" field -- the real BatchIsAuthorizedWithTokenInputItem (unlike BatchIsAuthorizedInputItem) has no principal member, since the principal comes from the token and is echoed once at the top level instead. Same aud/client_id validation + issuer-matching + TEMPLATE_LINKED evaluation fixes as IsAuthorizedWithToken. FIXED 2026-08-23: same entities/context wiring fix as BatchIsAuthorized, see the gaps entry below."}
  BatchGetPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed BatchGetPolicyOutputItem correctly has NO top-level effect/actions/principal/resource fields (unlike GetPolicy/ListPolicies) -- verified against the real SDK type, left as-is. FIXED (2026-08-20): an unresolvable-alias policyStoreId in a batch item was coded as errors[].code=POLICY_STORE_NOT_FOUND -- the real SDK's BatchGetPolicyErrorCode enum (types/enums.go:24-30) declares a dedicated POLICY_STORE_ALIAS_NOT_FOUND value for exactly this case, distinct from a bare-ID miss. Right key, wrong value; a bare (non-alias) policyStoreId miss inside the backend's own item loop already used POLICY_STORE_NOT_FOUND correctly and is unaffected. Proven with a real-client round-trip test (TestBatchGetPolicy_UnresolvableAlias_ErrorCode) since BatchGetPolicyErrorItem.Code is a typed enum field the SDK client surfaces directly."}
  CreateIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): CreateIdentitySourceOutput was echoing the full principalEntityType + configuration back -- the real output shape is minimal (identitySourceId/policyStoreId/timestamps only); those two fields don't exist on the real CreateIdentitySourceOutput type. ClientToken now wired to idempotency (was parsed but silently discarded before). Last pass: identityTokenOnly.clientIds + cognitoUserPoolConfiguration.issuer fixes carry forward."}
  GetIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass -- the fuller identitySourceOutput shape (with principalEntityType + configuration) IS correct here, matching the real GetIdentitySourceOutput"}
  ListIdentitySources: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass -- IdentitySourceItem's fuller shape also correctly includes principalEntityType + configuration"}
  UpdateIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): same over-eager-echo bug as CreateIdentitySource -- UpdateIdentitySourceOutput was echoing principalEntityType, a field the real UpdateIdentitySourceOutput doesn't have (minimal id/policyStoreId/timestamps shape, same as Create)"}
  DeleteIdentitySource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: now also clears the deleted identity source's resourceTags entry"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePolicyStoreAlias: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW (SDK bumped to v1.36.0). Validates the target policy store exists by ID (ResourceNotFoundException otherwise -- alias resolution deliberately NOT applied to this field, matching the real SDK's explicit \"the alias name cannot be used\"); enforces the required policy-store-alias/ prefix (ValidationException); enforces account/region-unique alias names (ConflictException on a name collision against a different target or a PendingDeletion alias); idempotent replay on an exact (aliasName, policyStoreId) repeat against an Active alias, matching the real SDK's documented idempotency."}
  GetPolicyStoreAlias: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW. Returns the alias regardless of state (Active/PendingDeletion) -- reporting that distinction is this op's job."}
  ListPolicyStoreAliases: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW. Supports the optional filter.policyStoreId narrowing and maxResults/nextToken pagination (shared listByPolicyStore helper, same pattern as ListPolicyTemplates/ListIdentitySources)."}
  DeletePolicyStoreAlias: {wire: ok, errors: partial, state: ok, persist: ok, note: "NEW. Idempotent on a missing aliasName (200, per real SDK doc). deletionMode SoftDelete (default) transitions the alias to PendingDeletion instead of removing it; HardDelete removes it immediately. errors=partial: the real SDK also declares InvalidStateException for this op, but its documented message (\"The policy store can't be deleted because deletion protection is enabled...\") is byte-for-byte identical to DeletePolicyStore's InvalidStateException text and references a deletionProtection field aliases don't have -- strong evidence of copy-pasted/auto-generated doc boilerplate rather than a real alias-specific trigger. Left unimplemented rather than guessing a fabricated trigger condition; see gaps."}
gaps:                     # known divergences NOT fixed
  - "FIXED 2026-08-23: this note previously said IsAuthorized/IsAuthorizedWithToken never read a context or entities field at all, and that BatchIsAuthorized(WithToken) did accept entities. Re-investigated: the premise understated the bug -- ALL FOUR evaluation ops (IsAuthorized, IsAuthorizedWithToken, BatchIsAuthorized, BatchIsAuthorizedWithToken) were affected. IsAuthorized/IsAuthorizedWithToken's input structs genuinely had no context/entities fields at all (accept-and-drop: a real client's context/entities JSON keys were silently discarded by json.Unmarshal). BatchIsAuthorized(WithToken)'s pre-existing entities field was ALSO wrong shape (a bare array, not the real union {\"entityList\": [...]}/{\"cedarJson\": ...}) AND, worse, was parsed but never threaded into evaluateCedar/cedar.Authorize at all -- entities and per-item context were accepted (or silently mis-shaped) and then dropped before reaching Cedar, so a policy referencing context.* or an entity's attributes could never see real data on any of the four ops. Fixed end-to-end: cedar_attributes.go adds an AWS-AttributeValue-JSON -> cedar-go Value converter (boolean/string/long/decimal/datetime/duration/ipaddr/entityIdentifier/record/set, matching serializers.go's awsAwsjson10_serializeDocumentAttributeValue) plus entitiesToCedar/contextToCedar for both real union variants (entityList/contextMap -- typed AttributeValue objects -- and cedarJson -- a literal string that happens to match cedar-go's own native Entity/Record JSON shape, confirmed against cedar-go@v1.8.0's EntityMap.UnmarshalJSON/Record.UnmarshalJSON, so that variant reuses cedar-go's own decoder directly). AuthorizationRequest gained an internal (non-wire) Context field; StorageBackend's four IsAuthorized*/BatchIsAuthorized* methods gained an entities cedar.EntityMap parameter; evaluateCedar passes both into cedar.Authorize instead of a hardcoded nil entities store and an empty Context. Cedar 'tags' (EntityItem.Tags, a newer, separate AttributeValue-shaped member) remain unconverted -- disclosed, not fabricated, every converted entity has an empty tag set. Proven via two real aws-sdk-go-v2/service/verifiedpermissions client round trips (context_entities_test.go): a policy keyed on context.mfa==true and a policy keyed on resource.owner==principal (via a supplied entity attribute) each flip DENY->ALLOW only when the real client actually supplies that context/entities data -- hand-reverted (all 5 touched files, cp-based per this batch's protocol), confirmed both tests fail with DENY instead of ALLOW against the pre-fix code, restored, md5sum byte-identical. make build-check clean repo-wide (StorageBackend has no external implementers)."
  - "IsAuthorizedWithToken/BatchIsAuthorizedWithToken: JWT signature verification is not performed (needs the issuer's real signing keys -- genuinely out of scope for an in-memory mock). Tokens are trusted at face value once their claims parse; expiration is also not checked. aud/client_id-against-configured-client-IDs matching WAS implemented this pass (see ops notes above) since it's a plain data comparison against configuration this backend already stores, not cryptography."
  - "DeletePolicyStoreAlias: the real SDK declares an InvalidStateException, but its documented trigger text is (byte-for-byte) DeletePolicyStore's own \"deletion protection is enabled\" message, which does not apply to aliases (no deletionProtection field exists on PolicyStoreAlias). Treated as unreliable auto-generated API-reference boilerplate rather than implemented as a guessed condition; if AWS's real behavior differs (e.g. re-soft-deleting an already-PendingDeletion alias), this needs a follow-up once the actual trigger is confirmed."
  - "CreatePolicyStoreAlias's ServiceQuotaExceededException is declared as a possible error but no numeric per-account/region alias quota is documented anywhere in the API reference, so none is enforced -- consistent with how this service (and others in gopherstack) leaves undocumented-threshold quota exceptions unenforced rather than fabricating a number."
  - "resolvePolicyStoreID (alias-as-policyStoreId resolution, wired into every other policyStoreId-accepting op this pass) was independently verified against the AWS API reference for 6 ops spanning distinct categories -- GetPolicyStore, UpdatePolicyStore (implied by GetPolicyStore's identical doc text), IsAuthorized, CreatePolicy, DeletePolicy, PutSchema -- all carrying byte-identical documented wording. Applied by strong pattern consistency to the remaining ~15 policyStoreId-accepting ops (policy templates, identity sources, GetSchema, the Batch* evaluation ops) rather than independently doc-verified one-by-one; the two documented exceptions (CreatePolicyStoreAlias, DeletePolicyStore) are confirmed and excluded. Flagging this as an inference rather than a silently-assumed fact."
deferred: []              # the one item deferred last pass (CreatePolicyStore ClientToken) is now implemented; see ops notes
leaks: {status: clean, note: "no goroutines/janitors in this service; InMemoryBackend uses a single lockmetrics.RWMutex. Prior pass fixed real ghost-row leaks: DeletePolicy/DeleteIdentitySource/DeletePolicyStore's cascade/DeletePolicyTemplate's cascade all clear resourceTags (previously only arnIndex was cleaned, so a tagged-then-deleted resource left its tag map entry behind forever); DeletePolicyStore also clears policySetCache/policySetDirty for the deleted store. This pass adds policyStoreAliases (a new store.Table registered on b.registry, keyed by AliasName) to that same cascade: DeletePolicyStore now also deletes every alias pointing at the store being deleted (see policy_stores.go's DeletePolicyStore -- the real API's docs are silent on this since DeletePolicyStore predates aliases entirely, so gopherstack picked cascade-delete per this campaign's documented-choice convention, proven by TestVPHandler_DeletePolicyStore_CascadesAliases/TestBackend_DeletePolicyStore_CascadesAliases). Aliases carry no arnIndex/resourceTags entries at all (not a taggable resource type in the real API -- TagResource's own doc says only policy stores can be tagged), so no ARN/tag cleanup was needed for them. clientTokens (ClientToken idempotency state) remains an ephemeral, never-persisted map; entries age out via the 8h idempotencyWindow check at lookup time (no janitor goroutine). Snapshot/Restore of the new policyStoreAliases table fully exercised by persistence_test.go's TestInMemoryBackend_SnapshotRestore_FullState (extended this pass) plus store_test.go's new alias tests."}

## Notes

**2026-08-22 (gopherstack-tpu3): PolicyTemplate.Name missing end-to-end.**
Filed during the zquj keycheck sweep as structural (not a tag rename), since
`Name` had no field on the `PolicyTemplate` model at all -- confirmed against
`verifiedpermissions@v1.36.4`: `CreatePolicyTemplateInput.Name`/
`UpdatePolicyTemplateInput.Name` are real, settable request members
(`api_op_CreatePolicyTemplate.go:94`, `api_op_UpdatePolicyTemplate.go:104`),
and both `GetPolicyTemplateOutput` and `types.PolicyTemplateItem` (used by
`ListPolicyTemplates`) require a `"name"` key in their deserializers
(`deserializers.go:9615`, `:7584`). Neither Create/UpdatePolicyTemplateOutput
carries a Name member, so it is not echoed there -- only on Get/List, which
is what this fix wires up.

Fixed: added `Name string` to the `PolicyTemplate` model (a `store.Table`
value serialized directly into the snapshot -- purely additive, so
`verifiedpermissionsSnapshotVersion` was **not** bumped; the
`TestSnapshotVersionGuard` golden was refreshed with `-update` to add the one
new field line, confirmed via `git diff` to touch nothing else), threaded
`name` through `Backend.CreatePolicyTemplate`/`UpdatePolicyTemplate`
(interface signature change, all callers updated) and through
`createPolicyTemplateInput`/`updatePolicyTemplateInput`/
`getPolicyTemplateOutput`/`policyTemplateView`. `CreatePolicyTemplate`'s
idempotency fingerprint now includes `name` too, so a retried `ClientToken`
with a different name is correctly treated as a conflicting request rather
than silently keeping the first name.

Proven end-to-end via a real `aws-sdk-go-v2` client
(`TestPolicyTemplate_NameRoundTrip`,
`services/verifiedpermissions/policy_template_name_test.go`):
Create-with-Name -> Get/List both decode it -> Update-with-a-new-Name ->
Get sees the new value. Hand-reverted (production files only, then also the
call-site test edits, to isolate a genuine runtime assertion rather than a
bare compile failure) and confirmed failing with `Name` decoding empty at
every step; restored, md5sum byte-identical. Also extended
`TestInMemoryBackend_SnapshotRestore_FullState` to set a Name and assert it
survives Snapshot/Restore.

No test asserted the old (missing-Name) shape, so none needed correcting.

Protocol: awsjson1.0 (`application/x-amz-json-1.0`, `X-Amz-Target: VerifiedPermissions.<Op>`),
correctly matched by `RouteMatcher`/`ExtractOperation` (targetPrefix check).

Timestamps (`createdDate`/`lastUpdatedDate`/schema dates) are ISO-8601 strings
(`smithytime.ParseDateTime` on the real client side), NOT epoch-seconds numbers --
gopherstack's `timeFormat = "2006-01-02T15:04:05.000Z"` + `.UTC().Format(...)` is correct
as-is. This is a "looks-wrong-but-correct" trap: don't reflexively reach for
`pkgs/awstime.Epoch` here, this service is one of the ISO8601-not-epoch awsjson1.0 services.

## This pass's findings (2026-07-25: policy store aliases, SDK bump to v1.36.0)

The Go SDK module was bumped from v1.31.4 to v1.36.0, which added a policy-store-alias
family: `CreatePolicyStoreAlias`, `GetPolicyStoreAlias`, `ListPolicyStoreAliases`,
`DeletePolicyStoreAlias`. A policy store alias is a human-readable, account/region-unique
name (always prefixed `policy-store-alias/`) that resolves to a policy store's real ID.
All four ops are now implemented for real (see the `ops:` entries above) -- new backend
state in `policy_store_aliases.go` (a `store.Table[PolicyStoreAlias]` registered on
`b.registry`, keyed by `AliasName`, plus a `byPolicyStore` index) and new handlers in
`handler_policy_store_aliases.go`, both field-diffed against the real SDK's
`serializers.go`/`deserializers.go` (confirmed awsjson1.0, `X-Amz-Target:
VerifiedPermissions.<Op>`, same protocol as every other op in this service).

### Alias ARNs are region-populated, unlike every other resource in this service (fixed as new, not a regression)

Every other ARN in this service (`policyStoreARN`, `policyARN`, `policyTemplateARN`,
`identitySourceARN`) uses `arnNoRegion` -- confirmed correct by a prior pass against real
`CreatePolicyStore` doc examples, which show an empty region segment
(`arn:aws:verifiedpermissions::123456789012:policy-store/...`). But the real SDK's own
`CreatePolicyStoreAlias`/`GetPolicyStoreAlias`/`ListPolicyStoreAliases` example responses
consistently populate the region
(`arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store-alias/example-policy-store`)
across all three independently-fetched doc pages. `policyStoreAliasARN` (in
`policy_store_aliases.go`) therefore uses `arn.Build`'s normal region-populated form
instead of `arnNoRegion` -- a real, doc-confirmed wire-shape distinction, not an
oversight.

### Referential integrity: target-store validation, uniqueness, and cascade delete

Aliases are fundamentally a referential-integrity feature, so the wiring around them
mattered more than the CRUD itself:

- **`CreatePolicyStoreAlias` validates the target policy store exists** in this backend
  before creating the alias (`InMemoryBackend.CreatePolicyStoreAlias` checks
  `b.policyStores.Has(policyStoreID)`), returning the real `ResourceNotFoundException`
  otherwise. Proven by `TestVPHandler_CreatePolicyStoreAlias`'s "target policy store does
  not exist" case and `TestBackend_CreatePolicyStoreAlias_TargetNotFound`.
- **Alias names are unique within account/region** (the real SDK's documented
  constraint) -- enforced by keying the `policyStoreAliases` table directly off
  `AliasName`. A `CreatePolicyStoreAlias` retry with the same `(aliasName,
  policyStoreId)` pair against an `Active` alias replays the original (the real SDK's
  documented idempotency: "a Success response will be returned and a new policy store
  alias will not be created"); the same `aliasName` against a *different*
  `policyStoreId`, or against an alias currently `PendingDeletion`, is a
  `ConflictException` (the real SDK's `GetPolicyStoreAlias` doc: "creating a policy
  store alias with the same alias name will fail" while `PendingDeletion`, with no
  same-target exception carved out). One policy store may have multiple aliases (the
  real `ListPolicyStoreAliases` doc example shows two aliases for one store) --
  gopherstack imposes no artificial one-alias-per-store limit.
- **Deleting a policy store cascade-deletes its aliases.** `DeletePolicyStore`'s own doc
  page predates aliases entirely and never mentions them -- the real API is silent on
  this exact interaction. Per this campaign's documented-choice convention (a recent
  fix in this campaign found the identical bug class in emr: sessions surviving cluster
  termination), gopherstack picks **cascade-delete** rather than leaving a dangling
  alias that would keep resolving (via `ResolvePolicyStoreAlias`) to a policy store ID
  that no longer exists. Implemented in `policy_stores.go`'s `DeletePolicyStore` (new
  loop over `b.policyStoreAliasesByStore.Get(policyStoreID)`) and proven end-to-end by
  `TestVPHandler_DeletePolicyStore_CascadesAliases` (asserts the alias is gone via both
  `GetPolicyStoreAlias` and `ListPolicyStoreAliases`, and that the freed alias name can
  be immediately reused for a new store) plus the backend-level
  `TestBackend_DeletePolicyStore_CascadesAliases` and the extended
  `TestInMemoryBackend_SnapshotRestore_FullState` (proves the cascade also holds after a
  Snapshot/Restore round trip).
- **`DeletePolicyStoreAlias`'s `SoftDelete`/`HardDelete` distinction** is real and
  implemented: `SoftDelete` (the default) transitions the alias to `PendingDeletion`
  (still visible via `Get`/`List`, but ineligible for a new `CreatePolicyStoreAlias`
  with the same name and for alias resolution elsewhere -- see below); `HardDelete`
  removes the row immediately. `DeletePolicyStoreAlias` on a nonexistent `aliasName` is
  a no-op success, matching the real SDK's documented idempotency.

### Alias resolution is real elsewhere in the API, and is now wired in

Checked whether the ~20 other ops that accept a `policyStoreId` field should also accept
an alias name in that field. They do: the real API reference's `GetPolicyStore`,
`CreatePolicy`, `DeletePolicy`, `IsAuthorized`, and `PutSchema` doc pages (independently
fetched, spanning store/policy/evaluation/schema categories) all carry byte-identical
wording -- *"To specify a policy store, use its ID or alias name. When using an alias
name, prefix it with `policy-store-alias/`."* The two documented exceptions are
`CreatePolicyStoreAlias.policyStoreId` and `DeletePolicyStore.policyStoreId`, both of
which explicitly require the literal ID (*"the alias name cannot be used"*).

Implemented as a single shared `Handler.resolvePolicyStoreID` (in
`handler_policy_store_aliases.go`), called at the top of every other
policyStoreId-accepting handler (`GetPolicyStore`, `UpdatePolicyStore`, `CreatePolicy`,
`GetPolicy`, `ListPolicies`, `UpdatePolicy`, `DeletePolicy`, `BatchGetPolicy` -- resolved
per-item, an unresolvable alias fails only that item rather than the whole batch --
`CreatePolicyTemplate`, `GetPolicyTemplate`, `ListPolicyTemplates`,
`UpdatePolicyTemplate`, `DeletePolicyTemplate`, `CreateIdentitySource`,
`GetIdentitySource`, `ListIdentitySources`, `UpdateIdentitySource`,
`DeleteIdentitySource`, `PutSchema`, `GetSchema`, `IsAuthorized`,
`IsAuthorizedWithToken`, `BatchIsAuthorized`, `BatchIsAuthorizedWithToken`), resolving to
the real policy store ID before any backend call and before it's echoed back in a
response (so responses always show the real ID, never the alias, matching the real SDK's
example responses). `CreatePolicyStoreAlias` and `DeletePolicyStore` deliberately do NOT
call this helper. A `PendingDeletion` alias fails resolution with
`ResourceNotFoundException` (`ResolvePolicyStoreAlias` only resolves `Active` aliases),
matching the real SDK's documented behavior once an alias enters that state. Proven by
`TestVPHandler_ResolvePolicyStoreID_AcceptsAlias` (table across
Get/UpdatePolicyStore-accepts vs CreatePolicyStoreAlias/DeletePolicyStore-rejects) and
`TestVPHandler_ResolvePolicyStoreID_PendingDeletionAliasFails`. See the `gaps:` entry
above for the honesty caveat on how broadly this was doc-verified vs inferred by pattern.

## Prior pass's findings (2026-07-23 re-audit)

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

### This pass (2026-08-10): aud/client_id validation implemented, missing `principal` response field, request-echo wire bug

Follow-up on gopherstack-w3hm, which named `aud`/`client_id` matching and JWT signature
verification as both "out of scope for a mock." Pushed on that framing rather than
inheriting it:

- **`aud`/`client_id` matching is a plain data comparison, not cryptography, and is now
  implemented.** The client IDs/audiences are configuration this backend already stores
  on the identity source (`IdentitySource.ClientIDs` for Cognito,
  `OIDCTokenSelection.Audiences` for OIDC); the token is caller-supplied. Comparing one
  against the other needs no signing key. Per real AWS's documented behavior
  ([cognito-validation.html](https://docs.aws.amazon.com/verifiedpermissions/latest/userguide/cognito-validation.html),
  [oidc-validation.html](https://docs.aws.amazon.com/verifiedpermissions/latest/userguide/oidc-validation.html)):
  Cognito sources compare their configured client IDs against the ID token's `aud` claim
  or the access token's `client_id` claim; OIDC sources compare against `aud` for ID
  tokens, and `aud` (falling back to `cid`/`client_id` when `aud` is absent) for access
  tokens. `matchIdentitySource`/`matchesConfiguredAudience`
  (`handler_authorization.go`) implement this: a token whose claim doesn't match the
  matched source's configured list no longer resolves a principal from that source
  (fails closed to no-principal, i.e. DENY) instead of being silently accepted --
  previously, a token minted for a *different* application but sharing the same trusted
  issuer would still resolve a principal and could be ALLOWed by a permissive policy, an
  over-authorization gap. A source with no client IDs/audiences configured keeps
  accepting any token, matching AWS's "when you enter one or more values for Client
  application validation" (i.e. validation is opt-in).
- **JWT signature verification remains genuinely out of scope** -- it needs the actual
  issuer's signing keys, which this in-memory mock has no way to obtain or verify
  against. Confirmed this holds rather than assumed it: `IsAuthorizedWithToken`'s
  declared error set (AccessDeniedException, InternalServerException,
  ResourceNotFoundException, ThrottlingException, ValidationException -- no dedicated
  "invalid token" exception) plus the response's `principal` field being *optional*
  (not "This member is required," unlike `decision`/`determiningPolicies`/`errors`)
  is concrete evidence that real AWS's own design tolerates a token that can't be
  resolved to a principal: it evaluates with no principal (implicit DENY, since no
  principal-scoped policy can match) and simply omits the field, rather than hard
  erroring the whole request. A malformed/unparseable token (bad JWT shape, bad
  base64, bad JSON) already took exactly this path in `principalFromToken` and still
  does -- verified sane, not a bug, left as-is.
- **Missing wire field, both ops (real bug, unrelated to crypto):**
  `IsAuthorizedWithTokenOutput`/`BatchIsAuthorizedWithTokenOutput` both declare a
  `principal` field (`EntityIdentifier`, "The identifier of the principal in the ID or
  access token") that gopherstack's response never populated at all -- the exact
  principal `principalFromToken` already resolves was computed but silently dropped
  before it reached the wire. Now echoed (omitted when no principal resolves).
- **`BatchIsAuthorizedWithToken` per-item `request` echo wire bug:** it was reusing
  plain `BatchIsAuthorized`'s echo shape, which includes a `principal` field once the
  internal `AuthorizationRequest.PrincipalEntityType` was set (from the token) -- but the real
  SDK's `BatchIsAuthorizedWithTokenInputItem` (unlike plain `BatchIsAuthorizedInputItem`)
  has no principal member at all; the principal comes from the token and is carried once
  at the top level instead. Split into a dedicated
  `batchIsAuthorizedWithTokenRequestEchoJSON` (`action`/`resource` only).

`sdk_module` bumped `v1.36.0` -> `v1.36.4` (the actual `go.mod` pin; PARITY.md's header
had gone stale). Diffed the two versions' extracted module-cache trees
(`aws-sdk-go-v2/service/verifiedpermissions@v1.36.0` vs `@v1.36.4`): only
`CHANGELOG.md`/`go.mod`/`go.sum`/`go_module_metadata.go` differ, no API/type changes --
every wire claim in this file still holds against the pinned version.

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

## This pass (2026-08-20): wrapper-key/nested-shape/union sweep (gopherstack-c733)

This service was flagged as a good stress test for a wrapper-key/nested-shape campaign
that had, across 15 prior services, found bugs in generalized-member reuse (a), wrong
JSON type (b), case-sensitive key near-misses (c), and wrong HTTP binding (d), but not
yet in a union-heavy service. verifiedpermissions has more smithy unions than any
service swept so far in this campaign.

**Unions verified, all correct:** `PolicyDefinition`/`PolicyDefinitionDetail`/
`PolicyDefinitionItem` (`static`/`templateLinked` -- confirmed the three near-identical
types each match their own real sibling: `StaticPolicyDefinitionItem` genuinely has no
`statement` field, unlike `...Detail`); `Configuration`/`ConfigurationDetail`/
`ConfigurationItem` (`cognitoUserPoolConfiguration`/`openIdConnectConfiguration`);
`OpenIdConnectTokenSelection`/`...Detail`/`...Item` (`identityTokenOnly`/
`accessTokenOnly`, including the `clientIds`-vs-`audiences` field-name split between the
two variants); `EntityReference` (`identifier`/`unspecified`); `SchemaDefinition`
(single-member `cedarJson`). All discriminator keys and casing confirmed byte-for-byte
against `serializers.go`/`deserializers.go`'s `object.Key(...)`/`case "..."` lines, not
inferred from field names.

**Four summary/full pairs and two three-way families, verified:**
- `PolicyStoreItem` vs `GetPolicyStoreOutput`: distinct, correctly leaner (item drops
  validationSettings/deletionProtection/cedarVersion/tags) -- unchanged from prior pass.
- `PolicyItem` vs `GetPolicyOutput`: correctly share top-level shape (both have
  effect/actions/principal/resource), differ only in their nested `Definition` union
  member types (Item vs Detail) -- unchanged from prior pass.
- `IdentitySourceItem` vs `GetIdentitySourceOutput`: **identical** wire shape by design
  (both carry `principalEntityType`+`configuration`), and gopherstack correctly shares
  one Go type for both -- confirmed the real `CognitoUserPoolConfigurationDetail`/
  `...Item` and `OpenIdConnectConfigurationDetail`/`...Item` are themselves
  field-identical, so unification here can't hide a divergence.
- `PolicyTemplateItem` vs `GetPolicyTemplateOutput`: **NOT identical** -- this is the bug
  found this pass (see ops: ListPolicyTemplates above). The two shapes look like the
  IdentitySource pair above but aren't: `PolicyTemplateItem` has no `statement`.
- `PolicyDefinition{,Detail,Item}` three-way family: verified above, clean.
- `Configuration{,Detail,Item}` three-way family: verified above, clean.

**Bugs found and fixed (2):**
1. `ListPolicyTemplates`' `policyTemplateView` (handler_policy_templates.go) echoed a
   `statement` field for every item -- `types.PolicyTemplateItem`
   (verifiedpermissions@v1.36.4: types/types.go:2121) has no such member. Pattern (a):
   generalized from the wider `GetPolicyTemplateOutput`/`PolicyTemplateItem`'s own
   `getPolicyTemplateOutput`/`policyTemplateIDsOutput` sibling types in the same file,
   which legitimately do carry it. Fixed by dropping the field from `policyTemplateView`
   and its one construction site. Proven with a raw-body assertion
   (`TestListPolicyTemplates_ItemHasNoStatement`, policy_template_item_shape_test.go)
   since a typed SDK client silently ignores an unknown JSON key and can't observe the
   leak directly. Hand-revert reproduced the exact predicted symptom (item map contains
   `"statement"`); restored fix confirmed byte-identical (md5) and green.
2. `BatchGetPolicy`'s per-item alias-resolution-failure path (handler_policies.go) coded
   `errors[].code` as `"POLICY_STORE_NOT_FOUND"` for every failure, including when the
   failing `policyStoreId` was an unresolvable alias. The real SDK's
   `BatchGetPolicyErrorCode` enum (types/enums.go:24-30) declares
   `POLICY_STORE_ALIAS_NOT_FOUND` specifically for that case, distinct from
   `POLICY_STORE_NOT_FOUND`. `resolvePolicyStoreID` only ever returns an error when the
   input carried the `policy-store-alias/` prefix and resolution failed -- a bare
   non-alias ID is passed through unchanged and only fails inside the backend's own item
   loop (`policies.go`), which already used `POLICY_STORE_NOT_FOUND` correctly -- so this
   was a clean, unambiguous right-key-wrong-value bug affecting only the alias path.
   Fixed by changing the literal to `"POLICY_STORE_ALIAS_NOT_FOUND"`. Proven with a real
   `aws-sdk-go-v2` client round-trip (`TestBatchGetPolicy_UnresolvableAlias_ErrorCode`,
   batch_get_policy_alias_error_test.go) asserting
   `types.BatchGetPolicyErrorCodePolicyStoreAliasNotFound` on `out.Errors[0].Code` --
   `Code` is a typed enum field the client surfaces directly, no raw-body inspection
   needed. Hand-revert reproduced the exact predicted mismatch; restored fix confirmed
   byte-identical (md5) and green.

**No other wire bugs found.** Every response struct in policies.go/policy_stores.go/
policy_templates.go/identity_sources.go/policy_store_aliases.go/schema.go/
authorization.go was field-diffed against its own real `api_op_*.go` Output struct or
`types/types.go` type -- not against a sibling gopherstack type or a same-looking SDK
type. `IsAuthorizedOutput`/`IsAuthorizedWithTokenOutput`/`BatchIsAuthorizedOutputItem`/
`BatchIsAuthorizedWithTokenOutputItem`/`DeterminingPolicyItem`/`EvaluationErrorItem`/
`BatchGetPolicyOutputItem`/`PolicyStoreAliasItem`/`CreatePolicyStoreAliasOutput`/
`GetPolicyStoreAliasOutput`/`ListPolicyStoreAliasesOutput`/`PutSchemaOutput`/
`GetSchemaOutput`/`TagResource`/`UntagResource`/`ListTagsForResource` all confirmed
clean, no changes needed.

**Genuine gap disclosed, not fixed** (out of this pass's wrapper-key scope): `IsAuthorized`/
`IsAuthorizedWithToken`'s request Input never reads a `context`/`entities` field at all,
unlike `BatchIsAuthorized(WithToken)` which does accept `entities` (attrs passed through
as raw JSON, never parsed into a typed `AttributeValue` union). This is a request-side
Cedar-evaluation completeness gap, not a response wrapper-key bug -- surfaced
incidentally while reading these ops' Input structs, not chased further. See `gaps:`
above.

**`last_audit_commit` provenance:** the prior manifest cited `2eb5bdb71`, dated
`2026-08-10` by `git show -s --format=%ad`, matching the manifest's own
`last_audit_date: 2026-08-10` exactly -- not "weeks older," so no red flag by that
criterion, even though the sha is unreachable from `main` (`git merge-base
--is-ancestor` fails), consistent with this campaign's repo-wide finding
(gopherstack-z31a) that essentially every manifest cites an unreachable sha from a
worktree/squash-merge workflow. The CONTENT held up well under re-verification: of ~30
response shapes and 8 union families checked against the pinned SDK, only the two bugs
above were found, both narrow and both now fixed with proof. This pass's own
`last_audit_commit` is `92bc04738` (HEAD at audit time, pre-commit), dated `2026-08-20`,
matching this entry's `last_audit_date`.
