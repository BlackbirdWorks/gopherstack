---
service: cloudfront
sdk_module: aws-sdk-go-v2/service/cloudfront@v1.60.2
sibling_sdk_modules: [aws-sdk-go-v2/service/cloudfrontkeyvaluestore@v1.15.2]  # KeyValueStore data-plane ops (GetKey/PutKey/DeleteKey/ListKeys/UpdateKeys); see key_value_stores family
last_audit_commit: PENDING (worked in the parity-3 campaign worktree; not committed by this agent)
last_audit_date: 2026-07-23
overall: A            # Full re-audit this pass: closed all three previously-filed gaps
                       # (gopherstack-a9t managed policies, gopherstack-na4 InUse guards,
                       # gopherstack-mzx CallerReference AlreadyExists), and found three
                       # NEW real wire bugs via field-diff against aws-sdk-go-v2 that were
                       # not previously flagged despite these families being marked "ok":
                       # (1) CachePolicy/OriginRequestPolicy/ResponseHeadersPolicy whitelist
                       # Items lists were silently dropped on parse (CachePolicy only) and on
                       # every read response (all three) -- see "Wire-shape fixes" note below;
                       # (2) UpdateOriginRequestPolicy was routed to require a "/config" URL
                       # suffix that no real SDK client ever sends (real wire is a bare-ID PUT),
                       # so every real UpdateOriginRequestPolicy call 404'd against this
                       # emulator; (3) CreateDistribution/CreateStreamingDistribution treated
                       # CallerReference reuse as unconditionally idempotent, when real AWS
                       # returns *AlreadyExists on ANY reuse regardless of content (stricter
                       # than the previously-filed gopherstack-mzx description, which assumed
                       # a content-comparison rule -- verified against the live API docs).
                       # go build/vet/test -race/golangci-lint all pass clean this pass.
ops:
  CreateDistribution: {wire: ok, errors: fixed, state: ok, persist: ok, note: "FIXED this pass: CallerReference reuse now ALWAYS returns DistributionAlreadyExists (was unconditionally idempotent); real API docs state this happens regardless of DistributionConfig content -- verified against the live CreateDistribution reference page, not just the SDK doc comment"}
  CreateDistributionWithTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "inherits the CreateDistribution CallerReference fix"}
  GetDistribution: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDistributionConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDistribution: {wire: ok, errors: ok, state: fixed, persist: ok, note: "If-Match/ETag enforced; validateQuantities added. FIXED this pass (gopherstack-k3fi): the InProgress status UpdateDistribution sets now really transitions back to Deployed on its own, via a b.work.After-scheduled async hop (distributions.go's scheduleDistributionDeployed) -- the same pkgs/worker idiom services/mgn/exportimport.go and services/outposts's order lifecycle use. The scheduled hop is re-armed on Restore (rearmPendingDistributionDeploysLocked) so a distribution restored mid-transition still reaches Deployed instead of sticking InProgress forever, unlike a bare timer that would only survive a process restart, not a Snapshot/Restore round trip. Scoped to Distribution only -- see deferred note below for the other 5 resource kinds with their own status semantics."}
  DeleteDistribution: {wire: ok, errors: ok, state: ok, persist: ok, note: "If-Match enforced; DistributionNotDisabled enforced"}
  ListDistributions: {wire: ok, errors: ok, state: ok, persist: ok}
  CopyDistribution: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "FIXED this pass: did not track/enforce CallerReference uniqueness at all (distributionCallerRefs was never populated by CopyDistribution); now returns DistributionAlreadyExists on reuse, matching the real CopyDistribution error list"}
  CreateInvalidation: {wire: ok, errors: ok, state: ok, persist: ok, note: "validateQuantities added for Paths; background reconciler transitions InProgress->Completed"}
  GetInvalidation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInvalidations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCachePolicy: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass: request-parsing tags for whitelisted Headers/Cookies/QueryStrings used Headers>Header/Cookies>Cookie/QueryStrings>QueryString, which matches no real CloudFront wire path -- real is Headers>Items>Name (verified against the live CreateCachePolicy/UpdateCachePolicy request syntax); every whitelist/allExcept request silently lost its listed names on unmarshal. Also now returns CachePolicyAlreadyExists; validateQuantities added"}
  UpdateCachePolicy: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "same parse fix as Create; managed policies (Managed-CachingOptimized etc.) now return IllegalUpdate (400) instead of being silently rewritten; If-Match enforced; validateQuantities added"}
  DeleteCachePolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "CachePolicyInUse guard via distribution config token index (prior pass); managed policies now return IllegalDelete (400) instead of being silently removed (this pass)"}
  GetCachePolicy / GetCachePolicyConfig / ListCachePolicies: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED this pass: every response previously omitted the Headers/Cookies/QueryStrings Items lists entirely (only a bare HeaderBehavior/CookieBehavior/QueryStringBehavior, no Quantity, no Items) and GetCachePolicyConfig omitted ParametersInCacheKeyAndForwardedToOrigin altogether -- a real client could never discover which names a policy actually whitelists. Managed-vs-custom Type=managed|custom filter added (gopherstack-a9t, closed) and List summaries now carry the correct <Type> element"}
  CreateOriginRequestPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns OriginRequestPolicyAlreadyExists; validateQuantities added"}
  UpdateOriginRequestPolicy: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED this pass, CLIENT-BREAKING: routing required a PUT to .../origin-request-policy/{id}/config, but the real UpdateOriginRequestPolicy wire is a bare-ID PUT (.../origin-request-policy/{id}, no /config suffix, verified against the live API reference) -- every real SDK client's UpdateOriginRequestPolicy call 404'd with NoSuchOperation against this emulator. Managed policies now return IllegalUpdate (400)"}
  DeleteOriginRequestPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "OriginRequestPolicyInUse guard (prior pass); managed policies now return IllegalDelete (400) (this pass)"}
  GetOriginRequestPolicy / GetOriginRequestPolicyConfig / ListOriginRequestPolicies: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass: same Items-list omission as CachePolicy -- orpResponseXML emitted only a bare Quantity, and GetOriginRequestPolicyConfig omitted HeadersConfig/CookiesConfig/QueryStringsConfig entirely. Type=managed|custom filter added"}
  CreateResponseHeadersPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns ResponseHeadersPolicyAlreadyExists; validateQuantities added"}
  UpdateResponseHeadersPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same as Create; managed policies now return IllegalUpdate (400)"}
  DeleteResponseHeadersPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "ResponseHeadersPolicyInUse guard (prior pass); managed policies now return IllegalDelete (400) (this pass)"}
  GetResponseHeadersPolicy / GetResponseHeadersPolicyConfig / ListResponseHeadersPolicies: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass: CorsConfig's four list fields (AccessControlAllowOrigins/Headers/Methods, AccessControlExposeHeaders) and SecurityHeadersConfig's ContentTypeOptions/ContentSecurityPolicy were completely absent from every response even though the request parser already captured them. GetResponseHeadersPolicyConfig omitted the whole config body. Type=managed|custom filter added. STILL SIMPLIFIED (see items_still_open): XSSProtection is a single string field, not the real 4-field Override/Protection/ModeBlock/ReportUri struct, and STS/FrameOptions/ReferrerPolicy/ContentSecurityPolicy have no per-header Override flag modeled (only ContentTypeOptions does) -- not restructured this pass"}
  CreateOriginAccessControl: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns OriginAccessControlAlreadyExists; validateQuantities added"}
  UpdateOriginAccessControl: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as above"}
  DeleteOriginAccessControl: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED this pass (gopherstack-na4): OriginAccessControlInUse guard added via the same token-index pattern as CachePolicy/Function; verified against a distribution whose Origin.OriginAccessControlId references it"}
  GetOriginAccessControl / GetOriginAccessControlConfig / ListOriginAccessControls: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCloudFrontOriginAccessIdentity: {wire: ok, errors: fixed, state: ok, persist: ok, note: "FIXED this pass: CallerReference reuse with an identical Comment is idempotent (correct, per the real CloudFrontOriginAccessIdentityConfig doc), but reuse with a DIFFERENT Comment previously still returned the existing OAI silently instead of CloudFrontOriginAccessIdentityAlreadyExists; validateQuantities added (harmless no-op for this shape)"}
  UpdateCloudFrontOriginAccessIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "If-Match enforced"}
  DeleteCloudFrontOriginAccessIdentity: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED this pass (gopherstack-na4): CloudFrontOriginAccessIdentityInUse guard added, matching on the real S3OriginConfig.OriginAccessIdentity wire value \"origin-access-identity/cloudfront/{id}\" (not the bare ID) -- If-Match still enforced"}
  GetCloudFrontOriginAccessIdentity / Config / List: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass: response was missing required FunctionMetadata.FunctionARN/CreatedTime/LastModifiedTime; now returns FunctionAlreadyExists (was DistributionAlreadyExists); validateQuantities added"}
  UpdateFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same wire fix; If-Match enforced; validateQuantities added"}
  PublishFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same wire fix; If-Match enforced; LastModifiedTime now bumped"}
  DeleteFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW: FunctionInUse guard (keyed by FunctionARN, not name)"}
  GetFunction / DescribeFunction / ListFunctions / TestFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "GetFunction/DescribeFunction/ListFunctions share the same FunctionMetadata fix"}
  TagResource / UntagResource / ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateAlias / AssociateDistributionWebACL / AssociateDistributionTenantWebACL: {wire: ok, errors: ok, state: ok, persist: ok, families: cross-service}
families:
  distribution_tenants_connection_groups: {status: ok, note: "CreateDistributionTenant/UpdateDistributionTenant now run validateQuantities; If-Match enforced on update/delete; audited, no new findings beyond the Quantity gap"}
  field_level_encryption: {status: ok, note: "Create/Update for config + profile now run validateQuantities and return the correct *AlreadyExists code (FieldLevelEncryptionConfigAlreadyExists / FieldLevelEncryptionProfileAlreadyExists) instead of DistributionAlreadyExists; FLEProfileInUse guard on profile delete pre-existed and is correct"}
  public_keys_key_groups: {status: ok, note: "CreatePublicKey/CreateKeyGroup/UpdateKeyGroup return PublicKeyAlreadyExists/KeyGroupAlreadyExists instead of DistributionAlreadyExists; PublicKeyInUse guard on public-key delete pre-existed and is correct; FIXED this pass (gopherstack-na4): DeleteKeyGroup now returns ResourceInUse (matching the real DeleteKeyGroup error list -- there is no dedicated KeyGroupInUse type) when the key group is referenced by a distribution's TrustedKeyGroups"}
  realtime_log_configs: {status: ok, note: "CreateRealtimeLogConfig now returns RealtimeLogConfigAlreadyExists instead of DistributionAlreadyExists"}
  key_value_stores: {status: ok, note: "control-plane Create/Update run validateQuantities (no-op, shape has no Quantity/Items pairs); data-plane GetKey/PutKeys/ListKeys correctly use the separate JSON protocol, out of scope for this XML-focused sweep. UPDATE (2026-07-31, reverse sdkcheck sweep, gopherstack-vhw2): confirmed by name against aws-sdk-go-v2/service/cloudfrontkeyvaluestore that DeleteKey/GetKey/ListKeys/PutKey/UpdateKeys are exactly its 5 non-DescribeKeyValueStore ops (added to go.mod; pkgs/sdkcheck's reverse check was flagging these 5 as 'phantom' only because it compared them against cloudfrontsdk.Client instead of the data-plane client that owns them -- sdk_completeness_test.go now checks them separately against cfkvssdk.Client). No wire-shape field-diff done, naming/completeness only."}
  vpc_origins: {status: ok, note: "Create/Update run validateQuantities (no-op for this shape)"}
  continuous_deployment_policy: {status: ok, note: "Create/Update run validateQuantities; If-Match already enforced"}
  invalidations_realtime_status: {status: ok, note: "background reconciler goroutine (runInvalidationReconciler) has a clean stopCh lifecycle via Close(); no leak"}
  monitoring_subscriptions_public_resource_policy_connection_groups: {status: ok, note: "audited via handler_new_ops.go/handler_batch2.go dispatch; no Quantity/AlreadyExists-code issues found in these shapes"}
  managed_policies: {status: ok, note: "NEW this pass (gopherstack-a9t): 7 managed cache policies, 8 managed origin request policies, and 5 managed response headers policies seeded at backend construction/Reset/Restore with their real, permanent, verified-against-live-AWS-docs IDs and configs (see managed_policies.go's doc comment for the exact verification method and the deliberately-omitted Amplify-internal policies). Managed=true policies reject Update/Delete with IllegalUpdate/IllegalDelete (400); List* honors the real Type=managed|custom query filter and each summary carries the correct <Type> element"}
  streaming_distributions: {status: ok, note: "FIXED this pass: CreateStreamingDistribution treated non-empty CallerReference reuse as unconditionally idempotent; real AWS returns StreamingDistributionAlreadyExists on any reuse regardless of content (verified against the live CreateStreamingDistribution API reference, same rule as CreateDistribution)"}
gaps: []
  # All three gaps filed by the previous pass are closed as of this pass:
  #  - gopherstack-a9t (managed policies + Type filter): closed, see managed_policies family above.
  #  - gopherstack-na4 (OAI/OAC/KeyGroup delete InUse guards): closed, see the three
  #    "FIXED this pass (gopherstack-na4)" op rows above (DeleteOriginAccessControl,
  #    DeleteCloudFrontOriginAccessIdentity, DeleteKeyGroup via public_keys_key_groups family).
  #  - gopherstack-mzx (CallerReference AlreadyExists): closed, but the actual real-AWS rule
  #    is STRICTER than originally filed -- CreateDistribution/CreateStreamingDistribution
  #    always conflict on CallerReference reuse (content-independent), not just when content
  #    differs. CreateOAI genuinely IS content-comparison idempotent (the filed gap's
  #    assumption was correct for OAI specifically) and was fixed for the differing-content
  #    case. CopyDistribution didn't enforce CallerReference uniqueness at all and was also
  #    fixed. See the CreateDistribution/CopyDistribution/CreateStreamingDistribution/
  #    CreateCloudFrontOriginAccessIdentity op rows above for the exact behavior each has now.
deferred:
  - "Distribution status InProgress->Deployed transition timer: FIXED this pass (gopherstack-k3fi) for Distribution specifically -- see UpdateDistribution's op row above. The other 5 resource kinds with their own InProgress/Deployed-shaped status semantics (DistributionTenant, StreamingDistribution, ConnectionGroup/ConnectionFunction, AnycastIPList, TrustStore) still persist InProgress indefinitely; still deferred, now for a narrower, more honest reason -- extending the same worker.Group timer to each is straightforward but out of this pass's scope, not blocked on anything."
  - "KeyValueStore data-plane (GetKey/PutKeys/ListKeys, separate JSON protocol) -- explicitly out of scope per this task's op enumeration and the pre-existing note that it uses a different wire protocol (cloudfront-keyvaluestore), not REST-XML."
  - "Full per-op audit of DistributionConfig nested shape correctness (Origins/OriginGroups/CacheBehaviors/ViewerCertificate/Restrictions field-by-field) beyond the Quantity/Items validation and the pre-existing minimal-parse (RawConfig) model. This pass verified the specific sub-fields needed for the InUse-guard fixes (S3OriginConfig.OriginAccessIdentity path format, Origin.OriginAccessControlId, TrustedKeyGroups.Items) are correct, but a full field-by-field audit of the rest of DistributionConfig's ~60 nested types was not attempted -- RawConfig storage design predates this pass and was not restructured."
  - "ResponseHeadersPolicySecurityHeadersConfig is a flattened simplification of the real 5-sub-struct shape: XSSProtection is stored/emitted as a single string (matches only the real ReportUri sub-field) instead of the real ResponseHeadersPolicyXSSProtection{Override, Protection, ModeBlock, ReportUri} struct, and only ContentTypeOptions has a per-header Override flag modeled (STS/FrameOptions/ReferrerPolicy/ContentSecurityPolicy hardcode Override=false in every response, which happens to match every seeded managed policy's real Override:No default but is not read from request input for those four). Restructuring RHPSecurityHeaders to the full real shape is a breaking model change (cascades to persistence JSON tags and every existing test that constructs one) out of proportion to fix alongside this pass's other work; the CORS list fields and ContentTypeOptions/ContentSecurityPolicy value (the parts client code actually round-trips today) were fixed."
leaks: {status: clean, note: "runInvalidationReconciler goroutine has a proper stopCh + Close() lifecycle; no unbounded maps found. This pass added b.work (*pkgs/worker.Group), the mgn/outposts-style scheduled-timer idiom used by scheduleDistributionDeployed -- Close() now also calls b.work.Stop(), which cancels every pending timer and joins its goroutines, so nothing outlives the backend. seedManagedPoliciesLocked (prior pass) does no allocation beyond the fixed ~20-entry seed tables and is called only at construction/Reset/Restore, never per-request."}
---

## Notes

**ETag/IfMatch** (proven, not touched this pass): Update/Delete for Distribution, CachePolicy,
OriginRequestPolicy, ResponseHeadersPolicy, OriginAccessControl, OAI, CloudFront Function,
ContinuousDeploymentPolicy, and DistributionTenant all require an `If-Match` header equal to
the resource's current ETag, else `412 PreconditionFailed`. This was already correct across
the board before this sweep; verified op-by-op, no gaps found.

**InconsistentQuantities (the headline fix this pass)**: CloudFront's wire format pairs a
caller-supplied `<Quantity>N</Quantity>` with an `<Items>...</Items>` list virtually
everywhere in the schema (57 distinct SDK types carry a `Quantity *int32` field). Real
AWS rejects a request where `N` disagrees with the actual number of items with
`InconsistentQuantities` (400). Before this pass, the emulator had **zero** occurrences of
this validation anywhere in the codebase -- `grep -rn InconsistentQuantities` was empty.
Root cause: `DistributionConfig` (and most other configs) is parsed into either a minimal
typed struct or stored as opaque `RawConfig` bytes; nothing ever re-derived the caller's
stated `Quantity` and compared it to the real list length, because Go slices don't need an
explicit count. Fix: `services/cloudfront/quantity_validation.go` adds a generic recursive
XML-tree walker (`validateQuantities`) that finds every `<X><Quantity>..</Quantity>
<Items>..</Items></X>` pairing in an arbitrary config body and flags a mismatch --
no per-resource schema modeling required, and provably safe against false positives
because it only fires when both `Quantity` and `Items` siblings are actually present
(verified against `KeyGroupConfig`/`PublicKeyConfig`/`RealtimeLogConfig`/`VpcOriginConfig`,
none of which use this pattern in the real SDK, via the smithy serializers). Wired into
all ~58 Create/Update body-parsing call sites across `handler.go`, `handler_batch2.go`,
and `handler_new_ops.go`.

**AlreadyExists error codes were all wrong (second major finding)**: `handleError`'s
`ErrAlreadyExists` sentinel had `code = "DistributionAlreadyExists"` and was reused
verbatim for CachePolicy, OriginRequestPolicy, ResponseHeadersPolicy, OriginAccessControl,
CloudFront Function, FieldLevelEncryptionConfig, FieldLevelEncryptionProfile, PublicKey,
KeyGroup, and RealtimeLogConfig name/CallerReference collisions -- i.e. creating a second
cache policy with a taken name returned the literal string `DistributionAlreadyExists`,
which is CloudFront's *distribution*-specific error code and was never even triggered by
an actual distribution collision (`CreateDistribution` doesn't use this sentinel at all;
it's fully idempotent on CallerReference, see gap above). Two existing tests
(`TestRefinement1_CachePolicyUniqueness`, `TestRefinement1_ErrorMapping`) asserted this
wrong code as if it were correct -- both fixed with justification comments pointing at the
real `aws-sdk-go-v2/service/cloudfront/types` error type names. Fix: 11 new distinct
sentinel errors (one per resource, matching the real SDK's dedicated error type where one
exists, falling back to the real generic `EntityAlreadyExists` where the SDK has no
resource-specific type -- e.g. Anycast IP lists, key value stores, trust stores). The
`handleError` switch (which had grown to cyclomatic complexity 23) was refactored into a
data-driven `errCodeMapping` table (pattern already established by EC2's `errCodeLookup`),
fixing a `cyclop` lint violation as a side effect.

**Function responses were missing FunctionARN/CreatedTime/LastModifiedTime (third
finding)**: `FunctionMetadata` requires `FunctionARN` and `LastModifiedTime` per the real
SDK (`CreatedTime`/`Stage` too). The emulator's `Function` backend struct *did* compute
and store an ARN (`b.functionARN(name)`) on create, but `functionResponseXML` (shared by
Create/Get/Describe/Publish/Update) and the inline `FunctionSummary` builder in
`handleListFunctions` never emitted it -- a real SDK caller had no way to get a function's
ARN back from any read operation, which makes attaching the function to a distribution's
`FunctionAssociations` (which require the ARN, not the name) impossible. Fixed by adding
`CreatedTime`/`LastModifiedTime` fields to `Function`, populating them on
Create/Update/Publish, and emitting all four `FunctionMetadata` fields from both XML
builders.

**InUse-on-delete guards (fourth finding)**: `DeleteCachePolicy`, `DeleteOriginRequestPolicy`,
`DeleteResponseHeadersPolicy`, and `DeleteFunction` had **no** check for whether the
resource was still referenced by a distribution -- real AWS returns `CachePolicyInUse` /
`OriginRequestPolicyInUse` / `ResponseHeadersPolicyInUse` / `FunctionInUse` (409) in that
case. (`PublicKeyInUse` and `FieldLevelEncryptionProfileInUse` already existed and are
correct -- not touched.) Fixed by adding `tokenReferencedByAnyDistribution` to
`backend_search_index.go`, reusing the pre-existing inverted token index that already
backs `ListDistributionsByCachePolicyID` etc. (built for the `ListDistributionsBy*`
control-plane ops) -- an O(1) check with no new scanning logic.

**gopherstack-na4 closed this pass: `KeyGroup`/`OAI`/`OriginAccessControl` InUse guards.**
`DeleteKeyGroup`, `DeleteOAI`, and `DeleteOriginAccessControl` had the same missing-guard gap
as the fourth finding above, deferred previously because each needed a slightly different
search token than the bare-ID case `tokenReferencedByAnyDistribution` already handled:
- `KeyGroup`: bare ID, referenced via `TrustedKeyGroups.Items` -- same pattern as
  CachePolicy, just a drop-in `tokenReferencedByAnyDistribution(id)` call. Returns
  `ResourceInUse` (409) on conflict: real `DeleteKeyGroup` has no dedicated `KeyGroupInUse`
  type, `ResourceInUse` is the actual documented error (verified against the live API
  reference), matching the existing `ErrKeyGroupNotFound` -> `NoSuchResource` precedent.
- `OAI`: referenced via `S3OriginConfig.OriginAccessIdentity`, whose real wire value is the
  literal path string `"origin-access-identity/cloudfront/{id}"`, not the bare ID (verified
  against the real `S3OriginConfig.OriginAccessIdentity` doc comment). Added
  `oaiReferencePath(id)` (also now shared by `oaiARN`) and check
  `tokenReferencedByAnyDistribution(oaiReferencePath(id))`. Returns
  `CloudFrontOriginAccessIdentityInUse` (409).
- `OriginAccessControl`: referenced via `Origin.OriginAccessControlId`, a bare ID like
  CachePolicyId -- same drop-in pattern as KeyGroup. Returns `OriginAccessControlInUse`
  (409).

All three verified end-to-end via `Test_ResourceInUse_BlocksDelete` in
`resource_in_use_test.go` (extended this pass): create the resource, attach it to a
distribution's raw config, assert delete is blocked with the correct code, disable+delete
the distribution, assert delete now succeeds.

**InconsistentQuantities trap for the next auditor**: don't add per-resource Quantity
validation by hand if you find a new Create/Update body-parsing handler missing the
`validateQuantities(body)` call -- just add the one-line call. The generic walker already
covers any shape with a `<Quantity>`/`<Items>` sibling pair; it is a no-op (returns nil)
for shapes that don't use the pattern, so it is always safe to add defensively.

**"Looks wrong but is correct" traps**:
- `ErrKeyGroupNotFound`'s wire code is `NoSuchResource`, not `NoSuchKeyGroup` -- this
  matches the real SDK (`types.NoSuchResource` is what CloudFront actually returns for a
  missing key group; there is no dedicated `NoSuchKeyGroup` type). Don't "fix" this.
- `ErrKeyValueStoreNotFound`/the new fallback `ErrAlreadyExists` both use `EntityNotFound`/
  `EntityAlreadyExists` -- also correct; the real SDK has no KVS-specific *NotFound/
  *AlreadyExists type either.
- `CreateAnycastIPList`/`CreateKeyValueStore`/`CreateTrustStore` intentionally still use the
  generic `ErrAlreadyExists` (now `EntityAlreadyExists`) sentinel rather than a dedicated
  one -- there is no `AnycastIpListAlreadyExists`/`KeyValueStoreAlreadyExists` type in
  `aws-sdk-go-v2/service/cloudfront/types@v1.60.2` to match; this is the AWS-accurate
  fallback, not an oversight.

**Protocol**: REST-XML throughout (control plane). KeyValueStore's data-plane
(`handler_audit.go`, `GetKey`/`ListKeys`/`UpdateKeys`) correctly uses a separate JSON
protocol matching the real `cloudfront-keyvaluestore` service -- do not "fix" it to XML.

---

## This pass's findings (2026-07-23 re-audit)

**Fifth finding: CachePolicy/OriginRequestPolicy/ResponseHeadersPolicy whitelist Items
lists, both directions.** Field-diffing these three families against the real SDK request
syntax (not just the Go struct field names, which matched) turned up the same bug class as
the second finding, but worse because it hit both parse AND serialize:
- **Parse (CachePolicy only)**: `cachePolicyHeadersConfigXML`/`CookiesConfigXML`/
  `QueryStringsConfigXML` used `xml:"Headers>Header"` / `"Cookies>Cookie"` /
  `"QueryStrings>QueryString"`. The real wire path (verified against the live
  `CreateCachePolicy`/`UpdateCachePolicy` request syntax) is `Headers>Items>Name` /
  `Cookies>Items>Name` / `QueryStrings>Items>Name`. Every whitelist/allExcept request a real
  SDK client sent had its listed names silently discarded on unmarshal -- `Headers` came back
  an empty slice with no error. (OriginRequestPolicy's parse-side tags were already correct;
  only its response side was broken -- see next bullet.)
- **Serialize (all three families, every read op)**: `cachePolicyResponseXML`,
  `orpResponseXML`, and `rhpResponseXML` either omitted the Items list entirely (emitting a
  bare `<Quantity>N</Quantity>` with no `<Items>`/no wrapper element at all) or, for
  `ResponseHeadersPolicy`'s CORS config, dropped all four list fields
  (`AccessControlAllowOrigins`/`AccessControlAllowHeaders`/`AccessControlAllowMethods`/
  `AccessControlExposeHeaders`) and two `SecurityHeadersConfig` fields
  (`ContentTypeOptions`, `ContentSecurityPolicy`) completely, even though the request parser
  already captured all of them correctly. `GetCachePolicyConfig`/
  `GetOriginRequestPolicyConfig`/`GetResponseHeadersPolicyConfig` omitted the entire nested
  config block (`ParametersInCacheKeyAndForwardedToOrigin`/`HeadersConfig+CookiesConfig+
  QueryStringsConfig`/`CorsConfig+SecurityHeadersConfig`) -- not just the lists. A real SDK
  caller had no way to discover which headers/cookies/query-strings/origins/methods a policy
  actually configures via any read op.

  Fix: added `xmlNameItems`/`xmlPluralItems` shared helpers (`handler_cache_policies.go`) and
  `cachePolicyConfigXMLBlock`/`orpConfigXMLBlock`/`rhpConfigXMLBlock` builders reused across
  each family's full response, config-only response, and List summary -- eliminating the
  triplicated, inconsistent hand-built XML that let the three call sites drift out of sync
  with each other in the first place. Locked in by
  `TestCachePolicyWhitelistItems_WireRoundTrip`,
  `TestOriginRequestPolicyWhitelistItems_WireRoundTrip`, and
  `TestResponseHeadersPolicyCORSItems_WireRoundTrip`.

**Sixth finding, CLIENT-BREAKING: `UpdateOriginRequestPolicy` routed to the wrong path.**
`parseCFOriginRequestPolicyPath` only matched `PUT` when the URL suffix ended in `/config`.
The real `UpdateOriginRequestPolicy` wire request is `PUT /2020-05-31/origin-request-policy/
{Id}` -- the bare-ID path, exactly like `UpdateCachePolicy` and `UpdateResponseHeadersPolicy`
(verified against the live API reference request syntax for all three). No real SDK client
ever sends `/config` on a PUT; `/config` is GET-only (`GetOriginRequestPolicyConfig`). Every
real `UpdateOriginRequestPolicy` call against this emulator 404'd with `NoSuchOperation:
unknown operation: Unknown`. An existing test (`TestOriginRequestPolicyCRUD/update_orp`) had
encoded this wrong path as correct and was fixed alongside the route.

**Seventh finding: `CreateDistribution`/`CopyDistribution`/`CreateStreamingDistribution`
CallerReference semantics.** The previously-filed gopherstack-mzx gap assumed a
content-comparison rule (idempotent if identical, conflict if different) by analogy with OAI.
Re-verified against the live API reference pages, not just the SDK's terser doc comments:
`CreateDistribution`'s docs state CallerReference reuse returns `DistributionAlreadyExists`
"regardless of the content of the DistributionConfig object" -- i.e. it NEVER treats reuse as
idempotent, even for byte-identical bodies. Same wording for `CreateStreamingDistribution`
(-> `StreamingDistributionAlreadyExists`) and `CopyDistribution` (which additionally wasn't
tracking CallerReference uniqueness at all before this pass -- `distributionCallerRefs` was
never populated by `CopyDistribution`). `CreateOAI` is the one family where the SDK doc's
content-comparison language is accurate and was implemented as such (identical `Comment` ->
idempotent return; different `Comment` -> `CloudFrontOriginAccessIdentityAlreadyExists`).
Existing tests asserting the old (wrong) always-idempotent behavior for Distribution and
StreamingDistribution were fixed: `TestCallerReferenceReuse` (renamed from
`TestCallerReferenceIdempotency`), `TestPersistenceRoundTrip_IndexesRebuilt`,
`TestStreamingDistributionSnapshotRestore`, `TestInMemoryBackend_StreamingDistribution`.

**Managed policies (gopherstack-a9t, closed)**: see the `managed_policies` family row above
and `managed_policies.go`'s doc comment for the full rationale, verification method, and the
deliberately-omitted Amplify-internal policy set. Every ID was cross-checked against the live
AWS documentation pages (not invented, not guessed) via `WebFetch`, since a wrong ID posing as
a real managed-policy ID would be worse than not seeding one at all.
