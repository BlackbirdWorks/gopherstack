---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: accessanalyzer
sdk_module: aws-sdk-go-v2/service/accessanalyzer@v1.51.4
last_audit_commit: 19eea66b2
last_audit_date: 2026-08-10
overall: A            # multiple real wire-shape bugs found and fixed; two gaps closed for real; dead route deleted
ops:
  CreateAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: now accepts+persists the AnalyzerConfiguration union (\"configuration\") and the inline \"archiveRules\" array (each creates a real ArchiveRule via CreateArchiveRule, including its auto-archive-existing-findings side effect), neither of which was previously read from the request body at all."}
  GetAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: response now includes \"configuration\" when the analyzer has one (previously never returned, since Configuration was not modeled)."}
  ListAnalyzers: {wire: ok, errors: ok, state: ok, persist: ok, note: "Confirmed correctly omits \"configuration\" per the real API's ListAnalyzers/GetAnalyzer asymmetry (see analyzerToJSON's includeConfiguration param)."}
  DeleteAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS (leak): now cascade-deletes tags, findingRecommendations (by finding ID), analyzedResources, and accessPreviews for the deleted analyzer's ARN, in addition to the findings/archiveRules cascade that already existed. Previously b.tags[analyzerARN] and finding-recommendation/analyzed-resource/access-preview rows for the analyzer were never cleaned up -- ghost rows that would resurface (e.g. stale tags) if an analyzer of the same name was re-created."}
  UpdateAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS (was: state: partial): Configuration union is now read from the request body, persisted, and echoed back in the response. Also fixed a real wire-shape bug: the response wrongly included an \"arn\" key -- the real UpdateAnalyzerOutput has ONLY \"configuration\", no arn member. Also upgraded the backend method from RLock to Lock (it now genuinely mutates state instead of being a no-op read)."}
  CreateServiceLinkedAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: now accepts configuration + inline archiveRules, same as CreateAnalyzer (CreateServiceLinkedAnalyzerInput has both fields on the real API too)."}
  DeleteServiceLinkedAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateArchiveRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "auto-archives existing active findings on creation, matching real AWS behavior"}
  GetArchiveRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListArchiveRules: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteArchiveRule: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateArchiveRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ApplyArchiveRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFinding: {wire: ok, errors: ok, state: ok, persist: ok, note: "Routing/resource/resourceOwnerAccount/analyzedAt fixed in a prior pass. FIXED THIS PASS: \"condition\" is a required Finding member (per types.Finding) and was previously omitted whenever a finding had no condition map; now always present (as {} when empty)."}
  ListFindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "Same \"condition\" always-present fix as GetFinding (shared findingToJSON)."}
  UpdateFindings: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingV2: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS (was: wire: partial): findingDetails now returns a real []types.FindingDetails-shaped array with one ExternalAccessDetails union member (condition/action/principal/isPublic, built from the same Finding fields findingToJSON already used) instead of always []; findingType is now \"ExternalAccess\" instead of absent. InMemoryBackend only ever produces external-access-shaped findings (AddFinding has no unused-access/internal-access modeling anywhere in this service), so reporting findingType=ExternalAccess + one ExternalAccessDetails member is a complete, honest representation of everything this backend can produce -- not a disguised partial stub of the other four union members (InternalAccessDetails/UnusedIamRoleDetails/UnusedIamUserAccessKeyDetails/UnusedIamUserPasswordDetails), which remain correctly unmodeled because InMemoryBackend has zero state to back them."}
  ListFindingsV2: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: findingType now \"ExternalAccess\" (FindingSummaryV2 has no findingDetails member at all, unlike GetFindingV2Output, so nothing else to add here)."}
  GetFindingsStatistics: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS (real wire-shape bug, not just a gap): types.ExternalAccessFindingsStatistics serializes its three counters as flat integers totalActiveFindings/totalArchivedFindings/totalResolvedFindings (confirmed against awsRestjson1_deserializeDocumentExternalAccessFindingsStatistics in the SDK's deserializers.go) -- gopherstack was emitting a nested {\"activeFindings\":{\"total\":N}} shape that no real deserializer recognizes; a real SDK client would have silently gotten zero counts back. Also added the missing analyzerArn-required validation (matches GetFindingsStatisticsInput's required field, same pattern as ListFindings)."}
  GenerateFindingRecommendation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingRecommendation: {wire: partial, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS (real wire bugs, kept as gap otherwise): resourceArn and startedAt (both required GetFindingRecommendationOutput members) and completedAt were entirely missing from the response; now populated from the finding record and the recommendation job's own timestamps. recommendationType's wire value was \"UNUSED_PERMISSION\", which does not match the real types.RecommendationType enum's only value, \"UnusedPermissionRecommendation\" (enums.go:579) -- fixed. Also fixed a silent-accept bug: GenerateFindingRecommendation previously created a recommendation record for ANY finding ID, including nonexistent ones, without checking it existed; it now 404s (ResourceNotFoundException) like GetFindingRecommendation already did, and captures the finding's real resourceArn while doing so. recommendedSteps remains always [] -- content generation is still a genuinely separate feature (IAM Access Analyzer's unused-permission-removal recommendation engine) with no state in this backend to derive it from; Status is always SUCCEEDED (synchronous), matching the StartPolicyGeneration convention elsewhere in this service."}
  GetAnalyzedResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS (real wire-shape bug): resourceOwnerAccount is a required types.AnalyzedResource member and was entirely missing from the response; now defaults to the backend's own AccountID(), the same convention findingToJSON already used for Finding.resourceOwnerAccount."}
  ListAnalyzedResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: same resourceOwnerAccount fix as GetAnalyzedResource -- it's also required on types.AnalyzedResourceSummary and was missing from every list item."}
  StartResourceScan: {wire: ok, errors: ok, state: ok, persist: n/a, note: "verifies analyzer exists by ARN; no actual resource scanning to simulate (matches other AA scan endpoints elsewhere in gopherstack)"}
  StartPolicyGeneration: {wire: ok, errors: ok, state: ok, persist: ok, note: "completes synchronously (SUCCEEDED immediately) rather than modeling async IN_PROGRESS -- acceptable since it still reaches a real terminal state and GetGeneratedPolicy/ListPolicyGenerations reflect it; not a stuck-forever no-op. FIXED THIS PASS (silent drop): the optional cloudTrailDetails member (types.CloudTrailDetails) was parsed from the request but entirely discarded; now stored and echoed back (see GetGeneratedPolicy)."}
  GetGeneratedPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS (real wire-shape bug): jobDetails wrongly included \"principalArn\" -- the real types.JobDetails (GetGeneratedPolicyOutput.jobDetails) has NO principalArn member; that value only exists under generatedPolicyResult.properties.principalArn (types.GeneratedPolicyProperties), which was already correct. Split the shared serializer into jobDetailsToJSON (no principalArn) vs policyGenerationToJSON (has principalArn, used by ListPolicyGenerations' types.PolicyGeneration, which DOES carry it) so the two real, differently-shaped types stop being conflated. FIXED THIS PASS: properties.cloudTrailProperties (types.CloudTrailProperties) is now populated from the cloudTrailDetails supplied to StartPolicyGeneration, when present -- previously silently dropped on the floor despite being real, client-supplied, already-available data (same pattern as Analyzer.Configuration from a prior pass). generatedPolicies still always [] -- IAM policy statement synthesis from CloudTrail activity remains a distinct, unimplemented analysis engine with no backing data in this backend."}
  CancelPolicyGeneration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicyGenerations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAccessPreview: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "gopherstack-afi1: Configurations, the required access-control configuration being previewed (api_op_CreateAccessPreview.go:39-43, a 13-member types.Configuration union per resource type -- confirmed via awsRestjson1_serializeDocumentConfiguration in serializers.go), was read by neither the handler's decode struct nor the backend method signature at all -- only analyzerArn was ever consulted. Now decoded (map[string]json.RawMessage, \"configurations\" wire key) and validated to contain exactly one element (the doc comment's stated constraint); stored opaquely rather than decoded into the full union, since ListAccessPreviewFindings (this backend's only Configurations-adjacent behavior) reuses the analyzer's existing findings and never interprets Configurations' semantic content -- see AccessPreview.Configurations godoc (models.go) for the full reasoning. Missing/multi-entry Configurations -> ValidationException, following this handler's existing analyzerArn-required convention (this op declares no validation-style exception in its own error switch)."}
  GetAccessPreview: {wire: fixed, errors: ok, state: ok, persist: ok, note: "response now echoes Configurations back (accessPreviewToJSON(ap, true)), matching real GetAccessPreviewOutput.accessPreview (types.AccessPreview, which has a Configurations member) -- see CreateAccessPreview."}
  ListAccessPreviews: {wire: ok, errors: ok, state: ok, persist: ok, note: "unaffected by the CreateAccessPreview fix: real ListAccessPreviewsOutput.accessPreviews is []types.AccessPreviewSummary, which has NO Configurations member (unlike Get's types.AccessPreview) -- accessPreviewToJSON(ap, false) correctly omits it here, same asymmetry as ListAnalyzers/GetAnalyzer's Configuration field above."}
  ListAccessPreviewFindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS (was: wire: partial): now builds the real types.AccessPreviewFinding shape (id/changeType/resourceOwnerAccount/resourceType/status/createdAt required members, plus action/principal/condition/isPublic when set) via a new accessPreviewFindingToJSON, instead of reusing findingToJSON's v1 Finding/FindingSummary shape (which has analyzerArn and no changeType -- a different, incompatible shape). Every finding is reported as changeType \"New\" since access previews here are not diffed against a prior finding set, so existingFindingId/existingFindingStatus are never populated (both are documented as \"provided only for existing findings\"). Also added the missing analyzerArn-required validation (ListAccessPreviewFindingsInput requires it)."}
  CheckAccessNotGranted: {wire: ok, errors: ok, state: ok, persist: n/a, note: "genuine IAM policy evaluation (policy_analysis.go), not a stub"}
  CheckNoNewAccess: {wire: ok, errors: ok, state: ok, persist: n/a}
  CheckNoPublicAccess: {wire: ok, errors: ok, state: ok, persist: n/a}
  ValidatePolicy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED THIS PASS (real wire-shape bug): findingDetails is a required types.ValidatePolicyFinding member (\"a localized message that explains the finding\") and was never emitted at all. Added findingDetailMessages, a static IssueCode->message lookup covering every code this package's validators can produce (locked in by TestValidatePolicy_FindingDetailsPopulated, which fails if any finding is emitted with an empty message)."}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "FIXED THIS PASS: deleted pathAnalyzedResource (\"analyzedResource\", no hyphen) dead legacy routing -- RouteMatcher claimed it but no parser ever resolved an op for it (always 404'd; no real SDK client sends this path, only the real hyphenated \"/analyzed-resource\" via pathAnalyzedResourceHyph). Removed the RouteMatcher prefix entry and the dead parseRESTPath case; updated TestAccessAnalyzerHandler_RouteMatcher accordingly (now asserts /analyzedResource is NOT claimed and /analyzed-resource IS). All other families re-verified unchanged against aws-sdk-go-v2 serializers.go this pass (archive-rule PUT/GET/DELETE paths+methods, tags GET/POST/DELETE, policy-generation PUT/GET paths, access-preview PUT/GET/POST) -- no further routing bugs found."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "GetFindingRecommendation.recommendedSteps is always [] -- IAM Access Analyzer's actual unused-permission-removal recommendation content generation is a distinct feature with no backing state in InMemoryBackend to derive concrete steps from (RecommendationType/ResourceArn/Status/StartedAt/CompletedAt are ALL real, state-backed, and correctly wire-shaped as of gopherstack-kwht). Not attempted this pass; would need a genuine recommendation-generation model, not a fabricated placeholder. Tracked as bd issue gopherstack-kwht."
  - "GetGeneratedPolicy.generatedPolicyResult.generatedPolicies is always [] -- actual IAM policy generation from CloudTrail activity is a distinct, large feature (statement synthesis from simulated CloudTrail events) with no backing data in this backend. properties (including cloudTrailProperties as of gopherstack-kwht)/jobDetails ARE real, state-backed. Tracked as bd issue gopherstack-kwht."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "store.go/store_setup.go/persistence.go internal locking and Table[T]/Index[T] generic implementation (pkgs/store) not re-audited line-by-line this pass beyond the DeleteAnalyzer cascade fix and the Configuration field addition to the Analyzer table's JSON shape (verified generically compatible with store.Table's JSON-marshal-based Snapshot/Restore, no special-casing needed); no correctness issues observed."
leaks: {status: clean, note: "FIXED THIS PASS: DeleteAnalyzer previously left ghost rows in tags/findingRecommendations/analyzedResources/accessPreviews (see DeleteAnalyzer note above) -- these are now cascade-deleted. No goroutines/janitors in this service; all state is synchronous map/store access under lockmetrics.RWMutex, and every lock acquisition uses defer Unlock/RUnlock (re-verified this pass)."}
---

## Notes

**Protocol**: restjson1. Timestamps are ISO8601 strings via `smithytime.ParseDateTime`
on the real deserializer side (NOT epoch-seconds) -- `time.RFC3339` formatting used
throughout gopherstack's handler*.go is correct; do not "fix" this to `awstime.Epoch`
in a future pass.

**This pass's fixes, in order of severity**:

1. **GetFindingsStatistics wire-shape bug** (real bug, not a gap): the real
   `types.ExternalAccessFindingsStatistics` serializes `totalActiveFindings`/
   `totalArchivedFindings`/`totalResolvedFindings` as flat integers (confirmed against
   `awsRestjson1_deserializeDocumentExternalAccessFindingsStatistics` in the SDK's
   `deserializers.go`), not the `{"activeFindings":{"total":N}}` nested-object shape
   gopherstack was emitting. A real SDK client parsing gopherstack's old response would
   have gotten all-zero counts back silently. Fixed in `handleGetFindingsStatistics`
   (handler_findings.go).
2. **GetGeneratedPolicy jobDetails wrongly included `principalArn`**: `types.JobDetails`
   (the real `GetGeneratedPolicyOutput.jobDetails` type) has no such member -- only
   `types.PolicyGeneration` (used by `ListPolicyGenerations`) does. The two were being
   built by one shared function; split into `jobDetailsToJSON`/`policyGenerationToJSON`
   (handler_generated_policies.go).
3. **GetAnalyzedResource/ListAnalyzedResources missing required `resourceOwnerAccount`**:
   both `types.AnalyzedResource` and `types.AnalyzedResourceSummary` require it; neither
   response included it. Fixed in handler_analyzed_resources.go.
4. **ValidatePolicy findingDetails (required) never emitted**: added a static
   IssueCode -> message table (`findingDetailMessages`, policy_analysis.go) covering
   every code the validators produce.
5. **UpdateAnalyzer response wrongly included `arn`**: the real `UpdateAnalyzerOutput`
   has only `configuration`. Fixed alongside implementing the Configuration union for
   real (see gap-closure below).
6. **v1 Finding's required `condition` field was conditionally omitted** instead of
   always present (as `{}` when empty) -- fixed in `findingToJSON`.
7. **DeleteAnalyzer ghost-row leak**: tags, finding recommendations, analyzed
   resources, and access previews for a deleted analyzer's ARN were never cleaned up.
   Fixed with an explicit cascade (see `analyzers.go`); locked in by
   `TestDeleteAnalyzer_CascadesGhostRows`.
8. **Dead route deleted**: `pathAnalyzedResource` ("analyzedResource", no hyphen) --
   see families.route_matcher above.

**Gaps closed for real this pass** (previously listed under `gaps:`, now implemented,
not just narrowed):
- **UpdateAnalyzer/CreateAnalyzer/CreateServiceLinkedAnalyzer Configuration** (the
  `AnalyzerConfiguration` union) is now accepted, persisted (`Analyzer.Configuration
  json.RawMessage`), and echoed back opaquely -- gopherstack does not semantically
  interpret unused-access/internal-access analysis rules, but no longer silently drops
  client-supplied configuration on the floor either.
- **CreateAnalyzer/CreateServiceLinkedAnalyzer inline `archiveRules`**: now creates real
  archive rules (via the existing `CreateArchiveRule`, including its
  auto-archive-existing-findings side effect) instead of being ignored.
- **GetFindingV2/ListFindingsV2 findingDetails/findingType**: `findingType` is now
  always `"ExternalAccess"` and `GetFindingV2`'s `findingDetails` now returns a real
  one-element `[]types.FindingDetails`-shaped array (`externalAccessDetails`, built from
  the same Principal/Condition/Action/IsPublic fields `findingToJSON` already exposes).
  This is not a disguised partial stub: `InMemoryBackend.AddFinding` has no
  internal-access/unused-access modeling anywhere, so external-access is the only
  finding type this backend can honestly report, and it is now reported completely and
  correctly for that one type.
- **ListAccessPreviewFindings wire shape**: now builds the real
  `types.AccessPreviewFinding` shape (`accessPreviewFindingToJSON`) instead of reusing
  the incompatible v1 `Finding`/`FindingSummary` shape.

**Remaining gaps** (see `gaps:` above): `GetFindingRecommendation.recommendedSteps` and
`GetGeneratedPolicy...generatedPolicies` are both always `[]` -- both would require
modeling genuinely separate content-generation features (unused-permission-removal
recommendations; CloudTrail-activity-derived policy statement synthesis) with no
backing state anywhere in this service to derive real content from. Left as explicit
gaps rather than fabricated per parity-principles #1.

**gopherstack-kwht follow-up (this pass)**: re-audited both "always empty" gaps against
their *surrounding* fields rather than accepting the label at face value. Both were
still genuine gaps (no fabricated content added), but each had a real, separately
fixable bug next to it:
- `GetFindingRecommendation` was missing two **required** response members entirely
  (`resourceArn`, `startedAt`) plus the optional `completedAt`, and its
  `recommendationType` wire value (`"UNUSED_PERMISSION"`) didn't match the real
  `types.RecommendationType` enum's only value, `"UnusedPermissionRecommendation"`
  (SDK `types/enums.go:579`, v1.51.4) -- a real client would never recognize the type it
  received. Also: `GenerateFindingRecommendation` created a recommendation record for
  *any* finding ID, including nonexistent ones, with no existence check at all; it now
  resolves the real finding (capturing its `resourceArn`) and 404s
  (`ResourceNotFoundException`) like `GetFindingRecommendation` already did.
- `StartPolicyGeneration` accepted `cloudTrailDetails` (`types.CloudTrailDetails`) from
  the request and silently discarded it -- a client-supplied-and-dropped bug, not a
  missing-analysis-engine one. It's now stored and echoed back by `GetGeneratedPolicy`
  as `properties.cloudTrailProperties` (`types.CloudTrailProperties`,
  `types/types.go:2375`), matching the pattern already established for
  `Analyzer.Configuration`.
- Also fixed in passing: `PolicyGenerationStatusRunning` was declared as `"RUNNING"`,
  which doesn't match the real `types.JobStatus` enum's `"IN_PROGRESS"`
  (`types/enums.go:394`). Currently unassigned (StartPolicyGeneration completes
  synchronously), so not a live bug, but wrong if ever used.
- `sdk_module` pin was stale (`v1.48.0` recorded vs. `v1.51.4` actually pinned in
  `go.mod`); corrected. All wire claims in this file re-checked against v1.51.4 sources
  during this pass; no other drift found.

**Wire-shape trap for future auditors**: `Finding`/`FindingSummary` (used by
GetFinding/ListFindings/GetFindingV2/ListFindingsV2) serialize the resource under the
JSON key **"resource"**, not "resourceArn". "resourceArn" is only correct for the
unrelated `AnalyzedResource` type (used by GetAnalyzedResource/ListAnalyzedResources) --
do NOT conflate the two; they look similar but differ on the wire.
`types.AccessPreviewFinding` (ListAccessPreviewFindings) is a THIRD, still different
shape again (`id`/`changeType`, no `analyzerArn` member at all) -- do not conflate it
with `Finding`/`FindingSummary` either, despite gopherstack modeling all three from the
same underlying `*Finding` record.

**Confirmed NOT stubs** (would look suspicious on a grep-only pass, verified by reading):
`ValidatePolicy`, `CheckAccessNotGranted`, `CheckNoNewAccess`, `CheckNoPublicAccess`
(`policy_analysis.go`) do genuine IAM policy statement evaluation (glob-matching
Action/Resource/Principal, Allow/Deny semantics) -- not always-empty/always-PASS stubs.
`StartPolicyGeneration` completes synchronously to `SUCCEEDED` rather than modeling an
async `IN_PROGRESS` state machine; this is a deliberate simplification (not a
stuck-forever no-op) since `GetGeneratedPolicy`/`ListPolicyGenerations` immediately
reflect the terminal state and a polling client will see it complete on the first call.

**Persistence**: `Handler.Snapshot`/`Handler.Restore` delegate to
`InMemoryBackend.Snapshot`/`Restore` (persistence.go), which round-trips all
`store.Registry`-registered tables (analyzers, findings, analyzedResources,
policyGenerations, accessPreviews, findingRecommendations) plus the "dirty" archiveRules
table via an ephemeral DTO registry, plus the plain `tags` map. The new
`Analyzer.Configuration json.RawMessage` field round-trips for free through the
generic JSON-marshal-based `store.Table[Analyzer]` Snapshot/Restore -- no DTO or special
casing needed (verified via `TestAnalyzerConfiguration`-style round-trip in
persistence_test.go's existing analyzer coverage plus manual review of store.Table's
marshal path).
