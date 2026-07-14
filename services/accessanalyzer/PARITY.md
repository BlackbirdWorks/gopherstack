---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: accessanalyzer
sdk_module: aws-sdk-go-v2/service/accessanalyzer@v1.48.0
last_audit_commit: 7d7a3363
last_audit_date: 2026-07-12
overall: A            # critical routing bug found and fixed for a high-traffic op family
ops:
  CreateAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAnalyzers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAnalyzer: {wire: ok, errors: ok, state: partial, persist: ok, note: "PUT /analyzer/{name} routing correct; Configuration (AnalyzerConfiguration union) not modeled/persisted -- backend treats it as a no-op refresh. Response omits configuration content, which is optional on the wire, so this is not a wire bug, just an unmodeled feature (gap)."}
  CreateServiceLinkedAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteServiceLinkedAnalyzer: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateArchiveRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "auto-archives existing active findings on creation, matching real AWS behavior"}
  GetArchiveRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListArchiveRules: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteArchiveRule: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateArchiveRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ApplyArchiveRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFinding: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: was routed at GET /analyzer/{name}/finding/{id} (fabricated path never sent by a real SDK client, and outside the RouteMatcher's /analyzer prefix -- unreachable). Real path is GET /finding/{id}?analyzerArn=... Also fixed wire shape: resource was serialized as \"resourceArn\" (real API key is \"resource\"); resourceOwnerAccount/analyzedAt were entirely missing (both required fields)."}
  ListFindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: was routed at POST /analyzer/{name}/findings (fabricated, unreachable). Real path is POST /finding with analyzerArn in the JSON body. Same resource/resourceOwnerAccount/analyzedAt wire-shape fix as GetFinding. analyzerArn is now validated as required (matches SDK's required-field contract) instead of being silently read-and-ignored."}
  UpdateFindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: was routed at PUT /analyzer/{name}/findings (fabricated, unreachable). Real path is PUT /finding with analyzerArn in the JSON body (was parsed but silently discarded in favor of a path segment that never existed on the wire)."}
  GetFindingV2: {wire: partial, errors: ok, state: ok, persist: ok, note: "FIXED wire-shape half of THIS PASS (resource/resourceOwnerAccount). findingDetails ([]types.FindingDetails, a large union of ExternalAccessDetails/UnusedIAMRoleDetails/UnusedIAMUserAccessKeyDetails/UnusedIAMUserPasswordDetails/UnusedPermissionDetails) is NOT modeled -- always returns []. findingType also not populated. Deliberately left as an explicit gap rather than fabricating a partial union (parity-principles #1)."}
  ListFindingsV2: {wire: partial, errors: ok, state: ok, persist: ok, note: "Same fix + same findingDetails/findingType gap as GetFindingV2 (FindingSummary omits findingDetails but still lacks findingType)."}
  GetFindingsStatistics: {wire: ok, errors: ok, state: ok, persist: ok}
  GenerateFindingRecommendation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingRecommendation: {wire: partial, errors: ok, state: ok, persist: ok, note: "recommendedSteps always []; RecommendationType/Status are real fields backed by state"}
  GetAnalyzedResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAnalyzedResources: {wire: ok, errors: ok, state: ok, persist: ok}
  StartResourceScan: {wire: ok, errors: ok, state: ok, persist: n/a, note: "verifies analyzer exists by ARN; no actual resource scanning to simulate (matches other AA scan endpoints elsewhere in gopherstack)"}
  StartPolicyGeneration: {wire: ok, errors: ok, state: ok, persist: ok, note: "completes synchronously (SUCCEEDED immediately) rather than modeling async IN_PROGRESS -- acceptable since it still reaches a real terminal state and GetGeneratedPolicy/ListPolicyGenerations reflect it; not a stuck-forever no-op"}
  GetGeneratedPolicy: {wire: partial, errors: ok, state: ok, persist: ok, note: "generatedPolicies always []; jobDetails/properties.principalArn are real"}
  CancelPolicyGeneration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicyGenerations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAccessPreview: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessPreview: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccessPreviews: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccessPreviewFindings: {wire: partial, errors: ok, state: ok, persist: ok, note: "real op returns AccessPreviewFinding (changeType, existingFindingId, etc.), not Finding; gopherstack reuses findingToJSON (v1 Finding shape) as an approximation -- resource/resourceOwnerAccount/analyzedAt now correct after this pass's fix, but changeType and other AccessPreviewFinding-only fields are absent. Pre-existing gap, not a regression."}
  CheckAccessNotGranted: {wire: ok, errors: ok, state: ok, persist: n/a, note: "genuine IAM policy evaluation (policy_analysis.go), not a stub"}
  CheckNoNewAccess: {wire: ok, errors: ok, state: ok, persist: n/a}
  CheckNoPublicAccess: {wire: ok, errors: ok, state: ok, persist: n/a}
  ValidatePolicy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "genuine structural/semantic policy validation, not always-empty"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "audited every op's real path+method against aws-sdk-go-v2 serializers.go this pass. Found and fixed the GetFinding/ListFindings/UpdateFindings mismatch (see ops above). All other families (analyzer, archive-rule, access-preview, service-linked-analyzer, recommendation, findingv2, policy/*, analyzed-resource, tags) verified to match real REST paths and HTTP methods exactly."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "GetFindingV2/ListFindingsV2 findingDetails ([]types.FindingDetails union) and findingType always empty/absent -- large feature (5 distinct nested detail shapes: ExternalAccessDetails, UnusedIAMRoleDetails, UnusedIAMUserAccessKeyDetails, UnusedIAMUserPasswordDetails, UnusedPermissionDetails) not modeled by InMemoryBackend at all; fabricating one shape would itself be a disguised partial stub, so left as an explicit gap per parity-principles #1 rather than fixed this pass. No bd issue filed yet."
  - "ListAccessPreviewFindings returns the v1 Finding shape instead of AccessPreviewFinding (missing changeType/existingFindingId/existingFindingStatus). No bd issue filed yet."
  - "UpdateAnalyzer's Configuration (AnalyzerConfiguration union, used for internal/unused-access analyzer settings) is accepted on neither request read nor persisted; response always returns an empty configuration object. Low impact since Configuration is optional on the wire."
  - "pathAnalyzedResource (\"analyzedResource\", camelCase, no hyphen) is dead legacy routing left over from before the real \"analyzed-resource\" (hyphenated) path was added; RouteMatcher still claims it but parseRESTPath/parseRESTPathAppendixA never resolve an op for it, so it 404s. Harmless (no real SDK client sends this path) but worth deleting in a future cleanup pass."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "backend.go/backend_appendixa.go internal locking/persistence audited only incidentally (via the findingToJSON accountID threading change); no correctness issues observed, but a dedicated pass wasn't done this round"
leaks: {status: clean, note: "no goroutines/janitors in this service; all state is synchronous map/store access under lockmetrics.RWMutex"}
---

## Notes

**Protocol**: restjson1. Timestamps are ISO8601 strings via `smithytime.ParseDateTime`
on the real deserializer side (NOT epoch-seconds) -- `time.RFC3339` formatting used
throughout gopherstack's handler.go/handler_appendixa.go is correct; do not "fix" this
to `awstime.Epoch` in a future pass.

**The critical bug this pass** (route-matcher class, same family that has previously hit
backup/eks/s3control/guardduty/cleanrooms/iotwireless/appsync/kafka/bedrock/efs/appconfig/
macie2/xray/elasticsearch): GetFinding, ListFindings, and UpdateFindings were routed at
`/analyzer/{name}/finding/{id}` (GET), `/analyzer/{name}/findings` (POST/PUT) -- paths that
do not exist anywhere in the real API. The actual aws-sdk-go-v2 serializers put all three
at the top level: `GET /finding/{id}?analyzerArn=...`, `POST /finding` (analyzerArn in the
JSON body), `PUT /finding` (analyzerArn in the JSON body). Because gopherstack's
`RouteMatcher` only claimed paths under `/analyzer`, a real SDK client's `GetFinding`/
`ListFindings`/`UpdateFindings` calls would not even be routed to this handler -- they'd
404 (or fall through to whatever else is registered). This is exactly the "unit tests
bypass the matcher" trap: `handler_appendixa_test.go`/`handler_test.go` never had a single
test that called `GetFinding`/`ListFindings`/`UpdateFindings` through `h.Handler()` at all
(only direct `InMemoryBackend.GetFinding(analyzerName, ...)` calls in `backend_test.go`,
which don't exercise routing). Fixed by adding `/finding` + `/finding/{id}` to the
RouteMatcher and path parser, and rewriting the three handlers to read `analyzerArn`
from the query string (Get) / JSON body (List, Update) instead of a nonexistent path
segment, converting ARN -> analyzer name via the existing `analyzerNameFromArn` helper.
Added `TestGetFinding`/`TestListFindings`/`TestUpdateFindings` in `handler_test.go`,
all driven through `h.Handler()` (not backend calls), plus new `RouteMatcher` test rows
for `/finding` and `/finding/{id}` (and `/findingv2` boundary cases) to lock this in.

**Wire-shape trap for future auditors**: `Finding`/`FindingSummary` (used by
GetFinding/ListFindings/GetFindingV2/ListFindingsV2/ListAccessPreviewFindings) serialize
the resource under the JSON key **"resource"**, not "resourceArn". "resourceArn" is only
correct for the unrelated `AnalyzedResource` type (used by GetAnalyzedResource/
ListAnalyzedResources) -- do NOT conflate the two; they look similar but differ on the
wire. Also required-but-easy-to-miss fields on Finding/FindingSummary:
`resourceOwnerAccount` (string) and `analyzedAt` (timestamp). gopherstack does not track
per-finding resource-owner account, so `resourceOwnerAccount` defaults to the backend's
own `AccountID()` (reasonable since emulated resources belong to the same test account);
`analyzedAt` mirrors `UpdatedAt` (same convention the pre-existing GetFindingV2 code
already used for the same underlying `Finding.UpdatedAt` field).

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
table via an ephemeral DTO registry, plus the plain `tags` map. Verified wired correctly;
not touched this pass.
