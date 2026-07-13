---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: wafv2
sdk_module: aws-sdk-go-v2/service/wafv2@v1.71.2   # version audited against
last_audit_commit: 22d69640                       # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # genuine fixes found this pass (wire-shape gap across 4 Create* ops + errCodeLookup completeness)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateWebACL: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Summary was missing Description field"}
  GetWebACL: {wire: ok, errors: ok, state: ok, persist: ok, note: "ApplicationIntegrationURL top-level field not modeled (see gaps)"}
  UpdateWebACL: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWebACL: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWebACLs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateIPSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Summary was missing Description field"}
  GetIPSet: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateIPSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIPSet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIPSets: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRegexPatternSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Summary was missing Description field"}
  GetRegexPatternSet: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRegexPatternSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRegexPatternSet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRegexPatternSets: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Summary was missing Description field"}
  GetRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRuleGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly blocks delete while referenced by a WebACL rule"}
  ListRuleGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateWebACL: {wire: ok, errors: ok, state: ok, persist: ok, note: "resource-type allowlist is deliberately permissive for unknown types (see Notes)"}
  DisassociateWebACL: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent no-op on missing association, matches AWS"}
  GetWebACLForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourcesForWebACL: {wire: ok, errors: ok, state: ok, persist: ok}
  CheckCapacity: {wire: ok, errors: ok, state: partial, note: "flat 1 WCU/rule instead of AWS's per-statement cost model (see gaps)"}
  CreateAPIKey: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAPIKey: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAPIKeys: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDecryptedAPIKey: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: partial, errors: ok, state: ok, persist: ok, note: "ignores Limit/NextMarker (see gaps); low impact, max 50 tags/resource"}
  PutLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLoggingConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  PutPermissionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePermissionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPermissionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFirewallManagerRuleGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  GetManagedRuleSet: {wire: partial, errors: ok, state: ok, persist: ok, note: "no Description/LabelNamespace fields modeled; low-traffic vendor-only API"}
  ListManagedRuleSets: {wire: partial, errors: ok, state: ok, persist: ok, note: "summary omits Description/LabelNamespace, same gap as Get"}
  PutManagedRuleSetVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateManagedRuleSetVersionExpiryDate: {wire: ok, errors: ok, state: ok, persist: ok, note: "epoch-seconds int64 pass-through, verified vs deserializers.go"}
  GetRateBasedStatementManagedKeys: {wire: ok, errors: ok, state: partial, note: "always returns empty ManagedKeys lists (no rate-limiting simulation); documented AWS-accurate empty shape"}
  GetSampledRequests: {wire: ok, errors: ok, state: partial, note: "always returns empty SampledRequests/PopulationSize=0; no traffic sampling exists to report"}
  GetTopPathStatisticsByTraffic: {wire: ok, errors: ok, state: partial, note: "always returns empty UrlStatistics; no traffic exists to report"}
  DescribeAllManagedProducts: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static catalog, no persistence needed"}
  DescribeManagedProductsByVendor: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeManagedRuleGroup: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListAvailableManagedRuleGroups: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListAvailableManagedRuleGroupVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  GenerateMobileSdkReleaseUrl: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetMobileSdkRelease: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListMobileSdkReleases: {wire: ok, errors: ok, state: ok, persist: n/a}
# Families audited as a group (when per-op is impractical):
families:
  route_matcher: {status: ok, note: "X-Amz-Target prefix AWSWAF_20190729. verified against real SDK protocol.go; single-endpoint awsjson1.1, dispatch table 55/55 matches GetSupportedOperations and the real SDK's api_op_*.go surface exactly (no missing/extra ops)"}
  locktoken_optimistic_concurrency: {status: ok, note: "every Update*/Delete* op checks lockToken != stored token; empty-string lockToken is treated as skip-check by design (see Notes) -- not exploitable via compliant SDK clients since LockToken is a client-side-validated required field on every op that takes it"}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to Backend.Snapshot/Restore (persistence.go); clean tables via store.Registry, dirty tables (managedRuleSets/apiKeys) via DTO registry with Region json:\"-\" round-trip; version-gated (wafv2SnapshotVersion) with clean discard on mismatch"}
  errCodeLookup: {status: ok, note: "fixed: ErrUnavailableEntity/ErrConfigurationWarning sentinels existed but had no switch case in handleError, would have 500'd if ever returned (currently unreachable/dead -- no handler returns them yet, but the lookup gap is now closed for when they are wired up)"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "CheckCapacity uses a flat 1 WCU per rule instead of AWS's real per-statement-type capacity cost model (e.g. RateBasedStatement, regex/byte-match cost more). Correct emulation requires porting AWS's published WCU table; deferred as a distinct, larger effort."
  - "GetWebACL response omits the optional top-level ApplicationIntegrationURL field (only populated when a web ACL uses AWSManagedRulesATPRuleSet/ACFPRuleSet with client app integration -- niche, rarely asserted by IaC tooling)."
  - "ListTagsForResource ignores Limit/NextMarker pagination params and always returns the full tag set. Low impact: AWS caps tags at 50/resource (maxTagsPerResource), well under any default page size."
  - "GetManagedRuleSet/ListManagedRuleSets don't model Description/LabelNamespace (ManagedRuleSet struct has no such fields). Vendor-only Firewall-Manager API family, not used by Terraform/CDK for the common WAFv2 workflow."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; all state is InMemoryBackend maps + store.Table guarded by lockmetrics.RWMutex; Reset()/resetTablesLocked() cover all fields including the two \"dirty\" (unregistered) tables"}
---

## Notes

- Protocol is awsjson1.1: single POST endpoint, `X-Amz-Target: AWSWAF_20190729.<Op>`. Route
  matcher (`RouteMatcher`) does a header-prefix match; confirmed the dispatch table's 55 keys
  are byte-identical to `GetSupportedOperations()` and to the real SDK's 55
  `api_op_*.go` file stems (v1.71.2) — no missing or extra operations.

- **LockToken empty-string bypass is deliberate, not a bug**: every Update/Delete op does
  `if lockToken != "" && lockToken != stored.LockToken { return ErrOptimisticLock }`. An
  empty/omitted LockToken skips the match check entirely rather than failing. This looks like
  the classic "op that ignores optimistic-concurrency" bug class, but two things confirm it's
  intentional and low-risk:
  1. LockToken is a smithy `@required` field on every Update*/Delete* input in the real SDK
     (verified in `api_op_Update*.go`/`api_op_Delete*.go`), so aws-sdk-go-v2 and botocore both
     refuse to serialize/send the request client-side if it's missing — real traffic (including
     Terraform's `aws_wafv2_*` resources) can never reach the emulator with an empty token.
  2. The existing test suite has explicit `LockToken: ""` cases asserting 200 OK success
     (`handler_test.go` `TestHandler_UpdateWebACL`/`TestHandler_DeleteWebACL` "existing" cases),
     so this is validated, deliberate test-convenience behavior, not an oversight. Left as-is
     to avoid a large, low-value blast radius across 5 test files for behavior unreachable by
     compliant clients.

- **`validateAssociationScope` (handler.go) is deliberately permissive**, not a disguised
  no-op: it rejects CLOUDFRONT WebACL ARNs (`/global/`) but always returns nil for REGIONAL
  ones regardless of whether the resource ARN's service is in `regionalResourceServices` — the
  code comment says this is intentional ("If service is unrecognised, still allow, for
  compatibility with unknown resource types"), guarding against the allowlist going stale as
  AWS adds new associable resource types (Amplify, Verified Access, etc.). Confirmed
  intentional via the comment; not treated as a bug.

- Fixed this pass: `CreateWebACL`/`CreateIPSet`/`CreateRegexPatternSet`/`CreateRuleGroup`
  responses were missing `Description` in their `Summary` object. Real
  `WebACLSummary`/`IPSetSummary`/`RegexPatternSetSummary`/`RuleGroupSummary` (types.go in the
  real SDK) all carry `ARN/Description/Id/LockToken/Name`; gopherstack only ever emitted
  `Id/Name/ARN/LockToken`. The corresponding `List*`/`Get*` handlers already included
  Description correctly — only the four `Create*` summaries had the gap. Verified no existing
  test asserts an exact-map equality on `Summary` (all access individual keys), so no test
  changes were needed.

- Fixed this pass: `handleError`'s error-type switch (the wafv2 equivalent of an
  `errCodeLookup` table) was missing cases for the `ErrUnavailableEntity` and
  `ErrConfigurationWarning` sentinels declared in backend.go. Neither is currently returned by
  any handler (dead code today), so this was not yet an observable bug, but leaving it
  unmapped meant `errors.Is(err, awserr.ErrConflict)` would have silently reclassified a future
  `ErrUnavailableEntity` as `WAFDuplicateItemException` (wrong exception name) and
  `ErrConfigurationWarning` would have fallen through to the generic 500
  `WAFInternalErrorException` (since it wraps `awserr.ErrInvalidParameter`, a different
  sentinel than the local `errInvalidRequest`). Added explicit cases mapping to
  `WAFUnavailableEntityException` / `WAFConfigurationWarningException` at 400, matching AWS's
  documented exception names, so the lookup table is complete before either sentinel is wired
  up to an actual code path.

- WebACL/IPSet/RegexPatternSet/RuleGroup all use a "clean" `store.Table[T]` registered on
  `b.registry`, keyed by `region+id` composite (`regionKey`), with `store.Index` for
  by-ARN/by-name+scope/by-region lookups — see `store_setup.go`'s file doc for the full
  Phase-3.3 rationale. `managedRuleSets`/`apiKeys` are "dirty" tables (their storage region
  isn't recoverable from other fields) round-tripped through a separate DTO registry in
  `persistence.go`. Both halves are exercised correctly by `Snapshot`/`Restore`.

- CLOUDFRONT (global) resources always store/lookup under region key `""`; REGIONAL resources
  use the request region. `lookupXByID` helpers fall back to the `""` bucket after a
  region-specific miss so CLOUDFRONT resources remain reachable regardless of the caller's
  request region — this is intentional and matches AWS's global-resource behavior, not a bug.

- Timestamps: `ManagedRuleSetVersion`/`UpdateManagedRuleSetVersionExpiryDate` pass epoch-second
  `*int64` straight through from request to response, matching the real SDK's
  `smithytime.ParseEpochSeconds(f64)` deserializer (verified in `deserializers.go`) — no
  ISO8601-vs-epoch mismatch here (unlike the QuickSight/IoT bug class in the parity memory).
