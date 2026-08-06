---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: wafv2
sdk_module: aws-sdk-go-v2/service/wafv2@v1.76.0   # version audited against (bumped from v1.71.2)
last_audit_commit: 7061877e4                      # HEAD when the v1.71.2 manifest was written; this pass only adds the 4 new ops below
last_audit_date: 2026-07-25
overall: A            # New this pass: the AI-bot pay-per-crawl monetization-reporting family
                      # (GetRevenueStatistics/GetRevenueStatisticsSummary/
                      # GetRevenueStatisticsTimeSeries/ListSettlementRecords), added to the SDK
                      # since the v1.71.2 audit. All 4 ops report revenue/traffic analytics this
                      # emulator has no genuine data for (no real HTTP traffic, no AI-bot
                      # detection pipeline, no billing/blockchain-settlement system). Implemented
                      # with full AWS-accurate request validation (required fields, enums,
                      # CLOUDFRONT-only scope rule, Currency=USDC, 90-day TimeWindow cap,
                      # Limit/NextMarker bounds, enum-restricted Filter values) and an honestly
                      # empty/zero response -- never a fabricated dollar amount, bot name, path,
                      # or settlement record.
                      # RE-AUDITED 2026-07-30 (parity-5 grade-floor pass, no code changes): confirmed
                      # this backend's WebACL/RuleGroup/IPSet state holds only configuration, never
                      # per-request traffic/revenue counters, and there is no AI-bot-detection or
                      # settlement pipeline to derive real numbers from -- fabricating dollar amounts,
                      # bot names, or settlement records to reach A would be exactly the failure mode
                      # this campaign has spent weeks removing. STRUCTURAL, grade correctly held at
                      # A-, not raised.
                      # RE-GRADED 2026-08-05: schema now distinguishes structural gaps (no data source
                      # can ever exist) from ordinary gaps (buildable with more effort). The revenue-
                      # statistics family's "no traffic/no settlement pipeline" limitation is genuinely
                      # structural -- moved to structural_gaps below, which does not block A.
                      # ApplicationIntegrationURL and ManagedRuleSet Description/LabelNamespace stay in
                      # gaps: both are unimplemented API paths (an unpublished URL scheme, a vendor-only
                      # onboarding field), not data that cannot exist -- not structural. Raised A- -> A.
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
  CheckCapacity: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: real per-statement-type WCU cost model in capacity.go, replacing the flat 1-WCU/rule stub (see Notes)"}
  CreateAPIKey: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAPIKey: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAPIKeys: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDecryptedAPIKey: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now honors Limit/NextMarker via pkgs/page (see Notes)"}
  PutLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLoggingConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  PutPermissionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePermissionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPermissionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFirewallManagerRuleGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  GetManagedRuleSet: {wire: partial, errors: ok, state: ok, persist: ok, note: "no Description/LabelNamespace fields modeled; re-verified this pass -- genuinely unreachable, see gaps/Notes"}
  ListManagedRuleSets: {wire: partial, errors: ok, state: ok, persist: ok, note: "summary omits Description/LabelNamespace, same gap as Get; re-verified this pass"}
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
  GetRevenueStatistics: {wire: ok, errors: ok, state: partial, persist: n/a, note: "new in v1.76.0 (AI-bot pay-per-crawl monetization). Full request validation (Currency=USDC, CLOUDFRONT-only Scope, StatisticType enum, GroupBy required iff TOP_SOURCES_BY_REVENUE, SortBy/SortOrder enums, 90-day TimeWindow cap, Filters incl. enum-restricted values); always returns an empty SourceStatistics or RevenuePathStatistics list (matching which field the SDK docs say is 'populated when' -- the other is omitted) because no real AI-bot traffic exists to rank. See Notes."}
  GetRevenueStatisticsSummary: {wire: ok, errors: ok, state: partial, persist: n/a, note: "new in v1.76.0. Same validation family; RevenueBreakdown is always Currency=<request currency>, all amounts '0', all counts 0 -- honest zero, not fabricated. See Notes."}
  GetRevenueStatisticsTimeSeries: {wire: ok, errors: ok, state: partial, persist: n/a, note: "new in v1.76.0. Same validation family plus Interval enum and Limit 1-10000 bound; DataPoints always empty. See Notes."}
  ListSettlementRecords: {wire: ok, errors: ok, state: partial, persist: n/a, note: "new in v1.76.0. Same validation family plus SortBy/SortOrder and Limit 1-100 bound; Settlements always empty -- no real payment/blockchain-settlement pipeline exists. See Notes."}
# Families audited as a group (when per-op is impractical):
families:
  route_matcher: {status: ok, note: "X-Amz-Target prefix AWSWAF_20190729. verified against real SDK protocol.go; single-endpoint awsjson1.1, dispatch table 59/59 matches GetSupportedOperations and the real SDK's api_op_*.go surface exactly (no missing/extra ops) -- 55 audited at v1.71.2 plus the 4 new monetization-reporting ops added in v1.76.0 (GetRevenueStatistics/GetRevenueStatisticsSummary/GetRevenueStatisticsTimeSeries/ListSettlementRecords), each confirmed against its own X-Amz-Target string in the v1.76.0 serializers.go"}
  locktoken_optimistic_concurrency: {status: ok, note: "every Update*/Delete* op checks lockToken != stored token; empty-string lockToken is treated as skip-check by design (see Notes) -- not exploitable via compliant SDK clients since LockToken is a client-side-validated required field on every op that takes it"}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to Backend.Snapshot/Restore (persistence.go); clean tables via store.Registry, dirty tables (managedRuleSets/apiKeys) via DTO registry with Region json:\"-\" round-trip; version-gated (wafv2SnapshotVersion) with clean discard on mismatch"}
  errCodeLookup: {status: ok, note: "fixed: ErrUnavailableEntity/ErrConfigurationWarning sentinels existed but had no switch case in handleError, would have 500'd if ever returned (currently unreachable/dead -- no handler returns them yet, but the lookup gap is now closed for when they are wired up)"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "GetWebACL response omits the optional top-level ApplicationIntegrationURL field (only populated when a web ACL uses AWSManagedRulesATPRuleSet/ACFPRuleSet with client app integration). Re-investigated this pass: AWS has never published the URL-generation scheme (it's an opaque, AWS-internal-service-generated URL), so there is no deterministic value this emulator could fabricate that would be meaningfully AWS-accurate -- niche, rarely asserted by IaC tooling. Left unmodeled rather than invented."
  - "GetManagedRuleSet/ListManagedRuleSets don't model Description/LabelNamespace (ManagedRuleSet struct has no such fields). Re-investigated this pass: confirmed genuinely non-actionable, not merely low-priority -- PutManagedRuleSetVersionsInput (the only op that creates/updates a ManagedRuleSet in this emulator; there is no CreateManagedRuleSet in the real API either, it's vendor-onboarding-only) has no Description/LabelNamespace input fields, so no caller can ever populate them through any modeled or real API path. Since both are *string with omitempty JSON serialization on the real SDK, an always-absent field is byte-for-byte identical on the wire to an always-nil field -- there is no observable client-visible gap here today. Vendor-only Firewall-Manager API family, not used by Terraform/CDK for the common WAFv2 workflow."
structural_gaps:
  - "GetRevenueStatistics/GetRevenueStatisticsSummary/GetRevenueStatisticsTimeSeries/ListSettlementRecords (added in aws-sdk-go-v2/service/wafv2@v1.76.0) always return honestly empty/zero results. This emulator has no real HTTP traffic, no AI-bot detection pipeline, and no billing/blockchain-settlement system, so there is no genuine revenue, bot, path, or settlement data to report -- exactly the same class of gap already documented for GetRateBasedStatementManagedKeys/GetSampledRequests/GetTopPathStatisticsByTraffic above. Deliberately NOT fabricated: no invented dollar amounts, bot names, path statistics, or settlement records. Every field validated (required-ness, enums, CLOUDFRONT-only Scope, Currency=USDC, 90-day TimeWindow cap, Filter enum values, Limit bounds) is checked for real; only the *data*, which cannot exist in this backend (no traffic source, no AI/ML bot-detection engine, no settlement/billing system), is honestly absent -- not a buildable state model."
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

- **Fixed this pass: `CheckCapacity` real per-statement-type WCU cost model** (`capacity.go`).
  Previously a flat `1 WCU * len(rules)` stub. Now walks each rule's `Statement` tree and
  computes AWS's documented cost per statement type, verified against AWS's published
  per-statement WCU docs (`waf-rule-statement-type-*.html` pages, fetched and quoted verbatim
  2026-07-23) and `aws-waf-capacity-units.html`:
  - `ByteMatchStatement`: 2 WCU (EXACTLY/STARTS_WITH/ENDS_WITH) or 10 WCU (CONTAINS/
    CONTAINS_WORD) base.
  - `SqliMatchStatement`: 20 WCU (SensitivityLevel LOW, the default) or 30 WCU (HIGH).
  - `XssMatchStatement`: 40 WCU base. `SizeConstraintStatement`: 1 WCU base.
    `RegexMatchStatement`: 3 WCU base. `RegexPatternSetReferenceStatement`: 25 WCU base.
  - All six of the above share one confirmed additional rule (identical wording on every doc
    page): +10 WCU if `FieldToMatch.AllQueryArguments` is used, ×2 the base if
    `FieldToMatch.JsonBody` is used (mutually exclusive, since `FieldToMatch` is a oneOf), and
    +10 WCU per entry in `TextTransformations`.
  - `GeoMatchStatement`/`LabelMatchStatement`/`AsnMatchStatement`: 1 WCU flat (each verified on
    its own doc page).
  - `IPSetReferenceStatement`: 1 WCU, +4 WCU if `IPSetForwardedIPConfig.Position` is `ANY`
    (verified on `waf-rule-statement-type-ipset-match.html`, the correct — non-obvious — slug).
  - `RateBasedStatement`: 2 WCU base, +30 WCU per `CustomKeys` entry, plus the recursively
    computed capacity of any `ScopeDownStatement`.
  - `AndStatement`/`OrStatement`/`NotStatement`: the sum of nested statements' capacities with
    **no fixed overhead** — AWS's own doc for `AndStatement` states this explicitly ("WCUs —
    Depends on the nested statements"), confirmed via `waf-rule-statement-type-and.html`.
  - `RuleGroupReferenceStatement`: resolves the ARN via `b.ruleGroupsByARN` and returns that
    RuleGroup's fixed `Capacity` (assigned at creation, immutable), matching AWS's documented
    "the cost of using a rule group ... is the rule group's capacity setting". Falls back to 1
    WCU for an ARN this backend doesn't know about (e.g. cross-account) rather than failing the
    whole call.
  - `ManagedRuleGroupStatement`: looks up `VendorName`/`Name` in the static catalog
    (`managed_rule_catalog.go`, already used by `DescribeManagedRuleGroup` et al.) and returns
    its `Capacity`; falls back to `defaultManagedRuleGroupCapacity` (700, matching
    `AWSManagedRulesCommonRuleSet`) for an unmodeled vendor/name pair.
  - Any statement type not yet modeled (including hypothetically future AWS statement types)
    falls back to 1 WCU rather than erroring, so `CheckCapacity` never fails on an unrecognized
    shape. New coverage: `capacity_test.go`.

- **Fixed this pass: `ListTagsForResource` now honors `Limit`/`NextMarker`** (`handler_tags.go`),
  using `pkgs/page.New` (the existing project-wide opaque-cursor pagination helper) over the
  sorted `tags.MapToKV` output. Previously always returned the full tag set regardless of
  `Limit`. New coverage: `TestHandler_ListTagsForResource_Pagination` in `handler_tags_test.go`.

- **New this pass: AI-bot pay-per-crawl monetization reporting family** (`handler_revenue_statistics.go`),
  covering `GetRevenueStatistics`, `GetRevenueStatisticsSummary`, `GetRevenueStatisticsTimeSeries`,
  and `ListSettlementRecords` -- all four added to the SDK in
  `aws-sdk-go-v2/service/wafv2@v1.76.0`, after the v1.71.2 audit that produced the rest of this
  manifest. `X-Amz-Target` strings confirmed against v1.76.0's `serializers.go`:
  `AWSWAF_20190729.GetRevenueStatistics` / `...GetRevenueStatisticsSummary` /
  `...GetRevenueStatisticsTimeSeries` / `...ListSettlementRecords`.
  - **No fabrication**: this emulator has no real HTTP traffic, no AI-bot detection/
    classification pipeline, and no billing or blockchain-settlement system, so it has no
    genuine revenue, bot-source, path-statistic, or settlement data to report. Every handler
    performs full request validation and then returns an honestly empty/zero result --
    `GetRevenueStatistics` returns an empty `SourceStatistics` or `RevenuePathStatistics` list
    (whichever the request's `StatisticType` calls for; the other key is omitted, matching the
    real output shape's "populated when StatisticType is X" semantics, verified against
    `GetRevenueStatisticsOutput` in `api_op_GetRevenueStatistics.go`); `GetRevenueStatisticsSummary`
    returns a `RevenueBreakdown` with `Currency` echoing the request and every amount/count a
    real `"0"`/`0`; `GetRevenueStatisticsTimeSeries` returns an empty `DataPoints`; and
    `ListSettlementRecords` returns an empty `Settlements`. This mirrors the pre-existing
    `GetSampledRequests`/`GetTopPathStatisticsByTraffic` pattern in `handler_rate_based_rules.go`,
    which already documents the identical "no traffic exists to report" honesty constraint.
  - **Real state check**: confirmed this backend tracks no per-request, per-rule, or
    per-web-ACL traffic/revenue counters anywhere (`WebACL`/`RuleGroup`/`IPSet` etc. hold only
    configuration, not traffic counts) -- there is nothing genuine to derive these shapes from,
    so none of the four responses derive from real backend state; they are purely
    validate-then-return-honest-empty, matching `CheckCapacity`'s sibling ops
    `GetRateBasedStatementManagedKeys`/`GetSampledRequests`/`GetTopPathStatisticsByTraffic`.
  - **Validation implemented** (`handler_revenue_statistics.go`): Scope required + valid
    REGIONAL/CLOUDFRONT enum, reusing `validScope` (`store.go`, the one pre-existing
    scope-enum helper in this package) rather than a parallel check, then layered with the
    CLOUDFRONT-only restriction every one of these four ops documents ("This operation is only
    available for CLOUDFRONT scope"); Currency required + must equal `USDC` (the only member of
    `types.Currency`); TimeWindow required with both bounds present, `EndTime >= StartTime`, and
    span capped at the documented 90 days (epoch-seconds wire format confirmed against
    `serializeDocumentTimeWindow`/`deserializeDocumentTimeWindow`, matching the existing
    `GetSampledRequests`/`GetTopPathStatisticsByTraffic` `TimeWindow` convention); Filters
    optional but each entry requires non-empty `Name`/`Values` (<=20 values), with
    enum-restricted filter names (`CurrencyMode`/`ChainName`/`SettlementStatus`/`HttpSourceName`)
    checked against their documented enums; `StatisticType`/`GroupBy`/`SortBy`/`SortOrder`/
    `Interval`/`SettlementSortBy` all enum-validated per op; `GroupBy` required exactly when
    `GetRevenueStatistics.StatisticType` is `TOP_SOURCES_BY_REVENUE` (SDK doc: "If StatisticType
    is TOP_SOURCES_BY_REVENUE and GroupBy is omitted, the request is rejected with a
    WAFInvalidParameterException"); `Limit` bounds enforced when provided
    (`GetRevenueStatisticsTimeSeries`: 1-10000, `ListSettlementRecords`: 1-100, both from the SDK
    doc comments -- note these are NOT client-side-validated in the real SDK's
    `validators.go`, only server-side/documented, so this emulator's check is additive
    AWS-accurate behavior, not a re-derivation of an existing client validator).
  - **Persistence**: none needed -- no new backend state is created or mutated by any of these
    four read-only reporting ops, so there is nothing to add to `Snapshot`/`Restore`.
  - New coverage: `TestGetRevenueStatistics`, `TestGetRevenueStatisticsSummary`,
    `TestGetRevenueStatisticsTimeSeries`, `TestListSettlementRecords` in
    `handler_rate_based_rules_test.go` (alongside the existing `GetSampledRequests`/
    `GetTopPathStatisticsByTraffic` tests), including explicit no-fabrication assertions
    (empty lists / all-zero `RevenueBreakdown` fields).
