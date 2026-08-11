---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: xray
sdk_module: aws-sdk-go-v2/service/xray@v1.39.4   # version audited against (go.mod pin; was stale at v1.36.20)
last_audit_commit: b72533e7a                       # HEAD when this manifest was last rewritten
last_audit_date: 2026-08-10
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  PutTraceSegments: {wire: ok, errors: ok, state: ok, persist: ok}
  PutTelemetryRecords: {wire: ok, errors: ok, state: ok, persist: deferred, note: "ring buffer, intentionally ephemeral"}
  GetTraceSummaries: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): EntryPoint was a plain string, real wire shape is a ServiceId object {Name,Type} -- a real client's deserializer errors on a string here; per-item StartTime was entirely missing (a required real-API field); per-item ApproximateTime was a gopherstack-INVENTED field (DELETED) -- the real ApproximateTime is an envelope-level field on GetTraceSummariesOutput (now added there instead)"}
  BatchGetTraces: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): added missing LimitExceeded field (always false; gopherstack does not enforce/track the trace-document size limit, matching the not-exceeded case)"}
  GetServiceGraph: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): Edge objects now carry SummaryStatistics/StartTime/EndTime, aggregated from the downstream segment on each edge (buildEdgeStats reuses accumulateNodeStats). Also FIXED a direction bug: edgeKey{From,To} was built as {callee,caller} (buildEdgeSet), so nodeToView attached each edge to the DOWNSTREAM node pointing back at its caller -- backwards from the real Edge doc ('Connections to downstream services', types/types.go:1192 on Service.Edges), and meant every real client's rendered service map had arrows running the wrong way, and the upstream node's own Edges list was always empty. Now From=caller/To=callee, matching real semantics. EdgeType intentionally left unset: it is only populated for async 'link' edges (types/types.go:114-115), and gopherstack does not model segment links, so omitting it is the honest case, not a gap. See handler_service_graph_test.go:TestGetServiceGraph_EdgeStatisticsAndDirection."}
  GetTraceGraph: {wire: ok, errors: ok, state: ok, persist: ok, note: "same edge-statistics/direction fix as GetServiceGraph (shared buildServiceGraph)"}
  GetTimeSeriesServiceStatistics: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): InsightsConfiguration was parsed from the request body and silently discarded -- UpdateGroup could never actually change insights/notifications settings. Also FIXED: FilterExpression was unconditionally overwritten (including with empty string) even when the caller only wanted to change InsightsConfiguration; both fields are now independently optional (pointer/patch semantics), matching real UpdateGroupInput"}
  DeleteGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSamplingRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): added missing SamplingRateBoost field (config passthrough only, see gaps) and missing RuleLimitExceededException cap enforcement (2000 rules/account, AWS default quota)"}
  GetSamplingRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): SamplingRateBoost now included in samplingRuleView"}
  UpdateSamplingRule: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): added RuleARN-based lookup (previously RuleName-only; real SamplingRuleUpdate allows specifying either); added SamplingRateBoost update support. FIXED 2026-08-07 (gopherstack-6iwu): the real SamplingRuleUpdate type (types.go, confirmed against aws-sdk-go-v2/service/xray) has an Attributes map[string]string field that samplingRuleUpdateInput had no field for at all, so a real client's UpdateSamplingRule Attributes value was silently dropped by json.Unmarshal even though Attributes round-tripped correctly on CreateSamplingRule -- added Attributes to samplingRuleUpdateInput/SamplingRuleUpdate, threaded it into UpdateSamplingRuleWithPointers (maps.Clone on provided, nil leaves unchanged, matching every other optional-pointer field's semantics), and reverted the xray dashboard's read-only-Attributes workaround now that the backend accepts it. Verified with TestHandler_UpdateSamplingRule_Attributes."}
  DeleteSamplingRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): added RuleARN-based lookup, and fixed the Default-rule-undeletable check to run against the resolved rule's name (previously checked the raw ruleName parameter, which combined with an ARN-lookup path would have let a caller delete Default by ARN)"}
  GetSamplingStatisticSummaries: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (prior pass): REST path was /GetSamplingStatisticSummaries, real SDK sends /SamplingStatisticSummaries"}
  GetSamplingTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (prior pass): REST path was /GetSamplingTargets, real SDK sends /SamplingTargets. FIXED (this pass), STUB CLASS BUG: SamplingBoostStatisticsDocuments was completely absent from the request struct (silently dropped by json.Unmarshal for any real client using SamplingRateBoost) and UnprocessedBoostStatistics was absent from the response envelope. Now parsed/echoed: a boost document for a known rule is accepted (not reported unprocessed), a boost document for an unknown rule is reported in UnprocessedBoostStatistics, same as SamplingStatisticsDocuments. SamplingTargetDocument.SamplingBoost is deliberately left unset in every case -- an earlier version of this fix computed a fabricated BoostRate by linearly interpolating FixedRate/MaxRate against the observed anomaly ratio, but AWS does not publish the real boost-trigger algorithm (API_SamplingBoostStatisticsDocument.html describes the inputs only qualitatively) and a plausible-looking invented rate is a worse gap than an absent one: a client reads and acts on a boost rate, so a wrong-but-plausible number does more damage than a missing field a client would notice and investigate. This is now an honest 'accepted, no engine behind it' gap, same shape as GetInsightImpactGraph's Services: []."}
  GetEncryptionConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "real SDK always POST /EncryptionConfig; handler also accepts GET, harmless superset"}
  PutEncryptionConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelTraceRetrieval: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): previously a silent idempotent no-op on an unknown RetrievalToken -- PARITY.md previously (incorrectly) asserted this 'matches AWS' without checking the modeled error set. CancelTraceRetrieval declares ResourceNotFoundException (confirmed in deserializers.go's awsRestjson1_deserializeOpErrorCancelTraceRetrieval switch); an unknown token now returns 400 ResourceNotFoundException, and cancelling the same token twice now correctly fails on the second call"}
  StartTraceRetrieval: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRetrievedTraces: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): each RetrievedTrace's document-list field was wire key \"Segments\"; the real field is \"Spans\" (types.Span{Document,Id}) -- awsRestjson1_deserializeDocumentRetrievedTrace only recognizes \"Spans\" and silently drops unknown keys, so every real SDK client received an EMPTY Spans list for every retrieved trace despite a 200 response. Also FIXED: unknown RetrievalToken now returns ResourceNotFoundException (see CancelTraceRetrieval) instead of a fabricated COMPLETE/empty response. Also added the previously-missing TraceFormat field (always \"XRAY\": gopherstack never stores OTEL-format spans)"}
  GetRetrievedTracesGraph: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): same unknown-token ResourceNotFoundException fix as CancelTraceRetrieval/ListRetrievedTraces"}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): PolicyRevisionId was parsed by the handler but never passed to/enforced by the backend -- the atomic/guarded delete this parameter exists for was a complete no-op. Now validated against the stored policy's current revision, returning InvalidPolicyRevisionIdException on mismatch"}
  ListResourcePolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): resourcePolicyView now includes LastUpdatedTime (see PutResourcePolicy)"}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (prior pass): (1) ResourcePolicy.LastUpdatedTime was completely absent from the model and wire view (a real, documented field: 'When the policy was last updated, in Unix time seconds') -- added and set on every Put; (2) the max-5-policies violation used the wrong exception -- was InvalidRequestException, now correctly PolicyCountLimitExceededException (PutResourcePolicy's modeled error set does not even include InvalidRequestException as a fallback, per deserializers.go); (3) added PolicySizeLimitExceededException enforcement, previously entirely unenforced (AWS docs: policy document 'can be up to 5kb in size'). Revision-ID conflict + JSON validation remain correctly enforced. RE-CHECKED (this pass): BypassPolicyLockoutCheck/LockoutPreventionException confirmed still genuinely blocked, not merely under-implemented -- see gaps for why (this is NOT the same as the other 'IAM simulation' claims that turned out reachable this campaign; the blocker here is architectural, not effort)"}
  GetIndexingRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): each IndexingRule now includes the Rule.Probabilistic.{DesiredSamplingPercentage,ActualSamplingPercentage} object (see UpdateIndexingRule)"}
  UpdateIndexingRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass), STUB CLASS BUG: the request's Rule field (Rule.Probabilistic.DesiredSamplingPercentage -- the entire point of this operation, a probabilistic-sampling-percentage update) was not modeled anywhere: IndexingRule had no Rule field, the handler ignored any Rule in the request body, and UpdateIndexingRule only ever bumped ModifiedAt. This is exactly the 'real-looking op that is a disguised stub' pattern (parity-principles.md #4) -- it always returned 200 but never changed anything a caller asked it to change. Now implemented: ProbabilisticRuleValue{DesiredSamplingPercentage,ActualSamplingPercentage} added to the model, wired through the request (tagged-union {\"Probabilistic\":{...}} per IndexingRuleValueUpdate) and response. Also FIXED a second, independent bug in the same handler: the not-found response was hand-built as json.Marshal(map[string]any{\"ModifiedAt\": rule.ModifiedAt}) -- marshaling a raw time.Time produces an RFC3339 string, not the required epoch-seconds number (the exact epoch-seconds bug class this audit was briefed to hunt); GetIndexingRules already did this correctly, only UpdateIndexingRule's response had the bug. Also FIXED wrong error code: not-found was InvalidRequestException, real modeled error is ResourceNotFoundException"}
  GetInsight: {wire: ok, errors: ok, state: ok, persist: ok, note: "REST path /Insight (fixed prior pass). FIXED (this pass), STUB CLASS BUG: insightView already declared a Categories field but toInsightView never set it -- a real-looking field that was silently always empty. Categories is now always [\"FAULT\"] on every gopherstack-detected insight, which is not a guess: InsightCategory (types/enums.go) has exactly one defined value, FAULT, and gopherstack's detector (detectInsights) is exclusively a fault-rate-threshold detector, so this is the only value that could ever be correct. Also FIXED: ClientRequestImpactStatistics (real field: 'the number of requests to the client service and whether the requests were faults or okay' -- i.e. literally {OkCount,FaultCount,TotalCount}) is now populated straight from the same w.Total/w.FaultCount window counters detectInsights already computes to decide whether to open the insight in the first place -- this is not new analysis, just surfacing data the detector already had. RootCauseServiceId/RootCauseServiceRequestImpactStatistics/TopAnomalousServices remain unset (see gaps): those claim a cross-service root-cause determination gopherstack's single-service threshold detector does not perform, unlike Categories/ClientRequestImpactStatistics which are direct reflections of already-tracked counts."}
  GetInsightEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "REST path /InsightEvents (fixed prior pass)"}
  GetInsightImpactGraph: {wire: ok, errors: ok, state: ok, persist: deferred, note: "REST path /InsightImpactGraph (fixed prior pass); Services always [] (out of scope, see gaps -- unchanged this pass)"}
  GetInsightSummaries: {wire: ok, errors: ok, state: ok, persist: ok, note: "REST path /InsightSummaries (fixed prior pass). FIXED (prior pass): InsightSummary.LastUpdateTime was completely absent. FIXED (this pass): same Categories/ClientRequestImpactStatistics fix as GetInsight -- InsightSummary (distinct type from GetInsight's Insight, but declares the identical Categories/ClientRequestImpactStatistics/RootCauseServiceId/RootCauseServiceRequestImpactStatistics/TopAnomalousServices field set) now surfaces both via insightSummaryView"}
  GetTraceSegmentDestination: {wire: ok, errors: ok, state: ok, persist: ok, note: "traceSegmentDest snapshot/Reset fixed prior pass"}
  UpdateTraceSegmentDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): (1) resourceTags is now included in backendSnapshot/Restore, closing the previously-deferred persistence gap; (2) added ResourceARN existence validation -- previously any ARN, including ones that were never a real group or sampling rule, silently returned an empty tag list. Real AWS declares ResourceNotFoundException for TagResource/UntagResource/ListTagsForResource (confirmed in deserializers.go); now enforced against groupsByARN/samplingRulesByARN"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): same ResourceARN existence check as ListTagsForResource, plus added TooManyTagsException enforcement (50 tags/resource cap, AWS docs 'Maximum number of user-applied tags per resource: 50') -- previously unenforced, an unbounded number of tags could be applied"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (this pass): same ResourceARN existence check as ListTagsForResource"}
families:
  route_matcher: {status: ok, note: "unchanged this pass; prior pass audited all 34 dispatch-table paths against serializers.go opPath literals and fixed 6 mismatches (GetInsight/GetInsightEvents/GetInsightImpactGraph/GetInsightSummaries/GetSamplingStatisticSummaries/GetSamplingTargets)"}
  persistence: {status: ok, note: "FIXED (this pass): resourceTags was a plain map (not store.Table-backed) that (a) was never included in backendSnapshot -- tags were lost across every gopherstack restart -- and (b) was never cleared by InMemoryBackend.Reset(), the exact same bug class the prior pass fixed for traceSegmentDest but missed here. Both fixed: resourceTags now round-trips through Snapshot/Restore and is reset to an empty map in Reset()"}
  error_codes: {status: ok, note: "FIXED (this pass): independently field-diffed every operation's modeled error set against aws-sdk-go-v2/service/xray@v1.36.20's deserializers.go per-op error switch (awsRestjson1_deserializeOpError<Op>), not just handleError's own type switch. Found and fixed: UpdateIndexingRule not-found was InvalidRequestException (real: ResourceNotFoundException); PutResourcePolicy's policy-count-limit violation was InvalidRequestException (real: PolicyCountLimitExceededException, and InvalidRequestException isn't even in that op's modeled error set); TagResource/UntagResource/ListTagsForResource/CancelTraceRetrieval/ListRetrievedTraces/GetRetrievedTracesGraph never returned ResourceNotFoundException at all despite it being modeled for all six. Added ErrResourceNotFound/ErrTraceRetrievalNotFound/ErrPolicySizeLimitExceeded/ErrRuleLimitExceeded/ErrTooManyTags sentinels and corresponding handleError overrides. Confirmed unchanged/correct: GetGroup/DeleteGroup/UpdateGroup/GetSamplingRules/CreateSamplingRule/UpdateSamplingRule/DeleteSamplingRule/GetInsight*/DeleteResourcePolicy all declare ONLY InvalidRequestException (+ThrottledException, +RuleLimitExceededException for CreateSamplingRule) for not-found -- X-Ray's Smithy model does NOT give these ops ResourceNotFoundException, so gopherstack's existing InvalidRequestException mapping for Group/SamplingRule/Insight/ResourcePolicy not-found was already correct and is unchanged"}
gaps:
  - PutTelemetryRecords ring buffer (100 entries) not persisted across restart; low-risk, AWS telemetry data itself is operational/ephemeral by nature (unchanged this pass)
  - "Insight.RootCauseServiceId, RootCauseServiceRequestImpactStatistics, TopAnomalousServices, and GetInsightImpactGraph's Services remain always empty/unset -- these claim a cross-service root-cause/topology determination that gopherstack's insight detector (detectInsights in insights.go, a single-service fault-rate-threshold heuristic with no service-graph awareness) does not perform. NARROWED this pass: Categories and ClientRequestImpactStatistics were moved OUT of this gap and implemented (see GetInsight/GetInsightSummaries ops) once it was established they need no anomaly-detection algorithm at all -- Categories has exactly one possible enum value (FAULT) given gopherstack's detector, and ClientRequestImpactStatistics is a direct surfacing of the w.Total/w.FaultCount counters the detector already computes to decide whether to open the insight. The remaining fields genuinely require cross-service causality analysis this detector was never designed to do; judged out of scope, same as before."
  - "SamplingRateBoost's runtime boost-trigger VALUE (the actual BoostRate number X-Ray would compute) is NOT implemented and never will be guessed: AWS does not publish the algorithm (API_SamplingBoostStatisticsDocument.html describes the inputs, AnomalyCount/SampledAnomalyCount/TotalCount, only qualitatively), so SamplingTargetDocument.SamplingBoost is always left unset. An earlier draft of this pass computed a fabricated rate (linear interpolation between FixedRate and MaxRate by anomaly ratio) and was reverted on review: a fabricated quota/price/rate is worse than an absent one, because a client reads and acts on it without rechecking a plausible-looking number. NARROWED this pass: the WIRE gap (SamplingBoostStatisticsDocuments/UnprocessedBoostStatistics were previously silently absent regardless of the algorithm question) IS fixed -- documents for known rules are now accepted, documents for unknown rules are now reported in UnprocessedBoostStatistics. The net effect for a client: submitting a boost document for a rule with SamplingRateBoost configured is accepted and produces no error, but also produces no observable SamplingBoost on the returned target -- an honest 'accepted, no engine behind it' gap."
  - "PutResourcePolicy's BypassPolicyLockoutCheck field is parsed but LockoutPreventionException is never raised. RE-VERIFIED this pass (WebFetch against docs.aws.amazon.com/xray/latest/api/API_PutResourcePolicy.html): the real check is 'the policy would prevent THE CALLER OF THIS REQUEST from calling PutResourcePolicy in the future' -- i.e. it evaluates the submitted policy document against the calling IAM principal's identity, not against any abstract/generic principal. gopherstack's xray package never resolves or threads a calling principal into request handling at all (grep confirms zero use of pkgs/awsmeta, which only carries Account/Region/Partition/RequestID, not a principal ARN) -- there is no 'the caller' value in scope to evaluate against. This is a genuine architectural gap distinct from the six other 'blocked' claims resolved this campaign: those were blocked by unimplemented-but-available logic, this one is blocked by an identity concept the request pipeline does not carry at all. Implementing a real per-principal check would require adding caller-identity plumbing to the whole service (or repo-wide), which is out of scope for a resource-policy op. The parameter is still accepted (matches wire shape) but has no effect, which is safe (never falsely rejects a real client's request) even though it under-enforces relative to real AWS."
  - "ThrottledException is declared in the modeled error set for every X-Ray operation but is never emitted anywhere in gopherstack (no rate limiting is modeled). This is consistent with the rest of gopherstack's emulation approach (no service throttles by default) and is not treated as a gap specific to X-Ray."
deferred:
  - none; all routed ops covered by ops/families above
leaks: {status: clean, note: "Janitor.Run uses pkgs/worker.Group with Ticker + Stop() on ctx.Done(); sweepExpiredTraces holds b.mu.Lock only around map mutation, releases before telemetry/logging calls. Re-verified this pass: no new goroutines/tickers introduced; all new lock paths (resourceExists, resolveSamplingRule, DeleteResourcePolicy's revision check) execute entirely within their caller's existing Lock/RLock and use defer Unlock/RUnlock."}
---

## Notes

- **Route-matcher bug class** (prior pass, unchanged): 6 of 34 routed X-Ray operations used
  their operation-name-shaped path (e.g. `/GetInsight`) instead of the actual REST path the
  real `aws-sdk-go-v2/service/xray` client serializes (e.g. `/Insight`). See git history for
  the full list; not re-audited this pass since the route table is unchanged.
- **Paths confirmed correct** (prior pass, unchanged): `GetServiceGraph`→`/ServiceGraph`,
  `GetTraceGraph`→`/TraceGraph`, `GetGroup`→`/GetGroup`, `GetGroups`→`/Groups`,
  `GetSamplingRules`→`/GetSamplingRules`, `GetIndexingRules`→`/GetIndexingRules`,
  `GetRetrievedTracesGraph`→`/GetRetrievedTracesGraph`,
  `GetTraceSegmentDestination`→`/GetTraceSegmentDestination`,
  `GetTimeSeriesServiceStatistics`→`/TimeSeriesServiceStatistics`.
- `/EncryptionConfig` real SDK client only ever sends POST for `GetEncryptionConfig`;
  gopherstack's `RouteMatcher` also accepts GET on that path -- harmless superset, left as-is.
- **New this pass -- client-breaking wire-shape bugs found by field-diffing responses
  against `deserializers.go`, not just request routing**: the route-matcher audit in the
  prior pass proved 6 ops were *unreachable*; this pass found operations that *were*
  reachable and returned HTTP 200, but whose response bodies a real SDK client would
  silently mis-parse or partially drop:
  - `GetTraceSummaries`: `EntryPoint` was serialized as a JSON string; the real
    `TraceSummary.EntryPoint` field is a `ServiceId` object. A real client's
    `awsRestjson1_deserializeDocumentServiceId` call fails outright on a string value
    (`"unexpected JSON type"`), meaning any real SDK caller reading `EntryPoint` off of
    `GetTraceSummaries` would have hit a deserialization error on every single trace
    summary with a root segment.
  - `ListRetrievedTraces`: each retrieved trace's span-document list was sent under the
    key `"Segments"`; the real key is `"Spans"`. Because `awsRestjson1_deserializeDocumentRetrievedTrace`
    silently ignores unrecognized keys (the `default: _, _ = key, value` case), this
    doesn't error -- it just silently produces an **empty** `Spans` slice on every
    `RetrievedTrace`, for every caller, forever. A 200 response with quietly-dropped
    data is worse than an error: nothing signals the client that its request was
    misunderstood.
  - `GetTraceSummaries`'s per-item `ApproximateTime` field was invented (not in the real
    `TraceSummary` type at all) while the *real* `ApproximateTime` field -- which
    genuinely exists, just one level up, on `GetTraceSummariesOutput` itself ("the start
    time of this page of results") -- was completely absent from the response envelope.
    This is a case where fixing "no such field" and "missing field" were the same
    one-line move: delete the wrong one, add the right one at the right nesting level.
  - `UpdateIndexingRule`'s not-found response path hand-built its own JSON with
    `json.Marshal(map[string]any{"ModifiedAt": rule.ModifiedAt})`, marshaling a raw
    `time.Time` (RFC3339 string) instead of the epoch-seconds number every other
    timestamp field in this service correctly uses via `float64(t.Unix())`. This is the
    exact "epoch-seconds timestamp bug class" this audit was briefed to hunt for --
    it had evidently been missed in earlier passes because it only fires on the
    (previously essentially untested) success path of one specific handler, not the
    general JSON-marshal path most other ops share via a shared `toXView` helper.
- **Stub-class bug**: `UpdateIndexingRule` is the clearest example found this pass of
  parity-principles.md's warning #4 ("a 'real-looking' op may be a disguised stub"). It
  always returned HTTP 200 with a plausible-looking `IndexingRule` body, and every
  existing unit test for it passed -- but the one thing a caller uses this operation
  for (changing the indexing sampling percentage) had no code path at all: the request's
  `Rule` field was never read, and `IndexingRule` itself had no field to hold a sampling
  percentage in the first place. Green tests did not catch this because the tests only
  asserted "the call succeeds and `ModifiedAt` changes," never "the sampling percentage
  I asked for is reflected back."
- `SamplingRule.Version` in the wire view (`samplingRuleView.Version`) is hardcoded to
  `1` -- matches real AWS behavior (X-Ray sampling rules do not expose a mutable version
  counter via the API), NOT a bug. Unchanged this pass.
- `evaluateFilter` implements a deliberately small subset of the X-Ray filter-expression
  grammar (fault/error/throttle/http.status/responsetime/annotation.KEY); judged
  acceptable emulator scope, unchanged this pass.
- `maxSamplingRules`: RE-VERIFIED this pass against **two independent sources**, both
  keyed to the same AWS Service Quotas quota code `L-8C0C998A`: (1)
  docs.aws.amazon.com/general/latest/gr/xray.html's published "Service quotas" table
  ("Custom sampling rules per region", Default 25, Adjustable: Yes); (2) a third-party
  snapshot of AWS's own `list-aws-default-service-quotas` API output
  (github.com/fanovilla/aws-default-service-quotas, quotas/xray_quotas.json --
  `"QuotaCode": "L-8C0C998A", "QuotaName": "Custom sampling rules per region", "Value":
  25.0, "Adjustable": true`), independently confirming the same value from the API
  gopherstack's own number was supposed to reflect, not just a second doc page scrape
  of the same source. FIXED: constant corrected from 2000 to 25.
  The 2000 value under-enforced relative to real AWS by two orders of magnitude: a
  real client relying on RuleLimitExceededException firing at the documented quota
  (e.g. to test its own quota-handling logic) would have seen gopherstack silently
  accept up to 2000 rules instead. Proven with a probe test that failed against the old
  constant (creating a 26th rule returned 200 instead of RuleLimitExceededException);
  existing `TestCreateSamplingRule_RuleLimitExceeded` (whitebox_test.go) parameterizes off
  the constant itself, so it required no changes and still passes at the corrected value.
  NOTE on "Adjustable: Yes": unlike the ELB precedent (CreateLoadBalancerPolicy's
  TooManyPolicies, gopherstack-6851 -- left unenforced because AWS's official quota
  table has NO row at all for that limit, so no default number exists to enforce), X-Ray's
  25-rule quota IS an AWS-published DEFAULT, just one a real account can raise via a
  Service Quotas increase request. Enforcing the published default is not fabricating a
  hard cap: it is the same "enforce the documented default, since gopherstack cannot know
  any individual real account's raised limit" pattern this file already uses elsewhere
  without controversy (TooManyTagsException at 50 tags/resource, maxResourcePolicies at 5
  -- both also technically increasable in real AWS). A real, unmodified account hits
  RuleLimitExceededException at 25, same as gopherstack now does.
- `defaultIndexingRuleSamplingPct = 1.0` (the built-in "Default" indexing rule's initial
  `DesiredSamplingPercentage`/`ActualSamplingPercentage`): RE-VERIFIED this pass (WebSearch
  against CloudWatch Transaction Search docs) -- AWS's actual default indexing rate is
  1%, confirming this constant was already correct. No change needed.
