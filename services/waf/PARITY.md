---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: waf
sdk_module: aws-sdk-go-v2/service/waf@v1.33.4   # WAF Classic (legacy WAF/WAF Regional), distinct from wafv2
last_audit_commit: 8c56f4eb9
last_audit_date: 2026-08-07
overall: A            # 2026-08-29 (cursor-population sweep): all 16 List ops declare a real NextMarker
                      # (from the pinned SDK Output structs directly), and 15 of 16 already read
                      # NextMarker/Limit from the request and set NextMarker on the response through the
                      # shared paginate() helper (handler.go:124). The one exception, ListSubscribedRule-
                      # Groups, is correctly left unpaginated: its backend (rule_groups.go) always returns
                      # an empty slice -- there is no real AWS Marketplace subscription state for this
                      # mock to page over, so the gap is unobservable. No code changed this pass.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetChangeToken: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChangeTokenStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: unknown token returns INSYNC per real AWS behavior (pre-existing, verified not re-broken)}
  GetSampledRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "TimeWindow.StartTime/EndTime epoch-seconds shape verified ok; SampledHTTPRequest.Request/.Timestamp fields present for wire-shape completeness. This pass: WebAclId is now validated against real backend state -- unknown WebACL returns WAFNonexistentItemException instead of silently succeeding with an empty sample. RuleId is accepted without existence validation since real AWS defines it as one of three shapes (a Rule's RuleId, a RuleGroup's RuleGroupId, or the literal 'Default_Action'), and gopherstack has no verified AWS source pinning which combination is checked server-side, so validating it risks false rejections; unvalidated is the same behavior real AWS shows for the RuleGroupId/Default_Action cases. The sample list itself stays empty: see structural_gaps."}
  GetRateBasedRuleManagedKeys: {wire: ok, errors: ok, state: ok, persist: n/a, note: "RuleId is validated against real RateBasedRule state (WAFNonexistentItemException for unknown rule, pre-existing). ManagedKeys list itself stays empty: see structural_gaps."}
families:
  WebACL: {status: ok, note: "fixed CreateWebACL: added missing ChangeToken parameter (interface didn't even accept one) + validation on Create/Update/Delete. UpdateWebACL correctly applies INSERT/DELETE ActivatedRule updates and sorts by Priority. This pass: DeleteWebACL now returns WAFNonEmptyEntityException while Rules is non-empty."}
  Rule: {status: ok, note: "fixed Create/Update/Delete to validate ChangeToken; DeleteRule now returns WAFReferencedItemException if still activated in a WebACL or RuleGroup (previously deleted unconditionally, silently orphaning ActivatedRule references). This pass: DeleteRule now also returns WAFNonEmptyEntityException while Predicates is non-empty."}
  RateBasedRule: {status: ok, note: "same ChangeToken + ReferencedItem fixes as Rule (a RateBasedRule's RuleId can be activated in a WebACL with Type=RATE_BASED). This pass: DeleteRateBasedRule now also returns WAFNonEmptyEntityException while MatchPredicates is non-empty."}
  IPSet: {status: ok, note: "ChangeToken validation added; DeleteIPSet now returns WAFReferencedItemException if referenced by a Rule/RateBasedRule Predicate.DataId. This pass: DeleteIPSet now also returns WAFNonEmptyEntityException while IPSetDescriptors is non-empty."}
  ByteMatchSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete (same pattern as IPSet). This pass: DeleteByteMatchSet now also returns WAFNonEmptyEntityException while ByteMatchTuples is non-empty."}
  SizeConstraintSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete. This pass: DeleteSizeConstraintSet now also returns WAFNonEmptyEntityException while SizeConstraints is non-empty."}
  SqlInjectionMatchSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete. This pass: DeleteSqlInjectionMatchSet now also returns WAFNonEmptyEntityException while SqlInjectionMatchTuples is non-empty."}
  XssMatchSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete. This pass: DeleteXssMatchSet now also returns WAFNonEmptyEntityException while XssMatchTuples is non-empty."}
  GeoMatchSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete. This pass: DeleteGeoMatchSet now also returns WAFNonEmptyEntityException while GeoMatchConstraints is non-empty."}
  RegexPatternSet: {status: ok, note: "ChangeToken validation; DeleteRegexPatternSet now returns WAFReferencedItemException if referenced by a RegexMatchSet tuple's RegexPatternSetId. This pass: DeleteRegexPatternSet now also returns WAFNonEmptyEntityException while RegexPatternStrings is non-empty."}
  RegexMatchSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete (a RegexMatchSet is itself a match set referenceable from a Rule Predicate). This pass: DeleteRegexMatchSet now also returns WAFNonEmptyEntityException while RegexMatchTuples is non-empty."}
  RuleGroup: {status: ok, note: "ChangeToken validation; DeleteRuleGroup now returns WAFReferencedItemException if activated in a WebACL with Type=GROUP. DeleteRuleGroup also returns WAFNonEmptyEntityException while it still has activated rules. 2026-08-30 (marker-cursor sweep): UpdateRuleGroup's INSERT action now rejects a RuleId already active in the group (WAFInvalidParameterException) -- previously unchecked, so the same RuleId could be activated twice at different priorities, and since ListActivatedRulesInRuleGroup resumes pagination by matching a RuleId marker (handler_rule_groups.go), a duplicate RuleId broke that resume. Fixed at the mutation boundary rather than the read path, matching this repo's established pattern (e.g. wafv2 rate_based_rules.go rejecting duplicate Name/Priority on write)."}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified against real shapes -- no ChangeToken involved in real AWS, correctly not required here"}
  Logging: {status: ok, note: "PutLoggingConfiguration/GetLoggingConfiguration/DeleteLoggingConfiguration/ListLoggingConfigurations -- no ChangeToken in real AWS, correctly not required"}
  PermissionPolicy: {status: ok, note: "no ChangeToken in real AWS, correctly not required"}
  Migration: {status: ok, note: "CreateWebACLMigrationStack returns a deterministic S3 URL shape; genuinely can't produce a real migration template without wafv2 state, documented as a stub-shape return, not a disguised no-op"}
gaps:
  - "2026-08-29 (constrain-not-honoured sweep, confirmed clean): every List op's Limit/NextMarker is applied via the shared paginate() chokepoint (handler.go) except opListSubscribedRuleGroups, which ignores its request body entirely. Not fixed: ListSubscribedRuleGroups' backend (rule_groups.go) always returns an empty slice (structural_gaps: no marketplace-subscription simulation), so there is never more than zero items to paginate -- Limit/NextMarker have no observable effect either way. GetRateBasedRuleManagedKeys.NextMarker is documented on the SDK itself as \"not currently used\" (api_op_GetRateBasedRuleManagedKeys.go), correctly unread. No other List/Get op in this service accepts a filter/selector parameter beyond Limit/NextMarker on the pinned v1.33.4 SDK -- verified by reading every api_op_List*.go/api_op_Get*ManagedKeys.go input struct."
structural_gaps:
  - "GetSampledRequests always returns an empty SampledRequests list: real AWS randomly samples from actual HTTP requests evaluated against the WebACL's rules. Gopherstack has no request-proxying subsystem -- it never sees or evaluates real client traffic through WAF rules, so there is no request data to sample from, ever. Producing non-empty samples would mean fabricating fictitious HTTP requests, exactly the failure mode this parity campaign exists to remove. (WebAclId existence validation IS buildable from real state and was added this pass; the sample content is not.) (bd: gopherstack-smld)"
  - "GetRateBasedRuleManagedKeys always returns an empty ManagedKeys list: real AWS derives it from live request-rate tracking against the rule's RateLimit over a trailing 5-minute window, which requires the same real-traffic evaluation GetSampledRequests lacks. Nothing in InMemoryBackend's state (RateBasedRule config, WebACL associations) encodes request rates, so there is no rate to threshold against. (RuleId existence validation IS buildable and already present.) (bd: gopherstack-smld)"
deferred: []
leaks: {status: clean, note: "no goroutines/timers/background workers in this service; InMemoryBackend is plain locked maps + store.Table, no leak surface. New non-empty checks only read already-locked in-memory slices/maps under the existing coarse b.mu -- no new lock paths, no new persisted state. FIXED (gopherstack-cq0z, 2026-09-06): DeleteIPSet, DeleteRateBasedRule, DeleteRuleGroup, DeleteWebACL and DeleteRule all left their entry in the tags map. ListTagsForResource has no existence check at all, so it still returned the stale tags for any of these five resource types after delete, and tags is persisted verbatim in Snapshot() regardless. Now cleared in all five delete paths. See TestWAF_Delete_ClearsTags."}
---

## Notes

- **2026-08-14 (gopherstack-dv4s batch five): over-wide List-response audit, 13/13
  candidate ops verified clean, zero leaks.** All 13 List ops flagged by
  matching real SDK Output element type names against
  `Summary|Item|Brief|Entry|Ref|Preview|Metadata|Info`
  (`ListByteMatchSets`/`ListGeoMatchSets`/`ListIPSets`/`ListRateBasedRules`/
  `ListRegexMatchSets`/`ListRegexPatternSets`/`ListRuleGroups`/`ListRules`/
  `ListSizeConstraintSets`/`ListSqlInjectionMatchSets`/
  `ListSubscribedRuleGroups`/`ListWebACLs`/`ListXssMatchSets`) already use a
  dedicated `*Summary` Go type per family, each hand-verified field-for-field
  against the pinned `waf@v1.33.4` `types/types.go` declaration — every one
  is an exact `{Id, Name}` pair (`SubscribedRuleGroupSummary` also carries
  `MetricName`, matching `types.SubscribedRuleGroupSummary` exactly).
  `ListRateBasedRules` is the one case worth naming: gopherstack's
  `RateBasedRuleSummary` is a distinct Go type name, but the real
  `ListRateBasedRulesOutput.Rules` is `[]types.RuleSummary` — the same
  `{RuleId, Name}` shape WAF Classic reuses for plain Rules, confirmed by
  reading `api_op_ListRateBasedRules.go` directly rather than assuming a
  name match implied a type match. No shared Get/List converter exists
  anywhere in this family (`handler_match_sets.go`'s file-level comment
  explains the seven near-identical families were deliberately merged into
  one file for a `dupl` lint reason, not a shared-conversion reason) — the
  structural signal that has predicted a clean result everywhere else it
  held (cleanrooms' membership-scope ops, most of the dv4s pass-4 services)
  held again here.

- **ChangeToken workflow was a complete no-op before this pass.** Every mutating backend
  method (Create/Update/Delete across all 12 resource families) accepted a `changeToken`
  parameter but discarded it via `_`; `CreateWebACL`'s interface didn't even have the
  parameter. `ErrStaleToken` (WAFStaleDataException) was defined and wired into
  `Handler.handleError` but never returned by anything — any string, including `""` or a
  token from a completely different backend, worked as a "change token." Fixed by adding
  `InMemoryBackend.validateChangeToken(token string) error`, called at the top of every
  Create/Update/Delete method (after acquiring the write lock, before the existence/mutation
  logic): rejects unless `token` was returned by an earlier `GetChangeToken` call and is
  still `PROVISIONED` (not yet consumed). It does **not** itself consume the token — token
  consumption (`PROVISIONED` → `INSYNC`) stays where it already was, in
  `Handler.dispatch`'s post-success `MarkChangeTokenUsed` sniff of the request body. This
  split matters: `InMemoryBackend` methods are also called directly (bypassing the HTTP
  handler) by tests and potentially other code, and those callers never go through
  `dispatch`, so a token obtained via `GetChangeToken()` and reused directly against the
  backend stays `PROVISIONED` indefinitely unless `MarkChangeTokenUsed` is called
  explicitly. `persistence_test.go`'s full-state round-trip test relied on exactly this
  reuse pattern (one token, ~20 backend calls) and needed a small fix: it had been calling
  `MarkChangeTokenUsed` on its one token *before* reusing it for unrelated resource
  creation, which now correctly fails — split into a dedicated `staleToken` (marked used,
  asserted INSYNC after snapshot/restore) and a separate `token` that stays PROVISIONED for
  the actual resource-creation calls.

- **WAFReferencedItemException was defined and wired into the error-code switch but never
  returned.** Real AWS WAF Classic rejects deleting an object that is still referenced by
  another object: "you tried to delete a ByteMatchSet that is still referenced by a Rule,"
  "you tried to delete a Rule that is still referenced by a WebACL." Before this pass, every
  `Delete*` method deleted unconditionally once the target existed, silently leaving
  dangling `RuleId`/`DataId` references in whatever WebACL/RuleGroup/Rule/RateBasedRule
  still pointed at the deleted object — a `GetWebACL` after such a delete would return a
  `Rules[]` entry whose `RuleId` no longer resolves via `GetRule`, which real AWS makes
  structurally impossible. Fixed with three small reference-scan helpers on
  `InMemoryBackend` (all O(n) over the small in-memory resource sets, called with `b.mu`
  already held):
  - `ruleReferenced(id)` — true if `id` appears as `ActivatedRule.RuleId` in any WebACL's
    `Rules` or any RuleGroup's activated-rules list. Covers Rule, RateBasedRule, and
    RuleGroup deletion (WAF Classic's `WafRuleType` enum is `REGULAR | RATE_BASED | GROUP`,
    and all three share the same `RuleId`-in-`ActivatedRule` reference shape).
  - `matchSetReferenced(id)` — true if `id` appears as `Predicate.DataId` in any Rule's or
    RateBasedRule's predicates. Covers IPSet, ByteMatchSet, SqlInjectionMatchSet,
    XssMatchSet, SizeConstraintSet, GeoMatchSet, and RegexMatchSet deletion (a
    `Predicate.DataId` is a "unique identifier ... such as ByteMatchSetId or IPSetId,"
    untyped by resource kind on the wire, and UUID collision across kinds is not a
    practical concern).
  - `regexPatternSetReferenced(id)` — true if `id` appears as a
    `RegexMatchTuple.RegexPatternSetId` in any RegexMatchSet.

  Verified against every existing lifecycle test (`handler_audit1_test.go`,
  `handler_audit2_test.go`, `parity_b_test.go`) before writing the fix: none of them
  exercise a delete-while-referenced path, so no existing test assertions needed to change
  for this half of the fix (unlike the ChangeToken fix, which did require the
  `persistence_test.go` restructuring described above). New coverage in
  `backend_test.go` exercises both the rejection and the success-after-reference-removed
  path for each of the five reference shapes (Rule↔WebACL, Rule↔RuleGroup,
  RuleGroup↔WebACL via Type=GROUP, RateBasedRule↔WebACL via Type=RATE_BASED,
  IPSet↔Rule-predicate, RegexPatternSet↔RegexMatchSet).

- **GetSampledRequests wire-shape bug**: `TimeWindow.StartTime`/`EndTime` were modeled as
  JSON strings, but `aws-sdk-go-v2/service/waf`'s `serializeDocumentTimeWindow` always
  sends (and the deserializer always expects) a JSON **number** of seconds since the Unix
  epoch (`types.TimeWindow.{Start,End}Time` are `*time.Time`, protocol shape
  `unixTimestamp`) — this is the one and only timestamp-bearing shape in the entire WAF
  Classic API (`grep -rl time.Time` across `types/types.go` and every `api_op_*.go` in the
  SDK module confirms no other operation has this issue). A real SDK client's
  `GetSampledRequests` request would have failed to unmarshal server-side under the old
  code (`json: cannot unmarshal number into Go struct field ... of type string`). Fixed by
  changing the request-parsing struct's `StartTime`/`EndTime` fields to `float64`, which
  also fixes the response encoding for free (the same `float64` values are echoed back into
  the response map, and `encoding/json` renders a `float64` as a JSON number automatically).
  `parity_a_test.go`'s `TestParity_GetSampledRequestsReturnsTimeWindow` and
  `handler_audit1_test.go`'s `TestWAF_GetSampledRequests_EmptyStub` both asserted the old
  (wrong) ISO8601-string shape and were updated to send/expect epoch-seconds numbers.

- **`TargetString` in `ByteMatchTuple` is intentionally plain `string`, not base64** — this
  looks wrong at first glance (real `types.ByteMatchTuple.TargetString` is `[]byte`,
  serialized via `Base64EncodeBytes`) but is actually correct: gopherstack never decodes or
  re-encodes the field, just stores and echoes back whatever base64 text the SDK sent
  verbatim, so a real SDK client's own base64 encode-on-request / decode-on-response
  round-trips correctly through the opaque string. Confirmed by reading
  `serializers.go`/`deserializers.go` — do not "fix" this to `[]byte` in a future pass
  without checking whether that changes the stored representation in a way that breaks the
  round-trip (it currently doesn't, precisely because nothing on the gopherstack side ever
  interprets the bytes).

- **WAFNonEmptyEntityException was not modeled at all before this pass** (tracked as an
  explicit gap in the 2026-07-13 audit). Real AWS WAF Classic rejects deleting a container
  object while it still holds child entities, distinct from `WAFReferencedItemException`
  (which rejects deleting an object still referenced *by* something else): "You can't
  delete a WebACL if it still contains any Rules," "You can't delete a Rule if ... it
  still includes any predicates," and the equivalent doc comment on every other
  `Delete*` operation in `aws-sdk-go-v2/service/waf/api_op_Delete*.go` (confirmed by
  reading all twelve). Before this pass every `Delete*` method deleted unconditionally
  once the reference check passed, even with a non-empty child slice/map still attached —
  a `DeleteWebACL` on a WebACL with `Rules` still populated, or a `DeleteRule` on a Rule
  with `Predicates` still populated, both silently succeeded. Fixed by changing every
  `Delete*` method from `!b.<table>.Has(id)` to `<table>.Get(id)` (needed the value
  anyway to check its length) and adding `if len(<child slice/map>) > 0 { return
  ErrNonEmptyEntity }` after the existing `ErrReferencedItem` check, for all twelve
  families: WebACL (`Rules`), Rule/RateBasedRule (`Predicates`/`MatchPredicates`),
  RuleGroup (`ruleGroupRules[id]`), IPSet (`IPSetDescriptors`), ByteMatchSet
  (`ByteMatchTuples`), SizeConstraintSet (`SizeConstraints`), SqlInjectionMatchSet
  (`SqlInjectionMatchTuples`), XssMatchSet (`XssMatchTuples`), GeoMatchSet
  (`GeoMatchConstraints`), RegexMatchSet (`RegexMatchTuples`), and RegexPatternSet
  (`RegexPatternStrings`). New `ErrNonEmptyEntity` sentinel added to `errors.go`
  (`WAFNonEmptyEntityException`, HTTP 400, same `awserr.ErrConflict` class as
  `ErrReferencedItem`) and wired into `Handler.handleError`. Six pre-existing lifecycle
  tests (`byte_match_sets_test.go`, `size_constraint_sets_test.go`,
  `sql_injection_match_sets_test.go`, `xss_match_sets_test.go`, `geo_match_sets_test.go`,
  `regex_pattern_sets_test.go`) deleted a populated set directly and needed updating to
  first remove the tuple/pattern (matching real AWS) before the delete now correctly
  succeeds; every other existing lifecycle test already removed children before deleting
  (no change needed), which is itself evidence the bug had gone unexercised. New dedicated
  coverage in `non_empty_entity_test.go` (one test per family, all twelve) asserts both the
  blocked-while-non-empty case and the succeeds-after-removal case, following the same
  create → populate → blocked-delete → depopulate → delete pattern as the existing
  `referenced_item_test.go`.

- **SampledHTTPRequest wire-shape completeness**: the model was missing the `Request`
  (`HTTPRequest`) and `Timestamp` fields present on the real
  `types.SampledHTTPRequest` (`Request` is even marked "This member is required" in the
  SDK doc comment). Added `HTTPRequest`/`HTTPHeader` types and the two missing fields to
  `models.go`. Does not change any test-observable behavior today because
  `GetSampledRequests` always returns an empty `SampledRequests` list (the pre-existing,
  documented traffic-inspection stub) — this is forward-looking wire-shape correctness for
  if/when real sample data is ever populated, not a currently-reachable bug fix.

- **GetChangeTokenStatus's "unknown token → INSYNC" behavior** (comment + dedicated test
  `TestParity_ChangeTokenStatus_UnknownReturnsINSYNC`) predates this audit and was not
  touched. It is orthogonal to the `validateChangeToken` fix added this pass: the new
  validation lives in the *write* path (Create/Update/Delete reject an unknown/reused
  token), while `GetChangeTokenStatus` is a pure *read* that intentionally still returns
  `INSYNC` for a token it has never seen, matching the pre-existing, already-verified
  parity finding.

- **gopherstack-smld follow-up**: `GetSampledRequests` now validates `WebAclId` against
  `InMemoryBackend.webACLs` before returning, since that is real state the backend already
  holds — an unknown WebACL now gets `WAFNonexistentItemException` instead of a silent
  empty-sample 200. `GetRateBasedRuleManagedKeys` already validated `RuleId` the same way
  (pre-existing, unchanged). The actual sampled-request/managed-key *content* stays
  unimplemented and is now recorded in `structural_gaps` rather than `gaps`: gopherstack
  has no subsystem that proxies or evaluates real HTTP traffic through WAF rules, so there
  is no request/rate data for either op to report — inventing sample requests or blocked
  IPs would be fabrication, not emulation.

- **2026-08-28 wrapper-key/layer-2 re-sweep (bug class gopherstack-6flj/21my), no bugs
  found.** Protocol re-confirmed against the pinned `waf@v1.33.4` module:
  `awsAwsjson11_*` serializer prefix (JSON-RPC), not WAFv2's protocol — read directly, not
  assumed from `_PROTOCOLS.md`. Per-op manifest-mention check found the match-set
  families' individual Create/Get/Update op names (ByteMatchSet/IPSet/SizeConstraintSet/
  SqlInjectionMatchSet/XssMatchSet/GeoMatchSet/RegexPatternSet/RegexMatchSet),
  CreateRule/CreateRuleGroup/GetRuleGroup/UpdateRuleGroup,
  CreateRateBasedRule/GetRateBasedRule/UpdateRateBasedRule,
  ListActivatedRulesInRuleGroup, and PutPermissionPolicy/GetPermissionPolicy/
  DeletePermissionPolicy at zero literal mentions (this manifest tracks status by
  *family*, e.g. `IPSet:`, not by individual op name) — swept each against its own
  `api_op_*.go` Output struct and `deserializers.go` document-deserializer field-for-field
  at both the wrapper-key and nested-tuple/type layer. All confirmed clean: wrapper keys
  (`IPSet`/`ByteMatchSet`/.../`Rule` for GetRateBasedRule, `Rules` for
  ListRateBasedRules) match; `ByteMatchTuple.TargetString` was checked as a possible
  base64/[]byte type mismatch (real deserializer base64-decodes it,
  `deserializers.go:10420`) but gopherstack passes the wire-format base64 string through
  verbatim on both the accept and echo path (never decoding), so a real client's own
  base64 round trip still produces the original bytes -- not a bug, just an internal
  representation choice. `ActivatedRule`/`WafAction`/`WafOverrideAction`/`ExcludedRule`/
  `LoggingConfiguration`/`RedactedFields` also spot-checked clean. No source changes this
  pass.

- **2026-08-30 marker-cursor-over-a-tie-prone-key sweep.** Audited all 16 List ops'
  marker/sort key for duplicate-admission. All 12 `store.Table`-keyed listings
  (WebACLs/Rules/RateBasedRules/IPSets/ByteMatchSets/SizeConstraintSets/
  SqlInjectionMatchSets/XssMatchSets/GeoMatchSets/RegexPatternSets/RegexMatchSets/
  RuleGroups) sort/mark by their own `store.Table` key (`store_setup.go` `*KeyFn`
  functions) — duplicates structurally impossible. `ListLoggingConfigurations` marks by
  `ResourceArn`, also the table key. `ListTagsForResource` marks by `Tag.Key`, unique by
  Go map-key construction. `ListSubscribedRuleGroups` is unpaginated (always empty,
  documented in `structural_gaps`). The one exception: **`ListActivatedRulesInRuleGroup`**
  marks by `ActivatedRule.RuleId`, a field of a *side slice* (`b.ruleGroupRules[id]`), not
  a `store.Table` entry — `UpdateRuleGroup`'s INSERT action never checked for a
  already-active RuleId, so two `ActivatedRule` entries could share the same RuleId and
  break marker resume deterministically once a page boundary landed inside that pair.
  Fixed (see `RuleGroup` family note above); reproduced first in
  `rule_groups_test.go::TestUpdateRuleGroup_RejectsDuplicateRuleId` (fails against
  unmodified code, passes after the fix). All existing pagination fixtures
  (`pagination_test.go`) use distinct names/IDs throughout and could not have caught this.
