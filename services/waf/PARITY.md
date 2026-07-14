---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: waf
sdk_module: aws-sdk-go-v2/service/waf@v1.30.24   # WAF Classic (legacy WAF/WAF Regional), distinct from wafv2
last_audit_commit: d9aee9cb
last_audit_date: 2026-07-13
overall: A            # ~700 LOC of genuine fixes: ChangeToken workflow + ReferencedItem enforcement were no-ops
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetChangeToken: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChangeTokenStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: unknown token returns INSYNC per real AWS behavior (pre-existing, verified not re-broken)}
  GetSampledRequests: {wire: ok, errors: ok, state: partial, persist: n/a, note: "fixed TimeWindow.StartTime/EndTime wire shape (was string, real protocol is epoch-seconds number); sample data itself is a stub (empty) since gopherstack does not proxy/inspect real HTTP traffic through WAF rules -- same class of limitation as CloudWatch metric stubs elsewhere"}
  GetRateBasedRuleManagedKeys: {wire: ok, errors: ok, state: partial, persist: n/a, note: "always returns empty list -- same traffic-inspection limitation as GetSampledRequests, not fixable without real request proxying"}
families:
  WebACL: {status: ok, note: "fixed CreateWebACL: added missing ChangeToken parameter (interface didn't even accept one) + validation on Create/Update/Delete. UpdateWebACL correctly applies INSERT/DELETE ActivatedRule updates and sorts by Priority."}
  Rule: {status: ok, note: "fixed Create/Update/Delete to validate ChangeToken; DeleteRule now returns WAFReferencedItemException if still activated in a WebACL or RuleGroup (previously deleted unconditionally, silently orphaning ActivatedRule references)"}
  RateBasedRule: {status: ok, note: "same ChangeToken + ReferencedItem fixes as Rule (a RateBasedRule's RuleId can be activated in a WebACL with Type=RATE_BASED)"}
  IPSet: {status: ok, note: "ChangeToken validation added; DeleteIPSet now returns WAFReferencedItemException if referenced by a Rule/RateBasedRule Predicate.DataId"}
  ByteMatchSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete (same pattern as IPSet)"}
  SizeConstraintSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete"}
  SqlInjectionMatchSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete"}
  XssMatchSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete"}
  GeoMatchSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete"}
  RegexPatternSet: {status: ok, note: "ChangeToken validation; DeleteRegexPatternSet now returns WAFReferencedItemException if referenced by a RegexMatchSet tuple's RegexPatternSetId"}
  RegexMatchSet: {status: ok, note: "ChangeToken validation + ReferencedItem check on delete (a RegexMatchSet is itself a match set referenceable from a Rule Predicate)"}
  RuleGroup: {status: ok, note: "ChangeToken validation; DeleteRuleGroup now returns WAFReferencedItemException if activated in a WebACL with Type=GROUP"}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified against real shapes -- no ChangeToken involved in real AWS, correctly not required here"}
  Logging: {status: ok, note: "PutLoggingConfiguration/GetLoggingConfiguration/DeleteLoggingConfiguration/ListLoggingConfigurations -- no ChangeToken in real AWS, correctly not required"}
  PermissionPolicy: {status: ok, note: "no ChangeToken in real AWS, correctly not required"}
  Migration: {status: ok, note: "CreateWebACLMigrationStack returns a deterministic S3 URL shape; genuinely can't produce a real migration template without wafv2 state, documented as a stub-shape return, not a disguised no-op"}
gaps:
  - WAFNonEmptyEntityException not modeled (DeleteWebACL/DeleteRule/DeleteByteMatchSet/etc. with real AWS reject deletion of an object that still contains children -- e.g. a WebACL that still has Rules, a Rule that still has Predicates, a ByteMatchSet that still has ByteMatchTuples). Only the separate WAFReferencedItemException (deleting an object still referenced BY another object) was fixed this pass; the "still contains children" check is a distinct, real gap left for a follow-up (bd: file on session close).
  - GetSampledRequests/GetRateBasedRuleManagedKeys return empty data (traffic-inspection stub) since gopherstack does not proxy real HTTP requests through WAF rule evaluation -- architectural limitation, not a quick fix.
deferred: []
leaks: {status: clean, note: "no goroutines/timers/background workers in this service; InMemoryBackend is plain locked maps + store.Table, no leak surface"}
---

## Notes

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

- **GetChangeTokenStatus's "unknown token → INSYNC" behavior** (comment + dedicated test
  `TestParity_ChangeTokenStatus_UnknownReturnsINSYNC`) predates this audit and was not
  touched. It is orthogonal to the `validateChangeToken` fix added this pass: the new
  validation lives in the *write* path (Create/Update/Delete reject an unknown/reused
  token), while `GetChangeTokenStatus` is a pure *read* that intentionally still returns
  `INSYNC` for a token it has never seen, matching the pre-existing, already-verified
  parity finding.
