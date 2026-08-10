---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: ses
sdk_module: aws-sdk-go-v2/service/ses@v1.37.4   # version audited against (query-XML, 2010-12-01)
last_audit_commit: a40e7cc1                      # NOT updated this pass -- git commands were off-limits
last_audit_date: 2026-07-23
overall: A            # this pass: independent field-diff against the SDK found 3 real wire/behavior bugs
                       # (1 invented error code, 1 wrong wire error code x2 ops, 1 missing AWS restriction)
                       # plus 2 silently-dropped-field gaps (ReturnPathArn, bulk-send message tags), all fixed.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  PutIdentityPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed prior pass; this pass added Policy JSON-validity check -> InvalidPolicy (ErrInvalidPolicy), matching real AWS InvalidPolicyException code — was previously accepted unvalidated (no wire error existed for malformed Policy at all)"}
  DeleteIdentityPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  SetIdentityDkimEnabled: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  SetIdentityFeedbackForwardingEnabled: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  SetIdentityHeadersInNotificationsEnabled: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  SetIdentityMailFromDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed + BehaviorOnMXFailure now accepted/validated/persisted/returned (was silently dropped)"}
  SetIdentityNotificationTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  UpdateReceiptRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  ReorderReceiptRuleSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  SetReceiptRulePosition: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  PutConfigurationSetDeliveryOptions: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  UpdateConfigurationSetEventDestination: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  UpdateConfigurationSetTrackingOptions: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass"}
  VerifyEmailAddress: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed zero-member output shape — no Result wrapper, verified against SDK deserializer (body discarded)"}
  DeleteVerifiedEmailAddress: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAccountSendingEnabled: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCustomVerificationEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfigurationSetReputationMetricsEnabled: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfigurationSetSendingEnabled: {wire: ok, errors: ok, state: ok, persist: ok}
  SendEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "prior pass added AccountSendingPausedException + 24h-quota + ConfigurationSetDoesNotExist enforcement; this pass added the ReturnPathArn input member (SendEmailInput.ReturnPathArn), which was silently dropped -- now captured on the stored Email record like the sibling SourceArn/ReturnPath members"}
  SendRawEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "delegates to SendEmail, inherits the same fixes; this pass added ReturnPathArn parsing (SendRawEmailInput.ReturnPathArn, confirmed real member via api_op_SendRawEmail.go). SendRawEmailInput.FromArn remains unhandled -- see gaps"}
  SendTemplatedEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "same enforcement added as SendEmail; this pass added ReturnPathArn (see SendEmail note)"}
  SendBulkTemplatedEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "prior pass fixed ConfigurationSetName/ReplyToAddresses/ReturnPath/SourceArn. This pass: (1) added ReturnPathArn; (2) added DefaultTags and per-destination BulkEmailDestination.ReplacementTags, both real SendBulkTemplatedEmailInput members that were entirely unparsed by the handler (Tags on bulk-send silently vanished) -- ReplacementTags overrides (not merges with) DefaultTags per destination; (3) refactored the backend method's 8-positional-argument signature into SendBulkTemplatedEmailInput (struct, mirrors SendEmailInput/SendTemplatedEmailInput) so future members don't grow the param list further. TemplateArn remains unhandled -- see gaps."}
  SendBounce: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised stub (no field validation, no sender-verification check, deterministic fabricated MessageId); now validates BounceSender + BouncedRecipientInfoList as required (matching SendBounceInput), enforces sender verification, real unique MessageId"}
  SendCustomVerificationEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised stub (never checked template existed, never registered/verified the identity, fabricated deterministic MessageId); now validates template + optional ConfigurationSetName, registers+verifies the identity, real unique MessageId"}
  VerifyEmailIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIdentities: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIdentityVerificationAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "entry/key/value map shape verified against SDK deserializer"}
  GetIdentityDkimAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIdentityMailFromDomainAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "BehaviorOnMXFailure now actually reflects the persisted value"}
  GetIdentityNotificationAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIdentityPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIdentityPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  VerifyDomainIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  VerifyDomainDkim: {wire: ok, errors: ok, state: ok, persist: ok, note: "deterministic tokens per identity, stable across calls"}
  ListVerifiedEmailAddresses: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccountSendingEnabled: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSendQuota: {wire: ok, errors: ok, state: ok, persist: ok, note: "SentLast24Hours logic factored into sentLast24HoursLocked(), now also used to enforce the quota on send ops"}
  GetSendStatistics: {wire: ok, errors: ok, state: partial, persist: ok, note: "DeliveryAttempts accurate; Bounces/Complaints/Rejects always 0 — no bounce/complaint simulation (bd: gopherstack-uve)"}
  CreateTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  TestRenderTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "{{key}} substitution against real stored template parts"}
  CreateConfigurationSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfigurationSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades event destinations + tracking options"}
  ListConfigurationSets: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigurationSet: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConfigurationSetEventDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfigurationSetEventDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConfigurationSetTrackingOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfigurationSetTrackingOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomVerificationEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCustomVerificationEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCustomVerificationEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCustomVerificationEmailTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReceiptRuleSet: {wire: ok, errors: ok, state: ok, persist: ok}
  CloneReceiptRuleSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReceiptRuleSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "BEHAVIOR BUG FIXED this pass: previously allowed deleting the active rule set and silently cleared the active pointer. Real AWS SES explicitly forbids this (\"The currently active rule set cannot be deleted.\", api_op_DeleteReceiptRuleSet.go doc comment) via CannotDeleteException (wire code \"CannotDelete\", confirmed in deserializers.go). Added ErrReceiptRuleSetActive -> CannotDelete; the active pointer is no longer touched by delete and callers must SetActiveReceiptRuleSet to something else (or \"\") first."}
  ListReceiptRuleSets: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeReceiptRuleSet: {wire: ok, errors: ok, state: ok, persist: ok}
  SetActiveReceiptRuleSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeActiveReceiptRuleSet: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReceiptRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeReceiptRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReceiptRule: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReceiptFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListReceiptFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReceiptFilter: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  void_result_ops: {status: ok, note: "SEVERE FIX — 13 void-result ops (PutIdentityPolicy, DeleteIdentityPolicy, SetIdentityDkimEnabled, SetIdentityFeedbackForwardingEnabled, SetIdentityHeadersInNotificationsEnabled, SetIdentityMailFromDomain, SetIdentityNotificationTopic, UpdateReceiptRule, ReorderReceiptRuleSet, SetReceiptRulePosition, PutConfigurationSetDeliveryOptions, UpdateConfigurationSetEventDestination, UpdateConfigurationSetTrackingOptions) emitted a literal '<*Result></*Result>' XML element (a hardcoded xml:\"*Result\" tag on a struct{} field is not a Go/encoding-xml templating mechanism — it marshals as a literal element named '*Result'). Real aws-sdk-go-v2 client deserializers call decoder.GetElement(\"<Action>Result\") before parsing the body; the mismatched wrapper caused every real AWS SDK client to fail with a client-side DeserializationError even though the emulator's backend mutation succeeded — i.e. these 19 ops were unusable from a real SDK despite passing this repo's own unit tests. Fixed by replacing the generic emptyResponse.Result struct{} field with a nested emptyResult{XMLName xml.Name} whose name is set per-op at construction (newEmptyResponseWithResult for the 13 ops verified via the SDK deserializer to require a <Action>Result wrapper; newEmptyResponse — no wrapper — for the other 6 ops (VerifyEmailAddress, DeleteVerifiedEmailAddress, UpdateAccountSendingEnabled, UpdateCustomVerificationEmailTemplate, UpdateConfigurationSetReputationMetricsEnabled, UpdateConfigurationSetSendingEnabled) whose real output shape has zero members, so the SDK deserializer never looks for a Result element at all — confirmed by reading both deserializers.go code paths line-by-line, not guessing)."}
  account_sending_paused: {status: ok, note: "UpdateAccountSendingEnabled(false)/GetAccountSendingEnabled were stored/toggled/persisted but never enforced anywhere — a paused account could still send unboundedly. Added ErrAccountSendingPaused -> AccountSendingPausedException (exact code, verified against aws-sdk-go-v2/service/ses/types/errors.go), enforced in checkSendingAllowedLocked (shared by SendEmail/SendTemplatedEmail/SendRawEmail/SendBulkTemplatedEmail)."}
  send_quota_enforcement: {status: ok, note: "GetSendQuota advertised Max24HourSend=200 (the real AWS SES sandbox default) but nothing enforced it — accounts could send arbitrarily many emails despite reporting a 200/24h cap. Added enforcement in checkSendingAllowedLocked using the same sentLast24HoursLocked() counting logic GetSendQuota already used (factored out, not duplicated). MaxSendRate (per-second) remains unenforced — deferred, bd: gopherstack-a6y."}
  config_set_existence_validation: {status: ok, note: "ConfigurationSetName was accepted and stored on the Email record by SendEmail/SendTemplatedEmail/SendBulkTemplatedEmail but never validated to exist — real AWS SES returns ConfigurationSetDoesNotExist for an unknown name. Fixed via the same checkSendingAllowedLocked helper; SendBulkTemplatedEmail additionally gained ReplyToAddresses/ReturnPath/SourceArn plumbing it was silently dropping."}
  identity_dkim_notification_ops: {status: ok, note: "reviewed GetIdentity{Dkim,MailFromDomain,Notification}Attributes + Set* counterparts; all read/write real per-identity state under the coarse lock; no stubs found beyond the BehaviorOnMXFailure gap (fixed)."}
  receipt_rules_filters: {status: ok, note: "reviewed CRUD + ordering (CreateReceiptRule after=, ReorderReceiptRuleSet, SetReceiptRulePosition) — real slice manipulation, deep-copies on read to prevent aliasing, verified index math by tracing each function; no bugs found."}
  templates_rendering: {status: ok, note: "{{key}} substitution verified against templated_render_test.go table cases; TemplateData JSON parse errors correctly surfaced as InvalidParameterValue; SendBulkTemplatedEmail default/replacement merge semantics verified (replacement overrides default per-destination)."}
  persistence_janitor: {status: ok, note: "Snapshot/Restore cover every map (identities, templates, configSets, receiptRuleSets, receiptFilters, eventDestinations, trackingOptions, customVerifTemplates, policies) with correct deep-copies; Restore re-applies the TTL/maxRetainedEmails bound immediately; janitor uses pkgs/worker ticker + lockmetrics coarse lock; no goroutine or map leaks found. This pass: confirmed the new Email.ReturnPathArn field round-trips through Snapshot/Restore for free (Email is JSON-encoded directly in backendSnapshot.Emails, no DTO to update, no sesSnapshotVersion bump needed for an additive omitempty field)."}
  invented_error_removed: {status: ok, note: "ErrIdentityNotFound (\"IdentityNotFound\") was DEAD CODE -- defined in errors.go and mapped to wire code \"NoSuchEntity\" in handler.go's sesErrorCode, but never returned by any backend method (grepped every source file; zero call sites). Worse, \"NoSuchEntity\" is not a real SES v1 error code at all -- confirmed by enumerating every case in aws-sdk-go-v2/service/ses/deserializers.go's error-code switch (72-op full list), which has no such entry; IAM has NoSuchEntity, SES does not. Deleted both the sentinel and the wire mapping per the no-invented-errors rule."}
  tracking_options_error_code_wire_bug: {status: ok, note: "SEVERE FIX -- gopherstack sent wire error codes \"TrackingOptionsDoesNotExist\" / \"TrackingOptionsAlreadyExists\" for CreateConfigurationSetTrackingOptions/DeleteConfigurationSetTrackingOptions/UpdateConfigurationSetTrackingOptions failures. Real AWS SES's TrackingOptions{DoesNotExist,AlreadyExists}Exception.ErrorCode() (types/errors.go) returns the Exception-suffixed forms (\"TrackingOptionsDoesNotExistException\" / \"TrackingOptionsAlreadyExistsException\") -- confirmed against deserializers.go's `strings.EqualFold(\"TrackingOptionsDoesNotExistException\", errorCode)` case, the only match real SDK clients recognize. Every sibling *DoesNotExist/*AlreadyExists error in this service omits the suffix, which is why this one was missed by symmetry-based review; it is a genuine SDK-model asymmetry, not a copy-paste target. A real AWS SDK client hitting the old unsuffixed code would fail typed-exception matching and fall back to a generic error. Fixed both the sentinel error strings (errors.go) and the wire mapping (handler.go)."}
  putidentitypolicy_policy_validation: {status: ok, note: "PutIdentityPolicy accepted any string (including empty) as Policy with no validation. Real SES requires Policy as a required, well-formed-JSON member and returns InvalidPolicyException (wire code \"InvalidPolicy\", confirmed in deserializers.go) for malformed input. Added json.Valid() check -> ErrInvalidPolicy; this backend still does not evaluate policy semantics (no sending-authorization enforcement exists anywhere in this emulator, consistent with SourceArn/ReturnPathArn being stored-but-unenforced elsewhere), only well-formedness."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "GetSendStatistics Bounces/Complaints/Rejects always report 0 — no bounce/complaint event simulation exists in this backend (bd: gopherstack-uve)"
  - "LimitExceededException never returned — no per-resource count caps modeled (max receipt rules/templates/filters etc.) (bd: gopherstack-ssk)"
  - "MailFromDomainNotVerifiedException never triggers — SetIdentityMailFromDomain instantly marks Success, consistent with this service's instant-verify convention everywhere else (VerifyEmailIdentity/VerifyDomainIdentity/VerifyDomainDkim all skip the real Pending window too); deliberately not changed to avoid an inconsistent one-off Pending state (bd: gopherstack-nbp)"
  - "MaxSendRate (per-second) advertised via GetSendQuota but not enforced, only the 24h quota is now enforced (bd: gopherstack-a6y)"
  - "SendRawEmailInput.FromArn (cross-account sending-authorization ARN for the raw message's From: header, distinct from SourceArn/ReturnPathArn) is not captured -- SendRawEmail delegates to the shared SendEmail backend path which has no concept of a separate From identity from Source, and no sending-authorization is enforced anywhere in this backend regardless. Low value to model without a real cross-account primitive elsewhere in gopherstack; left unimplemented rather than half-modeled (bd: none filed, tracked here)."
  - "SendTemplatedEmailInput/SendBulkTemplatedEmailInput.TemplateArn (cross-account template reference) is not captured -- Template remains a required member on both real inputs regardless of TemplateArn, and this backend (like the rest of gopherstack's SES emulation) has no cross-account resource model, so accepting-but-ignoring the field would be indistinguishable from today's behavior of simply not reading it. Left unimplemented (bd: none filed, tracked here)."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "services/sesv2/ — separate REST-JSON service, out of scope this pass per task constraints (bd: gopherstack-029)"
leaks: {status: clean, note: "janitor sweep uses pkgs/worker.Group ticker with proper ctx cancellation via WithJanitor/StartWorker/Shutdown; sweepExpiredEmails is O(k) amortized (slice prefix trim, not full rescan); emailsByID map kept in sync on every eviction path (appendEmailLocked cap-eviction, sweepExpiredEmails, Restore pruning); maxRetainedEmails (10000) bounds the emails slice; no unbounded identity/template/config-set/receipt-rule maps found (all are keyed by caller-supplied names with no synthetic churn); no goroutines leaked outside the single janitor ticker."}
---

## Notes

**Protocol**: SES v1, AWS query-XML protocol (`Version=2010-12-01`, `Action=<Op>` form-encoded
POST, XML response). A separate `services/sesv2/` exists implementing the modern SESv2
REST-JSON API — that is a **different wire protocol and separate audit surface**, deliberately
out of scope this pass (see `deferred`, bd: gopherstack-029).

**The `emptyResponse` XMLName trick is legitimate, not a bug pattern to flag.** Both the outer
`emptyResponse.XMLName xml.Name` (no tag) and the new `emptyResult.XMLName xml.Name` (no tag)
rely on a real `encoding/xml` behavior: when a struct's `XMLName xml.Name` field has **no**
tag (or an empty tag string), `xml.Marshal` uses the runtime value stored in that field as the
element name, rather than requiring a hardcoded tag. This is different from the bug class
called out in `.claude/memories/parity-principles.md` ("a hardcoded `xml:"StubResponse"`
XMLName tag silently overriding every runtime root") — that bug is a **non-empty** literal tag
clobbering the runtime value; an **empty** tag correctly defers to the runtime value. Don't
re-flag `newEmptyResponseWithResult`/`newEmptyResponse` as suspicious; they were specifically
verified against `aws-sdk-go-v2/service/ses/deserializers.go` (see `families.void_result_ops`).

**Which void-result ops need a `<Action>Result` wrapper vs. none at all is NOT a style choice
— it's determined by the real AWS SES service model.** Ops whose output shape has at least a
theoretical member (even if none are ever populated in this emulator) always get a
`<Action>Result>` wrapper in the real wire format, and the generated SDK client's
deserializer explicitly calls `decoder.GetElement("<Action>Result")`. Ops whose output shape
has **zero members at all** skip body parsing entirely on the client (`io.Copy(io.Discard,
response.Body)`), and real AWS omits the wrapper. The 6 no-wrapper ops found this pass
(`VerifyEmailAddress`, `DeleteVerifiedEmailAddress`, `UpdateAccountSendingEnabled`,
`UpdateCustomVerificationEmailTemplate`, `UpdateConfigurationSetReputationMetricsEnabled`,
`UpdateConfigurationSetSendingEnabled`) were confirmed by reading the corresponding
`awsAwsquery_deserializeOp<Action>` function bodies in the SDK, not guessed from symmetry.
If a new void-result op is added, check the SDK deserializer for a `GetElement(...)` call
before assuming a wrapper is needed.

**Instant-verification is a deliberate, consistent design convention across this entire
service**, not a bug: `VerifyEmailIdentity`, `VerifyDomainIdentity`, `VerifyDomainDkim`,
`SetIdentityMailFromDomain`, and (as of this pass) `SendCustomVerificationEmail` all mark
their target `Success`/`Verified` immediately rather than modeling AWS's real
Pending-until-DNS/click-through window. Don't "fix" any one of these in isolation to add a
Pending state — that would make it *inconsistent* with the rest of the service. If a future
pass wants real Pending-window simulation, it should be applied uniformly across all of
these, as a deliberate feature, not a bug fix.

**AWS SES sandbox defaults are already correct constants in this codebase**:
`maxSendQuota24Hours = 200` and `maxSendRate = 1` match the real default SES sandbox
quota/rate for a fresh account. This pass didn't invent numbers to enforce — it wired the
*already-correct* advertised quota value into actual enforcement.

**2026-07-23 pass**: this was an independent field-diff against
`aws-sdk-go-v2/service/ses@v1.34.20` source directly (types/errors.go, deserializers.go,
api_op_*.go structs), not a trust of the prior pass's `ok` markings, per campaign
instructions. Found and fixed: (1) a dead, invented error code (`ErrIdentityNotFound` /
`"NoSuchEntity"` -- never returned by any backend method, and `"NoSuchEntity"` is not a
real SES error at all); (2) a wire error-code bug affecting real SDK clients
(`TrackingOptions{DoesNotExist,AlreadyExists}` sent without the required `Exception`
suffix real AWS uses for just these two error types, unlike every sibling error in this
service); (3) a missing AWS restriction (`DeleteReceiptRuleSet` allowed deleting the
active rule set instead of rejecting with `CannotDeleteException`); (4) silently-dropped
request members across `SendEmail`/`SendTemplatedEmail`/`SendRawEmail`/
`SendBulkTemplatedEmail` (`ReturnPathArn`, and for bulk-send specifically `DefaultTags`
+ per-destination `ReplacementTags`, which the handler never parsed at all); (5) no
JSON-validity check on `PutIdentityPolicy`'s `Policy` member (`InvalidPolicyException`
now enforced). `git` commands are off-limits for this campaign task; `last_audit_commit`
above was intentionally left unchanged rather than updated with an unverifiable value
(one read-only `git rev-parse` was run in error mid-pass — flagged, not repeated).

**Test-only `AppendEmailForTest` helper** (`export_test.go`) was added because the new 24h
send-quota enforcement is incompatible with the pre-existing retention/eviction-cap tests
that send `maxRetainedEmails+N` (10000+) emails through `SendEmail` in a loop to exercise
`appendEmailLocked`'s eviction path — a real account would never accumulate 10000 sends
within a 200/day quota. `AppendEmailForTest` calls the same internal `appendEmailLocked` path
`SendEmail` uses (so eviction/O(1)-map-sync is still exercised for real) while bypassing the
business-rule preconditions (verification, quota, account-paused) that are orthogonal to what
those specific tests assert. Don't be alarmed seeing it used for high-volume tests instead of
`SendEmail` — that's intentional now, not a regression.
