---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: ses
sdk_module: aws-sdk-go-v2/service/ses@v1.34.20   # version audited against (query-XML, 2010-12-01)
last_audit_commit: a40e7cc1                      # HEAD when this manifest was written
last_audit_date: 2026-07-05
overall: A            # ~370 LOC of genuine production fixes + ~500 LOC of test additions/updates
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  PutIdentityPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire fixed this pass — see families.void_result_ops"}
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
  SendEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "added AccountSendingPausedException + 24h-quota + ConfigurationSetDoesNotExist enforcement (all previously unenforced)"}
  SendRawEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "delegates to SendEmail, inherits the same fixes"}
  SendTemplatedEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "same enforcement added as SendEmail"}
  SendBulkTemplatedEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "was silently dropping ConfigurationSetName/ReplyToAddresses/ReturnPath/SourceArn — all real SendBulkTemplatedEmailInput members; now parsed and threaded through"}
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
  DeleteReceiptRuleSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "clears activeRuleSet if it was the active set"}
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
  persistence_janitor: {status: ok, note: "Snapshot/Restore cover every map (identities, templates, configSets, receiptRuleSets, receiptFilters, eventDestinations, trackingOptions, customVerifTemplates, policies) with correct deep-copies; Restore re-applies the TTL/maxRetainedEmails bound immediately; janitor uses pkgs/worker ticker + lockmetrics coarse lock; no goroutine or map leaks found."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "GetSendStatistics Bounces/Complaints/Rejects always report 0 — no bounce/complaint event simulation exists in this backend (bd: gopherstack-uve)"
  - "LimitExceededException never returned — no per-resource count caps modeled (max receipt rules/templates/filters etc.) (bd: gopherstack-ssk)"
  - "MailFromDomainNotVerifiedException never triggers — SetIdentityMailFromDomain instantly marks Success, consistent with this service's instant-verify convention everywhere else (VerifyEmailIdentity/VerifyDomainIdentity/VerifyDomainDkim all skip the real Pending window too); deliberately not changed to avoid an inconsistent one-off Pending state (bd: gopherstack-nbp)"
  - "MaxSendRate (per-second) advertised via GetSendQuota but not enforced, only the 24h quota is now enforced (bd: gopherstack-a6y)"
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

**Test-only `AppendEmailForTest` helper** (`export_test.go`) was added because the new 24h
send-quota enforcement is incompatible with the pre-existing retention/eviction-cap tests
that send `maxRetainedEmails+N` (10000+) emails through `SendEmail` in a loop to exercise
`appendEmailLocked`'s eviction path — a real account would never accumulate 10000 sends
within a 200/day quota. `AppendEmailForTest` calls the same internal `appendEmailLocked` path
`SendEmail` uses (so eviction/O(1)-map-sync is still exercised for real) while bypassing the
business-rule preconditions (verification, quota, account-paused) that are orthogonal to what
those specific tests assert. Don't be alarmed seeing it used for high-volume tests instead of
`SendEmail` — that's intentional now, not a regression.
