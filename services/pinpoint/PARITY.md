---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: pinpoint
sdk_module: aws-sdk-go-v2/service/pinpoint@v1.42.4
last_audit_commit: 31283c0f
last_audit_date: 2026-08-13
overall: A            # genuine field-diff bugs found and fixed this pass across the template family
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateVoiceTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed to full parity: added TemplateType, LastModifiedDate, DefaultSubstitutions, LanguageCode, TemplateDescription, Version, VoiceId vs VoiceTemplateResponse/VoiceTemplateRequest — was missing all of these"}
  GetVoiceTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "same field set fix as CreateVoiceTemplate"}
  UpdateVoiceTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "now applies DefaultSubstitutions/LanguageCode/TemplateDescription/VoiceId and advances LastModifiedDate/Version, matching every other Update*Template"}
  DeleteVoiceTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "was leaking its templateVersionHistory entry (only Delete{Email,InApp,Push,Sms}Template cleaned it up) — fixed; locked by TestDeleteVoiceTemplate_ReleasesVersionHistory"}
  CreateEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "DefaultSubstitutions was wire-typed as a nested JSON object (map[string]any); the real EmailTemplateRequest/Response serializers/deserializers treat it as a JSON-*encoded string* — fixed. Added missing required TemplateType field"}
  GetEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "same DefaultSubstitutions + TemplateType fixes; simplified to return the model directly instead of a hand-built map (cloneEmailTemplateToResponse deleted, now redundant)"}
  UpdateEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "same DefaultSubstitutions fix"}
  CreateInAppTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "added missing TemplateType (required) and CustomConfig (map[string]string) fields vs InAppTemplateResponse/InAppTemplateRequest"}
  GetInAppTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "same TemplateType/CustomConfig fix"}
  UpdateInAppTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "now applies CustomConfig updates"}
  CreatePushTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETED invented top-level Body/Title fields — the real PushNotificationTemplateRequest/Response has no such fields, per-platform body/title live inside ADM/APNS/Baidu/Default/GCM only. Added missing ADM, Baidu, DefaultSubstitutions (string, same wire-type fix as email), RecommenderId, TemplateType"}
  GetPushTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "same field set fix as CreatePushTemplate"}
  UpdatePushTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "same field set fix; decomposed into applyPushTemplateUpdate to keep the op function's complexity down given the larger field set"}
  CreateSmsTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETED invented SenderId field — the real SMSTemplateRequest/Response has no SenderId (that's an SMS *channel* field, SMSChannelRequest, not a template field). Added missing DefaultSubstitutions (string), RecommenderId, TemplateType"}
  GetSmsTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "same field set fix as CreateSmsTemplate"}
  UpdateSmsTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "same field set fix"}
  UpdateSmsChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETED PromotionalMessagesPerSecond/TransactionalMessagesPerSecond from the request type — real SMSChannelRequest has no such fields (they're SMSChannelResponse-only, AWS-computed account throughput); gopherstack was accepting and echoing back caller-supplied values for fields no real SDK client can send"}
  UpdateEmailChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "added missing OrchestrationSendingRoleArn field vs EmailChannelRequest/EmailChannelResponse"}
  GetCampaignVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was silently falling back to the CURRENT campaign when the requested version number wasn't in history, instead of 404 NotFoundException; AWS's own resource docs for /v1/apps/{appId}/campaigns/{campaignId}/versions/{version} document 404 NotFoundException as the response when \"the specified resource was not found\" — fixed to always 404 on an unknown version. Locked by TestGetCampaignVersion_UnknownVersionNotFound"}
  GetSegmentVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fallback bug and fix as GetCampaignVersion. Locked by TestGetSegmentVersion_UnknownVersionNotFound"}
  # ops carried forward unchanged from the 2026-07-12 pass (files not touched this pass, still trusted):
  GetJourneyExecutionMetrics: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fix from prior pass; now covered by full-state persistence too"}
  GetJourneyExecutionActivityMetrics: {wire: ok, errors: ok, state: ok, persist: ok}
  GetJourneyRunExecutionMetrics: {wire: ok, errors: ok, state: ok, persist: ok}
  GetJourneyRunExecutionActivityMetrics: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveAttributes: {wire: ok, errors: ok, state: ok, persist: n/a}
  SendMessages: {wire: ok, errors: ok, state: ok, persist: n/a}
  SendUsersMessages: {wire: ok, errors: ok, state: ok, persist: n/a}
  SendOTPMessage: {wire: ok, errors: ok, state: ok, persist: n/a}
  UpdateApnsChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApnsChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCampaignVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetSegmentVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
families:
  App: {status: ok, note: "unchanged this pass; last verified 2026-07-12"}
  Campaign: {status: ok, note: "unchanged this pass except GetCampaignVersion fallback-to-current bug (see ops)"}
  Segment: {status: ok, note: "unchanged this pass except GetSegmentVersion fallback-to-current bug (see ops)"}
  Endpoint: {status: ok, note: "unchanged this pass; now participates in full persistence (see Persistence section)"}
  EventStream: {status: ok, note: "unchanged this pass; now participates in full persistence"}
  Channels: {status: ok, note: "SMS channel PromotionalMessagesPerSecond/TransactionalMessagesPerSecond request-side hygiene fix + Email channel OrchestrationSendingRoleArn field addition this pass (see ops); all 10 channel types re-diffed against GCM/APNS/Email/SMS/ADM/Baidu/Voice *ChannelRequest types, no other gaps found. Now participates in full persistence"}
  Tags: {status: ok, note: "unchanged this pass"}
  Template (email): {status: ok, note: "field-diffed this pass: DefaultSubstitutions wire-type bug + missing TemplateType fixed (see ops). Was previously marked ok on an incomplete field-diff — this pass caught what the prior pass missed"}
  Template (inapp): {status: ok, note: "field-diffed this pass: added missing TemplateType + CustomConfig (see ops)"}
  Template (push): {status: ok, note: "field-diffed this pass: DELETED invented top-level Body/Title, added ADM/Baidu/DefaultSubstitutions/RecommenderId/TemplateType (see ops). This family had the largest gap between gopherstack's shape and the real SDK's shape found this pass"}
  Template (sms): {status: ok, note: "field-diffed this pass: DELETED invented SenderId (real field lives on the SMS channel, not the template), added DefaultSubstitutions/RecommenderId/TemplateType (see ops)"}
  Template (voice): {status: ok, note: "was partial — now field-diffed to full parity against VoiceTemplateRequest/VoiceTemplateResponse: added TemplateType/LastModifiedDate/DefaultSubstitutions/LanguageCode/TemplateDescription/Version/VoiceId, plus fixed a templateVersionHistory leak on delete (see ops). Locked by TestVoiceTemplate_FullFieldSet"}
  Journey: {status: ok, note: "unchanged this pass; last verified 2026-07-12"}
  Job (export/import): {status: ok, note: "unchanged this pass"}
  Recommender: {status: ok, note: "unchanged this pass"}
  Messaging (SendMessages/SendUsersMessages/OTP/PutEvents): {status: ok, note: "unchanged this pass"}
  Phone: {status: ok, note: "unchanged this pass"}
  Route matcher: {status: ok, note: "gopherstack-jqh2: added TestExtractOperation_SDKRouteTable (handler_paths_sdk_diff_test.go), a permanent per-op method+path diff of all 122 real ops extracted from pinpoint@v1.42.4 serializers.go against ExtractOperation, including the generic {TemplateName}/{TemplateType}/versions and /active-version paths (discriminated from the per-type Create/Get/Update/Delete paths, which use a literal type segment, not a placeholder). 122/122 pass; no route-matcher bugs found, no duplicate op-resolution table, no query-flag-discriminated ops, no wrong-date-prefix paths."}
  Persistence: {status: ok, note: "was the biggest structural gap: persistRegistry() excluded voiceTemplates/endpoints/eventStreams/channels (all store.Table-backed — mechanical fix, just needed registering) and appSettings/campaignVersions/segmentVersions/templateVersionHistory/campaignActivities/journeyRuns/appEvents/sentMessages/otpCodes (map-shaped state, added as direct JSON fields on backendSnapshot since every value type is already plain-JSON-friendly). Snapshot version bumped 1->2 so an old on-disk snapshot is cleanly discarded (not partially misdecoded) rather than silently accepted with a shape mismatch. Locked by the rewritten TestSnapshotRestore_FullStateRoundTrip, which now asserts these resource kinds SURVIVE a restart instead of asserting they don't"}
gaps: []                 # no known divergences left open this pass
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "GetApplicationDateRangeKpi/GetCampaignDateRangeKpi/GetJourneyDateRangeKpi always return an empty KpiResult.Rows — acceptable stub-shaped-but-real-state pattern (queries real backend, returns AWS-accurate empty analytics), not re-flagged"
  - "SendMessages has thin per-channel-type payload assertions (SMS/EMAIL/push body shape) — response envelope itself is fully covered, but content-shape assertions per channel type could be deepened in a future pass"
  - "PushTemplate/APNSPushNotificationTemplate/AndroidPushNotificationTemplate/DefaultPushNotificationTemplate sub-objects (ADM/APNS/Baidu/Default/GCM) are stored as generic map[string]any rather than field-validated structs, consistent with the project's existing convention for nested platform-override objects elsewhere in this file (Campaign.MessageConfiguration, Journey.Activities, etc.) — round-tripped but not field-validated. Not re-flagged as a gap since gopherstack does not field-validate equivalent nested objects anywhere else in this service either"
leaks: {status: clean, note: "no goroutines/timers spawned by this service; purgeAppStateLocked correctly frees all per-app maps on DeleteApp (verified by reading the function and its purgeTableByAppID/deletePrefixed helpers, leak_test.go covers it). This pass additionally fixed DeleteVoiceTemplate leaking its templateVersionHistory entry (only Delete{Email,InApp,Push,Sms}Template cleaned theirs up — VoiceTemplate was the odd one out); locked by new TestDeleteVoiceTemplate_ReleasesVersionHistory"}
---

## Notes

Protocol: **restjson1**, `/v1/...` paths, service alias `mobiletargeting` (checked via
`httputils.ExtractServiceFromRequest`). Tags on every taggable resource use a **lowercase**
`"tags"` JSON key (confirmed against `deserializers.go`/`serializers.go` `object.Key("tags")`
call sites) while every other field is PascalCase — this looks like a bug if you're skimming
but is AWS-accurate; don't re-flag it.

### Highest-severity finding this pass: the template family had systematic wire-shape drift

Field-diffing every `Create/Get/Update*Template` op against
`aws-sdk-go-v2/service/pinpoint/types` (not just re-trusting the prior pass's "ok" status, per
the audit brief's explicit instruction not to mark a family ok on a no-stub basis alone) found
that **every one of the five template types had real bugs**, not just the previously-flagged
voice template:

- **`DefaultSubstitutions` was wire-typed wrong on every template that has it (email/push/sms/voice).**
  The real `EmailTemplateRequest`/`EmailTemplateResponse`/`PushNotificationTemplateRequest`/
  `PushNotificationTemplateResponse`/`SMSTemplateRequest`/`SMSTemplateResponse`/
  `VoiceTemplateRequest`/`VoiceTemplateResponse` types all declare `DefaultSubstitutions *string`
  — confirmed against the deserializer (`jtv, ok := value.(string)`) and serializer
  (`ok.String(*v.DefaultSubstitutions)`) for each. gopherstack stored/serialized it as a nested
  JSON object (`map[string]any`) instead of the JSON-encoded string a real SDK client actually
  sends/receives. Fixed on `EmailTemplate`/`PushTemplate`/`SmsTemplate`/`VoiceTemplate` to a plain
  `string` field; a real client is expected to pass an already-`json.Marshal`ed string, same as it
  would to the real API.
- **Every template response was missing the required `TemplateType` field.** All five
  `*TemplateResponse` types mark `TemplateType` "This member is required" (`EMAIL`/`SMS`/`VOICE`/
  `PUSH`/`INAPP`). None of gopherstack's five template model structs had it at all — a real SDK
  client reading `output.EmailTemplateResponse.TemplateType` (etc.) always got the zero value.
  Fixed by adding the field to every template struct and populating it at create time.
- **`PushTemplate` had two INVENTED fields (`Body`, `Title`) that don't exist on the real wire.**
  `PushNotificationTemplateRequest`/`PushNotificationTemplateResponse` have no top-level
  `Body`/`Title` — per-platform body/title live inside `ADM`/`APNS`/`Baidu`/`Default`/`GCM` only
  (confirmed against `awsRestjson1_serializeDocumentPushNotificationTemplateRequest`, which has no
  `Body`/`Title` cases). Deleted both fields per the audit brief's "delete gopherstack-invented
  fields" rule. Also added the two real fields gopherstack was missing entirely: `ADM` and `Baidu`
  (the same generic-map treatment already used for `APNS`/`Default`/`GCM`), plus
  `RecommenderId`.
- **`SmsTemplate` had an INVENTED field (`SenderId`) that doesn't exist on the real wire.**
  `SMSTemplateRequest`/`SMSTemplateResponse` have no `SenderId` at all (confirmed against
  `awsRestjson1_serializeDocumentSMSTemplateRequest`) — `SenderId` is a *channel* setting
  (`SMSChannelRequest`), not a template field; the channel-side `SenderId` (in `channels.go`/
  `handler_channels.go`) is unaffected and correct. Deleted the invented field from
  `SmsTemplate`/`createSmsTemplateRequest`; added the real missing `RecommenderId` field.
- **`VoiceTemplate` (previously flagged `partial`) was missing six real fields**:
  `TemplateType`, `LastModifiedDate`, `DefaultSubstitutions`, `LanguageCode`,
  `TemplateDescription`, `Version`, `VoiceId`. All added; `UpdateVoiceTemplate` now advances
  `Version`/`LastModifiedDate` the same way every other `Update*Template` does (it previously did
  neither).
- **`InAppTemplate` was missing `TemplateType` and `CustomConfig`** (`map[string]string`, a real
  field on `InAppTemplateRequest`/`InAppTemplateResponse`). Added both.

All test files under `templates_*_test.go` that exercised the invented fields (`TestSMSTemplate_SenderID`,
`TestSMSTemplate_UpdateSenderID`, the top-level `Body`/`Title` assertions in
`TestPushTemplate_PerPlatformOverrides`/`TestPushTemplate_UpdatePerPlatform`) were rewritten to
exercise the real fields instead (renamed to `TestSMSTemplate_RecommenderID`/
`TestSMSTemplate_UpdateRecommenderID`; push tests now nest `Body`/`Title` inside `Default`, and
also cover `ADM`/`Baidu`). New tests lock every added field:
`TestVoiceTemplate_FullFieldSet`, `TestEmailTemplate_TemplateType`,
`TestInAppTemplate_TemplateTypeAndCustomConfig`, and the rewritten
`TestEmailTemplate_DefaultSubstitutions` (now asserts the string wire shape instead of a nested
object).

### Second-highest-severity finding: persistRegistry() excluded most of the backend's state

The prior pass documented this as a known gap rather than fixing it (see the 2026-07-12 gaps
list, now empty). `voiceTemplates`/`endpoints`/`eventStreams`/`channels` are `store.Table`-backed
the same as every persisted resource kind — they simply weren't registered in
`persistRegistry()`. `appSettings`/`campaignVersions`/`segmentVersions`/
`templateVersionHistory`/`campaignActivities`/`journeyRuns`/`appEvents`/`sentMessages`/
`otpCodes` are map-shaped (`map[string][]T` / `map[string]T`, not `map[string]*T`) so they can't
go through `store.Table` (which requires a pure key function on one concrete pointer type), but
every value type is already a plain JSON-friendly struct, so they're persisted as direct fields
on `backendSnapshot` instead of a separate DTO. `pinpointSnapshotVersion` bumped 1→2 so an
old-shape snapshot is cleanly discarded (the existing version-mismatch path already did this —
`resetMapStateLocked`/`nonNil*Map` helpers added so the discard path and a snapshot from before
these fields existed both leave every map non-nil, never triggering a nil-map write panic).

### Third finding: GetCampaignVersion/GetSegmentVersion silently fell back to the current resource

Flagged as an open question in the prior pass ("low confidence on whether this is intentional
leniency vs a bug"). Resolved this pass by checking AWS's own API reference docs for
`/v1/apps/{appId}/campaigns/{campaignId}/versions/{version}`: the documented response table
lists `404 NotFoundException` as "The request failed because the specified resource was not
found" — a requested version number absent from history is exactly that case. Fixed both ops to
404 instead of substituting the current campaign/segment under the wrong `Version` number in the
response (which would be actively misleading to a caller who explicitly asked for e.g. version 3
and silently got version 7's content labeled `"Version": 7`).

### Fourth finding: SMS channel and Email channel wire hygiene

- `updateSMSChannelRequest` accepted `PromotionalMessagesPerSecond`/`TransactionalMessagesPerSecond`
  from the request body and echoed back whatever the caller sent. The real `SMSChannelRequest` has
  no such fields — they exist only on `SMSChannelResponse` as AWS-computed account throughput. No
  real SDK client can send them (there's no field on the Go request struct to set), so this was
  harmless in practice, but per the audit brief's field-diff instruction it's wire-shape noise
  that shouldn't exist. Deleted from the request type.
- `updateEmailChannelRequest` was missing `OrchestrationSendingRoleArn`, a real field on both
  `EmailChannelRequest` and `EmailChannelResponse`. Added.

### DeleteVoiceTemplate template-version-history leak

`DeleteVoiceTemplate` was the only one of the five `Delete*Template` ops that didn't clean up its
`templateVersionHistory[name+"/VOICE"]` entry — `Delete{Email,InApp,Push,Sms}Template` all
`delete()` their corresponding key, `DeleteVoiceTemplate` didn't. Fixed; locked by
`TestDeleteVoiceTemplate_ReleasesVersionHistory` in `leak_test.go`.

### funlen nolint removed

`Handler.GetSupportedOperations` carried `//nolint:funlen` over a ~140-line literal list of
operation-name strings. Decomposed into one small per-resource-family helper function each
(`supportedOpsAppFamily`, `supportedOpsCampaignFamily`, ...), concatenated by the now-short
`GetSupportedOperations`. No package-level state introduced — each helper returns a fresh local
slice, so there was no need for a `sync.OnceValue`/`gochecknoglobals` route-table pattern here
(that pattern is for lookup tables consulted per-request; this list is built once per call and is
cheap either way).

### Traps for the next auditor (looks-wrong-but-correct)

- `GetApplicationDateRangeKpi`/`GetCampaignDateRangeKpi`/`GetJourneyDateRangeKpi` always return
  an empty `KpiResult.Rows` slice. This is intentional — gopherstack has no analytics engine to
  compute real KPI rows, and an empty-but-correctly-shaped result is the honest emulation choice
  (not a disguised no-op, since the ops do validate the parent resource exists and return the
  real response envelope). Do not re-flag without a concrete plan for synthesizing KPI data.
- `UpdateTemplateActiveVersion` is a genuine no-op on the version-history data structure (the
  last entry in `templateVersionHistory` already *is* the active version by construction — every
  `Update*Template` call appends), but it still validates the template exists and returns
  `NotFoundException` correctly, and returns the real `202 Accepted` envelope. Confirmed correct,
  not a stub.
- `tags` uses a lowercase JSON key while everything else is PascalCase (see protocol note above)
  — this is real AWS behavior, not a bug.
- `ADM`/`APNS`/`Baidu`/`Default`/`GCM` on `PushTemplate`, and `Attributes`/`Dimensions`/etc.
  elsewhere in this service, are intentionally generic `map[string]any` rather than fully typed
  structs — this matches the project's existing convention for nested platform-override objects
  (`Campaign.MessageConfiguration`, `Journey.Activities`, ...) and is round-tripped, not
  field-validated, by design. Do not re-flag as a gap without a concrete plan to type every nested
  object in the service consistently.

### Persistence

`persistence.go`'s `Snapshot`/`Restore` now round-trips the ENTIRE backend: every
`store.Table`-backed resource (`apps`, `campaigns`, `channels`, `emailTemplates`, `endpoints`,
`eventStreams`, `exportJobs`, `importJobs`, `inAppTemplates`, `journeys`, `pushTemplates`,
`recommenders`, `segments`, `smsTemplates`, `voiceTemplates`) through `store.Registry`, plus the
map-shaped state (`appSettings`, `campaignVersions`, `segmentVersions`,
`templateVersionHistory`, `campaignActivities`, `journeyRuns`, `appEvents`, `sentMessages`,
`otpCodes`) as direct JSON fields on `backendSnapshot`. `pinpointSnapshotVersion` is `2`; an
older-version (or otherwise shape-mismatched) snapshot is discarded and the backend starts
empty rather than attempting a partial decode, same policy as before, now also resetting the
map-shaped state to non-nil empty maps on that path.
