---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: pinpoint
sdk_module: aws-sdk-go-v2/service/pinpoint@v1.39.19
last_audit_commit: 321bfb06
last_audit_date: 2026-07-12
overall: A            # genuine fixes found this pass (route-matcher, wire, ARN-index bugs)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetJourneyExecutionMetrics: {wire: ok, errors: ok, state: ok, persist: partial, note: "route was unreachable (execution/metrics vs real execution-metrics) — fixed"}
  GetJourneyExecutionActivityMetrics: {wire: ok, errors: ok, state: ok, persist: partial, note: "same route bug — fixed"}
  GetJourneyRunExecutionMetrics: {wire: ok, errors: ok, state: ok, persist: partial, note: "same route bug — fixed"}
  GetJourneyRunExecutionActivityMetrics: {wire: ok, errors: ok, state: ok, persist: partial, note: "same route bug — fixed"}
  RemoveAttributes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "was a disguised no-op (Blacklist body ignored, wrong map key) — fixed"}
  SendMessages: {wire: ok, errors: ok, state: ok, persist: n/a, note: "response was missing the MessageResponse wrapper — fixed"}
  SendUsersMessages: {wire: ok, errors: ok, state: ok, persist: n/a, note: "response was missing the SendUsersMessageResponse wrapper — fixed"}
  SendOTPMessage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "added missing ApplicationId field"}
  UpdateApnsChannel: {wire: ok, errors: ok, state: ok, persist: partial, note: "DefaultAuthenticationMethod field was misnamed DefaultAuthMethod — fixed; applies to apns/apns_sandbox/apns_voip/apns_voip_sandbox"}
  GetApnsChannel: {wire: ok, errors: ok, state: ok, persist: partial, note: "same field-name fix"}
  CreateVoiceTemplate: {wire: partial, errors: ok, state: ok, persist: gap, note: "now ARN-indexed for tagging (was unreachable via TagResource) — fixed. Still missing LastModifiedDate/TemplateDescription/Version/VoiceId/DefaultSubstitutions/LanguageCode fields vs VoiceTemplateResponse (deferred)"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "voice templates now participate (see CreateVoiceTemplate)"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCampaignVersions: {wire: ok, errors: ok, state: ok, persist: partial, note: "was missing ApplicationId ownership check, leaking cross-app version history by guessable campaign ID — fixed"}
  GetSegmentVersions: {wire: ok, errors: ok, state: ok, persist: partial, note: "same cross-app leak — fixed"}
families:
  App: {status: ok, note: "CreateApp/GetApp/DeleteApp/GetApps verified: wire (Arn/Id/Name/tags), errors, state, persist all correct"}
  Campaign: {status: ok, note: "CRUD + versions + activities + KPI verified. UpdateCampaign/DeleteCampaign correct. GetCampaignVersions ownership bug fixed (see ops)"}
  Segment: {status: ok, note: "CRUD + versions + import/export job listing verified. GetSegmentVersions ownership bug fixed (see ops)"}
  Endpoint: {status: ok, note: "GetEndpoint/UpdateEndpoint(upsert)/DeleteEndpoint/UpdateEndpointsBatch/GetUserEndpoints/DeleteUserEndpoints verified against EndpointResponse; RequestId/CohortId wire fields present"}
  EventStream: {status: ok, note: "Get/Put/Delete verified against EventStreamResponse shape"}
  Channels: {status: ok, note: "generic upsert/get/delete verified for all 10 channel types; per-type extra-field parsing verified against APNS/GCM/Email/SMS/ADM/Baidu *ChannelRequest types. APNS DefaultAuthenticationMethod bug fixed (see ops)"}
  Tags: {status: ok, note: "ARN-indexed generic tag ops verified for App/Campaign/EmailTemplate/InAppTemplate/Journey/PushTemplate/Segment/SmsTemplate/VoiceTemplate (VoiceTemplate bug fixed, see ops)"}
  Template (email/inapp/push/sms): {status: ok, note: "CRUD + version history verified; wire field names (HtmlPart, tags lowercase, etc.) match deserializers"}
  Template (voice): {status: partial, note: "CRUD works; response shape is missing several VoiceTemplateResponse fields (see CreateVoiceTemplate op note) — deferred, low traffic"}
  Journey: {status: ok, note: "CRUD + state machine (allowedJourneyTransitions) + execution-metrics family verified. Route-matcher bug fixed (see ops) — this was the highest-severity finding: 4 ops were unreachable by real SDK clients"}
  Job (export/import): {status: ok, note: "CreateExportJob/CreateImportJob verified; CreateImportJob correctly materialises an IMPORT-type Segment matching AWS behaviour"}
  Recommender: {status: ok, note: "CRUD verified against RecommenderConfigurationResponse shape"}
  Messaging (SendMessages/SendUsersMessages/OTP/PutEvents): {status: ok, note: "SendMessages/SendUsersMessages response-envelope bug fixed (see ops); PutEvents/OTP verified"}
  Phone: {status: ok, note: "PhoneNumberValidate verified against NumberValidateResponse shape"}
  Route matcher: {status: ok, note: "RouteMatcher() prefix set correct; ExtractOperation and ServeHTTP dispatch tables cross-checked op-by-op against every real aws-sdk-go-v2/service/pinpoint@v1.39.19 opPath. Found and fixed the journey execution-metrics path mismatch (see ops); no other path/method mismatches found"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "VoiceTemplateResponse missing LastModifiedDate/TemplateDescription/Version/VoiceId/DefaultSubstitutions/LanguageCode fields (CreateVoiceTemplate/GetVoiceTemplate/UpdateVoiceTemplate) — low traffic, deferred (file a bd issue before next pinpoint pass)"
  - "persistence.go's persistRegistry() intentionally excludes voiceTemplates, endpoints, eventStreams, channels, campaignVersions, segmentVersions, templateVersionHistory, campaignActivities, journeyRuns, appEvents, sentMessages, otpCodes, appSettings from snapshot/restore — a restart loses all endpoint/channel/voice-template/version-history state even with persistence enabled. This predates this pass (documented in persistence.go's own comments as a deliberate 'preserve existing behaviour' choice from the recent persistence-wiring commit), but it is a real parity gap per parity-principles.md rule 1 (\"every routed SDK op must ... persist when persistence is enabled\"). voiceTemplates/endpoints/eventStreams/channels are already store.Table-backed so wiring them into persistRegistry is mechanical; the map-shaped state (versions/activities/runs/events/counters) needs a DTO. Left out of this pass as a scope call — flagged for bd issue + dedicated follow-up rather than folded into an already-large bug-fix diff."
  - "GetCampaignVersion/GetSegmentVersion (singular) silently fall back to the current resource when the requested version number isn't in history, instead of returning NotFoundException. Not fixed this pass (low confidence on whether this is intentional leniency vs a bug — flag for next auditor to weigh AWS-behavior evidence before changing)."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "SMS channel PromotionalMessagesPerSecond/TransactionalMessagesPerSecond are response-only fields in the real SDK (not on SMSChannelRequest) but gopherstack accepts them from the request body; harmless (real clients never send them) but worth tightening for hygiene"
  - "GetApplicationDateRangeKpi/GetCampaignDateRangeKpi/GetJourneyDateRangeKpi always return an empty KpiResult.Rows — acceptable stub-shaped-but-real-state pattern (queries real backend, returns AWS-accurate empty analytics), not re-flagged"
  - "SendMessages has zero direct HTTP-level test coverage before this pass; added TestAudit6_SendMessages_ResponseEnvelope this pass but broader message-content assertions (per-channel-type SMS/EMAIL/push payload shape) are still untested"
leaks: {status: clean, note: "no goroutines/timers spawned by this service; purgeAppStateLocked correctly frees all per-app maps (appEvents/eventStreams/otpCodes/appSettings/sentMessages/campaignVersions/segmentVersions/campaignActivities/journeyRuns/endpoints/channels/campaigns/segments/journeys) on DeleteApp — verified by reading the function and its purgeTableByAppID/deletePrefixed helpers; leak_test.go already covers this and passes"}
---

## Notes

Protocol: **restjson1**, `/v1/...` paths, service alias `mobiletargeting` (checked via
`httputils.ExtractServiceFromRequest`). Tags on every taggable resource use a **lowercase**
`"tags"` JSON key (confirmed against `deserializers.go`/`serializers.go` `object.Key("tags")`
call sites) while every other field is PascalCase — this looks like a bug if you're skimming
but is AWS-accurate; don't re-flag it.

### Highest-severity finding this pass: journey execution-metrics route bug

`aws-sdk-go-v2/service/pinpoint`'s real HTTP paths for the four journey execution-metrics ops
use a single hyphenated segment `execution-metrics`:

```
/v1/apps/{ApplicationId}/journeys/{JourneyId}/execution-metrics
/v1/apps/{ApplicationId}/journeys/{JourneyId}/activities/{JourneyActivityId}/execution-metrics
/v1/apps/{ApplicationId}/journeys/{JourneyId}/runs/{RunId}/execution-metrics
/v1/apps/{ApplicationId}/journeys/{JourneyId}/runs/{RunId}/activities/{JourneyActivityId}/execution-metrics
```

gopherstack's dispatch tables (`extractJourneySubOp`, `dispatchJourneyByID`, `dispatchJourneyRun`
in `handler.go`) matched on `execution/metrics` (two path segments, slash-separated) instead.
Since `RouteMatcher()` only checks the `/v1/apps` prefix before handing off to `ServeHTTP`, a
real SDK client calling any of these four ops got routed into the pinpoint handler and then
fell through every `switch` case to a 404 `NotFoundException` — a total, silent unreachability
that no unit test caught because the existing coverage test
(`TestCoverage_JourneyCRUD`) hard-coded the same wrong path shape the buggy dispatcher expected,
so handler and test agreed with each other while both disagreed with the real SDK. This is
exactly the route-matcher bug class flagged in the audit brief (cf. backup/eks/s3control/
guardduty/cleanrooms/bedrockagent). Fixed by changing the `subPathExecutionMetrics` constant
from `"execution/metrics"` to `"execution-metrics"` and updating the corresponding suffix checks;
`TestCoverage_JourneyCRUD` was corrected to assert the real paths so it can no longer mask a
regression.

### Other real bugs fixed this pass

- **RemoveAttributes was a disguised no-op.** The real `RemoveAttributesInput` carries a
  `UpdateAttributesRequest{Blacklist []string}` body naming the specific attribute names/glob
  patterns to remove; `AttributeType` in the URL is a *category* selector
  (`endpoint-custom-attributes` / `endpoint-metric-attributes` / `endpoint-user-attributes`),
  not an attribute name. The handler discarded the request body entirely and the backend tried
  to `delete(e.Attributes, attributeType)` — deleting a key equal to the category string, which
  never matches a real per-endpoint attribute name. Fixed: the handler now parses `Blacklist`,
  and the backend removes matching keys (exact-match or trailing-`*` glob, per AWS docs) from
  the correct map (`Attributes` / `Metrics` / `UserAttributes`) based on `AttributeType`.
- **SendMessages / SendUsersMessages response envelope.** `SendMessagesOutput` wraps its
  payload under a `MessageResponse` key and `SendUsersMessagesOutput` under
  `SendUsersMessageResponse` — confirmed against both types' deserializers. gopherstack returned
  the inner `Result` map bare at the JSON top level, so a real SDK client's
  `output.MessageResponse` (or `.SendUsersMessageResponse`) would always be nil. Fixed by adding
  `sendMessagesResponse`/`sendUsersMessagesResponse` wrapper types and populating the previously
  missing `ApplicationId` field. `SendOTPMessage` already wrapped correctly and was used as the
  reference shape.
- **APNS-family `DefaultAuthenticationMethod` field misnamed.** `updateAPNSChannelRequest` (and
  the corresponding response merge in `parseAPNSChannelExtra`) used the wire key
  `DefaultAuthMethod`; the real field on `APNSChannelRequest`/`APNSChannelResponse` (and the
  sandbox/voip/voip_sandbox variants, which share the same request/response shapes) is
  `DefaultAuthenticationMethod`. A real client setting this field had it silently dropped on
  both the way in and the way out. Fixed the JSON tag and the handler reference; GCM already had
  the correct field name and was used as the reference.
- **VoiceTemplate never entered the ARN index.** Every other template type
  (Email/InApp/Push/Sms) implements `tagHolder` and registers itself in `arnIndex` on create /
  deregisters on delete, so `TagResource`/`UntagResource`/`ListTagsForResource` work by ARN.
  `VoiceTemplate` has a `Tags` field and accepts `tags` at creation time but was missing from
  both the `tagHolder` implementations list and the `arnIndex` writes — every tag operation on a
  voice template ARN returned `NotFoundException`. Fixed: added `getARN`/`getTags`/`setTags`,
  wired `arnIndex` writes into `CreateVoiceTemplate`/`DeleteVoiceTemplate`, and added voice
  templates to `rebuildARNIndexLocked` for consistency (voice templates are not yet part of the
  persisted snapshot set — see gaps).
- **GetCampaignVersions / GetSegmentVersions cross-app leak.** Both looked up the
  campaign/segment purely by ID (`b.campaigns.Get(campaignID)`) without checking
  `ApplicationId` ownership, unlike every sibling op (`GetCampaign`, `UpdateCampaign`,
  `DeleteCampaign`, `GetCampaignVersion`, etc., all of which check `c.ApplicationID != appID`).
  A caller who knew (or guessed) a campaign/segment ID belonging to a *different* app could read
  its version history through the wrong app's URL. Fixed to match the ownership-check pattern
  used everywhere else in the file.

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

### Persistence

`persistence.go`'s `Snapshot`/`Restore` wiring (added in a recent prior commit) is intact and
functioning — `Handler.Snapshot`/`Restore` delegate to `InMemoryBackend`, which round-trips
`apps`, `campaigns`, `emailTemplates`, `exportJobs`, `importJobs`, `inAppTemplates`, `journeys`,
`pushTemplates`, `recommenders`, `segments`, `smsTemplates` through `store.Registry`. See the
**gaps** section above for the (pre-existing, not introduced this pass) set of resource kinds
that are excluded from the persisted snapshot despite being live `store.Table`s or plain maps.
