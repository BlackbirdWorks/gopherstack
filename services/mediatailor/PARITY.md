---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: mediatailor
sdk_module: aws-sdk-go-v2/service/mediatailor@v1.59.2   # version audited against
last_audit_commit: 024e43bf                              # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: A            # ~6 genuine wire/routing/validation bugs found and fixed, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  PutPlaybackConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - over-strict validation required AdDecisionServerUrl+VideoContentSourceUrl; real model only requires Name"}
  GetPlaybackConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePlaybackConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent per real API"}
  ListPlaybackConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "query params PascalCase MaxResults/NextToken - correct"}
  CreateChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - CreationTime/LastModifiedTime were RFC3339 strings, must be epoch-seconds numbers"}
  DescribeChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly rejects delete while RUNNING"}
  ListChannels: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - query params are lowercase maxResults/nextToken, handler only checked PascalCase"}
  StartChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "real state transition to RUNNING, idempotent"}
  StopChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "real state transition to STOPPED, idempotent"}
  CreateSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly rejects delete with attached vod/live sources"}
  ListSourceLocations: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - lowercase maxResults/nextToken query params"}
  CreateVodSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeVodSource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateVodSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVodSource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListVodSources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - lowercase maxResults/nextToken query params"}
  CreateLiveSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLiveSource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLiveSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLiveSource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLiveSources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - lowercase maxResults/nextToken query params"}
  CreatePrefetchSchedule: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - Retrieval/Consumption StartTime/EndTime were RFC3339 strings, must be epoch-seconds"}
  GetPrefetchSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePrefetchSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPrefetchSchedules: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - real op is POST with MaxResults/NextToken in JSON body, not GET with query params; handler required GET so a real SDK client's request never routed"}
  CreateProgram: {wire: partial, errors: ok, state: ok, persist: ok, note: "AdBreaks/AudienceMedia/ClipRange/ScheduledStartTime/DurationMillis not modeled - see gaps"}
  DescribeProgram: {wire: partial, errors: ok, state: ok, persist: ok, note: "same missing optional fields as CreateProgram"}
  UpdateProgram: {wire: gap, errors: ok, state: partial, persist: ok, note: "real op requires ScheduleConfiguration/AdBreaks and mutates them; gopherstack's UpdateProgram takes no body and is a no-op read - see gaps"}
  DeleteProgram: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChannelSchedule: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - backend ignored maxResults/nextToken entirely (disguised pagination no-op), and handler only checked PascalCase query params; real op is lowercase"}
  PutChannelPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChannelPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteChannelPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFunctions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - backend ignored maxResults/nextToken entirely (disguised pagination no-op) and handler hardcoded 0/\"\""}
  ListAlerts: {wire: ok, errors: ok, state: ok, persist: n/a, note: "returns empty Items - alerts aren't modeled/generated anywhere in the backend, matches a fresh account with no alerts"}
  ConfigureLogsForChannel: {wire: ok, errors: ok, state: partial, persist: n/a, note: "validates channel exists and echoes LogTypes but doesn't persist them anywhere queryable - low-impact, no real AWS op reads this back"}
  ConfigureLogsForPlaybackConfiguration: {wire: ok, errors: ok, state: partial, persist: n/a, note: "same as ConfigureLogsForChannel"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - Tags wire key must be lowercase 'tags'"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - same tags-key casing bug"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent, tagKeys query param confirmed correct"}
families:
  routing: {status: ok, note: "all 47 routed ops' HTTP method+path verified against aws-sdk-go-v2 serializers.go and botocore service-2.json; only ListPrefetchSchedules was wrong (GET->POST, fixed)"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "UpdateProgram doesn't implement AdBreaks/ScheduleConfiguration mutation (real op requires ScheduleConfiguration and updates ad breaks; gopherstack's Program type has no AdBreaks/ClipRange/AudienceMedia fields at all). Modeling this properly requires adding AdBreak/ScheduleConfiguration/ClipRange/AudienceMedia types to interfaces.go and wiring them through Create/UpdateProgram - a materially larger feature than this pass's budget covered. (needs bd issue)"
  - "CreateProgram/DescribeProgram/UpdateProgram/ProgramScheduleEntry omit optional response fields the real API returns: AdBreaks, AudienceMedia, ClipRange, ScheduledStartTime, DurationMillis, CreationTime. Missing optional fields don't break real SDK clients (zero-value on the Go struct), so this is lower severity than the wire-shape bugs fixed this pass, but is a completeness gap. (needs bd issue)"
  - "SourceLocation/VodSource/LiveSource/PrefetchSchedule Go structs in interfaces.go declare CreationTime/LastModified fields that the backend never populates and the handler never serializes into responses (dead fields). Real DescribeSourceLocation/DescribeVodSource/etc. responses include these. Same completeness-gap severity as the Program fields above. (needs bd issue)"
  - "ConfigureLogsForChannel/ConfigureLogsForPlaybackConfiguration validate + echo their inputs but don't persist log-delivery config anywhere queryable (no real op reads it back, so this is low-impact, but flagged for completeness)."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "CreateChannel/UpdateChannel Audiences and TimeShiftConfiguration fields (real API supports them; not modeled in gopherstack at all)"
  - "PutPlaybackConfiguration's many optional sub-configs (AdConditioningConfiguration, AvailSuppression, Bumper, CdnConfiguration, DashConfiguration, ManifestProcessingRules, etc.) - only Name/AdDecisionServerUrl/VideoContentSourceUrl/Tags are modeled"
  - "ListPrefetchSchedules ScheduleType/StreamId request filters (routing + pagination fixed this pass; filtering by type/stream not implemented)"
leaks: {status: clean, note: "no goroutines, timers, or janitors in this service; all state lives in store.Table/Index + plain maps guarded by one lockmetrics.RWMutex"}
---

## Notes

MediaTailor is restjson1. Two systemic, high-impact wire bugs dominated this
pass and are worth remembering explicitly for the next auditor:

1. **Tags JSON key is lowercase `"tags"`, not `"Tags"`, on every single
   operation** (PutPlaybackConfiguration, Create/Describe/List for Channel,
   SourceLocation, VodSource, LiveSource, Program, Function, plus
   TagResource/ListTagsForResource). Every other field in the entire service
   is PascalCase — `tags` is the one deliberate exception in the real smithy
   model (`locationName: "tags"`), confirmed independently against both
   aws-sdk-go-v2's generated (de)serializers and botocore's
   `service-2.json` (`"required": [...]`/`"locationName"` entries). This is
   NOT a general MediaTailor convention to extrapolate elsewhere in
   gopherstack — it's specific to this service's model.

2. **CreationTime/LastModifiedTime (Channel) and Retrieval/Consumption
   StartTime/EndTime (PrefetchSchedule) are `unixTimestamp` shapes** — JSON
   numbers of seconds since epoch — not RFC3339 strings. A real SDK client's
   deserializer hard-errors ("expected __timestampUnix to be a JSON Number,
   got string instead") on the old RFC3339-string output, so this wasn't a
   silent-data bug like the tags key, it broke the call outright. Use
   `pkgs/awstime.Epoch` on the response side; parse the incoming JSON number
   directly from the `map[string]any` body (no `json.Number` intermediary
   needed since the request body decoder targets `any`).

3. **Query-string param casing is genuinely inconsistent within this one
   service** — not a service-wide convention either way. ListChannels,
   ListSourceLocations, ListVodSources, ListLiveSources, and
   GetChannelSchedule bind MaxResults/NextToken lowercase
   (`maxResults`/`nextToken`); ListPlaybackConfigurations and ListFunctions
   bind them PascalCase (`MaxResults`/`NextToken`). `extractPaginationParams`
   now checks both casings as a fallback rather than duplicating the helper
   per op family. ListPrefetchSchedules is the outlier again: it's the only
   List op that's POST with MaxResults/NextToken/ScheduleType/StreamId
   carried in the JSON body instead of the query string at all (verified via
   aws-sdk-go-v2's `awsRestjson1_serializeOpDocumentListPrefetchSchedulesInput`).

4. **A prior "parity pass 1" fix (`TestParity_PutPlaybackConfiguration_
   RequiredFields`, Gap 1) was itself wrong** — it added a requirement that
   PutPlaybackConfiguration must include AdDecisionServerUrl and
   VideoContentSourceUrl, rejecting requests with a fabricated
   BadRequestException. The real model's only required member is `Name`
   (independently confirmed via aws-sdk-go-v2's `validators.go` and
   botocore's `service-2.json` `"required": ["Name"]`). Lesson for future
   passes: a previous pass's test asserting a behavior is not itself proof
   that behavior is correct — verify against the SDK model directly, even
   for things that look already "fixed."

5. **Disguised pagination no-ops**: `ListFunctions` and `GetChannelSchedule`
   both accepted `maxResults`/`nextToken` parameters in their backend method
   signatures but silently discarded them (`_ int, _ string`), always
   returning every item with an empty NextToken. `ListFunctions`'s handler
   additionally hardcoded `0, ""` instead of reading the query string at
   all. Both now use the same `page.New` pattern as every other List op in
   this backend.

None of the fixes in this pass required schema/state additions beyond what
`interfaces.go` already declared — every wire-format fix corrected how an
already-tracked value is read from the request or written to the response.
The one exception is `ListPrefetchSchedules`'s POST/body routing, which is a
request-shape correction, not a new capability.
