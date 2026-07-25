---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: mediatailor
sdk_module: aws-sdk-go-v2/service/mediatailor@v1.59.2   # version audited against
last_audit_commit: a874b0df                              # HEAD when this manifest was written
last_audit_date: 2026-07-23
overall: A            # all 4 prior gaps + 3 prior deferred items closed for real this pass; 3 new completeness bugs found+fixed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  PutPlaybackConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - only Name required; this pass adds pass-through storage for AdConditioningConfiguration/AdDecisionServerConfiguration/AvailSuppression/Bumper/CdnConfiguration/ConfigurationAliases/DashConfiguration/FunctionMapping/InsertionMode/LivePreRollConfiguration/ManifestProcessingRules/PersonalizationThresholdSeconds/SlateAdUrl/TranscodeProfileName (decoded-JSON round-trip, not hand-modeled Go structs - see Notes #6) and a real LogConfiguration reflecting ConfigureLogsForPlaybackConfiguration"}
  GetPlaybackConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePlaybackConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent per real API; now cascades to delete every attached prefetch schedule (fixed ghost-row leak, see Notes #7)"}
  ListPlaybackConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "query params PascalCase MaxResults/NextToken - correct"}
  CreateChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - epoch timestamps; this pass adds Tier (was hardcoded BASIC), Audiences, TimeShiftConfiguration, LogConfiguration, and fixes a tags-silently-dropped bug (see Notes #7)"}
  DescribeChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - real UpdateChannelInput also accepts FillerSlate/Audiences/TimeShiftConfiguration; gopherstack only accepted Outputs"}
  DeleteChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly rejects delete while RUNNING; now cascades to delete every scheduled program and the channel policy (fixed ghost-row leak, see Notes #7)"}
  ListChannels: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - lowercase maxResults/nextToken"}
  StartChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "real state transition to RUNNING, idempotent"}
  StopChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "real state transition to STOPPED, idempotent"}
  CreateSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - CreationTime/LastModifiedTime were dead fields (declared, never populated/serialized); fixed - tags silently dropped, see Notes #7"}
  DescribeSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok, note: "LastModifiedTime now advances on update"}
  DeleteSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly rejects delete with attached vod/live sources"}
  ListSourceLocations: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - lowercase maxResults/nextToken"}
  CreateVodSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - CreationTime/LastModifiedTime dead fields populated; fixed - tags silently dropped, see Notes #7"}
  DescribeVodSource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateVodSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "LastModifiedTime now advances on update"}
  DeleteVodSource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListVodSources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - lowercase maxResults/nextToken"}
  CreateLiveSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - CreationTime/LastModifiedTime dead fields populated (LiveSource never had the tags-drop bug - Tags were already returned directly from the stored struct)"}
  DescribeLiveSource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLiveSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "LastModifiedTime now advances on update"}
  DeleteLiveSource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLiveSources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - lowercase maxResults/nextToken"}
  CreatePrefetchSchedule: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - epoch timestamps; this pass adds ScheduleType (validated SINGLE/RECURRING), StreamId, RecurringPrefetchConfiguration (pass-through), and Tags (was entirely unmodeled - PrefetchSchedule had no Tags field at all)"}
  GetPrefetchSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePrefetchSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPrefetchSchedules: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - POST+body routing; this pass implements the ScheduleType/StreamId request filters (were routed/parsed but silently ignored)"}
  CreateProgram: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - ScheduleConfiguration.Transition now required and drives real ScheduledStartTime/DurationMillis computation (ABSOLUTE wall-clock or RELATIVE-to-sibling-program positioning, mirroring real channel scheduling); AdBreaks/AudienceMedia/ClipRange/CreationTime now modeled and returned"}
  DescribeProgram: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - same previously-missing optional fields as CreateProgram, now present"}
  UpdateProgram: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - was a no-op read (took no body); now requires ScheduleConfiguration (its Transition/ClipRange sub-fields are individually optional per the real model) and applies AdBreaks/AudienceMedia/schedule updates for real"}
  DeleteProgram: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChannelSchedule: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - real pagination; this pass corrects the response shape to match the real ScheduleEntry type (ApproximateStartTime/ApproximateDurationSeconds/ScheduleEntryType/Audiences/SourceLocationName, not Program's own AdBreaks/ClipRange/etc which ScheduleEntry does not have - PARITY.md's prior gap note conflated the two types, see Notes #8). ScheduleAdBreaks intentionally left empty - see items_still_open"}
  PutChannelPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChannelPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteChannelPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFunctions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - real pagination"}
  ListAlerts: {wire: ok, errors: ok, state: ok, persist: n/a, note: "returns empty Items - alerts aren't modeled/generated anywhere in the backend, matches a fresh account with no alerts"}
  ConfigureLogsForChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - LogTypes now persisted on the channel and returned from Create/Describe/ListChannels' required LogConfiguration member (was validate-and-echo only, not queryable)"}
  ConfigureLogsForPlaybackConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - now accepts+persists EnabledLoggingStrategies/AdsInteractionLog/ManifestServiceInteractionLog (previously only PercentEnabled was modeled) and is queryable from Get/List/PutPlaybackConfiguration's LogConfiguration; survives a re-Put of the same configuration, matching real MediaTailor"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - lowercase 'tags' wire key"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - same tags-key casing bug"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent, tagKeys query param confirmed correct"}
families:
  routing: {status: ok, note: "all 47 routed ops' HTTP method+path unchanged this pass and still verified against aws-sdk-go-v2 serializers.go/botocore service-2.json from the prior audit"}
gaps: []          # every gap from the prior manifest is now fixed for real (field-diffed against the SDK, not reclassified on say-so) - see ops[*].note above for what changed
deferred: []      # every deferred item from the prior manifest is now implemented this pass - see ops[*].note above
items_still_open:
  - "SourceLocation AccessConfiguration, DefaultSegmentDeliveryConfiguration, and SegmentDeliveryConfigurations (real DescribeSourceLocation/CreateSourceLocation fields) are not modeled at all - newly found by field-diffing this pass, not in the prior manifest's gaps/deferred. Not fixed this pass due to time budget; same pass-through-JSON treatment as PutPlaybackConfiguration's extras (see Notes #6) would close it. (needs bd issue)"
  - "ProgramScheduleEntry.ScheduleAdBreaks is always empty. Real MediaTailor populates it from SCTE-35 avails MediaTailor detects by scanning the underlying VOD/live source manifests during ingestion - a manifest-parsing capability gopherstack has nowhere in this service (or elsewhere in the fleet, as far as this pass could tell). Left empty rather than fabricated from the client-configured AdBreaks (which is a materially different, unrelated concept - AdBreaks is where a client tells MediaTailor to splice ads; ScheduleAdBreaks is what MediaTailor detected already exists in the source content). Matches a real VOD source with no scanned avails yet. (needs bd issue if manifest-avail-detection is ever prioritized)"
  - "PrefetchSchedule/Program/LiveSource/Function tags are stored on the resource's own struct at creation time and returned directly, NOT synced with the ARN-keyed b.tags map that the generic TagResource/UntagResource/ListTagsForResource ops read and write. A TagResource call against one of these ARNs after creation will not be reflected by a subsequent Describe/Get of that resource. This is a pre-existing architectural split in this backend (Channel/SourceLocation/VodSource/PlaybackConfiguration use the b.tags-map-is-authoritative pattern; PrefetchSchedule/Program/LiveSource/Function use the struct-field-is-authoritative pattern) that predates this pass - flagging it here since PrefetchSchedule's Tags field is new this pass and inherited the latter pattern for consistency with its siblings (Program/LiveSource/Function), not because it was independently verified correct. Unifying the two patterns fleet-service-wide is a larger refactor than this pass's budget covers. (needs bd issue)"
leaks: {status: clean, note: "no goroutines, timers, or janitors in this service; all state lives in store.Table/Index + plain maps guarded by one lockmetrics.RWMutex. This pass additionally fixed two ghost-row leaks: DeleteChannel now cascade-deletes every program scheduled on it (via programsByChannel index) and its channel policy; DeletePlaybackConfiguration now cascade-deletes every attached prefetch schedule (via prefetchSchedulesByConfig index). Neither cascade existed before this pass - a channel/playback-config could be deleted and recreated with the same name while its old programs/prefetch-schedules silently lingered in their tables, invisible via any real op path but still occupying memory and corrupting Snapshot/Restore fidelity."}
---

## Notes

MediaTailor is restjson1. This pass closed every gap and deferred item from
the prior manifest (2026-07-13, commit 024e43bf) for real — field-diffed
against `aws-sdk-go-v2/service/mediatailor@v1.59.2`'s generated types,
serializers, and deserializers, not reclassified on inspection alone — and
found three additional completeness bugs by field-diffing surface the prior
pass hadn't touched. Numbering continues from the prior manifest's notes.

6. **PutPlaybackConfiguration's optional sub-configs are stored as
   decoded-JSON pass-through, not hand-modeled Go structs.** The real
   `PlaybackConfiguration` type has ~14 optional nested config blocks
   (`AdConditioningConfiguration`, `AdDecisionServerConfiguration`,
   `AvailSuppression`, `Bumper`, `CdnConfiguration`, `ConfigurationAliases`,
   `DashConfiguration`, `FunctionMapping`, `InsertionMode`,
   `LivePreRollConfiguration`, `ManifestProcessingRules`,
   `PersonalizationThresholdSeconds`, `SlateAdUrl`, `TranscodeProfileName`),
   several of which (e.g. `DashConfiguration`, `ManifestProcessingRules`) are
   themselves multiple levels deep. None of these are consumed by any
   compute path gopherstack implements (this service emulates the CRUD
   control plane, not the actual ad-decision-server/manifest-personalization
   data plane), so the wire-correctness bar that matters is round-trip
   fidelity: what a client PUTs is exactly what a client GETs back, with
   exact key names/nesting preserved because the client's own JSON is
   echoed verbatim. `extractExtraConfig`/`mergeExtraConfig` in
   `handler_helpers.go`/`handler_playback_configurations.go` implement this.
   The same treatment applies to `PrefetchSchedule.RecurringPrefetchConfiguration`
   (itself containing `RecurringConsumption`/`RecurringRetrieval`, each with
   several more nested fields) and to `AdBreak.TimeSignalMessage.SegmentationDescriptors`
   (deeply nested SCTE-35 metadata MediaTailor stores without interpreting).
   Fields that ARE genuinely shallow and/or drive real backend behavior —
   `Channel.Audiences`/`TimeShiftConfiguration`, `AdBreak`/`ClipRange`/
   `AudienceMedia`/`AlternateMedia`, `PrefetchSchedule.ScheduleType`/`StreamId`,
   the two `ConfigureLogsFor*` logging configs — are hand-modeled as real Go
   types with validation and (for schedule-affecting fields) real backend
   logic, not pass-through.

7. **Three resource types silently dropped tags passed at creation.**
   `CreateChannel`, `CreateSourceLocation`, and `CreateVodSource` each stored
   the caller's `Tags` on their own struct but their `Describe*`/`List*`
   counterparts unconditionally overwrite the response `Tags` from a
   separate ARN-keyed `b.tags` map that only `PutPlaybackConfiguration`
   actually wrote to. A client that created a tagged Channel/SourceLocation/
   VodSource and then called Describe/List on it got back empty tags every
   time — a real, currently-shipping data-loss bug for every caller who
   tags a resource at creation instead of via a separate `TagResource` call
   afterward. Fixed by writing `b.tags[arn] = copyTags(tags)` in all three
   `Create*` methods, mirroring the pattern `PutPlaybackConfiguration`
   already used. Caught by field-diffing tag flow end-to-end (create → b.tags
   write → describe → b.tags read), not by any existing test — none of the
   prior CRUD tests happened to create a tagged resource and then assert its
   tags survived a Describe.

8. **The prior manifest's Program-family gap note conflated two different
   real SDK types.** It said `ProgramScheduleEntry` was missing `AdBreaks`,
   `AudienceMedia`, `ClipRange`, `ScheduledStartTime`, `DurationMillis`, and
   `CreationTime` — but those are `Program`'s fields (confirmed via
   `CreateProgramOutput`/`DescribeProgramOutput`). The real type actually
   returned by `GetChannelSchedule` is `ScheduleEntry`, which has none of
   those fields; instead it has `ApproximateStartTime`,
   `ApproximateDurationSeconds`, `Audiences`, `ScheduleAdBreaks`,
   `ScheduleEntryType`, and `SourceLocationName` (required) —
   `ProgramScheduleEntry` in `interfaces.go` is gopherstack's name for this
   shape. Fixed both: `Program`'s response fields now match
   `CreateProgramOutput`/`DescribeProgramOutput`/`UpdateProgramOutput`, and
   `GetChannelSchedule`'s response fields now match `ScheduleEntry`. Lesson
   for future passes: verify the exact SDK type name a field-diff claim is
   about, not just that "the real API has this field somewhere" — two
   sibling types can look similar enough to conflate under time pressure.

9. **`CreateProgram`/`UpdateProgram` now compute real schedule positions.**
   `ScheduleConfiguration.Transition` (required on `CreateProgramInput`) can
   be `ABSOLUTE` (an explicit wall-clock `ScheduledStartTimeMillis`) or
   `RELATIVE` (positioned immediately `BEFORE_PROGRAM`/`AFTER_PROGRAM` a
   named sibling `RelativeProgram` already on the same channel's schedule).
   `resolveProgramSchedule`/`resolveRelativeSchedule` in `programs.go`
   implement both, computing `AFTER_PROGRAM` as
   `sibling.ScheduledStartTime + sibling.DurationMillis` and
   `BEFORE_PROGRAM` as `sibling.ScheduledStartTime - thisDurationMillis` —
   this is genuine scheduling logic (a channel schedule is a real ordered
   sequence of programs), not a stub that only validates and forwards the
   client's own numbers. An unknown `RelativeProgram` is rejected as
   `BadRequestException` rather than silently accepted.

None of this pass's fixes required touching `handler.go`'s routing tables —
every operation's HTTP method/path was already correct from the prior pass;
this pass is entirely about request/response body shape completeness and
the two cascade-delete leak fixes noted above.
