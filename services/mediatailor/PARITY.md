---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: mediatailor
sdk_module: aws-sdk-go-v2/service/mediatailor@v1.63.4   # version audited against
last_audit_commit: a874b0df                              # HEAD when this manifest was written
last_audit_date: 2026-07-23
overall: A            # all 4 prior gaps + 3 prior deferred items closed for real this pass; 3 new completeness bugs found+fixed
# gopherstack-vdrs (2026-08-10, targeted follow-up, not a full re-audit): closed all 3 filed
# items -- SourceLocation's 3 unmodeled fields now hand-modeled (Notes #10), the
# PrefetchSchedule/Program/LiveSource/Function tags split reproduced+fixed (Notes #11),
# ScheduleAdBreaks reconfirmed structural. Also fixed 3 bugs found sweeping for the same
# classes today's other passes kept finding: CreateProgram accepted a nonexistent
# SourceLocation/VodSource/LiveSource and reported success; PutFunction accepted any
# FunctionType string; every mediatailor error response was undecodable by a real SDK
# client (see families.errors).
# gopherstack-gt9o (2026-08-11, targeted follow-up, not a full re-audit): closed the
# AdsPersonalizationConcurrency/AdsPersonalizationTimeouts drop by generalizing
# extractExtraConfig; modeled (unset) the DualStackPlaybackEndpointPrefix/
# DualStackSessionInitializationEndpointPrefix response fields. See Notes #13.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  PutPlaybackConfiguration: {wire: partial, errors: ok, state: ok, persist: ok, note: "fixed prior pass - only Name required; adds pass-through storage for AdConditioningConfiguration/AdDecisionServerConfiguration/AvailSuppression/Bumper/CdnConfiguration/ConfigurationAliases/DashConfiguration/FunctionMapping/InsertionMode/LivePreRollConfiguration/ManifestProcessingRules/PersonalizationThresholdSeconds/SlateAdUrl/TranscodeProfileName/AdsPersonalizationConcurrency/AdsPersonalizationTimeouts (decoded-JSON round-trip, not hand-modeled Go structs - see Notes #6) and a real LogConfiguration reflecting ConfigureLogsForPlaybackConfiguration. gopherstack-gt9o: extractExtraConfig (handler_helpers.go) was rewritten from a fixed 14-key enumeration to an exclude-known-handled-keys pass-through, so AdsPersonalizationConcurrency/AdsPersonalizationTimeouts now round-trip and any future SDK-added sub-config will too without another code change -- see Notes #13. Still wire:partial, not wire:ok: PlaybackConfiguration's response-only DualStackPlaybackEndpointPrefix/DualStackSessionInitializationEndpointPrefix (no PutPlaybackConfigurationInput member sets them) are modeled on the Go struct but deliberately left unset -- gopherstack has no real dual-stack endpoint to report, and fabricating one would be a dialable-but-fake URL, worse than an absent field."}
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
  CreateSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - CreationTime/LastModifiedTime were dead fields (declared, never populated/serialized); fixed - tags silently dropped, see Notes #7; gopherstack-vdrs: AccessConfiguration/DefaultSegmentDeliveryConfiguration/SegmentDeliveryConfigurations now hand-modeled (see Notes #10), was entirely unmodeled"}
  DescribeSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSourceLocation: {wire: ok, errors: ok, state: ok, persist: ok, note: "LastModifiedTime now advances on update; gopherstack-vdrs: now accepts/persists AccessConfiguration/DefaultSegmentDeliveryConfiguration/SegmentDeliveryConfigurations same as Create"}
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
  CreateProgram: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - ScheduleConfiguration.Transition now required and drives real ScheduledStartTime/DurationMillis computation (ABSOLUTE wall-clock or RELATIVE-to-sibling-program positioning, mirroring real channel scheduling); AdBreaks/AudienceMedia/ClipRange/CreationTime now modeled and returned; gopherstack-vdrs: now validates SourceLocationName is required and exists, and VodSourceName/LiveSourceName (if given) exist under it -- previously accepted any name and reported success, see Notes #11"}
  DescribeProgram: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - same previously-missing optional fields as CreateProgram, now present"}
  UpdateProgram: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - was a no-op read (took no body); now requires ScheduleConfiguration (its Transition/ClipRange sub-fields are individually optional per the real model) and applies AdBreaks/AudienceMedia/schedule updates for real"}
  DeleteProgram: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChannelSchedule: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass - real pagination; this pass corrects the response shape to match the real ScheduleEntry type (ApproximateStartTime/ApproximateDurationSeconds/ScheduleEntryType/Audiences/SourceLocationName, not Program's own AdBreaks/ClipRange/etc which ScheduleEntry does not have - PARITY.md's prior gap note conflated the two types, see Notes #8). ScheduleAdBreaks intentionally left empty - see items_still_open"}
  PutChannelPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChannelPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteChannelPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-vdrs: FunctionType now validated against the real HTTP_REQUEST/CUSTOM_OUTPUT/SEQUENTIAL_EXECUTOR enum -- previously any non-empty string was accepted; also now writes b.tags on create, see Notes #11"}
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
  errors: {status: ok, note: "gopherstack-vdrs: FIXED a service-wide wire bug -- respondErr never set the X-Amzn-Errortype header or any body code/__type field, so aws-sdk-go-v2's restjson.GetErrorInfo (aws/protocol/restjson/decoder_util.go, checked at v1.43.4, the pinned aws-sdk-go-v2 core) had no code to read and every mediatailor error deserialized client-side as smithy.GenericAPIError{Code:\"UnknownError\"} regardless of the real failure (404/409/400 all included). Fixed by setting X-Amzn-Errortype from the sentinel error's mapped exception name, matching the sibling convention already used by services/account and services/apigatewayv2. See Notes #11."}
gaps:
  - "FIXED by gopherstack-gt9o: PlaybackConfiguration's AdsPersonalizationConcurrency/AdsPersonalizationTimeouts input sub-configs now round-trip through extractExtraConfig, generalized from a fixed 14-key enumeration to exclude-known-handled-keys pass-through (handler_helpers.go). See Notes #13."
  - "FIXED by gopherstack-ic73: PlaybackConfiguration's three response-only dual-stack fields (DualStackPlaybackEndpointPrefix, DualStackSessionInitializationEndpointPrefix, and HlsConfiguration's own DualStackManifestEndpointPrefix -- aws-sdk-go-v2/service/mediatailor@v1.63.4 types/types.go:688) are now modeled on the Go PlaybackConfiguration struct and wired into toPlaybackConfigOutput, but deliberately left unset -- no PutPlaybackConfigurationInput member sets any of them, and gopherstack has no real dual-stack endpoint to report; fabricating one would be a dialable-but-fake URL, worse than an absent field. The rest of gopherstack-ic73's premise did not hold: there is no GetHlsManifestConfiguration operation in the pinned SDK (v1.63.4 has no api_op_GetHlsManifestConfiguration.go and no such op in service-2.json's op list) -- that name does not exist to model. DualStackPlaybackUrl (types.go:1388) is real but belongs to a different, unrelated type -- ResponseOutputItem, part of Channel.Outputs (CreateChannel/DescribeChannel/UpdateChannel) -- out of scope for PlaybackConfiguration/HlsConfiguration entirely. There is also no separate 'SessionInitializationEndpoint' type in the pinned SDK; DualStackSessionInitializationEndpointPrefix appears exactly once, on PlaybackConfiguration itself, already covered above. Both claims were carried over from a prior pass's note and could not be verified against the pinned aws-sdk-go-v2 source."
deferred: []      # every deferred item from the prior manifest is now implemented this pass - see ops[*].note above
items_still_open:
  - "ProgramScheduleEntry.ScheduleAdBreaks is always empty. Real MediaTailor populates it from SCTE-35 avails MediaTailor detects by scanning the underlying VOD/live source manifests during ingestion - a manifest-parsing capability gopherstack has nowhere in this service (or elsewhere in the fleet, as far as this pass could tell). Left empty rather than fabricated from the client-configured AdBreaks (which is a materially different, unrelated concept - AdBreaks is where a client tells MediaTailor to splice ads; ScheduleAdBreaks is what MediaTailor detected already exists in the source content). Matches a real VOD source with no scanned avails yet. Reconfirmed this pass (gopherstack-vdrs item 2): genuinely structural, not attempted. (needs bd issue if manifest-avail-detection is ever prioritized)"
leaks: {status: clean, note: "no goroutines, timers, or janitors in this service; all state lives in store.Table/Index + plain maps guarded by one lockmetrics.RWMutex. This pass additionally fixed two ghost-row leaks: DeleteChannel now cascade-deletes every program scheduled on it (via programsByChannel index) and its channel policy; DeletePlaybackConfiguration now cascade-deletes every attached prefetch schedule (via prefetchSchedulesByConfig index). Neither cascade existed before this pass - a channel/playback-config could be deleted and recreated with the same name while its old programs/prefetch-schedules silently lingered in their tables, invisible via any real op path but still occupying memory and corrupting Snapshot/Restore fidelity."}
---

## Notes

**2026-08-13 (gopherstack-jqh2 pass 3):** re-extracted all 48 ops' real
method+path directly from `mediatailor@v1.63.4` serializers.go and drove
them through `ExtractOperation` via the new
`handler_sdk_route_table_test.go` (`TestExtractOperation_SDKRouteTable`, one
subtest per op, `t.Parallel()`). Confirmed the ListPrefetchSchedules
POST-not-GET quirk (already handled with a doc comment) held. One
test-construction wrinkle, not a service bug: the three tag ops'
`ExtractOperation` requires the `/tags/{arn}` ARN to contain `:mediatailor:`
to disambiguate from FIS's identically-shaped path — a bare PLACEHOLDER
resolved to `Unknown`, fixed by using a realistic
`arn:aws:mediatailor:...:channel/...` ARN in the table instead (a real SDK
client's ARN always satisfies this). No pre-existing table existed to
check, and no real routing bugs found. This test is now the permanent
regression guard for route-table drift.

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

10. **`SourceLocation.AccessConfiguration`/`DefaultSegmentDeliveryConfiguration`/
    `SegmentDeliveryConfigurations` are shallow and now hand-modeled, not
    pass-through.** Sized before typing (mediatailor@v1.63.4 `types/types.go`):
    `AccessConfiguration` has 2 members — `AccessType` (a 3-value enum:
    `S3_SIGV4`/`SECRETS_MANAGER_ACCESS_TOKEN`/`AUTODETECT_SIGV4`, `types/enums.go:9-11`)
    and `SecretsManagerAccessTokenConfiguration` (level 2, 3 flat string
    members, no further nesting). `DefaultSegmentDeliveryConfiguration` is 1
    flat string member; `SegmentDeliveryConfiguration` is 2. Total depth 3
    levels, max union count 3 — well under the deep/pass-through bar Notes #6
    sets. All three appear on both the `Create`/`UpdateSourceLocationInput`
    and the `SourceLocation` response type reused by `Describe`/`List`
    (confirmed via `api_op_CreateSourceLocation.go`/`api_op_UpdateSourceLocation.go`),
    so both directions needed wiring, not just echo. None of the three
    members are required in botocore's `service-2.json` (2018-04-23), so
    typing exposed one real gap worth validating: `AccessType` had no enum
    check at all before this pass — a client sending a made-up value got
    a 200. Added a validation switch in `validateAccessConfiguration`
    (`source_locations.go`) with a round-trip test through the real SDK
    client (`handler_source_location_configs_test.go`).

11. **The Prefetch/Program/LiveSource/Function tags split was a real,
    provable divergence, not just an architectural note — now closed.**
    Traced each resource type's actual Create/Get/Describe/List/Delete code
    path instead of restating the note: `CreatePrefetchSchedule`,
    `CreateProgram`, and `CreateLiveSource` all wrote the caller's tags to
    *both* the resource's own struct field *and* `b.tags[arn]` at creation
    (so the two started in sync), but `GetPrefetchSchedule`,
    `DescribeProgram`, and `DescribeLiveSource` returned the struct's own
    `Tags` field directly, never reading `b.tags`. Since `TagResource`/
    `UntagResource` only ever write `b.tags`, any tag change made *after*
    creation via those generic ops was invisible to the resource's own
    Describe/Get — while `ListTagsForResource` (which reads `b.tags`) saw it
    immediately. Reproduced with a real-SDK-client test
    (`TestTagResource_VisibleOnDescribe`, `handler_tags_divergence_test.go`):
    create untagged, `TagResource` a tag onto the ARN, assert
    `ListTagsForResource` sees it (passed) and `Get`/`DescribeXxx` sees it
    (failed pre-fix — asserted empty vs `"video"`). `PutFunction` diverged
    the *other* direction and more severely: it never wrote `b.tags` at
    creation at all (unlike the other three), so `ListTagsForResource`
    against a function tagged at creation returned empty immediately, no
    `TagResource` call needed (`TestPutFunction_TagsVisibleOnListTagsForResource`,
    same file). `b.tags` is the pattern Channel/SourceLocation/VodSource/
    PlaybackConfiguration already use correctly (see Notes #7), so it's the
    real source of truth: fixed by making `Get`/`Describe`/`List` for all
    four types return a copy with `Tags` overlaid from `b.tags[arn]` (never
    mutating the table's own pointer, since `store.Table.Get` returns the
    live pointer, not a copy — confirmed in `pkgs/store/table.go`), making
    `PutFunction` write `b.tags` like its siblings, and cascading
    `delete(b.tags, arn)` on all four `Delete*` paths (matching
    `DeleteSourceLocation`'s existing pattern) so a deleted-and-recreated
    resource can't inherit a stale tag set.

12. **Two more bugs matched the highest-yield classes from today's other
    service passes.** `CreateProgram`'s required `SourceLocationName` (and
    the `VodSourceName`/`LiveSourceName` it names) was never checked against
    `b.sourceLocations`/`b.vodSources`/`b.liveSources` — every sibling
    `Create*` op in this service (`CreateVodSource`, `CreateLiveSource`)
    already validates its `SourceLocationName` FK, so this was an
    inconsistency with the codebase's own established pattern, not a new
    design decision. A `CreateProgram` naming a nonexistent source location
    or VOD/live source reported success instead of `NotFoundException`;
    fixed with existence checks placed in the same position (after the
    required-field check, before the table write) as the other `Create*`
    ops, reproduced/verified via `TestCreateProgram_RejectsUnknownReferences`.
    Separately, `PutFunction` checked `FunctionType` for non-empty but never
    against the real 3-value enum (`HTTP_REQUEST`/`CUSTOM_OUTPUT`/
    `SEQUENTIAL_EXECUTOR`, `types/enums.go`) — several existing tests
    happened to pass fabricated values (`"AWS_LAMBDA"`, `"CHANNEL_ASSEMBLY"`,
    `"AD_DECISION_SERVER_URL"`) that a real client could never send
    successfully; fixed the validation and corrected those fixtures to real
    enum values rather than trust the tests as documentation of intended
    behavior. Verifying the `CreateProgram` fix through the real SDK client
    surfaced a third, larger bug: `respondErr` never set an error code
    anywhere in the response (no `X-Amzn-Errortype` header, no body
    `code`/`__type` field), so aws-sdk-go-v2's `restjson.GetErrorInfo` had
    nothing to read and *every* error from *every* mediatailor operation
    deserialized client-side as a generic `UnknownError`, not just the ones
    touched this pass — fixed service-wide in `handler.go`'s `respondErr`
    by setting `X-Amzn-Errortype`, matching the convention already
    established in `services/account` and `services/apigatewayv2`.

13. **`extractExtraConfig`'s fixed key list generalized to exclude-known-handled,
    closing the `AdsPersonalizationConcurrency`/`AdsPersonalizationTimeouts`
    drop and future-proofing against the next SDK bump.** Confirmed against
    `aws-sdk-go-v2/service/mediatailor@v1.63.4`: `PutPlaybackConfigurationInput`
    gained `AdsPersonalizationConcurrency *types.AdsPersonalizationConcurrency`
    (`EnableVodVastParallelization *bool`, `MaxConcurrentAdsRequests *int32`,
    wire keys identical to the Go field names, `api_op_PutPlaybackConfiguration.go:58`,
    `serializers.go:4694`) and `AdsPersonalizationTimeouts *types.AdsPersonalizationTimeouts`
    (5 `*int32` millisecond fields, same file:63, `serializers.go:4711`) since the
    prior manifest's v1.59.2 pin — both silently dropped because
    `extractExtraConfig`'s pass-through was a fixed 14-key enumeration, not a
    generic "everything I don't handle by name" pass-through. Rather than adding
    2 keys to a list that will need the same fix on the next SDK bump,
    `extractExtraConfig` now inverts the check: it copies every request body key
    *except* the 4 (`Name`, `AdDecisionServerUrl`, `VideoContentSourceUrl`,
    `tags`) that `handlePutPlaybackConfiguration`/`extractTags` already parse
    individually (`extraConfigHandledKeys`, `handler_helpers.go`). This is a
    small, mechanical change (invert a loop) that closes the whole class, not
    just these two fields — a future SDK addition to `PlaybackConfiguration`'s
    optional sub-configs survives without touching this file again. Considered
    and rejected a more general fix at the framework level (e.g. an
    unknown-field-preserving decode shared across services): not attempted
    because this service's `PutPlaybackConfiguration` is unusual in *never*
    interpreting these sub-configs (real MediaTailor validates them during
    ad-decision-server calls this emulator doesn't perform, see Notes #6) — a
    generic cross-service mechanism would need to reconcile with services that
    *do* need typed validation of unknown-today fields, which is a materially
    larger design question than this issue's two-service scope.

    Also modeled (shape only, deliberately unset) `PlaybackConfiguration`'s two
    response-only dual-stack fields: `DualStackPlaybackEndpointPrefix
    *string` and `DualStackSessionInitializationEndpointPrefix *string`
    (`types/types.go:1049,1053`). Neither has a `PutPlaybackConfigurationInput`
    member — real MediaTailor generates them server-side the same way it
    generates `PlaybackEndpointPrefix`/`SessionInitializationEndpointPrefix` —
    but unlike those two (which this service already fabricates plausible
    URLs for), a fabricated dual-stack URL is worse than an absent field: a
    client might actually dial it. Added the two fields to the
    `PlaybackConfiguration` Go struct and to `toPlaybackConfigOutput`
    (`handler_playback_configurations.go`, same `if != ""` pattern as
    `HlsManifestEndpointPrefix`) so the wire key mapping exists, but nothing
    in `PutPlaybackConfiguration` ever sets them — the response correctly
    omits them, matching a real account with dual-stack endpoints not
    provisioned. `TestPutPlaybackConfiguration_DualStackFieldsAbsent`
    (`handler_playback_configurations_test.go`) asserts on the raw decoded
    body that the keys are absent, not null/empty.
