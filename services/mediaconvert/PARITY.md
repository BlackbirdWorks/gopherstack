---
service: mediaconvert
sdk_module: aws-sdk-go-v2/service/mediaconvert@v1.97.1
last_audit_commit: 911ff167
last_audit_date: 2026-07-31
overall: A            # 2026-07-24: genuine wire-breaking bugs found and fixed this pass
                      # 2026-07-31: pkgs/sdkcheck reverse check re-flagged UpdateJob, which the 2026-07-24 pass had already correctly identified as not-a-real-op (see Notes) but left ADVERTISED in GetSupportedOperations()/ChaosOperations() -- i.e. the finding was documented but not actually corrected. Now removed from the advertised list; route stays wired as internal test scaffolding, unreachable by real clients either way. See its Notes entry and handler.go's opUpdateJob comment.
ops:
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "was reading arn from URL path (always empty since real client sends POST /tags with arn in JSON body); fixed to read arn from body"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "was routed on DELETE with tagKeys from query string; real op is PUT with tagKeys in JSON body -- real SDK calls 404'd before this fix"}
  CreateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "input field was jobEngineVersionRequested; real wire key is jobEngineVersion (response field IS jobEngineVersionRequested -- request/response names differ). This pass: statusUpdateInterval/simulateReservedQueue were parsed from the request body but silently overridden with hardcoded defaults (SECONDS_60/DISABLED) instead of the caller's value -- fixed via CreateJobFull's new JobCreateExtras parameter"}
  CreateQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "input field was reservationPlan; real wire key is reservationPlanSettings (response field IS reservationPlan). gopherstack-gt9o: maximumConcurrentFeeds (*int32, added since v1.87.3) now stored and echoed via QueueCreateExtras -- previously silently dropped, see Notes"}
  UpdateQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "reservationPlanSettings field name fixed; concurrentJobs and reservationPlanSettings were entirely unsupported on update (silently dropped), now applied. gopherstack-gt9o: maximumConcurrentFeeds now applied too -- previously silently dropped, see Notes"}
  StartJobsQuery: {wire: ok, errors: ok, state: ok, persist: ok, note: "output field was queryId; real wire key is id"}
  GetJobsQueryResults: {wire: ok, errors: ok, state: ok, persist: ok, note: "added missing status field (JobsQueryStatus); always COMPLETE since this backend resolves queries synchronously"}
  GetJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-21 (gopherstack-us9u kind-mismatch sweep) -- Job.LastShareDetails was emitted as a {shareToken, sharedAt} object; the real types.Job.LastShareDetails is a bare *string (types.go), so every real SDK client's GetJob call failed outright once a job had ever been shared via CreateResourceShare. Fixed via a MarshalJSON/UnmarshalJSON pair on Job projecting LastShareDetails to its ShareToken as a plain string at the wire boundary (SharedAt has no documented place in the real string form and is dropped rather than invented into it), keeping the richer ShareDetails struct for internal/domain use. Proven via a real aws-sdk-go-v2/service/mediaconvert client round trip (wire_last_share_details_test.go), hand-reverted/confirmed-failing (expected __string to be of type string, got map[string]interface {} instead)/restored, md5sum-verified byte-identical."}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "extra non-AWS totalCount field in response; additive, harmless to real clients"}
  CancelJob: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateJobTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: added accelerationSettings/hopDestinations/statusUpdateInterval, which the real CreateJobTemplateInput wire shape accepts but JobTemplate previously had no fields for (silently dropped) -- see CreateJobTemplateFull"}
  GetJobTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListJobTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateJobTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: added accelerationSettings/hopDestinations/statusUpdateInterval support via UpdateJobTemplateFull -- previously silently dropped despite the real UpdateJobTemplateInput accepting them (was the last remaining gap for this family)"}
  DeleteJobTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePreset: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPreset: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPresets: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePreset: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePreset: {wire: ok, errors: ok, state: ok, persist: ok}
  GetQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  ListQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEndpoints: {wire: ok, errors: ok, state: ok, persist: n/a, note: "this pass: real op is POST-only with maxResults/nextToken/mode in a JSON body -- gopherstack previously answered any HTTP method and ignored the body. Fixed: route now requires POST (GET/other methods 404 as unknown operation, matching real-client behavior against a real endpoint), and the body is parsed (mode/maxResults honored; nextToken accepted but there is never a next page since exactly one synthetic endpoint ever exists)"}
  GetPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  Probe: {wire: ok, errors: ok, state: ok, persist: n/a}
  SearchJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "extra non-AWS totalCount not present -- SearchJobsOutput matches wire shape exactly"}
  CreateResourceShare: {wire: partial, errors: ok, state: ok, persist: ok, note: "real input also requires supportCaseId; not validated/stored (harmless, output is void)"}
families:
  queue: {status: ok, note: "CreateQueue/GetQueue/ListQueues/UpdateQueue/DeleteQueue verified op-by-op against restjson1 serializers; reservationPlanSettings wire-name bug fixed on both create and update"}
  jobTemplate: {status: ok, note: "verified op-by-op; this pass closed the AccelerationSettings/HopDestinations/StatusUpdateInterval gap on both Create and Update (CreateJobTemplateFull/UpdateJobTemplateFull) -- family is now full field parity, no open gaps"}
  job: {status: ok, note: "CreateJob/GetJob/ListJobs/CancelJob verified; jobEngineVersion wire-name bug fixed; this pass also fixed CreateJob silently overriding statusUpdateInterval/simulateReservedQueue with hardcoded defaults instead of applying the caller's request values; UpdateJob is a gopherstack-only extension, unadvertised as of 2026-07-31 (see notes)"}
  preset: {status: ok, note: "verified op-by-op, full field parity"}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource: two critical wire bugs fixed (see gaps->fixed above); this is the class of bug parity-principles.md warns about (ARN routing) but the actual defect here was ARN-in-body vs ARN-in-URL and DELETE-vs-PUT method, not slash-escaping"}
  jobsQuery: {status: ok, note: "StartJobsQuery/GetJobsQueryResults: id/status wire-name bugs fixed"}
  endpoints/policy/certificates/misc: {status: ok, note: "DescribeEndpoints/GetPolicy/PutPolicy/DeletePolicy/AssociateCertificate/DisassociateCertificate/ListVersions/Probe/SearchJobs/CreateResourceShare verified op-by-op; this pass closed the DescribeEndpoints method/body gap (now POST-only, body parsed)"}
gaps:
  - Queue.ServiceOverrides is typed map[string]any in gopherstack vs a real []types.ServiceOverride list on the wire; currently dormant (CreateQueueInput has no serviceOverrides input member in the real API, so the field can never be populated by a real client) but the type would emit the wrong JSON shape (object instead of array) if ever populated internally. Re-verified this pass against aws-sdk-go-v2/service/mediaconvert@v1.97.1 (pin corrected from the stale v1.87.3 recorded here by gopherstack-u8my): still no serviceOverrides member on CreateQueueInput or UpdateQueueInput, so this remains genuinely unreachable/harmless -- left as-is rather than reshaping a field no real client can ever populate.
  - "FIXED by gopherstack-gt9o: CreateQueueInput/UpdateQueueInput's MaximumConcurrentFeeds *int32 member (Elemental Inference feed concurrency, added since v1.87.3) now read, stored, and echoed. See Notes."
deferred:
  - JobSettings/JobTemplateSettings/PresetSettings deep-structure field-level validation (gopherstack stores these as opaque map[string]any and round-trips them verbatim, which is the established pattern for this service; no validation of e.g. OutputGroups internals was audited)
leaks: {status: clean, note: "janitor.go uses pkgs/worker.Group.Ticker bound to ctx cancellation; no goroutine/map leaks found. lockmetrics.RWMutex used as the single coarse backend lock; safemap not used (not applicable, all backend collections are cross-map transactional and correctly share the coarse lock). Re-verified this pass: no new goroutines/tickers/maps introduced by the CreateJob/CreateJobTemplate/UpdateJobTemplate/DescribeEndpoints fixes; all new code paths run synchronously under the existing b.mu lock or (DescribeEndpoints) hold no lock at all since it reads no mutable backend state."}
---

## Notes

- Protocol: restjson1, paths under `/2017-08-29/...`. Errors are returned as JSON
  `{"__type": "<Code>Exception", "message": "..."}` with an HTTP status matching the
  code (400/404/409/500); the real `restjson.GetErrorInfo` reads either `code` or
  `__type` from the body, so this shape round-trips correctly with the real client.
- Timestamps are already epoch-seconds floats (`epochSeconds` helper) everywhere,
  matching `pkgs/awstime.Epoch`-style behavior expected by the JSON protocol -- no
  epoch/ISO8601 bug found in this service (unlike other services previously audited).
- **Request/response field-name asymmetry is a recurring MediaConvert wire trap**:
  several fields have *different* names on the input vs. the output resource:
  - `CreateJobInput.jobEngineVersion` (request) vs. `Job.jobEngineVersionRequested`
    (response).
  - `CreateQueueInput.reservationPlanSettings` / `UpdateQueueInput.reservationPlanSettings`
    (request) vs. `Queue.reservationPlan` (response).
  Before this pass, gopherstack's input structs mistakenly used the *response* field
  name for the *request* JSON tag, so a real `aws-sdk-go-v2` client's request body
  never matched the field the handler unmarshaled into -- these fields were silently
  dropped on every real CreateJob/CreateQueue/UpdateQueue call. Fixed by giving the
  input structs the correct request-side JSON tags (`handler.go`: `createJobInput`,
  `createQueueInput`, `updateQueueInput`).
- **TagResource/UntagResource were the most severe bugs found**: the real
  `TagResource` operation is `POST /2017-08-29/tags` with the target ARN in the JSON
  body (`{"arn": ..., "tags": {...}}`), not in the URL. gopherstack was pulling the
  ARN from the URL suffix (`route.resource`), which is always empty for the real
  request shape -- every real-SDK `TagResource` call was silently tagging the
  empty-string resource key instead of the intended one. Separately, the real
  `UntagResource` operation is `PUT /2017-08-29/tags/{Arn}` with `tagKeys` in a JSON
  body; gopherstack routed it on `DELETE` with `tagKeys` as a repeated query
  parameter, so real SDK `UntagResource` calls never matched any route and always
  404'd (`Unknown operation`). Both fixed in `handler.go` (`parseTagRoute`,
  `handleTagResource`, `handleUntagResource`) -- see `TestMediaConvert_ExtractOperation`
  route-matcher-level cases and `TestMediaConvert_Tags` for the corrected wire shape.
- **StartJobsQuery/GetJobsQueryResults wire-name bug**: `StartJobsQueryOutput`'s sole
  member is `id`, not `queryId` -- gopherstack returned `{"queryId": ...}`, which a
  real SDK client's deserializer (looking for `id`) would silently leave nil, breaking
  the entire `StartJobsQuery` → `GetJobsQueryResults` polling workflow for real
  clients. Fixed the JSON tag; also added the missing `status` field on
  `GetJobsQueryResultsOutput` (always `COMPLETE` since this backend resolves queries
  synchronously inside `GetJobsQueryResults`, not asynchronously like real AWS).
- **`UpdateJob` is not a real MediaConvert operation** (confirmed by grepping the
  `aws-sdk-go-v2/service/mediaconvert` SDK: no `UpdateJobInput`/`UpdateJobOutput`/
  `Client.UpdateJob` exist, and botocore's mediaconvert service-2.json has no PUT
  route under `/jobs/{id}`). gopherstack still routes `PUT /2017-08-29/jobs/{id}` to
  an `UpdateJob` handler. This is harmless (no real SDK client can ever construct such
  a call, since the SDK exposes no method for it) and is a gopherstack-only extension,
  not AWS parity surface. **2026-07-31 correction:** this note correctly identified the
  problem back in 2026-07-24 but the "left in place" resolution was incomplete --
  `GetSupportedOperations()`/`ChaosOperations()` still *advertised* `UpdateJob` as
  supported SDK surface, which pkgs/sdkcheck's reverse check (commit 12cfe14d5;
  gopherstack-vhw2 category A) correctly re-flagged. The route itself stays wired as
  internal test scaffolding (still unreachable by real clients, still not gratuitous
  churn to delete), but it is no longer advertised — see handler.go's opUpdateJob
  comment. Same resolution as EMR's ListTagsForResource and CloudFront's
  GetFunctionAssociations/SetFunctionAssociations.
- `ListJobsOutput`/`SearchJobsOutput`: gopherstack's `ListJobs` response includes an
  extra `totalCount` field not present in the real API shape. This is additive-only
  (unknown JSON fields are ignored by `aws-sdk-go-v2`'s deserializer) so it does not
  break real clients; left as-is. `SearchJobsOutput` has no such extra field and
  matches the real shape exactly.
- Locking: single `lockmetrics.RWMutex` (`b.mu`) guards all backend maps/tables,
  consistent with the pkgs-catalog.md rule (coarse lock at the cross-map-transaction
  boundary). `safemap` is correctly not used here since every resource type
  (queues/jobTemplates/jobs/presets/tags/queueCounters/tokenIndex) participates in
  cross-map invariants (e.g. CreateJob updates jobs + queueCounters + tokenIndex
  atomically).
- Job state machine is real (not a disguised no-op): `janitor.go`'s ticker calls
  `AdvanceJobPhase`, which walks SUBMITTED → PROGRESSING(PROBING → TRANSCODING →
  UPLOADING) → COMPLETE, updating per-queue counters and populating
  `OutputGroupDetails` at completion. `CancelJob` only allows cancellation from
  SUBMITTED/PROGRESSING, matching real AWS semantics. Persistence
  (`Snapshot`/`Restore`) is wired through `Handler.Snapshot`/`Restore` delegating to
  `InMemoryBackend`, versioned (`mediaconvertSnapshotVersion`), confirmed non-dead
  (see the doc comment on `Handler.Snapshot` explaining why this delegation matters).

## 2026-07-24 pass -- closed all remaining gaps/deferred-whole-family items

- **`JobTemplate` gained `AccelerationSettings`/`HopDestinations`/`StatusUpdateInterval`**
  (`models.go`). The real `CreateJobTemplateInput`/`UpdateJobTemplateInput` wire
  shapes both accept these three fields (confirmed against
  `aws-sdk-go-v2/service/mediaconvert@v1.97.1`'s `api_op_CreateJobTemplate.go` /
  `api_op_UpdateJobTemplate.go`), but `JobTemplate` previously had no fields to
  hold them, so a real SDK client setting e.g. `AccelerationSettings` on
  `CreateJobTemplateInput` had it silently dropped -- the response would never
  reflect it. Fixed by adding the fields to `JobTemplate`, threading them through
  new `CreateJobTemplateFull`/`UpdateJobTemplateFull` backend methods (the
  existing `CreateJobTemplate`/`UpdateJobTemplate` signatures are preserved as
  thin wrappers so no caller outside this fix needed to change), and parsing them
  in `handler_job_templates.go`'s `createJobTemplateInput`/`updateJobTemplateInput`.
  `StatusUpdateInterval` defaults to `SECONDS_60` when unset, matching the
  behavior `Job` already had. `cloneJobTemplate` deep-copies the new pointer/slice
  fields so returned copies can't alias backend state (mirrors `cloneJob`'s
  existing pattern for the identical `Job` fields).
- **`CreateJob` was silently overriding `statusUpdateInterval`/`simulateReservedQueue`
  with hardcoded defaults** (`SECONDS_60`/`DISABLED`) instead of applying the
  caller's request values -- both are real, accepted `CreateJobInput` members
  (confirmed against `api_op_CreateJob.go`) that `handler_jobs.go`'s
  `createJobInput` never even parsed from the request body. Fixed by adding both
  fields to `createJobInput` and threading them into `CreateJobFull` via a new
  variadic `JobCreateExtras` trailing parameter (`jobs.go`) -- variadic so the
  ~20 pre-existing `CreateJobFull(...)` call sites across the test suite keep
  compiling unchanged (Go allows omitting a trailing variadic argument entirely).
  `CreateJobFull`'s body was split into `buildNewJobLocked` to stay under the
  `funlen` budget after the added logic.
- **`DescribeEndpoints` now POST-only with its JSON body parsed.** The real
  operation's serializer (`serializers.go`'s
  `awsRestjson1_serializeOpDescribeEndpoints`) hardcodes `request.Method = "POST"`
  and sends `{maxResults, mode, nextToken}` in the body; gopherstack previously
  matched the `/2017-08-29/endpoints` path on *any* HTTP method and never read the
  body. Fixed: the route now requires POST (other methods fall through to
  `opUnknown` → 404, matching what a real client would see hitting a real
  MediaConvert endpoint with the wrong method), and `handleDescribeEndpoints` now
  parses and honors `maxResults` (caps the returned list) and accepts `mode`/
  `nextToken` for wire accuracy. Behavior is otherwise unchanged: gopherstack
  always has exactly one synthetic endpoint (the host the request arrived on), so
  `mode=DEFAULT` vs `mode=GET_ONLY` can't observably differ here, and there is
  never a next page.
- **Re-verified, left unchanged as genuinely non-actionable**: `Queue.ServiceOverrides`
  (dormant -- no real input member exists to ever populate it) and
  `CreateResourceShare`'s missing `supportCaseId` validation (output is void, so
  this is unobservable to a real client either way). Both re-checked against the
  v1.87.3 SDK at that time; re-confirmed still accurate against the now-current
  v1.97.1 pin by the gopherstack-u8my pass (`Queue.ServiceOverrides` -- see `gaps`
  above, which also now notes a new, separate `MaximumConcurrentFeeds` gap found
  by that same pass).
- No leaks introduced: all new code (`buildNewJobLocked`, `CreateJobTemplateFull`,
  `UpdateJobTemplateFull`, `handleDescribeEndpoints`'s body parsing) runs
  synchronously with no new goroutines, tickers, or maps -- `CreateJobTemplateFull`/
  `UpdateJobTemplateFull` execute under the existing coarse `b.mu` lock exactly
  like their pre-existing counterparts, and `handleDescribeEndpoints` touches no
  backend state at all.

## 2026-08-11 pass (gopherstack-gt9o) -- MaximumConcurrentFeeds no longer dropped

- **`CreateQueueInput`/`UpdateQueueInput.MaximumConcurrentFeeds` now wired
  end to end.** Confirmed against `aws-sdk-go-v2/service/mediaconvert@v1.97.1`:
  `MaximumConcurrentFeeds *int32` on both inputs (`api_op_CreateQueue.go:47-49`,
  wire key `maximumConcurrentFeeds`, `serializers.go:635` doc-serializer,
  same key on `UpdateQueueInput`'s at `serializers.go:2940`), and on the
  `Queue` response resource (`deserializers.go:24653`'s shared
  `awsRestjson1_deserializeDocumentQueue`, field set at line 96 of that
  function's body). Threaded through as `*int` (not a plain `int`, unlike the
  pre-existing `ConcurrentJobs`) specifically so a caller-supplied `0` stays
  distinguishable from "not supplied" — `models.go`'s `Queue.MaximumConcurrentFeeds`,
  `queues.go`'s new `QueueCreateExtras{MaximumConcurrentFeeds *int}` (a
  variadic trailing parameter on `CreateQueueFull`, same pattern
  `JobCreateExtras` already established for `CreateJobFull` above, so the
  ~6 pre-existing `CreateQueueFull` call sites keep compiling unchanged), and
  a new 6th parameter on `UpdateQueue` (that method has exactly one caller,
  `handler_queues.go`, so no variadic trick was needed there). `cloneQueue`
  deep-copies the pointer (`cloneIntPtr`) so returned `Queue` values can't
  alias backend state, matching the existing `ReservationPlan`/`ServiceOverrides`
  clone pattern. No `mediaconvertSnapshotVersion` bump: `Queue` already
  round-trips through the generic `store.Table` JSON snapshot, and the new
  field is `*int` with `json:"maximumConcurrentFeeds,omitempty"` — additive
  and omitted when nil, so old snapshots restore unchanged.
  `TestMediaConvert_CreateQueue_MaximumConcurrentFeeds`/
  `TestMediaConvert_UpdateQueue_MaximumConcurrentFeeds` (`queues_test.go`)
  and `TestPersistence_NewFieldsRoundTrip` (`persistence_test.go`) cover it.
- Not attempted as a general mechanism fix, unlike the parallel mediatailor
  fix in the same issue: `createQueueInput`/`updateQueueInput` are
  hand-modeled Go structs (typed fields, not a generic pass-through map), so
  there is no equivalent of mediatailor's "exclude known-handled keys"
  inversion available here — every field this service accepts has to be
  declared on the struct one way or another. The real fix for "SDK bump adds
  a field, gopherstack silently drops it" in a hand-modeled service is the
  `pkgs/sdkcheck`-style diff sweep that found this gap in the first place
  (gopherstack-u8my), not a code-level mechanism change.

## gopherstack-y1zn (2026-08-21): unknown-key sweep, 1 confirmed bug

`Probe`: {wire: fixed} -- each result was double-wrapped as
`{"probeResult": {"container": ..., "inputFile": ...}}`; ProbeOutput.ProbeResults
(api_op_Probe.go) is `[]types.ProbeResult` directly -- each item IS the
Container/Metadata/TrackMappings object, not wrapped under a "probeResult"
key, and there is no "inputFile" echo member at all. Proven via
`TestProbe_ResultContainsContainer` (probe_test.go, strengthened in place),
hand-reverted/confirmed-failing/restored/`md5sum`-verified byte-identical.

## 2026-08-21 (gopherstack-hjdd): snapshot-version guard, unbumped retype

`mediaconvertSnapshotVersion` bumped 1 -> 2. `d83f4b5d3` gave `Job` (the registered
`jobs` table's value type) a custom `MarshalJSON`/`UnmarshalJSON` pair that renders
`LastShareDetails` as the real deserializer's bare string instead of the previous
`{shareToken, sharedAt}` object, without bumping the snapshot version. A pre-fix (v1)
snapshot's object no longer unmarshals into the new string field at all -- `RestoreAll`
now errors outright rather than silently losing data, but the whole backend then fails
to restore, which the version guard exists to convert into a clean, recoverable
"discard and start empty" instead.

Found via `pkgs/persistence`'s snapshot-version guard, extended this session
(gopherstack-hjdd) to recursively expand fields of every type reached through a
`store.Register`/`store.New` table registration.

**Proof:** `TestInMemoryBackend_RestoreV1JobLastShareDetailsDiscarded` (persistence_test.go)
builds a v1-shaped `jobs` snapshot with an object-shaped `lastShareDetails` and asserts
`Restore` succeeds (discarding cleanly) rather than erroring. Hand-reverted to version 1:
the same test then fails with `Restore` returning `json: cannot unmarshal object into Go
struct field .lastShareDetails of type string`, confirming the symptom; restored and
`md5sum`-verified byte-identical.

**Gates:** `go build`, `go vet` (default/e2e/integration), `gofmt -l` (clean), `go test -race`
(pass), `golangci-lint run` (0 issues).
