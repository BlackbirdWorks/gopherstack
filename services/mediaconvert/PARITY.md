---
service: mediaconvert
sdk_module: aws-sdk-go-v2/service/mediaconvert@v1.87.3
last_audit_commit: 911ff167
last_audit_date: 2026-07-13
overall: A            # genuine wire-breaking bugs found and fixed this pass
ops:
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "was reading arn from URL path (always empty since real client sends POST /tags with arn in JSON body); fixed to read arn from body"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "was routed on DELETE with tagKeys from query string; real op is PUT with tagKeys in JSON body -- real SDK calls 404'd before this fix"}
  CreateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "input field was jobEngineVersionRequested; real wire key is jobEngineVersion (response field IS jobEngineVersionRequested -- request/response names differ)"}
  CreateQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "input field was reservationPlan; real wire key is reservationPlanSettings (response field IS reservationPlan)"}
  UpdateQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "reservationPlanSettings field name fixed; concurrentJobs and reservationPlanSettings were entirely unsupported on update (silently dropped), now applied"}
  StartJobsQuery: {wire: ok, errors: ok, state: ok, persist: ok, note: "output field was queryId; real wire key is id"}
  GetJobsQueryResults: {wire: ok, errors: ok, state: ok, persist: ok, note: "added missing status field (JobsQueryStatus); always COMPLETE since this backend resolves queries synchronously"}
  GetJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "extra non-AWS totalCount field in response; additive, harmless to real clients"}
  CancelJob: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateJobTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetJobTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListJobTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateJobTemplate: {wire: partial, errors: ok, state: ok, persist: ok, note: "accelerationSettings/hopDestinations/statusUpdateInterval accepted by real API but not modeled here -- see gaps"}
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
  DescribeEndpoints: {wire: partial, errors: ok, state: ok, persist: n/a, note: "real op is POST with maxResults/nextToken/mode JSON body -- gopherstack ignores body and answers any method; functionally harmless (single synthetic endpoint, no real pagination need) but not strictly modeled"}
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
  jobTemplate: {status: ok, note: "verified op-by-op; AccelerationSettings/HopDestinations/StatusUpdateInterval gap noted below"}
  job: {status: ok, note: "CreateJob/GetJob/ListJobs/CancelJob verified; jobEngineVersion wire-name bug fixed; UpdateJob is a gopherstack-only extension, see notes"}
  preset: {status: ok, note: "verified op-by-op, full field parity"}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource: two critical wire bugs fixed (see gaps->fixed above); this is the class of bug parity-principles.md warns about (ARN routing) but the actual defect here was ARN-in-body vs ARN-in-URL and DELETE-vs-PUT method, not slash-escaping"}
  jobsQuery: {status: ok, note: "StartJobsQuery/GetJobsQueryResults: id/status wire-name bugs fixed"}
  endpoints/policy/certificates/misc: {status: ok, note: "DescribeEndpoints/GetPolicy/PutPolicy/DeletePolicy/AssociateCertificate/DisassociateCertificate/ListVersions/Probe/SearchJobs/CreateResourceShare verified op-by-op"}
gaps:
  - UpdateJobTemplate/CreateJobTemplate do not model AccelerationSettings, HopDestinations, or StatusUpdateInterval (real API accepts them; gopherstack silently drops them since JobTemplate has no such fields) (bd: TODO -- file at session close)
  - Queue.ServiceOverrides is typed map[string]any in gopherstack vs a real []types.ServiceOverride list on the wire; currently dormant (CreateQueueInput has no serviceOverrides input member in the real API, so the field can never be populated by a real client) but the type would emit the wrong JSON shape (object instead of array) if ever populated internally
  - DescribeEndpoints does not parse its POST JSON body (maxResults/nextToken/mode) and accepts GET as well as POST; functionally harmless today (always returns exactly one synthetic endpoint) but not strictly wire-accurate
deferred:
  - JobSettings/JobTemplateSettings/PresetSettings deep-structure field-level validation (gopherstack stores these as opaque map[string]any and round-trips them verbatim, which is the established pattern for this service; no validation of e.g. OutputGroups internals was audited)
leaks: {status: clean, note: "janitor.go uses pkgs/worker.Group.Ticker bound to ctx cancellation; no goroutine/map leaks found. lockmetrics.RWMutex used as the single coarse backend lock; safemap not used (not applicable, all backend collections are cross-map transactional and correctly share the coarse lock)"}
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
  `Client.UpdateJob` exist). gopherstack still routes `PUT /2017-08-29/jobs/{id}` to
  an `UpdateJob` handler. This is harmless (no real SDK client can ever construct such
  a call, since the SDK exposes no method for it) but is a gopherstack-only extension,
  not AWS parity surface. Left in place (not a bug -- unreachable by real clients,
  and removing it would be gratuitous churn outside this audit's bug-fixing scope).
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
