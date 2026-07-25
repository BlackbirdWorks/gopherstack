---
service: mediapackage
sdk_module: aws-sdk-go-v2/service/mediapackage@v1.39.25
last_audit_commit: f942b4d6b9d0353bd693cc733196bc7228ededd9
last_audit_date: 2026-07-24
overall: A            # genuine fixes found: invented ops deleted, missing required-field validation added
ops:
  CreateChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "added missing createdAt"}
  DescribeChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades origin endpoints, matches AWS"}
  ListChannels: {wire: ok, errors: ok, state: ok, persist: ok}
  ConfigureLogs: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised no-op -- fixed, see Notes"}
  RotateChannelCredentials: {wire: ok, errors: ok, state: ok, persist: ok, note: "route path AND rotation semantics were wrong -- fixed, see Notes"}
  RotateIngestEndpointCredentials: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateOriginEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "packaging blocks were discarded -- fixed, see Notes"}
  DescribeOriginEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateOriginEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "packaging blocks were discarded -- fixed, see Notes"}
  DeleteOriginEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOriginEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateHarvestJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "always created SUCCEEDED, no IN_PROGRESS phase -- see gaps; now validates all 5 SDK-required members (Id/OriginEndpointId/StartTime/EndTime/S3Destination.{BucketName,ManifestKey,RoleArn})"}
  DescribeHarvestJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHarvestJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Channel: {status: ok, note: "CreatedAt, EgressAccessLogs/IngressAccessLogs added; RotateChannelCredentials route+semantics fixed"}
  OriginEndpoint: {status: ok, note: "CreatedAt added; Authorization/HlsPackage/DashPackage/CmafPackage/MssPackage now round-trip (opaque passthrough, see Notes)"}
  HarvestJob: {status: ok, note: "no changes this pass; simplistic SUCCEEDED-on-create status noted as a gap"}
  Tags: {status: ok, note: "no changes this pass; prior sweep already wired tag<->resource sync"}
gaps:
  - "CreateHarvestJob always sets Status=SUCCEEDED synchronously; real MediaPackage harvest jobs start IN_PROGRESS and transition asynchronously. Low priority: most test suites just assert job existence/S3Destination, not the IN_PROGRESS state transition. Deliberately not implemented as a goroutine-driven async transition this pass -- that would trade a documented wire-shape simplification for a new leak-surface (timer/goroutine lifecycle tied to Reset/Snapshot) for a low-value behavior. (bd: TODO -- file issue if async harvest-job semantics become relevant)"
deferred:
  - "Packaging protocol nested config (HlsPackage/DashPackage/CmafPackage/MssPackage/Authorization) is stored and echoed back as an opaque map[string]any -- NOT semantically validated or interpreted (no SPEKE/encryption-contract logic, no ad-marker logic). This closes the 'discards config' bug (values now round-trip through Create/Update/Describe/List) but a client asserting on server-side validation of e.g. required Authorization sub-fields would not get real AWS's validation errors. Next pass: consider modeling the concrete sub-shapes if a consumer needs semantic validation."
leaks: {status: clean, note: "no goroutines/timers introduced; all ops are synchronous map operations under the existing lockmetrics.RWMutex"}
---

## Notes

**Protocol**: REST-JSON (restjson1), matching the real SDK's `awsRestjson1_*`
serializers/deserializers in `aws-sdk-go-v2/service/mediapackage@v1.39.25`.
Very few timestamps (createdAt/harvest-job start/end) and they are all
ISO8601 *string* wire values, not epoch numbers -- confirmed against the
SDK's `deserializeOpDocument*` functions, which decode `createdAt` etc. as
plain JSON strings via `ptr.String(jtv)`.

### Bugs found and fixed this pass

1. **RotateChannelCredentials was unreachable at its real path (route-matcher
   bug class).** `classifyChannelPath` matched `POST
   /channels/{id}/ingest_endpoints/credentials` for this op, but the real SDK
   sends `PUT /channels/{id}/credentials`
   (`awsRestjson1_serializeOpHttpBindingsRotateChannelCredentialsInput`, SDK
   `serializers.go:1179`). The wrong path was even baked into a unit test
   (`handler_audit1_test.go`), which is exactly why unit tests alone aren't
   parity proof -- they exercised the classifier via `h.Handler()` directly
   with a hand-picked path that a real client never sends. A genuine
   `RotateChannelCredentials` call would have 404'd as "unknown operation."
   Fixed the route (`handler.go`) and updated the two tests that referenced
   the fictional path.

2. **RotateChannelCredentials also had wrong rotation semantics.** The SDK
   doc comment says "Changes the Channel's first IngestEndpoint's username
   and password" (deprecated in favor of
   RotateIngestEndpointCredentials-by-ID), but the backend regenerated
   *both* ingest endpoints from scratch with brand-new IDs and URLs. Real AWS
   only rotates `ingestEndpoints[0]`'s credentials and leaves ID/URL (and the
   second endpoint) untouched. Fixed in `backend.go`.

3. **ConfigureLogs was a disguised no-op.** It accepted
   `egressAccessLogs`/`ingressAccessLogs` (each carrying `logGroupName`),
   discarded them with `_ = egressLogGroup; _ = ingressLogGroup`, and always
   returned the channel completely unchanged -- a textbook stub matching
   parity-principles rule #1 (never ship an op that reads/mutates nothing).
   Real MediaPackage's Channel/CreateChannelOutput/ConfigureLogsOutput always
   carry `egressAccessLogs`/`ingressAccessLogs` (each `{logGroupName:
   string}`), which `channelOutput` didn't even have fields for. Fixed:
   `storedChannel` now has `EgressLogGroupName`/`IngressLogGroupName
   *string`; `ConfigureLogs(id, egressLogGroup, ingressLogGroup *string)`
   only overwrites a side when the caller's request included that key (nil
   pointer means "leave existing config alone", matching AWS's
   independently-optional members); `channelOutput` gained
   `egressAccessLogs`/`ingressAccessLogs` (omitted when unset, present when
   configured).

4. **CreateOriginEndpoint/UpdateOriginEndpoint discarded the packaging
   protocol config entirely.** Real MediaPackage's OriginEndpoint carries
   `authorization`, `hlsPackage`, `dashPackage`, `cmafPackage`, and
   `mssPackage` (confirmed field names against
   `awsRestjson1_serializeOpDocumentCreateOriginEndpointInput` /
   `UpdateOriginEndpointInput` in the SDK). gopherstack's handler never even
   parsed these keys out of the request body, so any Terraform/CDK
   OriginEndpoint configured with e.g. `hls_manifest` blocks would silently
   lose that configuration on create -- a "create that discards config"
   no-op per parity-principles. Fixed: added a `PackagingConfig` struct
   (`Authorization`/`CmafPackage`/`DashPackage`/`HlsPackage`/`MssPackage`,
   each `map[string]any`) threaded through `CreateOriginEndpoint`/
   `UpdateOriginEndpoint`; each block is stored and echoed back verbatim on
   Describe/List (see "deferred" above for the scope of this fix -- it's an
   opaque passthrough, not semantic modeling of encryption/ad-marker
   config).

5. **CreatedAt was missing from both Channel and OriginEndpoint entirely.**
   Real AWS always returns this field (confirmed in both types' Describe
   deserializers). Added `CreatedAt string` to both, set at creation time.

### Bugs looked for but NOT found (already correct)

- ARN shapes (`arn:aws:mediapackage:<region>:<account>:channels/<id>` and
  `.../origin_endpoints/<id>`) match AWS's pattern.
- Error status codes: NotFoundException->404, UnprocessableEntityException->422
  (used for both "already exists" and "invalid parameter", matching the real
  SDK's error type set -- there is no ConflictException in this API).
  `__type` is included on error bodies for SDK exception classification.
- DeleteChannel/DeleteOriginEndpoint correctly return 202 Accepted with an
  empty body.
- List* pagination (`nextToken`, `maxResults`) uses `pkgs/page` uniformly.
- Tag<->resource sync (TagResource/UntagResource updating the resource's own
  `Tags` field, not just the separate ARN-keyed tag store) was already
  correct from a prior sweep.
- RotateIngestEndpointCredentials (the newer, ID-scoped op) already had the
  correct `PUT /channels/{id}/ingest_endpoints/{ingestId}/credentials` route
  and semantics.

### Invented ops deleted this pass

`CreatePackagingConfiguration`/`DescribePackagingConfiguration`/
`DeletePackagingConfiguration`/`ListPackagingConfigurations` and
`PutChannelLifecyclePolicy`/`GetChannelLifecyclePolicy` were registered and
routed in this service but **do not correspond to any operation in the real
`aws-sdk-go-v2/service/mediapackage` client** (v1.39.25) -- confirmed by
listing `api_op_*.go` in the downloaded module source: there are exactly 19
files, matching the 19 ops in the `ops:` table above, with no
`api_op_CreatePackagingConfiguration.go` / `api_op_PutChannelLifecyclePolicy.go`
etc., and no `PackagingConfiguration`/`PackagingGroup` type in `types/types.go`
either. PackagingConfiguration/PackagingGroup belong to MediaPackage VOD, a
separate AWS service with its own SigV4 signing name and REST surface (not a
dependency of this repo's go.mod). No real `aws-sdk-go-v2/service/mediapackage`
client will ever call these paths -- a prior audit pass flagged this but left
it in place; this pass deletes it outright, per the "delete gopherstack-invented
surface not in the real SDK" rule. This wasn't caught by `TestSDKCompleteness`
because that check only flags SDK ops *missing* from `GetSupportedOperations()`,
not extra ops beyond the SDK's surface.

Removed: the `/packaging_configurations` route family and `lifecycle_policy`
channel sub-route (`handler.go`); `handler_packaging_configurations.go` and
its test file; `packaging_configurations.go` (backend CRUD); the
`PackagingConfiguration`/`storedPackagingConfiguration` types and
`CreatePackagingConfiguration`/`Describe`/`Delete`/`List` +
`PutChannelLifecyclePolicy`/`GetChannelLifecyclePolicy` methods from
`StorageBackend` and `InMemoryBackend`; the `packagingConfigurations`
`store.Table` and its ARN builder; `storedChannel.LifecyclePolicy`; the
`PackagingConfigCount` test helper; and every test referencing any of the
above (`handler_packaging_configurations_test.go`, the `packagingConfigurations`
snapshot-restore subtest, the four `TestLifecyclePolicy*`/`TestChannelLifecyclePolicy*`
tests in `handler_channels_test.go`, and the packaging-config case in
`TestNotFound_ErrorType`). Not touched: `PackagingConfig` (no trailing "uration") --
that is a real, distinct type holding the Authorization/CmafPackage/DashPackage/
HlsPackage/MssPackage opaque blocks on the legitimate `OriginEndpoint` resource,
confirmed against `types.OriginEndpoint` in the real SDK; it was not renamed or
removed.

### CreateHarvestJob required-field validation added

The real SDK's `CreateHarvestJobInput` marks `EndTime`, `Id`, `OriginEndpointId`,
`S3Destination`, and `StartTime` all `// This member is required.`, and
`types.S3Destination` itself requires `BucketName`/`ManifestKey`/`RoleArn`
(confirmed in `api_op_CreateHarvestJob.go` and `types/types.go`). A previous
pass only validated `Id`/`OriginEndpointId` and flagged the rest as a gap;
this pass closes it: `CreateHarvestJob` (`harvest_jobs.go`) now 422s
(`ErrInvalidParameter`) when `StartTime`, `EndTime`, or any of the three
`S3Destination` fields is empty, matching what a real client would have
already validated client-side before the request ever reached the server.
