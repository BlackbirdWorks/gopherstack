---
service: mediapackage
sdk_module: aws-sdk-go-v2/service/mediapackage@v1.42.4
last_audit_commit: 711100b0006aeb09a8422f1e6c09a400068f27ee
last_audit_date: 2026-08-20
overall: A            # wrapper-key/nested-shape sweep: zero bugs found, prior audit's claims re-verified against SDK source
ops:
  CreateChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "added missing createdAt"}
  DescribeChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades origin endpoints, matches AWS"}
  ListChannels: {wire: ok, errors: ok, state: ok, persist: ok}
  ConfigureLogs: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised no-op -- fixed, see Notes"}
  RotateChannelCredentials: {wire: ok, errors: ok, state: ok, persist: ok, note: "route path AND rotation semantics were wrong -- fixed, see Notes"}
  RotateIngestEndpointCredentials: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateOriginEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "Authorization/MssPackage now typed+validated; Hls/Dash/Cmaf remain opaque passthrough -- see Notes"}
  DescribeOriginEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateOriginEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "Authorization/MssPackage now typed+validated; Hls/Dash/Cmaf remain opaque passthrough -- see Notes"}
  DeleteOriginEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOriginEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateHarvestJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "now starts IN_PROGRESS (was synchronously SUCCEEDED) and never transitions further -- see Notes; validates all 5 SDK-required members (Id/OriginEndpointId/StartTime/EndTime/S3Destination.{BucketName,ManifestKey,RoleArn})"}
  DescribeHarvestJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHarvestJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Channel: {status: ok, note: "CreatedAt, EgressAccessLogs/IngressAccessLogs added; RotateChannelCredentials route+semantics fixed"}
  OriginEndpoint: {status: ok, note: "CreatedAt added; Authorization/MssPackage fully typed+SPEKE-validated; Hls/Dash/Cmaf remain opaque passthrough (see Notes)"}
  HarvestJob: {status: ok, note: "CreateHarvestJob now starts IN_PROGRESS instead of synchronously SUCCEEDED -- see Notes"}
  Tags: {status: ok, note: "no changes this pass; prior sweep already wired tag<->resource sync"}
deferred:
  - "HlsPackage/DashPackage/CmafPackage remain an opaque map[string]any passthrough -- NOT semantically validated (no ad-marker/encryption logic for these three). Sized this pass: HlsPackage has 12 leaf fields across 3 enum types plus a nested HlsEncryption/SpekeKeyProvider chain; DashPackage has 14 leaf fields across 5 enum types plus DashEncryption/SpekeKeyProvider; CmafPackage additionally nests a *list* of HlsManifest sub-resources (~10 fields each) -- a materially larger, separate unit of work from Authorization/MssPackage (2 and ~11 leaf fields, single level of nesting, no repeated sub-resources), which this pass modeled to full depth instead of shaving fields to fit. Next pass: model Hls/Dash/Cmaf to the same full depth if a consumer needs it."
leaks: {status: clean, note: "no goroutines/timers introduced; all ops are synchronous map operations under the existing lockmetrics.RWMutex"}
---

## Notes

### 2026-08-20: wrapper-key / nested-shape wire-parity sweep (zero bugs found)

Full re-verification of every op's wire shape against
`aws-sdk-go-v2/service/mediapackage@v1.42.4` source (not against
gopherstack's own handler output), per the campaign's five bug-class hunt
(generalized/missing members, wrong nesting level, wrong JSON type,
case-sensitive key near-misses, right-key-wrong-value/invented enums, and
request-only fields leaking into responses).

**Provenance correction**: the prior stamp cited `last_audit_commit
f942b4d6b9d0353bd693cc733196bc7228ededd9` (`git show -s --format=%ad`:
2026-07-24) against `last_audit_date: 2026-08-10` -- a 17-day gap with the
sha predating the date, despite 12 intervening commits on `main` in that
window (including two full-repo parity sweeps, #2402/#2404/#2406). The sha
was stale/copied forward rather than reflecting HEAD at the actual 2026-08-10
write time. Content-wise the 2026-08-10 entry's claims all independently
re-verified true this pass (see below) -- this is a stamp-hygiene fix, not a
retraction of that audit's findings. Refreshed to current HEAD
(`711100b0006aeb09a8422f1e6c09a400068f27ee`) + today.

**Full-field-list diff, every op, optional members included**: `Channel`
(`types.go:29-56`), `OriginEndpoint` (`592-654`), `HarvestJob` (`262-297`),
`HlsIngest`/`IngestEndpoint` (`324-331,536-551`), `EgressAccessLogs`/
`IngressAccessLogs` (`228-235,553-560`), `Authorization` (`10-26`),
`MssPackage`/`MssEncryption`/`SpekeKeyProvider`/
`EncryptionContractConfiguration`/`StreamSelection` (`563-590,681-736`), and
`S3Destination` (`656-677`) each diffed member-by-member against
`interfaces.go`/`models.go`/the `*Output` structs in `handler_channels.go`,
`handler_origin_endpoints.go`, `handler_harvest_jobs.go`. No member missing
in either direction; no extra/fabricated member. `CmafPackage`/`DashPackage`/
`HlsPackage`/`HlsManifest`/`HlsManifestCreateOrUpdateParameters` remain
outside this diff -- see `deferred` (unchanged from the prior pass).

**Every field's JSON key name** re-confirmed against both
`awsRestjson1_serializeDocument*` (`serializers.go`) and
`awsRestjson1_deserializeDocument*` (`deserializers.go`) for `Channel`,
`OriginEndpoint`, `Authorization`, `MssPackage` chain, `HarvestJob`,
`S3Destination`, and the `List*Output` wrapper keys (`channels`,
`originEndpoints`, `harvestJobs`, `nextToken`, `tags`) -- all match
gopherstack's `json:` tags exactly, case included. No case-sensitive
near-miss found.

**Enum check, both directions**: `originationAllow = "ALLOW"` and
`harvestJobStatusInProgress = "IN_PROGRESS"` are the only two enum-typed
string constants gopherstack emits for this service (`store.go`) -- both are
real `types.Origination`/`types.Status` values (`enums.go:158-175,302-321`).
No gopherstack-invented enum constant exists anywhere in the package.
Direction 2 (every SDK enum value representable): moot for `Origination`/
`Status` since both are passed through as bare strings without a fixed
allowlist; all fifteen other enums (`AdMarkers`, `AdsOnDeliveryRestrictions`,
`CmafEncryptionMethod`, `EncryptionMethod`, `ManifestLayout`,
`PeriodTriggersElement`, `PlaylistType`, `PresetSpeke20Audio`,
`PresetSpeke20Video`, `Profile`, `SegmentTemplateFormat`, `StreamOrder`,
`UtcTiming`, `AdTriggersElement`) live entirely inside the opaque
`CmafPackage`/`DashPackage`/`HlsPackage` passthrough blobs or as unvalidated
bare strings (`StreamSelection.StreamOrder`) -- gopherstack never declares Go
constants for them, so it structurally cannot invent a value the SDK
doesn't have.

**The four package types / four encryption types**: `HlsPackage`,
`DashPackage`, `CmafPackage` remain opaque `map[string]any` (deferred, sized
and justified in the 2026-08-10 entry below -- unchanged this pass); only
`MssPackage`/`MssEncryption` are modeled, and were re-verified field-by-field
against `serializers.go:2156-2222`/`deserializers.go:5547-5650` this pass
with a new real-SDK round-trip test (see below). No cross-contamination
found between the modeled `MssPackage` and the opaque blocks -- they are
distinct fields on `OriginEndpoint`/`PackagingConfig`, never merged.

**`HlsManifest` vs `HlsManifestCreateOrUpdateParameters`**: no leak possible
-- gopherstack has no structured `HlsManifest` type at all (confirmed by
`grep -rn HlsManifest services/mediapackage/*.go`: the only hit is a doc
comment). Both real-SDK variants live entirely inside the opaque `CmafPackage`
blob, which is stored and echoed verbatim per-request; a consequence
(pre-existing, not new this pass) is that gopherstack's echoed `CmafPackage`
never gains the server-computed `HlsManifest.Url` field the real API adds on
top of what `HlsManifestCreateOrUpdateParameters` sends -- in scope of the
same `deferred` opaque-passthrough limitation already on file, not a new gap.

**New test**: `wire_sdk_roundtrip_test.go` --
`TestCreateOriginEndpoint_AuthorizationMssPackage_SDKRoundTrip` drives
`CreateOriginEndpoint`/`DescribeOriginEndpoint` through the real
`aws-sdk-go-v2` client for every leaf field of `Authorization` and the full
`MssPackage`->`MssEncryption`->`SpekeKeyProvider`->
`EncryptionContractConfiguration`/`StreamSelection` chain. Verified the test
actually catches a real bug (not a tautology): hand-reverted
`SpekeKeyProvider.ResourceID`'s json tag from `resourceId` to `resourceld`
in `interfaces.go` via `cp` (never git), reran the test -- failed exactly as
predicted (`ResourceId` deserialized empty, since the real SDK's
`awsRestjson1_deserializeDocumentSpekeKeyProvider` only recognizes
`resourceId`), then restored `interfaces.go` from the `cp` copy and confirmed
byte-identical via `md5sum` before and after.

**No bugs found this pass.** Gates: `go build`, `go vet`, `go fix -diff`
(empty), `gofmt -l` (empty), `go test -race` (pass), `golangci-lint run`
(0 issues) all clean on `services/mediapackage/...`.

---

### 2026-08-10 audit (prior pass, content re-verified above)

**Protocol**: REST-JSON (restjson1), matching the real SDK's `awsRestjson1_*`
serializers/deserializers in `aws-sdk-go-v2/service/mediapackage@v1.42.4`.
Very few timestamps (createdAt/harvest-job start/end) and they are all
ISO8601 *string* wire values, not epoch numbers -- confirmed against the
SDK's `deserializeOpDocument*` functions, which decode `createdAt` etc. as
plain JSON strings via `ptr.String(jtv)`.

### SDK pin correction

The audit frontmatter above previously cited `v1.39.25` while `go.mod` pins
`v1.42.4` -- a stale-pin mismatch found this pass (the same class flagged
across the campaign today). All wire claims in this file are now verified
against `v1.42.4`; the `Status` enum, `HlsManifest`/`SpekeKeyProvider`/
`EncryptionContractConfiguration` shapes, and field counts cited below are
all from that version's `types/types.go` and `types/enums.go`. No API surface
changed between the two versions for this service (still 19 ops, same field
sets on every type touched this pass).

### CreateHarvestJob: IN_PROGRESS instead of synchronous SUCCEEDED

`CreateHarvestJob` set `Status=SUCCEEDED` synchronously on every create. Real
MediaPackage harvest jobs start `IN_PROGRESS` and transition to
`SUCCEEDED`/`FAILED` asynchronously as content is actually copied to the
target S3 bucket (`types.HarvestJob.Status` doc comment: "Consider setting up
a CloudWatch Event to listen for HarvestJobs as they succeed or fail" --
`types/types.go:291-294`). This backend never performs that S3 copy, so
claiming `SUCCEEDED` on create asserted work had completed that never
happened -- a stronger, false claim than a job that simply never reaches a
terminal state.

This differs from the emrserverless/elasticsearch/kinesisanalytics async-op
simplifications audited today, which start in a legitimate non-terminal state
and never transition further -- self-consistent because no other field in
their wire shape claims otherwise. This service had inverted that pattern:
it jumped straight to a terminal state instead of stopping short of one.
Fixed to match the same *shape* of simplification those three use: `Status`
now starts `IN_PROGRESS` (`harvestJobStatusInProgress`, `harvest_jobs.go`)
and this backend never transitions it further -- no goroutine or timer
introduced, so no new leak surface, and every other field (`CreatedAt`,
`S3Destination`, etc.) remains internally consistent with "submitted, not yet
complete."

The enum itself was also incomplete: `types.Status` defines `IN_PROGRESS`,
`SUCCEEDED`, and `FAILED` (`types/enums.go:302-317`), but gopherstack only
had a `harvestJobStatusSucceeded` constant -- `IN_PROGRESS` did not exist as
a Go value anywhere in the package, which is a wire-completeness gap
independent of whether this backend ever reaches that state. Now declared
and used as the initial (and only) status this backend sets; `SUCCEEDED`/
`FAILED` are documented in a comment rather than declared as unused
identifiers (this backend never reaches either).

### Packaging protocol blocks: Authorization and MssPackage modeled to full depth

Per the modeling standard (full real-SDK depth or leave it and say why -- no
field-shaving), each opaque `map[string]any` packaging block was sized
against `types/types.go` in `aws-sdk-go-v2/service/mediapackage@v1.42.4`
before deciding what to model:

- **Authorization** (`types.go:10-26`): 2 required string fields
  (`CdnIdentifierSecret`, `SecretsRoleArn`), no nesting. Modeled fully as a
  typed `Authorization` struct (`interfaces.go`); `CreateOriginEndpoint`/
  `UpdateOriginEndpoint` now 422 if the block is present but either field is
  empty (previously any partial/malformed authorization map was silently
  accepted).
- **MssPackage** (`types.go:575-590`): 4 top-level fields, one level of
  nesting via `MssEncryption`→`SpekeKeyProvider` (`types.go:563-572,
  681-721`, 5 fields + nested `EncryptionContractConfiguration`,
  `types.go:246-259`, 2 fields) and `StreamSelection` (`types.go:724-736`, 3
  fields) -- 11 leaf fields total, no repeated sub-resources. Modeled fully
  as typed `MssPackage`/`MssEncryption`/`SpekeKeyProvider`/
  `EncryptionContractConfiguration`/`StreamSelection` structs; validates the
  SPEKE required-together chain (`SpekeKeyProvider` required if
  `Encryption` is present; `ResourceId`/`RoleArn`/`SystemIds`/`Url` required
  if `SpekeKeyProvider` is present; `PresetSpeke20Audio`/`PresetSpeke20Video`
  required together if `EncryptionContractConfiguration` is present) --
  closing the "no SPEKE handling" gap for this block.
- **HlsPackage/DashPackage/CmafPackage** are left as opaque passthrough --
  see `deferred` in the frontmatter for exact sizes. These are materially
  larger (12-16 leaf fields each across several enum types, and CmafPackage
  additionally nests a *list* of `HlsManifest` sub-resources, ~10 fields
  each) -- a distinct, larger unit of work than Authorization/MssPackage, not
  something to partially model and call done.

`PackagingConfig`/`OriginEndpoint`/`storedOriginEndpoint`/
`originEndpointOutput` all changed `Authorization`/`MssPackage` from
`map[string]any` to the new typed pointers; the JSON field names are
unchanged (`cdnIdentifierSecret`, `secretsRoleArn`, `resourceId`, `roleArn`,
`url`, `systemIds`, `certificateArn`, `encryptionContractConfiguration`,
`presetSpeke20Audio`, `presetSpeke20Video`, `manifestWindowSeconds`,
`segmentDurationSeconds`, `streamSelection`, `maxVideoBitsPerSecond`,
`minVideoBitsPerSecond`, `streamOrder`, `encryption`, `spekeKeyProvider`),
verified against both `serializers.go` and `deserializers.go` for symmetry,
so this is additive/compatible for any snapshot already using real field
names; the snapshot version was not bumped.

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
