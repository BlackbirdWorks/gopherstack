---
service: kinesisvideo
sdk_module: aws-sdk-go-v2/service/kinesisvideo@v1.41.1
last_audit_commit: 5bb0d02ee
last_audit_date: 2026-10-05
overall: A            # A: SDK-driven test/integration suite TestIntegration_KinesisVideo_StreamLifecycle + TestIntegration_KinesisVideo_SignalingChannelLifecycle (test/integration/kinesisvideo_test.go) + every buildable gap closed; remaining divergences are structural_gaps.
ops:
  CreateStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-10-05: CREATING, lazily ACTIVE after 500ms"}
  DescribeStream: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStreams: {wire: ok, errors: ok, state: ok, persist: ok, note: "StreamNameCondition BEGINS_WITH filter; opaque NextToken via pkgs/page"}
  UpdateStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "CurrentVersion optimistic lock; UPDATING, lazily ACTIVE"}
  DeleteStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "CurrentVersion optional per AWS docs; DELETING, lazily removed after 500ms, name reserved meanwhile"}
  UpdateDataRetention: {wire: ok, errors: ok, state: ok, persist: ok, note: "UPDATING, lazily ACTIVE"}
  GetDataEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns an AWS-shaped emulator hostname; not backed by a real data plane -- see structural_gaps"}
  TagStream: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagStream: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForStream: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "signaling-channel tags only, per AWS docs"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSignalingChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-10-05: CREATING, lazily ACTIVE after 500ms"}
  DescribeSignalingChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSignalingChannels: {wire: ok, errors: ok, state: ok, persist: ok, note: "ChannelNameCondition BEGINS_WITH filter"}
  UpdateSignalingChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "CurrentVersion optimistic lock; UPDATING, lazily ACTIVE"}
  DeleteSignalingChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETING, lazily removed after 500ms"}
  DescribeImageGenerationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateImageGenerationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeNotificationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateNotificationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStreamStorageConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "DefaultStorageTier, HOT when never set"}
  UpdateStreamStorageConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "CurrentVersion optimistic lock; bumps the stream version"}
  DescribeMediaStorageConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "absent until first Update"}
  UpdateMediaStorageConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "ENABLED needs an existing stream with non-zero retention (NoDataRetentionException)"}
  GetSignalingChannelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "one emulator-hosted endpoint per requested protocol; nothing listens on it"}
  StartEdgeConfigurationUpdate: {wire: ok, errors: ok, state: ok, persist: ok, note: "SYNCING, reported IN_SYNC after 2s in place of an edge agent ack"}
  DescribeEdgeConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "EdgeAgentStatus omitted: no agent exists -- see structural_gaps"}
  DeleteEdgeConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "removed immediately, no DELETING state"}
  DescribeMappedResourceConfiguration: {wire: deferred, errors: deferred, state: deferred, persist: n/a, note: "not implemented: the Type value space is unpublished -- see structural_gaps"}
  ListEdgeAgentConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "filtered by HubDeviceArn; opaque NextToken via pkgs/page"}
families:
  Stream: {status: ok, note: "CreateStream/DescribeStream/ListStreams/UpdateStream/DeleteStream/UpdateDataRetention verified end-to-end against the real aws-sdk-go-v2 client over an httptest server -- wire shapes, epoch CreationTime, ARN format, CurrentVersion optimistic locking, and error deserialization (ResourceNotFoundException/ResourceInUseException/VersionMismatchException) all round-trip cleanly."}
  SignalingChannel: {status: ok, note: "Same CRUD + optimistic-lock coverage as Stream. SingleMasterConfiguration.MessageTtlSeconds defaults to 60s per AWS docs."}
  Tags: {status: ok, note: "Two disjoint tag families, matching real AWS: TagStream/UntagStream/ListTagsForStream key off the stream; TagResource/UntagResource/ListTagsForResource key off a signaling channel ARN only (confirmed against aws-sdk-go-v2 doc comments -- these three ops predate stream tagging and were never extended to cover streams)."}
  ImageGenerationConfiguration: {status: ok, note: "Describe/Update round-trip the full nested shape (DestinationConfig/Format/ImageSelectorType/SamplingInterval/Status/FormatConfig/HeightPixels/WidthPixels)."}
  NotificationConfiguration: {status: ok, note: "Describe/Update round-trip DestinationConfig.Uri and Status."}
gaps: []
items_still_open: []
structural_gaps:
  - "Media data plane is not in this module: PutMedia/GetMedia, GetHLSStreamingSessionURL/GetDASHStreamingSessionURL/GetClip/ListFragments, the signaling-channel message plane (kinesisvideosignaling) and WebRTC storage sessions are separate AWS services with their own SDK modules, wire protocols (streaming MKV fragments, HLS/DASH muxing, ICE/TURN) and endpoints. GetDataEndpoint and GetSignalingChannelEndpoint return AWS-shaped hostnames that nothing listens on."
  - "EdgeAgentStatus / FailedStatusDetails on DescribeEdgeConfiguration are never populated: they report the state of a physical edge agent. SYNCING flips to IN_SYNC after 2s in its place."
  - "DescribeMappedResourceConfiguration is not implemented: it reports the resources mapped to a stream by WebRTC ingestion, but MappedResourceConfigurationListItem.Type is an unconstrained string whose values neither the pinned SDK nor the API reference publish, so any value emitted would be invented."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; lifecycle transitions are lazy deadlines evaluated under the backend lock, no goroutines or timers."}
---

## Notes

Initial implementation (2026-09-25): control-plane REST-JSON API modeled after
services/mediastore and services/iotanalytics. Every operation mutates/reads
real in-memory state via pkgs/store.Table + pkgs/lockmetrics.RWMutex, with
JSON snapshot/restore wired into pkgs/persistence. Wire shapes and error
codes were verified against the pinned aws-sdk-go-v2/service/kinesisvideo
v1.41.1 serializers.go/deserializers.go, including the real-AWS quirk that
TagResource/UntagResource/ListTagsForResource use PascalCase URI paths
(/TagResource) while every other operation uses camelCase (/createStream).

## 2026-10-04 (gopherstack-jrfzw multi-region)

kinesisvideo is region-isolated: streams, signaling channels and edge configurations live per region. Per-region sibling handlers via `pkgs/regionpeers`; snapshots gain an additive `regions` key only when a sibling exists (no version bump; older snapshots restore). `NewHandler` alone stays single-region. Proof: `TestHandler_MultiRegionIsolation`, `TestHandler_MultiRegionPersistence`, `TestRegionIsolation/kinesisvideo`. Limitation: the dashboard shows the home region only. CloudFormation provisions it in the stack's region. `TestHandler_MultiRegionReset` covers Reset.

## 2026-10-04 (gopherstack-uox6 value-semantics)

ListStreams MaxResults defaults to 10,000 and ListEdgeAgentConfigurations to 5 per the SDK docs (were 500); ListSignalingChannels stays 500.

### 2026-10-05: A grade, lifecycle states

Grade moves B to A on `TestIntegration_KinesisVideo_StreamLifecycle` and `TestIntegration_KinesisVideo_SignalingChannelLifecycle` (test/integration/kinesisvideo_test.go, real SDK client against the Docker image) plus the buildable gap the B grade named.

- Streams and signaling channels now pass through CREATING, UPDATING and DELETING (types.StatusCreating/Updating/Deleting) on 500ms lazy deadlines (`sweepLocked`, lifecycle.go), evaluated on every backend call instead of by a goroutine. DELETING keeps the name reserved until removal. UpdateStream, UpdateDataRetention and UpdateSignalingChannel move an ACTIVE resource to UPDATING. Mutations are not gated on a transitioning resource: this emulator does not reject them. Covered by `TestStreamLifecycle` and `TestChannelLifecycle` (lifecycle_test.go); the existing describe/delete tests now wait for ACTIVE and for NotFound.
- Persistence is additive: `Stream.PendingUntil` and `Channel.PendingUntil` (snapshot inventory rows added, no version bump).
- The media data plane, edge-agent status and DescribeMappedResourceConfiguration remain, recorded in structural_gaps. The last is the closest call: the mapping data exists (the signaling channel whose media storage targets the stream) but the response's Type string cannot be sourced, so the op stays unimplemented rather than half-working.

## 2026-10-05 errcodeaudit note (gopherstack-r3pr)

- Default-branch InternalFailureException: the pinned kinesisvideo SDK models no 5xx type, so a client sees a generic smithy APIError with that code; left as is.
