---
service: kinesisvideo
sdk_module: aws-sdk-go-v2/service/kinesisvideo@v1.41.1
last_audit_commit: 54869319e
last_audit_date: 2026-09-25
overall: B            # new service, control plane only, unit-tested against the real SDK client
ops:
  CreateStream: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStream: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStreams: {wire: ok, errors: ok, state: ok, persist: ok, note: "StreamNameCondition BEGINS_WITH filter; opaque NextToken via pkgs/page"}
  UpdateStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "CurrentVersion optimistic lock"}
  DeleteStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "CurrentVersion optional per AWS docs"}
  UpdateDataRetention: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDataEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns an AWS-shaped emulator hostname; not backed by a real data plane -- see items_still_open"}
  TagStream: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagStream: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForStream: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "signaling-channel tags only, per AWS docs"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSignalingChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSignalingChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSignalingChannels: {wire: ok, errors: ok, state: ok, persist: ok, note: "ChannelNameCondition BEGINS_WITH filter"}
  UpdateSignalingChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "CurrentVersion optimistic lock"}
  DeleteSignalingChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeImageGenerationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateImageGenerationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeNotificationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateNotificationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Stream: {status: ok, note: "CreateStream/DescribeStream/ListStreams/UpdateStream/DeleteStream/UpdateDataRetention verified end-to-end against the real aws-sdk-go-v2 client over an httptest server -- wire shapes, epoch CreationTime, ARN format, CurrentVersion optimistic locking, and error deserialization (ResourceNotFoundException/ResourceInUseException/VersionMismatchException) all round-trip cleanly."}
  SignalingChannel: {status: ok, note: "Same CRUD + optimistic-lock coverage as Stream. SingleMasterConfiguration.MessageTtlSeconds defaults to 60s per AWS docs."}
  Tags: {status: ok, note: "Two disjoint tag families, matching real AWS: TagStream/UntagStream/ListTagsForStream key off the stream; TagResource/UntagResource/ListTagsForResource key off a signaling channel ARN only (confirmed against aws-sdk-go-v2 doc comments -- these three ops predate stream tagging and were never extended to cover streams)."}
  ImageGenerationConfiguration: {status: ok, note: "Describe/Update round-trip the full nested shape (DestinationConfig/Format/ImageSelectorType/SamplingInterval/Status/FormatConfig/HeightPixels/WidthPixels)."}
  NotificationConfiguration: {status: ok, note: "Describe/Update round-trip DestinationConfig.Uri and Status."}
gaps: []
items_still_open:
  - "Media data plane (PutMedia, GetMedia, GetMediaForFragmentList, GetHLSStreamingSessionURL,
    GetDASHStreamingSessionURL, GetClip, GetImages, ListFragments) is not implemented -- structural,
    out of scope for this pass. This is the aws-sdk-go-v2/service/kinesisvideomedia and
    kinesisvideoarchivedmedia client family, a genuinely separate data-plane service with its own
    endpoint (obtained via this service's GetDataEndpoint) and its own SDK module; it is not part
    of the kinesisvideo control-plane module this backend implements. GetDataEndpoint returns a
    wire-accurate, AWS-shaped hostname so control-plane callers (e.g. Rekognition stream processor
    setup, which only needs a stream to exist and its ARN) get a realistic response, but nothing is
    listening on that hostname."
  - "GetSignalingChannelEndpoint, CreateSignalingChannel's WebRTC ingestion, and the Edge Agent /
    MediaStorageConfiguration operation family (DescribeEdgeConfiguration, DeleteEdgeConfiguration,
    StartEdgeConfigurationUpdate, ListEdgeAgentConfigurations, DescribeMediaStorageConfiguration,
    UpdateMediaStorageConfiguration, DescribeMappedResourceConfiguration,
    DescribeStreamStorageConfiguration, UpdateStreamStorageConfiguration) are not implemented --
    structural, out of scope for this pass (not needed by the terraform aws_kinesis_video_stream
    resource or by Rekognition stream processors, which only need CreateStream/DescribeStream)."
  - "CREATING/UPDATING/DELETING transient stream and channel states are not modeled: CreateStream
    and CreateSignalingChannel return ACTIVE immediately and DeleteStream/DeleteSignalingChannel
    remove the resource immediately, rather than lingering through a transient state on a lazy
    deadline the way e.g. services/mediastore's container lifecycle does. This is an accepted
    simplification (explicitly allowed for this service by the parity-sweep task that added it),
    not a fidelity gap that changes any client-observable outcome other than timing."
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
