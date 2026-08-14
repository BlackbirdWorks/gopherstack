---
service: kafka
sdk_module: aws-sdk-go-v2/service/kafka@v1.57.2
last_audit_commit: fcb3fbbb9f46c11d4cf4034410f5ec80e7f16f63
last_audit_date: 2026-08-05
overall: A            # topic/replicator field-name/shape gaps closed; two prior "ok" families had a real wire bug each, now fixed
ops:
  UpdateBrokerCount: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: was under /api/v2/clusters (wrong, unreachable), now /v1/clusters/{arn}/nodes/count. CurrentVersion now advances on success (see cluster_current_version_advance)."}
  UpdateBrokerStorage: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/nodes/storage"}
  UpdateBrokerType: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/nodes/type"}
  UpdateClusterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/configuration"}
  UpdateClusterKafkaVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/version"}
  UpdateConnectivity: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/connectivity"}
  UpdateMonitoring: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/monitoring"}
  UpdateRebalancing: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/rebalancing. Fixed (gopherstack-h910): dropped both CurrentVersion (optimistic-lock check every sibling Update op enforces) and Rebalancing.Status, behind a false comment claiming AWS exposes no per-field rebalancing configuration -- types.Rebalancing.Status is real and persistable. Now enforces CurrentVersion via requireCurrentVersion and persists Status onto Cluster.Rebalancing, echoed by DescribeCluster/DescribeClusterV2's new rebalancing field."}
  UpdateSecurity: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/security, method corrected PUT->PATCH"}
  UpdateStorage: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/storage"}
  RejectClientVpcConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "route+wire fixed: PUT /v1/clusters/{arn}/client-vpc-connection (singular), vpcConnectionArn read from JSON body not path. Verified against SDK: no separate AcceptClientVpcConnection op exists in this SDK version -- Reject is the only client-VPC-connection mutation, so the family is complete, not partial."}
  ListVpcConnections: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: GET /v1/vpc-connections (plural root, distinct from singular Create/Describe/Delete root). Item shape (types.VpcConnection) field-diffed: targetClusterArn/vpcConnectionArn/authentication/creationTime/state/vpcId all present; creationTime added this pass (was missing)."}
  GetCompatibleKafkaVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "route fixed: top-level GET /v1/compatible-kafka-versions?clusterArn=..., was wrongly nested under /v1/clusters/{arn}/..."}
  DescribeTopicPartitions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "route ok. Response body reworked to the real {nextToken, partitions:[{partition,leader,replicas,isr}]} shape (types.TopicPartitionInfo), field-diffed against deserializers.go. Backend synthesizes a round-robin leader/replica assignment over the cluster's broker IDs (1..NumberOfBrokerNodes) with the full replica set always reported in-sync (isr==replicas) -- this in-memory emulator has no real broker/ISR divergence to model; documented simplification, not a wire-shape gap."}
  UpdateReplicationInfo: {wire: ok, errors: ok, state: ok, persist: ok, note: "route ok. Request/response now match the real UpdateReplicationInfoInput/Output: currentVersion/sourceKafkaClusterArn/targetKafkaClusterArn (required) + optional topicReplication/consumerGroupReplication updates applied to the matching ReplicationInfoConfig flow; response is replicatorArn/replicatorState only. Optimistic-lock currentVersion check added (mismatch -> BadRequestException); unknown (source,target) flow -> NotFoundException."}
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateClusterV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING->ACTIVE lazy transition on first poll confirmed correct, not a stuck-CREATING bug. Now also echoes rebalancing (gopherstack-h910, added so UpdateRebalancing's persisted status is observable)"}
  DescribeClusterV2: {wire: ok, errors: ok, state: ok, persist: ok, note: "Provisioned.rebalancing now echoed (gopherstack-h910, added so UpdateRebalancing's persisted status is observable)"}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClustersV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBootstrapBrokers: {wire: ok, errors: ok, state: ok, persist: n/a, note: "field-diffed this pass against deserializers.go's switch on awsRestjson1_deserializeOpDocumentGetBootstrapBrokersOutput -- found and fixed 4 wrong JSON field names (see notes below). Was marked wire:ok pre-existing without ever being field-diffed; the bug predates this pass."}
  CreateConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "route was already correct: PUT /v1/configurations/{arn}"}
  DescribeConfigurationRevision: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListConfigurationRevisions: {wire: ok, errors: ok, state: ok, persist: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  BatchAssociateScramSecret: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDisassociateScramSecret: {wire: ok, errors: ok, state: ok, persist: ok}
  ListScramSecrets: {wire: ok, errors: ok, state: ok, persist: n/a}
  RebootBroker: {wire: ok, errors: ok, state: ok, persist: ok}
  ListNodes: {wire: partial, errors: ok, state: ok, persist: n/a, note: "2026-08-14 (gopherstack-dv4s batch five): 'wire: ok' was wrong -- found while auditing for over-wide leaks (this op itself is NOT over-wide: no extra fields beyond a genuine narrow type, since there is no DescribeNode to leak FROM). Real types.NodeInfo (kafka@v1.57.2 types.go) declares AddedToClusterTime/BrokerNodeInfo/ControllerNodeInfo/InstanceType/NodeARN/NodeType/ZookeeperNodeInfo -- seven members, six nested/detailed. BrokerNode (models.go:376-379) has only InstanceType (real) and BrokerID (json:\"brokerId\", not a real NodeInfo member under any name) -- missing six required-shape members and emitting one invented one. Not fixed here (out of the over-wide sweep's scope, needs new BrokerNodeInfo/ControllerNodeInfo/ZookeeperNodeInfo modeling); filed as gopherstack-mk3t."}
  ListKafkaVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeClusterOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeClusterOperationV2: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClusterOperations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "2026-08-14 (gopherstack-dv4s batch five): verified NOT a candidate for the over-wide List sweep -- real ListClusterOperationsOutput.ClusterOperationInfoList is []types.ClusterOperationInfo, the exact same type DescribeClusterOperationOutput uses (kafka@v1.57.2 api_op_ListClusterOperations.go/api_op_DescribeClusterOperation.go). AWS itself doesn't narrow V1, so reusing *ClusterOperation for both here is correct, unlike V2 below."}
  ListClusterOperationsV2: {wire: ok, errors: ok, state: ok, persist: n/a, note: "2026-08-14 (gopherstack-dv4s batch five): FIXED an over-wide leak -- unlike V1, the real API declares a genuinely narrower ClusterOperationV2Summary (clusterArn/clusterType/endTime/operationArn/operationState/operationType/startTime) distinct from ClusterOperationV2 (Describe's full type, with sourceClusterInfo/targetClusterInfo nested under a Provisioned/Serverless wrapper this backend doesn't model). This handler was marshaling the same *ClusterOperation domain struct DescribeClusterOperationV2 uses, leaking sourceClusterInfo/targetClusterInfo wholesale. Now builds a dedicated clusterOperationV2SummaryOutput with just clusterArn/operationArn/operationState/operationType -- clusterType/startTime/endTime are real required Summary members this backend has never tracked (V2 ops forward to the V1 backend, see cluster_operations.go) and are left absent rather than fabricated. operationArn is also the correct real wire key for this new type; Describe/V1 still emit the same data under the wrong key clusterOperationArn (real ClusterOperationInfo and ClusterOperationV2 both use operationArn too) -- a separate, larger pre-existing bug across the whole ClusterOperation family, filed as gopherstack-mk3t, not fixed here."}
  CreateVpcConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against api_op_CreateVpcConnection.go this pass: clientSubnets/securityGroups are REQUIRED real-API input fields gopherstack silently dropped entirely (not stored, not echoed back) -- now accepted, stored, and echoed. Fixed CreateVpcConnectionOutput to drop the extra targetClusterArn field the real output does not have and add clientSubnets/securityGroups/creationTime/tags, which it does."}
  DescribeVpcConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed: real DescribeVpcConnectionOutput adds securityGroups/subnets/tags/creationTime on top of the ListVpcConnections item shape -- all four were missing; now a dedicated describeVpcConnectionOutput DTO matches."}
  DeleteVpcConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClientVpcConnections: {wire: ok, errors: ok, state: ok, persist: n/a, note: "REAL BUG FOUND AND FIXED this pass (was marked wire:ok without ever being field-diffed): response used the wrong envelope key (vpcConnections instead of the real clientVpcConnections) and the wrong item shape (reused the full VpcConnection/targetClusterArn+vpcId shape instead of the real, narrower types.ClientVpcConnection: vpcConnectionArn/authentication/creationTime/owner/state). A real aws-sdk-go-v2 client's ListClientVpcConnections call got an empty list on every call before this fix, regardless of how many client VPC connections actually existed -- complete functional breakage, not a cosmetic field gap. owner is populated from the backend's AccountID as a best-effort placeholder (gopherstack has no cross-account VPC-connection-owner modeling)."}
  CreateReplicator: {wire: ok, errors: ok, state: ok, persist: ok, note: "gap closed: kafkaClusters ([]KafkaCluster: amazonMskCluster+vpcConfig) and replicationInfoList ([]ReplicationInfo: source/target ARN, targetCompressionType, topicReplication, consumerGroupReplication) are now accepted, validated field-for-field against types.KafkaCluster/types.ReplicationInfo, and fully persisted. Not hard-required server-side (real aws-sdk-go-v2 client-side validation middleware never sends a request missing either, so a real client can never trigger a missing-field rejection here) -- see kafka::replicators.go CreateReplicator doc comment."}
  DescribeReplicator: {wire: ok, errors: ok, state: ok, persist: ok, note: "now reflects real topology: kafkaClusters as []KafkaClusterDescription with kafkaClusterAlias resolved from the referenced MSK cluster's live ClusterName (falling back to the ARN's trailing resource segment if the cluster doesn't exist in this backend), replicationInfoList as []ReplicationInfoDescription with sourceKafkaClusterAlias/targetKafkaClusterAlias resolved the same way, plus currentVersion/creationTime/replicatorResourceArn/isReplicatorReference/stateInfo/tags."}
  ListReplicators: {wire: ok, errors: ok, state: ok, persist: n/a, note: "now returns real ReplicatorSummary shape: kafkaClustersSummary/replicationInfoSummaryList (alias-only, no VPC config or full replication settings) plus currentVersion/creationTime/replicatorResourceArn."}
  DeleteReplicator: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "gap closed: wire fields reworked to partitionCount/replicationFactor/configs (opaque Base64 string, stored/echoed verbatim, never interpreted) on input and status/topicArn/topicName on output, field-diffed against api_op_CreateTopic.go. topicArn built as arn:{partition}:kafka:{region}:{account}:topic/{clusterName}/{clusterUUID}/{topicName}, reusing the owning cluster's own ARN resource path the way real MSK topic ARNs do. Status is ACTIVE immediately (topic creation has no CREATING-poll protocol exposed by the real API the way cluster creation does); documented simplification."}
  DescribeTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "gap closed: response is configs/partitionCount/replicationFactor/status/topicArn/topicName only (clusterArn, needed internally for the primary key/topicsByCluster index, is intentionally excluded from the wire DTO -- see describeTopicOutputFrom in handler_topics.go)."}
  ListTopics: {wire: ok, errors: ok, state: ok, persist: n/a, note: "gap closed: element shape is now the real, distinct TopicInfo (topicArn/topicName/partitionCount/replicationFactor/outOfSyncReplicaCount -- no configs/status, unlike DescribeTopic). topicNameFilter query param now supported (was silently ignored before)."}
  UpdateTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "gap closed: same partitionCount/configs input rework as CreateTopic; response is status/topicArn/topicName only."}
  DeleteTopic: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "new in v1.57. POST /v1/clusters/{ClusterArn}/channels, ClusterArn URI-templated (validated against serializers.go's awsRestjson1_serializeOpCreateChannel/awsRestjson1_serializeOpHttpBindingsCreateChannelInput). Response is channelArn/clusterOperationArn only (awsRestjson1_deserializeOpDocumentCreateChannelOutput). Full required-field validation implemented server-side per validators.go's validateOpCreateChannelInput/validateIcebergDestinationConfiguration/validateS3DestinationConfiguration chains, plus a server-side 'exactly one of s3DestinationConfiguration/icebergDestinationConfiguration' check the client-side validator itself does not enforce (neither field is marked required there) but CreateChannelInput's doc comments describe as mutually exclusive. errCodeLookup covers BadRequestException/ConflictException/ForbiddenException/InternalServerErrorException/NotFoundException/ServiceUnavailableException/TooManyRequestsException/UnauthorizedException per awsRestjson1_deserializeOpErrorCreateChannel."}
  DeleteChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "new in v1.57. DELETE /v1/clusters/{ClusterArn}/channels/{ChannelArn}, both ARNs URI-templated. Response is channelArn/clusterOperationArn (awsRestjson1_deserializeOpDocumentDeleteChannelOutput). Cluster-scope check: a channelArn that exists under a different clusterArn 404s, matching the cluster-scoped resource model DescribeChannel/UpdateChannel also enforce."}
  DescribeChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "new in v1.57. GET /v1/clusters/{ClusterArn}/channels/{ChannelArn}. Response field-diffed against awsRestjson1_deserializeOpDocumentDescribeChannelOutput: channelArn/channelName/clusterOperationArn/creationTime/destinationType/encryptionConfiguration/icebergDestinationConfiguration/loggingInfo/s3DestinationConfiguration/stateInfo/status/tags/topicConfigurationList, all field-name-matched including every nested type (Catalog/DeadLetterQueueS3/DestinationTable/PartitionSpec/PartitionSource/RecordConverter/RecordSchema/S3Storage/SchemaEvolution/TableCreation) verified against types.go + their respective serializeDocument*/deserializeDocument* pairs. clusterArn (internal only, load-bearing for the ClusterArn-scope check and channelsByCluster index) is excluded from the wire DTO via describeChannelOutputFrom, the same pattern describeTopicOutputFrom uses for Topic."}
  ListChannels: {wire: ok, errors: ok, state: ok, persist: n/a, note: "new in v1.57. GET /v1/clusters/{ClusterArn}/channels, maxResults/nextToken/topicNameFilter as query params (awsRestjson1_serializeOpHttpBindingsListChannelsInput), reusing the package's existing base64url offset-token pagination helpers (kafkaPageSize/encodeKafkaPageToken/decodeKafkaPageToken). Element shape is the real, distinct types.ChannelInfo (channelArn/channelName/clusterOperationArn/creationTime/destinationType/status only -- no destination-configuration/logging/tags/topicConfigurationList detail), field-diffed against awsRestjson1_deserializeDocumentChannelInfo."}
  UpdateChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "new in v1.57. PUT /v1/clusters/{ClusterArn}/channels/{ChannelArn}, body is icebergDestinationUpdate/s3DestinationUpdate (mutually exclusive, exactly one required -- api_op_UpdateChannel.go's doc comment: 'You must update the same destination type the channel was created with; the destination type cannot be changed.'). Response is channelArn/clusterOperationArn only. Server-side validation rejects a destination-type mismatch (e.g. s3DestinationUpdate against an ICEBERG channel) with BadRequestException, since neither update field is marked client-side required in validators.go -- only the service, which knows the channel's actual DestinationType, can reject that."}
families:
  channels: {status: ok, note: "New MSK Channels family (aws-sdk-go-v2/service/kafka v1.57, streams an Express cluster topic to S3 or Apache Iceberg): CreateChannel/DeleteChannel/DescribeChannel/ListChannels/UpdateChannel all implemented against real backend state (services/kafka/channels.go), routed under /v1/clusters/{ClusterArn}/channels(/{ChannelArn}) alongside the existing Topic sub-resource (services/kafka/routes.go's parseClusterResourceV1Channels), and persisted through a new channels store.Table + channelsByCluster index (store_setup.go), included automatically in Snapshot/RestoreAll. Unlike Cluster/Configuration/Replicator/VpcConnection, Channel.Tags carries a normal (not json:\"-\") JSON tag, since the real DescribeChannelOutput wire shape genuinely includes tags -- see the Channel doc comment in models.go -- so Channel tags survive a Snapshot/Restore round trip without the persistence gap those four resources have. TagResource/UntagResource/ListTagsForResource (services/kafka/tags.go) were extended to recognize channel ARNs too, since CreateChannel accepts tags at creation and leaving the generic tag ops unable to retag a channel afterward would have been a real, if narrow, regression. See services/kafka/channels_test.go and services/kafka/handler_channels_test.go for full lifecycle, validation-failure, not-found, cross-cluster-scope, and Snapshot/Restore round-trip coverage, driven through the real HTTP wire path/JSON body per services/kafka/handler_channels_test.go's s3ChannelCreateBody helper."}
  cluster_v1_v2_crud: {status: ok, note: "CreateCluster(V2)/DescribeCluster(V2)/ListClusters(V2)/DeleteCluster verified wire-accurate; CREATING->ACTIVE lazy-poll transition confirmed correct"}
  cluster_update_ops: {status: ok, note: "10 Update* ops were 100% unreachable pre-fix (routed under wrong /api/v2/clusters prefix while the real SDK sends them to /v1/clusters/{arn}/...); fixed. CurrentVersion optimistic-lock token now advances on every successful update (see cluster_current_version_advance) -- a second Update* call against the same cluster must fetch the version the first call left behind, matching real MSK; TestClusterOperationTracking_V1 and TestUpdateOpsRequireCurrentVersion cover both the advance and the stale-version rejection."}
  cluster_current_version_advance: {status: ok, note: "Cluster.CurrentVersion (and Replicator.CurrentVersion, same mechanism) now advances via nextVersionToken() on every successful mutating operation (newClusterOperationLocked for clusters; UpdateReplicationInfo for replicators), closing the gap where a second update against the same resource incorrectly succeeded while reusing a stale version."}
  configuration_crud_and_revisions: {status: ok}
  tags: {status: ok}
  scram_secrets: {status: ok}
  vpc_connection: {status: ok, note: "ListClientVpcConnections had a real wire-envelope/shape bug (wrong JSON key + wrong item shape, returning an empty list to every real client) found and fixed this pass despite being marked ok previously -- see the ListClientVpcConnections op note. CreateVpcConnection/DescribeVpcConnection field-diffed and closed (clientSubnets/securityGroups/creationTime were missing). ListVpcConnections and RejectClientVpcConnection route fixes from the prior pass reconfirmed correct."}
  cluster_operations: {status: ok}
  cluster_policy: {status: ok}
  nodes_versions_bootstrap: {status: ok, note: "GetCompatibleKafkaVersions was unreachable pre-fix (wrong nesting); fixed. GetBootstrapBrokers field-diffed this pass (previously only spot-checked, not adversarially verified) -- 4 wrong JSON field names found and fixed, see the op note. ListNodes/ListKafkaVersions verified."}
  replicator: {status: ok, note: "full ReplicationInfo/KafkaCluster topology now implemented end-to-end: CreateReplicator accepts and persists kafkaClusters/replicationInfoList; DescribeReplicator/ListReplicators resolve real KafkaClusterAlias/SourceKafkaClusterAlias/TargetKafkaClusterAlias from the live cluster table; UpdateReplicationInfo enforces the real currentVersion/source/target contract against a specific replication flow. See services/kafka/replicators_test.go TestCreateReplicator_TopologyAndAliasResolution and TestUpdateReplicationInfo_Backend."}
  topic: {status: ok, note: "CreateTopic/DescribeTopic/ListTopics/UpdateTopic field-name divergence closed (partitionCount/configs, topicArn/status, distinct TopicInfo list shape). DescribeTopicPartitions now returns the real {nextToken, partitions} shape with synthesized round-robin leader/replica placement. See services/kafka/topics_test.go and services/kafka/handler_topics_test.go."}
gaps:
  - "Channel Create/Update/Delete are immediate (no CREATING/UPDATING/DELETING
    polling window) -- same documented simplification as Topic.Status (see
    below): the real API exposes a ClusterOperationArn/polling protocol this
    in-memory emulator has no async execution to model, so Channel.Status goes
    straight to ACTIVE and ClusterOperationArn is populated only on the
    mutating call's own response, never on the persisted record (matching what
    a real client would observe once the real async operation has already
    completed by the time it calls Describe)."
  - "CreateChannel does not restrict channel creation to MSK Express clusters,
    even though CreateChannel's doc comment says a channel streams from 'an
    Amazon MSK Express cluster topic'. gopherstack's Cluster model has no
    Express-vs-standard-broker-type distinction anywhere else in this service,
    and the SDK's client-side validators.go does not enforce it either (it can
    only be a server-side rule), so modeling this specific restriction here
    would mean inventing a cluster-type check found nowhere else in the
    codebase rather than verifying one against the SDK."
  - "CreateChannel does not verify that TopicConfigurationList[].TopicArn
    references a topic that actually exists in this backend. The real
    service's behavior here is unverifiable from the client SDK alone (no
    client-side check exists in validators.go), so enforcing an invented rule
    risks fabricating unproven behavior; the ARN is accepted, stored, and
    echoed back verbatim instead."
  # All 5 gaps from the 2026-07-12 audit (topic field names, DescribeTopicPartitions
  # shape, UpdateReplicationInfo shape, CreateReplicator missing topology fields,
  # Cluster.CurrentVersion never advancing) are closed -- see the op/family notes
  # above for exactly what changed and where. Two NEW real wire bugs were found and
  # fixed while closing out the deferred items below (GetBootstrapBrokers field
  # names, ListClientVpcConnections envelope+shape) plus a missing-required-field
  # gap on CreateVpcConnection/DescribeVpcConnection (clientSubnets/securityGroups).
  #
  # Documented simplifications (not wire-shape gaps -- these are internal-model
  # choices that do not diverge from any real MSK response field or type):
  #   - Topic.Status is always ACTIVE immediately on Create/Update; real MSK's
  #     TopicState enum also has CREATING/UPDATING/DELETING but topic creation
  #     exposes no polling protocol the way cluster creation does, so there is no
  #     externally observable "stuck CREATING" behavior to get wrong.
  #   - DescribeTopicPartitions' Isr is always == Replicas (fully in-sync); this
  #     in-memory emulator has no real per-broker replication lag to diverge from.
  #   - ClientVpcConnection.Owner is populated from the backend's own AccountID as
  #     a best-effort placeholder; gopherstack has no cross-account VPC-connection
  #     ownership model to draw a different value from.
deferred: []
  # Both prior deferred items are now resolved:
  #   - GetBootstrapBrokers: field-diffed against deserializers.go this pass (see
  #     the op note) -- found and fixed 4 wrong JSON field names.
  #   - ClientVpcConnection accept/reject: verified against the vendored SDK
  #     module directory listing (api_op_*.go for kafka@v1.49.0) -- there is no
  #     AcceptClientVpcConnection (or similarly named) operation in this SDK
  #     version. RejectClientVpcConnection is the only client-VPC-connection
  #     mutation the real API exposes, so gopherstack's coverage is complete.
leaks: {status: clean, note: "no goroutines/timers introduced or found this pass; all new Channels logic (channelARN derivation, deep-clone helpers, destination-update validation) is synchronous, computed under the existing coarse b.mu per call via the new channels store.Table, with no new background work."}
---

## Notes

**2026-08-13 (gopherstack-jqh2 pass 3):** re-extracted all 64 ops' real
method+path directly from `kafka@v1.57.2` serializers.go and drove them
through `ExtractOperation` via the new `handler_sdk_route_table_test.go`
(`TestExtractOperation_SDKRouteTable`, one subtest per op, `t.Parallel()`).
All 64 resolved correctly, including the singular/plural
`/v1/vpc-connection` vs `/v1/vpc-connections` split and every
suffix-discriminated collision (`/nodes` vs `/nodes/{count,storage,type}`,
`/scram-secrets` POST/PATCH/GET). Also spot-checked with a real
slash-embedded MSK cluster ARN (`arn:aws:kafka:...:cluster/name/uuid-1`) on
four representative ops to confirm the string-suffix-based parser (not
segment-count-based) handles embedded slashes correctly — it does. No
pre-existing table existed to check, and no new routing bugs found. This
test is now the permanent regression guard for route-table drift.

Kafka (MSK) is restjson1, with request paths split across four independent
roots: `/v1/clusters/...` (legacy "V1" surface -- also where **every** cluster
Update* op actually lives), `/api/v2/clusters/...` ("V2" surface -- only
Create/Describe/List/ListOperations, no updates), `/replication/v1/replicators/...`,
and `/v1/configurations/...` + `/v1/tags/{arn}` + `/v1/vpc-connection(s)` +
`/v1/kafka-versions` + `/v1/compatible-kafka-versions` as flat top-level roots.

### This pass: SDK bump to v1.57.2 exposed a new Channels family

`aws-sdk-go-v2/service/kafka` v1.49.0 -> v1.57.2 added a fifth resource
family, Channels (streams an MSK Express cluster topic to Amazon S3 or Apache
Iceberg), which made `TestSDKCompleteness` fail: `CreateChannel`,
`DeleteChannel`, `DescribeChannel`, `ListChannels`, `UpdateChannel` all
implement real backend state (`services/kafka/channels.go`), routed under the
existing `/v1/clusters/{ClusterArn}/channels(/{ChannelArn})` prefix as a
sibling of the Topic sub-resource (`services/kafka/routes.go`'s new
`parseClusterResourceV1Channels`, checked before the generic
Describe/DeleteCluster fallback the same way `parseClusterResourceV1Topics`
already is). Every field name, HTTP method, URI path, and error-code switch
was verified against the vendored `aws-sdk-go-v2/service/kafka@v1.57.2`
module's `api_op_*Channel*.go`, `serializers.go`, `deserializers.go`, and
`validators.go` (`$(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/kafka@v1.57.2`).

Two path segments deserve a specific callout: `DeleteChannel`/
`DescribeChannel`/`UpdateChannel` URI-template **both** `{ClusterArn}` and
`{ChannelArn}` on the same path
(`/v1/clusters/{ClusterArn}/channels/{ChannelArn}`) -- unlike every other
nested-ARN case in this file (topics, VPC connections), where only one side
of the path is an ARN. The real SDK's `httpbinding.Encoder.SetURI` percent-
encodes any `/` inside either ARN, so both ARNs arrive at the server as
opaque, slash-free segments before `parseClusterResourceV1`'s single
`url.PathUnescape(remainder)` call runs -- splitting on the literal
`"/channels/"` marker is therefore unambiguous, the same reasoning that
already justifies `"/topics/"` splitting for `parseClusterResourceV1Topics`.

`Channel.Tags` is the one resource field in this file that intentionally
breaks the `json:"-"` convention every other MSK resource
(Cluster/Configuration/Replicator/VpcConnection) uses for `Tags`: those four
resources' real `Describe*Output` never embeds tags (fetched separately via
`ListTagsForResource`), but `DescribeChannelOutput`'s wire shape genuinely
includes a `"tags"` key (field-diffed against
`awsRestjson1_deserializeOpDocumentDescribeChannelOutput`). Tagging
`Channel.Tags` with `json:"-"` to match the other four would have been a wire
bug, not consistency; it uses a normal `json:"tags,omitempty"` tag instead,
which also means Channel tags survive a Snapshot/Restore round trip without
the `fixNilTags` persistence gap the other four have (though `fixNilTags`
still guards the narrower zero-tags-omitted-by-`omitempty` case, see
`persistence.go`). `TagResource`/`UntagResource`/`GetTags`
(`services/kafka/tags.go`) were extended to recognize channel ARNs alongside
the existing four, since `CreateChannel` accepts tags at creation and leaving
the generic tag ops unable to retag a channel afterward would have been a
real functional gap, not just a documentation one.

See `services/kafka/channels_test.go` (backend-level lifecycle, validation
failures, not-found, cross-cluster-scope, tag lifecycle, Snapshot/Restore
round trip) and `services/kafka/handler_channels_test.go` (the same lifecycle
driven through the real HTTP wire path and JSON body, plus pagination and
invalid-body handling) for coverage.

### Prior pass: closing the topic/replicator field-name gaps + two new wire bugs

The 2026-07-12 audit fixed route-matcher bugs but left five real gaps and two
deferred items. This pass closed all of them:

- **Topic family** (`CreateTopic`/`DescribeTopic`/`ListTopics`/`UpdateTopic`/
  `DescribeTopicPartitions`): `services/kafka/models.go`'s `Topic` struct now
  matches `DescribeTopicOutput` field-for-field (`configs`/`partitionCount`/
  `replicationFactor`/`status`/`topicArn`/`topicName`); `ClusterArn` stays on
  the struct (required for the primary key and `topicsByCluster` index -- see
  the "ClusterArn json tag" trap below) but is excluded from the wire DTO
  built in `handler_topics.go`. `ListTopics` uses the real, smaller
  `TopicInfo` shape. `DescribeTopicPartitions` synthesizes per-partition
  leader/replica placement from the cluster's broker count.
- **Replicator family**: `CreateReplicator` now accepts and persists
  `kafkaClusters`/`replicationInfoList` (new `KafkaClusterConfig`/
  `ReplicationInfoConfig`/`TopicReplicationConfig`/
  `ConsumerGroupReplicationConfig` model types in `models.go`).
  `DescribeReplicator`/`ListReplicators` resolve `KafkaClusterAlias`/
  `SourceKafkaClusterAlias`/`TargetKafkaClusterAlias` from the live cluster
  table (`InMemoryBackend.clusterAliasForArn`), matching how real MSK derives
  these aliases from the referenced cluster's actual name rather than storing
  them at creation time. `UpdateReplicationInfo` now requires
  `currentVersion`/`sourceKafkaClusterArn`/`targetKafkaClusterArn` and mutates
  the matching flow, with a real optimistic-lock version check.
- **Cluster/Replicator CurrentVersion advancement**: `nextVersionToken()`
  (`store.go`) generates a new 14-char opaque token on every successful
  mutating operation (`newClusterOperationLocked` for clusters,
  `UpdateReplicationInfo` for replicators), so a second update against the
  same resource must supply the version the first update left behind --
  matching real MSK's optimistic-lock contract instead of letting a client
  reuse a stale version indefinitely.
- **GetBootstrapBrokers** (closing the first deferred item): field-diffed
  against `deserializers.go`'s
  `awsRestjson1_deserializeOpDocumentGetBootstrapBrokersOutput` switch and
  found 4 wrong JSON field names in `getBootstrapBrokersOutput`
  (`handler_clusters.go`) -- `bootstrapBrokerStringTlsPublic` should be
  `bootstrapBrokerStringPublicTls` (real MSK puts "Public" *before* the
  auth-method suffix), same for the SASL/SCRAM and SASL/IAM public variants,
  plus `bootstrapBrokerStringVpcConnectivityTLS` had the wrong casing
  (`...VpcConnectivityTls`, lowercase `ls`). A real SDK client's JSON
  unmarshal silently drops any key it doesn't recognize, so all four fields
  were unreachable by a real `aws-sdk-go-v2` client before this fix, despite
  the op being marked `wire: ok`.
- **ClientVpcConnection accept/reject** (closing the second deferred item):
  confirmed via the vendored SDK module's file listing
  (`aws-sdk-go-v2/service/kafka@v1.49.0`, `api_op_*.go`) that no
  `AcceptClientVpcConnection` operation exists in this SDK version --
  `RejectClientVpcConnection` is the only client-VPC-connection mutation the
  real API exposes, so no additional op needed implementing.
- **New bug found while investigating the above**: `ListClientVpcConnections`
  was reusing `ListVpcConnections`' response shape (`{"vpcConnections": [...]}`
  of full `VpcConnection` items) instead of the real, distinct
  `{"clientVpcConnections": [...]}` of narrower `ClientVpcConnection` items
  (`vpcConnectionArn`/`authentication`/`creationTime`/`owner`/`state` --
  no `targetClusterArn`/`vpcId`). A real client got an **empty list on every
  call**, regardless of how many client VPC connections existed, since the
  envelope key never matched. Fixed with a dedicated
  `listClientVpcConnectionsOutput`/`clientVpcConnectionOutput` DTO pair in
  `handler_vpc_connections.go`.
- **New gap found**: `CreateVpcConnection`'s real input requires
  `clientSubnets`/`securityGroups` (`api_op_CreateVpcConnection.go`);
  gopherstack silently dropped both. Now accepted, stored on `VpcConnection`,
  and echoed back by `CreateVpcConnection`/`DescribeVpcConnection`.
  `CreateVpcConnectionOutput` also incorrectly echoed `targetClusterArn`
  (the real output doesn't have it) and was missing `creationTime`/`tags`.

### The route-matcher bug from the prior pass (2026-07-12, unchanged)

Every one of the 10 cluster Update* operations (UpdateBrokerCount,
UpdateBrokerStorage, UpdateBrokerType, UpdateClusterConfiguration,
UpdateClusterKafkaVersion, UpdateConnectivity, UpdateMonitoring,
UpdateRebalancing, UpdateSecurity, UpdateStorage) was routed under
`/api/v2/clusters/{ClusterArn}/<suffix>`. The real aws-sdk-go-v2 serializers
(verified directly against `serializers.go` in the vendored SDK module) send
every one of these to `/v1/clusters/{ClusterArn}/<suffix>` instead -- the V2
surface only has DescribeClusterV2/ListClustersV2/ListClusterOperationsV2.
This meant a real SDK client calling e.g. `UpdateBrokerCount` would 404
every time, no matter what gopherstack's handler/backend implementation did
(and the backend implementation was in fact correct -- this was purely a
route-matcher bug, not a stub). `UpdateSecurity` additionally uses PATCH, not
PUT, on the real API; gopherstack had it under a PUT-only map.

Five more independent route-matcher bugs of the same shape were found:
`RejectClientVpcConnection` (wrong path shape entirely --
`/reject-client-vpc-connection/{arn}` doesn't exist; real path is
`/v1/clusters/{arn}/client-vpc-connection` with the target ARN in the JSON
body, not the path), `ListVpcConnections` (real root is the *plural*
`/v1/vpc-connections`, distinct from the singular `/v1/vpc-connection` used by
Create/Describe/Delete -- gopherstack only recognized the singular for both),
`GetCompatibleKafkaVersions` (real path is the flat top-level
`/v1/compatible-kafka-versions?clusterArn=...`, not nested under
`/v1/clusters/{arn}/...`), `DescribeTopicPartitions` (real suffix is
`/topics/{topicName}/partitions`, nested under `/topics`; gopherstack looked
for a sibling `/topic-partitions/{topicName}`), and `UpdateReplicationInfo`
(the real path is `.../replicators/{arn}/replication-info`; gopherstack's
parser accepted any PUT to `.../replicators/{arn}` but never stripped the
`/replication-info` suffix, so the "ARN" it extracted always had that literal
suffix glued onto it, guaranteeing an ErrNotFound on every real call).

All six are fixed under `services/kafka/handler.go`'s path-parsing functions
only; no backend.go changes were needed since the backend implementations
were already real (state-mutating, not stubs) -- they were simply
unreachable. Regression coverage lives in `services/kafka/routes_test.go`,
driven through the full `Handler()`/`RouteMatcher()` stack (not direct
handler calls) per the parity route-matcher-check protocol.

### Traps for the next auditor

- `/v1/vpc-connection` (singular, POST only) vs `/v1/vpc-connections` (plural,
  GET only) are two *separate* roots on the real API, not a
  trailing-slash/pluralization quirk -- don't "fix" this back to one root.
- `/v1/clusters/{arn}/storage` (UpdateStorage) is a *suffix* of
  `/v1/clusters/{arn}/nodes/storage` (UpdateBrokerStorage) --
  `parseClusterResourceV1BrokerUpdates` (checks the `/nodes/...` suffixes)
  MUST run before `parseClusterResourceV1ClusterUpdates` (checks the bare
  `/storage` suffix) or `strings.HasSuffix` will misroute broker-storage
  updates to UpdateStorage.
- Persistence (`Snapshot`/`Restore`) delegation from `Handler` to
  `InMemoryBackend` was already correctly wired (`services/kafka/persistence.go`
  lines ~172-180) -- checked per the silent-unregistration bug class, no issue
  found.
- **`json:"-"` on a field that also drives a persistence primary key or
  secondary index is a live bug, not just a wire-purity nicety.** This pass
  briefly introduced exactly this bug on `Topic.ClusterArn` (tagged `json:"-"`
  to keep it out of the `DescribeTopic` response, matching the real wire
  shape) and caught it via `TestBackend_SnapshotRestoreFullState`: after any
  Snapshot->Restore round-trip, every topic's `ClusterArn` silently reverted
  to `""`, which is fed into both `topicKeyFn` (the primary key) and
  `topicClusterIndexKeyFn` (the `topicsByCluster` index) -- so `DescribeTopic`
  and `ListTopics` both failed to find any restored topic by its real
  `clusterArn`. The fix: keep `ClusterArn` on the persisted struct with a real
  JSON tag, and build a dedicated response DTO (`describeTopicOutputFrom`)
  that omits it, instead of tagging the model field itself `json:"-"`. Same
  principle already applies (correctly) to `Tags` on
  Cluster/Configuration/Replicator/VpcConnection, which is a documented,
  *intentional* exception: `Tags` drives no key/index anywhere, so `json:"-"`
  there only costs a known, separately-tracked persistence gap (tags don't
  survive Restore), not a functional lookup break.
- `Cluster.CurrentVersion`/`Replicator.CurrentVersion` now change on every
  successful update (see `cluster_current_version_advance`). Tests that issue
  two sequential updates against the *same* cluster/replicator in one test
  case must re-fetch the version between calls (via Describe) rather than
  reusing `DefaultClusterVersion`/the value captured at creation --
  `TestClusterOperationTracking_V1` needed exactly this fix this pass.
- **A confident wrong comment is worse than no comment** -- `UpdateRebalancing`
  carried "AWS MSK exposes no per-field rebalancing configuration to persist"
  next to code that dropped `CurrentVersion`/`Rebalancing.Status`, when
  `types.Rebalancing.Status` is a real, persistable field (gopherstack-h910).
  The comment read as a verified fact and stopped this bug from being caught
  earlier. Don't trust an existing comment's premise over reading the pinned
  SDK's own struct.
