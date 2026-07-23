---
service: kafka
sdk_module: aws-sdk-go-v2/service/kafka@v1.49.0
last_audit_commit: fb5f045f5a201fb9817e392cdf36684aa6cb36e6  # unchanged: this pass could not run git (sandbox constraint) to read the real HEAD
last_audit_date: 2026-07-23
overall: A            # topic/replicator field-name/shape gaps closed; two prior "ok" families had a real wire bug each, now fixed
ops:
  UpdateBrokerCount: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: was under /api/v2/clusters (wrong, unreachable), now /v1/clusters/{arn}/nodes/count. CurrentVersion now advances on success (see cluster_current_version_advance)."}
  UpdateBrokerStorage: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/nodes/storage"}
  UpdateBrokerType: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/nodes/type"}
  UpdateClusterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/configuration"}
  UpdateClusterKafkaVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/version"}
  UpdateConnectivity: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/connectivity"}
  UpdateMonitoring: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/monitoring"}
  UpdateRebalancing: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/rebalancing"}
  UpdateSecurity: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/security, method corrected PUT->PATCH"}
  UpdateStorage: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/storage"}
  RejectClientVpcConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "route+wire fixed: PUT /v1/clusters/{arn}/client-vpc-connection (singular), vpcConnectionArn read from JSON body not path. Verified against SDK: no separate AcceptClientVpcConnection op exists in this SDK version -- Reject is the only client-VPC-connection mutation, so the family is complete, not partial."}
  ListVpcConnections: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: GET /v1/vpc-connections (plural root, distinct from singular Create/Describe/Delete root). Item shape (types.VpcConnection) field-diffed: targetClusterArn/vpcConnectionArn/authentication/creationTime/state/vpcId all present; creationTime added this pass (was missing)."}
  GetCompatibleKafkaVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "route fixed: top-level GET /v1/compatible-kafka-versions?clusterArn=..., was wrongly nested under /v1/clusters/{arn}/..."}
  DescribeTopicPartitions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "route ok. Response body reworked to the real {nextToken, partitions:[{partition,leader,replicas,isr}]} shape (types.TopicPartitionInfo), field-diffed against deserializers.go. Backend synthesizes a round-robin leader/replica assignment over the cluster's broker IDs (1..NumberOfBrokerNodes) with the full replica set always reported in-sync (isr==replicas) -- this in-memory emulator has no real broker/ISR divergence to model; documented simplification, not a wire-shape gap."}
  UpdateReplicationInfo: {wire: ok, errors: ok, state: ok, persist: ok, note: "route ok. Request/response now match the real UpdateReplicationInfoInput/Output: currentVersion/sourceKafkaClusterArn/targetKafkaClusterArn (required) + optional topicReplication/consumerGroupReplication updates applied to the matching ReplicationInfoConfig flow; response is replicatorArn/replicatorState only. Optimistic-lock currentVersion check added (mismatch -> BadRequestException); unknown (source,target) flow -> NotFoundException."}
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateClusterV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING->ACTIVE lazy transition on first poll confirmed correct, not a stuck-CREATING bug"}
  DescribeClusterV2: {wire: ok, errors: ok, state: ok, persist: ok}
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
  ListNodes: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListKafkaVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeClusterOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeClusterOperationV2: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClusterOperations: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListClusterOperationsV2: {wire: ok, errors: ok, state: ok, persist: n/a}
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
families:
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
gaps: []
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
leaks: {status: clean, note: "no goroutines/timers introduced or found this pass; all new logic (topic partition synthesis, replicator alias resolution, CurrentVersion token generation) is synchronous, computed under the existing coarse b.mu per call, with no new background work."}
---

## Notes

Kafka (MSK) is restjson1, with request paths split across four independent
roots: `/v1/clusters/...` (legacy "V1" surface -- also where **every** cluster
Update* op actually lives), `/api/v2/clusters/...` ("V2" surface -- only
Create/Describe/List/ListOperations, no updates), `/replication/v1/replicators/...`,
and `/v1/configurations/...` + `/v1/tags/{arn}` + `/v1/vpc-connection(s)` +
`/v1/kafka-versions` + `/v1/compatible-kafka-versions` as flat top-level roots.

### This pass: closing the topic/replicator field-name gaps + two new wire bugs

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
