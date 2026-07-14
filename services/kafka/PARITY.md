---
service: kafka
sdk_module: aws-sdk-go-v2/service/kafka@v1.49.0
last_audit_commit: fb5f045f5a201fb9817e392cdf36684aa6cb36e6
last_audit_date: 2026-07-12
overall: A            # route-matcher bugs made an entire high-traffic op family unreachable
ops:
  UpdateBrokerCount: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: was under /api/v2/clusters (wrong, unreachable), now /v1/clusters/{arn}/nodes/count"}
  UpdateBrokerStorage: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/nodes/storage"}
  UpdateBrokerType: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/nodes/type"}
  UpdateClusterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/configuration"}
  UpdateClusterKafkaVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/version"}
  UpdateConnectivity: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/connectivity"}
  UpdateMonitoring: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/monitoring"}
  UpdateRebalancing: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/rebalancing"}
  UpdateSecurity: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/security, method corrected PUT->PATCH"}
  UpdateStorage: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: now /v1/clusters/{arn}/storage"}
  RejectClientVpcConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "route+wire fixed: PUT /v1/clusters/{arn}/client-vpc-connection (singular), vpcConnectionArn read from JSON body not path"}
  ListVpcConnections: {wire: ok, errors: ok, state: ok, persist: ok, note: "route fixed: GET /v1/vpc-connections (plural root, distinct from singular Create/Describe/Delete root)"}
  GetCompatibleKafkaVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "route fixed: top-level GET /v1/compatible-kafka-versions?clusterArn=..., was wrongly nested under /v1/clusters/{arn}/..."}
  DescribeTopicPartitions: {wire: partial, errors: ok, state: ok, persist: n/a, note: "route fixed: GET /v1/clusters/{arn}/topics/{name}/partitions (was wrong sibling shape). Response body still echoes the Topic shape instead of the real {nextToken, partitions:[{partition,leader,replicas,isr}]} shape -- see gaps."}
  UpdateReplicationInfo: {wire: partial, errors: ok, state: ok, persist: ok, note: "route fixed: ARN no longer has literal /replication-info glued onto it. Request/response fields still don't match real API (description vs currentVersion/sourceKafkaClusterArn/targetKafkaClusterArn/topicReplication/consumerGroupReplication) -- see gaps."}
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateClusterV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING->ACTIVE lazy transition on first poll confirmed correct, not a stuck-CREATING bug"}
  DescribeClusterV2: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClustersV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBootstrapBrokers: {wire: ok, errors: ok, state: ok, persist: n/a}
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
  CreateVpcConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeVpcConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVpcConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClientVpcConnections: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateReplicator: {wire: partial, errors: ok, state: partial, persist: ok, note: "route ok. Missing required real fields kafkaClusters/replicationInfoList -- see gaps"}
  DescribeReplicator: {wire: partial, errors: ok, state: ok, persist: ok, note: "reflects the simplified backend model, not full ReplicationInfo topology"}
  ListReplicators: {wire: partial, errors: ok, state: ok, persist: n/a}
  DeleteReplicator: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTopic: {wire: partial, errors: ok, state: ok, persist: ok, note: "route ok (was never broken). Field names diverge from real API (numPartitions/configEntries vs partitionCount/configs, missing topicArn/status) -- see gaps"}
  DescribeTopic: {wire: partial, errors: ok, state: ok, persist: ok, note: "same field-name divergence as CreateTopic"}
  ListTopics: {wire: partial, errors: ok, state: ok, persist: n/a}
  UpdateTopic: {wire: partial, errors: ok, state: ok, persist: ok}
  DeleteTopic: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  cluster_v1_v2_crud: {status: ok, note: "CreateCluster(V2)/DescribeCluster(V2)/ListClusters(V2)/DeleteCluster verified wire-accurate; CREATING->ACTIVE lazy-poll transition confirmed correct"}
  cluster_update_ops: {status: ok, note: "10 Update* ops were 100% unreachable pre-fix (routed under wrong /api/v2/clusters prefix while the real SDK sends them to /v1/clusters/{arn}/...); now fixed and covered by services/kafka/route_matcher_fixes_test.go"}
  configuration_crud_and_revisions: {status: ok}
  tags: {status: ok}
  scram_secrets: {status: ok}
  vpc_connection: {status: ok, note: "ListVpcConnections and RejectClientVpcConnection were unreachable pre-fix; fixed"}
  cluster_operations: {status: ok}
  cluster_policy: {status: ok}
  nodes_versions_bootstrap: {status: ok, note: "GetCompatibleKafkaVersions was unreachable pre-fix (wrong nesting); fixed. ListNodes/ListKafkaVersions/GetBootstrapBrokers verified"}
  replicator: {status: partial, note: "routes fixed (UpdateReplicationInfo ARN-suffix bug); wire shapes are a simplified model of the real ReplicationInfo/KafkaCluster topology -- see gaps"}
  topic: {status: partial, note: "DescribeTopicPartitions route fixed; CreateTopic/DescribeTopic/ListTopics/UpdateTopic field-name divergence not fixed this pass -- see gaps"}
gaps:
  - "Topic family (CreateTopic/DescribeTopic/ListTopics/UpdateTopic) uses field
    names numPartitions/configEntries (map) in its wire JSON; the real API uses
    partitionCount (int) and configs (an opaque Base64 string), and responses
    are also missing topicArn/status. A real aws-sdk-go-v2 client's CreateTopic
    call currently creates a topic with partition count silently defaulting to
    0 because gopherstack reads 'numPartitions' but the SDK sends
    'partitionCount'. Needs a dedicated fix pass: rework services/kafka/backend.go
    Topic struct + services/kafka/handler.go topic DTOs to match
    aws-sdk-go-v2/service/kafka@v1.49.0/api_op_{Create,Describe,List,Update}Topic.go."
  - "DescribeTopicPartitions response body still returns the Topic shape
    (topicName/replicationFactor/numPartitions) instead of the real
    {nextToken, partitions: [{partition, leader, replicas, isr}]} shape --
    the backend has no per-partition/broker-leader model to synthesize this
    from. Route is now reachable (fixed this pass); shape is not accurate."
  - "UpdateReplicationInfo request/response wire shape uses a single
    'description' field; the real UpdateReplicationInfoInput requires
    currentVersion/replicatorArn/sourceKafkaClusterArn/targetKafkaClusterArn
    and optional consumerGroupReplication/topicReplication updates. Route/ARN
    extraction is now fixed (this pass); the field-level shape is not."
  - "CreateReplicator is missing the real API's required kafkaClusters and
    replicationInfoList request fields entirely -- the backend has no concept
    of source/target cluster replication topology, only name/description/role.
    DescribeReplicator/ListReplicators therefore can't reflect real replication
    topology either. This is a feature gap, not a small bug fix."
  - "Cluster.CurrentVersion is set once at CreateCluster time (DefaultClusterVersion)
    and never advances after a successful Update* operation. Real MSK bumps
    CurrentVersion on every successful update so a second update must supply
    the new version (optimistic-lock chaining). gopherstack's requireCurrentVersion
    check still enforces non-empty/matching currentVersion per call, so this
    only under-rejects a client that (incorrectly) reuses a stale version
    across multiple updates -- it does not block the standard
    describe-then-update client workflow used by boto3/Terraform."
deferred:
  - "GetBootstrapBrokers / bootstrapBrokersFor endpoint synthesis logic was
    read but not adversarially wire-checked field-by-field against
    api_op_GetBootstrapBrokers.go this pass (spot-checked, looked correct)."
  - "ClientVpcConnection accept/reject workflow beyond RejectClientVpcConnection
    (AWS also exposes an implicit accept-by-default and no explicit Accept op
    in this SDK version) not re-verified against docs."
leaks: {status: clean, note: "no goroutines/timers introduced; all fixes are pure request-routing/parsing changes plus one new JSON body field read (RejectClientVpcConnection) and one new query-param read (GetCompatibleKafkaVersions)"}
---

## Notes

Kafka (MSK) is restjson1, with request paths split across four independent
roots: `/v1/clusters/...` (legacy "V1" surface -- also where **every** cluster
Update* op actually lives), `/api/v2/clusters/...` ("V2" surface -- only
Create/Describe/List/ListOperations, no updates), `/replication/v1/replicators/...`,
and `/v1/configurations/...` + `/v1/tags/{arn}` + `/v1/vpc-connection(s)` +
`/v1/kafka-versions` + `/v1/compatible-kafka-versions` as flat top-level roots.

### The core bug this pass found

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
unreachable. Regression coverage lives in
`services/kafka/route_matcher_fixes_test.go`, driven through the full
`Handler()`/`RouteMatcher()` stack (not direct handler calls) per the parity
route-matcher-check protocol.

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
