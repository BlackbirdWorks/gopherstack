---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: dax
sdk_module: aws-sdk-go-v2/service/dax@v1.29.18   # awsjson1.1 protocol, target prefix AmazonDAXV3.
last_audit_commit: 61ba31abe8d8
last_audit_date: 2026-07-12
overall: A            # fresh audit, first PARITY.md for this service; several genuine wire bugs found and fixed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeClusters: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  IncreaseReplicationFactor: {wire: ok, errors: ok, state: ok, persist: ok}
  DecreaseReplicationFactor: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootNode: {wire: ok, errors: ok, state: ok, persist: ok, note: async recovery is intentional -- matches real AWS's transient "rebooting" status, see Notes}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeParameters: {wire: ok, errors: ok, state: partial, persist: ok, note: Source filter param (user/system/engine-default) accepted on the wire but not applied -- see gaps}
  DescribeDefaultParameters: {wire: ok, errors: ok, state: ok, persist: n/a}
  ResetParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEvents: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  cluster-lifecycle: {status: ok, note: "CreateCluster/DescribeClusters/UpdateCluster/DeleteCluster/IncreaseReplicationFactor/DecreaseReplicationFactor/RebootNode all mutate the real store.Table[Cluster], persist via backendSnapshot, and now emit the correct wire shape (Status key, epoch timestamps) -- see gaps for the 5 bugs found and fixed this pass."
  tags: {status: ok, note: "TagResource/UntagResource/ListTags mutate the ARN-keyed tags map and propagate cluster ARNs to Cluster.Tags; quota (50) and key/value length enforcement match AWS constraints."}
  parameter-groups: {status: ok, note: "CreateParameterGroup/DescribeParameterGroups/UpdateParameterGroup/DeleteParameterGroup/DescribeParameters/DescribeDefaultParameters/ResetParameterGroup all real; UpdateParameterGroup correctly cascades pending-reboot + NodeIdsToReboot to dependent clusters (now actually surfaced on the wire, see gaps)."
  subnet-groups: {status: ok, note: "CreateSubnetGroup/DescribeSubnetGroups/UpdateSubnetGroup/DeleteSubnetGroup real; in-use protection (blocks delete while referenced by a cluster) verified."}
  events: {status: ok, note: "DescribeEvents ring buffer (1000 cap) is real; StartTime/EndTime/SourceName/SourceType filtering verified after fixing the epoch-seconds request-parsing bug."}
  dataplane: {status: deferred, note: "Binary DAX client protocol (services/dax/dataplane/) is a separate, extensively self-tested subsystem (936-line dataplane_integration_test.go + dataplane/*_test.go) not covered by this control-plane wire-shape sweep. Not audited this pass."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "DescribeParameters ignores the request's Source filter (user/system/engine-default); AWS lets callers narrow the parameter list by source, gopherstack always returns all parameters in the group regardless of the filter value. Low traffic (DAX only ships 2 parameters total), not fixed this pass -- file a bd issue if prioritized."
  - "Cluster response omits the newer NetworkType/NodeIdsToRemove top-level fields and SubnetGroup/Subnet SupportedNetworkTypes (dual-stack IPv4/IPv6 networking, added to the DAX API after the original model was written). Not modeled; would require new Cluster/SubnetGroup fields plus backend support for ipv4/ipv6/dual_stack, out of scope for a wire-shape-focused sweep."
  - "TagResource/UntagResource/ListTags accept ARNs for parameter groups and subnet groups (via arnExists), not just clusters. Not verified against real AWS DAX docs whether non-cluster resources are taggable; left as-is (permissive) rather than guessing and narrowing behavior incorrectly."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - dataplane/ (binary DAX client protocol server, separate from the HTTP control-plane API audited here)
leaks: {status: clean, note: "CreateCluster/DeleteCluster/IncreaseReplicationFactor/DecreaseReplicationFactor/RebootNode each spawn a one-shot 1s-delay goroutine to simulate AWS's async state transition; every goroutine re-acquires b.mu, checks the resource still exists/is in the expected transient state, and exits -- no retry loop, no leaked goroutine. CreateCluster/DeleteCluster short-circuit synchronously under DAX_TEST_SYNC=1 for deterministic tests; Increase/Decrease/RebootNode intentionally do NOT (see Notes) and existing tests (TestRebootNodeRecovery) depend on the async path even under DAX_TEST_SYNC=1."}
---

## Notes

**Protocol**: DAX uses `awsjson1.1` (`X-Amz-Target: AmazonDAXV3.<Op>`), confirmed against the
SDK's `awsAwsjson11_*` (de)serializer function names in
`aws-sdk-go-v2/service/dax@v1.29.18/{serializers,deserializers}.go`.

**Bugs found and fixed this pass** (all in `services/dax/handler.go` unless noted):

1. **`clusterResponse.Status` wire key was `"ClusterStatus"`, should be `"Status"`.** The real
   deserializer (`awsAwsjson11_deserializeDocumentCluster`) only recognizes `"Status"`; any other
   key hits its `default: _, _ = key, value` case and is silently discarded, leaving the client's
   `Cluster.Status` `nil` for every Create/Describe/Update/Delete/RebootNode/IncreaseReplicationFactor/
   DecreaseReplicationFactor response. The pre-existing unit test (`TestHandlerCreateCluster`)
   asserted the *wrong* key (`cluster["ClusterStatus"]`) and passed, because it checked the
   handler's own (buggy) output rather than the real wire contract -- classic "unit tests are not
   parity proof" trap. Fixed, and the assertion now checks `"Status"`.

2. **Timestamps emitted as RFC3339 strings instead of epoch-seconds JSON numbers.** DAX's
   awsjson1.1 protocol uses the `unixTimestamp` wire format for `Node.NodeCreateTime` and
   `Event.Date` (confirmed via `smithytime.ParseEpochSeconds` in both deserializers); the real
   client rejects a JSON string here with `"expected TStamp to be a JSON Number, got string
   instead"`. `nodeResponse.NodeCreateTime` and `eventResponse.Date` are now `float64`, populated
   via `pkgs/awstime.Epoch`.

3. **`DescribeEventsInput.StartTime`/`EndTime` request fields were unmarshaled as RFC3339
   strings.** The real client serializes these as epoch-seconds JSON numbers
   (`smithytime.FormatEpochSeconds`), so any real SDK call passing `StartTime`/`EndTime` would fail
   `json.Unmarshal` with "cannot unmarshal number into Go struct field ... of type string" and
   surface as a wrongly-generic `SerializationException` instead of actually filtering events.
   Fixed: request fields are now `json.Number`, decoded via a local `parseEpochSeconds` helper
   (`time.Unix` construction; `pkgs/awstime` only has the encode direction, not decode).

4. **`paramGroupStatus.NodeIDsToReboot` wire key was `"NodeIDsToReboot"`, should be
   `"NodeIdsToReboot"`.** Case-sensitive key mismatch against
   `awsAwsjson11_deserializeDocumentParameterGroupStatus`; the real client would never see the
   pending-reboot node list. Fixed the JSON tag only -- the Go field name keeps the
   `NodeIDsToReboot` initialism spelling (golangci-lint `revive` var-naming requires `IDs`, not
   `Ids`, in Go identifiers; only the wire tag needs to match AWS's casing).

5. **`toClusterResponse` never copied `ParameterGroup.NodeIDsToReboot` into the wire response.**
   The backend (`UpdateParameterGroup` in `backend.go`) correctly computes and stores the
   pending-reboot node list on `Cluster.ParameterGroup.NodeIDsToReboot`, but the handler's
   `paramGroupStatus{...}` literal never read it -- a disguised no-op only visible at the HTTP
   layer (existing test `TestUpdateParameterGroupMarksPendingReboot` called the backend directly
   and never caught this). Fixed by adding the field to the literal; new
   `TestHandlerParameterGroupNodeIdsToRebootWireKey` exercises it end-to-end via `daxRequest`.

6. **`Parameter.ChangeType` value was `"requires-reboot"` (lowercase-hyphen), should be
   `"REQUIRES_REBOOT"`** (`services/dax/backend.go`, `buildParameter`). The real SDK's
   `types.ChangeType` enum only has two values, `"IMMEDIATE"` and `"REQUIRES_REBOOT"`; the
   deserializer stores whatever string arrives verbatim (no server-side validation), so a client
   comparing against `types.ChangeTypeRequiresReboot` would never match. Fixed the constant and
   the corresponding assertion in `backend_parity_test.go`.

**Traps confirmed NOT bugs (checked against the real deserializer, left alone)**:
- `Endpoint{Address,Port,URL}`, `Node{AvailabilityZone,Endpoint,NodeId,NodeStatus,
  ParameterGroupStatus}`, `NotificationConfiguration{TopicArn,TopicStatus}`,
  `SecurityGroupMembership{SecurityGroupIdentifier,Status}`, `SSEDescription{Status}`,
  `Parameter{AllowedValues,DataType,Description,IsModifiable,ParameterName,ParameterType,
  ParameterValue,Source}` (except ChangeType, see bug 6), `SubnetGroup{Description,
  SubnetGroupName,Subnets,VpcId}`, `Subnet{SubnetAvailabilityZone,SubnetIdentifier}`,
  `Tag{Key,Value}`, and all top-level output envelope keys (`Cluster`, `Clusters`, `ParameterGroup`,
  `ParameterGroups`, `SubnetGroup`, `SubnetGroups`, `Parameters`, `Tags`, `Events`, `NextToken`,
  `DeletionMessage`) all match the real serializers/deserializers exactly.
- `EncryptionTypeNone`/`EncryptionTypeTLS` ("NONE"/"TLS") and `IsModifiable` ("TRUE") enum values
  match `types.ClusterEndpointEncryptionType`/`types.IsModifiable` exactly.
- Error mapping (`mapError` in `handler.go`): every sentinel error maps to the AWS-documented fault
  code (`ClusterNotFoundFault`, `ParameterGroupAlreadyExistsFault`, `TagQuotaPerResourceExceeded`,
  etc.) via the `__type` envelope field, which is what `getProtocolErrorInfo` /
  `resolveProtocolErrorType` in the real deserializer read when `X-Amzn-ErrorType` is absent. HTTP
  status is uniformly 400 for client faults / 500 for `InternalFailure`; the real client only
  checks `>= 300` to decide "this is an error", so the exact 4xx code doesn't need to vary per
  fault the way REST protocols do.
- `RebootNode`/`IncreaseReplicationFactor`/`DecreaseReplicationFactor` intentionally do **not**
  short-circuit under `DAX_TEST_SYNC=1` the way `CreateCluster`/`DeleteCluster` do. I initially
  "fixed" this (reading `zz_testmain_test.go`'s doc comment, which claims reboot/replication-factor
  changes are covered) and it broke three real tests, including `TestRebootNodeRecovery` which
  deliberately sleeps 2s to assert the transient `"rebooting"` state is observable immediately
  after the call and recovers asynchronously. Reverted -- the async-only behavior is *more*
  AWS-accurate (a real `RebootNode` response shows the node still transitioning), and the
  `TestMain` comment is simply imprecise about which ops it covers. Left as-is.
