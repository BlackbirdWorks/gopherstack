---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: dax
sdk_module: aws-sdk-go-v2/service/dax@v1.32.4   # awsjson1.1 protocol, target prefix AmazonDAXV3.
last_audit_commit: 70eea523b6f0   # unchanged: this pass's changes are not yet committed by this agent
last_audit_date: 2026-08-10
overall: A            # 2026-07-24: follow-up pass: closed all 3 previously-known gaps, killed both banned nolints
                      # 2026-07-31: pkgs/sdkcheck reverse check found ResetParameterGroup wrongly advertised/documented as a real SDK op (it isn't -- see its ops-block note); corrected, route left wired as internal test scaffolding. Grade held at A: unreachable by real traffic either way, since DAX dispatches purely by X-Amz-Target and no real client can send this target.
                      # 2026-08-10: control-plane sweep (gopherstack-mmqd). Fixed state-mutated-before-validation in UpdateCluster and UpdateParameterGroup, a wrong error fault code on 6 required-field checks, a fabricated Tags field on the Cluster wire response, 3 unvalidated @required fields (TagResource.Tags, UntagResource.TagKeys, UpdateParameterGroup.ParameterNameValues), and a missing per-subnet SupportedNetworkTypes field. See Notes.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "IamRoleArn-required check now uses InvalidParameterValueException, not InvalidARNFault -- fixed 2026-08-10, see Notes"}
  DescribeClusters: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "no longer mutates Description/PreferredMaintenanceWindow/SecurityGroupIDs before validating ParameterGroupName exists; ClusterName-required check now uses InvalidParameterValueException, not InvalidARNFault -- both fixed 2026-08-10, see Notes"}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "ClusterName-required check now uses InvalidParameterValueException, not InvalidARNFault -- fixed 2026-08-10, see Notes"}
  IncreaseReplicationFactor: {wire: ok, errors: ok, state: ok, persist: ok, note: "ClusterName-required check now uses InvalidParameterValueException, not InvalidARNFault -- fixed 2026-08-10, see Notes"}
  DecreaseReplicationFactor: {wire: ok, errors: ok, state: ok, persist: ok, note: "ClusterName-required check now uses InvalidParameterValueException, not InvalidARNFault -- fixed 2026-08-10, see Notes"}
  RebootNode: {wire: ok, errors: ok, state: ok, persist: ok, note: async recovery is intentional -- matches real AWS's transient "rebooting" status, see Notes. ClusterName-required check now uses InvalidParameterValueException, not InvalidARNFault -- fixed 2026-08-10.}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags is now enforced as @required (rejects a request that omits the field entirely) -- fixed 2026-08-10, see Notes"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "TagKeys is now enforced as @required -- fixed 2026-08-10, see Notes"}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "no longer commits earlier-validated entries of a ParameterNameValues batch when a later entry is invalid (validate-then-apply, two passes); ParameterNameValues is now enforced as @required -- both fixed 2026-08-10, see Notes"}
  DeleteParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeParameters: {wire: ok, errors: ok, state: ok, persist: ok, note: Source filter (user/system) now applied -- fixed 2026-07-24, see Notes}
  DescribeDefaultParameters: {wire: ok, errors: ok, state: ok, persist: n/a}
  # ResetParameterGroup is intentionally NOT listed as an advertised SDK op
  # here. 2026-07-31 CORRECTION: the row that used to live at this position
  # ("wire: ok, ...") was inaccurate -- ResetParameterGroup is not a real AWS
  # DAX SDK operation at all (verified against botocore's dax
  # service-2.json: no such action exists in the 2017-04-19 model; the real
  # op list has no reset-to-defaults call for parameter groups). Caught by
  # pkgs/sdkcheck's reverse check (commit 12cfe14d5; gopherstack-vhw2
  # category A). DAX dispatches purely by X-Amz-Target header value through
  # the daxOperations table, so a real client can never send this target and
  # this route was already unreachable by real traffic; it stays wired as
  # internal test scaffolding, unadvertised. See handler.go's comment on the
  # GetSupportedOperations() entry. Same resolution as EMR's
  # ListTagsForResource and CloudFront's
  # GetFunctionAssociations/SetFunctionAssociations.
  CreateSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "each Subnet in the response now carries its own SupportedNetworkTypes field (types.Subnet has one distinct from SubnetGroup's) -- fixed 2026-08-10, see Notes"}
  DescribeSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEvents: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  cluster-lifecycle: {status: ok, note: "CreateCluster/DescribeClusters/UpdateCluster/DeleteCluster/IncreaseReplicationFactor/DecreaseReplicationFactor/RebootNode all mutate the real store.Table[Cluster], persist via backendSnapshot, and now emit the correct wire shape (Status key, epoch timestamps) -- see gaps for the 5 bugs found and fixed this pass."
  tags: {status: ok, note: "TagResource/UntagResource/ListTags mutate the ARN-keyed tags map and propagate cluster ARNs to Cluster.Tags; quota (50) and key/value length enforcement match AWS constraints. arnExists now recognizes cluster ARNs only (fixed 2026-07-24, see Notes) -- real DAX has no Arn field on ParameterGroup/SubnetGroup, so those were never taggable."}
  parameter-groups: {status: ok, note: "CreateParameterGroup/DescribeParameterGroups/UpdateParameterGroup/DeleteParameterGroup/DescribeParameters/DescribeDefaultParameters all real; UpdateParameterGroup correctly cascades pending-reboot + NodeIdsToReboot to dependent clusters; DescribeParameters now honors the request's Source filter (fixed 2026-07-24). ResetParameterGroup is NOT a real DAX op (see its ops-block note, corrected 2026-07-31) -- kept wired as internal test scaffolding only, unreachable by real clients."
  subnet-groups: {status: ok, note: "CreateSubnetGroup/DescribeSubnetGroups/UpdateSubnetGroup/DeleteSubnetGroup real; in-use protection (blocks delete while referenced by a cluster) verified. SupportedNetworkTypes now modeled (always [\"ipv4\"], fixed 2026-07-24, see Notes)."}
  events: {status: ok, note: "DescribeEvents ring buffer (1000 cap) is real; StartTime/EndTime/SourceName/SourceType filtering verified after fixing the epoch-seconds request-parsing bug."}
  dataplane: {status: deferred, note: "Binary DAX client protocol (services/dax/dataplane/) is a separate, extensively self-tested subsystem (936-line dataplane_integration_test.go + dataplane/*_test.go) not covered by this control-plane wire-shape sweep. Not audited this pass -- different reference material (aws-dax-go's binary encoding, not aws-sdk-go-v2/service/dax) would be needed."}
gaps: []                  # known divergences NOT fixed — link bd issue ids; all 3 prior gaps closed this pass
items_still_open:
  - "InsufficientClusterCapacityFault / ServiceLinkedRoleNotFoundFault (types.InsufficientClusterCapacityFault, types.ServiceLinkedRoleNotFoundFault) are real CreateCluster error types not modeled. Reason: both are account/infrastructure-state faults (missing DAX service-linked role; opportunistic hardware capacity shortage) with no deterministic, request-shape-driven trigger condition -- gopherstack tracks neither IAM service-linked-role state nor a hardware capacity pool. Inventing an arbitrary trigger (e.g. erroring above some ReplicationFactor) would itself be exactly the kind of fabricated, non-AWS-accurate behavior this audit exists to prevent. Left unmodeled; would need a deliberate design decision (e.g. a backend flag simulating SLR presence) before implementing."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - dataplane/ (binary DAX client protocol server, separate from the HTTP control-plane API audited here)
leaks: {status: clean, note: "CreateCluster/DeleteCluster/IncreaseReplicationFactor/DecreaseReplicationFactor/RebootNode each spawn a one-shot 1s-delay goroutine to simulate AWS's async state transition; every goroutine re-acquires b.mu, checks the resource still exists/is in the expected transient state, and exits -- no retry loop, no leaked goroutine. CreateCluster/DeleteCluster short-circuit synchronously under DAX_TEST_SYNC=1 for deterministic tests; Increase/Decrease/RebootNode intentionally do NOT (see Notes) and existing tests (TestRebootNodeRecovery) depend on the async path even under DAX_TEST_SYNC=1. DecreaseReplicationFactor's async goroutine now also clears the transient Cluster.NodeIDsToRemove list it sets (2026-07-24) -- verified no residual state after recovery via TestDecreaseReplicationFactorNodeIDsToRemoveClearsOnRecovery."}
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

## 2026-07-24 follow-up pass

Closed all 3 gaps left open by the 2026-07-12 audit, and removed both banned `cyclop` nolints
by decomposing to data-driven tables (field-diffed against
`aws-sdk-go-v2/service/dax@v1.29.18`, module downloaded read-only to the local mod cache for
diffing; `go.mod`/`go.sum` left untouched):

1. **`DescribeParameters` now honors the request's `Source` filter.** Confirmed against
   `types.DescribeParametersInput.Source` (`*string`, free text, doc example: `"system denotes
   a system-defined parameter"`) -- the real field has no enum; gopherstack's backend only ever
   produces `"user"`/`"system"` (never `"engine-default"`), so filtering on those two values is
   the correct, non-invented behavior. Wired through `InMemoryBackend.DescribeParameters`'s new
   `sourceFilter` parameter and the handler's `Source` request field.

2. **`Cluster.NetworkType`, `Cluster.NodeIdsToRemove`, and `SubnetGroup.SupportedNetworkTypes`
   are now modeled**, field-diffed against `types.Cluster`/`types.SubnetGroup`/
   `types.CreateClusterInput` and their wire keys confirmed in `serializers.go`/`deserializers.go`
   (`"NetworkType"`, `"NodeIdsToRemove"`, `"SupportedNetworkTypes"` all match exactly).
   - `NetworkType`: `CreateClusterInput` accepts `ipv4`/`ipv6`/`dual_stack`
     (`ErrInvalidParameterValue` on anything else), defaulting to `ipv4` when omitted --
     gopherstack subnet groups are always IPv4-only (no per-subnet CIDR/IP-family modeling), so
     `ipv4` is the only *correct* derived default; `UpdateClusterInput` does **not** have a
     `NetworkType` field in the real SDK, so it is create-only, matching AWS.
   - `NodeIdsToRemove`: transient, mirroring `NodeIdsToReboot`'s existing pattern -- populated on
     `Cluster` by `DecreaseReplicationFactor` with the node IDs that operation is removing (either
     the caller's explicit `NodeIDsToRemove` or the trailing nodes when unspecified), and cleared
     by the same 1s-delay async goroutine that already exists to flip `Status` back to
     `"available"`. No new goroutine, no new leak surface.
   - `SupportedNetworkTypes`: `SubnetGroup` always reports `["ipv4"]` (`NetworkTypeIPv4`) -- honest
     given gopherstack subnets have no real IP-family data to derive from; not fabricated as a
     configurable input since `CreateSubnetGroupInput`/`UpdateSubnetGroupInput` have no matching
     field in the real SDK either.

3. **Deleted the gopherstack-invented "parameter groups and subnet groups are taggable" behavior.**
   Confirmed by field-diffing `types.ParameterGroup`/`types.SubnetGroup` against `types.Cluster`:
   only `Cluster` has an `Arn`/`ClusterArn` field in the real SDK. `TagResource`/`UntagResource`/
   `ListTags` are documented as cluster-only operations for exactly this reason -- there is no ARN
   to tag on the other two resource types. `arnExists` (`tags.go`) no longer recognizes
   `parametergroup/`/`subnetgroup/` ARN prefixes; `TestTagResource`'s two corresponding subtests
   were converted from "tag succeeds" to "rejected as not found" (they were asserting invented
   behavior, not real AWS behavior).

4. **Both banned `//nolint:cyclop` uses in `handler.go` removed by decomposition, not suppression:**
   - `dispatch`'s 22-case operation switch became a `map[string]daxOpHandler` lookup
     (`daxOperations`) built from method expressions (`(*Handler).handleCreateCluster`, ...);
     `dispatch` itself is now a two-line map lookup.
   - `mapError`'s 20-case error-mapping switch became an ordered `[]errCodeMapping` table
     (`daxErrCodeMappings`) iterated with a single `errors.Is` loop. Ordering (specific sentinels
     before their generic `awserr.ErrNotFound`/`ErrConflict`/`ErrInvalidParameter` parents) is
     preserved exactly, since `errors.Is` still short-circuits on the first match and specific
     entries are listed first.
   - Both new tables are `gochecknoglobals`-exempted the same way `models.go`'s existing lookup
     tables are (package-level lookup table, immutable after init).

## 2026-08-10 control-plane sweep (gopherstack-mmqd)

Confirmed both recorded follow-up items are honest and out of scope, then swept the control
plane for the bug classes found repeatedly elsewhere in today's campaign (state mutated before
validation, wrong/missing required-field errors, fabricated or missing wire fields, allowlists
vs. SDK enums). All errors cited below are from botocore's `dax` `service-2.json`
(`2017-04-19`, matching the pinned `aws-sdk-go-v2/service/dax@v1.32.4`'s wire behavior) and that
SDK's generated `validators.go`/`deserializers.go`.

**Confirmed, not re-litigated:**

- `InsufficientClusterCapacityFault`/`ServiceLinkedRoleNotFoundFault`: both genuinely modeled as
  `strings.EqualFold`-matched error codes in the pinned SDK's deserializers (`deserializers.go`
  lines 127-128, 154-155 and throughout every cluster-mutating op's error switch) with no
  request-shape-driven trigger gopherstack could honestly derive -- left unmodeled, as recorded.
- `services/dax/dataplane/` is ~6900 LOC (`find services/dax/dataplane -name '*.go' | xargs wc -l`)
  implementing aws-dax-go's binary client protocol, a wholly separate wire format from the JSON
  control plane audited here -- confirmed out of scope, not touched.

**Bugs found and fixed this pass** (all backend/handler files under `services/dax/` unless noted):

1. **`UpdateCluster` mutated `Description`/`PreferredMaintenanceWindow`/`SecurityGroupIDs`
   directly on the live `*Cluster` (from `store.Table.Get`, not a copy) before validating that
   `ParameterGroupName` refers to an existing parameter group.** A request with a valid
   `PreferredMaintenanceWindow` and a bogus `ParameterGroupName` returned an error but still
   committed the maintenance-window change -- the caller's error implies nothing changed, but it
   did. Fixed by moving the `ParameterGroupName` existence check before any field is written
   (`clusters.go`). `TestUpdateClusterRejectedRequestDoesNotMutateState` fails on the pre-fix code
   with `PreferredMaintenanceWindow` observably changed after the rejected call.

2. **`UpdateParameterGroup` validated and wrote each `ParameterNameValues` entry in the same loop
   iteration**, so a 2-entry batch where entry 1 is valid and entry 2 is invalid committed entry 1
   to the live `ParameterGroup.Parameters` map before returning the error for entry 2. AWS rejects
   the whole request atomically. Fixed with a validate-then-apply split: `validateParameterNameValues`
   checks every entry first; only if all pass does a second loop write them (`parameter_groups.go`).
   `TestUpdateParameterGroupRejectedBatchDoesNotPartiallyApply` fails on the pre-fix code with
   `query-ttl-millis` observably changed despite the batch being rejected.

3. **Six required-field checks (`ClusterName` on `UpdateCluster`/`DeleteCluster`/
   `IncreaseReplicationFactor`/`DecreaseReplicationFactor`/`RebootNode`, `IamRoleArn` on
   `CreateCluster`) returned `ErrInvalidARN` (`InvalidARNFault`), a fault botocore declares only
   for `TagResource`/`UntagResource`/`ListTags`'s ARN-shaped `ResourceName` parameter** -- never
   for any of these six operations, all of which do declare `InvalidParameterValueException`. A
   real client checking for a specific typed fault (`errors.As(err, &types.ClusterNotFoundFault{})`
   -style) on one of these ops would see an untyped `smithy.GenericAPIError` instead of the
   documented error family. Fixed by switching all six sites to `ErrInvalidParameterValue`
   (`clusters.go`); `tags.go`'s three legitimate `ErrInvalidARN` uses (all on `ResourceName`) are
   untouched. `TestClusterOpsRequiredFieldErrorCode` fails on the pre-fix code for all six.

4. **`Cluster`'s wire response (`clusterResponse` in `handler_clusters.go`) serialized a `Tags`
   field that does not exist on the real `types.Cluster` shape.** Confirmed against botocore's
   `Cluster` shape (`ClusterName, Description, ClusterArn, TotalNodes, ActiveNodes, NodeType,
   Status, ClusterDiscoveryEndpoint, NodeIdsToRemove, Nodes, PreferredMaintenanceWindow,
   NotificationConfiguration, SubnetGroup, SecurityGroups, IamRoleArn, ParameterGroup,
   SSEDescription, ClusterEndpointEncryptionType, NetworkType` -- no `Tags` member) and the real
   deserializer (`awsAwsjson11_deserializeDocumentCluster`, which has no `"Tags"` case and would
   silently discard it). Harmless to a real client but not what AWS returns; tags are only ever
   readable via `ListTags`. Removed the field from the wire struct only -- the backend
   `Cluster.Tags` map is untouched (still the persistence/tag-propagation source of truth).
   `TestHandlerClusterResponseHasNoTagsField` fails on the pre-fix code; the pre-existing
   `TestHandlerCreateClusterTagsAsArray` asserted the fabricated field and was rewritten to verify
   tag storage via `ListTags` instead (the "unit tests are not parity proof" trap -- that test had
   locked in the bug it should have caught).

5. **Three `@required` fields were never validated, so omitting them from the request silently
   succeeded as a no-op** instead of being rejected: `TagResource.Tags` (`validators.go:603`),
   `UntagResource.TagKeys` (`validators.go:621`), and `UpdateParameterGroup.ParameterNameValues`
   (`validators.go:654`). All three check `nil`, not emptiness (matching the SDK client validator's
   own semantics: a present-but-empty array satisfies `@required`, only a wholly absent field
   doesn't), so each handler now checks the raw unmarshaled slice for `nil` before conversion --
   `UpdateParameterGroup`'s handler previously always allocated a non-nil `pvs` via `make(...)`
   regardless of whether the field was present, destroying the distinction before it reached the
   backend; fixed to only allocate when the source field is non-nil. Both `TestHandlerTagResourceRequiresTags`
   and `TestHandlerUntagResourceRequiresTagKeys` and `TestHandlerUpdateParameterGroupRequiresParameterNameValues`
   fail on the pre-fix code.

6. **`SubnetGroup.Subnets` entries had no per-subnet `SupportedNetworkTypes` field.** botocore's
   `Subnet` shape has its own `SupportedNetworkTypes` member (`NetworkTypeList`), distinct from
   `SubnetGroup`'s group-level field of the same name -- gopherstack modeled only the latter. Added
   `SubnetEntry.SupportedNetworkTypes` (always `["ipv4"]`, same honest derivation as the
   group-level field: gopherstack has no per-subnet CIDR/IP-family data), threaded through
   `subnetEntriesFromIDs` and the default subnet group seed. Additive `omitempty` field on an
   existing persisted type; `TestInMemoryBackend_SnapshotRestore_FullState` now asserts it
   round-trips. `TestHandlerSubnetGroupSubnetHasSupportedNetworkTypes` fails on the pre-fix code.

**Checked and found clean (no fix needed):**

- **Allowlist vs. SDK enum**: `ClusterEndpointEncryptionType` (`NONE`/`TLS`), `NetworkType`
  (`ipv4`/`ipv6`/`dual_stack`), `IsModifiable`, `ParameterType`, `SourceType`, `SSEStatus` in
  `types/enums.go` all match gopherstack's constants exactly -- no drift. `NodeType` is a free-text
  `*string` in the SDK (botocore shape `String`, no enum), not a smithy enum at all, so
  `validNodeTypes`'s 16-entry allowlist cannot be checked against an SDK-declared value set;
  left as-is rather than fabricating a source of truth that doesn't exist.
- **Tagging a nonexistent ARN**: `TagResource`/`UntagResource`/`ListTags` all call `arnExists`
  before any mutation; already fixed in the 2026-07-24 pass (parameter/subnet groups are
  correctly untaggable, matching the real SDK having no `Arn` field on those types).
- **IncreaseReplicationFactor/DecreaseReplicationFactor/RebootNode**: re-checked for the same
  mutate-before-validate pattern as bug 1 -- all three complete every validation (status check,
  factor bounds, `NodeIDsToRemove` existence/count via `removeSpecificNodes`, which returns a new
  slice rather than mutating in place) before touching the live cluster. Clean.
- **Lifecycle/status polling**: synchronous `CreateCluster`/`DeleteCluster` (`DAX_TEST_SYNC=1`)
  and the async 1s-delay paths both leave `DescribeClusters` reporting a status
  (`"available"`/`"creating"`/`"deleting"`/`"modifying"`/`"rebooting"`) a real client polling for
  `"available"` would accept; `DeleteCluster`'s async goroutine fully removes the cluster from
  `b.clusters` (subsequent `DescribeClusters` returns `ClusterNotFoundFault`, not a lingering
  `"deleting"` row) and cleans up `b.tags`. No live-status-after-deletion gap found.
- **DeleteParameterGroup/DeleteSubnetGroup**: both refuse deletion outright while referenced by a
  cluster (`ErrParameterGroupInUse`/`ErrSubnetGroupInUse`), so there is no path to a deleted
  resource still reporting a live status.
