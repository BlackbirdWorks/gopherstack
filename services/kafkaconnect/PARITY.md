---
service: kafkaconnect
sdk_module: aws-sdk-go-v2/service/kafkaconnect@v1.39.1
last_audit_commit: 5bb0d02ee
last_audit_date: 2026-10-05
overall: A            # A: SDK-driven test/integration suite TestIntegration_KafkaConnect_ConnectorLifecycle (test/integration/kafkaconnect_test.go) + every buildable gap closed; remaining divergences are structural_gaps.
ops:
  CreateConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-10-05: CREATING, lazily RUNNING after 500ms; referenced custom plugin and worker configuration ARNs must exist (NotFoundException)"}
  DescribeConnector: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConnectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "connectorNamePrefix filter; opaque nextToken via pkgs/page"}
  UpdateConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "currentVersion optimistic lock; exactly one of capacity/connectorConfiguration; connector goes UPDATING and the recorded ConnectorOperation UPDATE_IN_PROGRESS, both settling after 500ms; ConflictException while DELETING"}
  DeleteConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETING, lazily removed after 300ms (its operations go with it); a second delete is idempotent"}
  RestartConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "connector goes RESTARTING; records a RESTART_CONNECTOR operation RESTART_IN_PROGRESS then RESTART_COMPLETE after 500ms; ConflictException while DELETING"}
  DescribeConnectorOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConnectorOperations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomPlugin: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING, lazily ACTIVE after 500ms; with S3 wired, FileMd5/FileSize are read from the real object and a missing object ends CREATE_FAILED with a StateDescription message"}
  DescribeCustomPlugin: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCustomPlugins: {wire: ok, errors: ok, state: ok, persist: ok, note: "namePrefix filter"}
  DeleteCustomPlugin: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETING, lazily removed after 300ms"}
  CreateWorkerConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "propertiesFileContent stored/echoed as given (base64), always revision 1"}
  DescribeWorkerConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWorkerConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "namePrefix filter"}
  DeleteWorkerConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETING, lazily removed after 300ms; creation is ACTIVE immediately (the API has no creating state)"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "connector, custom plugin, or worker configuration by ARN"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Connector: {status: ok, note: "Create/Describe/List/Update/Delete/Restart verified end-to-end against the real aws-sdk-go-v2 client over an httptest server -- wire shapes, ISO8601 timestamps, ARN format, CurrentVersion optimistic locking, and error deserialization (BadRequestException/ConflictException/NotFoundException) all round-trip cleanly."}
  ConnectorOperation: {status: ok, note: "UpdateConnector and RestartConnector each record a real ConnectorOperation (UPDATE_CONNECTOR_CONFIGURATION/UPDATE_WORKER_SETTING/RESTART_CONNECTOR, *_IN_PROGRESS then *_COMPLETE with an endTime when the connector settles) retrievable via DescribeConnectorOperation/ListConnectorOperations."}
  CustomPlugin: {status: ok, note: "Create/Describe/List/Delete round-trip contentType, S3 location, and a file checksum/size read from the emulated S3 object (derived only when S3 is not wired). Plugin revisions beyond 1 and UpdateCustomPlugin are not part of the surface this pass implements."}
  WorkerConfiguration: {status: ok, note: "Create/Describe/List/Delete round-trip name/description/propertiesFileContent. Always revision 1: UpdateWorkerConfiguration (which would create later revisions) is not part of the AWS API."}
  Tags: {status: ok, note: "One generic tag family keyed by ARN across all three resource kinds, matching real AWS."}
gaps: []
items_still_open: []
structural_gaps:
  - "No Kafka Connect runtime: connector tasks never run, there is no worker fleet or autoscaling metric source, and MSK bootstrap-broker / VPC reachability, connector.class resolution and plugin-archive validity are never exercised. AWS surfaces those failures asynchronously as FAILED states whose StateDescription code vocabulary is not published, so connectors here never reach FAILED."
  - "A custom plugin whose S3 object is absent ends CREATE_FAILED with a message-only StateDescription (the code vocabulary is unpublished); archive contents are not validated as a real ZIP/JAR."
  - "UpdateCustomPlugin / UpdateWorkerConfiguration do not exist in the pinned SDK (v1.39.1), so plugin and worker-configuration revisions stay at 1."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; lifecycle transitions are lazy deadlines evaluated under the backend lock, no goroutines or timers."}
---

## Notes

Initial implementation (2026-09-25): control-plane REST-JSON API modeled after
services/kinesisvideo. Every operation mutates/reads real in-memory state via
pkgs/store.Table + pkgs/lockmetrics.RWMutex, with JSON snapshot/restore wired
into pkgs/persistence. Wire shapes, HTTP methods/paths (path-parameter REST,
not action-per-path like kinesisvideo), and error codes were verified against
the pinned aws-sdk-go-v2/service/kafkaconnect v1.39.1 serializers.go/
deserializers.go. Timestamps are ISO8601 strings (smithy date-time), unlike
kinesisvideo's epoch-seconds numbers -- confirmed via deserializers.go's
smithytime.ParseDateTime calls. The shared /v1/tags/{resourceArn} path (also
claimed by services/kafka, batch, appsync, mq, codeartifact, pinpoint) is
guarded by decoding the ARN's own service field, the same pattern
services/kafka uses for its own /v1/tags/{arn} claim, per
.claude/memories -- route-matcher-prefix-collision.

## 2026-10-04 (gopherstack-jrfzw multi-region)

kafkaconnect is region-isolated: connectors, custom plugins and worker configurations live per region. It does not call MSK, so there is no cluster lookup to route. Per-region sibling handlers via `pkgs/regionpeers`; snapshots gain an additive `regions` key only when a sibling exists (no version bump; older snapshots restore). `NewHandler` alone stays single-region. Proof: `TestHandler_MultiRegionIsolation`, `TestHandler_MultiRegionPersistence`, `TestRegionIsolation/kafkaconnect`. Limitation: the dashboard shows the home region only. CloudFormation provisions it in the stack's region. `TestHandler_MultiRegionReset` covers Reset.

### 2026-10-05: A grade, lifecycle and S3 coupling

Grade moves B to A on `TestIntegration_KafkaConnect_ConnectorLifecycle` (test/integration/kafkaconnect_test.go: S3 object, plugin, worker configuration, connector, update, restart, tags, delete through the real SDK client against the Docker image) plus closing the buildable gaps the B grade named.

- Real state transitions (`settleLocked`, lifecycle.go), evaluated lazily under the backend lock with no goroutines: connectors CREATING to RUNNING, UPDATING and RESTARTING back to RUNNING (completing the recorded ConnectorOperation with an endTime), DELETING to removed; custom plugins CREATING to ACTIVE and DELETING to removed; worker configurations DELETING to removed. CreateConnector now rejects an unknown custom plugin or worker configuration ARN (NotFoundException) and Update/Restart on a DELETING connector return ConflictException; both choices are within each op's documented error set, AWS publishes no per-case table. Covered by `TestConnectorLifecycleTransitions`, `TestConnectorDeleteMutationsConflict`, `TestCreateConnectorRejectsUnknownReferences`, `TestCustomPluginSettlesActive` (lifecycle_test.go).
- Custom plugins read their S3 object through the emulated S3 backend (cross_service.go, resolved lazily through the app config like services/grafana): FileMd5 and FileSize are the real MD5 and length, a missing object ends CREATE_FAILED. Covered by `TestCustomPluginReadsEmulatedS3` (plugin_s3_test.go). When S3 is not wired the old derived values remain.
- Persistence is additive: `PendingUntil` on connectors, plugins and worker configurations, `FailureMessage` on plugins (snapshot inventory rows added, no version bump).
- The MSK cluster named in a connector is still taken as opaque input: real AWS never validates it at API time (failures arrive later as FAILED), and no Connect runtime exists to produce them; recorded under structural_gaps.

## 2026-10-05 (gopherstack-uox6 pass 11, value semantics)

CreateConnector defaults NetworkType to IPV4 (api_op_CreateConnector.go:92). Proof: `TestCreateConnector_NetworkTypeDefault`. Recorded, unchanged: RestartConnector ignores OnlyFailedTasks because no per-task state is modeled; Update capacity/configuration is one-of full replacement.
