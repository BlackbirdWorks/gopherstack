---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: dms
sdk_module: aws-sdk-go-v2/service/databasemigrationservice@v1.61.8
last_audit_commit: d13e2307f4f1086d83076beb50c1303761fa8369
last_audit_date: 2026-07-12
overall: A            # genuine fixes found: disguised no-ops in the Serverless
                       # Replication family, SubnetGroup Modify, StartRecommendations
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateReplicationInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeReplicationInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReplicationInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects delete while tasks attached"}
  ModifyReplicationInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootReplicationInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "synchronous no-op reboot is correct emulation -- real reboot causes only a momentary outage, no persistent field changes"}
  ApplyPendingMaintenanceAction: {wire: ok, errors: ok, state: partial, persist: n/a, note: "correctly returns ResourcePendingMaintenanceActions (not ReplicationInstance); ApplyAction/OptInType accepted but not tracked -- low value, no actual pending actions ever exist to apply"}
  CreateEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects delete while referenced by a task"}
  ModifyEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  TestConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "records a Connection row, visible via DescribeConnections"}
  DescribeConnections: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "validates source/target endpoint and instance ARNs exist"}
  DescribeReplicationTasks: {wire: ok, errors: ok, state: ok, persist: ok}
  StartReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok}
  StopReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects stop unless currently running"}
  DeleteReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects delete while running"}
  ModifyReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects modify while running"}
  MoveReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReplicationSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- now validates ReplicationSubnetGroupDescription/SubnetIds as required (real API marks both required); SubnetIds accepted but not modeled (no VPC subnet emulation), matching pre-existing convention"}
  DescribeReplicationSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyReplicationSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- was a disguised no-op (looked up and echoed the existing group, discarding ReplicationSubnetGroupDescription entirely); now a real backend.ModifyReplicationSubnetGroup mutates and persists the description"}
  DeleteReplicationSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReplicationConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeReplicationConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyReplicationConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReplicationConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  StartReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- was a total disguised no-op: ignored ReplicationConfigArn/StartReplicationType, never validated the config existed, and returned an empty envelope instead of the real StartReplicationOutput{Replication}. Now validates StartReplicationType enum, rejects unknown config (404) and already-running (400), transitions Status created->running, and returns the wire-accurate Replication shape."}
  StopReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- same disguised-no-op class as StartReplication; now validates config exists and is running, transitions Status running->stopped, returns the Replication shape."}
  DescribeReplications: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- always returned an empty list regardless of any StartReplication ever called. Now backed by the replication-config table (every config has an implicit Replication resource, Status starts 'created'), supports replication-config-arn/replication-config-id filters and Marker pagination."}
  AddTagsToResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTagsFromResource: {wire: ok, errors: ok, state: ok, persist: ok}
  StartRecommendations: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- ignored DatabaseId entirely and never touched backend state (empty envelope was correct per SDK, but the required side effect -- a recommendation later visible via DescribeRecommendations -- never happened). Now validates DatabaseId is required and records a Recommendation via new backend.StartRecommendation."}
  BatchStartRecommendations: {wire: ok, errors: ok, state: ok, persist: ok, note: "seeds a recommendation per source endpoint; pre-existing, unchanged"}
  DescribeRecommendations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "recommendations are runtime-only (not in backendSnapshot); acceptable since Fleet Advisor overall is a low-value, AWS-EOL'd (May 2026) feature surface"}
  CreateDataMigration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDataMigrations: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDataMigration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataMigration: {wire: ok, errors: ok, state: ok, persist: ok}
  StartDataMigration: {wire: ok, errors: ok, state: ok, persist: ok}
  StopDataMigration: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDataProviders: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDataProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEventSubscriptions: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateInstanceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInstanceProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyInstanceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInstanceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMigrationProject: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMigrationProjects: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyMigrationProject: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMigrationProject: {wire: ok, errors: ok, state: ok, persist: ok}
  ImportCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCertificates: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccountAttributes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "quota usage computed live from real counts"}
  DescribeEvents: {wire: partial, errors: ok, state: ok, persist: n/a, note: "events recorded on Endpoint/ReplicationTask create/delete/start/stop, not persisted across restarts -- low value, matches many other services' event-log conventions"}
  DescribeOrderableReplicationInstances: {wire: ok, errors: ok, state: n/a, note: "static reference catalog, matches real AWS class list"}
  DescribeEngineVersions: {wire: ok, errors: ok, state: n/a, note: "static reference catalog"}
  DescribeEndpointTypes: {wire: ok, errors: ok, state: n/a, note: "static reference catalog"}
  DescribeEventCategories: {wire: ok, errors: ok, state: n/a, note: "static reference catalog"}
families:
  fleet-advisor: {status: ok, note: "CreateFleetAdvisorCollector/DeleteFleetAdvisorCollector/DescribeFleetAdvisorCollectors/DescribeFleetAdvisorDatabases/DeleteFleetAdvisorDatabases all mutate/read real backend state and persist. AWS is ending support for Fleet Advisor entirely on 2026-05-20 (already past as of this audit) -- low future value, audited but not deep-dived further."}
  metadata-model: {status: partial, note: "StartMetadataModelAssessment/Conversion/Creation/ExportAsScript/ExportToTarget/Import all real-write to metadataModelRequests (persisted); the corresponding Describe* ops read them back via ListMetadataModelRequests. DescribeMetadataModel itself returns an always-empty {} instead of {Definition, MetadataModelName} -- see gaps."}
  static-reference-data: {status: ok, note: "DescribeOrderableReplicationInstances/DescribeEngineVersions/DescribeEndpointTypes/DescribeEventCategories/DescribeApplicableIndividualAssessments return realistic static catalogs; legitimate for AWS reference-data ops (rule 4: an op with no mutable backend state behind it is not a stub)."}
  reload-tables: {status: partial, note: "ReloadReplicationTables/ReloadTables echo ReplicationTaskArn (matches real void-ish output) but never validate the task ARN exists -- see gaps."}
gaps:
  - "DescribeMetadataModel returns an empty {} instead of the real {Definition, MetadataModelName} shape (low-value SCT-adjacent feature; would require modeling schema-conversion SQL text, not attempted this pass)."
  - "ReloadReplicationTables / ReloadTables do not validate ReplicationTaskArn exists (would 404 on real AWS); currently accept any ARN silently."
  - "CreateEndpoint / ModifyEndpoint do not validate EndpointType (source|target) or EngineName against AWS's enum list; accepts arbitrary strings."
  - "ApplyPendingMaintenanceAction accepts ApplyAction/OptInType but never tracks a real pending-maintenance-action list (DescribePendingMaintenanceActions always returns empty), so apply is a true no-op on top of an always-empty list -- self-consistent but not a full emulation."
deferred:
  - "DescribeMetadataModel{Assessments,Children,Conversions,Creations,ExportsAsScript,ExportsToTarget,Imports} -- read paths for metadata-model family verified at a high level (backed by real metadataModelRequests table) but not exhaustively wire-checked field-by-field against types.go."
  - "Fleet Advisor Lsa Analysis / SchemaObjectSummary / Schemas describe ops -- static/empty responses, not deep-audited (AWS EOL'd 2026-05-20)."
  - "DescribeApplicableIndividualAssessments / DescribeReplicationTaskIndividualAssessments / DescribeReplicationTaskAssessmentResults -- always-empty lists; the assessment-run lifecycle (Start/Cancel/Delete/DescribeReplicationTaskAssessmentRuns) is real and persisted, but the *results* of an assessment run are never populated. Low value: assessments are informational pre-migration checks, not stateful resources Terraform/SDKs poll for correctness."
leaks: {status: clean, note: "no goroutines, janitors, or timers in this service; all state lives in store.Table/store.Index behind the single lockmetrics.RWMutex. leak_test.go / isolation_test.go pre-existing and passing."}
---

## Notes

- **Wire protocol**: `application/x-amz-json-1.1` (awsjson1.1), target prefix
  `AmazonDMSv20160101.<Action>`. All request/response bodies are flat JSON
  objects (no XML), matching `service.WrapOp` handler conventions used
  throughout.

- **Persistence**: every resource collection is registered on `b.registry`
  (see `store_setup.go`), and `Handler.Snapshot`/`Handler.Restore` delegate
  straight to `InMemoryBackend.Snapshot`/`Restore`, which round-trip the
  entire registry via `SnapshotAll`/`RestoreAll` plus `reinitTagsLocked` for
  the backend-owned `Tags` field. This service did **not** have the
  "Handler doesn't expose Snapshot/Restore" bug class found elsewhere in this
  sweep -- persistence wiring was already correct going in.

- **DMS Serverless "Replication" vs "ReplicationConfig" are two distinct AWS
  resources** sharing one ARN: `ReplicationConfig` (from
  CreateReplicationConfig/DescribeReplicationConfigs/ModifyReplicationConfig)
  has no `Status` field on the wire; `Replication` (from
  StartReplication/StopReplication/DescribeReplications) is the runtime
  state and does have `Status`. gopherstack models this as one Go struct
  (`ReplicationConfig` with an added `Status`/`StartReplicationType` pair)
  since a config has at most one associated replication in this emulation --
  but the two JSON shapes (`replicationConfigJSON` vs `replicationJSON`) are
  kept separate so `Status` is never accidentally leaked onto the
  ReplicationConfig wire shape. Don't conflate them when re-auditing.

- **`DescribeReplications` lists every ReplicationConfig, even ones never
  started** (`Status: "created"`), mirroring observed real AWS CLI behavior
  where the Replication runtime resource exists implicitly the moment its
  config is created. Do not "fix" this to filter out never-started configs
  without re-verifying against real AWS -- this was a deliberate design
  choice this pass, not an oversight.

- **RebootReplicationInstance is correctly a state no-op.** Real AWS reboot
  causes "a momentary outage" but no persistent field changes once complete;
  gopherstack's synchronous emulation (return the instance unchanged) matches
  the post-reboot steady state. Don't flag this as a disguised no-op again
  without a concrete field AWS actually changes.

- **Void-envelope ops that are legitimately empty** (verified against
  `types.go`/`api_op_*.go` this pass, not just assumed): `AddTagsToResource`,
  `RemoveTagsFromResource`, `DeleteFleetAdvisorCollector`,
  `DeleteReplicationSubnetGroup`, `StartRecommendations`. Each of these
  really does call into real backend state before returning `{}` -- this was
  double-checked, not just grepped.

- **Fleet Advisor and Schema Conversion (metadata-model) op families are
  low future value**: the AWS SDK source (`api_op_StartRecommendations.go`
  et al.) carries an explicit end-of-support notice for Fleet Advisor dated
  2026-05-20, which has already passed as of this audit (2026-07-12). Future
  audit passes should deprioritize these families relative to the core
  ReplicationInstance/Endpoint/ReplicationTask/SubnetGroup/ReplicationConfig
  surface.
