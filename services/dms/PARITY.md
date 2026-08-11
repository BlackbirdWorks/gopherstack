---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: dms
sdk_module: aws-sdk-go-v2/service/databasemigrationservice@v1.66.4
last_audit_commit: d13e2307f4f1086d83076beb50c1303761fa8369
last_audit_date: 2026-07-31
overall: A            # 2026-07-23 pass: closed all 4 gaps + all 3 deferred
                       # families from the prior audit (DescribeMetadataModel
                       # shape, ReloadTables/ReloadReplicationTables state
                       # validation + wire-field-name bug, Endpoint enum
                       # validation, ApplyPendingMaintenanceAction enum
                       # validation, the whole metadata-model Describe/Cancel/
                       # GetTargetSelectionRules wire-shape family, Fleet
                       # Advisor Lsa/SchemaObjectSummary/Schemas field-diff,
                       # and a real premigration-assessment-run state machine
                       # replacing always-empty individual-assessment/result
                       # lists). Also fixed a genuine epoch-timestamp bug
                       # (InstanceCreateTime/ReplicationTaskCreationDate were
                       # missing from the wire entirely).
                       #
                       # 2026-07-31 correction: that A rating was overstated.
                       # A dashboard sweep found 5 real field-level wire-shape
                       # bugs the 07-23 pass's "wire: ok" marks missed on
                       # EventSubscription, ReplicationSubnetGroup,
                       # Certificate, Endpoint, and DescribeConnections (see
                       # per-op notes below) -- all field-diffed against
                       # aws-sdk-go-v2/service/databasemigrationservice
                       # models.go directly this pass, not assumed. All 5 are
                       # now fixed, each with a test that fails against the
                       # pre-fix code and passes after. Re-graded A only
                       # because the fixes are landing in the same pass as
                       # this correction -- the prior "no phantoms" A claim
                       # about the op list itself remains true and unaffected
                       # by this correction (these were field-shape bugs
                       # within existing ok ops, not phantom/missing ops).
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateReplicationInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- InstanceCreateTime was entirely missing from the wire response (epoch-seconds bug class); now emitted via pkgs/awstime.Epoch"}
  DescribeReplicationInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReplicationInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects delete while tasks attached"}
  ModifyReplicationInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootReplicationInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "synchronous no-op reboot is correct emulation -- real reboot causes only a momentary outage, no persistent field changes"}
  ApplyPendingMaintenanceAction: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass -- ApplyAction/OptInType previously accepted arbitrary strings; now validated against the SDK's documented valid-values lists (os-upgrade|system-update|db-upgrade|os-patch and immediate|next-maintenance|undo-opt-in), 400 ValidationException otherwise. Still correctly returns an empty PendingMaintenanceActionDetails -- no pending-maintenance-action producer exists in this emulation, matching a freshly-created instance's real state."}
  CreateEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "EndpointType/EngineName validated against types.ReplicationEndpointTypeValue and the documented EngineName valid-values list. FIXED 2026-07-31 -- Password was accepted in the request but silently dropped (never stored, never usable); now stored on Endpoint.Password and never put on the wire (matching the real Endpoint type, which has no Password field -- AWS never echoes credentials back). FIXED 2026-08-10 (gopherstack-z79q) -- CreateEndpointInput/ModifyEndpointInput's 19 heterogeneous engine-specific settings structs (MySQLSettings/PostgreSQLSettings/S3Settings/OracleSettings/... totaling ~300 fields) were being silently dropped by encoding/json instead of modeled. Judgment: modeling all ~300 fields faithfully (validated types, stored, echoed on Describe, persisted) is not achievable in one pass, and a partial subset would be worse than the honest gap (a client seeing some settings preserved would reasonably assume the rest are too). Per the no-stub rule, the drop is now made visible instead: any request that sets one of the 19 settings fields is rejected with 400 ValidationException naming the field, matching the sagemaker PipelineDefinitionS3Location / cloudformation AccountFilterType precedent for explicitly-rejected-rather-than-silently-dropped fields. See engineSettingsFields in handler_endpoints.go."}
  DescribeEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects delete while referenced by a task"}
  ModifyEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "EndpointType/EngineName accepted on Modify, validated with the same enum check as Create, and applied. FIXED 2026-07-31 -- same Password fix as CreateEndpoint above. FIXED 2026-08-10 (gopherstack-z79q) -- same engine-settings explicit-rejection fix as CreateEndpoint above"}
  TestConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "records a Connection row, visible via DescribeConnections"}
  DescribeConnections: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-07-31 -- never called dmsPaginate or set Marker on the response, unlike every other Describe op in this service, so MaxRecords/Marker were silently ignored; now paginated like its siblings"}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "validates source/target endpoint and instance ARNs exist. FIXED this pass -- ReplicationTaskCreationDate was entirely missing from the wire response (epoch-seconds bug class); now emitted via pkgs/awstime.Epoch"}
  DescribeReplicationTasks: {wire: ok, errors: ok, state: ok, persist: ok}
  StartReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok}
  StopReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects stop unless currently running"}
  DeleteReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects delete while running"}
  ModifyReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects modify while running"}
  MoveReplicationTask: {wire: ok, errors: ok, state: ok, persist: ok}
  ReloadTables: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass -- was a disguised no-op that echoed ReplicationTaskArn without validating anything; now requires TablesToReload, validates ReloadOption enum, 404s on an unknown task, and 400 InvalidResourceStateFault unless the task is currently RUNNING (matches the SDK doc: 'You can only use this operation with a task in the RUNNING state')"}
  ReloadReplicationTables: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED this pass -- two bugs: (1) the request field was wrongly named ReplicationTaskArn instead of the real ReplicationConfigArn, silently discarding the client's ARN; (2) it never validated anything. Now requires TablesToReload, validates ReloadOption, 404s on an unknown replication config, and 400s unless the associated Replication is RUNNING"}
  DescribeReplicationTableStatistics: {wire: ok, errors: ok, state: partial, persist: n/a, note: "FIXED 2026-08-11 -- request/response fields were copy-pasted from the sibling DescribeTableStatistics op (ReplicationTaskArn/TableStatistics) instead of this op's real fields (ReplicationConfigArn/ReplicationTableStatistics); the wrong request field meant the config ARN was silently discarded and the handler queried an arbitrary replication task instead. Now validates the config exists (404 if not) and echoes ReplicationConfigArn. Always returns an empty ReplicationTableStatistics list -- ReplicationConfig carries no TableMappings state in this emulation (see models.go), so per-table stats have no honest backend source; adding fabricated stats would be worse than an accurate empty list"}
  CreateReplicationSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "validates ReplicationSubnetGroupDescription/SubnetIds as required (real API marks both required); SubnetIds accepted but not modeled (no VPC subnet emulation), matching pre-existing convention. FIXED 2026-07-31 -- the response wire shape emitted a ReplicationSubnetGroupArn field; the real ReplicationSubnetGroup type has no Arn field at all (subnet groups are referenced by identifier on the wire; a client must build the ARN itself from the deterministic arn:aws:dms:<region>:<account>:subgrp:<identifier> format to tag one). Field removed from the wire struct; the internal Go model still tracks an ARN for indexing/tagging lookups, which is correct -- only the JSON response was wrong"}
  DescribeReplicationSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "same Arn-field fix as CreateReplicationSubnetGroup (2026-07-31)"}
  ModifyReplicationSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "a real backend.ModifyReplicationSubnetGroup mutates and persists the description. Same Arn-field fix as CreateReplicationSubnetGroup (2026-07-31)"}
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
  ModifyDataProvider: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-11 -- request field was named DataProviderArn; the real ModifyDataProviderMessage field is DataProviderIdentifier, so every real client's identifier was silently discarded"}
  DeleteDataProvider: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-11 -- same DataProviderArn/DataProviderIdentifier bug as ModifyDataProvider"}
  CreateEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-07-31 -- the response wire shape (eventSubscriptionJSON) used SubscriptionName and EventCategories, which are CreateEventSubscriptionMessage (request) field names; the real EventSubscription response type uses CustSubscriptionId and EventCategoriesList instead. A real SDK client deserializing the response got an empty subscription identifier and empty categories. Request-side field names (SubscriptionName/EventCategories on the input) were already correct and left unchanged -- the asymmetry between request and response field names is genuine AWS behavior, not a bug"}
  DescribeEventSubscriptions: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CustSubscriptionId/EventCategoriesList fix as CreateEventSubscription (2026-07-31)"}
  ModifyEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CustSubscriptionId/EventCategoriesList fix as CreateEventSubscription (2026-07-31)"}
  DeleteEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CustSubscriptionId/EventCategoriesList fix as CreateEventSubscription (2026-07-31)"}
  CreateInstanceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInstanceProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyInstanceProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-11 -- request field was named InstanceProfileArn; the real ModifyInstanceProfileMessage field is InstanceProfileIdentifier, so every real client's identifier was silently discarded"}
  DeleteInstanceProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-11 -- same InstanceProfileArn/InstanceProfileIdentifier bug as ModifyInstanceProfile"}
  CreateMigrationProject: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMigrationProjects: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyMigrationProject: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-11 -- request field was named MigrationProjectArn; the real ModifyMigrationProjectMessage field is MigrationProjectIdentifier, so every real client's identifier was silently discarded"}
  DeleteMigrationProject: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-11 -- same MigrationProjectArn/MigrationProjectIdentifier bug as ModifyMigrationProject"}
  ImportCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-07-31 -- the backend stored CertificatePem on Import but the response wire shape (certificateJSON) never returned it, on Import or Describe, even though the real Certificate type carries CertificatePem. Now returned on both"}
  DescribeCertificates: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CertificatePem fix as ImportCertificate (2026-07-31)"}
  DeleteCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CertificatePem fix as ImportCertificate (2026-07-31) -- certToJSON is shared by all three certificate ops"}
  DescribeAccountAttributes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "quota usage computed live from real counts"}
  DescribeEvents: {wire: partial, errors: ok, state: ok, persist: n/a, note: "events recorded on Endpoint/ReplicationTask create/delete/start/stop, not persisted across restarts -- low value, matches many other services' event-log conventions"}
  DescribeOrderableReplicationInstances: {wire: ok, errors: ok, state: n/a, note: "static reference catalog, matches real AWS class list"}
  DescribeEngineVersions: {wire: ok, errors: ok, state: n/a, note: "static reference catalog"}
  DescribeEndpointTypes: {wire: ok, errors: ok, state: n/a, note: "static reference catalog"}
  DescribeEventCategories: {wire: ok, errors: ok, state: n/a, note: "static reference catalog"}
  DescribeMetadataModel: {wire: ok, errors: ok, state: n/a, note: "FIXED this pass (gap #1) -- was an always-empty {} that only checked MigrationProjectIdentifier. Now requires MigrationProjectIdentifier/Origin/SelectionRules (all three are 'This member is required' on the real input) and returns the real {Definition, MetadataModelName, MetadataModelType, TargetMetadataModels} shape. Definition/MetadataModelName/MetadataModelType stay empty -- no schema-conversion engine exists to produce them, and the SDK doc explicitly says Definition 'might not be populated for some metadata models', so an empty-but-correctly-shaped response is not a stub (rule 4)."}
  DescribeMetadataModelAssessments: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- wire content used invented field names (MigrationProjectIdentifier/SelectionRules) instead of the real SchemaConversionRequest shape (RequestIdentifier/MigrationProjectArn/Status); now correct. MigrationProjectIdentifier is now required, matching the real input"}
  DescribeMetadataModelConversions: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as DescribeMetadataModelAssessments"}
  DescribeMetadataModelCreations: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as DescribeMetadataModelAssessments"}
  DescribeMetadataModelExportsAsScript: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as DescribeMetadataModelAssessments"}
  DescribeMetadataModelExportsToTarget: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as DescribeMetadataModelAssessments"}
  DescribeMetadataModelImports: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as DescribeMetadataModelAssessments"}
  DescribeMetadataModelChildren: {wire: ok, errors: ok, state: n/a, note: "FIXED this pass -- response field was named 'Items' with the wrong (request) shape; real field is MetadataModelChildren, a list of MetadataModelReference{MetadataModelName,SelectionRules}. Now requires MigrationProjectIdentifier/Origin/SelectionRules like DescribeMetadataModel. Always empty -- no child-model producer exists (there is no StartMetadataModelChildren op in the real API either; children only ever arise from a completed schema conversion)."}
  CancelMetadataModelConversion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- output was a flat {RequestIdentifier}; real shape is {Request: SchemaConversionRequest}. Cancelling an untracked request still succeeds (real AWS's Cancel ops are fire-and-forget), echoing a minimal SchemaConversionRequest"}
  CancelMetadataModelCreation: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as CancelMetadataModelConversion"}
  DescribeConversionConfiguration: {wire: ok, errors: ok, state: n/a, note: "pre-existing, matches the real {ConversionConfiguration, MigrationProjectIdentifier} shape"}
  ModifyConversionConfiguration: {wire: ok, errors: ok, state: n/a, note: "pre-existing, matches the real shape; echoes the caller's ConversionConfiguration (no real schema-conversion config store)"}
  DescribeExtensionPackAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- was hardcoded to always return an empty list, disconnected from StartExtensionPackAssociation. Now reads real extension-pack request rows"}
  StartExtensionPackAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- was a disguised no-op returning a random UUID with no backend write, so the request was invisible to DescribeExtensionPackAssociations. Now records a real request row and requires MigrationProjectIdentifier"}
  GetTargetSelectionRules: {wire: ok, errors: ok, state: n/a, note: "FIXED this pass -- output was an invented {Rules: []} list shape; real output is a single TargetSelectionRules string. Now requires MigrationProjectIdentifier/SelectionRules and echoes the source rules as a best-effort identity mapping (no real schema-conversion engine to compute a genuine target counterpart)"}
  ExportMetadataModelAssessment: {wire: ok, errors: ok, state: n/a, note: "FIXED this pass -- PdfReport/CsvReport were missing the ObjectURL field (real ExportMetadataModelAssessmentResultEntry has both ObjectURL and S3ObjectKey); now both are legitimately omitted (optional pointer fields, no real S3 integration exists) instead of one being a fabricated empty string. MigrationProjectIdentifier/SelectionRules are now required"}
  DescribeApplicableIndividualAssessments: {wire: ok, errors: ok, state: n/a, note: "FIXED this pass -- was hardcoded to always return an empty list; now returns a representative static catalog of individual assessment names (legitimate reference-data emulation per rule 4 -- the SDK does not model these names as an enum, they're plain strings)"}
  DescribeReplicationTaskIndividualAssessments: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass (deferred item #3) -- was hardcoded to always return an empty list regardless of any assessment run. Now populated by StartReplicationTaskAssessmentRun and filterable by replication-task-assessment-run-arn/replication-task-arn/status"}
  DescribeReplicationTaskAssessmentResults: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass (deferred item #3) -- was hardcoded to always return an empty list. Now: with ReplicationTaskArn, returns exactly one result for that task's latest run (ignoring Marker/MaxRecords, matching the SDK doc); without it, lists the latest result per assessed task"}
  StartReplicationTaskAssessmentRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- AssessmentRunName/ReplicationTaskArn/ResultLocationBucket/ServiceAccessRoleArn were all documented as required but never validated; IncludeOnly/Exclude were accepted but ignored. Now validates all four required fields, rejects setting both IncludeOnly and Exclude, and synchronously completes the run (Status passed) with real IndividualAssessment rows and ResultStatistic counts -- no goroutines/tickers, matching the service's leak-free convention"}
  CancelReplicationTaskAssessmentRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- output was a hand-rolled {ReplicationTaskAssessmentRunArn, Status} map; now the real {ReplicationTaskAssessmentRun: ReplicationTaskAssessmentRun} shape"}
  DeleteReplicationTaskAssessmentRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "same wire-shape fix as CancelReplicationTaskAssessmentRun"}
  DescribeReplicationTaskAssessmentRuns: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- output items were a hand-rolled 4-field map; now the real ReplicationTaskAssessmentRun shape (AssessmentProgress, ResultStatistic, ResultLocationBucket/Folder, ServiceAccessRoleArn, creation-date epoch, IsLatestTaskAssessmentRun). Filters extended to replication-task-assessment-run-arn/replication-instance-arn/status (previously only replication-task-arn)"}
  StartReplicationTaskAssessment: {wire: ok, errors: ok, state: ok, persist: n/a}
families:
  fleet-advisor: {status: ok, note: "CreateFleetAdvisorCollector/DeleteFleetAdvisorCollector/DescribeFleetAdvisorCollectors/DescribeFleetAdvisorDatabases/DeleteFleetAdvisorDatabases all mutate/read real backend state and persist. DescribeFleetAdvisorLsaAnalysis/SchemaObjectSummary/Schemas field-diffed this pass (deferred item #2, now resolved): response field names (Analysis/FleetAdvisorSchemaObjects/FleetAdvisorSchemas + NextToken) match types.go exactly; the lists are legitimately always-empty since no LSA-analysis or schema-conversion engine exists to populate them (rule 4). AWS ended support for Fleet Advisor entirely on 2026-05-20 (already past as of this audit) -- low future value."}
  metadata-model: {status: ok, note: "FIXED this pass -- DescribeMetadataModel/DescribeMetadataModelChildren/the six Describe*Requests list ops/Cancel*/GetTargetSelectionRules/ExportMetadataModelAssessment/StartExtensionPackAssociation were all field-diffed against types.go and api_op_*.go this pass (deferred item #1, now resolved) and every wire-shape bug found was fixed -- see the per-op notes above. Definition/MetadataModelName/MetadataModelType/schema-object contents stay legitimately empty; no schema-conversion SQL-generation engine exists, matching the SDK doc's 'might not be populated' language."}
  static-reference-data: {status: ok, note: "DescribeOrderableReplicationInstances/DescribeEngineVersions/DescribeEndpointTypes/DescribeEventCategories/DescribeApplicableIndividualAssessments return realistic static catalogs; legitimate for AWS reference-data ops (rule 4: an op with no mutable backend state behind it is not a stub). DescribeEndpointTypes FIXED this pass -- EndpointType values were hardcoded uppercase SOURCE/TARGET, but the real enum is lowercase source/target."}
  assessment-runs: {status: ok, note: "FIXED this pass (deferred item #3, now resolved) -- StartReplicationTaskAssessmentRun now validates its four required fields and IncludeOnly/Exclude mutual exclusion, then synchronously runs a real (bounded, static-catalog-backed) set of IndividualAssessment checks, all passing. DescribeReplicationTaskIndividualAssessments and DescribeReplicationTaskAssessmentResults are now backed by that real state instead of hardcoded empty lists. Cancel/Delete/DescribeReplicationTaskAssessmentRuns now return the full real ReplicationTaskAssessmentRun wire shape instead of a hand-rolled 4-field map."}
gaps: []
deferred: []
leaks: {status: clean, note: "no goroutines, janitors, or timers in this service; all state lives in store.Table/store.Index behind the single lockmetrics.RWMutex. leak_test.go / isolation_test.go pre-existing and passing. Confirmed again this pass -- no new goroutines/tickers/channels were introduced by the assessment-run rework (StartReplicationTaskAssessmentRun completes synchronously)."}
---

## Notes

- **2026-07-31 field-level wire-shape sweep**: the 2026-07-23 audit's "119
  operations matching the SDK exactly, no phantoms" claim about the *op
  list* was accurate and remains true, but 5 of its "wire: ok" marks were
  wrong at the *field* level -- caught by an independent dashboard sweep,
  each re-verified directly against
  `aws-sdk-go-v2/service/databasemigrationservice/types/types.go` before
  fixing (not assumed from the ticket description). All 5 are fixed this
  pass, each with a test that fails against the pre-fix code:
  1. `EventSubscription` response used `SubscriptionName`/`EventCategories`
     (the *request* field names) instead of `CustSubscriptionId`/
     `EventCategoriesList`. Request-side names were already correct and
     left alone -- AWS genuinely uses different names on request vs.
     response for this type.
  2. `ReplicationSubnetGroup` response emitted a `ReplicationSubnetGroupArn`
     field; the real type has no Arn field at all.
  3. `Certificate` response never returned `CertificatePem` on Import,
     Describe, or Delete, even though the backend stored it and the real
     type carries it.
  4. `CreateEndpoint`/`ModifyEndpoint` accepted `Password` in the request
     but never stored it (silently dropped); now stored on `Endpoint`
     internally and never put on the wire (matching real AWS, which never
     echoes credentials back). Engine-specific nested settings blocks
     (`MySQLSettings`, `S3Settings`, etc.) were left deliberately unmodeled
     at the time -- resolved 2026-08-10, see the note below.
  5. `DescribeConnections` never called `dmsPaginate` or set `Marker` on the
     response, unlike every other Describe op in this service.

- **2026-08-10 engine-specific endpoint settings (gopherstack-z79q)**:
  `CreateEndpointInput`/`ModifyEndpointInput` accept 19 heterogeneous
  engine-specific settings structs (`MySQLSettings`, `PostgreSQLSettings`,
  `S3Settings`, `OracleSettings`, `MongoDbSettings`, `KafkaSettings`,
  `KinesisSettings`, `RedshiftSettings`, `DynamoDbSettings`,
  `ElasticsearchSettings`, `NeptuneSettings`, `DocDbSettings`,
  `IBMDb2Settings`, `MicrosoftSQLServerSettings`, `SybaseSettings`,
  `DmsTransferSettings`, `GcpMySQLSettings`, `RedisSettings`,
  `TimestreamSettings`), field-counted directly against
  `aws-sdk-go-v2/service/databasemigrationservice/types/types.go`
  (`@v1.61.8`): ~301 fields total (2 to 44 fields per struct; `S3Settings`
  alone has 41, `OracleSettings` 44). Modeling all of them faithfully
  (validated types, stored, echoed on `DescribeEndpoints`, persisted) is not
  achievable in one pass, and per the issue's own instruction a partial
  subset is worse than the honest gap -- a client seeing some settings
  preserved would reasonably assume the rest are too. Instead of leaving the
  silent drop in place (the pre-existing behavior: `encoding/json` ignores
  unknown fields), the drop is now made visible: `engineSettingsFields` in
  `handler_endpoints.go` decodes all 19 fields, and `CreateEndpoint`/
  `ModifyEndpoint` reject the request with 400 `ValidationException` naming
  the field if any is set, rather than accepting and discarding it. This
  follows the same explicit-rejection-over-silent-drop precedent as
  sagemaker's `PipelineDefinitionS3Location` and cloudformation's
  unsupported `AccountFilterType` values. The `Password` fix from
  2026-07-31 is unaffected and unchanged.

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

- **Epoch-seconds timestamp bug class (2026-07-23 pass)**: `ReplicationInstance`
  and `ReplicationTask` both track a `CreationTime time.Time` field
  internally (used correctly for persistence ordering) but neither
  `InstanceCreateTime` (real field name on `ReplicationInstance`) nor
  `ReplicationTaskCreationDate` (real field name on `ReplicationTask`) was
  ever put on the wire -- not wrong-format, just entirely absent. Fixed by
  adding both fields to `replicationInstanceJSON`/`replicationTaskJSON` as
  `float64` populated via `pkgs/awstime.Epoch`, matching awsjson1.1's
  unixTimestamp format. `Endpoint` has no timestamp field on the real wire
  shape, so it was correctly left alone.

- **`EndpointType` is lowercase, unlike most other DMS enum-ish fields**:
  `types.ReplicationEndpointTypeValue` is `"source"`/`"target"` (not
  `"SOURCE"`/`"TARGET"`). This is easy to get backwards since
  `types.OriginTypeValue` (used by the metadata-model family's `Origin`
  field) genuinely IS uppercase (`"SOURCE"`/`"TARGET"`). Don't "fix" one to
  match the other without re-checking `enums.go`.

- **DMS Serverless "ReloadReplicationTables" targets a *config*, not a
  *task***: `ReloadReplicationTablesInput.ReplicationConfigArn` is the real
  field name (a previous implementation used `ReplicationTaskArn`, silently
  discarding the client's ARN and never validating anything). Don't
  conflate this with `ReloadTablesInput.ReplicationTaskArn`, which is the
  correct field name for the *non*-serverless `ReloadTables` op.

- **Premigration assessment runs complete synchronously in this emulation**:
  real AWS runs `StartReplicationTaskAssessmentRun` asynchronously against
  actual source/target connectivity; gopherstack has neither, so (matching
  the service's leak-free, goroutine-free convention -- see `leak_test.go`)
  the run transitions straight to `Status: "passed"` with every selected
  `IndividualAssessment` also `"passed"`. `defaultApplicableIndividualAssessments()`
  in `assessment_runs.go` is a representative static catalog, not derived
  from AWS docs verbatim -- the SDK does not model these names as an enum
  (`IndividualAssessmentName` is a plain `*string`), so any reasonable
  catalog is wire-accurate; only the *shape* (a flat list of strings) is a
  real constraint.
