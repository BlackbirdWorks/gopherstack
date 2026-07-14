---
service: glue
sdk_module: aws-sdk-go-v2/service/glue@v1.137.2
last_audit_commit: a8c6614b
last_audit_date: 2026-07-12
overall: A            # small, well-verified set of real bugs found and fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateDatabase: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDatabase: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDatabases: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDatabase: {wire: partial, errors: ok, state: ok, persist: ok, note: "DatabaseInput only models Name/Description; real AWS DatabaseInput also has Parameters/LocationUri/CreateTableDefaultPermissions/TargetDatabase (bd: gopherstack-qd3.3, deferred — not fixed this pass)"}
  CreateTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: added Parameters/Owner/Retention to Table+TableInput, and full StorageDescriptor (InputFormat/OutputFormat/SerdeInfo/Parameters/BucketColumns/SortColumns/Compressed/NumberOfBuckets/StoredAsSubDirectories) and Column.Parameters, all previously silently dropped"}
  GetTable: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTables: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: was returning live backend *Table pointers uncloned (lock-bypass mutation/data-race risk); now clones like GetTable/SearchTables"}
  UpdateTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "same field-completeness fix as CreateTable"}
  DeleteTable: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDeleteTable: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTableVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-verified in depth this pass; existing coverage looked correct"}
  DeleteTableVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDeleteTableVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePartition: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: BatchCreatePartition (which CreatePartition delegates to) never checked the parent table existed, silently storing orphaned partitions against a nonexistent db/table; now returns EntityNotFoundException per AWS contract. Also added Partition/PartitionInput.Parameters + Partition.CreationTime/CatalogId"}
  BatchCreatePartition: {wire: ok, errors: ok, state: ok, persist: ok, note: "same table-existence fix as CreatePartition"}
  GetPartition: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPartitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "expression filter (segment) not re-verified in depth this pass"}
  BatchGetPartition: {wire: ok, errors: ok, state: ok, persist: ok, note: "SEVERE fix this pass: was a disguised stub — always returned an empty Partitions list regardless of backend state, with a comment falsely claiming \"the mock backend has no partition storage\". Now looks up each PartitionsToGet entry via GetPartition and reports misses in UnprocessedKeys per the real BatchGetPartitionResponse shape"}
  UpdatePartition: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also persists Parameters through both the in-place and rename paths"}
  BatchUpdatePartition: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePartition: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDeletePartition: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePartitionIndex: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-verified in depth this pass"}
  GetPartitionIndexes: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePartitionIndex: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCrawler: {wire: partial, errors: ok, state: ok, persist: ok, note: "fixed this pass (additively): CreateCrawler's positional signature is called from services/cloudformation (external package) so it was kept unchanged; added CreateCrawlerWithOptions(...,CrawlerOptions) carrying Schedule/Classifiers/Configuration/TablePrefix/Description, which the Glue handler now uses. CrawlerTarget gained JdbcTargets/CatalogTargets (was S3-only) and S3Target/JDBCTarget gained Exclusions. Still deferred: SchemaChangePolicy, RecrawlPolicy, LineageConfiguration, CrawlerSecurityConfiguration, LakeFormationConfiguration, DynamoDB/Delta/Hudi/Iceberg/MongoDB targets"}
  GetCrawler: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCrawlers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCrawlers: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCrawler: {wire: partial, errors: ok, state: ok, persist: ok, note: "same additive CrawlerOptions fix as CreateCrawler; also fixed a missing CrawlerRunningException guard (UpdateCrawler previously allowed updating a RUNNING/STARTING/STOPPING crawler, unlike DeleteCrawler which already checked this)"}
  DeleteCrawler: {wire: ok, errors: ok, state: ok, persist: ok}
  StartCrawler: {wire: ok, errors: ok, state: ok, persist: ok}
  StopCrawler: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCrawlerSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  StartCrawlerSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  StopCrawlerSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCrawlerMetrics: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-verified in depth this pass"}
  CreateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: added Job.MaxCapacity + NotificationProperty (previously missing entirely — the MaxCapacity vs WorkerType/NumberOfWorkers axis named explicitly in the audit brief), plus AWS's documented mutual-exclusion validation between MaxCapacity and WorkerType/NumberOfWorkers"}
  GetJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-verified in depth this pass"}
  UpdateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "same MaxCapacity/NotificationProperty fix as CreateJob"}
  DeleteJob: {wire: ok, errors: ok, state: ok, persist: ok}
  StartJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: JobRun now carries WorkerType/NumberOfWorkers/MaxCapacity/GlueVersion/Timeout inherited from the job at start time (previously GetJobRun told callers nothing about what capacity the run used). No per-run override support (StartJobRunRequest overrides not modeled) — deferred"}
  GetJobRun: {wire: ok, errors: ok, state: ok, persist: ok}
  GetJobRuns: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchStopJobRun: {wire: ok, errors: ok, state: ok, persist: ok}
  GetJobBookmark: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-verified in depth this pass"}
  ResetJobBookmark: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTags: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  connections: {status: deferred, note: "structural wire shape looked plausible (ConnectionType/ConnectionProperties/Tags) but PhysicalConnectionRequirements, MatchCriteria and connection-type-specific validation not audited this pass"}
  triggers: {status: partial, note: "fixed this pass: StartTrigger/StopTrigger unconditionally set State to ACTIVATED/DEACTIVATED for every trigger Type, but AWS docs (about-triggers.html) state ON_DEMAND triggers 'never enter the ACTIVATED or DEACTIVATED state — they always remain in the CREATED state.' Also, StartTrigger on an ON_DEMAND trigger is what fires it (starts its jobs/crawlers immediately) and this backend did nothing beyond a state-string flip — a disguised stub for the type's whole purpose. Fixed: ON_DEMAND stays CREATED across Start/Stop, and StartTrigger now runs each action's job (StartJobRun) or crawler (StartCrawler) outside the trigger lock (StartJobRun/StartCrawler take the same coarse lock, non-reentrant). Also added Action.CrawlerName (real types.Action supports firing a crawler, not just a job — was entirely unmodeled) and CreateTriggerInput.StartOnCreation (silently dropped on the wire; now activates SCHEDULED/CONDITIONAL/EVENT triggers at creation, ignored for ON_DEMAND per AWS: 'True is not supported for ON_DEMAND triggers'). Still deferred: predicate-condition wire-shape depth, Description/EventBatchingCondition/WorkflowName fields, the '2 crawlers per trigger' AWS soft limit."}
  workflows: {status: deferred, note: "not audited this pass"}
  dev_endpoints: {status: deferred, note: "not audited this pass"}
  security_configurations: {status: deferred, note: "not audited this pass"}
  schema_registry: {status: partial, note: "fixed this pass: CreateSchema returned the live *Schema map-stored pointer directly (not a clone). RegisterSchemaVersion (increments NextSchemaVersion/LatestSchemaVersion) and UpdateSchema (Compatibility/Description) both mutate that same pointer in place under the backend lock, while the CreateSchema handler reads Compatibility/SchemaStatus/etc. from it after the lock is released to build the wire response — a genuine unsynchronized read/write data race, plus a correctness bug if a concurrent Update raced the Create response. Fixed by cloning before return (matches the established pattern already used by DescribeSchema/ListSchemas/CreateConnection/CreateTrigger/etc.). CreateRegistry/RegisterSchemaVersion audited and found NOT to have the same bug (no field either one returns is ever mutated post-creation) — left as-is. Still not audited: GetSchemaByDefinition/compatibility-mode enforcement/AVRO-JSON-PROTOBUF validation"}
  data_quality_rulesets: {status: deferred, note: "audited this pass: CreateDataQualityRuleset/StartDataQualityRulesetEvaluationRun return their live map-stored pointer too, but their handlers only read the immutable Name/RunID identity fields from it, and no other op mutates those objects post-creation — not an actual bug, left as-is. Ruleset DQDL syntax / rule-type validation still not audited"}
  ml_transforms: {status: deferred, note: "not audited this pass"}
  blueprints: {status: deferred, note: "not audited this pass"}
  user_defined_functions: {status: deferred, note: "not audited this pass"}
  resource_policy: {status: ok, note: "fixed this pass: PutResourcePolicy silently dropped PolicyExistsCondition (MUST_EXIST/NOT_EXIST) and PolicyHashCondition entirely — every call unconditionally created/overwrote the policy regardless of what a caller passed, defeating the optimistic-concurrency guard those fields exist for. Worse, DeleteResourcePolicy's PolicyHashCondition parameter was already plumbed from the wire into the backend method but the backend signature discarded it as `_ string` — any caller's hash was ignored and the policy always deleted. Both now enforce the conditions and return the documented ConditionCheckFailureException (new sentinel ErrResourcePolicyConditionFailed, mapped in handler.go's handleError) or EntityNotFoundException (MUST_EXIST-but-missing) on mismatch. Interface signature PutResourcePolicy gained two params (existsCondition, hashCondition); only InMemoryBackend implements StorageBackend so this was safe. Not modeled: EnableHybrid."}
  integration_resource_properties: {status: ok, note: "fixed this pass (found while auditing the deferred families, not previously tracked in this ledger): GetIntegrationResourceProperty/CreateIntegrationResourceProperty/UpdateIntegrationResourceProperty/ListIntegrationResourceProperties and GetIntegrationTableProperties all returned the live map-stored pointer with its SourceProperties/TargetProperties (or SourceTableConfig/TargetTableConfig) maps uncloned. UpdateIntegrationResourceProperty/UpdateIntegrationTableProperties reassign those same map fields in place under the lock, while Get/Create's callers read them after the lock is released — a genuine data race, same bug class as the prior pass's GetTables fix. Fixed by cloning (new cloneIntegrationResourceProperty helper + inline clone for the table-properties Get)."}
  error_codes_global: {status: ok, note: "SEVERE systemic fix this pass: the shared ErrValidation sentinel wired \"ValidationException\" as its wire __type — confirmed against aws-sdk-go-v2/service/glue/deserializers.go that the vast majority of Create/Update/Delete operations (CreateDatabase, CreateTable, CreateJob, CreateCrawler, CreateTrigger, CreateBlueprint, CreateCustomEntityType, CreateUsageProfile, tag validation, ...) document InvalidInputException instead. Changed the shared sentinel + handler.go's hardcoded mapping to InvalidInputException, and fixed the ~8 existing tests that had encoded the wrong wire code. Also fixed awserrFromDetail (handler_stubs.go), which always wrapped batch-operation ErrorDetail as awserr.ErrNotFound regardless of the actual ErrorCode string — so e.g. an AlreadyExistsException detail from BatchCreatePartition surfaced to CreatePartition callers as EntityNotFoundException. Not touched: IdempotentParameterMismatchException, ResourceNumberLimitExceededException, OperationTimeoutException, ConcurrentModificationException remain unused — no account-level quota/concurrency-conflict modeling exists to trigger them realistically (bd: gopherstack-qd3.5)"}
gaps:
  - CrawlerTarget missing DynamoDBTargets/DeltaTargets/HudiTargets/IcebergTargets/MongoDBTargets (only S3/JDBC/Catalog modeled) (bd: gopherstack-qd3.1)
  - CreateCrawler/UpdateCrawler missing SchemaChangePolicy, RecrawlPolicy, LineageConfiguration, CrawlerSecurityConfiguration, LakeFormationConfiguration (bd: gopherstack-qd3.2)
  - DatabaseInput/Database missing Parameters, LocationUri, CreateTableDefaultPermissions, TargetDatabase (bd: gopherstack-qd3.3)
  - StartJobRun has no per-run capacity/argument overrides (WorkerType/NumberOfWorkers/MaxCapacity/Timeout/NotificationProperty are inherited from the job only, not overridable per AWS's StartJobRunRequest) (bd: gopherstack-qd3.4)
  - IdempotentParameterMismatchException/ResourceNumberLimitExceededException/OperationTimeoutException/ConcurrentModificationException are documented Glue exceptions never returned by this backend (no quota/idempotency-token/concurrency-conflict modeling) (bd: gopherstack-qd3.5)
  - Trigger/TriggerAction missing Description, EventBatchingCondition, WorkflowName, and the AWS "max 2 crawler actions per trigger" soft limit is not enforced (bd: gopherstack-qd4.1)
  - PutResourcePolicy does not model EnableHybrid (bd: gopherstack-qd4.2)
deferred:
  - workflows
  - dev endpoints
  - security configurations
  - schema registry (compatibility modes, AVRO/JSON/PROTOBUF validation depth, GetSchemaByDefinition)
  - data quality rulesets (DQDL syntax / rule-type validation)
  - ML transforms
  - blueprints
  - user-defined functions
  - connections (PhysicalConnectionRequirements/MatchCriteria depth)
  - triggers (predicate-condition wire-shape depth; Description/EventBatchingCondition/WorkflowName fields)
leaks: {status: clean, note: "backend_reconciler.go's managed goroutine (StartReconciler/StopReconciler/reconcileLoop) already exits deterministically on ctx.Done() or the stop channel with a WaitGroup — no unmanaged 'go b.runReconciler()' leak. Verified with go test -race; no new goroutines/timers introduced this pass."}
---

## Notes

- **Protocol**: json-1.1 (`X-Amz-Target: AWSGlue.<Op>`, `application/x-amz-json-1.1`),
  confirmed against `aws-sdk-go-v2/service/glue/deserializers.go`'s
  `awsAwsjson11_deserializeOpError<Op>` switch statements. Error responses use
  `{"__type": "<ExceptionName>", "message": "..."}`.

- **ValidationException vs InvalidInputException (important, easy to re-flag by
  mistake)**: Glue's SDK error model genuinely contains BOTH exception types.
  `ValidationException` IS a real type in `types/errors.go`, and a handful of newer
  operations (confirmed: `DeleteConnectionType`) do declare it as a documented error.
  But the overwhelming majority of hand-validation call sites in this backend
  (name-length checks, tag-limit checks, required-field checks across
  Create/Update/Delete for databases/tables/jobs/crawlers/triggers/blueprints/
  custom-entity-types/usage-profiles) correspond to AWS operations whose deserializer
  switch lists `InvalidInputException`, not `ValidationException` — confirmed by
  reading the actual `awsAwsjson11_deserializeOpErrorCreateXxx` functions in
  `deserializers.go` for CreateDatabase, CreateTable, CreateJob, CreateCrawler,
  CreateTrigger, CreateBlueprint, CreateCustomEntityType, CreateUsageProfile. Since
  `ErrValidation` is one shared sentinel used everywhere, the fix picks the option
  that's correct for the large majority of call sites. Do not "fix" this back to
  ValidationException without checking the SDK deserializer for the specific op in
  question first.

- **`awserrFromDetail` (handler_stubs.go)**: single-item AWS ops that are implemented
  by calling a batch backend method with a one-element slice (CreatePartition →
  BatchCreatePartition, DeletePartition → BatchDeletePartition) surface
  `errs[0].ErrorDetail` as a real Go error via this helper. It must switch on
  `d.ErrorCode` to pick the matching sentinel (AlreadyExists vs NotFound vs generic
  invalid-parameter) — do not revert it to unconditionally wrapping
  `awserr.ErrNotFound`, or AlreadyExistsException details get reported to SDK callers
  as EntityNotFoundException.

- **StorageDescriptor is shared by Table AND Partition** in real Glue (partitions
  carry their own StorageDescriptor that can override table-level SerDe/format
  settings). Because `CreateTable`/`UpdateTable`/`BatchCreatePartition`/
  `UpdatePartition` already copy the whole `StorageDescriptor` struct by value from
  the request input, adding fields to the `StorageDescriptor`/`Column` type
  definitions was sufficient to flow them through end-to-end — the remaining real
  work was fixing `cloneTable`/`clonePartition`/`cloneCrawler` to deep-copy the new
  nested maps/slices/pointers (Parameters maps, SerdeInfo pointer, BucketColumns/
  SortColumns slices, per-Column Parameters) so that `GetTable`/`GetPartitions`
  callers can't mutate live backend state through the returned pointers.

- **`CreateCrawler`/`UpdateCrawler` signature is called from
  `services/cloudformation/resources_phase5.go`** (outside this package) with the
  original 5-arg / 4-arg positional signatures. Per the audit's signature-safety
  rule, those signatures were left untouched; new capability (Schedule, Classifiers,
  Configuration, TablePrefix, Description) was added via new
  `CreateCrawlerWithOptions`/`UpdateCrawlerWithOptions` methods that the old methods
  now delegate to with a zero-value `CrawlerOptions`. The `StorageBackend` interface
  gained the two new methods additively; `InMemoryBackend` is the only implementer
  (verified — no mocks reference `StorageBackend` in this package's tests), so this
  is safe.

- **GetTables aliasing bug**: `GetTables` was the one read path in the whole backend
  that hadn't been updated to clone before returning (`GetDatabases`, `GetCrawlers`,
  `GetJobs`, `GetConnections`, `SearchTables`, `GetPartition(s)` all already cloned).
  Fixed to match the established pattern; verified no other `Get*` list method has
  the same gap.

## This pass (re-audit at HEAD `a8c6614b`, no local drift since `ce30166a`)

`git diff ce30166a..HEAD -- services/glue/` was empty (the ledger's real baseline —
the recorded `last_audit_commit: 704d7cda` did not exist in this branch's history;
`704d7cda` turned out to be an unrelated STS commit, so per the re-audit protocol
`ce30166a`, the commit that last touched this file, was used as the actual baseline).
SDK pin unchanged at `v1.137.2`. With zero drift, all rows the previous pass marked
`ok` were trusted as-is; this pass audited only the rows marked `partial`/`deferred`
and found genuine, narrowly-scoped bugs in triggers, schema registry, resource
policy, and (previously untracked) integration resource/table properties — see the
`families` notes above for each. Full details on all six fixes are in those notes;
summary: two more `Get*`-returns-live-pointer data races (schema registry,
integration properties — same bug class as the prior pass's `GetTables` fix),
`StartTrigger`/`StopTrigger` ignoring the ON_DEMAND-trigger state rule and
`StartTrigger` never actually firing an ON_DEMAND trigger's actions (a disguised
stub for that type's entire purpose), and `PutResourcePolicy`/`DeleteResourcePolicy`
silently dropping their optimistic-concurrency condition parameters.

New regression tests: `parity_pass5_test.go`.

## Follow-ups filed as SHARED-FILE / cross-service (NOT edited this pass)

No code changes were needed outside `services/glue/` this pass. The
`PutResourcePolicy` interface-signature change (two new params) and the additive
`Trigger.StartOnCreation`/`TriggerAction.CrawlerName` fields were checked against
`services/cloudformation/resources_phase5.go`, the one cross-package caller of Glue
backend methods (`gluebackend.CreateCrawler`/`BatchCreatePartition`/`CreateJob`/
`CreateTrigger`) — none of those call sites touch `PutResourcePolicy`, and the
`Trigger{}` struct literal it builds is unaffected by new additive fields, so no
follow-up is needed there.

Separately (found, NOT fixed — pre-existing, unrelated to this pass's Glue changes):
`go build ./services/cloudformation/...` currently fails on
`services/route53/*` — `rc.backends.Route53.Backend.CreateHostedZone` is called
with 4 args but the (concurrently-edited-by-another-agent) Route53 backend method
now takes 5. This is outside `services/glue/` and was left untouched per this
task's scope; flagging for whichever pass owns Route53/CloudFormation.
