---
service: bcmdataexports
sdk_module: aws-sdk-go-v2/service/bcmdataexports@v1.25.4
last_audit_commit: 4ae40e79e
last_audit_date: 2026-10-18
overall: B            # all 12 SDK operations are state-backed and SDK-driven in-package; B until a Docker integration test runs and the open items close.
ops:
  CreateExport: {wire: ok, errors: ok, state: ok, persist: ok, note: "validates name, S3 destination, Format/Compression combos, SYNCHRONOUS cadence, table names and properties, and parses the query (columns checked against the table schema); starts an execution; 50-export quota"}
  GetExport: {wire: ok, errors: ok, state: ok, persist: ok, note: "timestamps are RFC 3339 strings (schema timestampFormat date-time), not epoch numbers"}
  UpdateExport: {wire: ok, errors: ok, state: ok, persist: ok, note: "name is immutable; queues a new execution"}
  DeleteExport: {wire: ok, errors: ok, state: ok, persist: ok, note: "also removes executions"}
  ListExports: {wire: ok, errors: ok, state: ok, persist: ok, note: "page.Page, MaxResults 1-100"}
  GetExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "Export is the definition the execution ran with"}
  ListExecutions: {wire: ok, errors: ok, state: ok, persist: ok, note: "newest first; last 50 per export retained"}
  GetTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "properties validated and defaulted; INCLUDE_RESOURCES and INCLUDE_SPLIT_COST_ALLOCATION_DATA change the schema"}
  ListTables: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  executions: {status: ok, note: "a worker ticker (250ms) advances INITIATION_IN_PROCESS, QUERY_QUEUED, QUERY_IN_PROCESS, DELIVERY_IN_PROCESS, DELIVERY_SUCCESS one step per tick; QUERY_FAILURE on a query that no longer evaluates, DELIVERY_FAILURE (INSUFFICIENT_PERMISSION) when the bucket is missing; export status HEALTHY/UNHEALTHY follows the last execution"}
  query: {status: ok, note: "SELECT cols|* FROM table [WHERE cmp {AND|OR} ...] [LIMIT n]; operators = != <> < > <= >= LIKE; no aggregates, joins or functions"}
  delivery: {status: ok, note: "CSV (.csv.gz/.csv.zip) or .snappy.parquet at prefix/name/data/BILLING_PERIOD=yyyy-mm/ (CREATE_NEW_REPORT adds a timestamp-executionId directory) plus metadata/BILLING_PERIOD=.../name-Manifest.json"}
items_still_open:
  - tables: only COST_AND_USAGE_REPORT, FOCUS_1_0_AWS, CARBON_EMISSIONS and COST_OPTIMIZATION_RECOMMENDATIONS exist; FOCUS_1_2_AWS and other real tables are absent
  - columns: COST_AND_USAGE_REPORT carries about 40 of the real CUR 2.0 columns and FOCUS_1_0_AWS the 43 FOCUS 1.0 columns plus 5 x_ extensions; the exact x_ set is unverified
  - CARBON_EMISSIONS and COST_OPTIMIZATION_RECOMMENDATIONS: schemas only, exports contain headers and no rows (no emissions or recommendation data source)
  - manifest: the metadata manifest layout is an emulator shape, not verified against the real service
  - error bodies: ResourceNotFoundException omits ResourceId/ResourceType, ValidationException omits Reason/Fields
  - MaxResults bounds: 1-100 assumed
gaps: []
structural_gaps: []
deferred: []
leaks: {status: clean, note: "StartWorker group is stopped on context cancel; testleak.VerifyTestMain in package tests"}
---

## Notes
Protocol awsjson1.1, target prefix AWSBillingAndCostManagementDataExports., homed in us-east-1.
Timestamps in ExportStatus and ExecutionStatus use the date-time format: sending epoch seconds breaks SDK deserialization.
