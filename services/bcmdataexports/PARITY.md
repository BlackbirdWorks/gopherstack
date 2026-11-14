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
  ListExports: {wire: ok, errors: ok, state: ok, persist: ok, note: "page.Page, MaxResults 1-300 (default 100)"}
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
  - CARBON_EMISSIONS and COST_OPTIMIZATION_RECOMMENDATIONS: schemas only, exports contain headers and no rows (no emissions or recommendation data source)
  - manifest: delivery locations (one manifest in overwrite mode, a second per-execution copy in create-new mode) and the additionalOutputFiles section follow the documented layout; the spelling of the remaining manifest keys (columns, file list, time period) is not published
  - error bodies: ServiceQuotaExceeded QuotaCode and Throttling QuotaCode/ServiceCode are not set (Service Quotas codes are not published; throttling is never raised); query-parser validation errors report Reason other without Fields
gaps: []
structural_gaps: []
deferred: []
leaks: {status: clean, note: "StartWorker group is stopped on context cancel; testleak.VerifyTestMain in package tests"}
---

## Notes
Protocol awsjson1.1, target prefix AWSBillingAndCostManagementDataExports., homed in us-east-1.
Timestamps in ExportStatus and ExecutionStatus use the date-time format: sending epoch seconds breaks SDK deserialization.

## Verified against documentation
- Tables COST_AND_USAGE_REPORT, FOCUS_1_0_AWS, FOCUS_1_2_AWS, COST_AND_USAGE_DASHBOARD, CARBON_EMISSIONS, COST_OPTIMIZATION_RECOMMENDATIONS and their per-table export quotas (CUR 5, others 2) per dataexports-quotas.html.
- CUR 2.0 columns follow the per-group pages table-dictionary-cur2-{bill,line-item,pricing,product,reservation,savings-plan,discount,split-line-item,capacity-reservation,identity,cost-category,resource-tags}.html (plus the `tags` map named in the column-group list; the page headline count of 125 is lower than the per-group pages list). INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY removes discount and discount_total_discount; INCLUDE_CAPACITY_RESERVATION_DATA, INCLUDE_IAM_PRINCIPAL_DATA and BILLING_VIEW_ARN are accepted table properties.
- FOCUS 1.0 and FOCUS 1.2 per table-dictionary-focus-1-0-aws-columns.html and table-dictionary-focus-1-2-aws-columns.html; 1.2 drops x_CostCategories and x_UsageType and adds x_Operation/x_ServiceCode/x_Discounts only.
- COST_AND_USAGE_DASHBOARD columns per table-dictionary-cur-dashboard.html (column types are not published; costs are Decimal, the rest String).
- MaxResults 1-300 and ServiceQuotaExceeded ServiceCode AWSBillingAndCostManagementDataExports per API_DataExports_ListExports/ListExecutions/ListTables/ListTagsForResource/CreateExport.
- Manifest and data layout per dataexports-export-delivery.html.
