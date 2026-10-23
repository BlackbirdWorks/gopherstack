---
service: cur
sdk_module: aws-sdk-go-v2/service/costandusagereportservice@v1.43.4
last_audit_commit: 4ae40e79e
last_audit_date: 2026-10-18
overall: B            # all 7 SDK operations are state-backed and SDK-driven in-package; B until a Docker integration test runs and the open items close.
ops:
  PutReportDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "ReportName/S3Prefix patterns, TimeUnit, Format+Compression combos, S3Region enum, AdditionalArtifacts rules (ATHENA alone, Parquet + OVERWRITE_REPORT; REDSHIFT/QUICKSIGHT need textORcsv + GZIP), 10-report limit, duplicate name, S3 bucket must exist when S3 is wired; defaults RefreshClosedReports=true, ReportVersioning=CREATE_NEW_REPORT"}
  ModifyReportDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "same validation; tags and delivery status kept; definition name must equal ReportName"}
  DescribeReportDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "page.Page, default and max 5; ReportStatus omitted until the first delivery"}
  DeleteReportDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResponseMessage returned; delivered objects stay in S3"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  delivery: {status: ok, note: "worker ticker (1m) delivers each report once per HOURLY/DAILY/MONTHLY slot; writes CSV .csv.gz/.csv.zip or .snappy.parquet plus name-Manifest.json under prefix/name/yyyymmdd-yyyymmdd/[assemblyId/] (OVERWRITE_REPORT drops the assembly directory, ATHENA uses prefix/name/name/year=/month=/); REDSHIFT, QUICKSIGHT and ATHENA artifacts written and listed in additionalArtifactKeys; ReportStatus lastStatus SUCCESS, ERROR_NO_BUCKET, ERROR_PERMISSIONS"}
  rows: {status: ok, note: "rows come from the Cost Explorer ledger via GetCostAndUsage (daily); HOURLY splits each day into 24 equal rows, MONTHLY sums per service/region/usage type/account; with no Cost Explorer wired the file holds headers only"}
items_still_open:
  - columns: bill/lineItem/product/pricing/reservation/savingsPlan/discount core columns are present, but only those the cost ledger can fill carry values (the rest are empty cells); resourceTags/*, costCategory/* and the long tail of product/* attribute columns are absent
  - MANUAL_DISCOUNT_COMPATIBILITY: accepted but adds no columns
  - ResourceId and tags: lineItem/ResourceId is empty because the cost ledger has no per-resource data
  - lastDelivery: emitted as RFC 3339; the real service string format is unverified
  - ATHENA artifacts: crawler template is minimal; cost_and_usage_data_status.parquet (one row, status=READY) is written next to the data but its exact key and schema are recalled, unverified
  - BillingViewArn: format-checked only, no billing view backend
gaps: []
structural_gaps: []
deferred: []
leaks: {status: clean, note: "StartWorker group is stopped on context cancel; testleak.VerifyTestMain in package tests"}
---

## Notes
Protocol awsjson1.1, target prefix AWSOrigamiServiceGatewayService., homed in us-east-1 (ChaosRegions reports only that region).
Every error is HTTP 400 except InternalErrorException; codes: DuplicateReportNameException, ReportLimitReachedException,
ResourceNotFoundException, ValidationException.
