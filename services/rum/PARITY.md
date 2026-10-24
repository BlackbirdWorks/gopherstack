---
service: rum
sdk_module: aws-sdk-go-v2/service/rum@v1.38.4
last_audit_commit: acf0513c8
last_audit_date: 2026-10-18
overall: B            # all 20 SDK operations are backed by real state and verified through the real SDK client in-package; B until the open items below close.
ops:
  BatchCreateRumMetricDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "extended metrics validated against the documented Name/ValueKey/EventPattern table; custom metrics validated for namespace, first-level keys and dimension paths; invalid items returned in Errors while valid ones are created; 200 per call, 2000 per destination"}
  BatchDeleteRumMetricDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "unknown IDs returned in Errors, known ones deleted"}
  BatchGetRumMetricDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "creation order, paginated"}
  CreateAppMonitor: {wire: ok, errors: ok, state: ok, persist: ok, note: "Name pattern, Domain xor DomainList (required for Web), configuration/telemetry/source-map validation, 10 monitors per region (ServiceQuotaExceededException), CwLogEnabled creates /aws/vendedlogs/RUMService_<name><first 8 of id> in CloudWatch Logs"}
  DeleteAppMonitor: {wire: ok, errors: ok, state: ok, persist: ok, note: "drops the monitor with its events, destinations and metric definitions"}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "PolicyNotFoundException without a policy; stale PolicyRevisionId gives InvalidPolicyRevisionIdException"}
  DeleteRumMetricsDestination: {wire: ok, errors: ok, state: ok, persist: ok, note: "also removes the metric definitions sent to it"}
  GetAppMonitor: {wire: ok, errors: ok, state: ok, persist: ok, note: "State is CREATED until the first PutRumEvents batch is accepted, then ACTIVE (assumed transition)"}
  GetAppMonitorData: {wire: ok, errors: ok, state: ok, persist: ok, note: "stored events inside TimeRange (epoch seconds, or milliseconds above 1e11) matching every filter; Browser/Device/Country/Page/OS read the event metadata, EventType the event type, Invert excludes any matching value; 30 day retention; paginated"}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "PolicyNotFoundException without a policy"}
  ListAppMonitors: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /appmonitors with query paging, sorted by name"}
  ListRumMetricsDestinations: {wire: ok, errors: ok, state: ok, persist: ok, note: "paginated"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "by app monitor ARN"}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "4 KB limit (PolicySizeLimitExceededException), JSON with a Statement (MalformedPolicyDocumentException), PolicyRevisionId compare-and-set"}
  PutRumEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "data plane (host dataplane.rum.*, same signing name): events validated, stored, forwarded to CloudWatch Logs when CwLogEnabled, and matched against the default AWS/RUM metrics and the CloudWatch metric definitions"}
  PutRumMetricsDestination: {wire: ok, errors: ok, state: ok, persist: ok, note: "CloudWatch takes no DestinationArn; Evidently needs DestinationArn and IamRoleArn"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "app monitor ARN; 50 tags"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "app monitor ARN"}
  UpdateAppMonitor: {wire: ok, errors: ok, state: ok, persist: ok, note: "only supplied parts change; Domain and DomainList replace each other"}
  UpdateRumMetricDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "same validation as create; replaces the definition in place"}
families:
  app_monitors: {status: ok, note: "restjson1, signing name rum; accounts and regions isolated; names unique per account and region"}
  data_plane: {status: ok, note: "PutRumEvents/GetAppMonitorData over stored events; CloudWatch Logs delivery and CloudWatch metrics wired in internal/app/rum_wiring.go"}
  metrics: {status: ok, note: "default AWS/RUM metrics (SessionCount, PageViewCount, JsErrorCount, PerformanceNavigationDuration, PerformanceResourceDuration, WebVitals*, Http4xxCount, Http5xxCount, HttpErrorCount, Navigation*Transaction) plus extended and custom definitions published through the EventBridge-style pattern matcher"}
items_still_open:
  - Evidently destinations: destinations and their metric definitions are stored but no events are published to Evidently (no Evidently service)
  - PutRumEvents: the app monitor resource policy and its rum:alias condition are stored but not enforced
  - DeobfuscationConfiguration: source map settings are stored; stack traces are never unminified
  - Default metrics: Telemetries (errors, performance, http) do not gate the default metric set
  - BatchCreateRumMetricDefinitions: the per-item ErrorCode values (ValidationException, ServiceQuotaExceededException) are assumptions, the SDK does not enumerate them
  - GetAppMonitorData: the TimeRange unit is not stated by the SDK; seconds and milliseconds are both accepted
  - AccessDeniedException and ThrottlingException: never raised; no IAM or throttling is modeled
  - CloudFormation: AWS::RUM::AppMonitor is not provisioned by the CloudFormation emulator
gaps: []
structural_gaps:
  - Country filter and CountryCode dimension: the service derives countryCode from the sender IP, there is no GeoIP source; only a countryCode already present in event metadata matches
  - Cognito authorization of PutRumEvents: guest-role credentials from an identity pool cannot be issued here, so any signed caller may send events
deferred: []
leaks: {status: clean, note: "no goroutines or timers; per-monitor event storage is capped at 20000 events and 30 days"}
---

## Notes
Protocol restjson1, signing name `rum`; PutRumEvents is `POST /appmonitors/{Id}/` and the SDK prefixes the host with `dataplane.` unless the endpoint is overridden.
App monitor IDs are UUIDs; PutRumEvents resolves the monitor by ID alone (account-independent), everything else by name within the caller's account and region.
Stored events are the JSON strings returned by GetAppMonitorData and written to CloudWatch Logs: event_id, event_type, event_timestamp, event_details, metadata, user_details, batch_id, application_* and account_id.
Metric namespaces: extended metrics publish to AWS/RUM; custom metrics to RUM/CustomMetrics/<namespace>; every point carries the application_name dimension.
