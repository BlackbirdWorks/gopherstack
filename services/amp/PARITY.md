---
service: amp
sdk_module: aws-sdk-go-v2/service/amp@v1.54.5
last_audit_commit: c1af87ba3
last_audit_date: 2026-10-18
overall: B            # all 43 SDK operations and the Prometheus remote-write and query data plane are real and verified through the SDK client and raw HTTP; B until the open items close.
ops:
  CreateAlertManagerDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "alertmanager_config must be a YAML string with a route whose receiver is defined in receivers"}
  CreateAnomalyDetector: {wire: ok, errors: ok, state: ok, persist: ok, note: "query must be wrapped by avg|count|group|max|min|quantile|stddev|stdvar|sum; defaults interval 60s, shingle 8, sample 256"}
  CreateLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateQueryLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRuleGroupsNamespace: {wire: ok, errors: ok, state: ok, persist: ok, note: "YAML parsed and checked: groups list, unique group names, record xor alert, expr non-empty with balanced brackets, for duration; a rejected file is a synchronous ValidationException (reason CANNOT_PARSE), not an async CREATION_FAILED"}
  CreateScraper: {wire: ok, errors: ok, state: ok, persist: ok, note: "destination workspace must exist; EKS cluster checked against the EKS emulator when wired; scrape config must hold scrape_configs with unique job_name; role ARN is a service-linked role shape"}
  CreateWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING until the lifecycle dwell (500ms default), then ACTIVE; id ws-<uuid>; ClientToken replays; PrometheusEndpoint is the caller-facing base URL of the data plane"}
  DeleteAlertManagerDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAnomalyDetector: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteQueryLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRuleGroupsNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteScraper: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteScraperLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETING, then gone after the dwell with its namespaces, detectors and samples"}
  DescribeAlertManagerDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAnomalyDetector: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeQueryLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRuleGroupsNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScraper: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScraperLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeWorkspace: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeWorkspaceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDefaultScraperConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns a fixed default scrape configuration document"}
  ListAnomalyDetectors: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRuleGroupsNamespaces: {wire: ok, errors: ok, state: ok, persist: ok}
  ListScrapers: {wire: ok, errors: ok, state: ok, persist: ok, note: "filters alias, status, sourceArn, destinationArn from query parameters (OR within a key, AND across keys)"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAlertManagerDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAnomalyDetector: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "revision semantics: NO_POLICY or empty for the first policy, current revision afterwards, otherwise ConflictException"}
  PutRuleGroupsNamespace: {wire: ok, errors: ok, state: ok, persist: ok, note: "same validation; UPDATING then ACTIVE"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateQueryLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateScraper: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateScraperLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "upsert: there is no Create operation for scraper logging in the SDK"}
  UpdateWorkspaceAlias: {wire: ok, errors: ok, state: ok, persist: ok, note: "204 with no body (the one bodyless success)"}
  UpdateWorkspaceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH; settings stored and returned, enforcement is listed in items_still_open"}
families:
  Workspace: {status: ok, note: "Singleton sub-resources (alert manager, logging, query logging, configuration, resource policy) live inside the workspace record; mutations need an ACTIVE workspace (ConflictException otherwise). Deleting workspaces purge lazily on the next write."}
  RuleGroupsNamespace: {status: ok, note: "YAML validated; ARN arn:aws:aps:<region>:<account>:rulegroupsnamespace/<workspace>/<name>."}
  Scraper: {status: ok, note: "Region and account scoped; EKS source validated through internal/app/amp_wiring.go; scraper logging configuration is a child of the scraper."}
  AnomalyDetector: {status: ok, note: "Random Cut Forest configuration stored and returned; no model runs."}
  Tags: {status: ok, note: "Workspaces, rule groups namespaces, scrapers and anomaly detectors are taggable; keys starting with aws: are rejected."}
  RemoteWrite: {status: ok, note: "POST /workspaces/{id}/api/v1/remote_write: snappy block + protobuf WriteRequest decoded with klauspost/compress/snappy and protowire; samples kept in memory per series, ordered by time, a repeated timestamp is a duplicate; NaN and Inf values (including stale markers) are dropped; persisted in the snapshot."}
  Query: {status: ok, note: "GET and POST (form body) of /api/v1/query, query_range, series, labels and label/{name}/values return Prometheus JSON envelopes (success, bad_data 400, execution 422)."}
  PromQL: {status: ok, note: "services/amp/promql: instant and range selectors with =, !=, =~, !~ (fully anchored) and offset; rate, irate, increase, delta, idelta, changes, resets and the *_over_time family with Prometheus extrapolation; abs, ceil, floor, sqrt, exp, ln, log2, log10, round, clamp*, absent, time, vector, scalar, sort, sort_desc; sum, avg, min, max, count, group, stddev, stdvar, quantile, topk, bottomk with by and without; + - * / % ^, comparisons with bool, and, or, unless with on and ignoring."}
gaps: []
items_still_open:
  - PromQL: subqueries, the @ modifier, histogram_quantile and other histogram functions, label_replace, label_join, group_left and group_right matching, count_values, predict_linear, deriv, holt_winters and the date and time functions are not implemented (a parse error is returned)
  - RuleGroupsNamespace: rules are validated and stored but never evaluated, so recording rules produce no series and alerts never fire; rule expressions get only a balanced-bracket check, not a full parse
  - AlertManagerDefinition: validated and stored; no alert manager runs and no notification is sent
  - Scraper: stored with a real ACTIVE lifecycle but nothing scrapes the EKS or VPC source
  - WorkspaceConfiguration: retentionPeriodInDays, outOfOrderTimeWindowInSeconds, ruleQueryOffsetInSeconds and limitsPerLabelSet are stored and returned but not enforced on ingest or query
  - RemoteWrite: out-of-order samples are always accepted, native histograms and exemplars in the request are ignored, per-workspace ingestion limits and throttling do not exist
  - Query: query_exemplars, metadata, targets, rules, alerts and status endpoints are not served; the query timeout parameter is ignored
  - LoggingConfiguration: the CloudWatch Logs log group ARN is checked for shape only, never looked up, and no log is delivered
  - Authorization: SigV4 signatures and resource policies are not evaluated on the data plane
  - CreateWorkspace: the 25 workspaces per Region quota and KMS key existence are not enforced
  - ResourceGroupsTaggingAPI: AMP tags are not exposed through the tagging API
structural_gaps:
  - "No ingestion at scale or durable storage: samples live in process memory (and the persistence snapshot), there is no compaction, replication or long-term object storage."
  - "The default scraper configuration document returned by GetDefaultScraperConfiguration is AWS-authored text; the emulator serves an equivalent Kubernetes service-discovery configuration."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; transitions are lazy deadlines read under the backend lock, no goroutines or timers."}
---

## Notes

Protocol is REST-JSON (signing name `aps`); timestamps are epoch-seconds numbers (`awstime.Epoch`) and blobs are base64. Shapes, URIs and status codes were read from the pinned SDK `schemas/schemas.go` (this SDK has no `serializers.go`). Errors carry `X-Amzn-Errortype` plus `message` and `Message`; ResourceNotFound and Conflict add `resourceId` and `resourceType`.

Routing: control-plane paths (`/workspaces`, `/scrapers`, `/tags/{arn}`, `/scraperconfiguration`) match only a SigV4 signing name of `aps` or the SDK user agent, so Grafana, Bedrock Agent and Network Manager keep their own `/tags/` and `/workspaces` paths (`TestTagPathsStayWithTheirService`). The Prometheus data-plane paths under `/workspaces/{id}/api/v1/` also match unsigned requests, so plain `curl` and Prometheus without sigv4 work.

Region and account come from the request (SigV4 scope and access key), so workspaces and scrapers are isolated per region and account.

Emulator choices the SDK does not document: a rejected rules or alert manager file is an immediate ValidationException; the lifecycle dwell follows `AMP_LIFECYCLE_DELAY` (default 500ms); deleted resources disappear on the first write after their window; workspace retention defaults to 150 days; ClientToken replays CreateWorkspace, CreateRuleGroupsNamespace, CreateScraper, CreateAnomalyDetector and the alert manager and logging creates.

A Prometheus server can write to `<endpoint>api/v1/remote_write` (the `PrometheusEndpoint` of the workspace) and Grafana can query `<endpoint>`.
