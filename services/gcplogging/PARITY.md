---
service: gcplogging
sdk_module: cloud.google.com/go/logging@v1.18.0
last_audit_commit: b1f3fa139
last_audit_date: 2026-10-21
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  WriteLogEntries: {wire: ok, errors: partial, state: ok, persist: ok, note: "log_name, resource and labels defaults from the request; entry labels win. insert_id defaults to a random id, timestamp to now, receive_timestamp is always set. A duplicate (log_name, insert_id, timestamp) is stored once per bucket. partial_success writes the valid entries and returns INVALID_ARGUMENT with a WriteLogEntriesPartialErrors detail keyed by index; without it any invalid entry fails the whole request. dry_run validates only. Entries are routed through every enabled sink of the log's parent, counted by log-based metrics and fed to tails. Limits: 10000 entries per request, 256 KiB per entry, 64 labels. The exact messages of the live service were not available."}
  ListLogEntries: {wire: ok, errors: partial, state: ok, persist: n/a, note: "resource_names: projects/P (reads _Default and _Required), or a bucket view name (reads that bucket, narrowed by the view filter). order_by is timestamp asc (default) or desc. page_size default and max 1000, offset-based tokens via pkgs/page. Without a timestamp restriction in the filter only the last 24h is searched, as in the live service. Entries past bucket retention are hidden."}
  ListLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Distinct full log names that hold entries, sorted, paged."}
  DeleteLog: {wire: ok, errors: ok, state: ok, persist: ok, note: "Removes the log's entries from every bucket of the parent; NOT_FOUND when it holds none."}
  TailLogEntries: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Bidirectional gRPC stream; the first request carries resource_names (up to 100) and filter. New entries of those parents that match the filter are batched (up to 100) into responses; a full 1024-entry buffer drops entries and reports suppression_info RATE_LIMIT. buffer_window is ignored and keepalive responses are not sent. At most 10 concurrent tails. Not available over REST (the live service has no REST tail either)."}
  ListMonitoredResourceDescriptors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "A static catalog of 23 common resource types (global, gce_instance, k8s_container, cloud_run_revision, cloud_function, gcs_bucket, pubsub_topic ...), paged."}
  CreateSink: {wire: ok, errors: partial, state: ok, persist: ok, note: "Name [A-Za-z0-9_.-]{1,100}; destination must be pubsub.googleapis.com/projects/P/topics/T, storage.googleapis.com/BUCKET, bigquery.googleapis.com/projects/P/datasets/D or logging.googleapis.com/projects/P[/locations/L/buckets/B]. Filter and exclusions are parsed. writer_identity is the shared cloud-logs service account or, with unique_writer_identity, a stable derived gcp-sa-logging account. Destination existence is not checked. name is returned as the short id."}
  GetSink: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Includes the built-in _Default and _Required sinks of each parent."}
  ListSinks: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged."}
  UpdateSink: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask paths: destination, filter, description, disabled, exclusions, include_children, output_version_format, bigquery_options(.use_partitioned_tables); an empty mask replaces the mutable fields. _Required cannot be modified."}
  DeleteSink: {wire: ok, errors: ok, state: ok, persist: ok, note: "_Required cannot be deleted."}
  CreateExclusion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Project, folder, organization and billing account parents. Exclusions drop entries from the _Default sink's bucket only; entries still reach user-defined sinks."}
  GetExclusion: {wire: ok, errors: ok, state: ok, persist: n/a, note: "name is returned as the short id."}
  ListExclusions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged."}
  UpdateExclusion: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask required: description, filter, disabled."}
  DeleteExclusion: {wire: ok, errors: ok, state: ok, persist: ok, note: "NOT_FOUND for unknown names."}
  CreateBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "retention_days 1..3650 (default 30), locked, restricted_fields, index_configs, analytics_enabled and cmek_settings are stored. Every bucket gets an _AllLogs view. Each parent has _Default (30 days) and _Required (400 days, locked) buckets created on first use."}
  CreateBucketAsync: {wire: ok, errors: ok, state: ok, persist: ok, note: "Applies the change synchronously and returns an already finished longrunning Operation (BucketMetadata SUCCEEDED, response LogBucket) that the Operations service can also return."}
  GetBucket: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Full resource name."}
  ListBuckets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Location - lists every location."}
  UpdateBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask required. A locked bucket cannot shorten retention or unlock; _Required cannot be updated."}
  UpdateBucketAsync: {wire: ok, errors: ok, state: ok, persist: ok, note: "As CreateBucketAsync."}
  DeleteBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "Marks the bucket DELETE_REQUESTED (its entries stop being listed and routed to); it is removed after 7 days. _Default, _Required and locked buckets cannot be deleted."}
  UndeleteBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "Restores a DELETE_REQUESTED bucket inside the 7 day window."}
  CreateView: {wire: ok, errors: ok, state: ok, persist: ok, note: "Filter is parsed; reading through a view applies its filter."}
  GetView: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Includes _AllLogs."}
  ListViews: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name."}
  UpdateView: {wire: ok, errors: ok, state: ok, persist: ok, note: "Mask paths description, filter. _AllLogs is immutable."}
  DeleteView: {wire: ok, errors: ok, state: ok, persist: ok, note: "_AllLogs cannot be deleted."}
  GetCmekSettings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Empty settings until updated."}
  UpdateCmekSettings: {wire: ok, errors: ok, state: ok, persist: ok, note: "kms_key_name is format-checked and stored with a derived service_account_id; it is never used to encrypt anything or checked against Cloud KMS."}
  GetSettings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Empty settings until updated."}
  UpdateSettings: {wire: ok, errors: ok, state: ok, persist: ok, note: "kms_key_name, storage_location and disable_default_sink; disable_default_sink disables the parent's _Default sink."}
  CreateLogMetric: {wire: ok, errors: ok, state: ok, persist: ok, note: "Filter is parsed. metric_descriptor is filled with DELTA, INT64 (DISTRIBUTION when a value_extractor is set), unit 1, type logging.googleapis.com/user/NAME and name. EXTRACT(field) and REGEXP_EXTRACT(field, regex) extractors are compiled; distribution metrics need linear, exponential or explicit bucket_options."}
  GetLogMetric: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Short id in name."}
  ListLogMetrics: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged."}
  UpdateLogMetric: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces the metric and creates it when missing. Series are kept unless the extractor or buckets change."}
  DeleteLogMetric: {wire: ok, errors: ok, state: ok, persist: ok, note: "NOT_FOUND for unknown names."}
  CreateLink: {wire: gap, errors: gap, state: gap, persist: n/a, note: "UNIMPLEMENTED (also GetLink, ListLinks, DeleteLink): linked BigQuery datasets are not served."}
  CopyLogEntries: {wire: gap, errors: gap, state: gap, persist: n/a, note: "UNIMPLEMENTED."}
families:
  filter_language: {status: partial, note: "Lexer and parser for the Logging query language: comparisons = != < <= > >= : =~ !~ on any field path (camelCase JSON names, quoted segments such as labels.\"a.b\", [n] indexes, arrays match on any element), AND, OR, NOT and -, implicit AND, parentheses, value groups like field:(a OR b), bare and quoted text, -- comments, and the functions LOG_ID(), SEARCH() and sample(). OR binds tighter than AND as documented. String comparison is case-insensitive, =~ is case-sensitive RE2. severity compares by level, timestamp by instant (RFC3339 or a date). A missing field makes != and !~ true. protoPayload fields are searchable only when the Any type is registered in the binary."}
  sink_routing: {status: partial, note: "WriteLogEntries evaluates each enabled sink of the parent synchronously. Pub/Sub: one message per entry, data is the LogEntry JSON, attribute logging.googleapis.com/timestamp. Cloud Storage: newline-delimited LogEntry JSON appended to an hourly object LOG_ID/YYYY/MM/DD/HH:00:00_HH:59:59_S0.json (the live export batches per hour with a delay; the emulator appends immediately). BigQuery: rows in LOG_ID_YYYYMMDD tables (the LOG_ID table when use_partitioned_tables), created on first use with a fixed schema in which payloads, resource, labels and httpRequest are JSON columns. Log buckets: entries are stored in the bucket. Export failures are logged and never fail the write. Destinations come from the in-process Pub/Sub, Storage and BigQuery emulators wired in internal/app."}
  retention: {status: ok, note: "Entries older than the bucket's retention_days (by receive time) are dropped on write and hidden on read; each bucket keeps at most 50000 entries."}
  log_metrics: {status: partial, note: "Counter and distribution metrics are evaluated on every write after creation. The accumulated series are readable through InMemoryBackend.MetricPoints and persist; there is no Cloud Monitoring emulator so they cannot be queried over an API."}
  routing_isolation: {status: ok, note: "Own gRPC server bound in StartWorker to --gcp-logging-port / GCP_LOGGING_PORT (default 8131, fails fast if taken); pkgs/gcprest serves the same API as REST/JSON (/v2/...) on that port. Never registered on the shared AWS router; cli.go reserves the port in the shared PortAlloc pool. Go clients use option.WithEndpoint with insecure credentials; terraform uses logging_custom_endpoint = http://localhost:8131/v2/."}
  auth: {status: partial, note: "No request is authenticated or authorised; writer identities receive no permissions."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state. RPC failures are logged at debug; gRPC calls are not fed to the shared telemetry operation metrics."}
items_still_open:
  - "Cloud Run and Cloud Functions container output is not written to Cloud Logging: those emulators do not capture container stdout, so there is no hook yet."
  - "Linked datasets (CreateLink, GetLink, ListLinks, DeleteLink) and CopyLogEntries return UNIMPLEMENTED."
  - "Sinks on folders, organizations and billing accounts are stored, but include_children aggregation is not applied: only entries written to that parent's own logs are routed."
  - "Sink destinations are not checked for existence at create time, and a missing destination only logs a warning when an entry is exported."
  - "Cloud Storage export appends to the hourly object immediately instead of batching with the live service's delay; BigQuery tables use a fixed schema with JSON columns instead of the live typed schema, and partitioned tables are plain tables."
  - "Log-based metric series are kept in memory and persisted but are not exposed through a Cloud Monitoring API."
  - "CMEK keys and settings are stored only; nothing is encrypted and the key is not validated against Cloud KMS."
  - "Error messages and the edge cases of the live service (entry timestamp bounds, quota errors, SaveStatus details) were not verified against it; ListLogEntries default page size and Tail buffer_window behaviour are approximations."
  - "Bucket index_configs, restricted_fields and analytics_enabled are stored but have no effect on queries."
leaks: {status: clean, note: "The only background resource is the grpc.Server (and its gcprest bridge), stopped and joined by Shutdown; tail streams end when the server stops. goleak runs in TestMain."}
---

Initial implementation (2026-10-21): LoggingServiceV2, ConfigServiceV2 and MetricsServiceV2 verified through the official cloud.google.com/go/logging client, logadmin and the generated apiv2 clients in the package tests, REST/JSON transcoding through resttest, sink routing against fake and real Pub/Sub, Storage and BigQuery emulators, and hashicorp/google in test/terraform/gcp (google_logging_project_sink, google_logging_metric, google_logging_project_exclusion).
