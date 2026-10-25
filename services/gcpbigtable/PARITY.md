---
service: gcpbigtable
sdk_module: cloud.google.com/go/bigtable@v1.58.0
last_audit_commit: fa4903d9d
last_audit_date: 2026-10-25
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "Instance plus clusters in one call, returned as an already finished operation. Ids follow the documented patterns, display_name and a cluster are required, PRODUCTION clusters need serve_nodes or an autoscaling config (limits, 10-80 cpu target), one cluster per location, DEVELOPMENT takes exactly one cluster without nodes."}
  GetInstance: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListInstances: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name; no paging (the request has no page size)."}
  UpdateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "Display name and type."}
  PartialUpdateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask required: display_name, type, edition, labels and labels.key. DEVELOPMENT to PRODUCTION only."}
  DeleteInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "Removes clusters, profiles, tables, rows, backups and policies."}
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "Finished operation returning the cluster; location must be projects/{p}/locations/{l} and unused in the instance."}
  GetCluster: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: n/a, note: "instances/- lists the clusters of every instance in the project."}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "Sets serve_nodes and removes autoscaling."}
  PartialUpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "serve_nodes and cluster_config.cluster_autoscaling_config (and its limit and target sub-paths); other fields are immutable."}
  DeleteCluster: {wire: ok, errors: partial, state: ok, persist: ok, note: "The last cluster of an instance and clusters an app profile routes to are FAILED_PRECONDITION (codes not verified against production). Deletes the cluster's backups."}
  CreateAppProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "A routing policy is required; cluster ids must exist. ignore_warnings is accepted but no warnings are evaluated."}
  GetAppProfile: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListAppProfiles: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged through pkgs/page."}
  UpdateAppProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "Masked paths description, routing and isolation; an empty mask replaces everything. A stale etag is ABORTED."}
  DeleteAppProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Instance, table and backup policies."}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces the policy; a stale etag is ABORTED. update_mask is ignored."}
  TestIamPermissions: {wire: ok, errors: ok, state: partial, persist: n/a, note: "Reports every requested permission as granted: no caller is authenticated."}
  CreateTable: {wire: ok, errors: partial, state: ok, persist: ok, note: "Column families with GC rules and value types, initial split keys, MILLIS or MICROS granularity, deletion protection, change stream, automated backup policy. Returns the schema view."}
  ListTables: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NAME_ONLY by default; views, page_size and page_token."}
  GetTable: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NAME_ONLY, SCHEMA_VIEW (default), REPLICATION_VIEW, ENCRYPTION_VIEW and FULL; cluster_states report READY and the cluster's default or customer-managed encryption."}
  UpdateTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "deletion_protection, change_stream_config (1-7 days), automated_backup_policy (3-90 days), row_key_schema and tiered_storage_config through update_mask paths."}
  DeleteTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "FAILED_PRECONDITION while deletion protection is on."}
  ModifyColumnFamilies: {wire: ok, errors: partial, state: ok, persist: ok, note: "Atomic create, update (gc_rule, value_type) and drop; a dropped family's cells are deleted. Missing-family and duplicate codes (NOT_FOUND, ALREADY_EXISTS) are not verified against production."}
  DropRowRange: {wire: ok, errors: ok, state: ok, persist: ok, note: "Key prefix or all data."}
  GenerateConsistencyToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Tokens live in memory only."}
  CheckConsistency: {wire: ok, errors: partial, state: partial, persist: n/a, note: "Always consistent for a known token: replication is synchronous because clusters share one data copy. An unknown token is INVALID_ARGUMENT (code unverified)."}
  CreateBackup: {wire: ok, errors: ok, state: ok, persist: ok, note: "Deep copy of the table at call time; expire_time must be 6 hours to 90 days away; finished operation."}
  GetBackup: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  UpdateBackup: {wire: ok, errors: ok, state: ok, persist: ok, note: "expire_time only."}
  DeleteBackup: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ListBackups: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged; clusters/- spans the instance. filter and order_by are UNIMPLEMENTED."}
  RestoreTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "New table with the backup's schema and data and restore_info; may restore into another instance."}
  ReadRows: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Row keys and ranges (open/closed ends, validated), rows_limit, reversed scans, the complete RowFilter set, cell chunks that name row, family and qualifier only when they change, last_scanned_row_key for clients that ask, request stats iteration counts. GC rules hide expired cells."}
  SampleRowKeys: {wire: ok, errors: ok, state: partial, persist: n/a, note: "Samples are the table's split points with the cumulative bytes before them, plus the end-of-table marker; no size-based sampling."}
  MutateRow: {wire: ok, errors: partial, state: ok, persist: ok, note: "SetCell (server time -1, granularity check), DeleteFromColumn with time range, DeleteFromFamily, DeleteFromRow, applied atomically after validating every mutation. NOT_FOUND for an unknown family (unverified against production)."}
  MutateRows: {wire: ok, errors: ok, state: ok, persist: ok, note: "One status per entry; entries are independent, each atomic."}
  CheckAndMutateRow: {wire: ok, errors: ok, state: ok, persist: ok, note: "Predicate filter evaluated on the live row; a nil predicate matches any cell. Rejected on app profiles that do not allow single-row transactions."}
  ReadModifyWriteRow: {wire: ok, errors: ok, state: ok, persist: ok, note: "Append and 64-bit big-endian increment, rules see earlier rules, new cell at max(server time, latest timestamp); returns the cells written. Same app profile restriction as CheckAndMutateRow."}
  PingAndWarm: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NOT_FOUND for a missing instance."}
  GetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.longrunning.Operations via pkgs/gcplro on projects/*/instances/*/operations/*."}
families:
  row_filters: {status: ok, note: "chain, interleave (duplicates kept), condition, sink, pass/block all, row key, family, qualifier and value RE2 regexes (whole-match, \\C as any byte), column range, value range, timestamp range, cells per row offset/limit, cells per column limit, strip value, apply label, value bitmask, row sample. Invalid filters fail before any row is read."}
  garbage_collection: {status: ok, note: "max versions, max age, union and intersection (nested). Applied when reading, on every mutation of a row, when a policy changes and when snapshotting; there is no background collector, so results are exact rather than eventual."}
  timestamps: {status: ok, note: "Microsecond cell timestamps in a millisecond (default) or microsecond table; -1 selects server time; other values must be multiples of the granularity."}
  data_limits: {status: partial, note: "Row key 4096 bytes, qualifier 16384 bytes, cell 100 MiB, 100000 mutations per row and entries per request are enforced; total request and row sizes are not."}
  routing_isolation: {status: ok, note: "Own gRPC server with the REST transcoder bound in StartWorker to --gcp-bigtable-port / GCP_BIGTABLE_PORT (default 8137, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool. 8086, the conventional BIGTABLE_EMULATOR_HOST port, belongs to Secret Manager here."}
  auth: {status: partial, note: "No request is authenticated or authorised."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; RPC failures are logged at debug. gRPC calls are not fed to the shared telemetry operation metrics. No dashboard page."}
items_still_open:
  - "Aggregate column families (value_type.aggregate_type) and the AddToCell/MergeToCell mutations are UNIMPLEMENTED."
  - "Authorized views, logical views, materialized views, schema bundles, table snapshots, UndeleteTable, CopyBackup, memory layers and hot tablets are UNIMPLEMENTED; so are ReadChangeStream, GenerateInitialChangeStreamPartitions, PrepareQuery/ExecuteQuery and the session RPCs (OpenTable, GetClientConfiguration). change_stream_config is stored but no change records are produced."
  - "Operations are recorded already finished: there is no CREATING state or progress, and CancelOperation has no effect."
  - "Clusters share one copy of the data: replication, per-cluster storage and routing are not simulated, app profile routing is validated but does not place requests, and autoscaling never resizes a cluster."
  - "App profile warnings (ignore_warnings) are never raised, and storage-type specific autoscaling storage targets, KMS key rules and node scaling factor constraints are not validated."
  - "ListBackups filter and order_by, and backup hot_to_standard_time, are not supported."
  - "ReadRows never splits large cell values across chunks and reports only the iteration counts in request stats; SampleRowKeys does no size-based sampling."
  - "Several error codes were chosen without a production reference: unknown column family on mutate (NOT_FOUND), duplicate/missing family on ModifyColumnFamilies, unknown consistency token (INVALID_ARGUMENT), row_sample_filter outside (0, 1) and a chain or interleave of fewer than two filters are rejected as INVALID_ARGUMENT."
leaks: {status: clean, note: "The only background resources are the grpc.Server and its serve goroutine; Shutdown stops the server and joins it."}
---

Initial implementation (2026-10-25): google.bigtable.admin.v2 BigtableInstanceAdmin and BigtableTableAdmin plus google.bigtable.v2 Bigtable verified against cloud.google.com/go/bigtable v1.58.0 through the official admin, instance admin and data clients in the package tests.
