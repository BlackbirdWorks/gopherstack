---
service: gcpspanner
sdk_module: cloud.google.com/go/spanner@v1.96.1
last_audit_commit: 364908f83
last_audit_date: 2026-10-22
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns a pending operation; a background job mirrors the instance into the Spanner emulator (when a container runtime is reachable) and moves it CREATING to READY. Exactly one of node_count and processing_units is required (units: multiples of 100 below 1000, else of 1000) and the other is derived; config must be an existing instanceConfig (emulator-config, regional-*, nam3, nam6, nam-eur-asia1, eur3, eur5, asia1); display_name 4-30 characters. Edition defaults to STANDARD."}
  GetInstance: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListInstances: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged through pkgs/page. The filter parameter is UNIMPLEMENTED."}
  UpdateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "field_mask is required. Mutable: display_name, node_count, processing_units, labels, edition, default_backup_schedule_type; anything else is INVALID_ARGUMENT. Capacity changes keep node_count and processing_units consistent. Returns a finished operation."}
  DeleteInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "Removes the instance, its databases and its IAM policy, and deletes the instance in the emulator."}
  ListInstanceConfigs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "A fixed catalog of Google-managed configs per project."}
  GetInstanceConfig: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Instance-level policy."}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces the policy; a stale etag is ABORTED. update_mask is ignored."}
  TestIamPermissions: {wire: ok, errors: ok, state: partial, persist: n/a, note: "Reports every requested permission as granted: no caller is authenticated."}
  CreateDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "create_statement must be CREATE DATABASE `id`; ids are 2-30 characters of a-z, 0-9, _ and -. With the emulator the statements are executed there (DDL errors surface as the emulator reports them); without it they are stored unvalidated. The returned operation is already done."}
  GetDatabase: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Answered from the backend's own metadata."}
  ListDatabases: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  UpdateDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "Only enable_drop_protection; the emulator does not know about it, so protection is enforced by DropDatabase here."}
  UpdateDatabaseDdl: {wire: ok, errors: ok, state: ok, persist: ok, note: "With the emulator, executed there and the stored DDL is re-read from it (also after a failure, so partially applied batches stay accurate); a failing statement fails the operation. Without it the statements are only appended. operation_id is honoured."}
  DropDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "FAILED_PRECONDITION while drop protection is on."}
  GetDatabaseDdl: {wire: ok, errors: ok, state: ok, persist: ok, note: "The emulator's normalised DDL when it ran the statements, else the statements as submitted. proto_descriptors are not supported."}
  GetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.longrunning.Operations via pkgs/gcplro, also on projects/*/instances/*/operations/* and projects/*/instances/*/databases/*/operations/*; unknown names fall through to the emulator when it runs."}
families:
  data_plane: {status: partial, note: "Spanner (sessions, ExecuteSql, StreamingRead, Commit, transactions, partitions, BatchWrite) and the remaining DatabaseAdmin methods (backups, backup schedules, database roles) are proxied byte-for-byte over gRPC and REST to the official gcr.io/cloud-spanner-emulator/emulator image, one container started on first use and shared by every instance. Without a container runtime (or with --gcp-spanner-engine=none) these calls return UNAVAILABLE; before any instance exists they return NOT_FOUND."}
  routing_isolation: {status: ok, note: "Own gRPC server with the REST transcoder bound in StartWorker to --gcp-spanner-port / GCP_SPANNER_PORT (default 8135, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool."}
  auth: {status: partial, note: "No API request is authenticated or authorised."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; failed RPCs are logged at debug. gRPC calls are not fed to the shared telemetry operation metrics."}
items_still_open:
  - "Table data is in the emulator's memory only: it is lost on restart. Persistence stores instances, policies, databases and their DDL, and replays the schema into a fresh emulator on first use."
  - "Without a container runtime database DDL is stored but not validated or executed: GetDatabaseDdl returns the statements as submitted, DROP and ALTER statements are not folded in."
  - "RestoreDatabase and ListDatabaseOperations are UNIMPLEMENTED: restoring would create emulator databases the backend does not track. Backups created through the proxy live only in the emulator and are lost on restart."
  - "User-managed instance configs (Create/Update/DeleteInstanceConfig), instance partitions, MoveInstance, autoscaling_config, replica_compute_capacity and the ListInstances filter are not implemented."
  - "The emulator runs every instance in its single emulator-config with 1 node; node_count and processing_units are metadata only, and the data plane is one shared emulator process."
  - "Customer-managed encryption on databases is UNIMPLEMENTED; backend instance and database create_time values are not shared with the emulator."
leaks: {status: clean, note: "Background resources are the grpc.Server, the emulator container with its gRPC client and the operation jobs, all stopped and joined by Shutdown and Reset."}
---

Initial implementation (2026-10-22): instance admin in-process, database metadata in-process with DDL executed by the official emulator image, data plane proxied. Verified against cloud.google.com/go/spanner v1.95.0 (data, instance and database admin clients) in services/gcpspanner, and against the hashicorp/google provider (google_spanner_instance, google_spanner_database) in test/terraform/gcp.
