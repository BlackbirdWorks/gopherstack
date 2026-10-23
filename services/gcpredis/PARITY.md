---
service: gcpredis
sdk_module: cloud.google.com/go/redis@v1.26.0
last_audit_commit: c7e82dfd3
last_audit_date: 2026-10-21
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns a pending operation; a background job starts the instance's own Redis server and moves it CREATING to READY. instance_id is 1-40 lowercase letters, digits or hyphens. tier BASIC or STANDARD_HA, memory_size_gb 1-300, redis_version REDIS_3_2 to REDIS_7_2 (default REDIS_7_0), zones must belong to the location, replica_count and read_replicas_mode follow the tier. Defaults match the service: DIRECT_PEERING, transit encryption DISABLED, persistence DISABLED, default network."}
  GetInstance: {wire: ok, errors: ok, state: ok, persist: n/a, note: "host and port are the live Redis endpoint; nodes and read_endpoint are derived for STANDARD_HA."}
  ListInstances: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged through pkgs/page; location - lists every location."}
  UpdateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask is required. Mutable: display_name, labels, memory_size_gb, redis_configs, replica_count, maintenance_policy, auth_enabled, persistence_config, read_replicas_mode; anything else is INVALID_ARGUMENT. Enabling AUTH issues a new string, disabling clears it, both apply to the live server."}
  DeleteInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stops the instance's Redis server and releases its port."}
  UpgradeInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "The target version must be supported and strictly newer."}
  FailoverInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "STANDARD_HA only; swaps current_location_id between the two zones. data_protection_mode is accepted; there is no replication lag to lose."}
  ImportInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "Reads an RDB file from the GCS emulator and replaces the keyspace. A bad URI is INVALID_ARGUMENT; a missing object or unreadable file fails the operation and the instance returns to READY."}
  ExportInstance: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Writes a version-9 RDB file to the GCS emulator; the bucket must exist."}
  GetInstanceAuthString: {wire: ok, errors: partial, state: ok, persist: ok, note: "FAILED_PRECONDITION when AUTH is not enabled (the real error code is unverified)."}
  RescheduleMaintenance: {wire: ok, errors: ok, state: ok, persist: ok, note: "IMMEDIATE, NEXT_AVAILABLE_WINDOW (next occurrence of the policy window, else the next UTC midnight) and SPECIFIC_TIME (future) set maintenance_schedule. No maintenance is ever run."}
  ListLocations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "google.cloud.location.Locations; each location carries LocationMetadata with its zones."}
  GetLocation: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  GetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.longrunning.Operations via pkgs/gcplro, also on the REST path projects/*/locations/*/operations/*."}
families:
  endpoint: {status: ok, note: "Each READY instance owns a miniredis server on a port from the shared pool (loopback ephemeral when none is configured). auth_enabled requires AUTH with the generated string; transit_encryption_mode SERVER_AUTHENTICATION serves TLS from a per-instance CA that is returned in server_ca_certs."}
  routing_isolation: {status: ok, note: "Own gRPC server with the REST transcoder bound in StartWorker to --gcp-redis-port / GCP_REDIS_PORT (default 8133, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool."}
  auth: {status: partial, note: "No API request is authenticated or authorised; authorized_network and IAM are not enforced."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; RPC failures are logged at debug. gRPC calls are not fed to the shared telemetry operation metrics."}
items_still_open:
  - "redis_configs are validated against the supported key set and Redis version and reported back, but miniredis has no CONFIG command so they never change engine behaviour (eviction, keyspace notifications, defrag)."
  - "memory_size_gb is not enforced as a maxmemory limit."
  - "Import supports RDB files that use plain encodings (strings, lists, sets, sorted sets, hashes, LZF and integer strings); files written by a real Redis server with ziplist, listpack, intset or stream encodings fail with INVALID_ARGUMENT. Export fails on instances holding streams and drops hash field TTLs."
  - "STANDARD_HA and read replicas are metadata only: one server serves both the primary and read endpoint (read_endpoint equals host), and failover swaps zone labels."
  - "RDB persistence_config is recorded but no periodic snapshots are taken (the emulator's own snapshot persistence covers restarts)."
  - "After a restore every instance starts on a new port and, for TLS, a new CA."
  - "Cross-region replication, customer-managed keys, maintenance_version and suspension reasons are stored or empty but not modelled."
leaks: {status: clean, note: "Background resources are the grpc.Server, per-instance Redis servers and operation jobs, all stopped and joined by Shutdown and Reset."}
---

Initial implementation (2026-10-21): CloudRedis gRPC and REST verified against cloud.google.com/go/redis v1.26.0 through the official client in the unit and integration tests, and against the hashicorp/google provider (google_redis_instance) in test/terraform/gcp.
