---
service: gcpredis
sdk_module: cloud.google.com/go/redis@v1.26.0
last_audit_commit: c7e82dfd3
last_audit_date: 2026-10-21
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "redis_configs are validated against the documented keys, versions and ranges and applied to the server (maxmemory and eviction, keyspace notifications, databases, stream node sizes, LFU tuning, idle timeout). Returns a pending operation; a background job starts the instance's own Redis server and moves it CREATING to READY. instance_id is 1-40 lowercase letters, digits or hyphens. tier BASIC or STANDARD_HA, memory_size_gb 1-300, redis_version REDIS_3_2 to REDIS_7_2 (default REDIS_7_0), zones must belong to the location, replica_count and read_replicas_mode follow the tier. Defaults match the service: DIRECT_PEERING, transit encryption DISABLED, persistence DISABLED, default network."}
  GetInstance: {wire: ok, errors: ok, state: ok, persist: n/a, note: "host and port are the live Redis endpoint; nodes and read_endpoint are derived for STANDARD_HA."}
  ListInstances: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged through pkgs/page; location - lists every location."}
  UpdateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask is required. Mutable: display_name, labels, memory_size_gb, redis_configs, replica_count, maintenance_policy, auth_enabled, persistence_config, read_replicas_mode; anything else is INVALID_ARGUMENT. Enabling AUTH issues a new string, disabling clears it, both apply to the live server. A resize or redis_configs change is applied to the running server when the operation settles; the databases config can only be set at creation."}
  DeleteInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stops the instance's Redis server and releases its port."}
  UpgradeInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "The target version must be supported and strictly newer."}
  FailoverInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "STANDARD_HA only; swaps current_location_id between the two zones. data_protection_mode is accepted; there is no replication lag to lose."}
  ImportInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "Reads an RDB file from the GCS emulator and replaces the keyspace, including streams with consumer groups and pending entries and hash field TTLs. A bad URI is INVALID_ARGUMENT; a missing object or unreadable file fails the operation and the instance returns to READY."}
  ExportInstance: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Writes an RDB file to the GCS emulator (version 9, 11 when streams are present, 12 when hash fields carry TTLs); the bucket must exist."}
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
  - "STANDARD_HA and read replicas are metadata only: one server serves both the primary and read endpoint (read_endpoint equals host), and failover swaps zone labels. Real replicas need a replication path (miniredis registers no REPLICAOF or PSYNC), a second port per instance from the shared pool (engine.go bindEngine publishes one host and port) and READONLY enforcement on the replica."
  - "After a restore every instance starts on a new port and, for TLS, a new CA: pkgs/portalloc has no acquire-specific-port call (Reserve marks the port reserved for good) and pkgs/devtls.CA cannot serialise its private key."
  - "Cross-region replication, customer-managed keys, maintenance_version and suspension reasons are stored or empty but not modelled."
leaks: {status: clean, note: "Background resources are the grpc.Server, per-instance Redis servers and operation jobs, all stopped and joined by Shutdown and Reset."}
---

CloudRedis gRPC and REST verified against cloud.google.com/go/redis v1.26.0 through the official client in the unit and integration tests, and against the hashicorp/google provider (google_redis_instance) in test/terraform/gcp.

ImportInstance decodes the packed RDB encodings through pkgs/redisengine (ziplist, intset, quicklist and quicklist2, listpack hashes, sets and sorted sets), verified against RDB files written by redis 6.2, 7.0 and 7.2 servers (pkgs/redisengine/testdata).

customer_managed_key resolves against gcpkms when wired: CreateInstance fails (instance not created) unless the key exists in the instance's region with an enabled primary version and ENCRYPT_DECRYPT purpose; global and multi-region keys do not match a region. An instance whose key later becomes unusable reads with no state and suspension_reasons CUSTOMER_MANAGED_KEY_ISSUE, rejects mutating calls, and resumes when the key is usable again (docs.cloud.google.com/memorystore/docs/redis/about-cmek).

redis_configs follow docs.cloud.google.com/memorystore/docs/redis/supported-redis-configurations: maxmemory-gb (20% of capacity up to capacity), maxmemory-policy (default volatile-lru), activedefrag, databases (1-100, creation only), lazyfree-lazy-eviction, -expire, -user-del and -user-flush (7.0+), lfu-decay-time and lfu-log-factor (4.0+, positive), maxmemory-clients (7.0+, N%), notify-keyspace-events (K E g $ l s h z x e A), stream-node-max-bytes and -max-entries (5.0+, >= 0) and timeout (0 or >= 120). slowlog-* and replica-ignore-maxmemory are not in the page and are rejected; the slowlog runs with the page's fixed defaults (10000 microseconds, 128 entries).

Applied to the server through pkgs/redisengine Tune: maxmemory is maxmemory-gb (default the instance capacity); writes over it evict by policy or fail with the OOM error; notify-keyspace-events publishes keyspace and keyevent messages including expired and evicted; databases bounds SELECT; stream-node limits shape exported stream listpacks (redis.io/docs/latest/develop/data-types/streams); lfu settings drive allkeys-lfu and volatile-lfu (redis.io/docs/latest/develop/reference/eviction); timeout closes idle clients; SLOWLOG GET, LEN and RESET and INFO server, memory, keyspace and stats counters answer from the server.

RDB reading and writing follow rdb.h and rdb.c of the redis 8.0 source (RDB_TYPE_STREAM_LISTPACKS, _2 and _3, RDB_TYPE_HASH_METADATA and RDB_TYPE_HASH_LISTPACK_EX), verified against files written by redis 6.2, 7.0, 7.2 and 8.0 servers and by loading the emulator's exports into redis 7.2 and 8.0 servers.

## Emulator choices where the vendor is silent

- used_memory is estimated from the size of each key's data plus 64 bytes per key; one GiB of memory_size_gb is 2^30 bytes.
- Eviction compares every key rather than sampling five, so the victim is deterministic.
- An idle client past timeout is disconnected when it next sends a command instead of by a timer.
- INFO server reports redis_version as the enum's major and minor with patch 0.
- Commands a Lua script runs are not counted for LRU or LFU recency and publish no keyspace notifications, and neither does the script call itself; the script's writes are measured on the next memory check.
- RDB hash zipmap and module values are rejected with INVALID_ARGUMENT.
- Stream entries_added and max_deleted_entry_id are written as the entry count and 0-0.

## External effects that are recorded, not performed

- activedefrag and the lazyfree-lazy-* settings are applied to the server's settings and nothing else: the emulator has no allocator fragmentation to defragment and frees keys synchronously, so INFO memory reports active_defrag_running:0 and lazyfree_pending_objects:0.
- maxmemory-clients is stored; client output buffers are not measured, so no client is evicted for memory.
- RDB persistence_config is stored and returned; no periodic RDB snapshots are written because the Redis engine keeps its keyspace in memory and the emulator's own snapshot persistence covers restarts.
