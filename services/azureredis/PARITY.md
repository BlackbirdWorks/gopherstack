---
service: azureredis
sdk_module: azure-sdk-for-go/sdk/resourcemanager/redis/armredis/v3@v3.3.0   # go.mod dependency (test-only): internal/app/azureredis_sdk_test.go drives Microsoft.Cache through ARM
last_audit_commit: 364908f83
last_audit_date: 2026-10-22
overall: B
ops:
  CreateOrUpdate: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT Microsoft.Cache/redis/{name}: 201 on create with Azure-AsyncOperation and provisioningState Creating until the injected clock passes a short creation delay, 200 on update. sku (Basic/Standard C0-C6, Premium P1-P5), enableNonSslPort, minimumTlsVersion, publicNetworkAccess, redisConfiguration (defaults per size are filled in), redisVersion, zones, identity, tags; unmodelled properties round-trip. Each cache starts a real Redis server (pkgs/redisengine) behind a TLS port and an optional plaintext port."}
  Update: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH merges tags, sku, enableNonSslPort, minimumTlsVersion and redisConfiguration (key by key); toggling enableNonSslPort opens or closes the plaintext port and minimumTlsVersion applies to the next TLS handshake."}
  Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE answers 202 with a finished async operation; a missing cache answers 204. Stops the cache's servers and releases its ports."}
  ListKeys: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST listKeys returns primaryKey and secondaryKey (32 random bytes, base64). Both keys authenticate over RESP2 (AUTH) and RESP3 (HELLO AUTH)."}
  RegenerateKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "keyType Primary or Secondary; the replaced key stops authenticating for new connections while the other key keeps working. authnotrequired=true in redisConfiguration turns key authentication off."}
  ForceReboot: {wire: ok, errors: ok, state: ok, persist: ok, note: "rebootType PrimaryNode, SecondaryNode (not on Basic) or AllNodes. Data survives when a replica remains (Standard/Premium, one node) or when persistence is on (Premium with rdb-backup-enabled or aof-backup-enabled, AllNodes); a Basic cache, or Standard AllNodes, is flushed. Open client connections are not dropped."}
  ImportData: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST import with format RDB and one file: a Blob Storage URL (virtual-hosted or path style, SAS query ignored). The RDB is read from the Azure Blob emulator and replaces the keyspace (pkgs/redisengine RDB reader: plain encodings, LZF, integer strings). The operation completes before the 202 is returned."}
  ExportData: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST export with format RDB, prefix and a container URL: writes <prefix>.rdb (RDB v9) to the container in the Azure Blob emulator. The SAS token is not validated."}
  FirewallRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT/GET/DELETE and list of redis/{name}/firewallRules/{rule} with IPv4 startIP/endIP validation. Rules are stored and returned; connections are not filtered by them."}
  PatchSchedules: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT/GET/DELETE and list of redis/{name}/patchSchedules/default for Premium caches; scheduleEntries (dayOfWeek, startHourUtc 0-23, maintenanceWindow) are validated and stored. No patching is performed."}
  LinkedServers: {wire: ok, errors: ok, state: partial, persist: ok, note: "PUT/GET/DELETE and list of redis/{name}/linkedServers/{linked} between two existing Premium caches (serverRole Primary or Secondary, name equals the linked cache). Metadata only: data is not replicated."}
families:
  arm_integration: {status: ok, note: "services/azurearm Microsoft.Cache provider (rp_redis*.go): ARM keeps resource metadata, firewall rules, patch schedule and linked servers; the Redis servers, access keys and data live in this service behind the azurearm.Redis seam (adapter in internal/app/azureredis_wiring.go). Applied, re-planned with no drift and destroyed by test/terraform/azure/redis_test.go, which connects over TLS with the primary key."}
  endpoints: {status: ok, note: "hostName is <name>.redis.cache.windows.net; sslPort and port (plaintext, only while enableNonSslPort) are the real emulator ports. The TLS endpoint always serves a self-signed or configured (--azure-redis-tls-cert) certificate valid for localhost and 127.0.0.1; dial the host and set the TLS server name to localhost."}
  sdk_compat: {status: ok, note: "armredis v3: create/update/delete pollers, ListKeys, RegenerateKey, ForceReboot, ImportData, ExportData, FirewallRules and PatchSchedules; azurerm_redis_cache applies without drift."}
gaps:
  - "Microsoft.Cache/redisEnterprise (Redis Enterprise and Azure Managed Redis) is not implemented."
  - "Clustering (shardCount, replicasPerPrimary), VNet injection, private endpoints, zones allocation and geo-replication data flow are not modelled; a cache is one Redis server."
  - "redisConfiguration is stored and echoed; miniredis has no CONFIG, so maxmemory-policy, maxclients and notify-keyspace-events are not enforced. rdb/aof backup settings only decide whether a full reboot keeps data."
  - "disableAccessKeyAuthentication and Entra ID (aad-enabled) authentication, access policies and policy assignments are not implemented."
  - "Reboots do not drop open connections; firewall rules do not filter connections."
  - "Import accepts one RDB file; compact ziplist/listpack/intset encodings and streams fail with a 400."
  - "Create completes the server immediately; only the reported provisioningState lags."
deferred: []
items_still_open:
  - "Microsoft.Cache/redisEnterprise (Redis Enterprise and Azure Managed Redis) is not implemented."
  - "Clustering, VNet injection, private endpoints and geo-replication data flow are not modelled."
  - "redisConfiguration settings are stored but not enforced (no CONFIG in miniredis)."
  - "Entra ID authentication, disableAccessKeyAuthentication and access policies are not implemented."
  - "Reboots do not drop connections and firewall rules do not filter them."
  - "No dashboard page."
leaks: {status: clean, note: "Reset and Shutdown close every cache's relays and Redis server and release their ports; the package runs under testleak."}
---

## Notes

Each cache is a loopback miniredis server fronted by relays: a TLS relay (always) and a plaintext relay (only while enableNonSslPort), both on ports from the shared port pool. Access keys live in a per-cache keyring consulted by a pre-command hook that rewrites the secondary key to the primary one in AUTH and HELLO, because miniredis holds a single password.

Persistence stores each cache's keys, spec and keyspace; servers restart on fresh ports and ARM reads the new ports on the next GET.
