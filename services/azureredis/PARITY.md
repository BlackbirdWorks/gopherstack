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
  ForceReboot: {wire: ok, errors: ok, state: ok, persist: ok, note: "rebootType PrimaryNode, SecondaryNode (not on Basic) or AllNodes. Data survives on Standard/Premium for every reboot type (Learn: azure-cache-for-redis/cache-administration, "Will I lose data from my cache if I do a reboot?": rebooting both nodes does not flush); a Basic cache is a single node and is flushed. Open client connections are dropped by PrimaryNode and AllNodes and on Basic (Learn: same page, "Reboot" and "Can I reboot the cache to clear client connections?"); a SecondaryNode reboot leaves them."}
  ImportData: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST import with format RDB and one file: a Blob Storage URL (virtual-hosted or path style, SAS query ignored). The RDB is read from the Azure Blob emulator and replaces the keyspace (pkgs/redisengine RDB reader: plain encodings, LZF, integer strings). The operation completes before the 202 is returned."}
  ExportData: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST export with format RDB, prefix and a container URL: writes <prefix>.rdb (RDB v9) to the container in the Azure Blob emulator. The SAS token is not validated."}
  FirewallRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT/GET/DELETE and list of redis/{name}/firewallRules/{rule} with IPv4 startIP/endIP validation. Rules are stored, returned and passed to the data plane on every change: with no rules every client may connect, with rules only clients whose IPv4 address lies in a rule range (Learn: azure-cache-for-redis/cache-configure, Firewall; rules apply to new connections on both the TLS and plaintext ports, an unreadable client address is refused, and a refused client is closed before the TLS handshake). Rules persist with the cache spec."}
  PatchSchedules: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT/GET/DELETE and list of redis/{name}/patchSchedules/default for Premium caches; scheduleEntries (dayOfWeek, startHourUtc 0-23, maintenanceWindow) are validated and stored. No patching is performed."}
  LinkedServers: {wire: ok, errors: ok, state: partial, persist: ok, note: "PUT/GET/DELETE and list of redis/{name}/linkedServers/{linked} between two existing Premium caches (serverRole Primary or Secondary, name equals the linked cache). Metadata only: data is not replicated."}
  AccessPolicies: {wire: ok, errors: partial, state: ok, persist: ok, note: "PUT/GET/DELETE and list of redis/{name}/accessPolicies/{policy}: the built-ins Data Owner, Data Contributor and Data Reader (type BuiltIn, immutable) plus custom policies whose permissions parse as a Redis ACL. Verified against armredis v3 (internal/app/azureredis_sdk_test.go)."}
  AccessPolicyAssignments: {wire: ok, errors: partial, state: ok, persist: ok, note: "PUT/GET/DELETE and list of redis/{name}/accessPolicyAssignments/{name}; the resolved ACL is pushed to the data plane, which enforces it on Entra-authenticated connections."}
families:
  arm_integration: {status: ok, note: "services/azurearm Microsoft.Cache provider (rp_redis*.go): ARM keeps resource metadata, firewall rules, patch schedule and linked servers; the Redis servers, access keys and data live in this service behind the azurearm.Redis seam (adapter in internal/app/azureredis_wiring.go). Applied, re-planned with no drift and destroyed by test/terraform/azure/redis_test.go, which connects over TLS with the primary key."}
  endpoints: {status: ok, note: "hostName is <name>.redis.cache.windows.net; sslPort and port (plaintext, only while enableNonSslPort) are the real emulator ports. The TLS endpoint always serves a self-signed or configured (--azure-redis-tls-cert) certificate valid for localhost and 127.0.0.1; dial the host and set the TLS server name to localhost."}
  sdk_compat: {status: ok, note: "armredis v3: create/update/delete pollers, ListKeys, RegenerateKey, ForceReboot, ImportData, ExportData, FirewallRules and PatchSchedules; azurerm_redis_cache applies without drift."}
gaps:
  - "Microsoft.Cache/redisEnterprise (Redis Enterprise and Azure Managed Redis) is not implemented."
  - "Clustering (shardCount, replicasPerPrimary), VNet injection, private endpoints, zones allocation and geo-replication data flow are not modelled; a cache is one Redis server."
  - "redisConfiguration is stored and echoed; miniredis has no CONFIG, so maxmemory-policy, maxclients and notify-keyspace-events are not enforced. rdb/aof backup settings only decide whether a full reboot keeps data."
  - "Entra authentication: tokens are not re-checked for expiry on an open connection; the three built-in access policies use permission strings derived from their names (+@all allkeys; +@all -@dangerous allkeys; +@read +@connection allkeys) because Learn (azure-cache-for-redis/cache-configure-role-based-access-control) and REST 2024-11-01 access-policy list publish no built-in permission strings; deleting a custom policy that an assignment uses answers 400 InvalidRequestBody (the REST page documents no such rule)."
  - "Import accepts one RDB file; compact ziplist/listpack/intset encodings and streams fail with a 400."
  - "Create completes the server immediately; only the reported provisioningState lags."
deferred: []
items_still_open:
  - "Microsoft.Cache/redisEnterprise (Redis Enterprise and Azure Managed Redis) is not implemented."
  - "Clustering, VNet injection, private endpoints and geo-replication data flow are not modelled."
  - "redisConfiguration settings are stored but not enforced (no CONFIG in miniredis)."
  - "Entra authentication: tokens are not re-checked for expiry on an open connection; the built-in access policy permission strings are not published by Learn or the REST reference and are derived from the policy names; deleting a custom policy that an assignment uses is refused although the REST page documents no such rule."
leaks: {status: clean, note: "Reset and Shutdown close every cache's relays and Redis server and release their ports; the package runs under testleak."}
---

## Notes

Firewall: Learn (azure-cache-for-redis/cache-configure, Firewall) says that when rules are configured only client connections from the specified ranges can connect, and that rules are available on Basic, Standard and Premium; the firewall is a per-cache allow-list the front listeners consult on accept.

Authentication: `disableAccessKeyAuthentication` (REST 2024-11-01 create, "Authentication to Redis through access keys is disabled when set as true") refuses AUTH with either key and terminates existing connections when the value changes (Learn: cache-azure-active-directory-for-authentication, "Disable access key authentication on your cache"). With redisConfiguration `aad-enabled` true, AUTH `<object id> <token>` is accepted when the token verifies against the emulator's token service, its audience is the Redis resource and its oid claim equals the user name (same page, "Microsoft Entra client workflow").

Data access policies: REST 2024-11-01 (learn.microsoft.com/rest/api/redis/access-policy and access-policy-assignment, create-update/delete/get/list) define accessPolicies (properties.permissions, type BuiltIn or Custom) and accessPolicyAssignments (accessPolicyName, objectId, objectIdAlias; PUT 200/201, DELETE 200/202/204). The permissions string is parsed as a Redis ACL (+@category, -@category, +command, ~key pattern, allkeys, &channel; pkgs/redisacl); an Entra token only authenticates when its principal has an assignment and the user name is its object ID or alias, and the connection then runs under that ACL (NOPERM replies as Redis answers them). Access-key connections stay full access.

Each cache is a loopback miniredis server fronted by relays: a TLS relay (always) and a plaintext relay (only while enableNonSslPort), both on ports from the shared port pool. Access keys live in a per-cache keyring consulted by a pre-command hook that rewrites the secondary key to the primary one in AUTH and HELLO, because miniredis holds a single password.

Persistence stores each cache's keys, spec and keyspace; servers restart on fresh ports and ARM reads the new ports on the next GET.
