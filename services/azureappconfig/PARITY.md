---
service: azureappconfig
sdk_module: azure-sdk-for-go/sdk/data/azappconfig/v2@v2.2.0   # go.mod dependency (test-only): drives the HMAC data plane in sdk_test.go and auth_test.go
last_audit_commit: b1f3fa139
last_audit_date: 2026-10-21
overall: B
ops:
  GetKeyValue: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET and HEAD /kv/{key}?label= (key slashes arrive as %2F or raw). 404 problem+json for a missing key, 304 for If-None-Match with the current ETag, 412 for a failing If-Match, $select field filtering, Accept-Datetime point-in-time reads (a deleted key is still readable at an earlier time). ETag header is quoted, the etag field is not; Sync-Token is on every response."}
  PutKeyValue: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /kv/{key}?label= with value, content_type and tags. If-Match and If-None-Match (including *) give 412; a locked key gives 409 key-locked. Every write is a revision and bumps the store's sync token."}
  DeleteKeyValue: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE returns 200 with the deleted key-value, 204 when it did not exist, 412 on a failing If-Match, 409 when locked. The last state stays available to /revisions and Accept-Datetime reads."}
  GetKeyValues: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET and HEAD /kv with key and label filters (exact, prefix*, *suffix, *contains*, comma lists of up to 5, backslash escapes, \\0 / %00 null label), repeated tags=name=value filters, $select, Accept-Datetime, and snapshot=<name>. Pages of 100 carry Link rel=next plus @nextLink. The SDK's after=<timestamp> point-in-time form is honoured. Collection ETag header."}
  GetKeys: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /keys?name= distinct live keys with the same filter syntax, paged."}
  GetLabels: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /labels?name= distinct non-null labels, paged."}
  GetRevisions: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /revisions with key, label and tags filters and Accept-Datetime. Every recorded state of every key-value is returned in chronological order (oldest first; the order is not verified against the service)."}
  PutLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /locks/{key}?label= makes a key-value read-only (writes and deletes then return 409); 404 for a missing key; honours If-Match / If-None-Match. A lock change is a new revision with a new ETag."}
  DeleteLock: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /locks/{key}?label= makes the key-value writable again."}
  CreateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /snapshots/{name} captures up to 3 key/label filters with composition_type key or key_label, retention_period (3600-7776000 s) and tags; 201 with Operation-Location that GET /operations?snapshot= answers Succeeded. 409 for an existing name."}
  GetSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET and HEAD /snapshots/{name} with $select; Link rel=items points at /kv?snapshot=. Snapshot items are frozen at creation; an archived snapshot disappears once its retention period has passed."}
  UpdateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /snapshots/{name} {status: archived|ready}: archive sets expires, recover clears it; the same status again gives 409; If-Match is honoured."}
  GetSnapshots: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /snapshots with name filter, status list and $select, paged."}
  GetOperationDetails: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /operations?snapshot=<name> returns {id, status: Succeeded} for an existing snapshot; the SDK poller finishes on it."}
families:
  arm_integration: {status: ok, note: "services/azurearm Microsoft.AppConfiguration: configurationStores (sku, identity, tags, unmodelled properties round-trip; endpoint https://{store}.{azconfig host}), listKeys / regenerateKey backed by the data plane's four HMAC credentials with Endpoint=;Id=;Secret= connection strings, keyValues ({key}${label}, ~XX escapes) that read and write the data plane's key-values, replicas (ARM metadata), soft-deleted stores (deletedConfigurationStores get/list/purge, createMode Recover), checkNameAvailability and listKeyValue. Applied, re-planned with no drift and destroyed by test/terraform/azure/appconfig_test.go."}
  sdk_compat: {status: ok, note: "azappconfig v2.2.0 with connection-string HMAC: set/get/delete with labels, filtered list and revision pagers, conditional updates, read-only locks, point-in-time reads, snapshot create/archive/recover/list, feature-flag and Key Vault reference content types. Sync-Token is returned on every response (the SDK dereferences it)."}
  auth: {status: ok, note: "Off by default (any credential is accepted). With --azure-appconfig-validate-hmac the Authorization: HMAC-SHA256 Credential=<id>&SignedHeaders=...&Signature=... header (the SDK's comma form too) is verified against the access key it names: signed host, x-ms-date or date, and x-ms-content-sha256 are required, the content hash must match the body, the date must be within 15 minutes, and read-only keys may only GET/HEAD (403 otherwise). The credential also selects the store when the host and path do not."}
  store_resolution: {status: ok, note: "/stores/{name}/... path prefix, then the first host label when it names an existing store, then the store that owns the HMAC credential, then the auto-created default store (--azure-appconfig-auto-create)."}
gaps:
  - "Bearer tokens are accepted when JWT-shaped; AAD token validation and RBAC data roles are not implemented."
  - "Revision retention (7 or 30 days) is not enforced; history is capped at 50000 entries per store."
  - "Snapshots are ready as soon as they are created; there is no provisioning or failed status."
  - "Replicas are ARM metadata only; their data is the store's data."
  - "Soft-deleted stores keep only ARM metadata; recovering one yields an empty store."
  - "Customer-managed key encryption, private endpoint connections and SKU quotas are not modelled."
deferred: []
items_still_open:
  - "Bearer token validation and AAD data-plane roles are not implemented; JWT-shaped bearer tokens are accepted without verification."
  - "Revision retention windows are not enforced and history is capped at 50000 entries per store."
  - "Snapshots complete synchronously: no provisioning or failed state, and composition_type key keeps the later filter's key-value when filters select several labels of one key (behaviour inferred, not verified)."
  - "Replicas are metadata only and do not replicate or serve a separate data set."
  - "Soft-deleted stores lose their key-values; recovery restores an empty store."
  - "Customer-managed key encryption, private endpoint connections, SKU request quotas and ARM store updates through PATCH are not modelled."
  - "No dashboard page."
leaks: {status: clean, note: "Shutdown closes the HTTPS server and waits for the serve goroutine; there are no background workers."}
---

## Notes

One data plane serves every store; the store is resolved from the path prefix, the host label or the HMAC credential, so a single listener works with the ARM-advertised `{store}.azconfig.<host>:<port>` endpoint and with the plain `Endpoint=` of a connection string.

`azurerm_app_configuration_key` and `azurerm_app_configuration_feature` cannot run against a custom `metadata_host`: go-azure-sdk's `environments.FromEndpoint` documents that the App Configuration domain suffix is missing from the metadata document, so the provider fails before it reaches the data plane. The terraform test therefore applies the store and drives key-values through the SDK with the connection string ARM hands out.

The azappconfig SDK sends its point-in-time selector as `after=<RFC 3339 time>` on list calls; the service treats a parseable timestamp in `after` as an Accept-Datetime and keeps it across pages through an `at` parameter on the next link.
