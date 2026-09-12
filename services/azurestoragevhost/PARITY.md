---
service: azurestoragevhost
sdk_module: hashicorp/terraform-provider-azurerm@v4.81.0 # not a go.mod dependency; version this listener was added to satisfy
last_audit_commit: cbf859f49
last_audit_date: 2026-09-11
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  HostRouting: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "Parses Host: {account}.{blob|queue|table}.{suffix} off every request; rejects (400) hosts with an unrecognized service label or fewer than 3 dot-separated components. Covered by TestHandler_RoutesByHostLabel/TestHandler_UnrecognizedHostIs400."}
  Delegation: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Rewrites r.URL.Path to prepend /{account} and calls straight into the wired *azureblob.Handler/*azurequeue.Handler/*azuretable.Handler's own Handler() -- zero duplicated business logic, same backend instances as the path-style listeners. If a service isn't wired (nil), returns 400 rather than panicking (TestHandler_UnwiredServiceIs400)."}
families:
  wiring: {status: ok, note: "cli.go's wireAzureStorageVHost type-asserts the three already-constructed path-style Handlers and assigns them onto the vhost Handler's Blob/Queue/Table fields post-construction, mirroring the existing wireSNSToSQS-style cross-service wiring pattern -- no second set of backend instances is ever created."}
gaps:
  - "This package owns no state of its own (Reset/Snapshot/Restore are no-ops); a snapshot/restore cycle on the wired services fully covers vhost-visible data too, so nothing is missing here specifically -- listed for completeness since most services have real persist behavior to audit."
  - "GetSupportedOperations() returns nil (no dedicated metrics-op enum); ExtractOperation/ExtractResource parse the Host header only for basic request-metrics labeling, not the fine-grained per-op accounting the wrapped services' own handlers already do internally once delegated to."
structural_gaps: []
deferred:
  - "M8 initial implementation (AZURE.md section 10.8 finding (9)). No prior audit passes to report."
leaks: {status: clean, note: "Handler.StartWorker/Shutdown mirror azureblob's own listener lifecycle exactly (fixed port, fail-fast bind); no additional goroutines beyond the one HTTP server."}
---

## Notes

Purely a Host-header translation layer, not a fourth storage service: it holds
no `StorageBackend`, no auth logic, no CRUD -- it exists solely because
`terraform-provider-azurerm`'s data-plane SDK (`jackofallops/giovanni`) hard-requires
virtual-hosted-style URLs (`{account}.{blob,queue,table}.{suffix}`) while
`services/azureblob`/`azurequeue`/`azuretable` (M0-M2) are path-style
(`{host}:{port}/{account}/...`). See `AZURE.md` section 10.8 finding (9) for
the full design rationale, including why Blob/Queue/Table share one port
(10010) here despite keeping three separate path-style ports for direct-SDK
access.

For this service's actual pinned protocol and decode case-sensitivity, don't
guess -- check `services/_PROTOCOLS.md` first (though as a pure router, this
package itself has no wire format of its own; the wrapped services' entries
apply once a request is delegated).
