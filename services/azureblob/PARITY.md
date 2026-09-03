---
service: azureblob
sdk_module: azure-sdk-for-go/sdk/storage/azblob@v1.7.0
last_audit_commit: (initial seed, no audit history yet)
last_audit_date: 2026-09-02
overall: C
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ListContainers: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /<account>?comp=list. No prefix/marker/maxresults pagination support yet -- returns all containers in one page with an always-empty NextMarker."}
  CreateContainer: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /<account>/<container>?restype=container. No metadata (x-ms-meta-*) or public-access-level support."}
  DeleteContainer: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<account>/<container>?restype=container. No lease/If-Match conditional support."}
  ListBlobs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<account>/<container>?restype=container&comp=list. Flat listing only -- no prefix/delimiter/marker/maxresults, no snapshot/version/metadata inclusion flags."}
  PutBlob: {wire: partial, errors: ok, state: ok, persist: ok, note: "PUT /<account>/<container>/<blob>, BlockBlob only (x-ms-blob-type required and validated). Whole-body single-request PUT only -- Put Block/Put Block List (large-object multipart upload) is not implemented, see gaps."}
  GetBlob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<account>/<container>/<blob>, single-range Range header supported (start-end, open-ended, and suffix forms); multi-range requests are rejected as unsatisfiable rather than served."}
  GetBlobProperties: {wire: ok, errors: ok, state: ok, persist: n/a, note: "HEAD /<account>/<container>/<blob>. Returns ETag/Last-Modified/Content-Length/Content-Type/x-ms-blob-type; no x-ms-meta-* or lease-state headers."}
  DeleteBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<account>/<container>/<blob>. No snapshot/version-scoped delete, no soft-delete."}
families:
  auth: {status: deferred, note: "Authorization header is accepted structurally (SharedKey prefix or absent) but never cryptographically verified -- matches services/s3's PresignSecret-opt-in philosophy. Real SharedKey canonicalization/HMAC verification is deferred to pkgs/azureauth, landing on a separate branch (azure/auth-pkg); this package has a TODO(azure-integration) marker at the handler's auth entry point."}
  blob_body_headers: {status: ok, note: "x-ms-version, x-ms-request-id, and Date are set on every response (success and error paths) via setCommonHeaders, so azure-sdk-for-go's response parsing does not error on missing headers."}
  routing_isolation: {status: ok, note: "Runs on its own dedicated *http.Server (default port 10000, AZURE_BLOB_PORT override), never registered into the shared AWS single-port Router -- see provider.go's Provider doc comment and AZURE.md section 4 for the full rationale."}
gaps:
  - "Put Block / Put Block List (large-object multipart upload) is not implemented -- Put Blob only accepts a single whole-body BlockBlob PUT. Deliberate M0 scope per AZURE.md; tracked for a later milestone (M1 in AZURE.md's plan)."
  - "No ACL / container public-access-level support (x-ms-blob-public-access, Set/Get Container ACL are unimplemented)."
  - "No blob or container metadata (x-ms-meta-* headers) -- neither stored on PUT/Create nor returned on GET/HEAD/List."
  - "No conditional-header support (If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since) on any operation -- every write unconditionally overwrites, every read unconditionally succeeds regardless of ETag/date preconditions."
  - "No Copy Blob (server-side or cross-account) support."
  - "No snapshot, versioning, soft-delete, lease, or tier (hot/cool/archive) support."
  - "List Containers / List Blobs return every result in one page; no prefix/marker/maxresults pagination."
  - "Auth is structurally permissive only -- see families.auth. Real SharedKey verification (pkgs/azureauth) is a separate, not-yet-landed dependency."
  All gaps above are intentional MVP scope per AZURE.md's M0/M1 split, not oversights; see AZURE.md sections 2 and 8 for the milestone plan.
deferred:
  - "Initial implementation pass (2026-09-02): seeded this service from scratch per AZURE.md M0. No prior audit history to reconcile. sdk_module pinned to the latest azure-sdk-for-go blob module version documented in AZURE.md at authoring time; not yet cross-checked against a live SDK import in this repo (azure-sdk-for-go is not currently a go.mod dependency -- this package speaks the wire protocol directly rather than through the SDK's server-side types)."
  - "cli.go registration is deliberately NOT wired up in this pass -- a human integrates this provider once pkgs/azureauth (a separate branch, azure/auth-pkg) also lands, per the task's explicit deferral."
  - "No Go integration test (test/integration/azureblob_test.go) yet -- that requires the cli.go wiring above, which is out of scope for this pass. Unit tests exercise the handler/backend directly via httptest instead."
leaks: {status: clean, note: "No background goroutines, tickers, or janitor: InMemoryBackend is pure in-memory maps guarded by one sync.RWMutex, with no TTL/expiry sweep in this MVP scope. The dedicated *http.Server started by StartWorker is stopped by Shutdown via srv.Shutdown(ctx), mirroring cli.go's own top-level server lifecycle."}
---

## Notes

### Why Azure Blob gets its own port instead of the shared AWS router
Every other gopherstack service registers a `RouteMatcher` into the shared
single-port AWS `Router` (`pkgs/service/router.go`), which disambiguates
services by header (`X-Amz-Target`) or distinctive path/form shape. Azure
Blob's REST path shape (`/<account>/<container>/<blob>`) has no such
service-identifying header, and colliding with Azure Queue/Table's identical
`/<account>/<resource>` shape (once those land) would be exactly the
ambiguity the AWS router avoids by construction. Instead, `Provider.Init`
resolves a dedicated port (default 10000, mirroring Azurite's own
Blob-service default) and the returned `*Handler` implements
`service.BackgroundWorker`, standing up its own `*echo.Echo` + `*http.Server`
in `StartWorker`. See `provider.go`'s `Provider` doc comment and AZURE.md
section 4 for the full rationale, including why `pkgs/portalloc.Allocator`
(which only hands out the next free port in a range, with no way to reserve
a *specific* preferred port) couldn't be used as-is.

### Auth
The `Authorization` header is accepted on structure alone -- a `SharedKey
...` prefix, or no header at all, both pass -- matching this repo's
permissive-by-default philosophy (`services/s3/sigv4.go`'s
`PresignSecret`-opt-in pattern). Real SharedKey HMAC verification is planned
for `pkgs/azureauth`, landing separately; `handler.go`'s `checkAuth` carries
the wiring TODO.

### Blob names with slashes
Azure blob names may contain `/` as a virtual-directory separator (e.g.
`logs/2026/09/02.txt`). `splitPath` only ever splits the URL into three
pieces (`account`, `container`, everything else as `blob`), so a blob name's
internal slashes are preserved intact rather than being mistaken for
additional path segments.

### Range reads
`Get Blob` supports the standard `Range: bytes=start-end`, open-ended
(`bytes=N-`), and suffix (`bytes=-N`) forms, returning `206 Partial Content`
with `Content-Range` set. Multi-range requests (`bytes=0-1,3-4`) are rejected
with `416 Requested Range Not Satisfiable` rather than served -- Azure's own
Get Blob does not support multi-range either.

## More

- [Full parity audit](PARITY.md)
- [All services](../../README.md#services)
