---
service: azureblob
sdk_module: azure-sdk-for-go/sdk/storage/azblob@v1.8.2
last_audit_commit: f1427114b
last_audit_date: 2026-09-03
overall: C
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ListContainers: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /<account>?comp=list. include=metadata returns per-container Metadata; Properties carry PublicAccess when set. prefix, marker and maxresults (1..5000, clamped) with opaque marker/NextMarker paging; Prefix/Marker/MaxResults echoed. include=metadata returns Metadata; include=deleted/system are accepted and ignored (soft-delete and system containers are not modeled). Entries carry LeaseStatus/LeaseState/LeaseDuration."}
  CreateContainer: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /<account>/<container>?restype=container. Stores x-ms-meta-* (names must be C# identifiers, else 400 InvalidMetadata) and x-ms-blob-public-access (blob|container, else 400 InvalidHeaderValue); returns ETag/Last-Modified."}
  DeleteContainer: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<account>/<container>?restype=container. If-Modified-Since/If-Unmodified-Since give 412 ConditionNotMet; an active container lease needs the matching x-ms-lease-id (412 LeaseIdMissing / LeaseIdMismatchWithContainerOperation)."}
  ListBlobs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<account>/<container>?restype=container&comp=list. prefix, delimiter (BlobPrefix entries), maxresults (1..5000, clamped) with opaque marker/NextMarker paging, include=snapshots (snapshots listed oldest-first before the base blob) and include=metadata (Metadata is always returned). include=versions/deleted are accepted but ignored because versioning and soft-delete are not modeled, which matches a real account with both disabled."}
  PutBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /<account>/<container>/<blob>, BlockBlob, AppendBlob (empty body) or PageBlob (x-ms-blob-content-length) per x-ms-blob-type. Whole-body single-request PUT only; stores x-ms-meta-*, x-ms-blob-content-{type,encoding,language,disposition,md5}, x-ms-blob-cache-control (md5 must be 16 base64 bytes) and x-ms-access-tier. Content-Type is the fallback for x-ms-blob-content-type. Overwrite keeps existing snapshots and replaces tags with x-ms-tags (query-string encoded, <= 2 KiB; none clears them). Put Block List takes x-ms-tags the same way. Content-MD5/x-ms-content-crc64 request checksums are verified (400 Md5Mismatch/Crc64Mismatch) and Content-MD5 is returned; conditional headers and the lease id apply (If-None-Match: * on an existing blob is 409 BlobAlreadyExists). With x-ms-copy-source and no comp the request is Copy Blob (see CopyBlob)."}
  GetBlob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<account>/<container>/<blob>. Single-range Range or x-ms-range (x-ms-range wins; the Python and JS SDKs send only x-ms-range); multi-range is rejected as unsatisfiable. Returns x-ms-meta-*, content-property headers, Content-MD5 (full reads only), x-ms-access-tier[-inferred], and serves ?snapshot= reads (x-ms-snapshot echoed). An Archive-tier blob returns 409 BlobArchived."}
  GetBlobProperties: {wire: ok, errors: ok, state: ok, persist: n/a, note: "HEAD /<account>/<container>/<blob>[?snapshot=]. Returns ETag/Last-Modified/Content-Length/Content-Type, the other content properties, x-ms-meta-*, x-ms-blob-type and access-tier headers. x-ms-lease-status/state/duration headers reflect the lease; conditional headers give 304/412."}
  DeleteBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<account>/<container>/<blob>. ?snapshot= deletes one snapshot; a base blob with snapshots needs x-ms-delete-snapshots include|only, else 409 SnapshotsPresent. No version-scoped delete and no soft-delete."}
  PutBlock: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=block&blockid=. Stages an uncommitted block on a blob that need not exist; block ids must be base64, <=64 decoded bytes and equal-length across the blob's staged and committed blocks (400 InvalidBlobOrBlock). Body <=4000 MiB, Content-MD5/x-ms-content-crc64 verified; returns Content-MD5 and x-ms-content-crc64. Staged blocks expire 7 days after the last Put Block (lazy, on the injectable clock). A leased blob needs x-ms-lease-id."}
  PutBlockFromURL: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=block with x-ms-copy-source. Only same-account blob URLs of this emulator are readable (others are 404 CannotVerifyCopySource); honours x-ms-source-range, x-ms-source-content-md5/crc64 and x-ms-source-if-* (412 SourceConditionNotMet)."}
  PutBlockList: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=blocklist. Committed/Uncommitted/Latest entries (Latest prefers staged), duplicates allowed, <=50000 blocks (409 BlockCountExceedsLimit), unknown ids 400 InvalidBlockList. Applies x-ms-blob-* properties, metadata and tier like Put Blob, keeps snapshots and lease, and discards every uncommitted block of the blob. The blob MD5 is only stored when x-ms-blob-content-md5 is sent."}
  GetBlockList: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET ?comp=blocklist&blocklisttype=committed|uncommitted|all (default committed), optional snapshot. Committed blocks keep commit order, uncommitted are sorted by id. A never-committed blob answers uncommitted/all from its staged blocks and is otherwise 404 BlobNotFound. Returns ETag, Last-Modified and x-ms-blob-content-length when the blob exists. A blob written by plain Put Blob has no committed blocks."}
  LeaseBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=lease with x-ms-lease-action acquire|renew|change|release|break. Durations are -1 (infinite) or 15..60, break periods 0..60; proposed ids must be GUIDs. States available/leased/expired/breaking/broken are derived from the injectable clock. Documented 409 codes: LeaseAlreadyPresent, LeaseNotPresentWithLeaseOperation, LeaseIdMismatchWithLeaseOperation, LeaseIsBrokenAndCannotBeRenewed, LeaseIsBreakingAndCannotBeAcquired/Changed. Put Blob, Put Block, Put Block List, Set Properties/Metadata, Delete and Copy (destination) require the active lease id (412 LeaseIdMissing, LeaseIdMismatchWithBlobOperation, LeaseNotPresentWithBlobOperation, LeaseLost); reads, Snapshot and Set Tier take it optionally. Leases survive overwrite, are not copied to snapshots, and persist."}
  LeaseContainer: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?restype=container&comp=lease, same actions and state machine as LeaseBlob. Delete Container needs the lease id while leased; blob writes are not gated by a container lease."}
  ConditionalHeaders: {wire: ok, errors: ok, state: ok, persist: n/a, note: "If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since evaluated atomically with the operation: Get Blob/Properties/Metadata answer 304 (If-None-Match hit, If-Modified-Since not modified, with ETag/Last-Modified) or 412 ConditionNotMet; writes, deletes, snapshot, tier, lease and copy destination answer 412; If-None-Match: * on an existing blob is 409 BlobAlreadyExists for Put Blob, Put Block List and Copy. Copy sources take x-ms-source-if-*. x-ms-if-tags / x-ms-source-if-tags are evaluated against the blob's real tags (see Tags): =, <>, >, >=, <, <=, AND, OR, parentheses, at most 10 logical operations; an unmet condition or a missing blob is 412 ConditionNotMet (SourceConditionNotMet for the copy source), a malformed expression 400 InvalidHeaderValue. A key absent from the blob makes its comparison false, including <> (SQL NULL reading, not stated in the docs)."}
  SetBlobTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=tags[&snapshot=] with a Tags/TagSet/Tag body replaces all tags and answers 204; an empty TagSet clears them. At most 10 tags, keys 1-128 and values 0-256 characters from a-z A-Z 0-9 space + - . / : = _, case-sensitive; violations are 400 (InvalidTag, TagsTooLarge for more than 10; the docs only state 400, the code names are not in the published error list), malformed XML 400 InvalidXmlDocument. ETag and Last-Modified are unchanged. Content-MD5/x-ms-content-crc64 verified, x-ms-if-tags and the lease id apply (a leased base blob needs its id). Version ids are not modeled."}
  GetBlobTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET ?comp=tags[&snapshot=] returns Tags/TagSet/Tag sorted by key (the docs do not guarantee order); x-ms-if-tags applies. Get Blob and Get Blob Properties return x-ms-tag-count only when the blob has tags; List Blobs include=tags adds Tags and TagCount."}
  FindBlobsByTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<account>?comp=blobs&where= and GET /<account>/<container>?restype=container&comp=blobs&where= return EnumerationResults/Where/Blobs/Blob(Name, ContainerName, Tags)/NextMarker for base blobs only (snapshots are not indexed). Grammar: AND-joined =, >, >=, <, <= on single-quoted values with double-quoted (or bare identifier) keys; <>, OR and parentheses are 400, as are same-sided ranges on one key and @container other than a single @container = '<name>' (not allowed in container scope). Invalid expression, maxresults <= 0 and a foreign marker are 400 InvalidQueryParameterValue (the docs state only 400); maxresults is clamped to 5000 with opaque marker paging ordered by container then blob name. The index is strongly consistent here. Copy Blob never inherits tags; x-ms-tags sets them on the destination."}
  GetServiceProperties: {wire: ok, errors: n/a, state: ok, persist: ok, note: "GET /<account>?restype=service&comp=properties returns the stored StorageServiceProperties (Logging, HourMetrics, MinuteMetrics, Cors, DefaultServiceVersion, DeleteRetentionPolicy, StaticWebsite); defaults before any Set."}
  SetServiceProperties: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?restype=service&comp=properties answers 202; elements present replace the stored ones, omitted elements stay unchanged; at most 5 CORS rules, retention days 1..365 (400 InvalidXmlDocument). DeleteRetentionPolicy drives blob soft delete."}
  AppendBlock: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=appendblock on x-ms-blob-type AppendBlob (created by Put Blob with an empty body). Returns x-ms-blob-append-offset and x-ms-blob-committed-block-count; x-ms-blob-condition-appendpos/maxsize give 412 AppendPositionConditionNotMet/MaxBlobSizeConditionNotMet; 50000 blocks (409 BlockCountExceedsLimit), 4 MiB per block; other blob types 409 InvalidBlobType."}
  PutPage: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=page with x-ms-page-write update|clear on a PageBlob (Put Blob with x-ms-blob-content-length, multiple of 512, emulator cap 256 MiB). Ranges are 512-aligned and inside the blob (416 InvalidPageRange); writes are <=4 MiB. Get Blob returns zeros for unwritten pages."}
  GetPageRanges: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET ?comp=pagelist[&snapshot=] returns PageList/PageRange with adjacent writes merged, optionally limited by x-ms-range. Set Blob Properties with x-ms-blob-content-length resizes (shrinking drops pages beyond)."}
  AbortCopyBlob: {wire: ok, errors: ok, state: ok, persist: n/a, note: "PUT ?comp=copy&copyid= with x-ms-copy-action: abort. Copies complete synchronously, so no copy is ever pending: 409 NoPendingCopyOperation (CopyIdMismatch when a pending copy has another id)."}
  UndeleteBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=undelete restores a blob deleted while DeleteRetentionPolicy was enabled (with its snapshots) until the retention days elapse; a live blob is left untouched, otherwise 404 BlobNotFound. List Blobs include=deleted returns Deleted, DeletedTime and RemainingRetentionDays."}
  GetContainerProperties: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET or HEAD /<account>/<container>?restype=container (also comp=metadata). Returns Last-Modified/ETag (change on metadata/ACL writes), lease headers, x-ms-meta-*, and x-ms-blob-public-access when not private."}
  SetBlobProperties: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=properties. Replaces the content properties from x-ms-blob-* headers; properties not sent are cleared, as in Azure. Bumps ETag/Last-Modified."}
  SetBlobMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=metadata replaces all x-ms-meta-*; no headers clears it."}
  GetBlobMetadata: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET or HEAD ?comp=metadata[&snapshot=] returns x-ms-meta-* headers and no body."}
  SetBlobTier: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=tier with x-ms-access-tier Hot|Cool|Cold|Archive. Leaving Archive answers 202 and starts a rehydration: the blob stays Archive with x-ms-archive-status rehydrate-pending-to-<tier> until x-ms-rehydrate-priority Standard (15h) or High (1h, the documented upper bounds) elapse on the backend clock, evaluated on read."}
  SnapshotBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?comp=snapshot returns 201 with x-ms-snapshot (100ns-resolution timestamp id). x-ms-meta-* replaces the inherited metadata."}
  CopyBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT with x-ms-copy-source: same-account blobs and snapshots are copied in-process; a source of another account or host is fetched through a loopback-only client (no redirects, no proxy, 256 MiB cap, no caller credentials; --azure-blob-allow-external-copy lifts the loopback limit). The destination carries x-ms-copy-id/status/source (query dropped)/progress/completion-time until Set Blob Properties, Put Blob or Put Block List replaces them. Copies complete synchronously."}
  SetContainerMetadata: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?restype=container&comp=metadata replaces container metadata."}
  GetContainerACL: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET ?restype=container&comp=acl returns SignedIdentifiers and x-ms-blob-public-access."}
  SetContainerACL: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT ?restype=container&comp=acl sets the public-access level (absent header = private) and up to 5 stored access policies. Policies are stored and returned; they do not gate SAS requests because SAS signatures are not verified."}
families:
  auth: {status: partial, note: "With --azure-blob-verify-signatures (AZURE_BLOB_VERIFY_SIGNATURES) SharedKey requests are verified against the account key (the development account key plus AZURE_BLOB_ACCOUNT_KEYS; both the SDK-for-Go path-style canonical resource and the account-less form are accepted) and SAS tokens are verified: service SAS (sr=c|b|bs, sv >= 2018-11-09) and account SAS signatures, st/se window, sip, spr, the permission letters the operation needs, and stored access policies (si). Failures answer 403 AuthenticationFailed or AuthorizationPermissionMismatch. Default is permissive (nothing verified). Bearer tokens and user delegation SAS are not verified (AAD/RBAC out of scope)."}
  public_access: {status: partial, note: "Anonymous (no Authorization header and no sig= query) requests honour the container level only when the opt-in --azure-blob-enforce-public-access / AZURE_BLOB_ENFORCE_PUBLIC_ACCESS is set: blob level allows Get Blob/Properties/Metadata, container level also List Blobs and Get Container Properties; everything else is 404 ResourceNotFound, and anonymous writes or account-level calls are 401 NoAuthenticationInformation. Off by default so unsigned curl keeps working (docs and examples rely on it)."}
  blob_body_headers: {status: ok, note: "x-ms-version, x-ms-request-id, and Date are set on every response (success and error paths) via setCommonHeaders, so azure-sdk-for-go's response parsing does not error on missing headers."}
  routing_isolation: {status: ok, note: "Runs on its own dedicated *http.Server, bound synchronously in StartWorker to a fixed port (default 10000 via --azure-blob-port/AZURE_BLOB_PORT, no fallback pool -- fails fast if unavailable, mirroring services/iot's MQTT broker), never registered into the shared AWS single-port Router -- see provider.go's Provider doc comment and AZURE.md section 4 for the full rationale. cli.go's reserveFixedServicePorts additionally reserves this port in the shared PortAlloc pool (pkgs/portalloc.Allocator.Reserve) at startup, since 10000 sits inside --port-range-start/--port-range-end's own default range and would otherwise be handed to an unrelated Acquire caller (fixed in M0 review)."}
  observability: {status: ok, note: "StartWorker wraps its Echo handler with telemetry.WrapEchoHandler so ExtractOperation/ExtractResource feed Prometheus metrics, and derives its listener logger via logger.WithWorker(ctx, \"azureblob\", \"listener\"). InMemoryBackend and the server-lifecycle mutex both use *lockmetrics.RWMutex instead of raw sync.RWMutex/Mutex, matching repo convention."}
items_still_open:
  - "Blob versions (x-ms-version-id, include=versions, version-scoped read/delete) are not modeled: account versioning is an ARM blobServices setting and services/azurearm keeps no blobServices state (handler_ops.go:277), so the data plane has nothing to read it from."
  - "Copy Blob always completes synchronously (x-ms-copy-status success), so a copy is never pending and Abort Copy has nothing to abort; Incremental Copy, Append Block From URL and Put Page From URL are not implemented."
  - "User delegation SAS and bearer tokens are not verified (needs AAD user delegation keys; RBAC is out of scope)."
  - "Page blobs: sequence-number actions/conditions, Get Page Ranges Diff and a blob size above 256 MiB are not supported."
deferred:
  - "Initial implementation pass (2026-09-02): seeded this service from scratch per AZURE.md M0. No prior audit history to reconcile."
  - "M0 review pass (2026-09-03): pkgs/azureauth, cli.go registration, and test/integration/azureblob_test.go all landed in the same PR as this service (see AZURE.md's M0 entry) -- the file was previously drafted assuming a multi-PR sequence that did not happen. sdk_module bumped to the azblob v1.8.0 actually pinned in go.mod (used by the integration test)."
  - "Dead-code sweep (2026-09-06, gopherstack-rxdr): removed errors.go's ErrInvalidBlobType/ErrInvalidRange sentinels -- unlike every other sentinel in this file (ErrContainerNotFound/ErrContainerAlreadyExists/ErrBlobNotFound, all returned by store.go and consumed via errors.Is at the handler.go boundary, matching the repo-wide convention seen in services/s3, services/sqs, services/dynamodb), these two were never returned by anything: putBlob's x-ms-blob-type check and getBlob's parseRange check are pure handler-local HTTP validation that never crosses into the backend layer. grep confirmed zero references anywhere in the repo outside their own declaration. TestPutBlob_RequiresBlockBlobType and TestGetBlob_RangeHeaderPartialRead/unsatisfiable assert on HTTP status/body strings, not the sentinels, and pass unchanged."
leaks: {status: clean, note: "No background goroutines, tickers, or janitor: InMemoryBackend is pure in-memory maps guarded by one *lockmetrics.RWMutex, with no TTL/expiry sweep in this MVP scope. The dedicated *http.Server started by StartWorker is stopped by Shutdown via srv.Shutdown(ctx) (falling back to srv.Close() on a graceful-shutdown error, both logged), mirroring cli.go's own top-level server lifecycle."}
---

## Notes

### Why Azure Blob gets its own port instead of the shared AWS router
Every other gopherstack service registers a `RouteMatcher` into the shared
single-port AWS `Router` (`pkgs/service/router.go`), which disambiguates
services by header (`X-Amz-Target`) or distinctive path/form shape. Azure
Blob's REST path shape (`/<account>/<container>/<blob>`) has no such
service-identifying header, and colliding with Azure Queue/Table's identical
`/<account>/<resource>` shape (once those land) would be exactly the
ambiguity the AWS router avoids by construction. Instead, the returned
`*Handler` implements `service.BackgroundWorker`, and `StartWorker`
synchronously binds a fixed port (default 10000, Azurite's own Blob-service
default, overridable via `--azure-blob-port`/`AZURE_BLOB_PORT`) before
standing up its own `*echo.Echo` + `*http.Server` to serve on that same
listener -- there is no probe-then-close window for another process to steal
the port between "we checked it was free" and "we're listening on it", and
no fallback into a different port if the bind fails (StartWorker returns the
bind error directly instead). This mirrors `services/iot`'s MQTT broker
(`services/iot/broker.go`), gopherstack's existing precedent for a
fixed-port service, rather than drawing from the shared `--port-range-start`/
`--port-range-end` `PortAlloc` pool used for on-demand ephemeral resources.
See `provider.go`'s `Provider` doc comment and AZURE.md section 4 for the
full rationale.

### Auth
The `Authorization` header is parsed via `pkgs/azureauth.ParseAuthorizationHeader`
(proving a real Azure SDK's header round-trips through this package), but a
malformed or absent header is still accepted -- matching this repo's
permissive-by-default philosophy (`services/s3/sigv4.go`'s
`PresignSecret`-opt-in pattern). `pkgs/azureauth.VerifySharedKey` implements
real SharedKey HMAC verification and is unit-tested, but `handler.go`'s
`checkAuth` does not call it yet; enforcing it is deliberately deferred past
this milestone.

### Blob names with slashes
Azure blob names may contain `/` as a virtual-directory separator (e.g.
`logs/2026/09/02.txt`). `splitPath` only ever splits the URL into three
pieces (`account`, `container`, everything else as `blob`), so a blob name's
internal slashes are preserved intact rather than being mistaken for
additional path segments.

### Range reads
`Get Blob` supports the standard `Range: bytes=start-end`, open-ended
(`bytes=N-`), and suffix (`bytes=-N`) forms, returning `206 Partial Content`
with `Content-Range` set. `x-ms-range` is also accepted, taking precedence
over `Range` when both are present, matching real Azure Blob -- the Python
(`azure-storage-blob`) and JS (`@azure/storage-blob`) generated clients send
`x-ms-range` exclusively for `Get Blob`, never
`Range`, so this was a real wire-compatibility gap until M4's cross-SDK
smoke test (`test/e2e/azure_crosssdk_test.go`) caught it -- the Go SDK's own
generated client sends `Range`, so unit/integration tests using it never
exercised this path. Multi-range requests (`bytes=0-1,3-4`) are rejected
with `416 Requested Range Not Satisfiable` rather than served -- Azure's own
Get Blob does not support multi-range either.

### ETags
Blob ETags are derived from the body *and* a per-backend monotonically
increasing counter incremented on every `Put Blob` (see `store.go`'s
`computeBlobETag`/`etagSeq`), not from the body alone -- overwriting a blob
with byte-identical content still produces a new ETag, matching real Azure
Blob semantics and keeping `If-Match`/`If-None-Match` concurrency checks
meaningful (conditional headers are enforced, see ConditionalHeaders). Container listing ETags (`List Containers`) use
a separate, simpler hash with no mutation-sequence component, since
containers have no mutable properties in this MVP.

### M8: Terraform-provisioned coverage
`test/terraform/azure/storage_dataplane_test.go`'s `TestTerraform_Azure_StorageDataPlane` provisions an `azurerm_storage_container` + `azurerm_storage_blob` (via an unmodified `hashicorp/azurerm` Terraform provider against `services/azurearm`'s Storage RP, M7) and reads the blob back with `azure-sdk-for-go/sdk/storage/azblob`, alongside this service's existing Go-SDK integration coverage (`test/integration/azureblob_test.go`) -- see `AZURE.md` section 10.10's M8 entry for current pass/skip status, which can depend on the running host's TLS-trust behavior independent of this service.

Getting real (unmodified) `terraform-provider-azurerm` traffic to reach this service at all required a new addressing layer -- see `services/azurestoragevhost` and `AZURE.md` section 10.8 finding (9) -- since the provider's data-plane SDK hard-requires virtual-hosted-style URLs (`{account}.blob.{suffix}`) that this service's path-style listener never produced on its own. Reaching this service through that path for the first time also surfaced two REST-surface gaps in `terraform-provider-azurerm`'s create-then-read flow, both now implemented: **Get Container Properties** (`GET ?restype=container`, used by `azurerm_storage_container`'s existence check) and **Set Blob Properties** (`PUT ?comp=properties`, issued after every blob upload).

## More

- [Full parity audit](PARITY.md)
- [All services](../../README.md#services)

## Emulator choices where the vendor is silent

- Signature verification and public-access rules for anonymous requests are opt-in (`--azure-blob-verify-signatures`) so unsigned curl keeps working by default.
- Service SAS permission mapping for container-level operations (Get Container Properties, ACL, metadata, lease, create/delete container) is account SAS only; a service SAS authorises blob operations, List Blobs and Find Blobs by Tags.
- Page blobs are capped at 256 MiB and copy sources at 256 MiB to bound memory.
- Soft-deleted blobs are kept per container and dropped lazily once their retention days elapse; overwriting a name does not turn the old blob into a soft-deleted snapshot.
- Default service properties of a fresh account (Hour metrics enabled with 7 days retention, everything else off) follow the Learn Get Blob Service Properties sample, from memory.
