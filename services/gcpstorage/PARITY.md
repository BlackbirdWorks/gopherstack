---
service: gcpstorage
sdk_module: cloud.google.com/go/storage@v1.65.1
last_audit_commit: 3a537de68
last_audit_date: 2026-10-18
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  InsertBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /storage/v1/b?project=. Stores location (upper-cased; locationType derived), storageClass, labels, versioning, defaultEventBasedHold and the passthrough configs cors, lifecycle, website, logging, encryption, retentionPolicy, iamConfiguration, billing, autoclass, softDeletePolicy and acl/defaultObjectAcl. Names are validated (3-63 chars, 222 with dots, lowercase, no goog prefix, not an IP). Names are global: a duplicate is 409."}
  GetBucket: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /storage/v1/b/{bucket} with ifMetagenerationMatch/NotMatch. iamConfiguration defaults to uniform access disabled and publicAccessPrevention inherited when unset."}
  ListBuckets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /storage/v1/b?project=; project is required and scopes the result. prefix, maxResults and pageToken paging via pkgs/page."}
  PatchBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "JSON merge patch: null clears a field, labels merge per key. Bumps metageneration and updated."}
  UpdateBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT replaces labels, versioning and all passthrough configs; storageClass is kept unless sent."}
  DeleteBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 while any object version, including noncurrent ones, remains."}
  GetBucketIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns storage#policy with bindings and etag; a new bucket has an empty policy (version 1, etag CAE=)."}
  SetBucketIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stores the bindings; a non-empty etag that does not match the stored one is 412. Conditions are stored verbatim. Policies are not evaluated against requests."}
  TestBucketIamPermissions: {wire: ok, errors: ok, state: partial, persist: n/a, note: "Echoes every requested permission as granted because callers are not authenticated."}
  InsertObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /upload/storage/v1/b/{bucket}/o with uploadType=media, multipart (multipart/related) and resumable. Real md5Hash/crc32c are computed; a supplied crc32c or md5Hash that does not match is 400. ifGeneration*/ifMetageneration* preconditions apply (ifGenerationMatch=0 means must not exist). A versioned bucket archives the previous live version."}
  ResumableUpload: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Session URL returned in Location. Chunks arrive as PUT or POST with Content-Range (bytes a-b/total|*, bytes */total status query); partial state is 308 (or 200 plus X-Http-Status-Code-Override when X-GUploader-No-308 is sent) with a Range header; the final chunk commits the object. DELETE cancels (499). Sessions are not persisted and expire after 7 days."}
  GetObject: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Metadata JSON, or media when alt=media. generation selects a version; ifGenerationMatch/ifMetagenerationMatch failures are 412 and the NotMatch forms are 304."}
  DownloadObject: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /download/storage/v1/b/{bucket}/o/{object}. Single and multi Range, conditional headers, X-Goog-Generation, X-Goog-Metageneration, X-Goog-Hash (crc32c, md5), X-Goog-Stored-Content-Length/Encoding, X-Goog-Meta-*."}
  PatchObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "Merge patch of contentType, contentEncoding, contentDisposition, contentLanguage, cacheControl, metadata (per-key null deletes), customTime, temporaryHold, eventBasedHold, storageClass. Bumps metageneration."}
  UpdateObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT replaces every writable field; fields not sent are cleared."}
  DeleteObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "Archives the live version in a versioned bucket, otherwise removes it; generation= permanently removes that version."}
  ListObjects: {wire: ok, errors: ok, state: ok, persist: n/a, note: "prefix, delimiter (prefixes[]), startOffset, endOffset, includeTrailingDelimiter, versions, maxResults and pageToken. Items and prefixes share the page size. matchGlob, softDeleted and includeFoldersAsPrefixes are not supported."}
  CopyObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST .../copyTo/b/{dst}/o/{obj}. A body with metadata fields replaces the destination metadata (contentType falls back to the source); otherwise the source metadata is inherited. sourceGeneration and ifSource* preconditions apply."}
  RewriteObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST .../rewriteTo/...; always completes in one call (done=true, totalBytesRewritten=objectSize). rewriteToken and maxBytesRewrittenPerCall are accepted and ignored because no call is ever partial."}
  ComposeObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-32 same-bucket sources with per-source generation and ifGenerationMatch. The result has componentCount, a crc32c over the whole content and no md5Hash, as in GCS."}
  XMLGetObject: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET/HEAD /{bucket}/{object}, also the unauthenticated public URL. generation= and Range supported; errors are the XML Error document (NoSuchKey, NoSuchBucket, PreconditionFailed)."}
  XMLPutObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /{bucket}/{object} with Content-Type, Cache-Control, Content-Encoding/Disposition/Language, X-Goog-Meta-* (keys lower-cased), X-Goog-Storage-Class and X-Goog-If-Generation-Match/Metageneration-Match. Returns X-Goog-Generation, X-Goog-Hash and the md5 ETag."}
  XMLDeleteObject: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /{bucket}/{object}[?generation=] returns 204."}
families:
  error_envelope: {status: ok, note: "JSON API errors are {error:{code,message,errors:[{message,domain,reason}]}} with reasons notFound, conflict, invalid, required, conditionNotMet and uploadTooLarge; a 304 has no body."}
  generations: {status: ok, note: "generation is a strictly increasing microsecond timestamp (kept monotonic across restore); metageneration starts at 1 and increments on every metadata change."}
  versioning: {status: ok, note: "Bucket versioning keeps noncurrent versions (timeDeleted set), listable with versions=true and addressable by generation."}
  lifecycle: {status: partial, note: "Lifecycle rules, retention, CORS and the other passthrough configs are stored and returned but never enforced: nothing expires, transitions or blocks deletes."}
  auth: {status: partial, note: "No request is authenticated or authorised. Signed URLs are not verified, IAM policies and ACLs are stored but not evaluated, so every object is effectively public."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --gcp-storage-port / GCP_STORAGE_PORT (default 4443, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool. Point the Go client at it with STORAGE_EMULATOR_HOST=localhost:4443."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards buckets and sessions."}
items_still_open:
  - "No bucket or object ACL endpoints (/b/{b}/acl, /b/{b}/defaultObjectAcl, /b/{b}/o/{o}/acl); acl fields on bucket bodies are stored but predefinedAcl is ignored."
  - "No gzip transcoding: an object stored with contentEncoding gzip is always served compressed."
  - "No notification configs, HMAC keys, bucket relocation, soft-delete restore, object restore, or the project serviceAccount endpoint."
  - "Lifecycle, retention policies, holds and CORS are stored but not enforced."
  - "Signed URLs (V2/V4) are not verified and no request is authenticated."
  - "Virtual-hosted-style URLs (bucket.storage.googleapis.com) and XML bucket listing, multipart XML uploads and x-goog-copy-source are not supported."
  - "Customer-supplied and KMS encryption keys are accepted but not applied."
  - "ListObjects ignores matchGlob; page tokens are index-based, so they shift if objects change between pages."
leaks: {status: clean, note: "The only background resource is the http.Server stopped by Shutdown. Abandoned resumable sessions are purged when a new session starts once older than 7 days, and by Reset."}
---

Initial implementation (2026-10-18): Cloud Storage JSON and XML APIs verified against cloud.google.com/go/storage v1.65.1 through the official client in sdk_test.go.
