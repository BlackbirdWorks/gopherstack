---
service: s3
sdk_module: aws-sdk-go-v2/service/s3   # version: v1.105.0 (go.mod, confirmed no new api_op_* files vs v1.104.2)
last_audit_commit: a007ec3e
last_audit_date: 2026-07-11
overall: B   # already mature from prior sweeps; 11 real fixes + op-by-op proof
protocol: REST-XML
families:
  multipart:    {status: ok, note: part-order InvalidPartOrder, non-last EntityTooSmall, ETag=MD5(concat part-MD5s)-N, SSE sealing}
  conditionals: {status: ok, note: If-Match/None-Match/(Un)Modified-Since 412/304 precedence; If-Range; Range 206/416 InvalidRange w/ ActualObjectSize}
  versioning:   {status: ok, note: delete markers, null-version, suspended vs never-configured, object-lock/legal-hold}
  pagination:   {status: ok, note: v1 NextMarker only w/ delimiter; v2 KeyCount, ContinuationToken/StartAfter, encoding-type on keys not tokens}
  errors:       {status: ok, note: full errorTable, no missing-lookup->500; HEAD bodiless}
  copy:         {status: ok, note: copy-self, directives, version-id, UploadPartCopy range}
ops:
  GetObject/HeadObject: {wire: ok, errors: ok, state: ok, persist: ok, note: FIXED response-* override query params (content-type/disposition/expires/cache-control)}
  PutBucketAcl:         {wire: ok, errors: ok, state: ok, persist: ok, note: FIXED reject object-only canned ACLs; read AccessControlPolicy body}
  PutBucketReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: FIXED require versioning=Enabled}
  GetObjectAttributes:  {wire: ok, errors: ok, state: ok, persist: ok, note: FIXED ObjectSize 0-byte, Last-Modified}
gaps:
  - object_lambda dual-lock + no delete-on-bucket-delete (bd: s3 objectLambda follow-up)
  - abandoned multipart uploads have no per-upload TTL (only Abort/Complete/Purge)
leaks: {status: clean, note: janitor ctx-parented w/ <-ctx.Done() stop; replication goroutines WaitGroup-drained; Shutdown() cancels}
---

## Notes
- **CRITICAL prior bug (fixed here):** SSE-S3/SSE-KMS EncryptionDEK/Nonce were `json:"-"` while ciphertext persisted → every encrypted object undecryptable after snapshot/restore (silent data loss). Multipart SSE same. Now persisted base64; SSE-C key stays request-scoped (`SSECKeyB64` json:"-").
- Persistence: backend converted (parity sweep 3, ce30166a) to `pkgs/store.Registry` — `buckets`/`uploads` are now `store.Table`s keyed by name/UploadID (bucket `Region` moved onto `StoredBucket`, replacing the old `map[region]map[name]` nesting + separate `bucketIndex`; `uploadsByBucket` is a `store.Index` replacing the old `map[bucket]map[uploadID]` nesting). Snapshot format bumped to `{"version":1,"tables":{...}}`; older/versionless snapshots are discarded cleanly on Restore (not partially decoded) — a deliberate one-time break, matching services/ec2 and services/sqs. `tags` stays a plain map (composite key, no identity field on the value).
- Trap: `x-amz-storage-class` header is OMITTED for STANDARD objects (AWS behavior) — don't re-add it.

## 2026-07-11 re-audit (a007ec3e, since 708d1961)
Only local drift was ce30166a's `pkgs/store` conversion of `backend_memory.go`/`persistence.go`/`janitor.go` (region-nested bucket maps + `bucketIndex` → `store.Table[StoredBucket]` keyed by name; `uploads` nesting → `store.Table[StoredMultipartUpload]` + `uploadsByBucket` `store.Index`) plus 3 no-op lint/formatting touches (bucket_ops.go, post_object.go, presign.go). Traced every call site touched by the refactor (getBucket/DeleteBucket/ListBuckets/Regions/BucketsByRegion/GetBucketMetadata/CreateMultipartUpload/CompleteMultipartUpload/AbortMultipartUpload/ListParts/janitor sweeps/Purge/Reset/Snapshot/Restore) against pkgs/store's documented semantics (no internal locking — still guarded by the single `b.mu`; `Index.Get` returns an index-owned slice — callers correctly copy IDs out before `Delete` in `purgeUploadsForBucketLocked`/`abortStaleMultipartUploads`). No wire-shape, error-code, state, or persistence regressions found — behavior is a faithful 1:1 port. `go build/vet/test -race/fix/golangci-lint` all clean (0 issues). SDK bumped v1.104.2→v1.105.0 (e51c0de9): CHANGELOG shows serializer-test-only change, `api_op_*.go` file set identical — no new ops to audit.
No fixes were required this sweep.
