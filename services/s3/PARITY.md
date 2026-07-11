---
service: s3
sdk_module: aws-sdk-go-v2/service/s3   # version: see go.mod (backfilled, confirm on next audit)
last_audit_commit: 708d1961
last_audit_date: 2026-07-04
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
- Persistence: backendSnapshot covers buckets/tags/uploads/region; per-bucket configs on StoredBucket; bucketIndex rebuilt on restore; legacy-uploads migration present.
- Trap: `x-amz-storage-class` header is OMITTED for STANDARD objects (AWS behavior) — don't re-add it.
