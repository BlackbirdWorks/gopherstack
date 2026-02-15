# Improvement Backlog

## Critical Bugs

- [x] **Expression cache `RLock` during write** — `ExpressionCache.Get()` calls `lru.MoveToFront()` (a mutation) while holding `RLock` instead of `Lock` (`dynamodb/expression_cache.go:30-44`)

- [x] **Handler breaks `StorageBackend` abstraction** — `putBucketVersioning` type-asserts to `*InMemoryBackend` and directly manipulates `backend.mu` (`s3/handler.go:473-486`). Should add `PutBucketVersioning` to the interface.

- [x] **Version slice returned by reference** — `getLatestVersion`/`getSpecificVersion` return `&v` (pointer into slice element). Concurrent modifications corrupt data (`s3/backend_memory.go:305-325`)

---

## Concurrency & Performance

- [x] **O(n) item lookups ignoring indexes** — `findExistingItem` does linear scan instead of using `pkIndex`/`pkskIndex`. Called on every `DeleteItem`, `UpdateItem`, `BatchGetItem`, `BatchWriteItem` (`dynamodb/item_ops.go:903-921`)

- [x] **Full index rebuild on every delete** — `DeleteItem` removes from the slice and calls `rebuildIndexes()` which is O(n). Should do incremental index update instead (`dynamodb/item_ops.go:243-246`)

- [ ] **S3 single global mutex** — One `sync.RWMutex` for the entire backend. Operations on different buckets contend needlessly. Should use per-bucket locking (`s3/backend_memory.go:24`)

- [x] **Lock held during compression** — `PutObject` holds the global lock while calling `compressor.Compress()`, blocking all reads during I/O (`s3/backend_memory.go:117-125`)

- [x] **O(n) version prepending** — `append([]ObjectVersion{ver}, obj.Versions...)` copies the entire slice on every write (`s3/backend_memory.go:195`)

- [x] **Batch operation deadlock risk** — `BatchWriteItem` locks tables in map iteration order. Two concurrent batches locking tables A,B and B,A can deadlock. Need sorted lock ordering (`dynamodb/item_ops.go:987-1006`)

- [x] **`reflect.DeepEqual` on hot path** — Used for key comparison in item lookups. Should do type-aware comparison since values are always DynamoDB attribute maps (`dynamodb/item_ops.go:1057`)

---

## Realism Gaps

- [ ] **No multipart uploads** — Can't handle large files. Missing `InitiateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`

- [x] **No Range requests** — No partial object downloads (`Range` header / `206 Partial Content`)

- [x] **No ListObjects pagination** — `IsTruncated` always false, `MaxKeys` ignored. Breaks with >1000 objects (`s3/handler.go:269-275`)

- [x] **No `TransactWriteItems`/`TransactGetItems`** — Critical DynamoDB feature for atomic multi-item operations

- [x] **No TTL support** — No expiration attribute handling or background cleanup

- [x] **`ReturnConsumedCapacity` returns hardcoded values** — Always returns 1.0 regardless of item size (`dynamodb/item_ops.go:150-155`)

- [ ] ** Realistic S3 error codes** — Return actual S3 error codes instead of generic `InternalServerError`

- [ ] **Realistic DynamoDB error codes** — Return actual DynamoDB error codes instead of generic `InternalServerError`

---

## Go Idioms & Code Quality

- [x] **Ignored errors in handler writes** — `w.Write(...)` and `xml.Encode(...)` errors silently dropped (`s3/handler.go:394,621`; `dynamodb/handler.go:107,124,140`)

- [x] **4 nearly-identical version-finding functions** — `getLatestVersion`, `findLatestVersion`, `getSpecificVersion`, `findSpecificVersion` — DRY violation (`s3/backend_memory.go:263-325`)

- [ ] **Type unwrapping duplicated 5+ times** — `unwrapAttributeValue`, `parseStr`, `toString`, `dbExtractValueFromToken` all do the same `map[string]any` extraction (`dynamodb/expressions.go`)

- [ ] **`item_ops.go` is 1249 lines** — Should split by operation type (put, query, scan, batch)

- [ ] **String-based expression parsing** — Brittle `strings.Split()` approach. No tokenizer or AST. Breaks on nested functions (`dynamodb/expressions.go:268-308`)

- [ ] **No input validation in S3** — Bucket names and keys extracted from URL with no RFC 3561 validation (`s3/handler.go:60-65`)

- [ ] **`CalculateItemSize` marshals entire item to JSON** — Just to get `len(b)`. Should calculate incrementally (`dynamodb/validation.go:19-39`)

- [ ] **Missing `context.Context` in S3 backend** — No way to timeout or cancel long operations
