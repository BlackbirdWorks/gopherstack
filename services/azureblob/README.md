<!-- Mirrors the format cmd/gendocs renders from PARITY.md for other services. Hand-authored this pass since cli.go is not yet wired to this provider (see Notes) -- keep in sync with PARITY.md by hand until that lands. -->
# Azure Blob Storage

**Parity grade: C** · SDK `azure-sdk-for-go/sdk/storage/azblob@v1.7.0` · last audited 2026-09-02 (initial seed)

## Coverage

| Metric | Value |
| --- | --- |
| PARITY entries audited | 8 (7 ok, 1 partial) |
| Feature families | 3 (2 ok, 1 deferred) |
| Known gaps | 8 |
| Deferred items | 3 |
| Resource leaks | clean |

### Known gaps

- Put Block / Put Block List (large-object multipart upload) is not implemented -- Put Blob only accepts a single whole-body BlockBlob PUT. Deliberate M0 scope per AZURE.md; tracked for a later milestone.
- No ACL / container public-access-level support (x-ms-blob-public-access, Set/Get Container ACL are unimplemented).
- No blob or container metadata (x-ms-meta-* headers) -- neither stored on PUT/Create nor returned on GET/HEAD/List.
- No conditional-header support (If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since) on any operation.
- No Copy Blob (server-side or cross-account) support.
- No snapshot, versioning, soft-delete, lease, or tier (hot/cool/archive) support.
- List Containers / List Blobs return every result in one page; no prefix/marker/maxresults pagination.
- Auth is structurally permissive only: the Authorization header is accepted on shape alone (a `SharedKey ...` prefix, or absent) and never cryptographically verified. Real SharedKey verification depends on `pkgs/azureauth`, which lands on a separate branch and is not yet importable from this package.

All gaps above are intentional MVP scope per AZURE.md's M0/M1 split, not oversights.

### Deferred

- Initial implementation pass (2026-09-02): seeded this service from scratch per AZURE.md M0. No prior audit history to reconcile.
- `cli.go` registration is deliberately not wired up in this pass -- a human integrates this provider once `pkgs/azureauth` (branch `azure/auth-pkg`) also lands.
- No Go integration test (`test/integration/azureblob_test.go`) yet -- that requires the `cli.go` wiring above. Unit tests exercise the handler/backend directly via `httptest` instead.

## More

- [Full parity audit](PARITY.md)
- [All services](../../README.md#services)
