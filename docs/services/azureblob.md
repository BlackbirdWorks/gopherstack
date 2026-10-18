# AzureBlob — Azure Blob Storage

Gopherstack provides an in-memory Azure Blob Storage implementation (Azurite-compatible wire protocol) with container/blob CRUD, metadata, content properties, public access levels, hierarchical listing, snapshots, access tiers, and single-range downloads.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `ListContainers` | List all containers in the account (`include=metadata`; single page) |
| `CreateContainer` | Create a container with optional `x-ms-meta-*` and `x-ms-blob-public-access` |
| `DeleteContainer` | Delete a container |
| `GetContainerProperties` | Container ETag, metadata and public-access level (`GET`/`HEAD`) |
| `SetContainerMetadata` / `GetContainerACL` / `SetContainerACL` | Container metadata, public access level and stored access policies |
| `ListBlobs` | List blobs with `prefix`, `delimiter` (`BlobPrefix`), `maxresults` + `marker`, `include=snapshots,metadata` |
| `PutBlob` | Upload a blob (BlockBlob only) with metadata, content properties and `x-ms-access-tier` |
| `GetBlob` | Download a blob or `?snapshot=` (single `Range` or `x-ms-range`); archived blobs return 409 |
| `GetBlobProperties` / `GetBlobMetadata` | Properties and `x-ms-meta-*` via `HEAD` / `?comp=metadata` |
| `SetBlobProperties` / `SetBlobMetadata` / `SetBlobTier` | Persisted content properties, metadata and Hot/Cool/Cold/Archive tier |
| `SnapshotBlob` / `CopyBlob` | `?comp=snapshot`; same-account synchronous copy via `x-ms-copy-source` |
| `DeleteBlob` | Delete a blob or one `?snapshot=`; `x-ms-delete-snapshots` include/only |

## Configuration

Azure Blob runs on its own dedicated, synchronously-bound TCP port — it is never multiplexed onto the shared AWS single-port router, since its `/<account>/<container>/<blob>` path shape would collide with Azure Queue/Table's identical shape.

| Setting | Default | Flag | Env var |
|---|---|---|---|
| Port | `10000` (Azurite's own Blob port) | `--azure-blob-port` | `AZURE_BLOB_PORT` |
| Enforce public access | `false` | `--azure-blob-enforce-public-access` | `AZURE_BLOB_ENFORCE_PUBLIC_ACCESS` |

Startup fails fast if the port is unavailable — there is no fallback pool.

## Authentication

Gopherstack accepts Azurite's published well-known development storage account by default:

```text
Account name: devstoreaccount1
Account key:  Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
```

The `Authorization` header is parsed (SharedKey/SharedKeyLite) but **signature verification is not enforced** — any (or no) credentials are accepted. There is no `--azure-blob-validate-auth` flag; this differs from Cosmos DB, which does offer opt-in enforcement.

## Go SDK Example

```go
import "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

cred, _ := azblob.NewSharedKeyCredential("devstoreaccount1",
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")

// Path-style addressing: account name is the first path segment.
client, _ := azblob.NewClientWithSharedKeyCredential(
    "http://localhost:10000/devstoreaccount1", cred, nil)

ctx := context.Background()
_, _ = client.CreateContainer(ctx, "my-container", nil)
_, _ = client.UploadBuffer(ctx, "my-container", "hello.txt", []byte("hello"), nil)
resp, _ := client.DownloadStream(ctx, "my-container", "hello.txt", nil)
```

## curl Examples

```bash
# Create a container
curl -X PUT "http://localhost:10000/devstoreaccount1/my-container?restype=container"

# Upload a blob (BlockBlob)
curl -X PUT "http://localhost:10000/devstoreaccount1/my-container/hello.txt" \
    -H "x-ms-blob-type: BlockBlob" \
    -H "x-ms-version: 2020-10-02" \
    --data-binary "hello world"

# List blobs
curl "http://localhost:10000/devstoreaccount1/my-container?restype=container&comp=list"

# Download a blob
curl "http://localhost:10000/devstoreaccount1/my-container/hello.txt"
```

## Known Limitations

- No Put Block / Put Block List — only whole-body single-request BlockBlob uploads (no large-object multipart upload).
- No conditional-header support (`If-Match`/`If-None-Match`/`If-Modified-Since`/`If-Unmodified-Since`) on any operation.
- Copy Blob is same-account and synchronous only (no cross-account or URL sources).
- No blob versions, soft-delete, or leases; Archive rehydration is instantaneous.
- `ListContainers` returns every container in one page — no prefix/marker/maxresults.
- Anonymous requests honour container public-access levels only with `--azure-blob-enforce-public-access` (`AZURE_BLOB_ENFORCE_PUBLIC_ACCESS`); by default unsigned requests are unrestricted.
- `GetBlob` supports a single range via `Range` or `x-ms-range` (the latter takes precedence when both are sent, matching real Azure); multi-range requests (`bytes=0-1,3-4`) are rejected rather than served.
- Auth verification is not enforced (see Authentication above).

## More

- [Service README](../../services/azureblob/README.md)
- [Full parity audit](../../services/azureblob/PARITY.md)
- [AZURE.md](../../AZURE.md) — overall Azure support plan and milestones
- [All services](../../README.md#services)
