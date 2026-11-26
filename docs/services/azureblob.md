# Azure Blob Storage

In-memory Azure Blob Storage with an Azurite-compatible wire protocol. Block, append and page blobs, blob index tags, leases and snapshots.

## Quick start

```bash
curl -X PUT "http://localhost:10000/devstoreaccount1/my-container?restype=container"

curl -X PUT "http://localhost:10000/devstoreaccount1/my-container/hello.txt" \
    -H "x-ms-blob-type: BlockBlob" -H "x-ms-version: 2020-10-02" \
    --data-binary "hello world"

curl "http://localhost:10000/devstoreaccount1/my-container?restype=container&comp=list"
curl "http://localhost:10000/devstoreaccount1/my-container/hello.txt"
```

Go SDK:

```go
cred, _ := azblob.NewSharedKeyCredential("devstoreaccount1",
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
client, _ := azblob.NewClientWithSharedKeyCredential(
    "http://localhost:10000/devstoreaccount1", cred, nil)
_, _ = client.CreateContainer(ctx, "my-container", nil)
_, _ = client.UploadBuffer(ctx, "my-container", "hello.txt", []byte("hello"), nil)
```

The account is path-style: the account name is the first path segment. The built-in account is Azurite's `devstoreaccount1` with its published development key.

## Supported operations

| Area | Operations |
|------|------------|
| Account | `ListContainers` (`prefix`, `marker`, `maxresults`, `include=metadata`), `GetServiceProperties`, `SetServiceProperties`, `FindBlobsByTags` |
| Containers | `CreateContainer`, `DeleteContainer`, `GetContainerProperties`, `SetContainerMetadata`, `GetContainerACL`, `SetContainerACL`, `LeaseContainer` |
| Blobs | `PutBlob`, `GetBlob`, `DeleteBlob`, `ListBlobs`, `GetBlobProperties`, `SetBlobProperties`, `GetBlobMetadata`, `SetBlobMetadata`, `SetBlobTier`, `SnapshotBlob`, `CopyBlob`, `AbortCopyBlob`, `UndeleteBlob`, `LeaseBlob` |
| Block blobs | `PutBlock`, `PutBlockFromURL`, `PutBlockList`, `GetBlockList` |
| Append blobs | `AppendBlock` |
| Page blobs | `PutPage`, `GetPageRanges` |
| Tags | `SetBlobTags`, `GetBlobTags` |

`ListBlobs` supports `prefix`, `delimiter`, `maxresults`, `marker` and `include=snapshots,metadata`. `GetBlob` accepts one range through `Range` or `x-ms-range` (`x-ms-range` wins). Multi-range requests are rejected.

## Configuration

Blob runs on its own TCP port, not the shared AWS router. Startup fails if the port is taken.

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--azure-blob-port` | `AZURE_BLOB_PORT` | `10000` | Listener port |
| `--azure-blob-enforce-public-access` | `AZURE_BLOB_ENFORCE_PUBLIC_ACCESS` | `false` | Reject anonymous requests the container's public-access level does not allow |
| `--azure-blob-verify-signatures` | `AZURE_BLOB_VERIFY_SIGNATURES` | `false` | Verify SharedKey and SAS credentials (signature, validity window, IP, protocol, permissions, stored access policy) |
| `--azure-blob-account-keys` | `AZURE_BLOB_ACCOUNT_KEYS` | none | `account=base64key` pairs used when verification is on. The development account is always known |
| `--azure-blob-allow-external-copy` | `AZURE_BLOB_ALLOW_EXTERNAL_COPY` | `false` | Let `CopyBlob` fetch non-loopback source URLs |

## Differences from Azure

> [!WARNING]
> Signatures are not verified by default. Any credentials, or none, are accepted. Turn on `--azure-blob-verify-signatures` to check them.

- Blob versions (`x-ms-version-id`, `include=versions`) are not modelled.
- `CopyBlob` always completes synchronously, so `AbortCopyBlob` has nothing to abort. Incremental Copy, Append Block From URL and Put Page From URL are not implemented.
- User delegation SAS and bearer tokens are not verified.
- Page blobs: sequence-number actions and conditions, Get Page Ranges Diff and blobs above 256 MiB are not supported.

See also the [service parity audit](../../services/azureblob/PARITY.md) and [AZURE.md](../../AZURE.md).
