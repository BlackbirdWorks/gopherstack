# Azure support

Gopherstack serves Azure data-plane and management APIs on dedicated ports next to the AWS port (`8000`). Each service has a fixed default port. A bind failure stops startup. There is no fallback port.

> [!NOTE]
> `AZURE.md section N` and `M<n>` citations in code comments refer to the archived implementation plan: [docs/history/AZURE.md](docs/history/AZURE.md).

## Services

Per-service coverage, grade and known gaps are in each service's generated `README.md` and its `PARITY.md`. Guides exist for the four storage and Cosmos DB services.

| Service | Default port | Flag prefix | Details |
|---------|--------------|-------------|---------|
| Blob Storage | `10000` | `--azure-blob-` | [guide](docs/services/azureblob.md), [parity](services/azureblob/README.md) |
| Queue Storage | `10001` | `--azure-queue-` | [guide](docs/services/azurequeue.md), [parity](services/azurequeue/README.md) |
| Table Storage | `10002` | `--azure-table-` | [guide](docs/services/azuretable.md), [parity](services/azuretable/README.md) |
| Service Bus (REST; AMQP on `5672`, TLS `5671`) | `10003` | `--azure-servicebus-` | [guide](docs/services/azureservicebus.md), [parity](services/azureservicebus/README.md) |
| Key Vault (HTTPS) | `10004` | `--azure-keyvault-` | [parity](services/azurekeyvault/README.md) |
| Event Hubs (AMQP `5673`, TLS `5674`, Kafka `9093`) | `10005` | `--azure-eventhubs-` | [parity](services/azureeventhubs/README.md) |
| Resource Manager and Entra ID endpoints (HTTPS) | `10006` | `--azure-arm-` | [parity](services/azurearm/README.md) |
| Event Grid (HTTPS) | `10007` | `--azure-eventgrid-` | [parity](services/azureeventgrid/README.md) |
| App Configuration (HTTPS) | `10008` | `--azure-appconfig-` | [parity](services/azureappconfig/README.md) |
| Files | `10009` | `--azure-file-` | [parity](services/azurefile/README.md) |
| Shared storage virtual-host listener (Blob, Queue, Table by `Host` header) | `10010` | `--azure-storage-vhost-` | [parity](services/azurestoragevhost/README.md) |
| Container Registry (HTTPS) | `10011` | `--azure-acr-` | [parity](services/azurecontainerregistry/README.md) |
| Functions | `10012` | `--azure-functions-` | [parity](services/azurefunctions/README.md) |
| Monitor Logs (HTTPS) | `10013` | `--azure-monitor-` | [parity](services/azuremonitor/README.md) |
| Microsoft Graph (HTTPS) | `10014` | `--azure-graph-` | [parity](services/azuregraph/README.md) |
| AI Search | `10015` | `--azure-search-` | [parity](services/azuresearch/README.md) |
| Cosmos DB (Core/SQL and Table API) | `8081` | `--cosmosdb-` | [guide](docs/services/cosmosdb.md), [parity](services/cosmosdb/README.md) |
| Cache for Redis | none (ARM-managed) | `--azure-redis-` | [parity](services/azureredis/README.md) |
| SQL Database | none (ARM-managed) | `--azure-sql-` | [parity](services/azuresql/README.md) |

Each service also has an environment variable for its port, for example `AZURE_BLOB_PORT` and `COSMOSDB_PORT`. Run `gopherstack --help` for the full list.

## Connect

Use the well-known Azurite development account for Blob, Queue and Table:

```
AccountName=devstoreaccount1;
AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;
QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;
TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;
```

(One line when used. Line breaks are for reading.)

Cosmos DB uses the public emulator master key at `http://localhost:8081`.

## Authentication

> [!WARNING]
> Credentials are not checked by default. Any account key, SAS token or bearer token is accepted.

- Blob signature verification is opt-in with `--azure-blob-verify-signatures`. Extra account keys go in `--azure-blob-account-keys`.
- Key Vault validates bearer tokens only with `--azure-keyvault-validate-tokens`.
- Event Hubs validates SAS only with `--azure-eventhubs-validate-sas`.
- HTTPS listeners generate a self-signed certificate on every start, valid for `localhost`, `127.0.0.1` and `::1`. Set the `*-tls-cert` and `*-tls-key` flags to use your own.

## Terraform and ARM

The Resource Manager listener serves cloud metadata, OpenID and token endpoints, subscriptions and resource-provider APIs, so the `azurerm` and `azuread` providers can point at it with `metadata_host`. Its `AZURE_ARM_ADVERTISE_*` variables set the data-plane endpoints that ARM resources report. Working provider setups and tests are in [`test/terraform/azure`](test/terraform/azure).

## Examples

- [`examples/azure-services`](examples/azure-services/README.md): Blob, Queue, Table and Cosmos DB with `curl`
- [`examples/azure-keyvault-app`](examples/azure-keyvault-app/README.md): Key Vault secret and key
- [`examples/azure-servicebus-cosmos-worker`](examples/azure-servicebus-cosmos-worker/README.md): Service Bus into Cosmos DB
