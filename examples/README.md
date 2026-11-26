# Gopherstack Examples

Runnable end-to-end examples. Each directory has a `demo.sh`. All but `gcp-run-pubsub-push` also have a `docker-compose.yml` that builds and starts Gopherstack from this checkout.

## Examples

| Example | Purpose | Tools Required |
|---------|---------|----------------|
| **`apigw-websocket-chat`** | API Gateway WebSocket API with a Node.js Lambda that echoes each message back. | OpenTofu, `wscat` |
| **`azure-services`** | Exercises Azure Blob/Queue/Table Storage and Azure Cosmos DB side-by-side in one gopherstack instance. | Bash (`curl`, `grep`, `head`, `sed`) |
| **`azure-keyvault-app`** | Go app reads its config from a Key Vault secret, signs a payload with a Key Vault key and verifies it. | Go, Bash (`curl`) |
| **`azure-servicebus-cosmos-worker`** | Go worker consumes a Service Bus queue into Cosmos DB, plus a change-feed reader printing the new documents. | Go, Bash (`curl`) |
| **`cognito-api-auth`** | Secures API Gateway endpoints using Cognito User Pool Authorizer and JWTs. | OpenTofu, Bash |
| **`ddb-lambda-chain`** | A 3-table event stream pipeline where DynamoDB Streams trigger a Go Lambda function. | Go, Bash |
| **`ec2-docker`** | Demonstrates provisioning EC2 instances and running SSH commands on them locally. | Bash (`ssh`, `dig`) |
| **`elasticache-valkey`** | Demonstrates provisioning an ElastiCache Valkey cluster and connecting via embedded DNS. | Bash (`valkey-cli` or `redis-cli`) |
| **`eventbridge-sqs`** | Routes custom events from Amazon EventBridge to an SQS queue. | AWS CLI v2 |
| **`gcp-bigquery-gcs-load`** | Uploads a CSV to Cloud Storage, runs a BigQuery load job and an aggregate query. | Go, Bash (`curl`) |
| **`gcp-run-pubsub-push`** | A Cloud Run service (Docker image) receiving Pub/Sub push messages. | Docker, Go, Bash (`curl`) |
| **`iam-role-enforcement`** | IAM enforcement with STS AssumeRole: an assumed role is allowed on one bucket and table and denied on another. | AWS CLI v2 |
| **`kinesis-lambda-aggregator`** | Kinesis stream invokes a Node.js Lambda to aggregate events into DynamoDB. | OpenTofu, Bash |
| **`s3-access-logs`** | Demonstrates configuring S3 server access logging. | AWS CLI v2 |
| **`s3-lambda-processor`** | S3 object uploads trigger a Go Lambda function to process the object. | OpenTofu, Go, Bash |
| **`sns-sqs-fanout`** | A pub/sub fanout architecture distributing SNS messages to multiple SQS queues. | AWS CLI v2 |
| **`stepfunctions-order-workflow`** | Serverless orchestration using Step Functions to manage an order processing state machine. | OpenTofu, Bash |

`extensions/hello` is a sample extension, not a demo. See its [README](extensions/hello/README.md).

## Prerequisites

- Docker with Compose v2 (`docker compose`).
- AWS CLI v2, for the AWS examples (the scripts pass `--no-cli-pager`).
- The tools in the table above.

> [!NOTE]
> Compose files use `build: ../..`. The first `up` builds the image from this repository and takes a few minutes.

## Run

```bash
cd examples/<name>
docker compose up -d --build
docker compose ps        # wait until healthy
./demo.sh
```

Clean up:

```bash
docker compose down -v
```

| Example | Difference |
|---------|------------|
| `gcp-run-pubsub-push` | No compose file. Run Gopherstack on the host with `go run .` from the repo root. |

> [!WARNING]
> Examples that run Lambda functions or EC2 instances mount `/var/run/docker.sock` into the Gopherstack container and run it as root. Anything that can reach that container controls the host Docker daemon. Use them on a development machine only.
