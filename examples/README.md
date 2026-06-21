# Gopherstack Examples

This directory contains complete, runnable examples demonstrating how to use Gopherstack to build and test serverless architectures locally. These examples cover a variety of AWS services interacting with each other, such as API Gateway, Lambda, DynamoDB, S3, Step Functions, and more.

## Prerequisites

To run these examples locally, you need:
1. **Gopherstack** running on `http://localhost:8000` (e.g. `LAMBDA_DOCKER_HOST=host.docker.internal go run .` from the repo root).
2. **OpenTofu** (or Terraform) for the Infrastructure-as-Code examples.
3. **AWS CLI** configured to hit the local endpoint (the scripts typically handle this via `--endpoint-url http://localhost:8000`).
4. **Docker** (if running the Docker Compose examples).
5. **Node.js** or **Go** to build the Lambda functions, depending on the example.

## Available Examples

### 1. API Gateway Websocket Chat (`apigw-websocket-chat`)
A real-time chat application using API Gateway WebSockets and a Node.js Lambda backend. Demonstrates connection management, broadcasting messages, and the `@connections` API.
- **Tools**: OpenTofu, Node.js.

### 2. Cognito API Authentication (`cognito-api-auth`)
Secures an API Gateway endpoint using an Amazon Cognito User Pool Authorizer. Demonstrates signing up a user, retrieving a JWT, and passing it to the API Gateway to invoke a Lambda function.
- **Tools**: OpenTofu, Bash `demo.sh`.

### 3. DynamoDB Lambda Chain (`ddb-lambda-chain`)
A 3-table event stream pipeline where DynamoDB Streams trigger a Go Lambda function that replicates the item into the next table in the chain. 
- **Tools**: Docker Compose, Go, Bash `demo.sh`.

### 4. EC2 Docker (`ec2-docker`)
Demonstrates provisioning EC2 instances and running Docker containers on them.
- **Tools**: Docker Compose.

### 5. ElastiCache Valkey (`elasticache-valkey`)
Demonstrates provisioning an ElastiCache Valkey cluster and connecting to it.
- **Tools**: Docker Compose.

### 6. EventBridge to SQS (`eventbridge-sqs`)
Routes custom events from Amazon EventBridge to an SQS queue.
- **Tools**: OpenTofu.

### 7. Kinesis Lambda Aggregator (`kinesis-lambda-aggregator`)
A stream processing example where a Kinesis data stream invokes a Node.js Lambda function to aggregate events and write the metrics into DynamoDB.
- **Tools**: OpenTofu, Bash `demo.sh`.

### 8. S3 Access Logs (`s3-access-logs`)
Demonstrates configuring S3 server access logging.
- **Tools**: OpenTofu.

### 9. S3 Lambda Processor (`s3-lambda-processor`)
An event-driven architecture where uploading a file to an S3 bucket triggers a Go Lambda function to process the object.
- **Tools**: OpenTofu, Go, Bash `demo.sh`.

### 10. SNS to SQS Fanout (`sns-sqs-fanout`)
A classic pub/sub fanout architecture where an SNS topic distributes messages to multiple SQS queues.
- **Tools**: OpenTofu.

### 11. Step Functions Order Workflow (`stepfunctions-order-workflow`)
A serverless orchestration example using AWS Step Functions to manage an order processing state machine, invoking Lambda functions at each step.
- **Tools**: OpenTofu, Bash `demo.sh`.

## Running an Example

All examples include a `docker-compose.yml` file for easy setup and teardown. 

To run an example, simply navigate into its directory and run `docker-compose up`:
```bash
cd examples/kinesis-lambda-aggregator
docker-compose up --build --abort-on-container-exit
```


You do not need to run Gopherstack manually in the background; the `docker-compose.yml` will automatically spin up Gopherstack and the demo script runner in isolated containers.
