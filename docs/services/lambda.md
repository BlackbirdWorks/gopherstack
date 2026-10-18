# Lambda

Gopherstack supports AWS Lambda with **Docker image-based functions only**.

> **Important:** Only `PackageType: Image` is supported. Zip deployments, S3-based code delivery, and inline code are **not supported**. Functions must be packaged as Docker images.

For full Lambda documentation including the runtime architecture, container pool management, and Podman support, see [`docs/architecture/lambda.md`](../architecture/lambda.md).

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateFunction` | Create a new image-based Lambda function |
| `GetFunction` | Get function configuration and code location |
| `ListFunctions` | List all functions |
| `DeleteFunction` | Delete a function and its warm containers |
| `UpdateFunctionCode` | Update the Docker image URI |
| `UpdateFunctionConfiguration` | Update memory, timeout, environment variables, etc. |
| `InvokeFunction` | Invoke a function synchronously or asynchronously |
| `CreateEventSourceMapping` | Create an SQS trigger for a function |
| `GetEventSourceMapping` | Get a specific event source mapping |
| `ListEventSourceMappings` | List event source mappings |
| `DeleteEventSourceMapping` | Remove an event source mapping |
| `CreateFunctionURLConfig` | Create a function URL |
| `GetFunctionURLConfig` | Get the function URL configuration |
| `DeleteFunctionURLConfig` | Delete a function URL |
| `PublishVersion` | Publish the current function code as an immutable version |
| `ListVersionsByFunction` | List published versions |
| `CreateAlias` | Create a function alias pointing to a version |
| `GetAlias` | Get an alias configuration |
| `ListAliases` | List all aliases for a function |

## AWS CLI Examples

```bash
# Create an image-based function
aws --endpoint-url http://localhost:8000 lambda create-function \
    --function-name my-function \
    --package-type Image \
    --code ImageUri=public.ecr.aws/lambda/python:3.12 \
    --role arn:aws:iam::000000000000:role/my-role

# Invoke synchronously
aws --endpoint-url http://localhost:8000 lambda invoke \
    --function-name my-function \
    --payload '{"key":"value"}' \
    response.json

# Invoke asynchronously (fire-and-forget)
aws --endpoint-url http://localhost:8000 lambda invoke \
    --function-name my-function \
    --invocation-type Event \
    --payload '{"key":"value"}' \
    /dev/null

# Update the image
aws --endpoint-url http://localhost:8000 lambda update-function-code \
    --function-name my-function \
    --image-uri myrepo/myimage:v2

# Create an SQS trigger
aws --endpoint-url http://localhost:8000 lambda create-event-source-mapping \
    --function-name my-function \
    --event-source-arn arn:aws:sqs:us-east-1:000000000000:my-queue \
    --batch-size 10

# Create a function URL
aws --endpoint-url http://localhost:8000 lambda create-function-url-config \
    --function-name my-function \
    --auth-type NONE
```

## Known Limitations

- Only `PackageType: Image` is supported. Zip/S3 deployments are not implemented.
- Docker must be running on the host machine.
- Lambda Layers are not supported.
- VPC configuration is accepted but not applied.
- Reserved concurrency and provisioned concurrency are accepted but not enforced.

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--lambda-docker-host` | `LAMBDA_DOCKER_HOST` | `172.17.0.1` | Host Lambda containers use to reach Gopherstack |
| `--lambda-pool-size` | `LAMBDA_POOL_SIZE` | `3` | Warm container pool size per function |
| `--lambda-idle-timeout` | `LAMBDA_IDLE_TIMEOUT` | `10m` | Idle container lifetime before reaping |
| `--container-runtime` | `CONTAINER_RUNTIME` | `docker` | `docker`, `podman`, or `auto` |

## Remote debugging

Set `LAMBDA_DEBUG_MODE=1` to publish a debugger port from a function's container to `127.0.0.1` on the host, so an IDE can attach. Configure functions with a YAML/JSON file (`LAMBDA_DEBUG_MODE_CONFIG_PATH`) or the env form `LAMBDA_DEBUG_MODE_FUNCTIONS=name=5678,other:1=5679`:

```yaml
functions:
  arn:aws:lambda:us-east-1:000000000000:function:my-fn:   # or "my-fn" / "my-fn:3"
    debug-port: 5678
    enforce-timeouts: false   # default true; false raises the invoke timeout to 1h
```

Env form timeouts: `LAMBDA_DEBUG_MODE_ENFORCE_TIMEOUTS=false`. Each function needs a distinct port; a collision, duplicate function or bad port disables debug mode and logs the error at startup.

| Runtime | Injected | Attach with |
|---------|----------|-------------|
| Node.js | `NODE_OPTIONS=--inspect=0.0.0.0:PORT` | Node inspector |
| Java | `JAVA_TOOL_OPTIONS=-agentlib:jdwp=...,address=*:PORT` | JDWP remote |
| Python | `PYTHONPATH` bootstrap calling `debugpy.listen(PORT)` | debugpy (the function package or a layer must contain `debugpy`) |
| .NET, Go, other | port published only | start `vsdbg` / `dlv --listen=:PORT` yourself (e.g. in a `provided.*` bootstrap) |

A debug-mode function keeps one warm container that the idle reaper and LRU eviction skip; invocations of it queue on that container. A qualifier in the config only matches invocations of that published version.
