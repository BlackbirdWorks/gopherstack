# Lambda

Lambda functions run in containers. Supports image and zip packages, layers, versions and aliases, function URLs, event source mappings, capacity providers and code signing. Architecture details: [Lambda architecture](../architecture/lambda.md).

> [!IMPORTANT]
> A container runtime (Docker by default, or Podman) must be reachable to invoke functions.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws lambda create-function --function-name my-function \
    --package-type Image \
    --code ImageUri=public.ecr.aws/lambda/python:3.12 \
    --role arn:aws:iam::000000000000:role/my-role

aws lambda invoke --function-name my-function --payload '{"key":"value"}' response.json
aws lambda invoke --function-name my-function --invocation-type Event \
    --payload '{"key":"value"}' /dev/null

aws lambda update-function-code --function-name my-function --image-uri myrepo/myimage:v2

aws lambda create-event-source-mapping --function-name my-function \
    --event-source-arn arn:aws:sqs:us-east-1:000000000000:my-queue --batch-size 10

aws lambda create-function-url-config --function-name my-function --auth-type NONE
```

Zip functions take `--zip-file` or `--code S3Bucket=...,S3Key=...`. With the magic bucket `hot-reload`, `S3Key` is a local directory mounted into the function, and changes are picked up without redeploying. Disable with `LAMBDA_DISABLE_HOT_RELOAD`.

## Supported operations

| Area | Operations |
|------|------------|
| Functions | `CreateFunction`, `GetFunction`, `GetFunctionConfiguration`, `ListFunctions`, `UpdateFunctionCode`, `UpdateFunctionConfiguration`, `DeleteFunction`, `Invoke` |
| Versions and aliases | `PublishVersion`, `ListVersionsByFunction`, `CreateAlias`, `GetAlias`, `UpdateAlias`, `DeleteAlias`, `ListAliases` |
| Function URLs | `Create/Get/Update/Delete/ListFunctionUrlConfig(s)` |
| Event source mappings | `Create/Get/Update/Delete/ListEventSourceMapping(s)` |
| Layers | `PublishLayerVersion`, `GetLayerVersion`, `GetLayerVersionByArn`, `ListLayers`, `ListLayerVersions`, `DeleteLayerVersion`, `AddLayerVersionPermission`, `RemoveLayerVersionPermission`, `GetLayerVersionPolicy` |
| Permissions and policy | `AddPermission`, `RemovePermission`, `GetPolicy`, `PutResourcePolicy`, `GetResourcePolicy`, `DeleteResourcePolicy` |
| Concurrency and scaling | `Put/Get/DeleteFunctionConcurrency`, `Put/Get/Delete/ListProvisionedConcurrencyConfig(s)`, `PutFunctionScalingConfig`, `GetFunctionScalingConfig` |
| Async config | `Put/Get/Update/Delete/ListFunctionEventInvokeConfig(s)` |
| Capacity providers | `Create/Get/Update/Delete/ListCapacityProvider(s)` |
| Code signing | `Create/Get/Update/Delete/ListCodeSigningConfig(s)`, `Put/Get/DeleteFunctionCodeSigningConfig`, `ListFunctionsByCodeSigningConfig` |
| Settings | `GetAccountSettings`, `Put/GetFunctionRecursionConfig`, `Put/GetRuntimeManagementConfig` |
| Tags | `TagResource`, `UntagResource`, `ListTags` |
| Durable execution | `GetDurableExecution`, `CheckpointDurableExecution` |

## Configuration

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--lambda-docker-host` | `LAMBDA_DOCKER_HOST` | `172.17.0.1` | Host containers use to reach the Runtime API |
| `--lambda-container-runtime` | `CONTAINER_RUNTIME` | `docker` | `docker`, `podman`, `auto` or `kubernetes` |
| `--lambda-isolation` | `LAMBDA_ISOLATION` | `container` | `container`, `gvisor` or `kata` |
| `--lambda-docker-flags` | `LAMBDA_DOCKER_FLAGS` | none | Extra container options: `-e`, `-v`, `--add-host`, `--dns`, `--network`, `-m`, `--cpus`, `--platform` |
| `--lambda-pool-size` | `LAMBDA_POOL_SIZE` | `3` | Warm containers per function |
| `--lambda-idle-timeout` | `LAMBDA_IDLE_TIMEOUT` | `10m` | Idle container lifetime |
| `--lambda-max-runtimes` | `LAMBDA_MAX_RUNTIMES` | `50` | Simultaneous per-function runtimes |
| `--lambda-keep-containers` | `LAMBDA_KEEP_CONTAINERS` | `false` | Keep containers alive for debugging |
| `--lambda-disable-hot-reload` | `LAMBDA_DISABLE_HOT_RELOAD` | `false` | Disable hot reload |
| `--lambda-hot-reload-interval-ms` | `LAMBDA_HOT_RELOAD_INTERVAL_MS` | `250` | Minimum time between change scans |
| `--lambda-strict-runtime-validation` | `LAMBDA_STRICT_RUNTIME_VALIDATION` | `false` | Reject deprecated runtimes |
| `--lambda-max-environment-bytes` | `LAMBDA_MAX_ENVIRONMENT_BYTES` | `0` (AWS: 4096) | Environment variable size limit |
| `--lambda-code-size-zipped` | `LAMBDA_CODE_SIZE_ZIPPED` | `0` (AWS: 50 MB) | Zipped package limit |
| `--lambda-code-size-unzipped` | `LAMBDA_CODE_SIZE_UNZIPPED` | `0` (AWS: 250 MB) | Unzipped package limit |
| `--lambda-create-function-request-size` | `LAMBDA_CREATE_FUNCTION_REQUEST_SIZE` | `0` (AWS: 70167211) | `CreateFunction` request limit |
| `--lambda-account-total-code-size` | `LAMBDA_ACCOUNT_TOTAL_CODE_SIZE` | `0` (AWS: 75 GB) | Account code storage limit |
| `--lambda-account-concurrent-executions` | `LAMBDA_ACCOUNT_CONCURRENT_EXECUTIONS` | `0` (AWS: 1000) | Account concurrency limit |
| `--lambda-account-min-unreserved-concurrency` | `LAMBDA_ACCOUNT_MIN_UNRESERVED_CONCURRENCY` | `0` (AWS: 100) | Minimum unreserved concurrency |

## Remote debugging

Set `LAMBDA_DEBUG_MODE=1` to publish a debugger port from a function's container to `127.0.0.1`. Configure functions with a YAML/JSON file (`LAMBDA_DEBUG_MODE_CONFIG_PATH`) or `LAMBDA_DEBUG_MODE_FUNCTIONS=name=5678,other:1=5679`:

```yaml
functions:
  arn:aws:lambda:us-east-1:000000000000:function:my-fn:   # or "my-fn" / "my-fn:3"
    debug-port: 5678
    enforce-timeouts: false   # default true; false raises the invoke timeout to 1h
```

Env form: `LAMBDA_DEBUG_MODE_ENFORCE_TIMEOUTS=false`. Each function needs a distinct port. A port collision, duplicate function or bad port disables debug mode and logs an error at startup.

| Runtime | Injected | Attach with |
|---------|----------|-------------|
| Node.js | `NODE_OPTIONS=--inspect=0.0.0.0:PORT` | Node inspector |
| Java | `JAVA_TOOL_OPTIONS=-agentlib:jdwp=...,address=*:PORT` | JDWP remote |
| Python | `PYTHONPATH` bootstrap calling `debugpy.listen(PORT)` | debugpy (the package or a layer must contain `debugpy`) |
| .NET, Go, other | port published only | start `vsdbg` or `dlv --listen=:PORT` yourself |

A debug-mode function keeps one warm container that idle reaping and LRU eviction skip. Invocations queue on it. A qualifier in the config matches only that published version.

## Isolation modes

| Mode | Sandbox | Requirement |
|------|---------|-------------|
| `container` | Daemon default OCI runtime (runc), shared host kernel | none |
| `gvisor` | gVisor (`runsc`) | `runsc` registered with the Docker daemon |
| `kata` | Kata Containers VM | `kata` or `kata-runtime` registered with the daemon, plus KVM |

Register gVisor in `/etc/docker/daemon.json` and restart dockerd:

```json
{ "runtimes": { "runsc": { "path": "/usr/local/bin/runsc" } } }
```

At startup Gopherstack lists the daemon's runtimes and refuses to start if the one for the chosen mode is missing. `--lambda-isolation=firecracker` is rejected at startup.

## Differences from AWS

- `CONTAINER_RUNTIME=kubernetes`: EFS file systems, hot reload, layer extensions, `LAMBDA_DOCKER_FLAGS` and host DNS/CA injection are not supported on Pods.
- Telemetry and Logs API: not emitted are `platform.restoreStart`, `RuntimeDone`, `Report` and `platform.fault`. SnapStart is stored as configuration only; no snapshot is taken or restored.
- Invoke `TenantId` is validated but does not route to a tenant-dedicated environment.
- `UpdateFunctionCode` `SourceKMSKeyArn` is stored and echoed only. Function `KMSKeyArn` encryption of environment variables is real.
- EFS: subnet, AZ and security-group NFS reachability are checked only as VPC membership. `EFSIO` and `EFSMount*` errors are never raised.
- Capacity providers: execution environments are logical slots over the function's single container, not separate containers or instances.
- Stream event source mappings: `ParallelizationFactor` and `TumblingWindowInSeconds` map records onto ordered lanes by key.
- Kafka and MQ mappings: MSK and Amazon MQ sources are polled only when a real broker runs (`--kafka-engine=docker`, `--mq-engine=docker`). Self-managed Kafka reaches plaintext brokers only; `SourceAccessConfigurations` (SASL, mTLS, TLS CA, VPC) and MSK auth settings are ignored. ActiveMQ is consumed over STOMP, not OpenWire.
- `SchemaRegistryErrorCount` and `EventPollerUnit` metrics are not emitted.
