# Kubernetes (Helm chart)

`charts/gopherstack` deploys Gopherstack into a cluster. It renders a Deployment (one replica, `Recreate`), a Service, a ConfigMap of environment settings, and optionally a PVC, an Ingress and a Docker-in-Docker sidecar.

## Install

From a checkout:

```bash
helm install gopherstack ./charts/gopherstack
kubectl port-forward svc/gopherstack 8000:8000
AWS_ENDPOINT_URL=http://localhost:8000 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws s3 mb s3://demo
```

The image is `ghcr.io/blackbirdworks/gopherstack`, tagged with the chart `appVersion` unless `image.tag` is set. The Service is named after the release when the release name already contains `gopherstack`, otherwise `<release>-gopherstack` (or `fullnameOverride`); in-cluster clients use `http://<service>.<namespace>.svc.cluster.local:8000`.

## Common values

| Value | Default | Effect |
|---|---|---|
| `config.region`, `config.accountId`, `config.logLevel` | `us-east-1`, `000000000000`, `info` | `REGION`, `ACCOUNT_ID`, `LOG_LEVEL` |
| `config.services` / `config.disableServices` | `[]` | `GOPHERSTACK_SERVICES` / `GOPHERSTACK_DISABLE_SERVICES` |
| `config.engines` | `stub` | `GOPHERSTACK_ENGINES` (`stub`, `auto`, `docker`) |
| `config.enforceIam` | `false` | `GOPHERSTACK_ENFORCE_IAM` |
| `config.extra` | `{}` | any other env var, `KEY: value` |
| `persistence.enabled` | `false` | PVC mounted at `/data`, sets `PERSIST=true` and `GOPHERSTACK_DATA_DIR` |
| `service.type` / `service.port` | `ClusterIP` / `8000` | main endpoint (`PORT` follows `service.port`) |
| `service.extraPorts` | `[]` | extra named container and Service ports (below) |
| `ingress.*` | disabled | standard Ingress for the main port |
| `probes.*` | `/_gopherstack/health` | liveness, readiness and startup probes |
| `container.runtime` | `docker` | `kubernetes` runs Lambda functions and ECS tasks as Pods through the Kubernetes API (below) |
| `container.kubernetes.*` | see `values.yaml` | namespace, fetch image and code port for the Pod executor |
| `dind.enabled` | `false` | privileged Docker-in-Docker sidecar (below) |
| `resources`, `nodeSelector`, `tolerations`, `affinity` | empty | scheduling |

`values.schema.json` validates the values at install time.

## Persistence

With `persistence.enabled=true` Gopherstack snapshots to the PVC on shutdown and restores on start, so state survives pod restarts. Without it, state is in memory. Use `persistence.existingClaim` to bring your own PVC.

## Per-cloud ports

AWS services share the main port. Azure, GCP and OCI emulators listen on dedicated ports, listed in `fixedServicePorts` in `internal/app/cli.go`. Expose the ones you need:

```yaml
service:
  extraPorts:
    - name: azure-blob      # AZURE_BLOB_PORT, default 10000
      port: 10000
    - name: cosmos          # COSMOSDB_PORT, default 8081
      port: 8081
```

Names must be at most 15 characters. A few defaults: Azure Blob 10000, Queue 10001, Table 10002, Service Bus 10003, Cosmos 8081, GCP Pub/Sub 8085, GCP Storage 4443, IoT MQTT 1883. If you change a port with `config.extra` (for example `AZURE_BLOB_PORT`), change the matching `extraPorts` entry.

## Docker-backed engines

Engines that start real containers (RDS, EKS, Lambda containers, ECS tasks, EC2 docker provider, ...) need a Docker daemon, which a pod does not have. By default the chart runs with `config.engines=stub`, so those services answer from metadata only. Mounting the node's Docker socket is not supported by the chart.

To use them anyway, enable the sidecar:

```yaml
dind:
  enabled: true
config:
  engines: docker
```

This adds a `docker:dind` container that runs **privileged** and exposes `tcp://127.0.0.1:2375` (no TLS, pod-local) as `DOCKER_HOST`. It needs a cluster that permits privileged pods and is not covered by the install test.

## Kubernetes pod executor

`container.runtime=kubernetes` (sets `CONTAINER_RUNTIME=kubernetes`) runs Lambda function containers as Pods in the release namespace (or `container.kubernetes.namespace`), so no privileged sidecar is needed. Docker stays the default. The chart then adds:

- a namespace-scoped `Role` and `RoleBinding`: `pods` create/get/list/watch/delete and `pods/log` get. No cluster-wide rules; `secrets` (create/patch/delete) and `pods/exec` (create) only with `container.kubernetes.ecs.enabled` / `.exec`;
- the ServiceAccount token mount (off otherwise);
- `POD_IP`, `LAMBDA_DOCKER_HOST` and `CONTAINER_K8S_ADVERTISE_HOST` set to the Gopherstack Pod IP, plus the `fn-code` container port.

How it works:

- Function Pods get no ServiceAccount token. They reach the Lambda Runtime API at `<gopherstack pod IP>:<per-function port>` (ports come from `PORT_RANGE_START`...; each function runtime listens on its own port), so a NetworkPolicy must allow function Pods to reach the Gopherstack Pod on those ports and on `container.kubernetes.codePort`. A Service port is not used because the ports are dynamic.
- There are no bind mounts. The function zip and layers are held in memory and an initContainer (`container.kubernetes.fetchImage`, needs `sh`, `wget`, `unzip`) downloads them from Gopherstack on the code port into read-only `emptyDir` volumes at `/var/task` and `/opt`. Each Pod gets a random bearer token (in the initContainer env only, never logged) that is valid for that Pod's archives alone and is revoked as soon as the Pod is running or removed. The transfer is plain HTTP inside the cluster; the token is visible to whoever can read Pods in the namespace, so restrict `get pods` there. Zips with absolute or `..` entries are rejected.
- Memory limit and request equal the function memory size. `OCIRuntime` (Lambda isolation) maps to `runtimeClassName`.
- Pods are labelled `gopherstack.managed=true`; the existing startup reconcile lists them and removes dead leftovers of earlier runs.

Not supported by the Pod executor yet: EC2 and other engines (they still need docker), EFS file systems, Lambda hot reload, extensions in layers, `LAMBDA_DOCKER_FLAGS`, host DNS/CA injection, Lambda `Exec`, and container stop/start (a Pod is created or deleted).

### ECS tasks as Pods

With `CONTAINER_RUNTIME=kubernetes` and ECS containers enabled (`config.engines=docker` or `GOPHERSTACK_ECS_RUNTIME=docker`) every ECS task is **one Pod**; its containers share the Pod's network namespace, as under `awsvpc`. The Pod is named `ecs-<task id>`. Chart values `container.kubernetes.ecs.enabled` (secrets create/patch/delete) and `container.kubernetes.ecs.exec` (`pods/exec` create) add only the grants the feature needs; both default to off.

| ECS | Pod |
|---|---|
| `entryPoint`, `command`, `environment`, `workingDirectory`, `user` (numeric or `root`), `readonlyRootFilesystem`, `linuxParameters.capabilities` | `command`, `args`, `env`, `workingDir`, `runAsUser`/`runAsGroup`, `readOnlyRootFilesystem`, `capabilities` |
| `secrets`, `environmentFiles` | resolved server-side into a Kubernetes `Secret` named after the Pod and referenced with `secretKeyRef`. The Pod owns the Secret (`ownerReferences`), and it is deleted with the task. Values are never in the Pod spec, logs or annotations |
| container `memory` / `memoryReservation` | memory limit / request |
| container `cpu` (units, 1024 = 1 vCPU) | cpu request. Task `cpu` and `memory` cap every container, as ECS caps the task |
| `healthCheck` (`CMD`, `CMD-SHELL`) | exec `readinessProbe` (interval, timeout, retries, startPeriod). Health is UNKNOWN until the first success, then HEALTHY/UNHEALTHY; a container that never becomes ready stays UNKNOWN |
| `stopTimeout` | `terminationGracePeriodSeconds` (the largest container value, default 30) |
| `essential` | the task stops when an essential container exits (`EssentialContainerExited`), exit codes are recorded per container; a non-essential exit leaves the task running |
| `dependsOn` `SUCCESS` | the target runs as an init container, in dependency order. It must be non-essential; a non-zero exit fails the task (`TaskFailedToStart`) |
| `dependsOn` `START` | accepted: containers are listed in dependency order but a Pod starts them together, so startup order is not guaranteed |
| `volumesFrom`, `mountPoints`, `linuxParameters.tmpfs`, `sharedMemorySize` | the source container's mounts, `emptyDir` volumes (memory-backed for tmpfs and `/dev/shm`) |
| task `volumes` with no host path, task-scoped `dockerVolumeConfiguration` | `emptyDir`, shared by the task's containers and gone with the Pod |
| `portMappings` | `containerPort` entries. Reachability is `<Pod IP>:<containerPort>`; `hostPort` is ignored. The task's `privateIPv4Address` becomes the Pod IP, so ECS service load balancer registration (ELBv2) and Cloud Map use it |
| `logConfiguration` `awslogs` | each container's Pod log stream is forwarded to CloudWatch Logs. The stream name is `<prefix>/<container>/<task id>`, or `<pod>/<container>` without a prefix |
| ECS Exec (`ExecuteCommand`) | `pods/exec` with a TTY, through the SSM session data channel (needs `container.kubernetes.ecs.exec`) |
| task state | PROVISIONING, PENDING and ACTIVATING are driven by the backend; RUNNING once the Pod runs, STOPPED with `stopCode`, `stoppedReason`, exit codes and `OutOfMemoryError` reasons from the Pod's container statuses. A Pod deleted outside Gopherstack stops the task (exit 137). Image pull failures report `CannotPullContainerError` |

Refused with `TaskFailedToStart` (a clear `stoppedReason`) instead of being approximated: host-path volumes, EFS, `configuredAtLaunch`, shared `dockerVolumeConfiguration`, `networkMode` `host`/`none`, `pidMode`/`ipcMode` `host`, `privileged`, `disableNetworking`, `devices`, container `restartPolicy`, `repositoryCredentials`, FireLens, and `dependsOn` `COMPLETE` and `HEALTHY`. `ulimits` and `ephemeralStorage` are ignored. Security-group enforcement, task adoption after a Gopherstack restart (leftover Pods are removed by the startup reconcile) and task networking policies are not implemented for Pods.

Running Gopherstack outside the cluster: set `CONTAINER_RUNTIME=kubernetes`, `CONTAINER_K8S_KUBECONFIG=<path>` and `CONTAINER_K8S_ADVERTISE_HOST=<an address Pods can reach this host on>` (also `LAMBDA_DOCKER_HOST` to the same value).

## Security

The container drops all capabilities, forbids privilege escalation, uses the `RuntimeDefault` seccomp profile and a read-only root filesystem (`/tmp` and `/data` are writable volumes). It runs as the image's non-root user (UID/GID 65532, `runAsNonRoot: true`); `fsGroup` 65532 makes the PVC writable. The release image is distroless `static:nonroot`, so its root directory is world-searchable under containerd as well. To run docker-backed engines from the cluster, enable the `dind` sidecar rather than mounting a host socket. It mounts no service account token and creates no RBAC unless `container.runtime=kubernetes` (see above).

## Tests

```bash
task helm-test        # lint + golden templates (docker, pinned alpine/helm image)
task helm-install-test  # installs into a real k3s cluster started with pkgs/k3sengine
```

Both run `go test ./test/helm/` with `GOPHERSTACK_TEST_DOCKER=1`. The install test loads a locally built image (`bin/gopherstack-linux` via `Dockerfile.test`, or `GOPHERSTACK_HELM_IMAGE`), installs the chart with persistence on a NodePort, checks `/_gopherstack/health`, creates and lists an S3 bucket, restarts the pod to prove persistence, then uninstalls. Refresh goldens with `GOPHERSTACK_UPDATE_GOLDEN=1`.
