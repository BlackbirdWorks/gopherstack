# Kubernetes (Helm chart)

`charts/gopherstack` renders a Deployment (one replica, `Recreate`), a Service, a ConfigMap of environment settings, and optionally a PVC, an Ingress, a Docker-in-Docker sidecar and Pod-executor RBAC.

## Install

```bash
helm install gopherstack ./charts/gopherstack
kubectl port-forward svc/gopherstack 8000:8000
AWS_ENDPOINT_URL=http://localhost:8000 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws s3 mb s3://demo
```

The image is `ghcr.io/blackbirdworks/gopherstack`, tagged with the chart `appVersion` unless `image.tag` is set. The Service is named after the release if the release name contains `gopherstack`, otherwise `<release>-gopherstack` (or `fullnameOverride`). In-cluster URL: `http://<service>.<namespace>.svc.cluster.local:8000`.

## Values

| Value | Default | Effect |
|---|---|---|
| `config.region`, `config.accountId`, `config.logLevel` | `us-east-1`, `000000000000`, `info` | `REGION`, `ACCOUNT_ID`, `LOG_LEVEL` |
| `config.services`, `config.disableServices` | `[]` | `GOPHERSTACK_SERVICES`, `GOPHERSTACK_DISABLE_SERVICES` |
| `config.engines` | `stub` | `GOPHERSTACK_ENGINES` (`stub`, `auto`, `docker`) |
| `config.enforceIam` | `false` | `GOPHERSTACK_ENFORCE_IAM` |
| `config.localstackCompat` | `false` | `GOPHERSTACK_LOCALSTACK_COMPAT` |
| `config.extra` | `{}` | Any other env var, as `KEY: value` |
| `persistence.enabled` | `false` | PVC at `persistence.mountPath` (`/data`); sets `PERSIST=true` and `GOPHERSTACK_DATA_DIR` |
| `persistence.existingClaim` | `""` | Use your own PVC |
| `service.type`, `service.port` | `ClusterIP`, `8000` | Main endpoint; `PORT` follows `service.port` |
| `service.extraPorts` | `[]` | Extra container and Service ports |
| `ingress.*` | disabled | Ingress for the main port |
| `probes.*` | path `/_gopherstack/health` | Liveness, readiness, startup |
| `container.runtime` | `docker` | `kubernetes` runs Lambda and ECS tasks as Pods |
| `container.kubernetes.*` | see `values.yaml` | `namespace`, `fetchImage` (`busybox:1.37.0`), `codePort` (`4599`), `ecs.enabled`, `ecs.exec` |
| `dind.enabled` | `false` | Privileged Docker-in-Docker sidecar |
| `resources`, `nodeSelector`, `tolerations`, `affinity` | empty | Scheduling |

`values.schema.json` validates values at install time.

Without persistence, state is in memory and lost on pod restart.

## Ports for Azure, GCP and OCI emulators

AWS services share the main port. Other clouds listen on dedicated ports (table `fixedServicePorts` in `internal/app/cli.go`). Expose the ones you need:

```yaml
service:
  extraPorts:
    - name: azure-blob    # AZURE_BLOB_PORT, default 10000
      port: 10000
    - name: cosmos        # COSMOSDB_PORT, default 8081
      port: 8081
```

Port names are at most 15 characters. Defaults: Azure Blob 10000, Queue 10001, Table 10002, Service Bus 10003, Cosmos 8081, GCP Pub/Sub 8085, GCP Storage 4443, IoT MQTT 1883. If you change a port via `config.extra`, change the matching `extraPorts` entry.

## Docker-backed engines

Engines that start containers (RDS, EKS, Lambda containers, ECS tasks, EC2 docker provider) need a Docker daemon, which a pod does not have. The chart defaults to `config.engines=stub`, so those services answer from metadata only. It does not mount the node's Docker socket.

To use them, enable the sidecar:

```yaml
dind:
  enabled: true
config:
  engines: docker
```

> [!WARNING]
> The sidecar is a `docker:27-dind` container running **privileged**, exposed to the pod as `DOCKER_HOST=tcp://127.0.0.1:2375` (no TLS). The cluster must allow privileged pods. The chart's install test does not cover it.

## Kubernetes Pod executor

`container.runtime=kubernetes` (sets `CONTAINER_RUNTIME=kubernetes`) runs Lambda function containers as Pods in the release namespace (or `container.kubernetes.namespace`). No privileged sidecar is needed. The chart adds:

- A namespace-scoped `Role` and `RoleBinding`: `pods` create/get/list/watch/delete, `pods/log` get. With `container.kubernetes.ecs.enabled`: `secrets` create/patch/delete. With `ecs.enabled` and `ecs.exec`: `pods/exec` create. No cluster-wide rules.
- The ServiceAccount token mount (off otherwise).
- `POD_IP`, `LAMBDA_DOCKER_HOST` and `CONTAINER_K8S_ADVERTISE_HOST`, all set to the Gopherstack Pod IP.

How it works:

- Function Pods get no ServiceAccount token. They reach the Runtime API at `<Gopherstack pod IP>:<per-function port>` (ports from the dynamic range, `PORT_RANGE_START`). A NetworkPolicy must let function Pods reach the Gopherstack Pod on those ports and on `container.kubernetes.codePort`.
- No bind mounts. An init container (`fetchImage`, needs `sh`, `wget`, `unzip`) downloads the function zip and layers from Gopherstack on the code port into read-only `emptyDir` volumes at `/var/task` and `/opt`.
- Each Pod gets a random bearer token, valid only for that Pod's archives and revoked once the Pod runs or is removed. Archives with absolute or `..` entries are rejected.
- Memory limit and request equal the function memory size. `OCIRuntime` maps to `runtimeClassName`.
- Pods are labelled `gopherstack.managed=true`. The startup reconcile removes dead leftovers.

> [!IMPORTANT]
> Archive transfer is plain HTTP inside the cluster and the token is visible to anyone who can read Pods in the namespace. Restrict `get pods` there.

Not supported by the Pod executor: engines other than Lambda and ECS (they need Docker), EFS file systems, Lambda hot reload, extensions in layers, `LAMBDA_DOCKER_FLAGS`, host DNS/CA injection, Lambda `Exec`, and stopping or restarting a container (a Pod is created or deleted).

### ECS tasks as Pods

With `CONTAINER_RUNTIME=kubernetes` and ECS containers enabled (`config.engines=docker` or `GOPHERSTACK_ECS_RUNTIME=docker`), each ECS task is one Pod named `ecs-<task id>`. Its containers share the Pod network namespace, as under `awsvpc`.

| ECS | Pod |
|---|---|
| `entryPoint`, `command`, `environment`, `workingDirectory`, `user` (numeric or `root`), `readonlyRootFilesystem`, `linuxParameters.capabilities` | `command`, `args`, `env`, `workingDir`, `runAsUser`/`runAsGroup`, `readOnlyRootFilesystem`, `capabilities` |
| `secrets`, `environmentFiles` | Resolved server-side into a `Secret` named after the Pod, referenced by `secretKeyRef`. The Pod owns the Secret, so it is deleted with the task |
| `memory`, `memoryReservation` | Memory limit, request |
| `cpu` (1024 = 1 vCPU) | CPU request. Task `cpu` and `memory` cap every container |
| `healthCheck` (`CMD`, `CMD-SHELL`) | Exec `readinessProbe`. Health is UNKNOWN until the first success |
| `stopTimeout` | `terminationGracePeriodSeconds` (largest value, default 30) |
| `essential` | Task stops when an essential container exits (`EssentialContainerExited`); a non-essential exit leaves it running |
| `dependsOn` `SUCCESS` | Target runs as an init container. It must be non-essential; a non-zero exit fails the task |
| `dependsOn` `START` | Accepted, but a Pod starts containers together: no ordering guarantee |
| `volumesFrom`, `mountPoints`, `linuxParameters.tmpfs`, `sharedMemorySize` | Source container's mounts; `emptyDir` volumes (memory-backed for tmpfs and `/dev/shm`) |
| Task volumes without a host path; task-scoped `dockerVolumeConfiguration` | `emptyDir`, gone with the Pod |
| `portMappings` | `containerPort` entries. Reach it at `<Pod IP>:<containerPort>`; `hostPort` is ignored. The Pod IP becomes the task's `privateIPv4Address`, used for ELBv2 and Cloud Map registration |
| `logConfiguration` `awslogs` | Pod log streams forwarded to CloudWatch Logs |
| ECS Exec | `pods/exec` with a TTY via the SSM session data channel (needs `container.kubernetes.ecs.exec`) |

Task state follows the Pod: RUNNING once the Pod runs; STOPPED with `stopCode`, `stoppedReason` and exit codes from container statuses. A Pod deleted outside Gopherstack stops the task (exit 137). Image pull failures report `CannotPullContainerError`.

Refused with `TaskFailedToStart` and a `stoppedReason` instead of being approximated:

- host-path volumes, EFS, `configuredAtLaunch`, shared `dockerVolumeConfiguration`
- `networkMode` `host`/`none`, `pidMode` `host`, `ipcMode` `host`
- `privileged`, `disableNetworking`, `devices`, container `restartPolicy`, `repositoryCredentials`, FireLens
- `dependsOn` `COMPLETE` and `HEALTHY`

`ulimits` and `ephemeralStorage` are ignored. Security-group enforcement, task adoption after a Gopherstack restart and task networking policies are not implemented for Pods.

### Running outside the cluster

Set `CONTAINER_RUNTIME=kubernetes`, `CONTAINER_K8S_KUBECONFIG=<path>`, and `CONTAINER_K8S_ADVERTISE_HOST` plus `LAMBDA_DOCKER_HOST` to an address Pods can reach the host on.

## Security defaults

- Drops all capabilities, forbids privilege escalation, `RuntimeDefault` seccomp, read-only root filesystem (`/tmp` and `/data` are writable volumes).
- Runs as UID/GID 65532, `runAsNonRoot: true`; `fsGroup` 65532 makes the PVC writable.
- No ServiceAccount token and no RBAC unless `container.runtime=kubernetes`.

## Tests

```bash
task helm-test           # lint, golden templates, schema rejection (needs docker)
task helm-install-test   # install into a k3s cluster started with pkgs/k3sengine (needs docker)
```

The install test uses a locally built image (`bin/gopherstack-linux`, or `GOPHERSTACK_HELM_IMAGE`), installs with persistence on a NodePort, checks `/_gopherstack/health`, creates and lists an S3 bucket, restarts the pod to prove persistence, then uninstalls. Refresh goldens with `GOPHERSTACK_UPDATE_GOLDEN=1`.
