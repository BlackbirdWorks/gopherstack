# Kubernetes (Helm chart)

`charts/gopherstack` deploys Gopherstack into a cluster. It renders a Deployment (one replica, `Recreate`), a Service, a ConfigMap of environment settings, and optionally a PVC, an Ingress and a Docker-in-Docker sidecar.

## Install

From a checkout:

```bash
helm install gopherstack ./charts/gopherstack
kubectl port-forward svc/gopherstack-gopherstack 8000:8000
AWS_ENDPOINT_URL=http://localhost:8000 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws s3 mb s3://demo
```

The image is `ghcr.io/blackbirdworks/gopherstack`, tagged with the chart `appVersion` unless `image.tag` is set. In-cluster clients use `http://<release>-gopherstack.<namespace>.svc.cluster.local:8000`.

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
| `container.runtime` | `docker` | `kubernetes` runs Lambda containers as Pods through the Kubernetes API (below) |
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

- a namespace-scoped `Role` and `RoleBinding`: `pods` create/get/list/watch/delete and `pods/log` get. No `pods/exec`, no secrets, no cluster-wide rules;
- the ServiceAccount token mount (off otherwise);
- `POD_IP`, `LAMBDA_DOCKER_HOST` and `CONTAINER_K8S_ADVERTISE_HOST` set to the Gopherstack Pod IP, plus the `fn-code` container port.

How it works:

- Function Pods get no ServiceAccount token. They reach the Lambda Runtime API at `<gopherstack pod IP>:<per-function port>` (ports come from `PORT_RANGE_START`...; each function runtime listens on its own port), so a NetworkPolicy must allow function Pods to reach the Gopherstack Pod on those ports and on `container.kubernetes.codePort`. A Service port is not used because the ports are dynamic.
- There are no bind mounts. The function zip and layers are held in memory and an initContainer (`container.kubernetes.fetchImage`, needs `sh`, `wget`, `unzip`) downloads them from Gopherstack on the code port into read-only `emptyDir` volumes at `/var/task` and `/opt`. Each Pod gets a random bearer token (in the initContainer env only, never logged) that is valid for that Pod's archives alone and is revoked as soon as the Pod is running or removed. The transfer is plain HTTP inside the cluster; the token is visible to whoever can read Pods in the namespace, so restrict `get pods` there. Zips with absolute or `..` entries are rejected.
- Memory limit and request equal the function memory size. `OCIRuntime` (Lambda isolation) maps to `runtimeClassName`.
- Pods are labelled `gopherstack.managed=true`; the existing startup reconcile lists them and removes dead leftovers of earlier runs.

Not supported by the Pod executor yet: ECS tasks, EC2 and other engines (they still need docker), EFS file systems, Lambda hot reload, extensions in layers, `LAMBDA_DOCKER_FLAGS`, host DNS/CA injection, `Exec`, and container stop/start (a Pod is created or deleted).

Running Gopherstack outside the cluster: set `CONTAINER_RUNTIME=kubernetes`, `CONTAINER_K8S_KUBECONFIG=<path>` and `CONTAINER_K8S_ADVERTISE_HOST=<an address Pods can reach this host on>` (also `LAMBDA_DOCKER_HOST` to the same value).

## Security

The container drops all capabilities, forbids privilege escalation, uses the `RuntimeDefault` seccomp profile and a read-only root filesystem (`/tmp` and `/data` are writable volumes). It runs as the image's non-root user (UID/GID 65532, `runAsNonRoot: true`); `fsGroup` 65532 makes the PVC writable. The release image is distroless `static:nonroot`, so its root directory is world-searchable under containerd as well. To run docker-backed engines from the cluster, enable the `dind` sidecar rather than mounting a host socket. It mounts no service account token and creates no RBAC unless `container.runtime=kubernetes` (see above).

## Tests

```bash
task helm-test        # lint + golden templates (docker, pinned alpine/helm image)
task helm-install-test  # installs into a real k3s cluster started with pkgs/k3sengine
```

Both run `go test ./test/helm/` with `GOPHERSTACK_TEST_DOCKER=1`. The install test loads a locally built image (`bin/gopherstack-linux` via `Dockerfile.test`, or `GOPHERSTACK_HELM_IMAGE`), installs the chart with persistence on a NodePort, checks `/_gopherstack/health`, creates and lists an S3 bucket, restarts the pod to prove persistence, then uninstalls. Refresh goldens with `GOPHERSTACK_UPDATE_GOLDEN=1`.
