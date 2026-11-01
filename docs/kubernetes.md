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

This adds a `docker:dind` container that runs **privileged** and exposes `tcp://127.0.0.1:2375` (no TLS, pod-local) as `DOCKER_HOST`. It needs a cluster that permits privileged pods and is not covered by the install test. A native Kubernetes pod executor for Lambda/ECS/EC2 is not implemented.

## Security

The container drops all capabilities, forbids privilege escalation, uses the `RuntimeDefault` seccomp profile and a read-only root filesystem (`/tmp` and `/data` are writable volumes). It runs as UID 0 by default: the release image is `FROM scratch`, and containerd unpacks such an image's root directory as `0700 root`, so a non-root UID cannot exec the binary or resolve any path (seen on k3s; `stat /root/gopherstack: permission denied`). With an image that has a world-searchable root, set `podSecurityContext.runAsNonRoot=true` and `podSecurityContext.runAsUser`; `fsGroup` (65532) makes the PVC writable either way. It mounts no service account token and the chart creates no RBAC, because Gopherstack does not call the Kubernetes API.

## Tests

```bash
task helm-test        # lint + golden templates (docker, pinned alpine/helm image)
task helm-install-test  # installs into a real k3s cluster started with pkgs/k3sengine
```

Both run `go test ./test/helm/` with `GOPHERSTACK_TEST_DOCKER=1`. The install test loads a locally built image (`bin/gopherstack-linux` via `Dockerfile.test`, or `GOPHERSTACK_HELM_IMAGE`), installs the chart with persistence on a NodePort, checks `/_gopherstack/health`, creates and lists an S3 bucket, restarts the pod to prove persistence, then uninstalls. Refresh goldens with `GOPHERSTACK_UPDATE_GOLDEN=1`.
