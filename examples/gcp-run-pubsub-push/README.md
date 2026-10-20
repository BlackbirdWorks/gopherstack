# gcp-run-pubsub-push

A Cloud Run service receiving Pub/Sub **push** deliveries:

1. `demo.sh` builds a tiny Go HTTP service (`service/`, a static binary in a `FROM scratch` image, no registry access needed).
2. `driver deploy` creates it as a Cloud Run service through the Cloud Run gRPC API (port `8126`). Gopherstack starts the image in Docker and fronts it with its HTTP ingress (port `8127`, hosts like `push-receiver-<hash>-us-central1.localhost:8127`).
3. The driver creates a Pub/Sub topic and a **push subscription** whose endpoint is the service's `/push` URL (Pub/Sub gRPC, port `8085`).
4. `driver publish 3` publishes three messages. Gopherstack POSTs them to the service, which logs each one.
5. The script checks the service's `GET /messages` and the container's `docker logs`, then deletes the service, subscription and topic.

Cloud Run and Pub/Sub are gRPC-only in gopherstack, so the driver uses the official `cloud.google.com/go/run` and `cloud.google.com/go/pubsub/v2` clients (own `go.mod`, same versions as the root module). No credentials are used.

## Prerequisites

- **Docker** (to build the image and for gopherstack's Cloud Run runtime).
- A Go toolchain, and `curl`.
- Gopherstack running **on the host** with access to the Docker socket, so the Cloud Run containers' published ports are reachable: from the repo root, `go run .`. There is deliberately no `docker-compose.yml` here.

## Running

```bash
go run ../..   # in another terminal
./demo.sh
```

Override addresses with `RUN_ADDR`, `PUBSUB_ADDR` and `RUN_INGRESS_ADDR` if you moved the ports.

## Expected output

```
deployed projects/demo-project/locations/us-central1/services/push-receiver (revision ...)
SERVICE_HOST=push-receiver-<hash>-us-central1.localhost:8127
push subscription orders-push -> http://push-receiver-<hash>-us-central1.localhost:8127/push
published order-1 id=...
published order-2 id=...
published order-3 id=...
service /messages: [{"id":"...","data":"order-1",...}, ...]
... received pubsub message id=... data="order-1" subscription=projects/demo-project/subscriptions/orders-push
... (order-2, order-3)
PASS: gcp-run-pubsub-push (3 Pub/Sub messages pushed to the Cloud Run service and logged)
```

The script exits non-zero with a `FAIL:` line otherwise. Re-running is safe: it recreates the service, topic and subscription.

## Notes

`*.localhost` names resolve to loopback on most systems (glibc/systemd-resolved, macOS). If yours does not, add the printed host to `/etc/hosts`.
