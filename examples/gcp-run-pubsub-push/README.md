# gcp-run-pubsub-push

A Cloud Run service receives Pub/Sub push messages.

## What it shows

1. `demo.sh` builds a small Go HTTP service (`service/`) into a `FROM scratch` image.
2. `driver deploy` creates it as a Cloud Run service over gRPC (port `8126`). Gopherstack starts the image in Docker and fronts it with an HTTP ingress on port `8127`, host `push-receiver-<hash>-us-central1.localhost:8127`.
3. The driver creates a Pub/Sub topic and a push subscription pointing at the service's `/push` URL (Pub/Sub gRPC, port `8085`).
4. `driver publish 3` publishes three messages. Gopherstack POSTs them to the service, which logs each one.
5. The script checks `GET /messages` and `docker logs`, then deletes the service, subscription and topic.

Cloud Run and Pub/Sub are gRPC-only, so the driver uses `cloud.google.com/go/run` and `cloud.google.com/go/pubsub/v2`. No credentials are used.

## Prerequisites

- Docker (the image build and the Cloud Run runtime), Go, `curl`.
- Gopherstack running on the host with access to the Docker socket. There is no `docker-compose.yml`, so the published container ports stay reachable from the host.

## Run

```bash
go run ../..     # in another terminal
./demo.sh
```

Override `RUN_ADDR` (`localhost:8126`), `PUBSUB_ADDR` (`localhost:8085`) and `RUN_INGRESS_ADDR` (`localhost:8127`) if you moved the ports.

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

Failures print `FAIL:` and exit non-zero. Re-running is safe: it recreates the service, topic and subscription.

> [!NOTE]
> `*.localhost` names resolve to loopback on most systems. If yours does not, add the printed host to `/etc/hosts`.

## Clean up

`demo.sh` removes the service, subscription and topic on exit. Stop the Gopherstack process yourself.
