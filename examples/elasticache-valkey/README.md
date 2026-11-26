# elasticache-valkey

`CreateCacheCluster` starts an in-process Valkey/Redis-compatible server. You find its address from the API and DNS, then talk to it with `valkey-cli`.

## What it shows

1. `aws elasticache create-cache-cluster` for `my-valkey-cluster` (`--engine redis`, `cache.t3.micro`, one node).
2. `describe-cache-clusters --show-cache-node-info` returns the node endpoint (`<id>.<hash>.us-east-1.cache.amazonaws.com`) and port.
3. `dig @127.0.0.1 -p 10053 +short <endpoint>` resolves the name through the embedded DNS server.
4. `PING`, `SET greeting` and `GET greeting` against the resolved address. The script fails if the value read back differs.

The compose file sets `ELASTICACHE_ENGINE=embedded`, `DNS_ADDR=:10053` and `DNS_RESOLVE_IP=127.0.0.1`.

| Port | Use |
|------|-----|
| `8000` | AWS API |
| `10053/udp` | Embedded DNS |
| `10000`-`10100` | Cache node ports (the demo got `10016`) |

## Prerequisites

- Docker, AWS CLI v2, `jq`, `dig`, and `valkey-cli` or `redis-cli`.

> [!NOTE]
> `demo.sh` has `http://localhost:8000` and the DNS address `127.0.0.1:10053` hard-coded. It exits early if neither `valkey-cli` nor `redis-cli` is installed.

## Run

```bash
docker compose up -d --build
docker compose ps        # wait until healthy
./demo.sh
```

## Expected output

The cluster JSON is printed first, then:

```
=== Resolving my-valkey-cluster.<hash>.us-east-1.cache.amazonaws.com via embedded DNS ===
my-valkey-cluster.<hash>.us-east-1.cache.amazonaws.com -> 127.0.0.1

=== Connecting to Valkey at 127.0.0.1:<port> ===
PING:
PONG

SET greeting "Hello from Gopherstack!":
OK

GET greeting:
Hello from Gopherstack!

=== Demo complete ===
```

The script deletes the cluster on exit.

## Clean up

```bash
docker compose down -v
```
