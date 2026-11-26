---
layout: home

hero:
  name: Gopherstack
  text: 190+ AWS services. One Go binary. Milliseconds, not minutes.
  tagline: A lightweight, in-memory local AWS, Azure, and GCP emulator with real cross-service triggers, a built-in dashboard, and native LocalStack compatibility.
  image:
    src: /logo.png
    alt: Gopherstack logo
  actions:
    - theme: brand
      text: Get Started
      link: /guide/quickstart
    - theme: alt
      text: Service Index
      link: /services/
    - theme: alt
      text: Migrate from LocalStack
      link: /guide/migration
    - theme: alt
      text: View on GitHub
      link: https://github.com/BlackbirdWorks/gopherstack

features:
  - icon: ⚡
    title: Single Go Binary, <100 ms Startup
    details: All services run in one ~30 MB process with zero database or JVM overhead — ideal for rapid unit tests, Testcontainers, and parallel CI pipelines.
  - icon: 🔗
    title: Real Cross-Service Integration
    details: DynamoDB Streams → Lambda ESMs, SNS → SQS fan-out, EventBridge Scheduler & Pipes, Step Functions ASL execution, and CloudFormation provisioning.
  - icon: 🔄
    title: Drop-In LocalStack Compatibility
    details: Flip --localstack-compat to serve port 4566, translate LOCALSTACK_* env vars, and expose /_localstack/* and /_aws/* developer endpoints.
  - icon: 🖥️
    title: Built-In Web Dashboard & Desktop App
    details: Inspect S3 buckets, query DynamoDB tables, browse queues, and monitor Prometheus metrics at /dashboard or in the native gopherstack-desktop app.
  - icon: 💥
    title: Chaos Engineering & IAM Enforcement
    details: Inject HTTP faults and network latency dynamically via /_gopherstack/chaos, or enable --enforce-iam to evaluate attached IAM policies on every request.
  - icon: 🧪
    title: Go Testcontainers & In-Memory SDK
    details: Spin up Gopherstack in Go tests via modules/gopherstack or embed service backends directly in Go without running an HTTP server.
---

<SiteStats />

## Quick Start

Run Gopherstack in Docker or install the binary, then point any AWS SDK, CLI, or Terraform/OpenTofu config at `http://localhost:8000`:

```bash
docker run --rm -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest

# Point the AWS CLI at Gopherstack — no credentials required
aws dynamodb list-tables --endpoint-url http://localhost:8000
aws s3 mb s3://my-bucket --endpoint-url http://localhost:8000
```

Open the built-in web dashboard at `http://localhost:8000/dashboard`.

## Performance at a Glance

Gopherstack keeps state in memory with **O(1) ARN indexing**, field-aligned structs, runtime `zstd`/`br`/`gzip` compression, and **Profile-Guided Optimization (`default.pgo`)** built directly into release binaries:

| Metric | LocalStack | Gopherstack |
|---|---|---|
| **Runtime** | Python | Single Go binary |
| **Memory footprint** | 300 MB+ | ~30 MB |
| **Cold start** | 5–30 s | &lt;100 ms |
| **Service loading** | Lazy / per-service | All services ready immediately |

Read the [Performance Guide](/guide/performance) for architecture details and how to run `task bench`.

## Migrating from LocalStack

Switching an existing LocalStack setup takes seconds:

1. **Change the endpoint** from `http://localhost:4566` to `http://localhost:8000` — or pass `--localstack-compat` (`GOPHERSTACK_LOCALSTACK_COMPAT=true`) to listen on `4566` automatically.
2. **Swap the container image** to `ghcr.io/blackbirdworks/gopherstack:latest` and drop `SERVICES=...` (every service is always active).
3. **Keep your init hooks & chaos API** — `/etc/localstack/init/{boot,start,ready,shutdown}.d` and `/_localstack/chaos/*` work out of the box.

See the full [Migrating from LocalStack](/guide/migration) guide for SDK, Docker Compose, and Terraform snippets.

## Explore Further

- **[Service Index](/services/)** — Browse all emulated AWS, Azure, and GCP services with live `PARITY.md` grades and audited operations.
- **[Examples](/examples/)** — End-to-end runnable stacks for API Gateway WebSockets, DynamoDB Streams → Lambda, Kinesis, Step Functions, and Azure.
- **[Limitations & Parity Gaps](/limitations)** — Transparent breakdown of platform boundaries and open `PARITY.md` gaps across services.
