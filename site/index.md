---
layout: home

hero:
  name: Gopherstack
  text: Local AWS, Azure, GCP and OCI emulator
  tagline: One Go binary. In-memory by default. LocalStack-compatible mode.
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
  - title: One process
    details: All services run in one Go binary with in-memory state. No database or JVM.
  - title: Cross-service wiring
    details: DynamoDB Streams to Lambda, SNS to SQS, EventBridge Scheduler and Pipes, Step Functions, CloudFormation.
  - title: LocalStack mode
    details: "`--localstack-compat` also listens on 4566, reads LOCALSTACK_* variables and serves /_localstack/* and /_aws/*."
  - title: Dashboard
    details: Web UI at /dashboard. A desktop build is published as gopherstack-desktop.
  - title: Chaos and IAM
    details: Inject faults through /_gopherstack/chaos. Enable IAM policy evaluation with --enforce-iam.
  - title: Testcontainers
    details: Modules for Go, Python and Node start the published image for tests.
---

<SiteStats />

## Quick start

```bash
docker run --rm -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:8000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:8000 dynamodb list-tables
```

- Any credentials and a region work. The CLI needs them to sign requests.
- Dashboard: `http://localhost:8000/dashboard`.
- Health check: `http://localhost:8000/_gopherstack/health`.

See [Quickstart](/guide/quickstart) for install options and SDK setup.

## Migrating from LocalStack

1. Point clients at `http://localhost:8000`, or start with `--localstack-compat` to also listen on 4566.
2. Use the image `ghcr.io/blackbirdworks/gopherstack:latest`. `SERVICES` is optional and only restricts which services run.
3. Init hooks in `/etc/localstack/init/{boot,start,ready,shutdown}.d` and `/_localstack/chaos/*` are supported.

Details: [Migrating from LocalStack](/guide/migration).

## More

- [Service Index](/services/): every service with its parity grade and audited operations.
- [Examples](/examples/): runnable multi-service setups.
- [Limitations & Parity Gaps](/limitations): platform limits and open gaps per service.
- [Performance](/guide/performance): how to run the benchmarks.
