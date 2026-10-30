# Gopherstack Benchmarks

In-process micro-benchmarks for core Gopherstack services.  All benchmarks
run against in-memory backends **without** any HTTP overhead, giving a clean
measure of pure storage and CPU cost per operation.

## Running the benchmarks

```bash
# Run all benchmarks, one iteration each
go test -bench=. -benchtime=1x -benchmem ./bench/

# Run with more iterations for stable results
go test -bench=. -benchmem ./bench/

# Run a single service
go test -bench=BenchmarkS3 -benchmem ./bench/
go test -bench=BenchmarkDynamoDB -benchmem ./bench/
```

## Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `BenchmarkS3_PutObject` | Write 1 KiB object to an in-memory bucket |
| `BenchmarkS3_GetObject` | Read one of 1 000 pre-loaded objects |
| `BenchmarkS3_ListObjectsV2` | List all 1 000 objects in a bucket |
| `BenchmarkSQS_SendMessage` | Enqueue a single message |
| `BenchmarkSQS_ReceiveMessage` | Receive up to 10 messages from 1 000 queued |
| `BenchmarkDynamoDB_PutItem` | Write one item to a hash-key table |
| `BenchmarkDynamoDB_GetItem` | Point-read one item out of 1 000 |
| `BenchmarkKMS_Encrypt` | Encrypt 32 bytes with an AES key |
| `BenchmarkKMS_Decrypt` | Decrypt a previously encrypted ciphertext |
| `BenchmarkSecretsManager_CreateSecret` | Create a new secret |
| `BenchmarkSecretsManager_GetSecretValue` | Retrieve an existing secret value |

## Example output

```
BenchmarkS3_PutObject-4                  10000           127067 ns/op       834472 B/op    318 allocs/op
BenchmarkS3_GetObject-4                  10000            69730 ns/op        48328 B/op     92 allocs/op
BenchmarkS3_ListObjectsV2-4               1000           836467 ns/op       526176 B/op   7080 allocs/op
BenchmarkSQS_SendMessage-4               50000            46216 ns/op         3992 B/op     61 allocs/op
BenchmarkSQS_ReceiveMessage-4            50000            20698 ns/op         4544 B/op     95 allocs/op
BenchmarkDynamoDB_PutItem-4              50000            40806 ns/op        10856 B/op    157 allocs/op
BenchmarkDynamoDB_GetItem-4             100000            17944 ns/op         3584 B/op     49 allocs/op
BenchmarkKMS_Encrypt-4                  100000            16180 ns/op         3088 B/op     34 allocs/op
BenchmarkKMS_Decrypt-4                  200000             6391 ns/op         2736 B/op     25 allocs/op
BenchmarkSecretsManager_CreateSecret-4   50000            24856 ns/op         4016 B/op     66 allocs/op
BenchmarkSecretsManager_GetSecretValue-4 200000             6131 ns/op         1664 B/op     28 allocs/op
```

## End-to-end benchmarks (full router + middleware)

`internal/app/e2e_bench_test.go`, `e2e_bench_sns_test.go`, `shared_path_server_bench_test.go`
and `bench_ddb_server_path_test.go` drive real `aws-sdk-go-v2` clients (S3, DynamoDB, SNS)
through the production Echo stack (routing, SigV4 scope parsing, telemetry, error mirroring,
cloudtrail capture, compression) over an in-memory `RoundTripper`. `leak_e2e_test.go` holds the
bounded-growth (goroutines/heap/series) leak tests for the same paths.

```bash
go test -run '^$' -bench 'BenchmarkE2E_' -benchmem -cpu=4 ./internal/app/
go test -run '^$' -bench 'BenchmarkServerPath' -benchmem -cpu=4 ./internal/app/   # server only, no SDK client
go test -run '^$' -bench 'BenchmarkE2E_S3/^head$' -cpuprofile cpu.prof -o app.test ./internal/app/
go tool pprof -top -cum -focus='echo/v5.\(\*Echo\).ServeHTTP' app.test cpu.prof   # server side only
```

Baseline (2026-10-30, `-cpu=4 -benchtime=300x`, shared 4-core box under load; ns/op is
indicative, allocs/op is exact):

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| E2E_S3/put_1KiB | 125 670 | 85 652 | 639 |
| E2E_S3/get_1KiB | 103 318 | 42 193 | 574 |
| E2E_S3/head | 90 622 | 41 348 | 531 |
| E2E_S3/list_prefix_delim_10k | 346 097 | 167 969 | 2 646 |
| E2E_S3/list_prefix_page (1000 keys) | 770 920 | 342 518 | 6 110 |
| E2E_S3/copy | 261 515 | 195 063 | 634 |
| E2E_S3/delete_objects_1000 | 3 671 798 | 1 335 628 | 22 649 |
| E2E_S3/multipart_3parts (5 MiB x2 + 1 KiB) | 39 916 568 | 11 834 908 | 3 368 |
| E2E_DynamoDB/putitem | 75 484 | 38 973 | 436 |
| E2E_DynamoDB/getitem | 75 851 | 36 873 | 417 |
| E2E_DynamoDB/updateitem_expr | 83 794 | 48 026 | 476 |
| E2E_DynamoDB/query_filter | 229 046 | 87 845 | 916 |
| E2E_DynamoDB/scan_filter_10k | 1 963 561 | 93 073 | 1 163 |
| E2E_DynamoDB/batchwrite_25 | 189 398 | 137 535 | 1 472 |
| E2E_DynamoDB/batchget_100 | 382 658 | 237 436 | 3 100 |
| E2E_DynamoDB/transactwrite_10 | 216 190 | 110 053 | 1 300 |
| ServerPath/ddb_getitem (server only) | ~24 000 | 9 106 | 96 |
| ServerPath/s3_getobject (server only) | ~35 000-47 000 | 9 180 | 123 |

Profile findings (cpu, alloc_space, alloc_objects, server side of the SDK round trip):

- The SDK client is about 70% of CPU and about 65% of allocations of an e2e small-object call
  (SigV4 payload sha256, checksum middleware, XML/JSON (de)serialisation). Server-side gains
  are therefore diluted roughly 3:1 in the e2e numbers; use `BenchmarkServerPath` to judge them.
- Large S3 payloads are dominated by hashing: server MD5 (ETag, unavoidable) and client sha256.
  `multipart_3parts` is about 18 ms MD5 plus about 16 ms client sha256 for 10 MiB.
- Server middleware (logger, apptrace, telemetry, cloudtrail capture, region/principal,
  chaos, compress) is flat: about 50 allocations per request, no single frame above 1.5% of
  allocations or 3% of CPU.
- `Router.Lookup` is the largest single server frame: about 23% of server CPU for an S3 object
  request, because S3 is the lowest-priority catch-all and about 47 ungated REST/host/SigV4
  matchers run first (DynamoDB is target-gated and runs 1). Each matcher is 100-200 ns;
  there is no cheap hot spot, only gating each matcher with its own prefix gate
  (`internal/routegates/path_gates.txt`), which needs a per-service proof.
- OR-alternative gates (`routegates.AltGates`, `service.AltGate`: target, path prefix/suffix, host,
  user-agent, content-type, query, SigV4 scope) cut S3 object lookups from 38 to 23 matchers;
  path-bucketed prefix lists cut per-candidate gate checks. `BenchmarkRouterLookupS3Object`
  (min over runs): Lookup about 2.3-2.9 us to 1.56 us; `ServerPath/s3_getobject` 24.2 to 21.7 us
  (-10%, median -13%, p=0.017), `s3_putobject` 37.7 to 35.5 us. Still ungated: APIGateway
  (custom-domain state), OpenSearch (endpoint strategy), query-protocol services.
- `scan_filter_10k` is memory-latency bound in `expr.Evaluator` small-map lookups
  (`item[attr]`, then `{"N": ...}` unwrap); no allocation to remove.
- `list_prefix_*`, `batchget_100`, `delete_objects_1000`: server frames are under 10% of CPU;
  the allocations counted belong to the SDK response decoder.
- Allocation-free apigateway custom-domain Host check was tried: -3 allocs/op on the server
  path, not measurable in ns/op (below the 5% bar) and reverted.

## Comparing with LocalStack

To compare Gopherstack latency against [LocalStack](https://localstack.cloud/):

1. Start LocalStack:

```bash
docker run --rm -d -p 4566:4566 localstack/localstack:latest
```

2. Run the Gopherstack server:

```bash
gopherstack --port 8000
```

3. Run the HTTP-level comparison benchmarks (found in individual service packages, e.g. `s3/bench_test.go`) pointing at each endpoint. The in-process numbers above represent the **minimum achievable latency** for Gopherstack — HTTP adds network and serialisation overhead on top.

Key differences vs LocalStack:

| Characteristic | Gopherstack | LocalStack |
|---------------|-------------|------------|
| Architecture | Single Go binary | Python + Docker |
| Cold-start | ~5 ms | 5–30 s |
| Per-request overhead | <1 ms (HTTP), <0.1 ms (in-process) | 5–50 ms |
| Memory footprint | ~30 MB | 300 MB+ |
| Service coverage | Core services | Near-full AWS |
