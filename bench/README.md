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
