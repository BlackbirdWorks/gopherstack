# DAX Data-Plane Emulation

This document describes gopherstack's emulation of the Amazon DAX **data plane** —
the binary wire protocol that the real DAX SDK (`amazon-dax-go` /
`amazon-dax-client`) uses for item operations. It is the implementation behind
`services/dax/dataplane`.

## Why a separate listener exists

DAX item operations (GetItem, PutItem, Query, ...) do **not** travel over HTTP.
The control plane (`CreateCluster`, `DescribeClusters`, ... under the
`AmazonDAXV3.*` HTTP target) is the only part the SDK sends to an HTTP endpoint.
For item traffic the SDK opens a raw TCP (optionally TLS) socket to the cluster
endpoint and exchanges CBOR-encoded, method-ID-framed messages. As a result, the
real DAX SDK never reaches gopherstack's HTTP handler for data-plane calls, which
is why this was the last open `parity.md` item.

DAX is a write-through cache in front of DynamoDB. For emulation, the cache is a
pass-through: the listener decodes a request, delegates to the existing
gopherstack DynamoDB backend (`services/dynamodb.InMemoryDB`), and encodes the
DynamoDB result back into the DAX wire format. Each DAX cluster owns its own
in-memory DynamoDB store (`Handler.DataPlaneBackend()`); there is no caching
layer, which is correct for emulation since reads and writes are always
consistent.

## Protocol, as reverse-engineered from `amazon-dax-go`

### Transport framing

CBOR (RFC 7049) is the entire wire encoding. Our codec
(`dataplane/cbor.go`) is a self-contained re-implementation of
`github.com/aws/aws-dax-go/dax/internal/cbor`, so the runtime has no third-party
dependency. (`amazon-dax-go` is a **test-only** dependency, used to drive the
integration tests.)

### Connection handshake

On connect the client writes, in order:

1. the magic CBOR string `"J7yne5G"`
2. a CBOR int `0` (layering)
3. a CBOR string session id
4. a CBOR map of header key/values (e.g. `{"UserAgent": "..."}`)
5. a CBOR int `0` (client mode)

The server consumes this preamble and then serves request frames.

### Request framing

Each call is `[serviceId:int=1, methodId:int, ...args]`. The client pipelines an
`authorizeConnection` call (method `1489122155`) ahead of the real method on the
same flush; the server consumes and accepts the auth args (any credentials are
accepted) and then reads the following real method frame. A single response
covers both.

Method IDs implemented:

| Method                | ID            | Purpose                              |
| --------------------- | ------------- | ------------------------------------ |
| authorizeConnection   | `1489122155`  | SigV4 auth (accepted, not verified)  |
| endpoints             | `455855874`   | Cluster discovery / health check     |
| defineKeySchema       | `-742646399`  | Table key schema lookup              |
| defineAttributeListId | `-1230579644` | Register an ordered attr-name list   |
| defineAttributeList   | `670678385`   | Resolve an attr-list id to names     |
| GetItem               | `263244906`   | Implemented (delegates to DynamoDB)  |
| PutItem               | `-2106490455` | Implemented                          |
| DeleteItem            | `1013539361`  | Implemented                          |
| BatchGetItem          | `-697851100`  | Implemented                          |
| BatchWriteItem        | `116217951`   | Implemented                          |
| UpdateItem            | `1425579023`  | Implemented (expr blob + UPDATED_*)  |
| Query                 | `-931250863`  | Implemented (key/filter/projection)  |
| Scan                  | `-1875390620` | Implemented (filter/projection)      |
| TransactWriteItems    | `-1160037738` | Implemented (multi-section framing)  |
| TransactGetItems      | `1866287579`  | Implemented (multi-section framing)  |

### Response framing

Every response begins with a CBOR **error array**: empty (`[]`) means success,
non-empty carries `[codes...]`, message, and a `[requestId, errorCode,
statusCode]` info array. On success the operation-specific body follows.

### Item encoding specifics

- **Keys** are encoded as an opaque CBOR byte string. For a single hash key the
  bytes are the raw value (S/B) or a CBOR number (N). For a compound key the
  hash is CBOR-encoded and the range component is appended raw (S/B) or as a
  lexicographic decimal (N). `dataplane/itemkey.go` reverses this using the
  table's key schema (fetched via the DynamoDB backend's `DescribeTable`); the
  numeric range-key lexdecimal codec lives in `dataplane/lexdecimal.go`.
- **Expressions** (UpdateExpression / ConditionExpression /
  KeyConditionExpression / FilterExpression) arrive as a pre-parsed CBOR
  s-expression blob produced by the DAX client's expression parser, not as
  strings. `dataplane/expression.go` decodes that blob back into a DynamoDB
  expression string plus `ExpressionAttribute{Names,Values}`, which is then
  handed to the backend's normal expression-parsing path.
- **Non-key attributes** are sent as a byte-wrapped `[attrListId, values...]`
  payload, where `attrListId` references a previously registered, sorted list of
  attribute names. The server maintains the id<->names mapping
  (`dataplane/control.go`).
- **AttributeValues** map directly to AWS SDK v2 `types.AttributeValue`
  (`dataplane/attrval.go`): strings, numbers (int / big.Int / CBOR tag-4
  decimal), binary, bool, null, string/number/binary sets (CBOR tags
  3321/3322/3323), lists, and maps.
- **Projected attributes** (a `ProjectionExpression` on Query/Scan/GetItem, or
  `ReturnValues=UPDATED_*` on UpdateItem) use a distinct response sub-protocol
  (`dataplane/projection.go`). The server decodes the projection's pre-parsed
  blob into the same ordered document paths the client builds via
  `buildProjectionOrdinals`, resolves each path against the backend's returned
  item, and emits the ordinal-keyed shape the client expects: a bare
  `{ordinal: value}` map per item for Query/Scan/GetItem (`decodeProjection`), or
  a byte-wrapped `[attrListId, {ordinal: value}]` payload for UpdateItem
  UPDATED_* (`decodeAttributeProjection`). Paths that do not resolve in the item
  are omitted, matching the real service.
- **Transactions** (`dataplane/transact.go`) arrive not as a per-item frame but
  as parallel arrays — operations / tableNames / keys / values /
  conditionExpressions / updateExpressions for writes; tableNames / keys /
  projectionExpressions for reads — exactly as `encodeTransactWriteItemsInput` /
  `encodeTransactGetItemsInput` emit them. The server rebuilds the SDK
  `TransactItems` (Put / Update / Delete / ConditionCheck for writes; Get for
  reads) and delegates to the backend's transaction path, so atomicity, the
  duplicate-item check, and condition-check rollback come from the real
  DynamoDB emulation. Responses are the multi-section
  `[returnValues, consumedCapacity, itemCollectionMetrics]` (write) and
  `[responses, consumedCapacity]` (read) shapes the client decodes.

## What works end to end

Verified with the **real `amazon-dax-go` client** in
`services/dax/dataplane_integration_test.go`:

- the full handshake + auth + `defineKeySchema` + `defineAttributeListId` flow,
- **PutItem -> GetItem** round-trip (string, number, bool attributes),
- **GetItem** of a missing key (returns an empty item),
- **DeleteItem** followed by a GetItem that no longer finds the item,
- **UpdateItem** with a `SET` UpdateExpression and expression attribute
  values/names, including `ReturnValues=ALL_NEW`,
- **Query** with a `KeyConditionExpression` over a numeric range key, with and
  without an additional `FilterExpression`,
- **Query / Scan** with a `ProjectionExpression` (including an
  `ExpressionAttributeNames` substitution), returning only the projected
  attributes via the projection response sub-protocol,
- **UpdateItem** with `ReturnValues=UPDATED_NEW` and `UPDATED_OLD` (in addition
  to `ALL_NEW`/`ALL_OLD`/`NONE`), returning the touched attributes,
- **Scan** with a `FilterExpression`,
- **TransactWriteItems** (Put + Update) followed by **TransactGetItems**, a
  TransactGetItems with a per-item `ProjectionExpression`, and a
  TransactWriteItems whose `ConditionCheck` fails (verifying the whole
  transaction is rolled back),
- a **numeric (N) range-key** round-trip across a hash+range table (negative,
  zero, integer and fractional sort keys), exercising the lexdecimal codec.

`BatchGetItem` and `BatchWriteItem` are implemented and decode/delegate using the
same key + attribute machinery. Protocol-level encode/decode is unit-tested in
`dataplane/codec_test.go`; the expression-blob decoder, the projection ordinal
decoder/resolver, the transact response writers, and the lexdecimal range-key
codec have dedicated unit tests in `dataplane/expression_test.go`,
`dataplane/transact_test.go` and `dataplane/lexdecimal_test.go`.

## Known gaps (honest status)

The full item read/write path — GetItem, PutItem, DeleteItem, BatchGetItem,
BatchWriteItem, **UpdateItem (including `UPDATED_OLD`/`UPDATED_NEW`), Query and
Scan (including `ProjectionExpression`)** — and the **TransactWriteItems /
TransactGetItems** transaction operations now work end to end against the real
client, including the pre-parsed expression sub-protocol, the ordinal-keyed
projection response sub-protocol, and numeric range keys. There are no remaining
data-plane operations served as `UnknownOperation`.

The remaining gaps are intentional emulator simplifications, not protocol
omissions:

1. **TLS / SigV4 verification.** The listener speaks plain TCP and accepts any
   credentials, which is appropriate for a local emulator. `daxs://` (encrypted)
   endpoints are not served.
2. **ConsumedCapacity / ItemCollectionMetrics.** Responses report empty/zero
   values rather than computed capacity. (For transactions the client decodes
   these as empty arrays / an empty map, which is accepted.)

### Test-harness note

The integration tests drive the real `amazon-dax-go` client, whose client-side
expression parser uses the old ANTLR Go runtime. That runtime mutates shared,
process-global ATN/DFA caches on every parse and is not goroutine-safe. The
expression-bearing integration tests therefore run sequentially (no
`t.Parallel()`); `paralleltest` is excluded for
`dax/dataplane_integration_test.go` in `.golangci.yml` for that reason. This is a
limitation of the third-party, test-only client, not of the gopherstack server,
whose listener is concurrency-safe (verified under `go test -race`).

## Lifecycle wiring

`services/dax/dataplane_server.go` attaches the listener to the DAX `Handler`.
The provider enables it on `:8111` (the port `DescribeClusters` reports), and the
listener starts via the `service.BackgroundWorker` `StartWorker` hook and stops
via the `service.Shutdowner` `Shutdown` hook. Tests bind `127.0.0.1:0` for an
ephemeral port.
