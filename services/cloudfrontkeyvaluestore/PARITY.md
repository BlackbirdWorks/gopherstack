---
service: cloudfrontkeyvaluestore
sdk_module: aws-sdk-go-v2/service/cloudfrontkeyvaluestore@v1.15.4
last_audit_commit: 1e78b7ca4
last_audit_date: 2026-08-13
overall: B
ops:
  DescribeKeyValueStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "ItemCount/TotalSizeInBytes computed from real per-store data; see gaps for the byte-accounting approximation"}
  GetKey: {wire: ok, errors: ok, state: ok, persist: ok}
  PutKey: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteKey: {wire: ok, errors: ok, state: ok, persist: ok}
  ListKeys: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults/NextToken pagination via pkgs/page"}
  UpdateKeys: {wire: ok, errors: ok, state: ok, persist: ok, note: "all-or-nothing per the real API is NOT modeled -- see gaps"}
gaps:
  - "TotalSizeInBytes is len(key)+len(value) summed per item. AWS's real byte accounting includes undocumented per-item overhead this emulator cannot replicate exactly; the number is real and deterministic (derived from actual stored data, not fabricated) but will not byte-for-byte match a real account. (bd: gopherstack-4ara)"
  - "UpdateKeys is not transactional: puts and deletes apply sequentially against the shared InMemoryBackend lock rather than as a single all-or-nothing batch. A backend error partway through (never currently possible, since PutKVSValue/DeleteKVSValue on an already-validated store/ETag cannot fail mid-batch) would leave a partial result. (bd: gopherstack-4ara)"
  - "No per-store size quota (AWS documents ~5MB/store, 1KB/value limits) and no AccessDeniedException path (no IAM enforcement in this emulator) -- see errors.go's doc comment. ServiceQuotaExceededException/AccessDeniedException are therefore never returned, though both are in the real client's exception set for several ops. (bd: gopherstack-4ara)"
structural_gaps:
  - "None. Every op here reads or mutates real per-KVS-store key/value state (services/cloudfront's keyValueStoreData/keyValueDataETags) -- there is no billing/ML/hardware dependency that would make any of these ops structurally unimplementable."
deferred: []
leaks: {status: clean, note: "Handler owns no goroutines, janitors, or independent maps -- see Handler's doc comment and persistence_test.go's TestHandler_OwnsNoState guard."}
---

## Notes

**Why this package exists** (gopherstack-4ara): AWS splits CloudFront's
KeyValueStore surface across two SDK clients/protocols. `cloudfront.Client`
(services/cloudfront) owns the *control plane*
(Create/Get/List/Delete/UpdateKeyValueStore, path
`/2020-05-31/key-value-store/...`, REST-XML). `cloudfrontkeyvaluestore.Client`
(this package) owns the *data plane* -- the actual key/value pairs inside a
store -- at an entirely different, unversioned path family
(`/key-value-stores/{KvsARN}/...`, REST-JSON, `ServiceID = "CloudFront
KeyValueStore"`, its own `AWS_ENDPOINT_URL_CLOUDFRONT_KEYVALUESTORE` env var).
A prior gopherstack pass implemented GetKey/PutKey/DeleteKey/ListKeys/
UpdateKeys as handlers *inside* services/cloudfront, routed under the
REST-XML `/2020-05-31/` prefix -- reachable by nothing, since no real
`cloudfrontkeyvaluestore` client request ever carries that prefix. The
underlying backend state (services/cloudfront's `keyValueStoreData`/
`keyValueDataETags` maps and their `GetKVSValue`/`PutKVSValue`/
`DeleteKVSValue`/`ListKVSValues`/`UpdateKVSValues` methods) was real, not
fabricated -- only the HTTP-layer routing and wire shape were wrong. This
package replaces the dead handlers with correct routing and wire shape, wired
(cli.go's `wireCloudFrontKeyValueStore`) directly to that same
`*cloudfront.InMemoryBackend`, mirroring how services/dynamodbstreams borrows
services/dynamodb's backend rather than owning a duplicate store.

**Wire shape, verified against cloudfrontkeyvaluestore@v1.15.4
serializers.go/deserializers.go directly** (not assumed from the prior dead
code, which got several of these wrong):

- JSON field names are **PascalCase** (`Key`, `Value`, `ItemCount`,
  `TotalSizeInBytes`, `KvsARN`, `Status`, `Created`, `LastModified`), not
  lowerCamelCase like most other restJson1 services in this repo.
- `KvsARN` and `Key` are URI path segments, percent-encoded by the real
  client (the ARN contains `:` and `/`). Decoding must happen per-segment,
  not on the whole decoded path, or the ARN's embedded `/` fragments the
  route -- same "ARN-in-path route-matching trap" as services/grafana and
  services/s3tables; `rawPathSegments` in handler.go is the same fix.
- `ETag` is **never** a JSON body field. It is an `ETag` response header on
  PutKey/DeleteKey/UpdateKeys/DescribeKeyValueStore outputs, and does not
  exist at all on GetKey/ListKeys outputs (verified: no
  `awsRestjson1_deserializeOpHttpBindings{GetKey,ListKeys}Output` function
  exists in the SDK).
- `DescribeKeyValueStoreOutput.ETag` is the **data-plane** ETag (the same
  value `ListKVSValues` returns), not the KeyValueStore resource's own
  control-plane ETag from services/cloudfront -- PutKeyInput's IfMatch doc
  comment says so explicitly ("which you can get using
  DescribeKeyValueStore"). Getting this wrong breaks the real
  Describe-then-Put/Delete/UpdateKeys workflow every real client uses, since
  IfMatch is a *required* field on Put/Delete/UpdateKeys.
- `Created`/`LastModified` are **epoch-seconds** JSON numbers
  (`smithytime.ParseEpochSeconds`), unlike services/cloudfront's own
  REST-XML API, which uses RFC3339 strings for the same underlying
  `KeyValueStore.CreatedTime`/`LastModifiedTime` fields -- `epochSeconds()`
  converts.
- `UpdateKeysInput.Deletes` is `[]{"Key": "..."}` objects, not a bare string
  array, despite carrying only a key.
- Error body is `{"message": "..."}` plus an `X-Amzn-Errortype` header
  naming the exception (`AccessDeniedException`, `ConflictException`,
  `InternalServerException`, `ResourceNotFoundException`,
  `ServiceQuotaExceededException`, `ValidationException` -- verified against
  each op's own `awsRestjson1_deserializeOpError<Op>` switch in
  deserializers.go, not assumed). **ETag mismatches map to `ConflictException`
  (409)**, not the HTTP 412 the removed services/cloudfront handlers used to
  send -- 412 does not appear anywhere in this SDK's error model.

**Testing this SDK client requires overriding `EndpointResolverV2`, not just
`BaseEndpoint`**: this service's endpoint ruleset derives a per-account-ID
virtual host from the `KvsARN` input, which `BaseEndpoint` alone does not
suppress -- see handler_test.go's `staticEndpointResolver`. Skipping this
makes every SDK-driven test fail with a DNS lookup on
`<accountID>.<host>`, not a gopherstack bug.

**services/cloudfront changes made alongside this package** (same commit):
added `KeyValueStore.CreatedTime` (needed for DescribeKeyValueStore's
required `Created` field, previously untracked) and persisted
`keyValueStoreData`/`keyValueDataETags` in `backendSnapshot`
(`cloudfrontSnapshotVersion` bumped 1 -> 2) -- the KVS data-plane key/value
pairs were silently dropped across a restart before this. Removed the dead
`/2020-05-31/key-value-store/{id}/keys/...` handlers, op constants, and
routing from services/cloudfront (kept the backend methods, now called by
this package instead).
