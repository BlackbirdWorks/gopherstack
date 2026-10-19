---
service: gcpfirestore
sdk_module: cloud.google.com/go/firestore@v1.26.0
last_audit_commit: 4566bb099
last_audit_date: 2026-10-18
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetDocument: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NOT_FOUND when absent; mask, transaction and read_time selectors."}
  ListDocuments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name or order_by, page_size (default 100, max 300) and page_token, mask, show_missing (name-only entries for documents that only have sub-collections; rejected with order_by), transaction and read_time."}
  CreateDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "Generates a 20-character id when document_id is empty; ALREADY_EXISTS on a duplicate."}
  UpdateDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "Without update_mask the document is replaced; with it only the masked paths change (absent paths are deleted). Upserts unless exists:true. exists and update_time preconditions."}
  DeleteDocument: {wire: ok, errors: ok, state: ok, persist: ok, note: "Idempotent for absent documents; preconditions honoured."}
  BatchGetDocuments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Streams found/missing entries in request order at one snapshot; transaction, new_transaction (id returned on the first response) and read_time selectors."}
  BatchWrite: {wire: ok, errors: ok, state: ok, persist: ok, note: "Each write is applied independently with its own status; more than one write to a document is INVALID_ARGUMENT; at most 500 writes."}
  BeginTransaction: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Read-write (retry_transaction supersedes the old one) and read-only (optionally at read_time). Transactions expire after 270s and are not persisted."}
  Commit: {wire: ok, errors: ok, state: ok, persist: ok, note: "Atomic multi-write with update/delete/legacy transform ops, update_mask, field transforms and preconditions; transform results returned. Read-write transactions are optimistic: ABORTED if any document or query scope they read changed since BeginTransaction."}
  Rollback: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Unknown or already-finished transaction ids are INVALID_ARGUMENT."}
  RunQuery: {wire: ok, errors: ok, state: ok, persist: n/a, note: "from (collection or all_descendants), field/unary/composite (AND/OR) filters with ==,!=,<,<=,>,>=,array-contains,array-contains-any,in,not-in,is-null/nan variants, orderBy, start_at/end_at cursors (incl. __name__), offset, limit, select; implicit ordering from inequality fields and __name__; documents missing an ordered field are excluded."}
  RunAggregationQuery: {wire: ok, errors: ok, state: ok, persist: n/a, note: "count (with up_to), sum (integer overflow falls back to double) and avg (null on no input) over the filtered, offset and limited result set; default aliases field_N."}
  PartitionQuery: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Collection-group queries only; returns up to partition_count-1 name cursors evenly spread over the matching documents, paged."}
  ListCollectionIds: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Collection ids directly under the database root or a document, including those reachable only through sub-collections; paged."}
  Listen: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Document and query targets: ADD, initial DocumentChange set, CURRENT, then consistent global NO_CHANGE with resume token and read_time; later commits send DocumentChange, DocumentRemove (left the query) and DocumentDelete. A target added with a resume token is answered with RESET plus a full snapshot."}
families:
  value_types: {status: ok, note: "null, bool, integer, double, timestamp (truncated to microseconds), string, bytes, reference, geo point, array, map with the Firestore cross-type ordering (NaN lowest number, int/double compared exactly)."}
  history: {status: ok, note: "Each document keeps one hour of versions so read_time and read-only transactions see a consistent past snapshot; history is not persisted, only current documents."}
  routing_isolation: {status: ok, note: "Own gRPC server bound in StartWorker to --gcp-firestore-port / GCP_FIRESTORE_PORT (default 8080, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool. The official client honours FIRESTORE_EMULATOR_HOST."}
  auth: {status: partial, note: "No request is authenticated or authorised and security rules are not evaluated."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; RPC failures are logged at debug. gRPC calls are not fed to the shared telemetry operation metrics."}
items_still_open:
  - "The bidirectional Write stream and ExecutePipeline return UNIMPLEMENTED; the official Go client uses Commit and BatchWrite instead."
  - "Only the (default)-style database named in the resource path is addressed implicitly; database creation and admin APIs (indexes, exports) are not served."
  - "Vector fields, find_nearest, explain options and field-level indexes are not supported; all queries run without index requirements."
  - "Listen sends no ExistenceFilter and does not replay missed changes on resume; a resumed target is reset and re-sent in full."
leaks: {status: clean, note: "Background resources are the grpc.Server and per-stream Listen goroutines, which end with their stream; Shutdown stops the server and joins the serve loop."}
---

Initial implementation (2026-10-18): google.firestore.v1 Firestore gRPC verified against cloud.google.com/go/firestore v1.26.0 through the official client in sdk_test.go.
