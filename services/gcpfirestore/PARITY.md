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
  RunQuery: {wire: ok, errors: ok, state: ok, persist: n/a, note: "from (collection or all_descendants), field/unary/composite (AND/OR) filters with ==,!=,<,<=,>,>=,array-contains,array-contains-any,in,not-in,is-null/nan variants, orderBy, start_at/end_at cursors (incl. __name__), offset, limit, select; implicit ordering from inequality fields and __name__; documents missing an ordered field are excluded. find_nearest over vector fields (EUCLIDEAN, COSINE, DOT_PRODUCT; limit 1 to 1000, query vectors up to 2048 dimensions, distance_threshold, distance_result_field; documents whose field is not a vector of the same dimension are skipped; vectors order between arrays and maps). explain_options: plan only (no documents) or analyze (documents then execution stats); the plan names the automatic index (query_scope, properties) built from the ordering fields, and read_operations and debug_stats count the documents the query returned or skipped."}
  RunAggregationQuery: {wire: ok, errors: ok, state: ok, persist: n/a, note: "count (with up_to), sum (integer overflow falls back to double) and avg (null on no input) over the filtered, offset and limited result set; default aliases field_N."}
  PartitionQuery: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Collection-group queries only; returns up to partition_count-1 name cursors evenly spread over the matching documents, paged."}
  ListCollectionIds: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Collection ids directly under the database root or a document, including those reachable only through sub-collections; paged."}
  Listen: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Document and query targets: ADD, initial DocumentChange set, CURRENT, then consistent global NO_CHANGE with resume token (read time in nanoseconds) and read_time; later commits send DocumentChange, DocumentRemove (left the query) and DocumentDelete. A target added with a resume_token or read_time inside the one hour history window replays only the documents changed since then (DocumentChange, DocumentDelete or DocumentRemove) followed by an ExistenceFilter whose count is the number of documents matching now; an unusable point (malformed, future or older than the history) is answered with RESET plus a full snapshot."}
  Write: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Bidirectional stream: a handshake request (database only) returns stream_id and a stream_token; each later request with writes is one atomic commit (the Commit path, at most 500 writes) answered with write_results, commit_time and the next token; a request with stream_id and the last acknowledged stream_token resumes and replays the unacknowledged responses (the last 64 per stream, at most 1000 streams). Stream state is in memory only."}
  FirestoreAdmin.databases: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreateDatabase (location_id required, ALREADY_EXISTS), GetDatabase, ListDatabases, UpdateDatabase (delete_protection_state, point_in_time_recovery_enablement, concurrency_mode) and DeleteDatabase (delete protection and etag honoured; removes the database's documents, indexes and field overrides). Operations finish at once and are readable through google.longrunning.Operations. The (default) database is created implicitly on first use; documents of any database name are served without registration."}
  FirestoreAdmin.indexes_fields: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreateIndex (query_scope required, at least two fields unless a vector field, duplicate definitions ALREADY_EXISTS, created READY), GetIndex, ListIndexes (paged, no filter), DeleteIndex, GetField (default fields report uses_ancestor_config; the * field lists the automatic indexes), UpdateField (index_config, ttl_config) and ListFields (only the two filters the API supports)."}
  FirestoreAdmin.export_import: {wire: ok, errors: ok, state: ok, persist: ok, note: "ExportDocuments validates a gs:// prefix, snapshots the matching collection groups and returns the .overall_export_metadata path; ImportDocuments restores such a snapshot (collection_ids filter) and is NOT_FOUND for a prefix the emulator did not export; BulkDeleteDocuments deletes the matching collection groups. Snapshots are kept in memory (16 at most) and in the persistence snapshot; no Cloud Storage objects are written."}
  Operations: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.longrunning.Operations on the Firestore listener: Get, Wait, List (name is the database, paged, no filter), Delete and Cancel (every operation has already finished) for the operations Admin calls return (1000 retained)."}
families:
  value_types: {status: ok, note: "null, bool, integer, double, timestamp (truncated to microseconds), string, bytes, reference, geo point, array, map with the Firestore cross-type ordering (NaN lowest number, int/double compared exactly)."}
  history: {status: ok, note: "Each document keeps one hour of versions so read_time and read-only transactions see a consistent past snapshot; history is not persisted, only current documents."}
  routing_isolation: {status: ok, note: "Own gRPC server bound in StartWorker to --gcp-firestore-port / GCP_FIRESTORE_PORT (default 8080, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool. The official client honours FIRESTORE_EMULATOR_HOST."}
  auth: {status: partial, note: "No request is authenticated or authorised and security rules are not evaluated."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; RPC failures are logged at debug. gRPC calls are not fed to the shared telemetry operation metrics."}
  audit_logs: {status: ok, note: "Commit, BatchWrite, CreateDocument, UpdateDocument and DeleteDocument are Data Access write records (google.firestore.v1.Firestore.*, resource the first written document or the database) written through pkgs/gcpaudit when a sink is set; reads and admin operations are not recorded."}
items_still_open:
  - "ExecutePipeline returns UNIMPLEMENTED: no evaluator exists for the google.firestore.v1 Pipeline stage language; the official Go client uses Commit, BatchWrite and RunQuery instead."
  - "Firestore Admin backups, backup schedules, user creds, clone and restore are not served, and ttl_config is stored but expired documents are not deleted."
  - "Queries never fail for a missing composite index and field-level index exemptions are not enforced (the vendor returns FAILED_PRECONDITION without a matching index); export files are not written as Cloud Storage objects."
leaks: {status: clean, note: "Background resources are the grpc.Server and per-stream Listen goroutines, which end with their stream; Shutdown stops the server and joins the serve loop."}
---

Initial implementation (2026-10-18): google.firestore.v1 Firestore gRPC verified against cloud.google.com/go/firestore v1.26.0 through the official client in sdk_test.go.

## Emulator choices where the vendor is silent

- ExistenceFilter is sent only on a resumed target; its count is the number of documents matching the target now.
- The explain plan summary reports the automatic index implied by the query's ordering; the emulator keeps no index to count entries against.
- Write stream tokens are decimal sequence numbers, and operations returned by Admin calls are finished when returned.

## External effects that are recorded, not performed

- ExportDocuments logs the Cloud Storage objects it would write and keeps the snapshot in memory for ImportDocuments.
