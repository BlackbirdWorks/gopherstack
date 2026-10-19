---
service: gcpcloudtasks
sdk_module: cloud.google.com/go/cloudtasks@v1.20.0
last_audit_commit: 1c5323168
last_audit_date: 2026-10-18
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "Queue ID [a-zA-Z0-9-]{1,100}; defaults filled (500/s, 1000 concurrent, 100 attempts, 0.1s-3600s backoff, 16 doublings; zero means unset); rate and retry validated; output-only max_burst_size derived (clamped 1-100, emulator-defined). A name deleted within 7 days is FAILED_PRECONDITION."}
  GetQueue: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListQueues: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged via pkgs/page. filter supports only AND-joined state=X, name=X and name:substring terms; anything else is INVALID_ARGUMENT."}
  UpdateQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "Upsert (creates a missing queue, as Cloud Tasks does). update_mask paths: rate_limits(.max_dispatches_per_second|.max_concurrent_dispatches), retry_config(.*), http_target, app_engine_routing_override, stackdriver_logging_config(.sampling_ratio); empty mask replaces all mutable fields; other paths (state, name, max_burst_size) are INVALID_ARGUMENT. Limits are re-read on every dispatch pass."}
  DeleteQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "Removes tasks and IAM policy; name reserved 7 days."}
  PurgeQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "Deletes all tasks (names reserved 1h) and sets purge_time."}
  PauseQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "Paused queues accept tasks but dispatch none."}
  ResumeQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: n/a}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stored per queue; stale etag is ABORTED. Not enforced."}
  TestIamPermissions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Every requested permission is reported granted."}
  CreateTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "Auto numeric names or explicit IDs (dedup -> ALREADY_EXISTS, also for names deleted, run or purged within 1h); schedule_time (past clamps to now, >30d ahead is INVALID_ARGUMENT); dispatch_deadline 15s-30m (default 10m); task-level retry_config overrides the queue's; HttpRequest and AppEngineHttpRequest validated (absolute http(s) URL, no body on GET/HEAD/OPTIONS, 1 MiB cap, OAuth/OIDC need a service account)."}
  GetTask: {wire: ok, errors: ok, state: ok, persist: n/a, note: "BASIC (default) omits request bodies, FULL includes them."}
  ListTasks: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Ordered by schedule_time then name, paged."}
  DeleteTask: {wire: ok, errors: ok, state: ok, persist: ok}
  RunTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "Starts an attempt now regardless of queue state, schedule or rate limits; returns immediately with dispatch_count bumped, the attempt completes asynchronously."}
  BatchCreateTasks: {wire: ok, errors: ok, state: ok, persist: ok, note: "Up to 100 CreateTaskRequests sharing the batch parent. Returns a done operation (BatchCreateTasksResponse.tasks = created tasks; metadata.failed_requests maps request index to a google.rpc.Status such as ALREADY_EXISTS). request_id makes retries return the same operation without re-creating."}
  BatchDeleteTasks: {wire: ok, errors: ok, state: ok, persist: ok, note: "Up to 1000 task names; Empty response, per-index NOT_FOUND / INVALID_ARGUMENT in metadata.failed_requests; deleted names reserved 1h. request_id is idempotent."}
  GetCmekConfig: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Unconfigured locations report an empty kms_key."}
  UpdateCmekConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "kms_key format validated and stored; nothing is encrypted."}
families:
  operations: {status: ok, note: "google.longrunning.Operations (Get, List, Wait, Cancel, Delete) is served on the same gRPC listener by pkgs/gcplro. Operations are recorded already done, so WaitOperation returns at once and CancelOperation is a no-op success; ListOperations takes the queue name as parent and rejects a filter. Operations persist in the snapshot."}
  queue_state_disabled: {status: ok, note: "Queue.State DISABLED only arises when an App Engine queue.yaml/queue.xml upload omits the queue and cannot be set directly (cloudtaskspb Queue.State docs), so no RPC can produce it here by design; ResumeQueue refuses it if it ever appears."}
  dispatch: {status: ok, note: "A ticker (100ms) picks due tasks of RUNNING queues; a per-queue token bucket (max_dispatches_per_second, burst = max_burst_size) and max_concurrent_dispatches gate starts. HttpRequest is really sent with pkgs/httpclient (method, headers, body, dispatch_deadline timeout, redirects not followed); 2xx deletes the task, anything else is a failure. X-CloudTasks-* headers are set."}
  retries: {status: ok, note: "Failure waits min_backoff, doubling max_doublings times, then growing linearly, capped at max_backoff; stops at max_attempts (-1 unlimited) or max_retry_duration and then drops the task. dispatch_count, response_count, first_attempt and last_attempt (with a google.rpc.Status mapped from the HTTP status) are recorded. 429/503 get no special treatment."}
  auth_tokens: {status: partial, note: "OIDC tokens are UNSIGNED JWTs (alg none, claim gopherstack=unsigned-test-token) and OAuth tokens are opaque placeholders; no verifier accepts them as real credentials."}
  queue_http_target: {status: partial, note: "Queue http_target uri_override (scheme, host, port, path, query), header_overrides, http_method and queue-level OAuth/OIDC are applied; uri_override_enforce_mode is ignored (always always-enforce)."}
  app_engine: {status: partial, note: "AppEngineHttpRequest is sent to --gcp-cloudtasks-appengine-url / GCP_CLOUDTASKS_APPENGINE_URL plus relative_uri with X-AppEngine-* headers; app_engine_routing is not used for routing. With no base URL set the attempt fails with FAILED_PRECONDITION 'not dispatchable' and follows the normal retry policy."}
  routing_isolation: {status: ok, note: "Own gRPC server bound in StartWorker to --gcp-cloudtasks-port / GCP_CLOUDTASKS_PORT (default 8123, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool. The official client has no emulator variable: use option.WithEndpoint with insecure credentials."}
  auth: {status: partial, note: "No request is authenticated or authorised; IAM policies are stored but not enforced."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; dropped tasks are logged at warn. gRPC calls are not fed to the shared telemetry operation metrics."}
items_still_open:
  - "ListQueues filter supports only state=, name= and name: terms."
  - "Queue max_burst_size is an emulator-chosen derivation; real Cloud Tasks burst sizing is not documented."
  - "Persisted in-flight attempts are not resumed mid-flight: after a restore the task is simply due again and its attempt count keeps the interrupted attempt."
leaks: {status: clean, note: "Background resources are the grpc.Server, the dispatcher ticker goroutine and in-flight delivery goroutines; Shutdown cancels the run context and joins all of them (goleak in TestMain)."}
---

Initial implementation (2026-10-18): google.cloud.tasks.v2 CloudTasks gRPC verified against cloud.google.com/go/cloudtasks v1.20.0 through the official client in the package tests; dispatch and retry timing covered with a fake clock and a testing/synctest run of the real ticker loop.
