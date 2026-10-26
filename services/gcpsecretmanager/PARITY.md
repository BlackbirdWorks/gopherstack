---
service: gcpsecretmanager
sdk_module: cloud.google.com/go/secretmanager@v1.22.0
last_audit_commit: 8d07d8213
last_audit_date: 2026-10-18
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateSecret: {wire: ok, errors: ok, state: ok, persist: ok, note: "Secret id [a-zA-Z0-9_-]{1,255}; replication required (automatic or user_managed with unique locations, optional CMEK). Validates labels/annotations limits, topics, rotation (needs topics, next_rotation_time and a period of at least 1h), version_destroy_ttl (at least 24h), version_aliases. ttl is converted to a stored expire_time."}
  GetSecret: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NOT_FOUND for unknown or expired secrets."}
  UpdateSecret: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask required; labels, annotations, topics, expire_time, ttl, rotation(.next_rotation_time/.rotation_period), version_aliases and version_destroy_ttl are mutable, anything else is INVALID_ARGUMENT. A stale etag is FAILED_PRECONDITION; every update issues a new etag."}
  ListSecrets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, page_size (default 25, max 25000) and page_token via pkgs/page, total_size. filter supports name/labels.K/annotations.K/topics.name/create_time/expire_time with = != : < <= > >=, AND/OR/NOT/-, parentheses and bare terms."}
  DeleteSecret: {wire: ok, errors: ok, state: ok, persist: ok, note: "Etag must match when supplied; removes all versions and the IAM policy."}
  AddSecretVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Payload max 64 KiB; data_crc32c (Castagnoli) is verified and a mismatch is INVALID_ARGUMENT; client_specified_payload_checksum and replication_status are populated. Version numbers are never reused."}
  GetSecretVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Accepts a number, latest (newest version in any state) or a version alias. Metadata only."}
  ListSecretVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Newest first, paged; filter on name, state, create_time, destroy_time."}
  AccessSecretVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Returns the resolved version name, payload and data_crc32c; FAILED_PRECONDITION unless the version is ENABLED."}
  EnableSecretVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "DISABLED to ENABLED, cancelling a scheduled destroy; DESTROYED is FAILED_PRECONDITION; etag checked."}
  DisableSecretVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "ENABLED to DISABLED; DESTROYED is FAILED_PRECONDITION; etag checked."}
  DestroySecretVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Immediate destroy clears the payload and sets destroy_time. With version_destroy_ttl the version becomes DISABLED with scheduled_destroy_time and is destroyed lazily or by the janitor once due."}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stores the policy per secret with a fresh etag; a non-matching etag is ABORTED; bindings need a role and members. Not enforced."}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Empty policy with the default etag when none is set."}
  TestIamPermissions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Every requested permission is reported granted; the emulator performs no authorization."}
  EnableManagedRotation: {wire: ok, errors: ok, state: ok, persist: ok, note: "Needs secret_type CLOUD_SQL_DB_CREDENTIALS and cloud_sql_single_user_credentials (instance_id, username); a second enable is FAILED_PRECONDITION. Adds a version holding the supplied or a generated password and sets rotation.managed_rotation_status ACTIVE."}
  RotateSecret: {wire: ok, errors: ok, state: ok, persist: ok, note: "FAILED_PRECONDITION unless managed rotation is ACTIVE; adds a version with a freshly generated password."}
  RegionalSecrets: {wire: ok, errors: ok, state: ok, persist: ok, note: "projects/*/locations/*/secrets/* names on every secret and version RPC; replication must be unset on regional secrets (INVALID_ARGUMENT), global ones still require it. Version replication_status is omitted. On a secretmanager.<loc>.rep.googleapis.com authority, resources outside <loc> are INVALID_ARGUMENT."}
families:
  rotation: {status: ok, note: "FireRotations (30s scheduler; injectable clock) publishes SECRET_ROTATE to each topic of every secret whose next_rotation_time passed, with attributes eventType, secretId (full name), dataFormat JSON_API_V1, timestamp and the secret JSON as data, then advances next_rotation_time by whole rotation_period steps past now. Wired to the gcppubsub backend in-process. rotation_period 1h-100y and next_rotation_time 5min-100y ahead are enforced on write (docs.cloud.google.com/secret-manager/docs/secret-rotation)."}
  expiry: {status: ok, note: "Secrets past expire_time are deleted lazily at the start of every RPC and by a 30s janitor; scheduled version destruction is finalised the same way."}
  routing_isolation: {status: ok, note: "Own gRPC server bound in StartWorker to --gcp-secretmanager-port / GCP_SECRETMANAGER_PORT (default 8086, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool. The official client has no emulator variable: use option.WithEndpoint with insecure credentials."}
  auth: {status: partial, note: "No request is authenticated or authorised; IAM policies are stored but not enforced."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; RPC failures are logged at debug. gRPC calls are not fed to the shared telemetry operation metrics."}
items_still_open:
  - "SECRET_ROTATE delivery is a single best-effort publish per due time (no 7-day retry), failures are dropped; a one-shot rotation clears next_rotation_time (inferred, not documented); TOPIC_CONFIGURED and other event types are not sent."
  - "Managed rotation generates and stores the password version only; no Cloud SQL instance password is changed."
  - "Regional secrets share the one listener; the secretmanager.<region>.rep.googleapis.com authority is honoured only for the location check, and location names are not validated against a region list."
  - "Customer-managed encryption keys are stored and echoed in replication_status but never used or validated against Cloud KMS."
  - "Secret tags and secret_type are stored but not validated."
leaks: {status: clean, note: "Background resources are the grpc.Server and the expiry janitor goroutine, both stopped and joined by Shutdown."}
---

Initial implementation (2026-10-18): SecretManagerService gRPC verified against cloud.google.com/go/secretmanager v1.22.0 through the official client in sdk_test.go.
