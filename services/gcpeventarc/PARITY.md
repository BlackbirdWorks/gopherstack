---
service: gcpeventarc
sdk_module: cloud.google.com/go/eventarc@v1.26.0
last_audit_commit: b6ec134e2
last_audit_date: 2026-10-25
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateTrigger: {wire: ok, errors: partial, state: ok, persist: ok, note: "Needs a type event filter naming a delivered event type and the filters its provider declares (bucket is required for storage events, subject takes match-path-pattern); destination cloud_run (service, region, path) or http_endpoint; optional service_account, labels, event_data_content_type (application/json only), retry_policy, channel. Creates the Pub/Sub transport (topic unless a user topic is supplied for messagePublished, plus a push subscription) and returns a finished operation. validate_only checks without creating. Trigger id length and charset, and the FAILED_PRECONDITION used for a missing user topic, are not verified against production."}
  GetTrigger: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListTriggers: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged through pkgs/page; order_by name, create_time or update_time (optionally desc); locations/- spans regions. filter is rejected."}
  UpdateTrigger: {wire: ok, errors: partial, state: ok, persist: ok, note: "update_mask paths event_filters, service_account, destination (and sub-paths), labels, retry_policy, event_data_content_type, or * ; an empty mask updates the fields present. The type filter and the transport cannot change. A stale etag is ABORTED (code unverified). allow_missing creates; validate_only validates."}
  DeleteTrigger: {wire: ok, errors: partial, state: ok, persist: ok, note: "Removes the push subscription and a topic Eventarc created; a user-supplied topic is kept. The operation returns the deleted trigger. etag mismatch is ABORTED (unverified); allow_missing and validate_only supported."}
  CreateChannel: {wire: ok, errors: partial, state: ok, persist: ok, note: "Third-party channel in state PENDING with an activation_token and a Pub/Sub topic; provider is a well-formed name and is not checked against the catalog."}
  GetChannel: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListChannels: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged; order_by name, create_time, update_time."}
  UpdateChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "crypto_key_name and labels."}
  DeleteChannel: {wire: ok, errors: partial, state: ok, persist: ok, note: "Deletes the channel's topic and connections; FAILED_PRECONDITION while a trigger uses it (code unverified)."}
  CreateChannelConnection: {wire: ok, errors: partial, state: ok, persist: ok, note: "The activation token must match the PENDING channel; the channel becomes ACTIVE and the token is spent. Token mismatch is INVALID_ARGUMENT (unverified)."}
  GetChannelConnection: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListChannelConnections: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged."}
  DeleteChannelConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "The channel becomes INACTIVE."}
  GetProvider: {wire: ok, errors: ok, state: ok, persist: n/a, note: "pubsub.googleapis.com and storage.googleapis.com only: the sources the emulator delivers."}
  ListProviders: {wire: ok, errors: ok, state: ok, persist: n/a, note: "filter understands name:\"x\" and displayName:\"x\" substrings; order_by name or display_name."}
  GetGoogleChannelConfig: {wire: ok, errors: ok, state: ok, persist: n/a, note: "An empty config with the right name until first updated."}
  UpdateGoogleChannelConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "crypto_key_name and labels; the key is stored, not used."}
  GetLocation: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Fixed catalog of regions plus global."}
  ListLocations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged; filter is rejected."}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Triggers and channels."}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces the policy; a stale etag is ABORTED."}
  TestIamPermissions: {wire: ok, errors: ok, state: partial, persist: n/a, note: "Reports every requested permission as granted: no caller is authenticated."}
  GetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.longrunning.Operations via pkgs/gcplro on projects/*/locations/*/operations/*."}
families:
  delivery_pubsub: {status: ok, note: "google.cloud.pubsub.topic.v1.messagePublished: the transport subscription is a push subscription on the Pub/Sub emulator whose endpoint is this service; each message is forwarded in binary content mode (ce-specversion, ce-id = message id, ce-source //pubsub.googleapis.com/{topic}, ce-type, ce-time, content-type application/json) with the Pub/Sub push envelope {message, subscription} as data. A non-2xx reply makes Pub/Sub redeliver with the subscription's retry policy (10s to 600s) for up to 24 hours of retention."}
  delivery_storage: {status: ok, note: "google.cloud.storage.object.v1 finalized, deleted, archived and metadataUpdated come from gcpstorage's ObserveObjects, are matched against trigger filters and published to the trigger's transport topic, then pushed like Pub/Sub events with ce-source //storage.googleapis.com/projects/_/buckets/{bucket}, ce-subject objects/{name} and the object resource as data."}
  path_patterns: {status: ok, note: "match-path-pattern: * inside a segment, ** across segments, {name} and {name=**} captures; only on attributes whose provider declares it (subject for storage). The pattern grammar is taken from the Eventarc docs, not production."}
  destinations: {status: partial, note: "cloud_run resolves the service against the Cloud Run emulator, then against a Cloud Functions emulator function of the same id (a 2nd gen function is a Cloud Run service), and posts to its serving URL plus path. http_endpoint posts to the URI. A service_account adds an unsigned test identity token (pkgs/gcpdispatch). cloud_function is rejected as read-only (as the API documents), workflow and gke destinations are rejected because those services are not emulated."}
  routing_isolation: {status: ok, note: "Own gRPC server with the REST transcoder and the push route bound in StartWorker to --gcp-eventarc-port / GCP_EVENTARC_PORT (default 8138, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool."}
  auth: {status: partial, note: "No request is authenticated or authorised, and the push route accepts any caller."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; RPC failures and delivery failures are logged at debug. No dashboard page."}
items_still_open:
  - "Cloud Audit Logs events (google.cloud.audit.log.v1.written) are not delivered: the Logging emulator keeps no audit log entries to match. Triggers for them are rejected."
  - "Only Pub/Sub and Cloud Storage events exist; every other Google source (Firebase, Cloud Build, Firestore, Artifact Registry, direct events of other services) and third-party providers are absent from ListProviders, and triggers on a channel are stored but receive no events."
  - "Triggers cannot target Workflows or GKE (not emulated); Cloud Functions are reached through the Cloud Run destination, a trigger cannot set cloud_function (the API forbids it)."
  - "MessageBus, Enrollment, Pipeline, GoogleApiSource, LoggingConfig and Eventarc Advanced are UNIMPLEMENTED."
  - "retry_policy.max_attempts is stored but not enforced, and there is no dead-letter handling; delivery retries until the subscription's 24 hour retention expires."
  - "network_config, event_data_content_type application/protobuf, CMEK (crypto_key_name) and satisfies_pzs are not honoured; trigger conditions are always empty."
  - "A Cloud Run service need not exist when a trigger is created; an unresolved service only fails deliveries (and is retried). Region and location rules (global triggers, storage bucket region match) are not enforced."
  - "Operations are recorded already finished: no CREATING state, and CancelOperation has no effect."
  - "After a restore the transport subscriptions keep the push endpoint of the run that created them; a changed --gcp-eventarc-port needs the triggers recreated."
  - "Trigger and channel id rules, the ABORTED etag code, FAILED_PRECONDITION for a missing user topic and INVALID_ARGUMENT for a bad channel activation token were chosen without a production reference."
leaks: {status: clean, note: "The only background resources are the grpc.Server and its serve goroutine, joined by Shutdown, and the pooled delivery client, closed there."}
---

Initial implementation (2026-10-25): google.cloud.eventarc.v1 Eventarc, Locations, IAMPolicy and Operations verified against cloud.google.com/go/eventarc v1.26.0 through the official client in the package tests; delivery verified with the Pub/Sub and Cloud Storage Go clients against an httptest destination.
