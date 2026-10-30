---
service: gcpapigateway
sdk_module: cloud.google.com/go/apigateway@v1.17.0
last_audit_commit: 8b70851a3
last_audit_date: 2026-10-30
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateApi: {wire: ok, errors: partial, state: ok, persist: ok, note: "Global location only; api_id is required. managed_service is generated as {api}-{hash}.apigateway.{project}.cloud.goog unless supplied. The returned operation runs on the backend clock (default 2s) and the API reads CREATING until it ends. The id charset, the AlreadyExists text and the 'locations/global only' rejection are not verified against production."}
  GetApi: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListApis: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged through pkgs/page; order_by name, create_time or update_time (optionally desc); filter is rejected."}
  UpdateApi: {wire: ok, errors: partial, state: ok, persist: ok, note: "update_mask display_name, labels or *; managed_service is immutable. An empty mask updates display_name and labels."}
  DeleteApi: {wire: ok, errors: partial, state: ok, persist: ok, note: "FAILED_PRECONDITION while the API has configs (code and text unverified). The API reads DELETING until the operation ends."}
  CreateApiConfig: {wire: ok, errors: partial, state: ok, persist: ok, note: "Exactly one openapi_documents entry (YAML or JSON bytes); grpc_services and managed_service_configs are rejected. An invalid document leaves the config FAILED and finishes the operation with INVALID_ARGUMENT 'Cannot convert to service config. ...' plus OperationMetadata.diagnostics. Only one config per API may be CREATING at a time (FAILED_PRECONDITION). gateway_service_account must be an IAM service account email or a projects/ name. service_config_id is {config}-{hash}."}
  GetApiConfig: {wire: ok, errors: ok, state: ok, persist: n/a, note: "view BASIC (default) omits openapi_documents; FULL returns them."}
  ListApiConfigs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged; BASIC view."}
  UpdateApiConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "display_name and labels only; the document and service account are immutable."}
  DeleteApiConfig: {wire: ok, errors: partial, state: ok, persist: ok, note: "FAILED_PRECONDITION while a gateway serves the config (code unverified)."}
  CreateGateway: {wire: ok, errors: partial, state: ok, persist: ok, note: "Regional: us-central1, us-east1, us-east4, us-west2-4, europe-west1-2, asia-northeast1, australia-southeast1 (the quickstart list). api_config must exist and not be FAILED or DELETING. default_hostname is {gateway}-{18 chars}.{region code}.gateway.dev; the hash and region codes other than uc are derived, not observed. effective_streaming_mode mirrors streaming_mode."}
  GetGateway: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListGateways: {wire: ok, errors: ok, state: ok, persist: n/a, note: "locations/- spans regions."}
  UpdateGateway: {wire: ok, errors: partial, state: ok, persist: ok, note: "display_name, labels, api_config; streaming_mode is immutable. The gateway reads UPDATING while the operation runs and keeps serving."}
  DeleteGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETING gateways stop serving."}
  GetLocation: {wire: ok, errors: ok, state: ok, persist: n/a, note: "global plus the gateway regions."}
  ListLocations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged; filter is rejected."}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "APIs, API configs and gateways (the REST API has these verbs; the Go client has no method for them, so they are exercised over gRPC IAMPolicy and REST)."}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces the policy; a stale etag is ABORTED."}
  TestIamPermissions: {wire: ok, errors: ok, state: partial, persist: n/a, note: "Reports every requested apigateway.* permission as granted: no caller is authenticated."}
  GetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.longrunning.Operations on projects/*/locations/*/operations/*. Operations finish on the backend clock; metadata carries target, verb, api_version v1 and diagnostics."}
  ListOperations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged; filter is rejected."}
  WaitOperation: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Waits up to min(remaining time, timeout) on a real timer."}
  CancelOperation: {wire: ok, errors: ok, state: partial, persist: ok, note: "A running create is rolled back, a running delete is undone, a running update keeps its change; the operation ends with CANCELLED. Cancelling a finished operation succeeds without effect."}
  DeleteOperation: {wire: ok, errors: partial, state: ok, persist: ok, note: "A running operation is FAILED_PRECONDITION (unverified)."}
families:
  openapi_validation: {status: partial, note: "Swagger 2.0 and OpenAPI 3.0/3.1 documents (the overview page lists all three). Checked: version, paths, responses, a backend for every operation (top-level or per operation x-google-backend with an http(s) address; that every operation needs one is the emulator's rule), path_translation enum, deadline 0-3600 s, protocol http/1.1 or h2, jwt_audience xor disable_auth, security references, apiKey name and in, JWT definitions (x-google-issuer unique, x-google-jwks_uri http(s)), x-google-management metrics (INT64, DELTA, displayName <= 40) and quota limits (unit 1/min/{project}, STANDARD integer), x-google-quota metricCosts, ambiguous path templates. Verified against docs.cloud.google.com api-gateway openapi-overview, oasv2-extensions and authenticating-users-jwt."}
  dataplane_routing: {status: ok, note: "Requests whose Host is a gateway's default_hostname, or whose path starts /_apigateway/{project}/{location}/{gateway}/, are served by the gateway on the same listener (default 8146). Paths match the document's templates ({name} fills one segment, literals win) below basePath; no match is 404 {code,message}."}
  path_translation: {status: ok, note: "From oasv2-extensions: top-level default APPEND_PATH_TO_ADDRESS appends the original request path to the address path; per-operation x-google-backend defaults to CONSTANT_ADDRESS, which sends to the address and turns path parameters into query parameters (/hello/{name} -> ?name=world). Request query parameters are kept. deadline defaults to 15 s (504 on expiry)."}
  api_key: {status: partial, note: "api_key definitions are enforced: the key is read from the query parameter named by the definition, key, or the x-api-key header; missing is 401. The emulator has no API Keys service, so any non-empty key is accepted and the key's project is not checked."}
  jwt: {status: partial, note: "x-google-issuer, x-google-jwks_uri and x-google-audiences (default audience https://{managed service}) are enforced: token from the Authorization Bearer header or the access_token query parameter; RS/PS/ES 256-512 only (alg none and HS* refused, key type must fit the algorithm); exp required and checked on the backend clock; JWK sets and kid-to-X.509 maps accepted; keys cached 5 minutes. Failures 401 (Jwt is missing, Jwt is expired, Jwt verification fails, Jwt issuer is not configured) and 403 (Audiences in Jwt are not allowed). The decoded payload reaches the backend base64url-encoded in X-Apigateway-Api-Userinfo (docs); a client-sent copy is dropped. The Authorization header is moved to X-Forwarded-Authorization unless disable_auth. The failure bodies are the Envoy-style texts recalled from ESPv2, not verified."}
  quota: {status: partial, note: "x-google-management metrics and quota limits with x-google-quota metricCosts are enforced per project per minute on the backend clock (all-or-nothing across an operation's metrics); excess is 429 RESOURCE_EXHAUSTED. Operations without x-google-quota are unlimited (docs). The 429 text is recalled, not verified. Counters are in memory and are not persisted."}
  cors: {status: partial, note: "The gateway adds no CORS headers and forwards OPTIONS like any method: an OPTIONS operation must be declared in the document or the call is 404. Recalled from the product behaviour, not verified."}
  backends: {status: ok, note: "HTTP(S) backends through one pooled client: TLS is always verified, redirects are returned, the dialer checks the resolved peer address and refuses anything outside 127.0.0.0/8 and ::1 unless --gcp-apigateway-allow-external-backends is set; the same rule covers remote JWKS fetches. Cloud Run and Cloud Functions serving URLs (*.localhost) are mapped to loopback, so they reach the emulated services without a separate interface. Refused destinations are 502, connect errors 503."}
  routing_isolation: {status: ok, note: "Own gRPC server with the REST transcoder and the gateway route bound in StartWorker to --gcp-apigateway-port / GCP_APIGATEWAY_PORT (default 8146, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool."}
  auth: {status: partial, note: "No control-plane request is authenticated or authorised."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; RPC failures are logged at debug. No dashboard page."}
items_still_open:
  - "The Cloud Run / Functions gateway-to-backend identity token (Authorization: Bearer ID token, jwt_audience) is not minted: the original Authorization header is moved to X-Forwarded-Authorization and the backend gets none."
  - "API keys are not validated against any project or key store (no API Keys emulator); any non-empty key passes."
  - "gRPC API configs (grpc_services, managed_service_configs), more than one OpenAPI document per config, h2 and WebSocket backends, x-google-allow, x-google-endpoints and x-google-jwt-locations are not supported."
  - "OpenAPI 3.x documents are accepted with the 2.0 extension spellings (x-google-issuer and friends, x-google-management) plus x-google-api-management backends with camelCase fields; the exact 3.x extension grammar (x-google-auth, jwtLocations) was not verified and is not enforced."
  - "Error bodies of the data plane (401 Jwt..., 404 Method does not exist., 429 Quota exceeded...) and the unmatched-method status (404, not 405) are recalled from ESPv2, not verified against the product."
  - "Only the most recent config's quota limits should apply API-wide; each gateway here uses its own config's limits."
  - "Control-plane error codes and texts (AlreadyExists, FAILED_PRECONDITION for in-use resources, id charset, the global-only rule for APIs and configs, the single CREATING config rule beyond the quickstart note) were chosen without a production reference."
  - "Gateway default hostname hash and the short region codes other than uc are derived, not observed."
  - "Operation durations are fixed emulator defaults (create 2 s, update and delete 1 s); there is no failure injection beyond document validation."
  - "No dashboard page or proxy."
leaks: {status: clean, note: "The only background resources are the grpc.Server and its serve goroutine, joined by Shutdown, and the pooled backend client, closed there."}
---

Initial implementation (2026-10-30): google.cloud.apigateway.v1 ApiGatewayService, Locations, IAMPolicy and Operations verified against cloud.google.com/go/apigateway v1.17.0 through the official client in the package tests; the data plane is verified against httptest backends and JWKS servers.
