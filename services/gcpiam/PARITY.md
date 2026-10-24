---
service: gcpiam
sdk_module: cloud.google.com/go/iam@v1.12.0
last_audit_commit: 880da1baa
last_audit_date: 2026-10-19
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateServiceAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "account_id 6-30 chars [a-z]([-a-z0-9]*[a-z0-9]); email <id>@<project>.iam.gserviceaccount.com; random 21-digit unique_id (also oauth2_client_id); display_name <=100 and description <=256 bytes; duplicate active email is ALREADY_EXISTS."}
  GetServiceAccount: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Name takes email or unique_id; project - is a wildcard; deleted accounts are NOT_FOUND."}
  ListServiceAccounts: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged via pkgs/page (default 20, max 100)."}
  UpdateServiceAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces display_name and description; the deprecated etag is neither returned nor enforced."}
  PatchServiceAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask paths display_name and description (optionally service_account.-prefixed); empty mask updates both; others INVALID_ARGUMENT."}
  DeleteServiceAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "Soft delete: keys are dropped, account and policy kept for 30 days, then purged lazily."}
  UndeleteServiceAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "By unique_id; NOT_FOUND after 30 days, FAILED_PRECONDITION if not deleted, ALREADY_EXISTS if the email was re-created. Keys are not restored."}
  EnableServiceAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableServiceAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "Disabled accounts refuse SignBlob and SignJwt with FAILED_PRECONDITION."}
  CreateServiceAccountKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "Real 2048-bit RSA key; KEY_ALG_RSA_1024 returns INVALID_ARGUMENT (never generate <2048-bit keys). TYPE_GOOGLE_CREDENTIALS_FILE (default) returns a Google JSON key file with a PKCS#8 private_key; TYPE_PKCS12_FILE returns a PKCS#12 bundle, password notasecret. Only the public key and a self-signed X.509 certificate are retained. Ten user-managed keys per account, then FAILED_PRECONDITION."}
  GetServiceAccountKey: {wire: ok, errors: ok, state: ok, persist: n/a, note: "public_key_type TYPE_X509_PEM_FILE returns the certificate, TYPE_RAW_PUBLIC_KEY a PKIX PEM public key; TYPE_NONE omits it. Works for system-managed keys too."}
  ListServiceAccountKeys: {wire: ok, errors: ok, state: ok, persist: n/a, note: "key_types filter; duplicates are INVALID_ARGUMENT; the system-managed signing key is created on first need."}
  UploadServiceAccountKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "PEM X.509 certificate with an RSA key (>=1024 bits); validity taken from the certificate; origin USER_PROVIDED."}
  DeleteServiceAccountKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "System-managed keys are FAILED_PRECONDITION."}
  DisableServiceAccountKey: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableServiceAccountKey: {wire: ok, errors: ok, state: ok, persist: ok}
  SignBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "RSASSA-PKCS1-v1_5 over SHA-256 with the account's system-managed key; verifiable with the key returned by GetServiceAccountKey. Deprecated upstream in favour of IAM Credentials."}
  SignJwt: {wire: ok, errors: ok, state: ok, persist: ok, note: "RS256 JWT with header {alg, typ, kid}; exp defaults to now+1h and must be within [now, now+12h]. Claims are re-serialised with sorted keys. Deprecated upstream."}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Per service account; empty policy carries the initial etag."}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Etag optimistic concurrency: a stale non-empty etag is ABORTED. Member syntax and role format validated; custom role references must exist and not be deleted. update_mask is ignored (whole policy replaced). Policies are stored, never enforced."}
  TestIamPermissions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Every requested permission is reported granted: the emulator authenticates no caller."}
  CreateRole: {wire: ok, errors: ok, state: ok, persist: ok, note: "Project and organization parents; role_id 3-64 of [A-Za-z0-9_.]; permissions deduplicated, sorted and format-checked (service.resource.verb) but not checked against a permission universe; etag per change; reusing a deleted id is ALREADY_EXISTS until purged (37 days)."}
  GetRole: {wire: ok, errors: ok, state: ok, persist: n/a, note: "roles/* from the built-in catalog (always FULL), custom roles including deleted ones (deleted=true)."}
  ListRoles: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Empty parent lists predefined roles; view BASIC omits permissions; show_deleted for custom roles; paged (default 300, max 1000)."}
  UpdateRole: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask: title, description, included_permissions, stage; empty mask replaces all four. Stale etag is ABORTED; deleted role is FAILED_PRECONDITION."}
  DeleteRole: {wire: ok, errors: ok, state: ok, persist: ok, note: "Soft delete (deleted=true, new etag); already deleted is FAILED_PRECONDITION; etag checked."}
  UndeleteRole: {wire: ok, errors: ok, state: ok, persist: ok, note: "Within 7 days of deletion, else FAILED_PRECONDITION; etag checked."}
  QueryGrantableRoles: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Predefined catalog plus custom roles of the project/organization in the resource name; serviceAccounts resources get only roles/iam.serviceAccount* and the basic roles."}
  QueryTestablePermissions: {wire: partial, errors: ok, state: ok, persist: n/a, note: "Permissions of the emulator's catalog (serviceAccounts resources only iam.serviceAccounts.*); name, stage GA and custom_roles_support_level SUPPORTED only."}
  QueryAuditableServices: {wire: ok, errors: ok, state: partial, persist: n/a, note: "A fixed list of services this emulator hosts, regardless of the resource."}
  LintPolicy: {wire: ok, errors: ok, state: partial, persist: n/a, note: "Condition objects only: empty expression, unbalanced brackets or quotes, >2048 characters and >12 logical operators produce ERROR results. Validation unit names ConditionSyntaxCheck and ConditionComplexityCheck are emulator-chosen; it is not a CEL parser."}
families:
  predefined_roles: {status: partial, note: "A catalog of ~45 common roles (iam, storage, pubsub, secretmanager, cloudkms, cloudtasks, cloudscheduler, logging, basic). Permission lists are representative, not Google's full lists; roles/owner, editor and viewer are derived from the catalog. Unknown predefined roles are NOT_FOUND on GetRole but accepted in policy bindings."}
  key_rotation: {status: gap, note: "The system-managed key is created once and never rotated or expired."}
  routing_isolation: {status: ok, note: "Own gRPC server bound in StartWorker to --gcp-iam-port / GCP_IAM_PORT (default 8125, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool. The official client has no emulator variable: use option.WithEndpoint with insecure credentials."}
  persistence: {status: ok, note: "Accounts, user and system keys (system private key included so signatures stay verifiable), policies, custom roles and the etag counter are in backendSnapshot. User-managed private keys are never stored."}
  auth: {status: partial, note: "No request is authenticated or authorised; policies are stored but not enforced; no ID token or access token is minted."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; key material is never logged. gRPC calls are not fed to the shared telemetry operation metrics."}
items_still_open:
  - "Predefined role permission lists are representative, not Google's complete lists."
  - "System-managed signing keys are not rotated."
  - "SetIamPolicy ignores update_mask; TestIamPermissions grants everything requested."
  - "Condition lint is syntactic only (no CEL parsing, no policy-level lint)."
  - "Deleted-service-account and deleted-role retention windows (30 and 7/37 days) are the documented figures, not verified against the live service."
leaks: {status: clean, note: "The only background resource is the grpc.Server and its Serve goroutine; Shutdown stops and joins them (goleak in TestMain)."}
---

# gcpiam parity notes

Dated history lives in git and bd; the front matter is authoritative.
