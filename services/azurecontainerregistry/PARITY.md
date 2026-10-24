---
service: azurecontainerregistry
sdk_module: azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry@v1.2.0   # go.mod dependency (test-only): drives the ARM plane in internal/app/registry_wiring_test.go
last_audit_commit: 364908f83
last_audit_date: 2026-10-22
overall: B
ops:
  CheckAPIVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /v2/ answers 200 {} with Docker-Distribution-Api-Version, or 401 with a Bearer realm=/oauth2/token challenge when the request carries no valid credential."}
  GetCatalog: {wire: ok, errors: ok, state: ok, persist: ok, note: "Docker Registry v2 /v2/_catalog (n, last, Link rel=next) served by the embedded distribution library, per registry."}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "/v2/{repo}/tags/list with n and last."}
  GetManifest: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET and HEAD by tag or digest; Docker schema 2, manifest lists and OCI manifests and indexes. A manifest or tag with readEnabled false answers 403 DENIED."}
  PutManifest: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT by tag or digest. Fires the registry webhooks (push, or chart_push for a Helm chart config) with the ACR event payload. Records created and updated times for the /acr/v1 API. A tag with writeEnabled false answers 403 DENIED."}
  DeleteManifest: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE by digest (and by tag). Fires the delete or chart_delete webhooks. deleteEnabled false on the repository, manifest or any tag pointing at it answers 403 DENIED."}
  StartBlobUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /v2/{repo}/blobs/uploads/; monolithic, chunked (PATCH with Content-Range) and completing PUT ?digest= all verified against the Go client in the conformance tests."}
  UploadBlobChunk: {wire: ok, errors: ok, state: ok, persist: ok, note: "Each request body is capped at 8 GiB (ociregistry.Limit) and streamed to storage; nothing is sized from Content-Length."}
  CompleteBlobUpload: {wire: ok, errors: ok, state: ok, persist: ok, note: "Digest mismatch is rejected by the library."}
  GetBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET and HEAD /v2/{repo}/blobs/{digest}."}
  DeleteBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "Library delete support is on; blobs are not garbage collected."}
  ExchangeToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "POST /oauth2/exchange (form): grant_type access_token, access_token_refresh_token or refresh_token. Any non-empty Entra ID access token is accepted and a refresh token (HS256 JWT, 3 hours, iss Azure Container Registry) comes back. Tokens are signed with a per-process key, so they stop working after a restart."}
  GetAccessToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "POST /oauth2/token with a refresh token, and GET /oauth2/token with Basic admin credentials or a refresh token as the password (the Docker token endpoint); both return {access_token} (1 hour) carrying the requested scopes."}
  ListRepositories: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /acr/v1/_catalog (n, last, Link). Repositories with listEnabled false are hidden. Only repositories holding a manifest appear."}
  GetRepositoryAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /acr/v1/{repo}: registry, imageName, createdTime, lastUpdateTime, manifestCount, tagCount, changeableAttributes. Repository names may contain slashes."}
  UpdateRepositoryAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH {value:{deleteEnabled,writeEnabled,readEnabled,listEnabled}}."}
  DeleteRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /acr/v1/{repo} deletes every manifest and answers 202 {manifestsDeleted, tagsDeleted}."}
  ListTagAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /acr/v1/{repo}/_tags with n, last, orderby=timeasc|timedesc and digest filter."}
  GetTagAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "name, digest, createdTime, lastUpdateTime, signed (always false), changeableAttributes."}
  UpdateTagAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH changeable attributes."}
  DeleteTag: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /acr/v1/{repo}/_tags/{tag} untags the manifest and keeps it."}
  ListManifestAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /acr/v1/{repo}/_manifests: digest, imageSize, architecture, os, mediaType, configMediaType and tags read from the stored manifest and its config blob; indexes list references."}
  GetManifestAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "manifest object with quarantineState Passed."}
  UpdateManifestAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH changeable attributes."}
families:
  arm_integration: {status: ok, note: "services/azurearm Microsoft.ContainerRegistry (internal/app adapter): registries (PUT, GET, PATCH, DELETE, list by group and subscription; sku Basic, Standard or Premium; adminUserEnabled; loginServer https-less {name}.azurecr.{host}:{port}), listCredentials, regenerateCredential, checkNameAvailability, replications (Premium only) and webhooks (getCallbackConfig, ping, listEvents; the data plane fires them on manifest push and delete). Credentials and registry existence live in the data plane; deleting a registry in ARM deletes its images."}
  host_routing: {status: ok, note: "The registry is the first label of the Host header (name.azurecr.localhost:10011), else the only registry when exactly one exists; an unknown host with several registries answers 404 NAME_UNKNOWN."}
  auth: {status: ok, note: "Basic with the admin user (username = registry name, password or password2, constant-time compared, only while adminUserEnabled), Basic with a refresh or access token as the password (username 00000000-0000-0000-0000-000000000000 for az acr login), or Bearer access token. Access tokens are limited to the scopes they carry (repository:<name>:pull,push,delete or *; registry:catalog:*): a missing scope answers 401 UNAUTHORIZED with a Bearer challenge naming the scope and error="insufficient_scope". Admin and refresh-token credentials are unrestricted. Anything else gets 401 with a Bearer challenge naming the scope."}
  storage: {status: ok, note: "One embedded distribution registry per registry, built on first use. In memory by default; with --persist blobs and manifests live under <data dir>/registries/<name>, while credentials and image metadata are in the snapshot."}
  terraform: {status: ok, note: "azurerm_container_registry (admin_enabled) applies, re-plans empty and destroys; the admin credentials push and pull an image over the registry API (test/terraform/azure/containerregistry_test.go)."}
gaps: []
deferred: []
items_still_open:
  - "Access-token scopes are enforced on /v2 only; the /acr/v1 metadata API accepts any valid token."
  - "The listener is plain HTTP unless --azure-acr-tls-cert/--azure-acr-tls-key are set, so az acr login and docker only trust it as an insecure or localhost registry."
  - "Anonymous pull, repository-scoped tokens, scope maps and tokens, connected registries, cache rules, credential sets, agent pools and tasks are not modelled."
  - "ARM importImage, listUsages, listPrivateLinkResources, private endpoint connections, customer-managed keys, network rules and policies other than their defaults are stored or defaulted but have no effect."
  - "Webhooks: no retries, the quarantine action never fires, each webhook keeps its last 100 events, and ARM webhook PATCH is not implemented (PUT replaces)."
  - "Replications are metadata only: there is one copy of the data."
  - "Blobs unreferenced by any manifest are never garbage collected, and registry size is not metered."
  - "Content trust, quarantine and retention policies are not implemented; quarantineState is always Passed and signed is always false."
  - "Each registry's embedded distribution library leaves one go-events broadcaster goroutine running until process exit."
  - "No dashboard page."
leaks: {status: partial, note: "Shutdown closes the HTTP server and every registry; the distribution library's event broadcaster goroutine (one per registry built) cannot be stopped and is ignored by the leak check."}
---

Initial implementation (2026-10-22): data plane verified against the Docker Registry v2 conformance calls in pkgs/ociregistry/ocitest, ARM against armcontainerregistry v1.2.0 (internal/app/registry_wiring_test.go), and against hashicorp/azurerm (azurerm_container_registry) in test/terraform/azure.
