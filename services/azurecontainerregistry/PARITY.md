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
  DeleteBlob: {wire: ok, errors: ok, state: ok, persist: ok, note: "Library delete support is on; deleting a manifest also removes the config, layer and manifest blobs no other manifest of the registry references."}
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
  arm_integration: {status: ok, note: "services/azurearm Microsoft.ContainerRegistry (internal/app adapter): registries (PUT, GET, PATCH, DELETE, list by group and subscription; sku Basic, Standard or Premium; adminUserEnabled; loginServer https-less {name}.azurecr.{host}:{port}), listCredentials, regenerateCredential, checkNameAvailability, replications (Premium only) and webhooks (getCallbackConfig, ping, listEvents; the data plane fires them on manifest push, import and delete), scopeMaps (with the system scope maps _repositories_admin, _repositories_pull and _repositories_push) and tokens, generateCredentials, importImage, cacheRules and credentialSets (Key Vault secrets read from the emulated vault), customer-managed keys (Premium, create-time only, RSA key checked against the emulated Key Vault) and the Premium network rule set and publicNetworkAccess, which the data plane enforces. Private endpoint connections and private link resources are served by the shared private-endpoint layer (rp_network_pelink.go, group id registry). Credentials and registry existence live in the data plane; deleting a registry in ARM deletes its images."}
  host_routing: {status: ok, note: "The registry is the first label of the Host header (name.azurecr.localhost:10011), else the only registry when exactly one exists; an unknown host with several registries answers 404 NAME_UNKNOWN."}
  auth: {status: ok, note: "Basic with the admin user (username = registry name, password or password2, constant-time compared, only while adminUserEnabled), Basic with a refresh or access token as the password (username 00000000-0000-0000-0000-000000000000 for az acr login), or Bearer access token. Access tokens are limited to the scopes they carry (repository:<name>:pull,push,delete or *; registry:catalog:*; on /acr/v1 metadata_read for reads, metadata_write for attribute PATCH and untag, delete for repository and manifest delete): a missing scope answers 401 UNAUTHORIZED with a Bearer challenge naming the scope and error="insufficient_scope". Token credentials (username = token name, password = a generated password1/password2, token enabled, password unexpired) are limited by the token's scope map, evaluated per request so scope map edits apply at once (content/read, content/write, content/delete, metadata/read, metadata/write on repositories/<repo> with * or <prefix>/* wildcards that add up); the token endpoint grants only the intersection of the requested scopes and the scope map. Admin and refresh-token credentials are unrestricted. /oauth2/exchange validates the Entra access token against the emulator's own issuer (and the tenant form field) when the ARM provider is present. Anything else gets 401 with a Bearer challenge naming the scope."}
  storage: {status: ok, note: "One embedded distribution registry per registry, built on first use. In memory by default; with --persist blobs and manifests live under <data dir>/registries/<name>, while credentials and image metadata are in the snapshot."}
  terraform: {status: ok, note: "azurerm_container_registry (admin_enabled) applies, re-plans empty and destroys; the admin credentials push and pull an image over the registry API (test/terraform/azure/containerregistry_test.go)."}
gaps: []
deferred: []
items_still_open:
  - "Connected registries, agent pools and ACR Tasks (tasks, runs, taskRuns, scheduleRun, task run logs) are not modelled: the ARM routes belong in services/azurearm/rp_acr_routes.go:45 (containerRegistryRoutes), an existing file this pass could not edit, and a task build step needs an image build API that pkgs/container does not have (PullImage at pkgs/container/container.go:196 is the only image call)."
  - "ARM listUsages is not served: its route belongs in services/azurearm/rp_acr_routes.go:45 and the Size usage needs registry size metering, which has no consumer until the route exists. Quarantine, content trust (Notary), export, soft delete and the other policies are stored and have no effect (the Learn quarantine page container-registry-quarantine answered 404 when checked), so quarantineState is always Passed, signed is always false and the webhook quarantine action never fires. Revoking the registry identity's Key Vault permission is not modelled (Key Vault permission evaluation is out of scope)."
  - "Each registry's embedded distribution library leaves one go-events broadcaster goroutine running until process exit: distribution v3.1.2 registry/handlers/app.go:526 builds events.NewBroadcaster into the unexported app.events field and App.Shutdown (app.go:461) never closes it."
leaks: {status: partial, note: "Shutdown closes the HTTP server and every registry; the distribution library's event broadcaster goroutine (one per registry built) cannot be stopped and is ignored by the leak check."}
---

Initial implementation (2026-10-22): data plane verified against the Docker Registry v2 conformance calls in pkgs/ociregistry/ocitest, ARM against armcontainerregistry v1.2.0 (internal/app/registry_wiring_test.go), and against hashicorp/azurerm (azurerm_container_registry) in test/terraform/azure.

/acr/v1 scope actions follow learn.microsoft.com/azure/container-registry/container-registry-token-based-repository-permissions (metadata/read lists tags and manifests, metadata/write updates attributes and untags, content/delete deletes a repository or manifest); the REST reference pages under rest/api/containerregistry/containerregistry returned 404, so the wire scope strings are the existing metadata_read challenge extended.

ARM PATCH of webhooks and replications merges WebhookUpdateParameters (tags; properties serviceUri, customHeaders, status, scope, actions): Azure/azure-rest-api-specs containerregistry 2023-07-01 containerregistry.json.

Anonymous pull (properties.anonymousPullEnabled via ARM) allows unauthenticated GET/HEAD of repository content and an anonymous pull-only /oauth2/token grant; push, delete and the catalog still need credentials (learn.microsoft.com/en-us/azure/container-registry/anonymous-pull-access). The Standard/Premium-only restriction on that page is not enforced for Basic registries.

Scope map actions, wildcard rules, system scope maps, token passwords (two, optional expiry, shown once) and token authentication follow learn.microsoft.com/azure/container-registry/container-registry-token-based-repository-permissions (the system scope map list there has three entries, so only those exist). Scope map and token ARM shapes: armcontainerregistry v1.2.0 ScopeMapProperties, TokenProperties, GenerateCredentialsParameters/Result; the SDK test internal/app/registry_tokens_test.go drives them.

importImage copies manifests, indexes, configs and layers from another emulated registry (source.resourceId, or a registryUri naming an emulated login server with credentials the source accepts) or, with --azure-acr-remote-imports, from an external registry through pkgs/httpclient with Basic or Bearer token authentication. targetTags, untaggedTargetRepositories and mode (NoForce refuses existing tags, Force overwrites) follow armcontainerregistry ImportImageParameters. Pushed events fire registry webhooks.

Cache rules pull a missing manifest from the upstream on first pull, keep it, refresh nothing (learn.microsoft.com/azure/container-registry/container-registry-artifact-cache: no automatic new-tag pulls, no overlapping rules, at most 1000 rules, Docker Hub paths get library/ when missing); the upstream login comes from the credential set's Key Vault secrets. Pulls through a rule need the same external-import switch unless the upstream is an emulated registry.

Network rules: ipRules and defaultAction Deny need Premium and answer 403; publicNetworkAccess Disabled overrides every IP rule (learn.microsoft.com/azure/container-registry/container-registry-access-selected-networks). Customer-managed keys: Premium, user-assigned identity, enabled only while creating, never disabled afterwards, RSA keys only; revoking or deleting the key blocks all registry data and restoring it restores access (learn.microsoft.com/azure/container-registry/tutorial-customer-managed-keys and tutorial-rotate-revoke-customer-managed-keys).

Retention policy (Premium only; properties.policies.retentionPolicy status and days, 0 to 365): a manifest that becomes untagged while the policy is enabled is deleted after the configured days on the backend clock, at once for 0; only Docker manifests and manifest lists are deleted, never a manifest an index references, never one with delete-enabled false, and disabling the policy cancels pending deletions (learn.microsoft.com/azure/container-registry/container-registry-retention-policy). Manifests untagged before the policy was enabled are not scheduled.

Deleting a manifest garbage collects the config, layer and manifest blobs that no remaining manifest of the registry references. Blobs uploaded but never referenced by a manifest are not collected, so an upload in flight is never lost.

## Emulator choices where the vendor is silent

- The firewall deny body is code DENIED with a client-IP message; a revoked customer-managed key answers 403 DENIED; both are emulator wording.
- A scope map used by a token and a credential set used by a cache rule answer 409 Conflict on delete; modifying a system scope map answers 400.
- Credential health on a credential set is Healthy when both Key Vault secrets resolve, else Unhealthy with SecretNotFound.
- importImage failures: unknown source image or registry 404, an existing target tag under NoForce 409, external registry disabled or refused 400.
- A cache rule does not check that the target repository is absent.
- The listener is plain HTTP unless --azure-acr-tls-cert and --azure-acr-tls-key are set: trusting a certificate is a client-side setting the emulator cannot change. This rests on reasoning, not on a checked source.
- Webhook deliveries are not retried and each webhook keeps its last 100 events: learn.microsoft.com/azure/container-registry/container-registry-webhook mentions neither retries nor an event history size.
- The retention sweep runs lazily when a request reaches the registry, not on a daily schedule: the Learn retention page gives no purge cadence.

## External effects that are recorded, not performed

- Replications are control-plane resources only: the registry keeps one copy of its data because copying to other Azure regions leaves the emulator.
