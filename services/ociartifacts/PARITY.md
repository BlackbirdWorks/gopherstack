---
service: ociartifacts
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every control-plane and content operation in the service tests
last_audit_commit: 4f3e4c057
last_audit_date: 2026-10-30
overall: B
ops:
  CreateContainerRepository: {wire: ok, errors: partial, state: ok, persist: ok, note: "200 with the ContainerRepository and etag. displayName and compartmentId are required; displayName is one or more lowercase path components of letters, digits, dots, underscores and hyphens joined by slashes, at most 255 characters (Docker distribution name grammar, which OCIR pushes must satisfy). Duplicate names in the tenancy are 409 Conflict (code unverified). isImmutable and isPublic default to false; readme needs content and a TEXT_MARKDOWN or TEXT_PLAIN format. createdBy is the signing user OCID."}
  GetContainerRepository: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Live imageCount, layerCount and layersSizeInBytes from the pushed images (shared layers count once), billableSizeInGBs rounded up to whole GB (unverified), namespace, timeLastPushed after the first push."}
  ListContainerRepositories: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; compartmentIdInSubtree through OCI Identity when linked; repositoryId, displayName (case-insensitive), isPublic and lifecycleState filters; sortBy TIMECREATED (default, newest first) or DISPLAYNAME; sortOrder; limit and opc-next-page; collection totals (repositoryCount, imageCount, layerCount, layersSizeInBytes, remainingItemsCount)."}
  UpdateContainerRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "isImmutable, isPublic, readme and tags; If-Match mismatch is 412 NoEtagMatch. Making a repository private takes effect for the next anonymous pull."}
  DeleteContainerRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "204. The repository, its images and signatures are removed and their manifests deleted from the embedded registry; a later GET is 404 (OCI documents DELETING and DELETED states; the emulator has no such window)."}
  ChangeContainerRepositoryCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 without a body; target compartment validated against OCI Identity when linked."}
  ListContainerImages: {wire: ok, errors: partial, state: ok, persist: n/a, note: "One summary per image (manifest digest). displayName is repo:latestVersion, or repo:lastVersion@digest when the image has no versions and repo:unknown@digest when it never had one (SDK ContainerImage field docs). Filters: displayName, imageId, repositoryId, repositoryName and version (trailing * wildcard), isVersioned, lifecycleState, imageDigest. DELETED images are hidden unless filtered for."}
  GetContainerImage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Full model: versions (push order), layers ordered by digest with sizes, layersSizeInBytes, manifestSizeInBytes, pullCount and timeLastPulled. DELETED images stay readable."}
  DeleteContainerImage: {wire: ok, errors: ok, state: ok, persist: ok, note: "204. The image becomes DELETED at once: listings hide it and the registry answers MANIFEST_UNKNOWN for its tags and digest until it is restored. Deleting a DELETED image is 404."}
  UpdateContainerImage: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags only; If-Match 412."}
  RestoreContainerImage: {wire: ok, errors: partial, state: ok, persist: ok, note: "DELETED to AVAILABLE, keeping versions not since taken by another image; an optional version re-tags the image (re-put in the registry). 409 IncorrectState when the image is not DELETED, 409 Conflict when the version is held in an immutable repository."}
  RemoveContainerVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Removes one version and the tag in the registry; the image stays. Unknown version is 404."}
  LookupContainerImageByUri: {wire: ok, errors: ok, state: ok, persist: n/a, note: "namespace/repo:version or namespace/repo@digest; only AVAILABLE images."}
  CreateContainerImageSignature: {wire: ok, errors: partial, state: ok, persist: ok, note: "All fields required; signingAlgorithm must be one of SHA_224/256/384/512_RSA_PKCS_PSS (SDK enum) else 400 InvalidParameter; message and signature must be base64 (SDK field docs: base64 encoded). The image must exist and be AVAILABLE. displayName is last 10 characters of kmsKeyId, kmsKeyVersionId, the algorithm and the last 10 characters of the signature id, joined by :: (SDK example). An identical signature is 409 Conflict. The signature is stored, not verified against Vault."}
  GetContainerImageSignature: {wire: ok, errors: ok, state: ok, persist: n/a, note: "With etag."}
  ListContainerImageSignatures: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; imageId, repositoryId, repositoryName (wildcard), imageDigest, displayName, kmsKeyId, kmsKeyVersionId and signingAlgorithm filters; paged and sorted like the other listings."}
  UpdateContainerImageSignature: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags only."}
  DeleteContainerImageSignature: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; removed outright."}
  GetContainerConfiguration: {wire: ok, errors: ok, state: ok, persist: n/a, note: "namespace and isRepositoryCreatedOnFirstPush (default true, unverified) with an etag. The configuration is tenancy-wide; compartmentId is required and checked."}
  UpdateContainerConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "isRepositoryCreatedOnFirstPush; If-Match 412. When false, a push to a repository that does not exist is refused with 403 DENIED."}
  CreateRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "Generic repositories only: repositoryType GENERIC (else 400), isImmutable and compartmentId required, displayName optional (generated as repository plus a timestamp when absent). Returns the GenericRepository with repositoryType."}
  GetRepository: {wire: ok, errors: ok, state: ok, persist: n/a, note: "With etag."}
  ListRepositories: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Generic repositories of a compartment; id, displayName, isImmutable and lifecycleState filters; paged."}
  UpdateRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, description and tags; repositoryType must be GENERIC."}
  DeleteRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; also deletes the repository's artifacts (unverified whether Oracle refuses a non-empty repository)."}
  ChangeRepositoryCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "200 without a body."}
  GetGenericArtifact: {wire: ok, errors: ok, state: ok, persist: n/a, note: "sha256 is upper-case hex of the uploaded bytes; displayName is path:version truncated to 255 (SDK field docs)."}
  GetGenericArtifactByPath: {wire: ok, errors: ok, state: ok, persist: n/a, note: "The SDK does not escape / in artifactPath, so the path is everything between /artifactPaths/ and the last /versions/."}
  ListGenericArtifacts: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId and repositoryId required; id, displayName, sha256 and lifecycleState filters; artifactPath and version are prefix filters (SDK docs); paged."}
  UpdateGenericArtifact: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags only."}
  UpdateGenericArtifactByPath: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags only."}
  DeleteGenericArtifact: {wire: ok, errors: ok, state: ok, persist: ok, note: "204."}
  DeleteGenericArtifactByPath: {wire: ok, errors: ok, state: ok, persist: ok, note: "204."}
  PutGenericArtifactContentByPath: {wire: ok, errors: partial, state: ok, persist: ok, note: "genericartifactscontent API (SDK path .../versions/{version}/content), served on the same listener instead of the generic.artifacts host. Creates the artifact or replaces it in a mutable repository (id kept, new sha256 and etag); an immutable repository answers 409 Conflict (code unverified); If-Match 412. Bodies are capped at 256 MiB (emulator limit) and held in memory."}
  GetGenericArtifactContent: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Raw bytes, application/octet-stream, etag."}
  GetGenericArtifactContentByPath: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Raw bytes by repository, path and version."}
  GetAccessToken: {wire: ok, errors: ok, state: ok, persist: n/a, note: "containerregistry API 20180419 GET /docker/token (SDK containerregistry.GetAccessToken): Basic credentials get a signed token for the requested repository scopes, a request without credentials gets an anonymous pull-only token. Body {token, access_token, scope, expires_in 3600}. Wrong namespace, missing user or empty password is 401."}
families:
  auth: {status: partial, note: "pkgs/ociauth verifies draft-cavage signatures for API keys registered through POST /_gopherstack/oci/keys or OCI Identity; accept-any otherwise, --oci-artifacts-enforce-auth rejects unsigned and unknown keys. Registry logins use docker login -u <namespace>/<user> with an OCI auth token as password (Oracle: Pushing Images Using the Docker CLI): the namespace must be the tenancy namespace. With OCI Identity linked the user must be an active Identity user and the token one of its current auth tokens (Identity keeps a salted PBKDF2 digest, compared in constant time) and createdBy is the user OCID; unlinked, any non-empty token passes. Tokens are never logged or returned."}
  error_envelope: {status: partial, note: "Control plane errors are {code,message} with opc-request-id: InvalidParameter, MissingParameter, NotAuthenticated, NotAuthorizedOrNotFound, NoEtagMatch, IncorrectState, Conflict, CannotParseRequest (OCI API error reference). Registry errors use the distribution {errors:[{code,message}]} envelope: UNAUTHORIZED with a WWW-Authenticate Bearer challenge, NAME_UNKNOWN, NAME_INVALID, MANIFEST_UNKNOWN, DENIED, TAG_INVALID, UNSUPPORTED."}
  registry_data_plane: {status: partial, note: "Docker Registry v2 from pkgs/ociregistry (distribution v3) at /v2 on the same listener, repository names <namespace>/<displayName>. docker login, push and pull were run against the listener with the real docker CLI. 401 challenges point at /20180419/docker/token (OCIR's token realm); Basic credentials are also accepted directly. Tokens are HMAC-signed with a per-process secret, carry the granted repository scopes and expire after one hour on the injectable clock; a token without a needed scope gets 401 insufficient_scope. Cross-repository blob mounts need pull on the source. _catalog is not offered (404 UNSUPPORTED)."}
  first_push: {status: partial, note: "A push to a repository that does not exist creates it in the tenancy root compartment (private, mutable, createdBy the pushing user name) when isRepositoryCreatedOnFirstPush is true (SDK ContainerConfiguration docs: repositories created this way belong to the root compartment). When false the push is 403 DENIED. Invalid names are 400 NAME_INVALID; a name outside the namespace is 404 NAME_UNKNOWN. Pulls never create repositories."}
  immutable_public: {status: partial, note: "In an immutable repository a tag that already points at a different manifest cannot be overwritten (409 TAG_INVALID, code unverified); re-pushing the same manifest, new tags and digest pushes are allowed. Public repositories allow anonymous pull (GET/HEAD manifests, blobs, tag lists) with an anonymous token; anonymous push and delete, and any access to private or unknown repositories, are 401."}
  images: {status: partial, note: "Every manifest pushed becomes an image keyed by digest with versions for its tags; a tag moves between images on overwrite. Layers and sizes come from the manifest; indexes list no layers. pullCount and timeLastPulled count manifest GETs answered 200 (HEAD and blob requests do not count). Data plane deletes by digest remove the image, by tag remove the version. Image and version createdBy is the user OCID with Identity linked, else the login name."}
  persistence: {status: partial, note: "Snapshot carries repositories, images, signatures, generic repositories and artifacts (content included), the container configuration, API keys and the sequence counter. Registry blobs and manifests live under the data directory when --persist is on; without one they are in memory and Restore drops the image and signature records so the control plane does not list images whose blobs are gone."}
  lifecycle: {status: partial, note: "All state changes are immediate; the injectable clock stamps timeCreated, timeLastPulled and token expiry."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-artifacts-port / OCI_ARTIFACTS_PORT (default 8166); never on the shared AWS router; cli.go reserves the port in the shared pool."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
items_still_open:
  - "IAM policies, repository policies, resource principals and instance principals are not applied; only API-key signatures are verified on the control plane (out of scope: OCI IAM evaluation)."
---

## Verified against Oracle documentation

- Duplicate container repository and identical signature creates return 409 NotAuthorizedOrResourceAlreadyExists (create-only code): https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm.

## 2026-11-13 verification

SDK `artifacts` v65.126.2 `ContainerImage.DisplayName` documents the `repo:lastVersion@digest` and `unknown` forms for an image without versions, matching `imageName`. RemoveContainerVersion, RestoreContainerImage, ChangeRepositoryCompartment and ChangeContainerRepositoryCompartment carry `opc-retry-token` in the SDK and replay the first answer for 24 hours.

## Primary-source checks (2026-11-13)
- ociartifacts: Registry registrycreatingarepository.htm confirms an on-first-push repository is a private standard repository in the root compartment.

## Emulator choices where the vendor is silent

- Limits: Registry/Concepts/registryoverview.htm gives 500 repositories and 500 GB per region and 100,000 images per repository; it names no error, so the control plane answers 400 LimitExceeded and a push 403 DENIED.
- Not published (checked artifacts/overview.htm, registrycreatingarepository.htm and the artifacts SDK comments): 409 Conflict for immutable generic overwrites; 409 TAG_INVALID for immutable tag overwrites; isRepositoryCreatedOnFirstPush default (true); billableSizeInGBs as whole GB rounded up; DeleteRepository cascading to artifacts; the 256 MiB artifact cap; one-hour registry token lifetime.
- Repository and image deletes are immediate (the SDK has a DELETING state but the registry pages give no retention window); blobs of deleted images stay in the embedded registry store, as the vendor documents no garbage-collection behaviour.
- What counts as a pull is undocumented: manifest GETs answered 200 count; HEAD and blob requests do not.
- systemTags are always empty; compartmentIdInSubtree needs the compartment tree from OCI Identity and without it matches only the exact compartment.
- Image scanning, vulnerability reports and registry replication belong to other OCI services and are not provided.

## External effects that are recorded, not performed

- Registry hostnames (<region-key>.ocir.io, generic.artifacts.<region>.oci.oraclecloud.com) cannot resolve to the emulator: one host:port serves every region key, the namespace is the first path segment, the generic content API shares the control-plane listener, and there is no TLS (docker treats localhost registries as insecure).
