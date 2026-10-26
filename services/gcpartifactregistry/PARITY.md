---
service: gcpartifactregistry
sdk_module: cloud.google.com/go/artifactregistry@v1.27.0
last_audit_commit: 364908f83
last_audit_date: 2026-10-22
overall: B
ops:
  CreateRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns an operation that is already done (pkgs/gcplro); the Repository is its response. repository_id is 1-63 lowercase letters, digits or hyphens; format is required (DOCKER, MAVEN, NPM, PYTHON, APT, YUM, KFP, GO, GENERIC). The id is unique per project across locations. docker_config is only valid for DOCKER; VIRTUAL and REMOTE modes need their config message. Default mode STANDARD_REPOSITORY."}
  GetRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "DOCKER repositories carry registry_uri (<host>:<port>/<project>/<repository>, from --gcp-artifacts-registry-host) and size_bytes summed from the stored manifests."}
  ListRepositories: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Per location or all locations with '-', sorted by name, paged through pkgs/page. filter is rejected with INVALID_ARGUMENT."}
  UpdateRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask over description, labels, cleanup_policies, cleanup_policy_dry_run, docker_config and vulnerability_scanning_config; an empty mask copies every set mutable field; anything else is INVALID_ARGUMENT."}
  DeleteRepository: {wire: ok, errors: ok, state: ok, persist: ok, note: "Deletes every image in the repository from the embedded registry."}
  ListPackages: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Each image pushed through the Docker API is a package named .../packages/{image with / as %2F}; create and update time span its manifests. GENERIC, NPM, PYTHON and MAVEN repositories list the packages their uploads created; other formats answer FAILED_PRECONDITION. filter is rejected."}
  GetPackage: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  DeletePackage: {wire: ok, errors: ok, state: ok, persist: ok, note: "Deletes every version of the image."}
  ListVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "A version is a manifest digest. related_tags lists its tags; view FULL adds metadata (imageSizeBytes, mediaType, name). order_by create_time, update_time or name with desc."}
  GetVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  DeleteVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "A version with tags needs force, else FAILED_PRECONDITION."}
  BatchDeleteVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "Names must belong to the parent package; validate_only checks and deletes nothing. Tagged versions are force-deleted."}
  ListTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Only the version=\"<name>\" filter is supported. On an NPM package the tags are its dist-tags."}
  GetTag: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  CreateTag: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags an existing version by writing the manifest under the new tag in the embedded registry, so docker pull of the tag works."}
  UpdateTag: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask version only; moving a tag in a repository with immutable_tags is FAILED_PRECONDITION."}
  DeleteTag: {wire: ok, errors: ok, state: ok, persist: ok, note: "Untags the manifest and keeps the version."}
  ListDockerImages: {wire: ok, errors: ok, state: ok, persist: n/a, note: "One entry per manifest: name .../dockerImages/{image}@{digest}, uri, tags, image_size_bytes, media_type, upload_time and update_time."}
  GetDockerImage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "By image@digest or image:tag."}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Repository policies are stored; there is no enforcement."}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "A stale etag is FAILED_PRECONDITION."}
  TestIamPermissions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Reports every requested permission as granted because IAM is not enforced."}
  UpdatePackage: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask display_name or annotations (an empty mask copies the set ones); anything else is INVALID_ARGUMENT. Edits on a DOCKER package are dropped with the package; generic packages keep them in their own record."}
  UpdateVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask description or annotations; update_time moves forward."}
  ListFiles: {wire: ok, errors: ok, state: ok, persist: n/a, note: "A DOCKER repository's files are its manifests and the config and layer blobs they reference, named files/{sha256:digest}, size_bytes, a SHA256 hash and owner = the version of the first manifest that holds the blob. A GENERIC repository's files are the uploads. filter: name, owner and annotations.<key>[:value] (backtick-quoted keys, * wildcards) joined by AND; order_by name, create_time or update_time with desc; paged through pkgs/page."}
  GetFile: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  UpdateFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask annotations only."}
  DeleteFile: {wire: ok, errors: ok, state: ok, persist: ok, note: "GENERIC repositories only, as the API documents; DOCKER answers FAILED_PRECONDITION. The version and package stay when their last file goes."}
  CreateRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "rule_id is required; action and operation must be set. A repository holds one rule for itself (empty package_id) and one per package, a second is ALREADY_EXISTS."}
  GetRule: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListRules: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  UpdateRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask over action, operation, condition and package_id."}
  DeleteRule: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  GetProjectSettings: {wire: ok, errors: ok, state: ok, persist: ok, note: "A project that never changed them reads REDIRECTION_FROM_GCR_IO_DISABLED."}
  UpdateProjectSettings: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask legacy_redirection_state or pull_percent (0-100)."}
  GetVPCSCConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "Per project and location; reads ALLOW until changed."}
  UpdateVPCSCConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask vpcsc_policy, ALLOW or DENY; stored only, nothing consults it."}
  UploadGenericArtifact: {wire: ok, errors: ok, state: ok, persist: ok, note: "REST only (POST /upload/v1/{parent}/genericArtifacts:create, uploadType=multipart), as in the discovery document; the operation is already done and its response is the GenericArtifact. Creates the package, the version and the file {package_id}:{version_id}:{filename}; a repeated file is ALREADY_EXISTS. Content is kept in memory and in the snapshot, at most 128 MiB per file."}
  DownloadFile: {wire: ok, errors: ok, state: ok, persist: n/a, note: "REST only (GET /v1/{name}:download?alt=media) for generic files and DOCKER manifests and blobs; without alt=media the response is {}. A DENY download rule answers PERMISSION_DENIED."}
  GetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.longrunning.Operations via pkgs/gcplro, also on the REST path projects/*/locations/*/operations/*."}
families:
  generic_data_plane: {status: ok, note: "GENERIC repositories store uploaded files with their package and version; ListFiles, GetFile, UpdateFile, DeleteFile, the package and version RPCs and the media download work on them (generic_test.go, through the Go client library and google.golang.org/api/artifactregistry/v1)."}
  docker_data_plane: {status: ok, note: "The Docker Registry v2 API is served on the same port as gRPC and REST (pkgs/gcprest.ServeWithFallback sends /v2 to it). Images live at /v2/{project}/{repository}/{image}/...; the repository must exist, be DOCKER format and, when the Host is {location}-docker.pkg.dev, be in that location, else 404 NAME_UNKNOWN. Auth: a request needs an Authorization header; any Basic credential (oauth2accesstoken:<token>, _json_key) or Bearer token is accepted, and an unauthenticated one is challenged to /v2/token. /v2/{project}/{repository}/_catalog lists the repository's images. Immutable tags answer 409 TAG_INVALID when a tag is pushed again."}
  package_manager_data_plane: {status: ok, note: "npm, Python and Maven repositories keep real bytes in the generic stores, so packages, versions, files, size_bytes, persistence and DENY download rules (tarball, wheel/sdist and Maven artifact reads) work as for GENERIC. Routes are served on <location>-npm|python|maven.pkg.dev hosts as /<project>/<repo>/... (the repository must be in the host's location, else 404; the path form below stays and works on any host; tarball URLs and redirects follow the form used), and at /npm/<project>/<repo>/ (PUT <pkg> publish with _attachments and dist-tags, GET packument with dist.shasum/integrity/tarball, GET <pkg>/-/<file>.tgz, /-/package/<pkg>/dist-tags[/<tag>] GET/PUT/DELETE, unpublish via PUT/DELETE .../-rev/<rev>, scoped names as @scope/name or @scope%2Fname, re-publish of a version is 409); /python/<project>/<repo>/ (POST multipart :action=file_upload with sha256/md5 digest checks, 409 on a repeated file, GET simple/ and simple/<name>/ PEP 503 with #sha256 fragments and data-requires-python, non-normalised names redirect, GET packages/<name>/<version>/<file>); /maven/<project>/<repo>/ (PUT/GET/HEAD by groupId/artifactId/version layout, package id groupId:artifactId, .sha1/.md5/.sha256/.sha512 computed on GET, generated artifact-level maven-metadata.xml, version-level snapshot metadata stored, version_policy RELEASE/SNAPSHOT enforced, release overwrites 409, snapshots overwrite only with allow_snapshot_overwrites). npm dist-tags are the package's Tag resources (ListTags, GetTag, CreateTag, UpdateTag, DeleteTag and the /-/package/<pkg>/dist-tags routes read and write the same state). Verified with the real npm and pip clients (pkgfmt_real_test.go); mvn is not installed here."}
  storage: {status: ok, note: "One embedded distribution registry for the service, repositories named project/repository/image. In memory by default; with --persist blobs and manifests live under <data dir>/registries/images and the repository and image metadata is in the snapshot."}
  routing_isolation: {status: ok, note: "Own gRPC server with the REST transcoder and Docker handler bound in StartWorker to --gcp-artifacts-port (default 8134, fails fast if taken); never registered on the shared AWS router."}
  terraform: {status: ok, note: "google_artifact_registry_repository (DOCKER and MAVEN) with artifact_registry_custom_endpoint applies, re-plans empty and destroys; an image pushed with the Docker API appears in dockerImages (test/terraform/gcp/artifactregistry_test.go)."}
gaps: []
deferred: []
items_still_open:
  - "apt, yum, KFP and Go repositories can be created but hold no artifacts: their artifact RPCs and uploads are not implemented and return FAILED_PRECONDITION."
  - "Maven and Python repositories expose no Tag resources (only npm dist-tags are mapped); Maven artifact-level maven-metadata.xml is generated, so a PUT of it is accepted and ignored; Maven HTTP DELETE, the Python JSON simple API (PEP 691) and yank, and npm search/audit/login endpoints are not implemented because Google's documentation does not describe them (deletion is through the API, console and gcloud); file and package names for these formats are the emulator's choice."
  - "Attachments, ExportArtifact, ImportAptArtifacts, ImportYumArtifacts and the resumable generic upload (uploadType=resumable) are not implemented."
  - "Rule conditions (CEL) are stored but never evaluated, so a rule with a condition never takes effect; download rules are enforced on Docker manifest and blob reads and on file downloads only."
  - "Project settings and the VPC-SC policy are stored; nothing consults them."
  - "Remote and virtual repositories are stored but never proxy or merge anything; cleanup policies are stored and never run."
  - "A repository id is unique per project across locations, and image data is not separated by location."
  - "Operations are recorded already finished; there is no pending state, and CancelOperation changes nothing."
  - "Authentication is not checked and IAM is not enforced; every credential is accepted."
  - "Docker pushes to a path whose repository does not exist are refused instead of creating it (gcr.io-style auto-creation is not modelled)."
  - "Vulnerability scanning, signed attestations and docker image build_time are not modelled; Artifact Registry's own location catalogue is not served."
  - "The embedded distribution library leaves one go-events broadcaster goroutine running until process exit."
  - "No dashboard page."
leaks: {status: partial, note: "Shutdown stops the gRPC server and releases the registry; the distribution library's event broadcaster goroutine cannot be stopped and is ignored by the leak check."}
---

Initial implementation (2026-10-22): API verified through cloud.google.com/go/artifactregistry v1.26.0 over gRPC and raw REST, the Docker data plane against the Docker Registry v2 conformance calls in pkgs/ociregistry/ocitest, and google_artifact_registry_repository in test/terraform/gcp.
