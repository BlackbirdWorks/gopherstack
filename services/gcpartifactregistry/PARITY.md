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
  ListPackages: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Each image pushed through the Docker API is a package named .../packages/{image with / as %2F}; create and update time span its manifests. Non-DOCKER repositories answer FAILED_PRECONDITION. filter is rejected."}
  GetPackage: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  DeletePackage: {wire: ok, errors: ok, state: ok, persist: ok, note: "Deletes every version of the image."}
  ListVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "A version is a manifest digest. related_tags lists its tags; view FULL adds metadata (imageSizeBytes, mediaType, name). order_by create_time, update_time or name with desc."}
  GetVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  DeleteVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "A version with tags needs force, else FAILED_PRECONDITION."}
  BatchDeleteVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "Names must belong to the parent package; validate_only checks and deletes nothing. Tagged versions are force-deleted."}
  ListTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Only the version=\"<name>\" filter is supported."}
  GetTag: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  CreateTag: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags an existing version by writing the manifest under the new tag in the embedded registry, so docker pull of the tag works."}
  UpdateTag: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask version only; moving a tag in a repository with immutable_tags is FAILED_PRECONDITION."}
  DeleteTag: {wire: ok, errors: ok, state: ok, persist: ok, note: "Untags the manifest and keeps the version."}
  ListDockerImages: {wire: ok, errors: ok, state: ok, persist: n/a, note: "One entry per manifest: name .../dockerImages/{image}@{digest}, uri, tags, image_size_bytes, media_type, upload_time and update_time."}
  GetDockerImage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "By image@digest or image:tag."}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Repository policies are stored; there is no enforcement."}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "A stale etag is FAILED_PRECONDITION."}
  TestIamPermissions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Reports every requested permission as granted because IAM is not enforced."}
  GetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.longrunning.Operations via pkgs/gcplro, also on the REST path projects/*/locations/*/operations/*."}
families:
  docker_data_plane: {status: ok, note: "The Docker Registry v2 API is served on the same port as gRPC and REST (pkgs/gcprest.ServeWithFallback sends /v2 to it). Images live at /v2/{project}/{repository}/{image}/...; the repository must exist, be DOCKER format and, when the Host is {location}-docker.pkg.dev, be in that location, else 404 NAME_UNKNOWN. Auth: a request needs an Authorization header; any Basic credential (oauth2accesstoken:<token>, _json_key) or Bearer token is accepted, and an unauthenticated one is challenged to /v2/token. /v2/{project}/{repository}/_catalog lists the repository's images. Immutable tags answer 409 TAG_INVALID when a tag is pushed again."}
  storage: {status: ok, note: "One embedded distribution registry for the service, repositories named project/repository/image. In memory by default; with --persist blobs and manifests live under <data dir>/registries/images and the repository and image metadata is in the snapshot."}
  routing_isolation: {status: ok, note: "Own gRPC server with the REST transcoder and Docker handler bound in StartWorker to --gcp-artifacts-port (default 8134, fails fast if taken); never registered on the shared AWS router."}
  terraform: {status: ok, note: "google_artifact_registry_repository (DOCKER and MAVEN) with artifact_registry_custom_endpoint applies, re-plans empty and destroys; an image pushed with the Docker API appears in dockerImages (test/terraform/gcp/artifactregistry_test.go)."}
gaps: []
deferred: []
items_still_open:
  - "Maven, npm, Python, apt, yum, KFP, Go and generic repositories can be created but hold no artifacts: their artifact, file, rule and attachment RPCs are not implemented and return UNIMPLEMENTED."
  - "ListFiles, GetFile, DeleteFile, UpdateFile, UpdatePackage, UpdateVersion, rules, attachments, ExportArtifact, project settings and VPC-SC configuration are not implemented (UNIMPLEMENTED)."
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
