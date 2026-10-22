---
service: gcpcloudbuild
sdk_module: cloud.google.com/go/cloudbuild@v1.30.0
last_audit_commit: 1c0729588
last_audit_date: 2026-10-22
overall: B
ops:
  CreateBuild: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns a pending operation (operations/build/{project}/{id}, or projects/*/locations/*/operations/{id} for a regional parent) that completes with the Build once it is terminal. steps are required; step ids are unique; wait_for names earlier ids or is ['-']; script excludes args and entrypoint; timeout 0-24h; substitution keys start with an underscore and every $VAR/${VAR} must resolve (built-ins PROJECT_ID, BUILD_ID, PROJECT_NUMBER, LOCATION) unless options.substitution_option is ALLOW_LOOSE; $$ is a literal dollar; options.pool.name must be a stored worker pool. Status QUEUED then WORKING then SUCCESS, FAILURE, TIMEOUT, CANCELLED or INTERNAL_ERROR, each change published to the cloud-builds topic."}
  GetBuild: {wire: ok, errors: ok, state: ok, persist: ok, note: "By name or project_id plus id (global builds). A build is visible only in its own location."}
  ListBuilds: {wire: ok, errors: ok, state: ok, persist: ok, note: "Newest first, paged through pkgs/page. filter: AND-joined terms over status, id/build_id, trigger_id, project_id, tags and create_time/start_time/finish_time with =, !=, <, <=, >, >=; anything else is INVALID_ARGUMENT."}
  CancelBuild: {wire: ok, errors: ok, state: ok, persist: ok, note: "Cancels queued or running builds: running containers are stopped and removed by id and unstarted steps end CANCELLED. A finished build is FAILED_PRECONDITION."}
  RetryBuild: {wire: ok, errors: ok, state: ok, persist: ok, note: "Queues a new build from the original request (before substitution), same location and trigger."}
  ApproveBuild: {wire: ok, errors: ok, state: ok, persist: ok, note: "Records APPROVED or REJECTED on a build whose approval is PENDING. The v1.30 BuildTrigger has no approval_config, so nothing in the emulator creates a PENDING build and every call on a real build is FAILED_PRECONDITION."}
  CreateBuildTrigger: {wire: ok, errors: ok, state: ok, persist: ok, note: "name is unique per project and location; a build template (build, filename, autodetect or git_file_source) is required; at most one of trigger_template, github, pubsub_config, webhook_config, repository_event_config. id and resource_name (projects/*/locations/*/triggers/*) are assigned; webhook and pubsub states are OK."}
  GetBuildTrigger: {wire: ok, errors: ok, state: ok, persist: ok, note: "By name or project_id plus trigger_id."}
  ListBuildTriggers: {wire: ok, errors: ok, state: ok, persist: ok, note: "Oldest first, paged."}
  DeleteBuildTrigger: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  UpdateBuildTrigger: {wire: ok, errors: ok, state: ok, persist: ok, note: "Full replace without update_mask; with a mask only the top-level fields named (a dotted path replaces the whole top-level field). id, resource_name and create_time are immutable."}
  RunBuildTrigger: {wire: ok, errors: ok, state: ok, persist: ok, note: "Runs the inline build template with the trigger's tags (plus trigger-{id}) and substitutions, overridden by the request source's substitutions; TRIGGER_NAME, REPO_NAME, BRANCH_NAME, TAG_NAME, COMMIT_SHA, SHORT_SHA, REVISION_ID and REF_NAME come from the source. A trigger reading its config from a repository file is FAILED_PRECONDITION."}
  ReceiveTriggerWebhook: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Reads the trigger's Secret Manager secret and compares it in constant time with the secret query parameter (PERMISSION_DENIED on mismatch), then runs the trigger. Payload bindings ($(body...)) are not evaluated."}
  CreateWorkerPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "Regional parent; worker_pool_id is 1-63 lowercase letters, digits or hyphens. The operation is already done and the pool is RUNNING with uid, times and an etag. validate_only stores nothing."}
  GetWorkerPool: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ListWorkerPools: {wire: ok, errors: ok, state: ok, persist: ok, note: "Sorted by name, paged."}
  UpdateWorkerPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask over display_name, annotations and the config; the etag changes with the content."}
  DeleteWorkerPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "A stale etag is ABORTED; allow_missing succeeds on a missing pool."}
  GetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.longrunning.Operations via pkgs/gcplro, on operations/** and projects/*/locations/*/operations/*. Build operations stay pending until the build ends; WaitOperation returns the current state at once (clients poll)."}
families:
  execution: {status: ok, note: "Each step runs as its own container (pkgs/container) on the host's Docker or Podman: image = step name, entrypoint, args, env (options.env plus step env), dir relative to /workspace, volumes shared between steps, script run from a mounted file, step timeout. /workspace is a host directory mounted into every step; /var/run/docker.sock is mounted too when present so docker build steps work. Steps run in parallel per wait_for (default: after every earlier step; ['-'] starts at once). The first failing step (non-zero exit not covered by allow_failure or allow_exit_codes) fails the build with 'build step N \"image\" failed: step exited with non-zero status: C' and stops the others. Containers are removed by id. Without a container runtime (or --gcp-cloudbuild-engine=none) the build goes QUEUED, WORKING, FAILURE with a status detail saying so; the API state still works."}
  timeouts: {status: ok, note: "Build timeout (default 10m) and step timeout end the run through an injectable timer: TIMEOUT with the step or build named in status_detail."}
  source: {status: partial, note: "storageSource: a .tar.gz or .zip object in the GCS emulator is extracted into /workspace (path traversal, links and oversized archives are rejected). A build without a source starts with an empty workspace. Object generations are ignored."}
  images: {status: ok, note: "images: each is tagged and pushed from the build host's daemon to the Artifact Registry emulator (<location>-docker.pkg.dev/<project>/<repository>/<image>[:tag] maps to localhost:<--gcp-artifacts-port>/<project>/<repository>/<image>); results.images carries name and digest. Any other registry host fails the build with PUSH_FAILED, a missing local image with PUSH_IMAGE_NOT_FOUND."}
  artifacts: {status: ok, note: "artifacts.objects: files matching paths (globs relative to /workspace, no **) are uploaded to the gs:// location keeping their relative path, with artifacts-<build id>.json (location and sha256 per file) beside them; results.artifact_manifest and num_artifacts are set."}
  logging: {status: ok, note: "Every line (phase markers and 'Step #N - \"id\": output') goes to Cloud Logging as logName projects/<p>/logs/cloudbuild on resource type build (build_id, build_trigger_id, project_id) and is uploaded as log-<id>.txt to logs_bucket (default gs://<project>_cloudbuild, created on demand). options.logging NONE, GCS_ONLY and CLOUD_LOGGING_ONLY are honoured. Secret Manager values are masked."}
  notifications: {status: ok, note: "QUEUED, WORKING and the terminal status are published to projects/<p>/topics/cloud-builds (created if missing) as the Build in protojson with buildId and status attributes."}
  secrets: {status: partial, note: "available_secrets.secret_manager values are read from the Secret Manager emulator into the env of steps that list the name in secret_env. KMS-encrypted secrets (secrets, available_secrets.inline) fail the build."}
  persistence: {status: ok, note: "Builds (with their original request), triggers, worker pools and operations are in the snapshot. A build that was QUEUED or WORKING when it was taken is INTERNAL_ERROR after a restore."}
  routing_isolation: {status: ok, note: "Own gRPC server with the REST transcoder bound in StartWorker to --gcp-cloudbuild-port (default 8136, fails fast if taken); never registered on the shared AWS router."}
  terraform: {status: ok, note: "google_cloudbuild_trigger (trigger_template and pubsub_config with inline build steps) with cloud_build_custom_endpoint applies, re-plans empty and destroys (test/terraform/gcp/cloudbuild_test.go)."}
gaps: []
deferred: []
items_still_open:
  - "Only storageSource is fetched: repoSource, gitSource, connectedRepository and storageSourceManifest builds fail with FETCH_SOURCE_FAILED, and triggers that read cloudbuild.yaml from a repository can be stored but not run."
  - "Event triggers (trigger_template, github, pubsub_config, repository_event_config) are stored and never fire; builds start only from CreateBuild, RetryBuild, RunBuildTrigger and ReceiveTriggerWebhook."
  - "KMS-encrypted secrets, dynamic (bash) substitutions and automap_substitutions are not supported."
  - "Approvals cannot be exercised: no trigger setting creates a PENDING build."
  - "Worker pools are stored only; options.pool, machine_type and disk_size_gb do not change where or how steps run, and queue_ttl never expires a build."
  - "results.build_step_images, build_step_outputs, source_provenance hashes, warnings and the FETCHSOURCE/PUSH timing entries are not populated; only the BUILD timing, step timing and artifact timing are."
  - "Artifacts other than artifacts.objects (Maven, Python, npm, Go modules) are not uploaded, and artifacts.images is not honoured beyond images."
  - "Images can only be pushed to the Artifact Registry emulator; gcr.io and other registries fail the build."
  - "Step containers share the host Docker daemon (and its socket when present), so builds are not isolated from each other or from the host."
  - "Locations are path parameters only: the v1 API has no ListLocations, and any location id is accepted."
  - "Authentication is not checked, IAM is not enforced and service_account is ignored."
  - "No dashboard page."
leaks: {status: ok, note: "Shutdown cancels running builds, removes their containers and workspaces, and waits for the build goroutines."}
---

Initial implementation (2026-10-22): API verified through cloud.google.com/go/cloudbuild v1.30.0 over gRPC and REST (test/integration/gcp_cloudbuild_test.go), execution against an in-process fake container runtime in the unit tests and a real Docker daemon in the docker-gated test, and google_cloudbuild_trigger in test/terraform/gcp.
