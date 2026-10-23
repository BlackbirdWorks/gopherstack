---
service: gcprun
sdk_module: cloud.google.com/go/run@v1.23.0
last_audit_commit: 61b7b4a94
last_audit_date: 2026-10-19
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "service_id [a-z][-a-z0-9]{0,48}; template needs 1-10 containers with images, one container may declare ports, PORT/K_SERVICE/K_REVISION/K_CONFIGURATION env are reserved; creates revision 1 and returns a pending operation that resolves once the container accepts connections (or the runtime is unavailable). validate_only returns a done operation without storing."}
  GetService: {wire: ok, errors: ok, state: ok, persist: n/a, note: "uri/urls are http://<service>-<hash>-<region>.localhost:<ingress port>; traffic_statuses resolve LATEST to latest_ready_revision."}
  ListServices: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged via pkgs/page; show_deleted is ignored because deletes are immediate."}
  UpdateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask takes top-level mutable fields (nested paths replace the whole top-level field); empty mask copies every populated mutable field; allow_missing creates. A changed template adds a revision and a pending operation; traffic-only changes add none and resolve at once. Traffic targets are validated (sum 100, known revisions, unique tags); revisions left without traffic are scaled to zero and restart on demand."}
  DeleteService: {wire: ok, errors: ok, state: ok, persist: ok, note: "Removes the service and all its revisions, stops containers; etag mismatch is FAILED_PRECONDITION."}
  GetRevision: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListRevisions: {wire: ok, errors: ok, state: ok, persist: n/a}
  DeleteRevision: {wire: ok, errors: ok, state: ok, persist: ok, note: "A revision that receives traffic or is tagged is FAILED_PRECONDITION."}
  CreateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "Defaults task_count 1, timeout 600s, max_retries 3 (cap 10), timeout cap 7d; the operation is done at once."}
  GetJob: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: n/a}
  UpdateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "UpdateJobRequest has no mask: populated mutable fields replace the stored ones and generation increments."}
  DeleteJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "Cancels running executions and removes executions and tasks."}
  RunJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "Creates an Execution with task_count tasks and runs them with the job's parallelism in real containers (CLOUD_RUN_JOB, CLOUD_RUN_EXECUTION, CLOUD_RUN_TASK_INDEX, CLOUD_RUN_TASK_ATTEMPT, CLOUD_RUN_TASK_COUNT env). The operation resolves with the final Execution when every task is terminal. Overrides: container args/clear_args/env, task_count, timeout. Exit status comes from the runtime; non-zero or timeout retries up to max_retries."}
  GetExecution: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListExecutions: {wire: ok, errors: ok, state: ok, persist: n/a}
  DeleteExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "A running execution is cancelled first."}
  CancelExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "Stops task containers and returns once the execution is complete; cancelled_count counts the stopped and never-started tasks. An already-completed execution is FAILED_PRECONDITION (not verified against the live service)."}
  GetTask: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListTasks: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Ordered by index."}
families:
  operations: {status: ok, note: "google.longrunning.Operations (Get, List, Wait, Cancel, Delete) is served on the same gRPC listener by pkgs/gcplro. Service deploys and RunJob are pending until their work finishes; pollers (the client's Wait) see the final state. WaitOperation does not block on a pending operation. Operations persist; pending ones are aborted on restore."}
  execution: {status: ok, note: "Workloads run through pkgs/container (Docker or Podman, CONTAINER_RUNTIME). A service container gets PORT, K_SERVICE, K_REVISION, K_CONFIGURATION, is published on a loopback port from the shared pool and must accept TCP connections within --gcp-run-startup-timeout. Jobs need a runtime exposing exit status (DockerRuntime.WaitExit)."}
  ingress: {status: ok, note: "A second listener (--gcp-run-ingress-port, default 8127) routes by Host: <service>-<hash>-<region>.localhost[:port] reverse-proxies to the revision chosen from the traffic split by a rotating 100-slot counter; <tag>---<host> routes to the tagged revision. *.localhost resolves to loopback in curl and browsers; otherwise send the Host header to 127.0.0.1:8127. 404 unknown host, 503 no ready revision or no runtime, 502 container unreachable."}
  runtime_unavailable: {status: ok, note: "With --gcp-run-engine=none or no reachable daemon, services deploy and report Ready with a message but serve 503; RunJob executions complete with every task failed ('container runtime is unavailable')."}
  routing_isolation: {status: ok, note: "Own gRPC server bound in StartWorker to --gcp-run-port / GCP_RUN_PORT (default 8126, fails fast if taken) plus the ingress port; never registered on the shared AWS router. cli.go reserves both ports in the shared PortAlloc pool. The official client has no emulator variable: use option.WithEndpoint with insecure credentials."}
  persistence: {status: partial, note: "Services, revisions, jobs, executions, tasks and operations persist as protojson. Containers and in-flight work do not: interrupted executions fail with a restart message, interrupted deployments fail, and a restored revision starts its container again on its first request."}
  auth: {status: partial, note: "No request is authenticated or authorised; invoker_iam_disabled, ingress and binary_authorization are stored but not enforced."}
  iam: {status: gap, note: "Services/Jobs GetIamPolicy, SetIamPolicy and TestIamPermissions are not served (gRPC UNIMPLEMENTED)."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; log_uri is empty. Service container stdout/stderr and ingress requests are written to Cloud Logging when gcplogging is enabled (internal/app wiring): logs run.googleapis.com/stdout|stderr|requests on a cloud_run_revision resource (service_name, revision_name, location, configuration_name, project_id), instanceId label, JSON lines become jsonPayload with severity/trace/spanId/labels/sourceLocation lifted like the structured-logging agent. gRPC calls are not fed to the shared telemetry operation metrics."}
items_still_open:
  - "Cloud Run job task output is not written to Cloud Logging (cloud_run_job resource); only service containers and ingress requests are. The Docker log stream is covered by a unit test of DockerRuntime.StreamLogs, not a live daemon run."
  - "Services/Jobs IAM policy RPCs, WorkerPools, Instances, Builds and Locations are not served."
  - "Only the ingress container (the one declaring ports, else the first) of a revision and only the first container of a task are run; sidecars, volumes, probes, scaling, VPC access, secrets and resource limits are stored but not applied."
  - "Revision scaling is one container per revision (no autoscaling, max_instance_request_concurrency or min_instances); idle revisions are stopped when they leave the traffic split."
  - "Deleted services, jobs and executions are removed at once: show_deleted lists nothing and there is no expire_time soft-delete window."
  - "The URI is an http .localhost address, not https://*.run.app."
  - "update_mask nested paths replace the whole top-level field."
leaks: {status: clean, note: "Background resources are the two servers, deployment and execution goroutines and the containers they own; Shutdown cancels the run context, joins them and removes every container (goleak in TestMain)."}
---

Initial implementation (2026-10-19): google.cloud.run.v2 Services, Revisions, Jobs, Executions and Tasks verified against cloud.google.com/go/run v1.23.0 through the official client in the package tests, using an in-process fake container runtime (published-port HTTP servers for services, gated exit codes for jobs). A Docker-backed run has not been exercised in CI.
