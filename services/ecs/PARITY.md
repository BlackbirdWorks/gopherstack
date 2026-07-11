---
service: ecs
sdk_module: aws-sdk-go-v2/service/ecs@v1.86.2
last_audit_commit: 86c2f9af
last_audit_date: 2026-07-05
overall: A
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "added capacityProviders/defaultCapacityProviderStrategy/tags at creation (previously silently dropped); tags echoed on create response"}
  DescribeClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "added include=[TAGS] gating (was previously unsupported; tags were never returned)"}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade delete of serviceDeployments fixed (was keyed wrong, silently a no-op)"}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateClusterSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  PutClusterCapacityProviders: {wire: ok, errors: partial, state: ok, persist: ok, note: "no existence validation of referenced capacity providers (gap, see gaps list)"}
  RegisterTaskDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTaskDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "include=[TAGS] already supported pre-sweep"}
  DeregisterTaskDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTaskDefinitions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTaskDefinitions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTaskDefinitionFamilies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "now records a real ServiceDeployment for the initial PRIMARY deployment (was a disguised stub, see gaps/fixes)"}
  DescribeServices: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "now syncs ServiceDeployment records when rotating the PRIMARY deployment"}
  DeleteService: {wire: ok, errors: ok, state: ok, persist: ok, note: "now cleans up its ServiceDeployment records (was leaking one entry per deleted service)"}
  ListServices: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServicesByNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTaskSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTaskSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTaskSets: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTaskSet: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateServicePrimaryTaskSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeServiceRevisions: {wire: ok, errors: ok, state: ok, persist: ok, note: "derived on read from Service.Deployments, not separately stored — intentional (see Notes)"}
  DescribeServiceDeployments: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised stub: filtered a map only the AddServiceDeploymentInternal test seed ever populated. Fixed by syncServiceDeploymentsLocked."}
  ListServiceDeployments: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as DescribeServiceDeployments"}
  StopServiceDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix; also now really has data to stop"}
  ContinueServiceDeployment: {wire: ok, errors: ok, state: partial, persist: n/a, note: "NEW op (was entirely unimplemented / absent from GetSupportedOperations). Lifecycle hooks (blue/green PAUSE stages) are not modeled, so every call returns an honest ClientException that no paused hook exists, after real ARN/hookId validation — never a fabricated success. See gaps."}
  RunTask: {wire: ok, errors: ok, state: ok, persist: ok}
  StartTask: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTasks: {wire: ok, errors: ok, state: ok, persist: ok}
  StopTask: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTasks: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterContainerInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterContainerInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeContainerInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  ListContainerInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContainerInstancesState: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContainerAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCapacityProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCapacityProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCapacityProviders: {wire: partial, errors: ok, state: ok, persist: ok, note: "no include=[TAGS] gating, no Cluster filter param, no Failures for unknown names (gap)"}
  UpdateCapacityProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAccountSetting: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccountSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAccountSetting: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAccountSettingDefault: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  ExecuteCommand: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetTaskProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTaskProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "resourceTags side map was NOT in backendSnapshot at all — fixed, see gaps/fixes"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateExpressGatewayService: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteExpressGatewayService: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeExpressGatewayService: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateExpressGatewayService: {wire: ok, errors: ok, state: ok, persist: ok}
  DiscoverPollEndpoint: {wire: ok, errors: ok, state: ok, persist: n/a}
  SubmitAttachmentStateChanges: {wire: ok, errors: ok, state: ok, persist: ok}
  SubmitContainerStateChange: {wire: ok, errors: ok, state: ok, persist: ok}
  SubmitTaskStateChange: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  daemon: {status: ok, note: "CreateDaemon/DeleteDaemon/.../UpdateDaemon family unchanged this sweep; deferred re-audit, no evidence of regressions"}
gaps:
  - "PutClusterCapacityProviders/CreateService/UpdateService/RunTask do not validate that a referenced capacityProviderStrategy name is a real (created or FARGATE/FARGATE_SPOT builtin) capacity provider. AWS rejects unknown providers; this backend accepts any string. Not fixed this sweep to limit blast radius (many call sites, risk of breaking existing tests that use ad-hoc provider names). File follow-up bd issue before next ecs sweep."
  - "DescribeCapacityProviders/DescribeContainerInstances/DescribeTaskSets/DescribeExpressGatewayService do not support include=[TAGS] gating (tags are simply never returned by these four; DescribeClusters and DescribeTaskDefinition now/already do). Lower priority: unlike the DescribeClusters gap, no create path lets users set these tags in a way that becomes invisible, since ListTagsForResource still exposes them; it is purely a wire-shape completeness gap."
  - "DescribeCapacityProviders additionally lacks the Cluster filter parameter and per-name Failures (unknown names are silently ok, not a discrete SDK gap but a shape simplification)."
  - "ContinueServiceDeployment always returns ClientException (no paused lifecycle hook) because PAUSE-stage lifecycle hooks for blue/green deployments are not modeled at all (no hookId tracking, no pause state in the ECS_SERVICE_DEPLOYMENT / EXTERNAL deployment controllers). Implementing real hook pausing is a substantial feature (Lambda-invocation simulation, TEST_TRAFFIC_SHIFT/BAKE_TIME lifecycle stages) out of scope for this sweep; the op is real (validates ARN/hookId, returns AWS-shaped errors) rather than a stub."
  - "ECS -> ELB/ELBv2 target registration is config-only: Service.LoadBalancers/ServiceRegistries are stored and echoed back on Describe/Update, but nothing calls services/elbv2 to register/deregister targets in a target group, and ELB health does not feed back into ECS task/service health. Cross-service, lives outside services/ecs/ — reported, not fixed. No bd issue found for this in the tracker at time of writing; recommend filing one scoped to services/elbv2 + services/ecs integration."
  - "ECS -> Auto Scaling Group capacity providers are config-only: AutoScalingGroupProvider (ARN, ManagedScaling, ManagedTerminationProtection, ManagedDraining) is stored/echoed but never calls services/autoscaling to validate the ASG exists or to actually scale it in response to managed-scaling target utilization. Cross-service, lives outside services/ecs/ — reported, not fixed."
deferred:
  - "Daemon* operation family (CreateDaemon..UpdateDaemon, 12 ops) — not re-audited this pass; backend_daemon.go (787 LOC) untouched, no evidence found of regressions while auditing adjacent code (persistence/tag/service-deployment map ownership)."
  - "docker_runner.go / real container lifecycle (vs NoopRunner) — not re-audited this pass."
  - "Full ServiceDeployment wire-shape parity (LifecycleStage, SourceServiceRevisions, TargetServiceRevision, Rollback, DeploymentCircuitBreaker, Alarms sub-objects) — the emulator's ServiceDeployment type covers only ServiceDeploymentArn/ClusterArn/ServiceArn/Status/StatusReason/CreatedAt/UpdatedAt. Now correctly populated for every real deployment (this sweep's fix), but the richer blue/green fields are not modeled."
leaks: {status: found, note: "DeleteService leaked one ServiceDeployment map entry per deleted service (unbounded growth under create/delete churn) because nothing ever populated serviceDeployments in production before this sweep, so the leak was latent/inert. Fixed alongside the disguised-stub fix (deleteServiceDeploymentsForServiceLocked, called from DeleteService/DeleteCluster/purgeClusterLocked). Reconciler (per-cluster launch semaphores), janitor (stopped-task TTL sweep), and lifecycle stepper were independently re-verified this sweep and are clean: ctx-cancelled tickers, EvictCluster hook releases semaphores on cluster delete, tasksByInstance/taskProtections/lifecycle entries are cleaned up on both the fast and delayed stop paths."
---

## Notes

Freeform findings from this sweep (gopherstack-7wu), for the next auditor.

### Severe, fixed this sweep

1. **`Restore()` never rebuilt `serviceIndex`.** `getServicesForReconciler`
   (backend.go) is the *only* feed for the deployment reconciler
   (reconciler.go) and reads the flat `serviceIndex` map with **no linear-scan
   fallback** (unlike `tasksByInstance`, which `enrichContainerInstance`
   explicitly falls back to scanning for — see the comment there). `Restore`
   loaded `b.services` from the snapshot but never repopulated
   `b.serviceIndex`, so every service that existed at snapshot time became
   permanently invisible to the reconciler after a restore/restart: desired-count
   convergence, scale up/down, and circuit-breaker evaluation would silently
   stop forever for pre-existing services. Fixed in `persistence.go` `Restore`
   by rebuilding `serviceIndex` (and, for consistency/performance,
   `tasksByInstance`) from the restored maps. Proven by
   `Test_Restore_RebuildsServiceIndex` (persistence_internal_test.go), which
   fails without the fix (reconciler `RunOnce` after restore launches zero
   tasks for a service with `DesiredCount > 0`).

2. **`resourceTags` side map was entirely absent from `backendSnapshot`.**
   Clusters and Services carry `Tags` inline on their own struct, but task
   definitions and daemon task definitions are tagged only through the
   `TagResource`/`UntagResource`/`ListTagsForResource` side map
   (`b.resourceTags`, keyed by `resourceTagKey(arn)`). That map was never
   included in `Snapshot()`/`Restore()`, so every tag applied via
   `TagResource` on a task definition silently vanished across a
   snapshot/restore cycle. Fixed by adding `ResourceTags` to
   `backendSnapshot` with proper deep-copy on snapshot and restore. Proven by
   `Test_Snapshot_Restore_PreservesResourceTags`.

3. **`DescribeServiceDeployments`/`ListServiceDeployments`/
   `StopServiceDeployment` were disguised stubs** (parity-principles.md rule
   4: "a real-looking op filtering a never-populated map is a disguised
   stub"). `b.serviceDeployments` was only ever written by the
   `AddServiceDeploymentInternal` test-seed helper — no real `CreateService`,
   `UpdateService`, or circuit-breaker rollback path ever created an entry.
   A real client following the documented `CreateService` ->
   `ListServiceDeployments` -> `DescribeServiceDeployments` workflow always
   got an empty result, even though the service had an active PRIMARY
   deployment tracked in `Service.Deployments`. Fixed by
   `syncServiceDeploymentsLocked`/`recordServiceDeploymentLocked`
   (backend_new_ops.go), called from `CreateService`, `UpdateService`, and
   `evaluateCircuitBreakerLocked` (deployment.go, covers both the rollback and
   halt-without-rollback branches). `ServiceDeploymentArn` is derived
   deterministically from the service ARN + `Deployment.ID`
   (`serviceDeploymentArnFor`, mirroring the existing `serviceRevisionArnFor`
   pattern for `arn:...:service-revision/...`). Proven end-to-end via HTTP in
   `TestECS_ServiceDeployments_RealDeploymentsAreVisible`.

   This uncovered a **second, previously-latent bug**: the cascade-delete
   code in `DeleteCluster` and `purgeClusterLocked` did
   `delete(b.serviceDeployments, svc.ServiceArn)` — deleting by
   `ServiceArn` used as a *map key*, but the map is keyed by
   `ServiceDeploymentArn`. This was silently a no-op before (the map was
   always empty in practice), but once real entries started flowing in it
   would have **leaked one entry per deleted service forever** (also true of
   plain `DeleteService`, which never even attempted cleanup). Fixed with a
   shared `deleteServiceDeploymentsForServiceLocked(serviceArn)` helper
   (matches by the `.ServiceArn` *field*) wired into `DeleteCluster`,
   `DeleteService`, and `purgeClusterLocked`. `TestDeleteCluster_
   CascadesServiceDeployments` encoded the old (wrong) key convention — it
   injected a fake entry keyed by `svc.ServiceArn` with the `.ServiceArn`
   field left blank, which only ever passed because of the bug. Rewritten to
   assert against the real auto-created deployment (via `ListServiceDeployments`)
   plus a correctly-keyed injected extra entry. New:
   `TestECS_ServiceDeployments_DeletedOnServiceDelete`.

### Moderate, fixed this sweep

4. **`CreateCluster` silently dropped `capacityProviders`,
   `defaultCapacityProviderStrategy`, and `tags`** — all three are real
   `CreateClusterInput` fields in aws-sdk-go-v2. Terraform's `aws_ecs_cluster`
   resource sets `tags` at creation time (no fallback `TagResource` call), so
   this was a real, silent tag-loss bug for the most common IaC flow, not just
   a theoretical gap. Fixed: `CreateClusterInput`/`createClusterInput` accept
   all three; `CreateCluster` stores capacity-provider fields directly and
   tags via a new `setResourceTagsLocked` helper (extracted from `TagResource`
   so it can be called while the write lock is already held, avoiding a
   self-deadlock on `lockmetrics.RWMutex`). `DescribeClusters` gained
   `include=["TAGS"]` support (previously unsupported — tags were never
   returned by Describe regardless of the wire shape technically supporting
   `tags,omitempty`). CreateCluster's own response always echoes back the
   tags it was just given (matches: no `include` gating exists on Create).
   Proven by `TestECS_CreateCluster_TagsAndCapacityProviders`.

5. **`ContinueServiceDeployment` was entirely unimplemented** — absent from
   `GetSupportedOperations()`/`buildOps()` and carried an explicit
   acknowledged gap in `sdk_completeness_test.go`. Since this backend does not
   model blue/green lifecycle-hook pause stages at all, a full implementation
   (Lambda-invocation simulation, `TEST_TRAFFIC_SHIFT`/`BAKE_TIME` stages) was
   out of scope; instead the op is now real-but-honest: validates
   `serviceDeploymentArn` and `hookId` are present, looks up the deployment
   (404 `ServiceDeploymentNotFoundException` if missing), validates `action`
   is `CONTINUE`/`ROLLBACK`/omitted, and returns `ClientException` reporting
   that no such lifecycle hook is currently paused — never a fabricated
   success. Removed from the `sdk_completeness_test.go` acknowledged-gap list
   since it is now routed. Proven by `TestECS_ContinueServiceDeployment`
   (three cases: real deployment/no hook, deployment not found, missing
   hookId).

### Verified accurate / traps for the next auditor

- `enrichContainerInstance`'s `tasksByInstance` fallback-to-linear-scan
  comment ("e.g. after restore") shows a prior sweep already reasoned about
  the post-restore-index-empty case for that specific map — but the parallel
  `serviceIndex` consumer (`getServicesForReconciler`) had no such fallback
  and no comment acknowledging it. Don't assume one documented index-rebuild
  concern means all of them were considered.
- `enrichService`'s `RolloutState -> COMPLETED` transition is computed
  transiently on every `DescribeServices`/`enrichService` call under an
  RLock and is **not** written back into the stored `Service.Deployments` —
  this is intentional/pre-existing (matches the `DescribeServiceRevisions`
  "derive on read" pattern, see `addServiceRevisionLocked`'s doc comment). Do
  not flag this as a persistence bug; it's a deliberate simplification. Note:
  the new `ServiceDeployment.Status` this sweep added inherits the same
  limitation — it snapshots `RolloutState` at deployment-creation/rollback
  time and will not itself flip to `SUCCESSFUL` once the deployment
  converges, since nothing re-invokes `syncServiceDeploymentsLocked` on the
  read path. Consistent with existing precedent, not a regression, but a
  known simplification worth closing in a future sweep if `DescribeServiceDeployments`
  status accuracy becomes load-bearing for a test.
- ECS deployment-circuit-breaker threshold math
  (`circuitBreakerThreshold`/`deployment.go`) matches AWS's documented
  floor-3/ceiling-200/half-of-desired-count formula exactly — verified
  against https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-circuit-breaker.html,
  no changes needed.
- `builtinCapacityProvider` correctly synthesizes FARGATE/FARGATE_SPOT
  without requiring explicit `CreateCapacityProvider` calls, matching AWS
  (these are AWS-managed, always-available providers) — verified, no gap.
- Reconciler/janitor/lifecycle-stepper goroutine and ticker hygiene was
  re-verified this sweep (ctx-cancelled loops, `EvictCluster` cluster-delete
  hook releasing per-cluster semaphores, task-protection/lifecycle/
  tasksByInstance cleanup on both the fast and stop-delay paths) — all clean,
  no changes needed.

### Cross-service / out-of-scope (reported, not fixed — services/ecs/ only)

- ECS -> ELB/ELBv2 target registration: config-only (`Service.LoadBalancers`
  stored/echoed, never registers/deregisters targets in `services/elbv2`).
  Matches the task brief's "known gap" — confirmed still present, no bd issue
  found referencing it in the tracker at audit time.
- ECS -> Auto Scaling Group capacity providers: config-only
  (`AutoScalingGroupProvider` stored/echoed, never calls `services/autoscaling`
  to validate the ASG or drive managed scaling).
- Repo-wide (unrelated to ecs): at `last_audit_commit` the root package fails
  `go build ./...` with two `cwBk.PutMetricData` call-site mismatches in
  `cli.go` (lines 3220, 4259) — a pre-existing break from a concurrent
  CloudWatch-service sweep on this shared branch, confirmed unrelated to any
  ecs change (`cli.go` has zero diff in this sweep) and out of scope
  (services/ecs/ only, shared file). `go build ./services/ecs/...` and
  `go build ./...` excluding the root package both pass clean.
