---
service: ecs
sdk_module: aws-sdk-go-v2/service/ecs@v1.88.0
last_audit_commit: fd9a0877
last_audit_date: 2026-07-23
overall: A
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "added capacityProviders/defaultCapacityProviderStrategy/tags at creation (previously silently dropped); tags echoed on create response; this sweep: defaultCapacityProviderStrategy now validated (rejects unknown capacity provider names, see PutClusterCapacityProviders note)"}
  DescribeClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "added include=[TAGS] gating (was previously unsupported; tags were never returned)"}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade delete of serviceDeployments fixed (was keyed wrong, silently a no-op); this sweep: also cascade-cleans the resourceTags side-map entry for the cluster itself plus every cascade-deleted service/container-instance (previously a ghost row that could resurrect stale tags on a same-name recreate, or leak permanently for random-ID resources -- see Notes)"}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: defaultCapacityProviderStrategy now validated (rejects unknown capacity provider names)"}
  UpdateClusterSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  PutClusterCapacityProviders: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: defaultCapacityProviderStrategy items are now validated against real (created via CreateCapacityProvider) or FARGATE/FARGATE_SPOT-builtin capacity providers, returning a 400 ClientException for an unknown name instead of silently accepting any string. Same validateCapacityProviderStrategyLocked helper wired into CreateCluster, UpdateCluster, CreateService, UpdateService, RunTask, and CreateTaskSet (all previously unvalidated too -- CreateCluster/UpdateCluster/CreateTaskSet were not even named in the prior sweep's gap description). Scoped narrowly: only strategy items are validated, not the separate capacityProviders association list (matches the gap's original scope; validating that list too is a larger, higher-blast-radius change not attempted this sweep)."}
  RegisterTaskDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTaskDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "include=[TAGS] already supported pre-sweep"}
  DeregisterTaskDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTaskDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: also cleans the resourceTags side-map entry per deleted revision (previously a permanent ghost row, see Notes)"}
  ListTaskDefinitions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTaskDefinitionFamilies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "now records a real ServiceDeployment for the initial PRIMARY deployment (was a disguised stub, see gaps/fixes); this sweep: capacityProviderStrategy now validated (see PutClusterCapacityProviders note)"}
  DescribeServices: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateService: {wire: ok, errors: ok, state: ok, persist: ok, note: "now syncs ServiceDeployment records when rotating the PRIMARY deployment; this sweep: capacityProviderStrategy now validated (see PutClusterCapacityProviders note)"}
  DeleteService: {wire: ok, errors: ok, state: ok, persist: ok, note: "now cleans up its ServiceDeployment records (was leaking one entry per deleted service); this sweep: also cleans its resourceTags side-map entry (previously a ghost row, see Notes)"}
  ListServices: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServicesByNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTaskSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: added capacityProviderStrategy (was entirely absent from both CreateTaskSetInput and the TaskSet wire shape -- a real SDK field, now validated + stored + echoed) and tags (stored via the resourceTags side map, echoed unconditionally on Create like CreateCluster)"}
  DeleteTaskSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: also cleans the task set's resourceTags side-map entry (previously a permanent ghost row for every tagged task set ever deleted, see Notes)"}
  DescribeTaskSets: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: added include=[TAGS] gating (tags previously had no wire-shape field at all) and capacityProviderStrategy in the response (see CreateTaskSet note)"}
  UpdateTaskSet: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateServicePrimaryTaskSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeServiceRevisions: {wire: ok, errors: ok, state: ok, persist: ok, note: "derived on read from Service.Deployments, not separately stored — intentional (see Notes)"}
  DescribeServiceDeployments: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised stub: filtered a map only the AddServiceDeploymentInternal test seed ever populated. Fixed by syncServiceDeploymentsLocked."}
  ListServiceDeployments: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as DescribeServiceDeployments"}
  StopServiceDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix; also now really has data to stop"}
  ContinueServiceDeployment: {wire: ok, errors: ok, state: partial, persist: n/a, note: "NEW op (was entirely unimplemented / absent from GetSupportedOperations). Lifecycle hooks (blue/green PAUSE stages) are not modeled, so every call returns an honest ClientException that no paused hook exists, after real ARN/hookId validation — never a fabricated success. See gaps."}
  RunTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: added capacityProviderStrategy input (was entirely absent from RunTaskInput -- a real SDK field, now validated) and capacityProviderName output on Task (real SDK field; this backend does not model AWS's weight/base task-distribution algorithm across multiple providers in a strategy, so it always selects the first entry -- documented simplification, not a stub, see Task.CapacityProviderName doc comment in models.go)"}
  StartTask: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTasks: {wire: ok, errors: ok, state: ok, persist: ok}
  StopTask: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTasks: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterContainerInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterContainerInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: also cleans the container instance's resourceTags side-map entry (previously a ghost row, see Notes)"}
  DescribeContainerInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: added include=[TAGS] gating (tags previously had no wire-shape field at all). Remaining gap: CONTAINER_INSTANCE_HEALTH include value / HealthStatus field not modeled -- no health-check state is tracked for container instances (niche, not in the original gap list, deferred)"}
  ListContainerInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContainerInstancesState: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContainerAgent: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCapacityProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCapacityProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCapacityProviders: {wire: ok, errors: ok, state: ok, persist: ok, note: "unknown names report a Failures[] entry (reason MISSING) instead of failing the whole call (fixed prior sweep). This sweep: added include=[TAGS] gating (tags were previously always returned regardless of Include) and the Cluster filter parameter (only capacity providers associated with the named cluster are returned; unknown cluster -> empty result, matching AWS filter-parameter semantics rather than a hard 404)."}
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
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "resourceTags side map was NOT in backendSnapshot at all — fixed, see gaps/fixes. This sweep: fixed a real bug where TagResource on an Express Gateway Service ARN silently never became visible on Describe or ListTagsForResource (see ExpressGatewayService notes below). A similar disconnect exists for ordinary Service ARNs (Service.Tags is a creation-time-only snapshot, never synced with resourceTags) -- found this sweep, NOT fixed (higher blast radius given how central Service is to the test suite; see gaps list)."}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateExpressGatewayService: {wire: partial, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: tags supplied at creation are now also mirrored into the resourceTags side map (previously only stored on the ExpressGatewayService.Tags struct field, never synced -- see Notes for the TagResource-invisibility bug this caused). REMAINING GAP found this sweep (not fixed, see gaps list): CreateExpressGatewayServiceInput is missing real SDK fields Cpu, Memory, HealthCheckPath, NetworkConfiguration, PrimaryContainer, ScalingTarget, TaskDefinitionArn, TaskRoleArn entirely -- this backend only models ExecutionRoleArn/InfrastructureRoleArn/Cluster/ServiceName/Tags."}
  DeleteExpressGatewayService: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: also cleans the service's resourceTags side-map entry (previously a ghost row, see Notes)"}
  DescribeExpressGatewayService: {wire: partial, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: added include=[TAGS] gating (tags were previously always returned regardless of Include) and tags now read from the resourceTags side map (kept in sync by TagResource/UntagResource) instead of a stale creation-time snapshot -- see Notes. REMAINING GAP found this sweep (not fixed): the real DescribeExpressGatewayServiceOutput.Service (types.ECSExpressGatewayService) also carries ActiveConfigurations, CurrentDeployment, and UpdatedAt, none of which this backend models (no deployment/config-revision tracking exists for Express services at all -- config-only like the CreateExpressGatewayService gap)."}
  UpdateExpressGatewayService: {wire: partial, errors: ok, state: ok, persist: ok, note: "tags now read from the resourceTags side map (see DescribeExpressGatewayService note; Update itself never accepted a tags parameter, matching real UpdateExpressGatewayServiceInput, which has no Tags field). REMAINING GAP found this sweep (not fixed): UpdateExpressGatewayServiceInput is missing real SDK fields Cpu, Memory, HealthCheckPath, NetworkConfiguration, PrimaryContainer, ScalingTarget, TaskDefinitionArn, TaskRoleArn -- this backend only supports updating ExecutionRoleArn/InfrastructureRoleArn."}
  DiscoverPollEndpoint: {wire: ok, errors: ok, state: ok, persist: n/a}
  SubmitAttachmentStateChanges: {wire: ok, errors: ok, state: ok, persist: ok}
  SubmitContainerStateChange: {wire: ok, errors: ok, state: ok, persist: ok}
  SubmitTaskStateChange: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  daemon: {status: partial, note: "Field-diffed for real this sweep (previous ledger entries for this family were no-stub-only assessments, not wire-shape diffs -- see Notes for the full field-diff writeup). Found and FIXED a real leak: DeleteDaemon never cleaned up daemonRevisions/daemonDeployments rows at all (only the daemons table entry), and the cluster-purge cleanup path (purgeDaemonsLocked) deleted from daemonRevisions by the wrong key (DaemonArn instead of DaemonRevisionArn, a documented-but-never-fixed no-op preserved through a prior mechanical refactor) -- both fixed via a new shared deleteDaemonAncillaryLocked helper. REMAINING GAP (not fixed, downgrades this family from ok to partial): the wire shape is wrong. Real AWS's DescribeDaemonOutput.Daemon (types.DaemonDetail) exposes only ClusterArn/CreatedAt/CurrentRevisions[]/DaemonArn/DeploymentArn/Status/UpdatedAt -- fields like daemonName, daemonTaskDefinitionArn, capacityProviderArns, tags, propagateTags, enableECSManagedTags/enableExecuteCommand live on the separate, per-revision DaemonRevision/DaemonRevisionDetail types (fetched via DescribeDaemonRevisions), not flat on the Daemon object. This backend's Daemon struct instead flattens all of that directly onto a single object returned by DescribeDaemon/ListDaemons, which does not match AWS's nested revision-based wire shape. Fixing this is a substantial data-model rewrite (mirroring how TaskDefinition revisions already work) touching daemon.go/handler_daemon.go (~1500 LOC combined) and every daemon test file -- out of scope for this sweep; flagged as a genuine gap rather than reclassified to ok. See items_still_open in the sweep return receipt."}
gaps:
  - "TagResource on a Service ARN is invisible on DescribeServices/ListServices and on ListTagsForResource for creation-time tags: Service.Tags is a create-time-only snapshot never synced with the resourceTags side map that TagResource/UntagResource/ListTagsForResource actually read and write. Found this sweep via the same pattern that was just fixed for ExpressGatewayService (see TagResource ops-table note and Notes) -- Service was NOT re-checked at the time of that fix. Not fixed this sweep: Service is the most heavily tested resource in this package and enrichService/resolveTaskTags (RunTask's propagateTags=SERVICE path) both read svc.Tags directly, so switching the source of truth to resourceTags needs careful handling of the tag-propagation-to-tasks path to avoid a regression; higher blast radius than the analogous ExpressGatewayService fix. File a follow-up bd issue before the next ecs sweep."
  - "DescribeServices/ListServices do not support include=[TAGS] gating at all (no Include parameter in the wire shape); tags are always returned unconditionally, unlike DescribeClusters/DescribeCapacityProviders/DescribeContainerInstances/DescribeTaskSets which now correctly gate on Include. Found this sweep during the Service tags field-diff above. Not fixed: adding Include gating to DescribeServices while every other existing test in this package assumes always-on tags is a larger, more invasive change than the narrower TagResource-sync bug it's tangled with; needs its own dedicated pass."
  - "CreateExpressGatewayService/UpdateExpressGatewayService/DescribeExpressGatewayService are missing real SDK fields: Cpu, Memory, HealthCheckPath, NetworkConfiguration, PrimaryContainer, ScalingTarget, TaskDefinitionArn, TaskRoleArn (create/update input), and ActiveConfigurations/CurrentDeployment/UpdatedAt (describe output). This backend only models ExecutionRoleArn/InfrastructureRoleArn/Cluster/ServiceName/Tags -- a real, substantial wire-shape gap found via field-diff this sweep (the prior ledger marked all four Express Gateway ops fully 'ok', which undersold it: no prior sweep had field-diffed this newer feature's config surface). Not fixed: ActiveConfigurations/CurrentDeployment in particular imply a deployment/config-revision tracking model that does not exist anywhere in this backend for Express services (config-only, same category as the pre-existing ECS->ELB and ECS->ASG cross-service gaps below). The simpler scalar fields (Cpu/Memory/HealthCheckPath/TaskDefinitionArn/TaskRoleArn) are a smaller, tractable follow-up; the nested types (NetworkConfiguration/PrimaryContainer/ScalingTarget) and the deployment-tracking fields are a larger effort."
  - "PutClusterCapacityProviders/CreateService/UpdateService/RunTask/CreateCluster/UpdateCluster/CreateTaskSet do not validate that the *association* list itself (capacityProviders, as opposed to a capacityProviderStrategy item) references real capacity providers -- e.g. PutClusterCapacityProviders(capacityProviders=[\"typo-cp\"]) is accepted. FIXED this sweep for capacityProviderStrategy *items* specifically (see PutClusterCapacityProviders note); the separate capacityProviders association-list gap is unchanged from the prior sweep's assessment and intentionally not fixed for the same reason (many call sites, tests using ad-hoc provider names in the association list specifically)."
  - "SDK bumped v1.86.2 -> v1.88.0 last sweep (no local services/ecs/ drift; SDK-only, re-confirmed unchanged this sweep). New surface: ServiceRevision.Overrides -> ServiceRevisionOverrides.RuntimePlatform (types.RuntimePlatformOverride, CpuArchitecture only) — an output-only field AWS populates when it auto-detects an architecture mismatch during an ECS Express deployment (doc: \"You can't set this value\"). Not modeled (DescribeServiceRevisions never populates Overrides); no client-visible regression since the field is optional/omitempty and no test or codepath claims architecture-mismatch detection. Niche, deferred."
  - "ContinueServiceDeployment always returns ClientException (no paused lifecycle hook) because PAUSE-stage lifecycle hooks for blue/green deployments are not modeled at all (no hookId tracking, no pause state in the ECS_SERVICE_DEPLOYMENT / EXTERNAL deployment controllers). Implementing real hook pausing is a substantial feature (Lambda-invocation simulation, TEST_TRAFFIC_SHIFT/BAKE_TIME lifecycle stages) out of scope for this sweep; the op is real (validates ARN/hookId, returns AWS-shaped errors) rather than a stub. Re-verified unchanged this sweep."
  - "ECS -> ELB/ELBv2 target registration is config-only: Service.LoadBalancers/ServiceRegistries are stored and echoed back on Describe/Update, but nothing calls services/elbv2 to register/deregister targets in a target group, and ELB health does not feed back into ECS task/service health. Cross-service, lives outside services/ecs/ — reported, not fixed. No bd issue found for this in the tracker at time of writing; recommend filing one scoped to services/elbv2 + services/ecs integration."
  - "ECS -> Auto Scaling Group capacity providers are config-only: AutoScalingGroupProvider (ARN, ManagedScaling, ManagedTerminationProtection, ManagedDraining) is stored/echoed but never calls services/autoscaling to validate the ASG exists or to actually scale it in response to managed-scaling target utilization. Cross-service, lives outside services/ecs/ — reported, not fixed."
deferred:
  - "Daemon* operation family (CreateDaemon..UpdateDaemon, 12 ops) — field-diffed for real this sweep; see families.daemon above for the full writeup (leak fixed, wire-shape gap found and remains open)."
  - "docker_runner.go / real container lifecycle (vs NoopRunner) — re-audited this sweep. Reviewed RunTask (pull/create/start with rollback-on-failure via rollbackContainers, only registers the task's containers in the tracking map after every container in the task started successfully) and StopTask (snapshots container IDs under lock, stops/removes outside the lock, retains only failed-to-stop IDs for retry). No stubs, no goroutine or container-tracking-map leaks found: a task that fails mid-RunTask is fully rolled back before ever being added to r.containers, so there is no leaked entry for it to begin with. No changes needed."
  - "Full ServiceDeployment wire-shape parity (LifecycleStage, SourceServiceRevisions, TargetServiceRevision, Rollback, DeploymentCircuitBreaker, Alarms sub-objects) — the emulator's ServiceDeployment type covers only ServiceDeploymentArn/ClusterArn/ServiceArn/Status/StatusReason/CreatedAt/UpdatedAt. Re-verified unchanged this sweep: correctly populated for every real deployment, but the richer blue/green fields are not modeled (same underlying reason ContinueServiceDeployment is deferred -- blue/green lifecycle is not modeled at all in this backend)."
leaks: {status: clean, note: "Prior 'found' status was stale documentation -- that leak (DeleteService's ServiceDeployment-map entry) was already fixed in the same prior sweep that wrote the note; the status field just never got flipped back to clean. Re-verified clean this sweep. Two NEW leaks found and fixed this sweep: (1) DeleteDaemon never cleaned up daemonRevisions/daemonDeployments rows, and purgeDaemonsLocked deleted from daemonRevisions by the wrong key so it silently matched nothing -- both fixed via deleteDaemonAncillaryLocked. (2) resourceTags side-map ghost rows were never cleaned up on delete for clusters/services/container-instances/task-sets/task-definitions/express-gateway-services -- fixed via deleteResourceTagsLocked. See Notes for full writeup and proof tests. Reconciler, janitor, lifecycle stepper, and docker_runner (re-audited this sweep) remain clean."}
---

## Notes

### 2026-07-23 re-audit (badges-automation branch, commit fd9a0877)

Scope: worked every item in the prior sweep's `gaps`/`deferred` lists, per
the parity-3 campaign brief for this service. `git diff 95dfa093..HEAD --
services/ecs/` showed zero local drift before this sweep's own changes
(consistent with the prior sweep's finding that the ecs SDK bump was the
only prior-prior change). No `//nolint:cyclop|gocyclo|gocognit|funlen` existed
before or after this sweep.

**Fixed: capacity provider strategy validation** (prior gap #1). Added
`validateCapacityProviderStrategyLocked` (capacity_providers.go): rejects any
`capacityProviderStrategy` item referencing a name that is neither a created
`CreateCapacityProvider` provider nor the FARGATE/FARGATE_SPOT builtins, with
a 400 `ClientException`. Wired into `CreateCluster`, `UpdateCluster`,
`PutClusterCapacityProviders`, `CreateService`, `UpdateService`, `RunTask`,
and `CreateTaskSet`. Field-diffing this gap surfaced two real, previously
undocumented wire-shape holes: `RunTaskInput` had **no**
`capacityProviderStrategy` field at all (real `ecs.RunTaskInput` has one; a
client could never actually set one via RunTask, so the "does not validate"
framing understated the prior gap -- the field didn't exist to validate), and
likewise for `CreateTaskSetInput`/`TaskSet` (real `ecs.CreateTaskSetInput`
and `types.TaskSet` both have `CapacityProviderStrategy`). Added the field to
both, plus `Task.CapacityProviderName` (the real SDK's per-task resolved-
provider output field -- this backend does not model AWS's weight/base
distribution algorithm across multiple strategy providers, so it always
selects the first entry; documented as a simplification, not a stub).
Scoped deliberately narrow: only strategy *items* are validated, not the
separate `capacityProviders` association list (unchanged gap, still listed).
Proven by `capacity_provider_strategy_validation_test.go` (new file:
`TestCapacityProviderStrategy_RejectsUnknownProvider` table-tests all seven
call sites, `TestCapacityProviderStrategy_AcceptsCreatedProvider`,
`TestRunTask_CapacityProviderStrategy_SetsCapacityProviderName`) plus new
cases in `handler_task_sets_test.go`
(`TestCreateTaskSet_CapacityProviderStrategy_Roundtrip`).

**Fixed: `include=[TAGS]` gating** (prior gap #2) for `DescribeCapacityProviders`,
`DescribeContainerInstances`, and `DescribeTaskSets`. `ContainerInstance` and
`TaskSet` had no `tags` field in their wire shape at all (real
`types.ContainerInstance`/`types.TaskSet` both have `Tags []Tag`); added the
field to both view types, gated by `Include`, sourced from the existing
`resourceTags` side map via `ListTagsForResource` (same pattern as
`DescribeClusters`). `CapacityProvider`'s `tags` field already existed but
was unconditionally populated regardless of `Include` -- now gated too.
While auditing this gap's four named ops, also found and fixed the same
"always-on tags" bug plus a deeper, previously-undocumented one for
`DescribeExpressGatewayService` (see the `ExpressGatewayService` writeup
below) -- `DescribeExpressGatewayService` was not in the original gap list at
all, but has the exact same real SDK `Include` parameter and the same bug.
Proven by `TestDescribeCapacityProviders_TagsRequireInclude`,
`TestDescribeContainerInstances_TagsRequireInclude`,
`TestDescribeTaskSets_TagsRequireInclude`,
`TestExpressGatewayService_TagResource_VisibleOnDescribe`.

**Fixed: `DescribeCapacityProviders` `Cluster` filter parameter** (prior gap
#3). When `cluster` is set, only capacity providers associated with that
cluster (via `CreateCluster`/`UpdateCluster`/`PutClusterCapacityProviders`)
are returned; an unknown cluster yields an empty result (AWS
filter-parameter semantics), not a 404. Proven by
`TestDescribeCapacityProviders_ClusterFilter` and
`TestDescribeCapacityProviders_ClusterFilter_UnknownCluster`.

**Re-verified unchanged, still accurate** (prior gaps #4-#7): the
`ServiceRevisionOverrides.RuntimePlatform` SDK-bump gap, `ContinueServiceDeployment`'s
honest-ClientException lifecycle-hook gap, and the two cross-service ECS->ELB
/ ECS->ASG config-only gaps. No changes needed; descriptions carried forward
verbatim except for a "re-verified" note.

**Real bug found and FIXED: `ExpressGatewayService` tags were a disguised
stub for `TagResource`.** `ExpressGatewayService.Tags` was populated at
creation and echoed on every Create/Describe/Update call, but was a
completely separate, never-synchronized copy from the `resourceTags` side
map that `TagResource`/`UntagResource`/`ListTagsForResource` actually read
and write -- so `TagResource(expressServiceArn, ...)` "succeeded" (200 OK)
but was invisible on every subsequent read path, and creation-time tags were
invisible to `ListTagsForResource`. Fixed by mirroring `CreateExpressGatewayService`'s
input tags into `resourceTags` (`setResourceTagsLocked`) and making
`DescribeExpressGatewayService`/`UpdateExpressGatewayService` read tags from
`resourceTags` (the now-authoritative source, kept in sync by TagResource)
instead of the stale struct-field snapshot. `DescribeExpressGatewayService`
also gained `Include=[TAGS]` gating in the same pass (see gap #2 above).
Proven end-to-end by `TestExpressGatewayService_TagResource_VisibleOnDescribe`;
did not regress the pre-existing `TestExpressGatewayService_DeepCopy_Tags`
backend-level test (which asserts the returned `Create` snapshot is an
independent deep copy -- still true, since `resourceTags` is seeded
independently at creation time too).

**Real gap found, NOT fixed: identical tags-disconnect bug exists for
`Service`.** Same pattern as the `ExpressGatewayService` bug just fixed:
`Service.Tags` is a creation-time snapshot, never synced with `resourceTags`.
Not fixed this sweep -- `Service` is the most heavily used and tested
resource in this package (`enrichService` and `RunTask`'s
`propagateTags=SERVICE` tag-resolution path both read `svc.Tags` directly),
so switching the source of truth carries materially higher regression risk
than the narrower `ExpressGatewayService` fix. Filed as a new gap; a future
sweep should budget dedicated time for it plus `DescribeServices`
`Include=[TAGS]` gating (also entirely absent -- a second, related gap found
in the same investigation).

**Real gap found via field-diff, NOT fixed: `ExpressGatewayService`
create/update/describe wire shape is substantially incomplete.** Real
`ecs.CreateExpressGatewayServiceInput`/`UpdateExpressGatewayServiceInput`
carry `Cpu`, `Memory`, `HealthCheckPath`, `NetworkConfiguration`,
`PrimaryContainer`, `ScalingTarget`, `TaskDefinitionArn`, `TaskRoleArn`; real
`types.ECSExpressGatewayService` (the Describe/Update output) also carries
`ActiveConfigurations`, `CurrentDeployment`, `UpdatedAt`. This backend models
none of these -- only `ExecutionRoleArn`/`InfrastructureRoleArn`/`Cluster`/
`ServiceName`/`Tags`. The prior ledger marked all four Express Gateway ops
fully `ok`; that was a no-stub assessment, not a field-diff (Express Gateway
is a newer, still-evolving ECS feature area and had apparently never been
diffed against the real SDK types before this sweep). Downgraded
`CreateExpressGatewayService`/`UpdateExpressGatewayService`/`DescribeExpressGatewayService`
from `wire: ok` to `wire: partial`. Not fixed: `ActiveConfigurations`/
`CurrentDeployment` imply a deployment/config-revision tracking model this
backend has nowhere else for Express services (config-only, same shape as
the pre-existing ECS->ELB/ECS->ASG cross-service gaps); the remaining scalar
and nested-type fields are a smaller, tractable follow-up but were not
attempted given the size of everything else in scope this sweep.

**Fixed: two real leaks in the Daemon family**, found while field-diffing it
(see `families.daemon` above for the wire-shape gap that remains open).
(1) `DeleteDaemon` never cleaned up `daemonRevisions`/`daemonDeployments`
rows at all -- only the `daemons` table entry itself was removed, so every
revision ever created via `CreateDaemon`/`UpdateDaemon` and every deployment
ever made leaked permanently once its owning daemon was deleted. (2)
`purgeDaemonsLocked`'s cleanup called `b.daemonRevisions.Delete(d.DaemonArn)`,
but `daemonRevisions` is keyed by `DaemonRevisionArn`, not `DaemonArn` --
this delete could never match anything. This was **already known and
explicitly documented as unfixed** in a code comment ("Preserved
byte-for-byte from the pre-conversion map-based code rather than fixed, per
the Phase 3.3 mechanical-conversion mandate") -- that mandate governed a
prior mechanical refactor PR, not this sweep, which is explicitly chartered
to find and fix leaks. Both fixed via a new shared
`deleteDaemonAncillaryLocked` helper (daemon.go), called from both
`DeleteDaemon` and `purgeDaemonsLocked`. Proven by
`TestDeleteDaemon_CleansRevisionsAndDeployments` and
`TestPurgeCluster_CleansDaemonRevisionsAndDeployments` (both fail without the
fix).

**Fixed: `resourceTags` ghost rows on delete**, a leak class explicitly
called out in this sweep's brief ("no ghost map rows after delete... tags").
`resourceTags` (the side map backing `TagResource`/`UntagResource`/
`ListTagsForResource`) was never cleaned up on delete for *any* resource
type: clusters, services, container instances, task sets, task definitions,
or express gateway services. For deterministic-ARN resources (cluster/
service/container-instance/express-gateway-service ARNs are all derived from
name, not a random ID) this meant a delete-then-recreate cycle with the same
name could resurrect stale tags from a previous incarnation of the resource;
for random-ID resources (task sets) it meant one permanently-leaked map row
per resource ever created and deleted. Fixed via a new shared
`deleteResourceTagsLocked` helper (tags.go), wired into `DeleteCluster`
(direct + cascaded services + cascaded container instances),
`purgeClusterLocked`'s equivalents, `DeleteService`,
`deleteTaskSetsForServiceLocked` (shared by `DeleteService`/`DeleteCluster`/
`purgeClusterLocked`), `DeleteTaskSet`, `DeregisterContainerInstance`,
`DeleteExpressGatewayService`, and `DeleteTaskDefinitions`. (Capacity
providers were checked and excluded: they store tags inline on the
`CapacityProvider` struct, not via `resourceTags`, so they die naturally with
the struct on delete -- no fix needed there. `DeregisterTaskDefinition` and
`DeleteDaemonTaskDefinition` were also checked and excluded: both only flip a
status flag to INACTIVE/DELETED, they never actually remove the record, so
there is nothing to clean up yet.) Proven by
`TestDeleteResource_CleansGhostResourceTags` (cluster + service subtests;
the same `deleteResourceTagsLocked` call sites cover container instances,
task sets, and express gateway services by construction, and are exercised
indirectly by the existing `TestECS_Delete*`/`TestPurge_*` suites, which all
still pass).

**`daemon` family: field-diffed for real this sweep** (prior ledger entries
were no-stub-only). See `families.daemon` above for the full writeup: leak
fixed (above), wire-shape gap found and remains open (downgraded from `ok`
to `partial`, NOT reclassified to `ok`).

**`docker_runner.go`: re-audited, clean, no changes.** See `deferred` above.

### 2026-07-11 re-audit (parity-4 branch, commit 95dfa093)

Re-audit protocol: `git diff ce30166a..HEAD -- services/ecs/` showed **zero
local drift** (the ledger's stated `last_audit_commit` 86c2f9af was not an
ancestor of HEAD — it's a cloudformation commit on an unrelated,
unmerged-at-audit-time branch `parity-sweep-3`; fell back to `ce30166a`, the
commit that actually authored this ledger, as baseline per protocol). The
only change in scope was an SDK bump, `aws-sdk-go-v2/service/ecs`
v1.86.2 -> v1.88.0 (no new/removed operations, no new enums/errors, only
doc-comment rewording plus one output-only field — see gaps list). Audit
therefore focused on the three previously-flagged `partial` rows.

**Fixed: `DescribeCapacityProviders` returned a whole-request 400 error
when *any* requested name/ARN was unknown**, instead of AWS's documented
partial-success behavior (`DescribeCapacityProvidersOutput.Failures
[]types.Failure`). This is the same `Arn`/`Reason: MISSING`/`Detail` pattern
already used correctly by `DescribeClusters`, `DescribeContainerInstances`,
`DescribeTasks`, `DescribeServices`, and `DeleteTaskDefinitions` in this same
package — `DescribeCapacityProviders` was the outlier, and had a test
(`TestECS_DescribeCapacityProviders` "unknown capacity provider returns
400") and a second test (`TestBatch3_CapacityProvider_Unknown_ReturnsError`)
that encoded the wrong behavior as the expected contract. A real client
calling `DescribeCapacityProviders(["my-cp", "typo-cp"])` to bulk-check
several providers would get a total failure instead of `my-cp`'s data plus
a `Failures` entry for `typo-cp` — this is a real behavioral divergence, not
just a missing optional field.

Fixed by changing `InMemoryBackend.DescribeCapacityProviders` (and the
`Backend` interface) from `([]CapacityProvider, error)` to
`([]CapacityProvider, []Failure, error)`, building a `Failure{Reason:
"MISSING"}` entry per unresolved name/ARN (after the existing
FARGATE/FARGATE_SPOT builtin fallback) instead of returning early with
`ErrCapacityProviderNotFound`. `handleDescribeCapacityProviders` now emits
`failures` in the JSON body (was entirely absent from the wire shape before).
Rewrote the two tests that asserted the old (wrong) all-or-nothing behavior
to assert the correct 200+Failures behavior, added a
"mix of known and unknown returns partial success" case. `ErrCapacityProviderNotFound`
is unchanged and still used correctly by `DeleteCapacityProvider` (single-resource
delete, where AWS *does* 400 on not-found) and `UpdateCapacityProvider`.

Files touched: `backend_iface.go`, `backend_new_ops.go`, `handler_new_ops.go`,
`handler_new_ops_test.go`, `handler_batch3_test.go`, `handler_refinement1_test.go`
(2-value call-site updates), `persistence_internal_test.go` (same). ~66 LOC.

Re-verified `PutClusterCapacityProviders` (still no existence validation of
referenced capacity-provider names — deliberate prior-sweep decision, see
gaps list, not re-litigated: fixing it touches `CreateService`/
`UpdateService`/`RunTask` too and risks breaking ad-hoc-named strategies used
throughout the test suite; out of scope for a targeted bug-fix pass) and
`ContinueServiceDeployment` (still an honest, real, ARN/hookId-validating
`ClientException` — no regression, ledger description remains accurate) —
both confirmed unchanged from the prior sweep's assessment.

### Prior sweep (gopherstack-7wu)

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
