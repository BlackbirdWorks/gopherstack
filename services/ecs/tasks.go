package ecs

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	platformVersionLatest = "LATEST"
	platformVersion140    = "1.4.0"
	platformVersion130    = "1.3.0"
	platformVersion120    = "1.2.0"
	platformVersion110    = "1.1.0"
	platformVersion100    = "1.0.0"
)

// isValidFargatePlatformVersion reports whether pv is a known Fargate platform version.
func isValidFargatePlatformVersion(pv string) bool {
	switch pv {
	case platformVersionLatest,
		platformVersion140,
		platformVersion130,
		platformVersion120,
		platformVersion110,
		platformVersion100:
		return true
	}

	return false
}

// validatePlatformVersion returns an error if the platform version is non-empty
// and not recognized.
func validatePlatformVersion(pv string) error {
	if pv == "" {
		return nil
	}

	upper := strings.ToUpper(pv)
	if isValidFargatePlatformVersion(upper) || isValidFargatePlatformVersion(pv) {
		return nil
	}

	return fmt.Errorf(
		"%w: unknown Fargate platform version %q; valid values: LATEST, 1.4.0, 1.3.0, 1.2.0, 1.1.0, 1.0.0",
		ErrInvalidParameter,
		pv,
	)
}

const connectivityConnected = "CONNECTED"

// taskWork pairs a task with its definition for lock-free Docker API calls and
// for collecting the final task state when building the API response.
type taskWork struct {
	task *Task
	td   *TaskDefinition
	// waiting tasks stay PROVISIONING until a managed-scaling provider supplies capacity.
	waiting bool
}

// RunTask starts one or more tasks on the given cluster. A per-task
// placement failure (no eligible container instance, or its host ports
// cannot satisfy the task definition's port mappings) is reported as a
// Failure entry rather than an error, matching real RunTask -- see
// createTaskEntriesLocked.
func (b *InMemoryBackend) RunTask(input RunTaskInput) ([]Task, []Failure, error) {
	if input.TaskDefinition == "" {
		return nil, nil, fmt.Errorf("%w: taskDefinition is required", ErrInvalidParameter)
	}

	if err := validatePlatformVersion(input.PlatformVersion); err != nil {
		return nil, nil, err
	}

	count := input.Count
	if count <= 0 {
		count = 1
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	var (
		ferr     error
		work     []taskWork
		failures []Failure
	)

	func() {
		b.mu.Lock("RunTask")
		defer b.mu.Unlock()

		b.ensureClusterLocked(clusterName)

		if err := b.validateCapacityProviderStrategyLocked(input.CapacityProviderStrategy); err != nil {
			ferr = err

			return
		}

		td, err := b.findTaskDefinitionLocked(input.TaskDefinition)
		if err != nil {
			ferr = err

			return
		}

		if err = b.validatePinnedTaskIDLocked(clusterName, input.PinnedTaskID, count); err != nil {
			ferr = err

			return
		}

		clusterArn := arn.Build("ecs", b.region, b.accountID, fmt.Sprintf("cluster/%s", clusterName))

		launchType := input.LaunchType
		if launchType == "" {
			launchType = defaultLaunchType(input.CapacityProviderStrategy)
		}

		// Resolve task tags respecting propagateTags + ECS-managed-tags semantics.
		// We read tdTags while holding the write lock — safe, no deadlock risk.
		tdTags := b.resourceTags[resourceTagKey(td.TaskDefinitionArn)]
		resolvedTags := resolveTaskTags(
			input.Tags,
			input.PropagateTags,
			input.EnableECSManagedTags,
			clusterName,
			input.serviceNameForTags,
			td,
			tdTags,
			input.serviceTagsForPropagate,
		)

		// Create all task entries in PROVISIONING state under the lock so they are
		// immediately visible, then release the lock before issuing Docker API calls.
		work, failures = b.createTaskEntriesLocked(
			clusterName,
			clusterArn,
			launchType,
			resolvedTags,
			count,
			td,
			input,
		)
	}()

	if ferr != nil {
		return nil, nil, ferr
	}

	b.startTasksOutsideLock(work)

	// Snapshot final task states to build the API response.
	b.mu.RLock("RunTask-response")
	defer b.mu.RUnlock()

	tasks := make([]Task, 0, len(work))
	for _, w := range work {
		tasks = append(tasks, b.taskWithLiveTagsLocked(w.task))
	}

	return tasks, failures, nil
}

// startTasksOutsideLock hands real-runner tasks to their launch goroutine (RunTask returns while they are
// PROVISIONING) and finishes runner-less tasks inline.
func (b *InMemoryBackend) startTasksOutsideLock(work []taskWork) {
	for _, w := range work {
		clusterName := clusterKey(clusterFromTaskARN(w.task.TaskArn))

		b.ensureAwslogsStreams(w.task, w.td)

		if w.waiting {
			continue
		}

		if _, noop := b.runner.(noopRunner); b.runner == nil || noop {
			if b.maybeRegisterStartLifecycle(w.task, clusterName) {
				continue
			}

			b.applyNoRunnerTransition(w.task, clusterName)

			continue
		}

		b.launchAsync(w, clusterName)
	}
}

// maybeRegisterStartLifecycle enrolls a no-runner task in the observable start
// pipeline when a start delay is configured, returning true if it did. When it
// returns false the caller applies the immediate transition instead. Must be
// called without any lock held.
func (b *InMemoryBackend) maybeRegisterStartLifecycle(task *Task, clusterName string) bool {
	b.mu.Lock("RunTask-registerStartLifecycle")
	defer b.mu.Unlock()

	if b.startDelay <= 0 || task.LastStatus != statusProvisioning {
		return false
	}

	b.registerStartLifecycleLocked(task, clusterName)

	return true
}

// applyNoRunnerTransition transitions a PROVISIONING task through PENDING to RUNNING
// when no container runtime is configured. Must be called without any lock held.
func (b *InMemoryBackend) applyNoRunnerTransition(task *Task, clusterName string) {
	b.mu.Lock("RunTask-setRunning")
	defer b.mu.Unlock()

	if task.LastStatus != statusProvisioning {
		return
	}

	task.LastStatus = statusPending
	b.emitTaskStateLocked(task)
	b.markRunningLocked(task, b.clock())
	b.emitTaskStateLocked(task)

	if c, _ := b.clusters.Get(clusterName); c != nil {
		c.PendingTasksCount--
		c.RunningTasksCount++
	}

	b.registerTaskWithELBv2Locked(task, clusterName)
	b.registerTaskWithCloudMapLocked(task, clusterName)
}

// applyRuntimeContainerIDs sets each container's RuntimeID to the ID its runtime assigned, when the
// runner exposes one. Caller holds b.mu.
func (b *InMemoryBackend) applyRuntimeContainerIDs(task *Task) {
	r, ok := b.runner.(interface{ ContainerIDs(taskArn string) []string })
	if !ok {
		return
	}

	for i, id := range r.ContainerIDs(task.TaskArn) {
		if i < len(task.Containers) {
			task.Containers[i].RuntimeID = id
		}
	}
}

// createTaskEntriesLocked creates task entries in PROVISIONING state. For an
// EC2-launch-type task that cannot be placed (no eligible container
// instance, or the chosen instance's ports cannot satisfy the task
// definition), no task is created and a Failure is appended instead --
// matching real RunTask/StartTask, which report placement failures inline
// rather than raising an exception (see
// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/api_failures_messages.html).
// Must be called with write lock held; the lock is NOT released here.
func (b *InMemoryBackend) createTaskEntriesLocked(
	clusterName, clusterArn, launchType string,
	resolvedTags []Tag,
	count int,
	td *TaskDefinition,
	input RunTaskInput,
) ([]taskWork, []Failure) {
	work := make([]taskWork, 0)

	var failures []Failure

	for range count {
		task := newTaskEntryLocked(b.region, b.accountID, clusterName, clusterArn, launchType, resolvedTags, td, input)

		var (
			hostPortBindings map[string][]NetworkBinding
			waiting          bool
		)

		if launchType == launchTypeFargate {
			subnet := firstRequestedSubnet(input.NetworkConfiguration)
			task.Attachments = []TaskAttachment{newFargateTaskAttachment(task.TaskArn, subnet)}
		} else {
			bindings, wait, failure := b.placeEC2TaskLocked(clusterName, task, td, input)
			if failure != nil {
				failures = append(failures, *failure)

				continue
			}

			hostPortBindings, waiting = bindings, wait
			task.AwaitingCapacity = wait
		}

		// Mirror tags into the resourceTags side map so TagResource/UntagResource/
		// ListTagsForResource (and DescribeTasks, which reads tags live from
		// resourceTags -- see DescribeTasks below) see the tags applied at
		// creation, matching the fix already applied to ExpressGatewayService and
		// CapacityProvider. Without this, task.Tags and resourceTags are two
		// independent stores and a TagResource call after RunTask is invisible to
		// DescribeTasks. Applied only once placement (if any) has succeeded, since
		// a failed placement creates no task to tag.
		if len(resolvedTags) > 0 {
			b.setResourceTagsLocked(task.TaskArn, resolvedTags)
		}

		task.Containers = buildContainersForTask(task, td)
		applyHostPortBindings(task, hostPortBindings)

		b.tasks.Put(task)
		b.emitTaskStateLocked(task)
		work = append(work, taskWork{task: task, td: td, waiting: waiting})

		// Increment the cached pending counter on the cluster.
		if c, _ := b.clusters.Get(clusterName); c != nil {
			c.PendingTasksCount++
		}
	}

	return work, failures
}

// newTaskEntryLocked builds a fresh Task in PROVISIONING state, resolving
// the effective task IAM role and selected capacity provider name. Does not
// set Attachments/ContainerInstanceArn/Containers -- those depend on launch
// type and are filled in by createTaskEntriesLocked's caller.
func newTaskEntryLocked(
	region, accountID, clusterName, clusterArn, launchType string,
	resolvedTags []Tag,
	td *TaskDefinition,
	input RunTaskInput,
) *Task {
	taskID := uuid.NewString()
	if input.PinnedTaskID != "" {
		taskID = input.PinnedTaskID
	}

	taskArn := arn.Build("ecs", region, accountID, fmt.Sprintf("task/%s/%s", clusterName, taskID))
	now := time.Now()

	// Resolve the effective task IAM role: per-run override takes precedence.
	taskRoleArn := td.TaskRoleArn
	if input.Overrides != nil && input.Overrides.TaskRoleArn != "" {
		taskRoleArn = input.Overrides.TaskRoleArn
	}

	// CapacityProviderName reflects the capacity provider actually selected for
	// this task. AWS distributes tasks across the strategy's providers by
	// weight/base; this backend does not model that distribution and always
	// selects the first entry (documented simplification -- see the
	// CapacityProviderName doc comment on the Task struct in models.go).
	var capacityProviderName string
	if len(input.CapacityProviderStrategy) > 0 {
		capacityProviderName = input.CapacityProviderStrategy[0].CapacityProvider
	}

	return &Task{
		TaskArn:              taskArn,
		ClusterArn:           clusterArn,
		TaskDefinitionArn:    td.TaskDefinitionArn,
		LastStatus:           statusProvisioning,
		DesiredStatus:        statusRunning,
		Group:                input.Group,
		LaunchType:           launchType,
		StartedBy:            input.StartedBy,
		PlatformVersion:      input.PlatformVersion,
		PropagateTags:        input.PropagateTags,
		Tags:                 resolvedTags,
		CreatedAt:            &now,
		Overrides:            input.Overrides,
		NetworkConfiguration: input.NetworkConfiguration,
		EnableExecuteCommand: input.EnableExecuteCommand,
		TaskRoleArn:          taskRoleArn,
		CapacityProviderName: capacityProviderName,
	}
}

// placeEC2TaskLocked selects a container instance for an EC2-launch-type task, honouring placement
// constraints/strategy and the CPU/memory/port ledger, and reserves the host ports its network mode needs.
// On success it sets task.ContainerInstanceArn and returns the NetworkBinding map. When no instance fits but
// the task's capacity provider has managed scaling ENABLED it returns waiting=true (the task stays
// PROVISIONING). Otherwise it returns a RESOURCE:* Failure and leaves task unmodified. Write lock held.
func (b *InMemoryBackend) placeEC2TaskLocked(
	clusterName string, task *Task, td *TaskDefinition, input RunTaskInput,
) (map[string][]NetworkBinding, bool, *Failure) {
	constraints := mergeConstraints(td.PlacementConstraints, input.PlacementConstraints)

	bindings, failure := b.tryPlaceTaskLocked(
		clusterName, task, td, constraints, input.PlacementStrategy, input.serviceNameForTags,
	)
	if failure == nil {
		return bindings, false, nil
	}

	if b.managedScalingEnabledLocked(task.CapacityProviderName) {
		return nil, true, nil
	}

	return nil, false, failure
}

// tryPlaceTaskLocked runs one placement attempt, committing the instance and ports on success.
func (b *InMemoryBackend) tryPlaceTaskLocked(
	clusterName string, task *Task, td *TaskDefinition,
	constraints []PlacementConstraint, strategies []PlacementStrategy, serviceName string,
) (map[string][]NetworkBinding, *Failure) {
	skip := b.providerSkipLocked(clusterName, task.CapacityProviderName)
	portsBlocked := false
	cpu, mem := taskReservation(task, td)
	ledger := ledgerView{
		shortfall: func(ci *ContainerInstance) string {
			return b.resourceShortfallLocked(clusterName, ci, int64(cpu), int64(mem))
		},
		free: func(ci *ContainerInstance) (int64, int64, bool) { return b.instanceFreeLocked(clusterName, ci) },
	}

	var (
		instanceArn string
		reason      string
		bindings    map[string][]NetworkBinding
	)

	for {
		instanceArn, reason = selectContainerInstance(
			b.containerInstancesByCluster.Get(clusterName),
			b.tasksByCluster.Get(clusterName),
			constraints, strategies, serviceName, skip, ledger,
		)

		if instanceArn == "" {
			break
		}

		var ok bool

		if bindings, ok = b.reserveTaskHostPortsLocked(clusterName, instanceArn, td); ok {
			break
		}

		skip[instanceArn] = true
		portsBlocked = true
	}

	if instanceArn == "" {
		return nil, placementFailure(td, portsBlocked, reason)
	}

	task.ContainerInstanceArn = instanceArn
	b.indexTaskOnInstance(clusterName, instanceArn, task.TaskArn)

	return bindings, nil
}

func placementFailure(td *TaskDefinition, portsBlocked bool, reason string) *Failure {
	switch {
	case portsBlocked:
		return &Failure{
			Reason: failureReasonResourcePorts,
			Detail: fmt.Sprintf(
				"no container instance has the host ports required by task definition %s",
				td.TaskDefinitionArn,
			),
		}
	case reason != "":
		return &Failure{
			Reason: reason,
			Detail: "no container instance has enough " + strings.ToLower(strings.TrimPrefix(reason, "RESOURCE:")) +
				" for task definition " + td.TaskDefinitionArn,
		}
	default:
		return &Failure{
			Reason: failureReasonResource,
			Detail: "no container instance available in the cluster that satisfies " +
				"the requested placement constraints",
		}
	}
}

// DescribeTasks returns tasks on a given cluster, optionally filtered by ARN.
// Unknown task ARNs are returned as failures, not errors, matching AWS behaviour.
func (b *InMemoryBackend) DescribeTasks(
	cluster string,
	taskArns []string,
) ([]Task, []Failure, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.stepTaskLifecycle(time.Now())

	b.mu.RLock("DescribeTasks")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterName) {
		return nil, nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	if len(taskArns) == 0 {
		clusterTasks := b.tasksByCluster.Get(clusterName)
		out := make([]Task, 0, len(clusterTasks))
		for _, t := range clusterTasks {
			out = append(out, b.taskWithLiveTagsLocked(t))
		}

		return out, nil, nil
	}

	out := make([]Task, 0, len(taskArns))
	failures := make([]Failure, 0, len(taskArns))

	for _, arn := range taskArns {
		t, found := b.tasks.Get(arn)
		if !found || clusterKey(t.ClusterArn) != clusterName {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("task %s not found", arn),
			})

			continue
		}

		out = append(out, b.taskWithLiveTagsLocked(t))
	}

	return out, failures, nil
}

// taskWithLiveTagsLocked copies t with live tags and deep-copied
// Containers/Attachments (mutated in place elsewhere). Needs at least RLock.
func (b *InMemoryBackend) taskWithLiveTagsLocked(t *Task) Task {
	cp := *t
	cp.Tags = copyTags(b.resourceTags[resourceTagKey(t.TaskArn)])
	cp.Containers = append([]Container(nil), t.Containers...)
	cp.Attachments = append([]TaskAttachment(nil), t.Attachments...)

	return cp
}

// StopTask stops a running task.
func (b *InMemoryBackend) StopTask(cluster, taskArn, reason string) (*Task, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	var (
		ferr        error
		delayedCp   *Task
		fastCp      Task
		fastTask    *Task
		instanceArn string
	)

	func() {
		b.mu.Lock("StopTask")
		defer b.mu.Unlock()

		if !b.clusters.Has(clusterName) {
			ferr = fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)

			return
		}

		task, ok := b.tasks.Get(taskArn)
		if !ok || clusterKey(task.ClusterArn) != clusterName {
			ferr = fmt.Errorf("%w: task %s not found", ErrInvalidParameter, taskArn)

			return
		}

		now := time.Now()
		prevStatus := task.LastStatus
		task.DesiredStatus = statusStopped
		task.StoppedReason = reason
		task.StopCode = stopCodeUserInitiated
		b.cancelLaunchLocked(taskArn)

		// Decrement the cached cluster counters once, as the task leaves its active
		// state. This is done up front for both the fast and delayed paths so the
		// counters stay correct regardless of when the task finally reaches STOPPED.
		if c, _ := b.clusters.Get(clusterName); c != nil {
			switch prevStatus {
			case statusRunning:
				c.RunningTasksCount--
			case statusProvisioning, statusPending, statusActivating:
				c.PendingTasksCount--
			}
		}

		// Delayed path: leave the task in DEACTIVATING and let the lifecycle stepper
		// advance it through STOPPING/DEPROVISIONING/STOPPED so SDK waiters observe
		// the intermediate states. Only used when a stop delay is configured and the
		// task is actually in an active state.
		if b.stopDelay > 0 && isStoppableStatus(prevStatus) {
			task.LastStatus = statusDeactivating
			b.emitTaskStateLocked(task)
			b.lifecycle[taskArn] = &taskLifecycle{
				clusterName: clusterName,
				kind:        lifecycleKindStop,
				phase:       statusDeactivating,
				nextAt:      now.Add(b.stopDelay),
				reason:      reason,
			}

			cp := b.taskWithLiveTagsLocked(task)
			delayedCp = &cp

			return
		}

		// Fast path: transition straight to STOPPED. Clear any lifecycle entry
		// left over from a start (or a prior, still in-flight stop) pipeline --
		// otherwise the lifecycle stepper later advances the stale entry and
		// resurrects this task past STOPPED (e.g. back to PENDING/RUNNING).
		task.LastStatus = statusStopped
		task.StoppedAt = &now
		syncContainerStatuses(task, nil)

		if prevStatus != statusStopped {
			b.emitTaskStateLocked(task)
		}

		b.deregisterTaskFromELBv2Locked(task, clusterName)
		b.deregisterTaskFromCloudMapLocked(task, clusterName)
		b.releaseTaskHostPortsLocked(clusterName, task)
		delete(b.lifecycle, taskArn)

		instanceArn = task.ContainerInstanceArn
		fastCp = b.taskWithLiveTagsLocked(task)
		fastTask = task
	}()

	if ferr != nil {
		return nil, ferr
	}

	if delayedCp != nil {
		return delayedCp, nil
	}

	// Docker API calls happen outside the lock so other backend operations are
	// not serialized behind potentially slow container stops.
	if b.runner != nil {
		_ = b.runner.StopTask(fastTask)
	}

	// Clean up task protection entry and reverse index to avoid stale entries.
	func() {
		b.mu.Lock("StopTask-cleanup")
		defer b.mu.Unlock()

		b.taskProtections.Delete(taskArn)
		b.unindexTaskFromInstance(clusterName, instanceArn, taskArn)
	}()

	return &fastCp, nil
}

// isStoppableStatus reports whether a task in the given state has an active
// lifecycle that can be observably wound down (as opposed to one that is already
// stopped or mid-transition).
func isStoppableStatus(status string) bool {
	switch status {
	case statusRunning, statusPending, statusProvisioning, statusActivating:
		return true
	default:
		return false
	}
}

// markTaskStoppedByContainerExit finalizes taskArn to STOPPED when a
// container the Docker runner started exits on its own, without an explicit
// StopTask call -- wired as realDockerRunner's completion handler (see
// SetTaskCompletionHandler in docker_runner.go and its wiring in
// provider.go). Only an essential container's exit stops the task; a concurrent
// StopTask wins the race, so this is a no-op once the task is inactive.
func (b *InMemoryBackend) markTaskStoppedByContainerExit(taskArn, containerName string, exitCode int) {
	var (
		clusterName string
		instanceArn string
		stopped     bool
		stoppedTask Task
	)

	func() {
		b.mu.Lock("markTaskStoppedByContainerExit")
		defer b.mu.Unlock()

		task, ok := b.tasks.Get(taskArn)
		if !ok || !isStoppableStatus(task.LastStatus) {
			return
		}

		if !b.containerExitStopsTaskLocked(task, containerName) {
			markContainerStopped(task, containerName, exitCode)

			return
		}

		prevStatus := task.LastStatus
		clusterName = clusterKey(task.ClusterArn)
		now := time.Now()

		task.LastStatus = statusStopped
		task.DesiredStatus = statusStopped
		task.StoppedAt = &now

		if task.StoppedReason == "" {
			task.StoppedReason = "Essential container in task exited"
		}

		if task.StopCode == "" {
			task.StopCode = stopCodeEssentialContainerExited
		}

		syncContainerStatuses(task, nil)
		setContainerExitCode(task, containerName, exitCode)
		b.emitTaskStateLocked(task)

		if c, _ := b.clusters.Get(clusterName); c != nil {
			switch prevStatus {
			case statusRunning:
				c.RunningTasksCount--
			case statusPending, statusProvisioning, statusActivating:
				c.PendingTasksCount--
			}
		}

		b.deregisterTaskFromELBv2Locked(task, clusterName)
		b.deregisterTaskFromCloudMapLocked(task, clusterName)
		b.releaseTaskHostPortsLocked(clusterName, task)
		delete(b.lifecycle, taskArn)

		instanceArn = task.ContainerInstanceArn
		stoppedTask = *task
		stopped = true
	}()

	if !stopped {
		return
	}

	if b.runner != nil {
		_ = b.runner.StopTask(&stoppedTask)
	}

	func() {
		b.mu.Lock("markTaskStoppedByContainerExit-cleanup")
		defer b.mu.Unlock()

		b.taskProtections.Delete(taskArn)
		b.unindexTaskFromInstance(clusterName, instanceArn, taskArn)
	}()
}

// ListTasksInput holds optional filters for ListTasks.
type ListTasksInput struct {
	Cluster           string
	ContainerInstance string
	Family            string
	ServiceName       string
	DaemonName        string
	DesiredStatus     string
	LaunchType        string
	StartedBy         string
}

func (b *InMemoryBackend) ListTasks(cluster string) ([]string, error) {
	return b.ListTasksFiltered(ListTasksInput{Cluster: cluster})
}

// ListTasksFiltered returns task ARNs matching the given filters.
// Per ListTasksInput.DesiredStatus's doc ("The default status filter is
// RUNNING"), an unset DesiredStatus narrows to RUNNING tasks rather than
// matching every status.
func (b *InMemoryBackend) ListTasksFiltered(input ListTasksInput) ([]string, error) {
	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.stepTaskLifecycle(time.Now())

	b.mu.RLock("ListTasksFiltered")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterName) {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
	}

	wantDesiredStatus := input.DesiredStatus
	if wantDesiredStatus == "" {
		wantDesiredStatus = statusRunning
	}

	clusterTasks := b.tasksByCluster.Get(clusterName)
	arns := make([]string, 0, len(clusterTasks))

	for _, task := range clusterTasks {
		if listTasksMatch(task, input, wantDesiredStatus) {
			arns = append(arns, task.TaskArn)
		}
	}

	return arns, nil
}

// listTasksMatch applies every ListTasks filter in input to task.
func listTasksMatch(task *Task, input ListTasksInput, wantDesiredStatus string) bool {
	switch {
	case input.ContainerInstance != "" && task.ContainerInstanceArn != input.ContainerInstance:
		return false
	case !strings.EqualFold(task.DesiredStatus, wantDesiredStatus):
		return false
	case input.LaunchType != "" && !strings.EqualFold(task.LaunchType, input.LaunchType):
		return false
	case input.StartedBy != "" && task.StartedBy != input.StartedBy:
		return false
	case input.Family != "" && !strings.Contains(task.TaskDefinitionArn, "/"+input.Family+":"):
		return false
	case input.ServiceName != "" && task.Group != "service:"+input.ServiceName:
		return false
	case input.DaemonName != "" && task.Group != "daemon:"+input.DaemonName:
		return false
	default:
		return true
	}
}

// StartTask places tasks on specific container instances (as opposed to RunTask which auto-places).
func (b *InMemoryBackend) StartTask(input StartTaskInput) ([]Task, []Failure, error) {
	if input.TaskDefinition == "" {
		return nil, nil, fmt.Errorf("%w: taskDefinition is required", ErrInvalidParameter)
	}

	if len(input.ContainerInstances) == 0 {
		return nil, nil, fmt.Errorf(
			"%w: at least one container instance is required",
			ErrInvalidParameter,
		)
	}

	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	var (
		ferr     error
		tasks    []Task
		failures []Failure
		td       *TaskDefinition
	)

	func() {
		b.mu.Lock("StartTask")
		defer b.mu.Unlock()

		b.ensureClusterLocked(clusterName)

		var err error

		td, err = b.findTaskDefinitionLocked(input.TaskDefinition)
		if err != nil {
			ferr = err

			return
		}

		clusterArn := arn.Build("ecs", b.region, b.accountID, fmt.Sprintf("cluster/%s", clusterName))

		tasks = make([]Task, 0, len(input.ContainerInstances))
		failures = make([]Failure, 0, len(input.ContainerInstances))

		for _, ciArn := range input.ContainerInstances {
			if ci, found := b.containerInstances.Get(scopedKey(clusterName, ciArn)); !found ||
				ci.Status == statusInactive {
				failures = append(failures, Failure{
					Arn:    ciArn,
					Reason: statusMissing,
					Detail: fmt.Sprintf("container instance %s not found", ciArn),
				})

				continue
			}

			t := b.newStartedTaskLocked(clusterName, clusterArn, ciArn, td, input)

			b.tasks.Put(t)
			b.emitTaskStateLocked(t)
			tasks = append(tasks, *t)
		}
	}()

	if ferr != nil {
		return nil, nil, ferr
	}

	for i := range tasks {
		b.ensureAwslogsStreams(&tasks[i], td)
	}

	return tasks, failures, nil
}

// newStartedTaskLocked builds and tags one RUNNING task on a chosen container instance.
func (b *InMemoryBackend) newStartedTaskLocked(
	clusterName, clusterArn, ciArn string,
	td *TaskDefinition,
	input StartTaskInput,
) *Task {
	taskArn := arn.Build("ecs", b.region, b.accountID, fmt.Sprintf("task/%s/%s", clusterName, uuid.New().String()))
	now := time.Now()

	taskRoleArn := td.TaskRoleArn
	if input.Overrides != nil && input.Overrides.TaskRoleArn != "" {
		taskRoleArn = input.Overrides.TaskRoleArn
	}

	resolvedTags := resolveTaskTags(
		input.Tags, input.PropagateTags, input.EnableECSManagedTags,
		clusterName, "", td, b.resourceTags[resourceTagKey(td.TaskDefinitionArn)], nil,
	)

	if len(resolvedTags) > 0 {
		b.setResourceTagsLocked(taskArn, resolvedTags)
	}

	return &Task{
		PropagateTags:        input.PropagateTags,
		Tags:                 resolvedTags,
		Overrides:            input.Overrides,
		NetworkConfiguration: input.NetworkConfiguration,
		EnableExecuteCommand: input.EnableExecuteCommand,
		TaskRoleArn:          taskRoleArn,
		TaskArn:              taskArn,
		ClusterArn:           clusterArn,
		TaskDefinitionArn:    td.TaskDefinitionArn,
		LastStatus:           statusRunning,
		DesiredStatus:        statusRunning,
		CreatedAt:            &now,
		Connectivity:         connectivityConnected,
		ConnectivityAt:       &now,
		Group:                input.Group,
		LaunchType:           "EC2",
		ContainerInstanceArn: ciArn,
		StartedBy:            input.StartedBy,
		StartedAt:            &now,
	}
}

// GetTaskProtection returns the protection state for the given tasks on a cluster.
func (b *InMemoryBackend) GetTaskProtection(
	cluster string,
	taskArns []string,
) ([]TaskProtection, []Failure, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	b.mu.RLock("GetTaskProtection")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterName) {
		return nil, nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	if len(taskArns) == 0 {
		clusterTasks := b.tasksByCluster.Get(clusterName)
		out := make([]TaskProtection, 0, len(clusterTasks))

		for _, t := range clusterTasks {
			out = append(out, b.taskProtectionLocked(t.TaskArn))
		}

		return out, nil, nil
	}

	out := make([]TaskProtection, 0, len(taskArns))
	failures := make([]Failure, 0, len(taskArns))

	for _, arn := range taskArns {
		if t, found := b.tasks.Get(arn); !found || clusterKey(t.ClusterArn) != clusterName {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("task %s not found", arn),
			})

			continue
		}

		out = append(out, b.taskProtectionLocked(arn))
	}

	return out, failures, nil
}

// taskProtectionLocked returns the protection for a task ARN (defaults to unprotected).
// Must be called with at least an RLock held.
func (b *InMemoryBackend) taskProtectionLocked(taskArn string) TaskProtection {
	if tp, ok := b.taskProtections.Get(taskArn); ok {
		return *tp
	}

	return TaskProtection{TaskArn: taskArn, ProtectionEnabled: false}
}

// UpdateTaskProtection updates task protection for tasks in a cluster.
func (b *InMemoryBackend) UpdateTaskProtection(
	cluster string,
	taskArns []string,
	protectionEnabled bool,
	expiresInMinutes *int,
) ([]TaskProtection, []Failure, error) {
	clusterName := clusterKey(b.resolveCluster(cluster))

	if len(taskArns) == 0 {
		return nil, nil, fmt.Errorf("%w: tasks is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateTaskProtection")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterName) {
		return nil, nil, fmt.Errorf("%w: %s", ErrClusterNotFound, cluster)
	}

	out := make([]TaskProtection, 0, len(taskArns))
	failures := make([]Failure, 0, len(taskArns))

	for _, arn := range taskArns {
		if t, found := b.tasks.Get(arn); !found || clusterKey(t.ClusterArn) != clusterName {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("task %s not found", arn),
			})

			continue
		}

		tp := &TaskProtection{
			TaskArn:           arn,
			ProtectionEnabled: protectionEnabled,
		}

		if protectionEnabled && expiresInMinutes != nil {
			exp := time.Now().Add(time.Duration(*expiresInMinutes) * time.Minute)
			tp.ExpirationDate = &exp
		}

		b.taskProtections.Put(tp)
		out = append(out, *tp)
	}

	return out, failures, nil
}

// containerExitStopsTaskLocked reports whether containerName's exit stops the task: true for
// essential containers, or when the definition is unresolvable or has none.
func (b *InMemoryBackend) containerExitStopsTaskLocked(task *Task, containerName string) bool {
	td, err := b.findTaskDefinitionLocked(task.TaskDefinitionArn)
	if err != nil {
		return true
	}

	exitedEssential := true
	anyEssential := false

	for _, cd := range td.ContainerDefinitions {
		anyEssential = anyEssential || cd.Essential

		if cd.Name == containerName {
			exitedEssential = cd.Essential
		}
	}

	return exitedEssential || !anyEssential
}

// markContainerStopped records the exit of a single container without
// changing the task's own status.
func markContainerStopped(task *Task, containerName string, exitCode int) {
	for i := range task.Containers {
		if task.Containers[i].Name == containerName {
			task.Containers[i].LastStatus = statusStopped
			task.Containers[i].ExitCode = &exitCode

			return
		}
	}
}

// firstRequestedSubnet returns the first awsvpc subnet in nc, or "".
func firstRequestedSubnet(nc *NetworkConfiguration) string {
	if nc == nil || nc.AwsvpcConfiguration == nil || len(nc.AwsvpcConfiguration.Subnets) == 0 {
		return ""
	}

	return nc.AwsvpcConfiguration.Subnets[0]
}

var taskIDPattern = regexp.MustCompile(`^[0-9a-f]{8}(?:-?[0-9a-f]{4}){3}-?[0-9a-f]{12}$`)

// validatePinnedTaskIDLocked checks a pinned task ID is well formed, unused, and for a single task.
func (b *InMemoryBackend) validatePinnedTaskIDLocked(clusterName, pinned string, count int) error {
	if pinned == "" {
		return nil
	}

	if count != 1 {
		return fmt.Errorf("%w: %s can only pin the ID of a single task", ErrInvalidParameter, tags.PinnedIDKey)
	}

	if err := tags.ValidatePinnedID(pinned, taskIDPattern, "a 32 hex digit task ID"); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidParameter, err)
	}

	arnStr := arn.Build("ecs", b.region, b.accountID, fmt.Sprintf("task/%s/%s", clusterName, pinned))
	if b.tasks.Has(arnStr) {
		return fmt.Errorf("%w: task %q already exists", ErrTaskIDInUse, pinned)
	}

	return nil
}
