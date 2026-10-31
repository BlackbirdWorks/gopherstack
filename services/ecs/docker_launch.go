package ecs

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	dependsStart    = "START"
	dependsComplete = "COMPLETE"
	dependsSuccess  = "SUCCESS"
	dependsHealthy  = "HEALTHY"

	defaultPollInterval      = time.Second
	defaultDependencyTimeout = 3 * time.Minute

	defaultRestartAttemptPeriod = 300 * time.Second
	minRestartAttemptPeriod     = 60 * time.Second

	rollbackStopTimeoutSeconds = 10

	healthDockerHealthy   = "healthy"
	healthDockerUnhealthy = "unhealthy"
	healthECSUnhealthy    = "UNHEALTHY"
	healthECSUnknown      = containerHealthStatusUnknown
	healthECSHealthy      = containerHealthStatusHealthy

	oomReason = "OutOfMemoryError: Container killed due to memory usage"
)

var (
	errTaskStoppedDuringLaunch = errors.New("task stopped while its containers were starting")
	errDependency              = errors.New("container dependency not satisfied")
	errDependencyCycle         = errors.New("circular container dependency")
	errUnsupportedCondition    = errors.New("unsupported condition")
	errNoExitWatcher           = errors.New("no exit watcher for container")
	errExited                  = errors.New("exited")
	errBecameUnhealthy         = errors.New("became unhealthy")
)

// launchState lets StopTask abort a RunTask that is still starting containers.
type launchState struct {
	abort chan struct{}
	stop  context.CancelFunc
	once  sync.Once
}

func (l *launchState) cancel() {
	l.once.Do(func() {
		close(l.abort)
		l.stop()
	})
}

// containerExit records a watched container's final exit code; done closes once code is set.
type containerExit struct {
	done chan struct{}
	code int
}

// launch is the per-RunTask bookkeeping.
type launch struct {
	state     *launchState
	task      *Task
	plan      *volumePlan
	ids       map[string]string
	secretEnv map[string][]string
	fileEnv   map[string][]string
	needsExit map[string]bool
	pidMode   string
	ipcMode   string
	order     []string
}

// containerOrderer is a depth-first topological sort over dependsOn and volumesFrom edges.
type containerOrderer struct {
	defs  []ContainerDefinition
	index map[string]int
	state []int
	out   []int
}

const (
	orderVisiting = 1
	orderDone     = 2
)

func (o *containerOrderer) dependencies(cd ContainerDefinition) ([]string, error) {
	names := make([]string, 0, len(cd.DependsOn)+len(cd.VolumesFrom))

	for _, d := range cd.DependsOn {
		names = append(names, d.ContainerName)
	}

	for _, v := range cd.VolumesFrom {
		names = append(names, v.SourceContainer)
	}

	for _, n := range names {
		if _, ok := o.index[n]; !ok {
			return nil, fmt.Errorf("%w: container %q references undefined container %q", errDependency, cd.Name, n)
		}
	}

	return names, nil
}

func (o *containerOrderer) visit(i int) error {
	switch o.state[i] {
	case orderDone:
		return nil
	case orderVisiting:
		return fmt.Errorf("%w involving %q", errDependencyCycle, o.defs[i].Name)
	}

	o.state[i] = orderVisiting

	names, err := o.dependencies(o.defs[i])
	if err != nil {
		return err
	}

	for _, n := range names {
		if err = o.visit(o.index[n]); err != nil {
			return err
		}
	}

	o.state[i] = orderDone
	o.out = append(o.out, i)

	return nil
}

// orderContainers returns the indices of defs sorted so dependsOn targets and volumesFrom sources come first,
// keeping definition order otherwise.
func orderContainers(defs []ContainerDefinition) ([]int, error) {
	o := &containerOrderer{
		defs:  defs,
		index: make(map[string]int, len(defs)),
		state: make([]int, len(defs)),
		out:   make([]int, 0, len(defs)),
	}

	for i, cd := range defs {
		o.index[cd.Name] = i
	}

	for i := range defs {
		if err := o.visit(i); err != nil {
			return nil, err
		}
	}

	return o.out, nil
}

// RunTask starts td's containers in dependency order, gating each on its dependsOn conditions.
func (r *realDockerRunner) RunTask(task *Task, td *TaskDefinition) error {
	return r.RunTaskContext(r.svcCtx, task, td)
}

// RunTaskContext is RunTask with ctx cancelling the launch (pulls, gates, starts) and rolling it back.
func (r *realDockerRunner) RunTaskContext(parent context.Context, task *Task, td *TaskDefinition) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	orderIdx, err := orderContainers(td.ContainerDefinitions)
	if err != nil {
		return &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
	}

	order := make([]ContainerDefinition, len(orderIdx))
	for pos, i := range orderIdx {
		order[pos] = td.ContainerDefinitions[i]
	}

	if fireErr := checkFirelens(order); fireErr != nil {
		return fireErr
	}

	secretEnv, err := r.resolveSecretEnv(ctx, order)
	if err != nil {
		return err
	}

	fileEnv, err := r.resolveFileEnv(ctx, order)
	if err != nil {
		return err
	}

	plan, err := r.resolveVolumes(ctx, task, td)
	if _, isLaunch := errors.AsType[*taskLaunchError](err); isLaunch {
		return err
	}

	if err != nil {
		return &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
	}

	l := &launch{
		task:      task,
		plan:      plan,
		ids:       make(map[string]string, len(order)),
		secretEnv: secretEnv,
		fileEnv:   fileEnv,
		needsExit: exitDependencies(order),
		pidMode:   hostOrNone(td.PidMode),
		ipcMode:   hostOrNone(td.IpcMode),
		state:     &launchState{abort: make(chan struct{}), stop: cancel},
	}

	r.mu.Lock()
	r.launching[task.TaskArn] = l.state
	r.taskVolumes[task.TaskArn] = plan.taskVols
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.launching, task.TaskArn)
		r.mu.Unlock()
	}()

	for _, cd := range order {
		if launchErr := r.launchContainer(ctx, l, cd); launchErr != nil {
			r.abortLaunch(r.svcCtx, l)

			return launchErr
		}
	}

	select {
	case <-l.state.abort:
		r.abortLaunch(r.svcCtx, l)

		return errTaskStoppedDuringLaunch
	case <-ctx.Done():
		r.abortLaunch(r.svcCtx, l)

		return fmt.Errorf("%w: %w", errTaskStoppedDuringLaunch, ctx.Err())
	default:
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	inDefOrder := make([]string, len(order))
	for pos, i := range orderIdx {
		inDefOrder[i] = l.order[pos]
	}

	r.containers[task.TaskArn] = inDefOrder

	return nil
}

func hostOrNone(mode string) string {
	if mode == "host" || mode == "none" {
		return mode
	}

	return ""
}

// exitDependencies returns the containers some other container waits on to exit.
func exitDependencies(defs []ContainerDefinition) map[string]bool {
	out := map[string]bool{}

	for _, cd := range defs {
		for _, d := range cd.DependsOn {
			if d.Condition == dependsComplete || d.Condition == dependsSuccess {
				out[d.ContainerName] = true
			}
		}
	}

	return out
}

func (r *realDockerRunner) resolveSecretEnv(
	ctx context.Context,
	defs []ContainerDefinition,
) (map[string][]string, error) {
	out := make(map[string][]string, len(defs))

	r.mu.Lock()
	lookup := r.secretLookup
	r.mu.Unlock()

	for _, cd := range defs {
		for _, sec := range cd.Secrets {
			if lookup == nil {
				return nil, secretLaunchError("asm", sec.ValueFrom, errSecretsUnavailable)
			}

			val, err := lookup(ctx, sec.ValueFrom)
			if err != nil {
				return nil, err
			}

			out[cd.Name] = append(out[cd.Name], sec.Name+"="+val)
		}
	}

	return out, nil
}

func (r *realDockerRunner) resolveFileEnv(
	ctx context.Context,
	defs []ContainerDefinition,
) (map[string][]string, error) {
	out := make(map[string][]string, len(defs))

	r.mu.Lock()
	lookup := r.envFileLookup
	r.mu.Unlock()

	for _, cd := range defs {
		if len(cd.EnvironmentFiles) == 0 {
			continue
		}

		if lookup == nil {
			return nil, envFileLaunchError(cd.EnvironmentFiles[0].Value, errEnvFilesUnavailable)
		}

		pairs, err := lookup(ctx, cd)
		if err != nil {
			return nil, err
		}

		out[cd.Name] = pairs
	}

	return out, nil
}

func (r *realDockerRunner) pullContainerImage(ctx context.Context, cd ContainerDefinition) error {
	auth, err := r.imageAuth(ctx, cd)
	if err != nil {
		return err
	}

	return r.pullImage(ctx, cd.Image, auth)
}

func (r *realDockerRunner) imageAuth(ctx context.Context, cd ContainerDefinition) (string, error) {
	if cd.RepositoryCredentials == nil {
		return "", nil
	}

	r.mu.Lock()
	lookup := r.registryAuth
	r.mu.Unlock()

	if lookup == nil {
		return "", secretLaunchError(sourceASM, cd.RepositoryCredentials.CredentialsParameter, errSecretsUnavailable)
	}

	return lookup(ctx, cd)
}

// SetRegistryAuthResolver wires the lookup that turns a container's repositoryCredentials into pull auth.
func (r *realDockerRunner) SetRegistryAuthResolver(
	fn func(ctx context.Context, cd ContainerDefinition) (string, error),
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.registryAuth = fn
}

// SetEFSVolumeResolver wires the lookup that resolves efsVolumeConfiguration volumes to host directories.
func (r *realDockerRunner) SetEFSVolumeResolver(
	fn func(region string, cfg *EFSVolumeConfiguration) (EFSVolumeMount, error),
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.efsLookup = fn
}

// SetEnvFileResolver wires the lookup that reads a container's environmentFiles.
func (r *realDockerRunner) SetEnvFileResolver(fn func(ctx context.Context, cd ContainerDefinition) ([]string, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.envFileLookup = fn
}

// launchContainer pulls, gates, creates and starts one container and wires its watchers.
func (r *realDockerRunner) launchContainer(ctx context.Context, l *launch, cd ContainerDefinition) error {
	select {
	case <-l.state.abort:
		return errTaskStoppedDuringLaunch
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", errTaskStoppedDuringLaunch, ctx.Err())
	default:
	}

	if err := r.pullContainerImage(ctx, cd); err != nil {
		return err
	}

	if err := r.awaitDependencies(ctx, l, cd); err != nil {
		return err
	}

	containerID, err := r.createContainer(ctx, l, cd)
	if err != nil {
		return err
	}

	log := logger.Load(ctx)

	if startErr := r.cli.ContainerStart(ctx, containerID, dockertypes.StartOptions{}); startErr != nil {
		if rmErr := r.cli.ContainerRemove(
			ctx, containerID, dockertypes.RemoveOptions{Force: true, RemoveVolumes: true},
		); rmErr != nil {
			log.WarnContext(ctx, "failed to remove container after start failure",
				"containerID", containerID, "error", rmErr)
		}

		r.closeFronts(containerID)

		return fmt.Errorf("start container %s: %w", containerID, startErr)
	}

	r.recordStarted(l, cd.Name, containerID)

	if frontErr := r.startFronts(containerID); frontErr != nil {
		return fmt.Errorf("front container %s: %w", containerID, frontErr)
	}

	taskID := taskIDFromARN(l.task.TaskArn)

	if cwLogs := r.cwLogsBackend(); cwLogs != nil {
		if group, stream, ok := awslogsContainerTarget(cd, taskID, containerID); ok {
			_ = cwLogs.EnsureLogGroupAndStream(group, stream)
			r.forwardContainerLogs(cwLogs, containerID, group, stream, "")
		}
	}

	handler := r.taskCompletionHandler()
	if handler != nil || l.needsExit[cd.Name] || cd.RestartPolicy != nil {
		r.watchContainerExit(handler, l.task.TaskArn, containerID, cd)
	}

	r.monitorHealth(l.task.TaskArn, containerID, cd)

	return nil
}

func (r *realDockerRunner) recordStarted(l *launch, name, containerID string) {
	l.ids[name] = containerID
	l.order = append(l.order, containerID)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.stopOrder[l.task.TaskArn] = append(r.stopOrder[l.task.TaskArn], containerID)
	r.containers[l.task.TaskArn] = append(r.containers[l.task.TaskArn], containerID)
}

// abortLaunch tears down everything a failed or aborted RunTask started, including task-scoped volumes.
func (r *realDockerRunner) abortLaunch(ctx context.Context, l *launch) {
	r.rollbackContainers(ctx, l.order)

	r.mu.Lock()
	delete(r.containers, l.task.TaskArn)
	delete(r.stopOrder, l.task.TaskArn)
	vols := r.taskVolumes[l.task.TaskArn]
	delete(r.taskVolumes, l.task.TaskArn)
	r.mu.Unlock()

	r.removeVolumes(ctx, vols)
	r.forgetContainers(l.order)
}

// forgetContainers drops per-container runtime state once its task is gone.
func (r *realDockerRunner) forgetContainers(ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, id := range ids {
		delete(r.exits, id)
		delete(r.stopTimeouts, id)
	}
}

// SetContainerHealthHandler wires fn to receive Docker health transitions of health-checked containers.
func (r *realDockerRunner) SetContainerHealthHandler(fn func(taskArn, containerName, status string)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.healthHandler = fn
}

// SetContainerReasonHandler wires fn to receive a container's failure reason (for example an OOM kill).
func (r *realDockerRunner) SetContainerReasonHandler(fn func(taskArn, containerName, reason string)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.reasonHandler = fn
}

// SetSecretResolver wires the lookup that turns a container secret's valueFrom into its value.
func (r *realDockerRunner) SetSecretResolver(fn func(ctx context.Context, valueFrom string) (string, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.secretLookup = fn
}

func (r *realDockerRunner) removeVolumes(ctx context.Context, names []string) {
	for _, name := range names {
		if err := r.cli.VolumeRemove(ctx, name); err != nil {
			logger.Load(ctx).WarnContext(ctx, "failed to remove task volume", "volume", name, "error", err)
		}
	}
}

// createContainer maps cd to a Docker container.
func (r *realDockerRunner) createContainer(ctx context.Context, l *launch, cd ContainerDefinition) (string, error) {
	task := l.task
	portBindings, exposedPorts := buildPortMappings(cd.PortMappings)
	diverted := r.divertPorts(task, cd.PortMappings, portBindings)

	env := append(r.runtimeEnv(task, cd.Name), l.fileEnv[cd.Name]...)
	env = append(env, buildEnv(cd.Environment)...)
	env = append(env, l.secretEnv[cd.Name]...)

	mounts, err := containerMounts(cd, l.plan)
	if err != nil {
		releasePending(diverted)

		return "", &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
	}

	cfg, hostCfg, err := buildDockerConfigs(containerInput{
		task:         task,
		cd:           cd,
		portBindings: portBindings,
		exposedPorts: exposedPorts,
		env:          env,
		mounts:       mounts,
		volumesFrom:  l.volumesFrom(cd),
		pidMode:      l.pidMode,
		ipcMode:      l.ipcMode,
	})
	if err != nil {
		releasePending(diverted)

		return "", &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
	}

	applyVolumeIdentity(cfg, hostCfg, cd, l.plan)
	maps.Copy(cfg.Labels, managedLabels(l, cd, len(diverted) > 0))

	resp, err := r.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, "")
	if err != nil {
		releasePending(diverted)

		return "", fmt.Errorf("create container for %s: %w", cd.Image, err)
	}

	r.stashFronts(resp.ID, diverted)

	r.mu.Lock()
	r.stopTimeouts[resp.ID] = *cfg.StopTimeout
	r.mu.Unlock()

	if attachErr := r.attachContainer(ctx, resp.ID); attachErr != nil {
		_ = r.cli.ContainerRemove(ctx, resp.ID, dockertypes.RemoveOptions{Force: true, RemoveVolumes: true})
		r.closeFronts(resp.ID)

		return "", fmt.Errorf("attach container %s to link-local network: %w", resp.ID, attachErr)
	}

	return resp.ID, nil
}

// volumesFrom returns the Docker containers whose volumes cd inherits, as "<id>[:ro]".
func (l *launch) volumesFrom(cd ContainerDefinition) []string {
	out := make([]string, 0, len(cd.VolumesFrom))

	for _, v := range cd.VolumesFrom {
		spec := l.ids[v.SourceContainer]
		if v.ReadOnly {
			spec += ":ro"
		}

		out = append(out, spec)
	}

	return out
}

// awaitDependencies blocks until every dependsOn condition of cd holds, or fails the launch.
func (r *realDockerRunner) awaitDependencies(ctx context.Context, l *launch, cd ContainerDefinition) error {
	if len(cd.DependsOn) == 0 {
		return nil
	}

	timeout := r.dependencyTimeout
	if cd.StartTimeout != nil && *cd.StartTimeout > 0 {
		timeout = time.Duration(*cd.StartTimeout) * time.Second
	}

	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for _, dep := range cd.DependsOn {
		id := l.ids[dep.ContainerName]

		var err error

		switch dep.Condition {
		case dependsStart:
		case dependsComplete:
			_, err = r.awaitExit(dctx, l, id)
		case dependsSuccess:
			err = r.awaitSuccess(dctx, l, id)
		case dependsHealthy:
			err = r.awaitHealthy(dctx, l, id)
		default:
			err = fmt.Errorf("%w %q", errUnsupportedCondition, dep.Condition)
		}

		if err != nil {
			return &taskLaunchError{
				Reason: fmt.Sprintf("Task failed to start: container %q dependency on %q (%s) not satisfied: %v",
					cd.Name, dep.ContainerName, dep.Condition, err),
				Err: errors.Join(errDependency, err),
			}
		}
	}

	return nil
}

func (r *realDockerRunner) awaitExit(ctx context.Context, l *launch, id string) (int, error) {
	r.mu.Lock()
	ex := r.exits[id]
	r.mu.Unlock()

	if ex == nil {
		return 0, fmt.Errorf("%w %s", errNoExitWatcher, id)
	}

	select {
	case <-ex.done:
		return ex.code, nil
	case <-l.state.abort:
		return 0, errTaskStoppedDuringLaunch
	case <-ctx.Done():
		return 0, fmt.Errorf("timed out waiting for exit: %w", ctx.Err())
	}
}

func (r *realDockerRunner) awaitSuccess(ctx context.Context, l *launch, id string) error {
	code, err := r.awaitExit(ctx, l, id)
	if err != nil {
		return err
	}

	if code != 0 {
		return fmt.Errorf("%w with code %d", errExited, code)
	}

	return nil
}

func (r *realDockerRunner) awaitHealthy(ctx context.Context, l *launch, id string) error {
	tick := time.NewTicker(r.pollInterval)
	defer tick.Stop()

	for {
		resp, err := r.cli.ContainerInspect(ctx, id)
		if err != nil {
			return fmt.Errorf("inspect container %s: %w", id, err)
		}

		if st := resp.State; st != nil {
			if !st.Running {
				return fmt.Errorf("%w with code %d before becoming healthy", errExited, st.ExitCode)
			}

			if st.Health != nil {
				switch string(st.Health.Status) {
				case healthDockerHealthy:
					return nil
				case healthDockerUnhealthy:
					return errBecameUnhealthy
				}
			}
		}

		select {
		case <-tick.C:
		case <-l.state.abort:
			return errTaskStoppedDuringLaunch
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for healthy: %w", ctx.Err())
		}
	}
}

// monitorHealth reports a health-checked container's Docker health status until it stops running.
func (r *realDockerRunner) monitorHealth(taskArn, containerID string, cd ContainerDefinition) {
	r.mu.Lock()
	report := r.healthHandler
	r.mu.Unlock()

	if cd.HealthCheck == nil || report == nil {
		return
	}

	ctx, cancel := context.WithCancel(r.svcCtx)

	r.mu.Lock()
	r.healthCancels[containerID] = cancel
	r.mu.Unlock()

	r.waitWG.Go(func() {
		defer cancel()
		defer func() {
			r.mu.Lock()
			delete(r.healthCancels, containerID)
			r.mu.Unlock()
		}()

		tick := time.NewTicker(r.pollInterval)
		defer tick.Stop()

		last := ""

		for {
			resp, err := r.cli.ContainerInspect(ctx, containerID)
			if err != nil || resp.State == nil || !resp.State.Running {
				return
			}

			if status := ecsHealthStatus(resp.State); status != last {
				last = status
				report(taskArn, cd.Name, status)
			}

			select {
			case <-tick.C:
			case <-ctx.Done():
				return
			}
		}
	})
}

func ecsHealthStatus(st *dockertypes.State) string {
	if st.Health == nil {
		return healthECSUnknown
	}

	switch string(st.Health.Status) {
	case healthDockerHealthy:
		return healthECSHealthy
	case healthDockerUnhealthy:
		return healthECSUnhealthy
	default:
		return healthECSUnknown
	}
}

// watchContainerExit waits for containerID to stop and, unless the wait is cancelled first (see
// cancelContainerWait, called once StopTask/rollback has confirmed the stop), applies cd's restart policy and
// then reports the exit to handler. Never leaked: it ends on exit, cancellation or shutdown.
func (r *realDockerRunner) watchContainerExit(
	handler func(taskArn, containerName string, exitCode int),
	taskArn, containerID string,
	cd ContainerDefinition,
) {
	ctx, cancel := context.WithCancel(r.svcCtx)
	ex := &containerExit{done: make(chan struct{})}
	startedAt := r.now()

	r.mu.Lock()
	r.waitCancels[containerID] = cancel
	r.exits[containerID] = ex
	r.mu.Unlock()

	r.waitWG.Go(func() {
		defer cancel()
		defer func() {
			r.mu.Lock()
			delete(r.waitCancels, containerID)
			r.mu.Unlock()
		}()

		for {
			result := r.cli.ContainerWait(ctx, containerID, dockertypes.WaitOptions{})

			select {
			case res := <-result.Result:
				code := int(res.StatusCode)
				if r.restartContainer(ctx, containerID, cd, code, startedAt) {
					startedAt = r.now()

					continue
				}

				r.finishContainer(ctx, handler, taskArn, containerID, cd.Name, ex, code)
			case err := <-result.Error:
				if err != nil && ctx.Err() == nil {
					logger.Load(ctx).WarnContext(ctx, "container wait failed", "containerID", containerID, "error", err)
				}
			case <-ctx.Done():
			}

			return
		}
	})
}

func (r *realDockerRunner) finishContainer(
	ctx context.Context,
	handler func(taskArn, containerName string, exitCode int),
	taskArn, containerID, name string,
	ex *containerExit,
	code int,
) {
	r.closeFronts(containerID)

	r.mu.Lock()
	reason := r.reasonHandler
	r.mu.Unlock()

	if reason != nil {
		if resp, err := r.cli.ContainerInspect(ctx, containerID); err == nil && resp.State != nil &&
			resp.State.OOMKilled {
			reason(taskArn, name, oomReason)
		}
	}

	ex.code = code
	close(ex.done)

	if handler != nil {
		handler(taskArn, name, code)
	}
}

// restartContainer applies cd.RestartPolicy: ECS restarts an exited container unless its exit code is
// ignored or it ran for less than restartAttemptPeriod.
func (r *realDockerRunner) restartContainer(
	ctx context.Context,
	containerID string,
	cd ContainerDefinition,
	code int,
	startedAt time.Time,
) bool {
	rp := cd.RestartPolicy
	if rp == nil || !rp.Enabled || slices.Contains(rp.IgnoredExitCodes, code) {
		return false
	}

	period := defaultRestartAttemptPeriod
	if rp.RestartAttemptPeriod > 0 {
		period = max(time.Duration(rp.RestartAttemptPeriod)*time.Second, minRestartAttemptPeriod)
	}

	if r.now().Sub(startedAt) < period {
		return false
	}

	return r.cli.ContainerStart(ctx, containerID, dockertypes.StartOptions{}) == nil
}

// cancelContainerWait stops the in-flight exit watcher and health monitor for containerID, if any, so a
// self-inflicted stop is not reported as a natural exit.
func (r *realDockerRunner) cancelContainerWait(containerID string) {
	r.mu.Lock()
	cancel, ok := r.waitCancels[containerID]
	hcancel, hok := r.healthCancels[containerID]
	r.mu.Unlock()

	if ok {
		cancel()
	}

	if hok {
		hcancel()
	}
}

// StopTask stops the task's containers in reverse start order, giving each its stopTimeout between SIGTERM and
// SIGKILL, and removes the task's containers and task-scoped volumes when it owns any.
func (r *realDockerRunner) StopTask(task *Task) error {
	var (
		containerIDs []string
		volumes      []string
	)

	func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		if ls := r.launching[task.TaskArn]; ls != nil {
			ls.cancel()
		}

		containerIDs = append([]string(nil), r.stopOrder[task.TaskArn]...)
		if len(containerIDs) == 0 {
			containerIDs = append([]string(nil), r.containers[task.TaskArn]...)
		}

		volumes = r.taskVolumes[task.TaskArn]
	}()

	if len(containerIDs) == 0 {
		return nil
	}

	slices.Reverse(containerIDs)

	ctx := r.svcCtx

	var (
		errs   []error
		failed []string
	)

	for _, containerID := range containerIDs {
		r.mu.Lock()
		timeout, ok := r.stopTimeouts[containerID]
		r.mu.Unlock()

		if !ok {
			timeout = rollbackStopTimeoutSeconds
		}

		if err := r.cli.ContainerStop(ctx, containerID, dockertypes.StopOptions{Timeout: &timeout}); err != nil {
			errs = append(errs, fmt.Errorf("stop container %s: %w", containerID, err))
			failed = append(failed, containerID)

			continue
		}

		r.cancelLogForwarding(containerID)
		r.cancelContainerWait(containerID)
		r.closeFronts(containerID)
	}

	if len(failed) == 0 && len(volumes) > 0 {
		for _, id := range containerIDs {
			_ = r.cli.ContainerRemove(ctx, id, dockertypes.RemoveOptions{Force: true, RemoveVolumes: true})
		}

		r.removeVolumes(ctx, volumes)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(failed) == 0 {
		delete(r.containers, task.TaskArn)
		delete(r.stopOrder, task.TaskArn)
		delete(r.taskVolumes, task.TaskArn)

		for _, id := range containerIDs {
			delete(r.exits, id)
			delete(r.stopTimeouts, id)
		}
	} else {
		slices.Reverse(failed)
		r.containers[task.TaskArn] = failed
		r.stopOrder[task.TaskArn] = failed
	}

	return errors.Join(errs...)
}
