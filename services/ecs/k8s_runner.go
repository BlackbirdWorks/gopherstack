package ecs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	corev1 "k8s.io/api/core/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	envContainerRuntime     = "CONTAINER_RUNTIME"
	k8sKilledExitCode       = 137
	k8sOOMKilledReason      = "OOMKilled"
	k8sPullReasonErrImage   = "ErrImagePull"
	k8sPullReasonBackoff    = "ImagePullBackOff"
	k8sPullReasonInvalid    = "InvalidImageName"
	pullFailureReasonPrefix = "CannotPullContainerError: "
)

// k8sCluster is the Pod API the runner needs; *container.KubernetesRuntime implements it.
type k8sCluster interface {
	LaunchPod(ctx context.Context, l container.PodLaunch) (*corev1.Pod, error)
	DeletePod(ctx context.Context, name string, withSecret bool) error
	FollowPod(ctx context.Context, name string, fn func(pod *corev1.Pod, gone bool) bool) error
	StreamPodLogs(ctx context.Context, pod, containerName string, w io.Writer) error
}

// k8sTask is the runner's record of one task's Pod.
type k8sTask struct {
	follow     context.CancelFunc
	logs       context.CancelFunc
	pod        string
	ip         string
	containers []k8sContainer
	logWG      sync.WaitGroup
	hasSecret  bool
}

// k8sRunner is a TaskRunner that runs each task as one Pod: containers share its network namespace, as under
// awsvpc. Hooks mirror realDockerRunner's so the backend wires both identically.
type k8sRunner struct {
	cluster           k8sCluster
	tasks             map[string]*k8sTask
	launching         map[string]*launchState
	mu                *lockmetrics.RWMutex
	svcCtx            context.Context
	cwLogs            CWLogsBackend
	completionHandler func(taskArn, containerName string, exitCode int)
	healthHandler     func(taskArn, containerName, status string)
	reasonHandler     func(taskArn, containerName, reason string)
	secretLookup      func(ctx context.Context, valueFrom string) (string, error)
	envFileLookup     func(ctx context.Context, cd ContainerDefinition) ([]string, error)
	taskEnv           func(task *Task, containerName string) []string
}

// NewKubernetesRunner creates a TaskRunner backed by the Kubernetes API selected by CONTAINER_K8S_* settings.
func NewKubernetesRunner(ctx context.Context) (TaskRunner, error) {
	return NewKubernetesRunnerWithConfig(ctx, container.KubernetesConfig{})
}

// NewKubernetesRunnerWithConfig is NewKubernetesRunner with explicit cluster settings; unset fields come from
// the environment.
func NewKubernetesRunnerWithConfig(ctx context.Context, kc container.KubernetesConfig) (TaskRunner, error) {
	rt, err := container.NewRuntime(container.Config{Runtime: container.RuntimeKubernetes, Kubernetes: &kc})
	if err != nil {
		return nil, fmt.Errorf("create kubernetes runtime: %w", err)
	}

	k, ok := rt.(*container.KubernetesRuntime)
	if !ok {
		return nil, fmt.Errorf("%w: kubernetes runtime has type %T", container.ErrUnknownRuntime, rt)
	}

	return newK8sRunner(ctx, k), nil
}

func newK8sRunner(ctx context.Context, cluster k8sCluster) *k8sRunner {
	if ctx == nil {
		ctx = context.Background()
	}

	return &k8sRunner{
		cluster:   cluster,
		tasks:     make(map[string]*k8sTask),
		launching: make(map[string]*launchState),
		mu:        lockmetrics.New("ecs.k8s"),
		svcCtx:    ctx,
	}
}

func usesKubernetes() bool {
	return os.Getenv(envContainerRuntime) == string(container.RuntimeKubernetes)
}

// SetCWLogsBackend wires CloudWatch Logs so awslogs-driver containers' output is forwarded from the Pod.
func (r *k8sRunner) SetCWLogsBackend(cwl CWLogsBackend) {
	r.mu.Lock("SetCWLogsBackend")
	defer r.mu.Unlock()

	r.cwLogs = cwl
}

// SetTaskCompletionHandler wires fn to receive each container exit observed on the Pod.
func (r *k8sRunner) SetTaskCompletionHandler(fn func(taskArn, containerName string, exitCode int)) {
	r.mu.Lock("SetTaskCompletionHandler")
	defer r.mu.Unlock()

	r.completionHandler = fn
}

// SetTaskEnvProvider wires the ECS runtime environment injected into every container.
func (r *k8sRunner) SetTaskEnvProvider(fn func(task *Task, containerName string) []string) {
	r.mu.Lock("SetTaskEnvProvider")
	defer r.mu.Unlock()

	r.taskEnv = fn
}

// SetContainerHealthHandler wires fn to receive readiness-derived container health.
func (r *k8sRunner) SetContainerHealthHandler(fn func(taskArn, containerName, status string)) {
	r.mu.Lock("SetContainerHealthHandler")
	defer r.mu.Unlock()

	r.healthHandler = fn
}

// SetContainerReasonHandler wires fn to receive a container's failure reason (an OOM kill).
func (r *k8sRunner) SetContainerReasonHandler(fn func(taskArn, containerName, reason string)) {
	r.mu.Lock("SetContainerReasonHandler")
	defer r.mu.Unlock()

	r.reasonHandler = fn
}

// SetSecretResolver wires the lookup that turns a container secret's valueFrom into its value.
func (r *k8sRunner) SetSecretResolver(fn func(ctx context.Context, valueFrom string) (string, error)) {
	r.mu.Lock("SetSecretResolver")
	defer r.mu.Unlock()

	r.secretLookup = fn
}

// SetEnvFileResolver wires the lookup that reads a container's environmentFiles.
func (r *k8sRunner) SetEnvFileResolver(fn func(ctx context.Context, cd ContainerDefinition) ([]string, error)) {
	r.mu.Lock("SetEnvFileResolver")
	defer r.mu.Unlock()

	r.envFileLookup = fn
}

type k8sHooks struct {
	cwLogs     CWLogsBackend
	completion func(taskArn, containerName string, exitCode int)
	health     func(taskArn, containerName, status string)
	reason     func(taskArn, containerName, reason string)
}

func (r *k8sRunner) hooks() k8sHooks {
	r.mu.RLock("hooks")
	defer r.mu.RUnlock()

	return k8sHooks{
		cwLogs: r.cwLogs, completion: r.completionHandler, health: r.healthHandler, reason: r.reasonHandler,
	}
}

// RunTask starts the task's Pod and returns once it is running or has run.
func (r *k8sRunner) RunTask(task *Task, td *TaskDefinition) error {
	return r.RunTaskContext(r.svcCtx, task, td)
}

// RunTaskContext is RunTask with ctx cancelling the launch and removing the Pod.
func (r *k8sRunner) RunTaskContext(parent context.Context, task *Task, td *TaskDefinition) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	plan, podName, err := r.planTask(ctx, task, td)
	if err != nil {
		return err
	}

	ls := &launchState{abort: make(chan struct{}), stop: cancel}

	r.mu.Lock("RunTask-launching")
	r.launching[task.TaskArn] = ls
	r.mu.Unlock()

	defer func() {
		r.mu.Lock("RunTask-launched")
		delete(r.launching, task.TaskArn)
		r.mu.Unlock()
	}()

	pod, err := r.cluster.LaunchPod(ctx, container.PodLaunch{
		Name:   podName,
		Labels: container.ManagedLabels("ecs", ecsResourceKey(task.TaskArn, "")),
		Spec:   plan.spec,
		Secret: plan.secret,
	})
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w", errTaskStoppedDuringLaunch, ctx.Err())
		}

		return k8sLaunchError(err)
	}

	select {
	case <-ls.abort:
		r.deletePodQuietly(podName, plan.secret != nil)

		return errTaskStoppedDuringLaunch
	case <-ctx.Done():
		r.deletePodQuietly(podName, plan.secret != nil)

		return fmt.Errorf("%w: %w", errTaskStoppedDuringLaunch, ctx.Err())
	default:
	}

	r.adopt(task, plan, podName, pod)

	return nil
}

func (r *k8sRunner) deletePodQuietly(pod string, withSecret bool) {
	if err := r.cluster.DeletePod(context.WithoutCancel(r.svcCtx), pod, withSecret); err != nil {
		logger.Load(r.svcCtx).WarnContext(r.svcCtx, "failed to delete pod", "pod", pod, "error", err)
	}
}

// planTask resolves the task's secrets and environment and maps it onto a Pod.
func (r *k8sRunner) planTask(ctx context.Context, task *Task, td *TaskDefinition) (*k8sPlan, string, error) {
	orderIdx, err := orderContainers(td.ContainerDefinitions)
	if err != nil {
		return nil, "", &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
	}

	order := make([]ContainerDefinition, len(orderIdx))
	for pos, i := range orderIdx {
		order[pos] = td.ContainerDefinitions[i]
	}

	if fireErr := checkFirelens(order); fireErr != nil {
		return nil, "", fireErr
	}

	r.mu.RLock("planTask")
	secretLookup, fileLookup, envFn := r.secretLookup, r.envFileLookup, r.taskEnv
	r.mu.RUnlock()

	secretEnv, err := resolveSecretEnv(ctx, secretLookup, order)
	if err != nil {
		return nil, "", err
	}

	fileEnv, err := resolveFileEnv(ctx, fileLookup, order)
	if err != nil {
		return nil, "", err
	}

	plain := make(map[string][]string, len(order))
	sensitive := make(map[string][]string, len(order))

	for _, cd := range order {
		if envFn != nil {
			plain[cd.Name] = envFn(task, cd.Name)
		}

		sensitive[cd.Name] = append(sensitive[cd.Name], fileEnv[cd.Name]...)
		plain[cd.Name] = append(plain[cd.Name], buildEnv(cd.Environment)...)
		sensitive[cd.Name] = append(sensitive[cd.Name], secretEnv[cd.Name]...)
	}

	podName := k8sPodName(taskIDFromARN(task.TaskArn))

	plan, err := planK8sPod(k8sPlanInput{
		td: td, order: order, plain: plain, sensitive: sensitive, podName: podName,
	})
	if err != nil {
		return nil, "", &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
	}

	return plan, podName, nil
}

// k8sLaunchError maps a Pod launch failure to the stoppedReason ECS reports.
func k8sLaunchError(err error) error {
	if pf, ok := errors.AsType[*container.PodFailedError](err); ok {
		switch pf.Reason {
		case k8sPullReasonErrImage, k8sPullReasonBackoff, k8sPullReasonInvalid:
			return &taskLaunchError{
				Reason: fmt.Sprintf(
					"%scontainer %q: %s: %s",
					pullFailureReasonPrefix,
					pf.Container,
					pf.Reason,
					pf.Message,
				),
				Err: err,
			}
		}
	}

	return &taskLaunchError{Reason: "Task failed to start: " + err.Error(), Err: err}
}

// adopt records the launched Pod and starts the goroutines that follow it and forward its logs.
func (r *k8sRunner) adopt(task *Task, plan *k8sPlan, podName string, pod *corev1.Pod) {
	followCtx, followCancel := context.WithCancel(r.svcCtx)
	logCtx, logCancel := context.WithCancel(r.svcCtx)

	st := &k8sTask{
		pod: podName, hasSecret: plan.secret != nil, containers: plan.containers,
		follow: followCancel, logs: logCancel, ip: pod.Status.PodIP,
	}

	r.mu.Lock("adopt")
	r.tasks[task.TaskArn] = st
	r.mu.Unlock()

	h := r.hooks()
	r.forwardLogs(logCtx, h.cwLogs, task, st)

	f := &k8sFollower{r: r, task: task, st: st, h: h, reported: map[string]bool{}, health: map[string]string{}}

	go func() {
		defer followCancel()

		if err := r.cluster.FollowPod(followCtx, podName, f.observe); err != nil && followCtx.Err() == nil {
			logger.Load(followCtx).WarnContext(followCtx, "pod watch ended", "pod", podName, "error", err)
		}
	}()
}

// forwardLogs streams every awslogs container's output into CloudWatch Logs.
func (r *k8sRunner) forwardLogs(ctx context.Context, cwl CWLogsBackend, task *Task, st *k8sTask) {
	if cwl == nil {
		return
	}

	taskID := taskIDFromARN(task.TaskArn)

	for _, c := range st.containers {
		runtimeID := st.pod + "/" + c.name

		group, stream, ok := awslogsContainerTarget(c.def, taskID, runtimeID)
		if !ok {
			continue
		}

		_ = cwl.EnsureLogGroupAndStream(group, stream)

		st.logWG.Go(func() {
			w := &awslogsWriter{cwLogs: cwl, group: group, stream: stream}
			_ = r.cluster.StreamPodLogs(ctx, st.pod, c.name, w)

			w.flush()
		})
	}
}

// k8sFollower turns Pod state changes into ECS container events.
type k8sFollower struct {
	r        *k8sRunner
	task     *Task
	st       *k8sTask
	reported map[string]bool
	health   map[string]string
	h        k8sHooks
}

// observe handles one Pod state; it returns true once every container has exited.
func (f *k8sFollower) observe(pod *corev1.Pod, gone bool) bool {
	if pod.Status.PodIP != "" {
		f.r.mu.Lock("observe-ip")
		f.st.ip = pod.Status.PodIP
		f.r.mu.Unlock()
	}

	statuses := make(map[string]corev1.ContainerStatus,
		len(pod.Status.ContainerStatuses)+len(pod.Status.InitContainerStatuses))
	for _, cs := range pod.Status.InitContainerStatuses {
		statuses[cs.Name] = cs
	}

	for _, cs := range pod.Status.ContainerStatuses {
		statuses[cs.Name] = cs
	}

	podEnded := gone || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
	finished := 0

	for _, c := range f.st.containers {
		cs, seen := statuses[c.name]

		switch {
		case seen && cs.State.Terminated != nil:
			f.exited(c, int(cs.State.Terminated.ExitCode), cs.State.Terminated.Reason)
		case podEnded:
			f.exited(c, k8sKilledExitCode, "")
		case seen:
			f.reportHealth(c, cs)
		}

		if f.reported[c.def.Name] {
			finished++
		}
	}

	return finished == len(f.st.containers)
}

func (f *k8sFollower) exited(c k8sContainer, code int, reason string) {
	if f.reported[c.def.Name] {
		return
	}

	f.reported[c.def.Name] = true

	if reason == k8sOOMKilledReason && f.h.reason != nil {
		f.h.reason(f.task.TaskArn, c.def.Name, oomReason)
	}

	if f.h.completion != nil {
		f.h.completion(f.task.TaskArn, c.def.Name, code)
	}
}

// reportHealth maps readiness to ECS health: UNKNOWN until the first success, then HEALTHY or UNHEALTHY.
func (f *k8sFollower) reportHealth(c k8sContainer, cs corev1.ContainerStatus) {
	if f.h.health == nil || c.init || k8sProbe(c.def.HealthCheck) == nil || cs.State.Running == nil {
		return
	}

	prev := f.health[c.def.Name]

	status := healthECSUnknown

	switch {
	case cs.Ready:
		status = healthECSHealthy
	case prev == healthECSHealthy || prev == healthECSUnhealthy:
		status = healthECSUnhealthy
	}

	if status == prev {
		return
	}

	f.health[c.def.Name] = status
	f.h.health(f.task.TaskArn, c.def.Name, status)
}

// ContainerIDs returns "<pod>/<container>" for each container of taskArn, in task definition order.
func (r *k8sRunner) ContainerIDs(taskArn string) []string {
	r.mu.RLock("ContainerIDs")
	defer r.mu.RUnlock()

	st := r.tasks[taskArn]
	if st == nil {
		return nil
	}

	out := make([]string, len(st.containers))
	for i, c := range st.containers {
		out[i] = st.pod + "/" + c.name
	}

	return out
}

// TaskAddress is the task's Pod IP, which is what service load balancers and discovery register.
func (r *k8sRunner) TaskAddress(taskArn string) string {
	r.mu.RLock("TaskAddress")
	defer r.mu.RUnlock()

	if st := r.tasks[taskArn]; st != nil {
		return st.ip
	}

	return ""
}

// StopTask deletes the task's Pod (and Secret), giving containers their stopTimeout to exit.
func (r *k8sRunner) StopTask(task *Task) error {
	r.mu.Lock("StopTask")

	ls := r.launching[task.TaskArn]
	st := r.tasks[task.TaskArn]

	r.mu.Unlock()

	if ls != nil {
		ls.cancel()
	}

	if st == nil {
		return nil
	}

	st.follow()

	if err := r.cluster.DeletePod(r.svcCtx, st.pod, st.hasSecret); err != nil {
		return fmt.Errorf("stop task %s: %w", task.TaskArn, err)
	}

	st.logs()
	st.logWG.Wait()

	r.mu.Lock("StopTask-forget")
	if r.tasks[task.TaskArn] == st {
		delete(r.tasks, task.TaskArn)
	}
	r.mu.Unlock()

	return nil
}
