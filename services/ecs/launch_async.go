package ecs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// contextRunner is implemented by runners whose launch can be cancelled through a context.
type contextRunner interface {
	RunTaskContext(ctx context.Context, task *Task, td *TaskDefinition) error
}

// launchTracker counts in-flight launch goroutines. idle is non-nil while n > 0 and closes when n returns to 0.
// All fields are guarded by InMemoryBackend.mu.
type launchTracker struct {
	cancels map[string]context.CancelFunc
	tasks   map[string]*Task
	idle    chan struct{}
	n       int
	closed  bool
}

func isLaunchingStatus(status string) bool {
	return status == statusProvisioning || status == statusPending || status == statusActivating
}

func (b *InMemoryBackend) clock() time.Time {
	if b.nowFn != nil {
		return b.nowFn()
	}

	return time.Now()
}

// markRunningLocked moves task to RUNNING and stamps startedAt and connectivityAt.
func (b *InMemoryBackend) markRunningLocked(task *Task, now time.Time) {
	task.LastStatus = statusRunning
	task.StartedAt = &now
	task.Connectivity = connectivityConnected
	task.ConnectivityAt = &now
	syncContainerStatuses(task, nil)
}

// launchAsync starts w's launch goroutine; the task stays PROVISIONING until it picks the task up.
func (b *InMemoryBackend) launchAsync(w taskWork, clusterName string) {
	ctx, cancel := context.WithCancel(context.Background())

	b.mu.Lock("launchAsync")

	lt := &b.launches
	if lt.closed {
		b.mu.Unlock()
		cancel()

		return
	}

	if lt.cancels == nil {
		lt.cancels = make(map[string]context.CancelFunc)
		lt.tasks = make(map[string]*Task)
	}

	if lt.n == 0 {
		lt.idle = make(chan struct{})
	}

	lt.n++
	lt.cancels[w.task.TaskArn] = cancel
	lt.tasks[w.task.TaskArn] = w.task
	b.mu.Unlock()

	go func() {
		defer b.finishLaunch(w.task.TaskArn)
		defer cancel()

		b.driveLaunch(ctx, w, clusterName)
	}()
}

func (b *InMemoryBackend) finishLaunch(taskArn string) {
	b.mu.Lock("finishLaunch")
	defer b.mu.Unlock()

	lt := &b.launches
	delete(lt.cancels, taskArn)
	delete(lt.tasks, taskArn)
	lt.n--

	if lt.n == 0 {
		close(lt.idle)
	}
}

// cancelLaunchLocked cancels taskArn's in-flight launch, if any.
func (b *InMemoryBackend) cancelLaunchLocked(taskArn string) {
	if cancel := b.launches.cancels[taskArn]; cancel != nil {
		cancel()
	}
}

// cancelLaunches cancels every in-flight launch and returns their tasks and the channel closing when all end.
func (b *InMemoryBackend) cancelLaunches(closing bool) ([]*Task, <-chan struct{}) {
	b.mu.Lock("cancelLaunches")
	defer b.mu.Unlock()

	lt := &b.launches
	if closing {
		lt.closed = true
	}

	pending := make([]*Task, 0, len(lt.tasks))

	for arn, cancel := range lt.cancels {
		cancel()

		pending = append(pending, lt.tasks[arn])
	}

	if lt.n == 0 {
		return pending, nil
	}

	return pending, lt.idle
}

// Close cancels in-flight task launches and waits for them to finish; later launches are ignored.
func (b *InMemoryBackend) Close() {
	pending, idle := b.cancelLaunches(true)
	if idle == nil {
		return
	}

	if b.runner != nil {
		for _, t := range pending {
			_ = b.runner.StopTask(t)
		}
	}

	<-idle
}

// stepLaunch moves a launching task from one status to the next. It reports false when the task was stopped,
// reset away or its launch cancelled in the meantime.
func (b *InMemoryBackend) stepLaunch(
	ctx context.Context, task *Task, from, to string, mutate func(now time.Time),
) bool {
	b.mu.Lock("stepLaunch")
	defer b.mu.Unlock()

	if ctx.Err() != nil || task.LastStatus != from || !b.isLiveTaskLocked(task) {
		return false
	}

	task.LastStatus = to
	if mutate != nil {
		mutate(b.clock())
	}

	b.emitTaskStateLocked(task)

	return true
}

func (b *InMemoryBackend) isLiveTaskLocked(task *Task) bool {
	cur, ok := b.tasks.Get(task.TaskArn)

	return ok && cur == task
}

// driveLaunch walks one task PROVISIONING, PENDING, ACTIVATING, RUNNING (or STOPPED on failure).
func (b *InMemoryBackend) driveLaunch(ctx context.Context, w taskWork, clusterName string) {
	task := w.task

	if !b.stepLaunch(ctx, task, statusProvisioning, statusPending, nil) {
		b.settleCancelled(ctx, task, clusterName)

		return
	}

	b.mu.Lock("driveLaunch-runtime")
	_, task.runtimeHealth = b.runner.(containerHealthRunner)
	b.mu.Unlock()

	activated := b.stepLaunch(ctx, task, statusPending, statusActivating, func(now time.Time) {
		task.PullStartedAt = &now
	})
	if !activated {
		b.settleCancelled(ctx, task, clusterName)

		return
	}

	var runErr error
	if cr, ok := b.runner.(contextRunner); ok {
		runErr = cr.RunTaskContext(ctx, task, w.td)
	} else {
		runErr = b.runner.RunTask(task, w.td)
	}

	b.completeLaunch(ctx, w, clusterName, runErr)
}

func (b *InMemoryBackend) completeLaunch(ctx context.Context, w taskWork, clusterName string, runErr error) {
	task := w.task
	superseded := false

	func() {
		b.mu.Lock("completeLaunch")
		defer b.mu.Unlock()

		if task.LastStatus != statusActivating || !b.isLiveTaskLocked(task) {
			superseded = true

			return
		}

		if ctx.Err() != nil {
			superseded = true
			b.stopCancelledLaunchLocked(task, clusterName)

			return
		}

		now := b.clock()
		task.PullStoppedAt = &now

		if runErr == nil {
			b.applyRuntimeContainerIDs(task)
			b.markRunningLocked(task, now)
			b.emitTaskStateLocked(task)

			if c, _ := b.clusters.Get(clusterName); c != nil {
				c.PendingTasksCount--
				c.RunningTasksCount++
			}

			b.registerTaskWithELBv2Locked(task, clusterName)
			b.registerTaskWithCloudMapLocked(task, clusterName)

			return
		}

		b.failLaunchLocked(task, clusterName, runErr, now)
	}()

	if superseded && runErr == nil && b.runner != nil {
		if err := b.runner.StopTask(task); err != nil {
			logger.Load(ctx).WarnContext(ctx, "failed to clean up superseded task launch",
				"task", task.TaskArn, "error", err)
		}
	}
}

// failLaunchLocked marks task STOPPED/TaskFailedToStart after its runner failed.
func (b *InMemoryBackend) failLaunchLocked(task *Task, clusterName string, runErr error, now time.Time) {
	task.LastStatus = statusStopped
	task.DesiredStatus = statusStopped
	task.StoppedAt = &now
	task.StoppedReason = fmt.Sprintf("container start failed: %v", runErr)
	task.StopCode = stopCodeTaskFailedToStart

	if launchErr, ok := errors.AsType[*taskLaunchError](runErr); ok {
		task.StoppedReason = launchErr.Reason
	}

	exitCode := 1
	syncContainerStatuses(task, &exitCode)
	b.emitTaskStateLocked(task)

	if c, _ := b.clusters.Get(clusterName); c != nil {
		c.PendingTasksCount--
	}

	b.recordServiceTaskFailureLocked(clusterName, task)
}

// awaitLaunches blocks until no launch goroutine is in flight.
func (b *InMemoryBackend) awaitLaunches() {
	b.mu.RLock("awaitLaunches")
	idle := b.launches.idle
	busy := b.launches.n > 0
	b.mu.RUnlock()

	if busy {
		<-idle
	}
}

// stopCancelledLaunchLocked stops a task whose launch was cancelled by backend shutdown rather than by StopTask.
func (b *InMemoryBackend) stopCancelledLaunchLocked(task *Task, clusterName string) {
	now := b.clock()
	task.LastStatus = statusStopped
	task.DesiredStatus = statusStopped
	task.StoppedAt = &now
	task.StopCode = stopCodeTaskFailedToStart
	task.StoppedReason = "Task launch cancelled: the backend is shutting down"
	syncContainerStatuses(task, nil)
	b.emitTaskStateLocked(task)

	if c, _ := b.clusters.Get(clusterName); c != nil {
		c.PendingTasksCount--
	}
}

// settleCancelled stops a task whose launch was cancelled before reaching the runtime, if nothing else did.
func (b *InMemoryBackend) settleCancelled(ctx context.Context, task *Task, clusterName string) {
	if ctx.Err() == nil {
		return
	}

	b.mu.Lock("settleCancelled")
	defer b.mu.Unlock()

	if b.isLiveTaskLocked(task) && isLaunchingStatus(task.LastStatus) {
		b.stopCancelledLaunchLocked(task, clusterName)
	}
}
