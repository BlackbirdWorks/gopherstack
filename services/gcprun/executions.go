package gcprun

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
)

type taskState string

const (
	statePending   taskState = "pending"
	stateRunning   taskState = "running"
	stateSucceeded taskState = "succeeded"
	stateFailed    taskState = "failed"
	stateCancelled taskState = "cancelled"
)

const execSuffixLen = 5

func (s taskState) terminal() bool {
	return s == stateSucceeded || s == stateFailed || s == stateCancelled
}

func applyOverrides(tt *runpb.TaskTemplate, ov *runpb.RunJobRequest_Overrides) error {
	if ov.GetTimeout() != nil {
		tt.Timeout = ov.GetTimeout()
	}

	for _, co := range ov.GetContainerOverrides() {
		c := matchContainer(tt.GetContainers(), co.GetName())
		if c == nil {
			return errInvalid("Container override %q does not match any container of the job.", co.GetName())
		}

		if co.GetClearArgs() {
			c.Args = nil
		} else if len(co.GetArgs()) > 0 {
			c.Args = co.GetArgs()
		}

		if err := validateEnv(co.GetEnv()); err != nil {
			return err
		}

		c.Env = mergeEnv(c.GetEnv(), co.GetEnv())
	}

	return nil
}

func matchContainer(cs []*runpb.Container, name string) *runpb.Container {
	for _, c := range cs {
		if c.GetName() == name {
			return c
		}
	}

	if name == "" && len(cs) > 0 {
		return cs[0]
	}

	return nil
}

func mergeEnv(base, over []*runpb.EnvVar) []*runpb.EnvVar {
	for _, o := range over {
		replaced := false

		for i, e := range base {
			if e.GetName() == o.GetName() {
				base[i], replaced = o, true
			}
		}

		if !replaced {
			base = append(base, o)
		}
	}

	return base
}

func (b *InMemoryBackend) newExecutionLocked(
	job *runpb.Job,
	ov *runpb.RunJobRequest_Overrides,
) (*execRec, []*taskRec, error) {
	et := job.GetTemplate()
	tt := clone(et.GetTemplate())

	if err := applyOverrides(tt, ov); err != nil {
		return nil, nil, err
	}

	count := et.GetTaskCount()
	if ov.GetTaskCount() > 0 {
		count = ov.GetTaskCount()
	}

	parallelism := et.GetParallelism()
	if parallelism <= 0 || parallelism > count {
		parallelism = count
	}

	now, uid := b.ts(), newUID()
	id := lastSegment(job.GetName()) + "-" + shortHash(uid)[:execSuffixLen]
	name := job.GetName() + "/executions/" + id

	ex := &runpb.Execution{
		Name: name, Uid: uid, Generation: 1, Labels: et.GetLabels(), Annotations: et.GetAnnotations(),
		CreateTime: now, UpdateTime: now, LaunchStage: job.GetLaunchStage(), Job: job.GetName(),
		Parallelism: parallelism, TaskCount: count, Template: tt, Reconciling: true, ObservedGeneration: 1,
		Conditions: []*runpb.Condition{
			newCondition("Completed", runpb.Condition_CONDITION_RECONCILING, "Execution is running.", now),
		},
	}
	ex.Etag = etag(uid, 1, now)

	rec := &execRec{pb: ex, done: make(chan struct{})}
	tasks := make([]*taskRec, 0, count)

	for i := range count {
		tasks = append(tasks, b.newTaskRec(ex, id, i))
	}

	return rec, tasks, nil
}

func (b *InMemoryBackend) newTaskRec(ex *runpb.Execution, execID string, idx int32) *taskRec {
	now, tt := b.ts(), ex.GetTemplate()

	return &taskRec{state: statePending, pb: &runpb.Task{
		Name:                 ex.GetName() + "/tasks/" + execID + "-task" + strconv.Itoa(int(idx)),
		Uid:                  newUID(),
		Generation:           1,
		CreateTime:           now,
		ScheduledTime:        now,
		UpdateTime:           now,
		Job:                  ex.GetJob(),
		Execution:            ex.GetName(),
		Containers:           clone(tt).GetContainers(),
		Volumes:              tt.GetVolumes(),
		MaxRetries:           tt.GetMaxRetries(),
		Timeout:              tt.GetTimeout(),
		ServiceAccount:       tt.GetServiceAccount(),
		ExecutionEnvironment: tt.GetExecutionEnvironment(),
		Reconciling:          true,
		ObservedGeneration:   1,
		Index:                idx,
		Conditions: []*runpb.Condition{
			newCondition("Completed", runpb.Condition_CONDITION_PENDING, "Task is waiting to start.", now),
		},
	}}
}

// RunJob starts an execution of the job and returns an operation that resolves when it completes.
func (b *InMemoryBackend) RunJob(in *runpb.RunJobRequest) (*longrunningpb.Operation, error) {
	n, err := parseName(in.GetName(), "jobs")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("RunJob")

	job, ok := b.jobs[in.GetName()]
	if !ok {
		b.mu.Unlock()

		return nil, errNotFound("JOB", n.ids[0], n.location, n.project)
	}

	if in.GetEtag() != "" && in.GetEtag() != job.GetEtag() {
		b.mu.Unlock()

		return nil, errPrecondition("The etag %q does not match the job's current etag.", in.GetEtag())
	}

	ex, tasks, err := b.newExecutionLocked(job, in.GetOverrides())
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	if in.GetValidateOnly() {
		view := clone(ex.pb)
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), view, view)
	}

	ctx, cancel := context.WithCancel(b.runCtx)
	ex.cancel = cancel

	b.executions[ex.pb.GetName()] = ex
	for _, t := range tasks {
		b.tasks[t.pb.GetName()] = t
	}

	job.ExecutionCount++
	job.LatestCreatedExecution = &runpb.ExecutionReference{Name: ex.pb.GetName(), CreateTime: ex.pb.GetCreateTime()}
	view := clone(ex.pb)
	b.mu.Unlock()

	name := opName(n.parent())

	op, err := b.pendingOp(name, view)
	if err != nil {
		cancel()

		return nil, err
	}

	b.wg.Go(func() { b.runExecution(ctx, ex.pb.GetName(), name) })

	return op, nil
}

func (b *InMemoryBackend) runExecution(ctx context.Context, execName, op string) {
	b.mu.RLock("runExecution")
	parallelism := int(b.executions[execName].pb.GetParallelism())
	names := sortedKeys(b.tasks, execName+"/tasks/")
	b.mu.RUnlock()

	sem := make(chan struct{}, max(parallelism, 1))

	wg := &sync.WaitGroup{}

loop:
	for _, tn := range names {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break loop
		}

		wg.Go(func() {
			defer func() { <-sem }()

			b.runTask(ctx, execName, tn)
		})
	}

	wg.Wait()
	b.completeExecution(execName, op)
}

// completeExecution cancels leftover tasks, stamps the execution complete and resolves its operation.
func (b *InMemoryBackend) completeExecution(execName, op string) {
	b.mu.Lock("completeExecution")

	ex, ok := b.executions[execName]
	if !ok || ex.pb.GetCompletionTime() != nil {
		b.mu.Unlock()

		return
	}

	for _, tn := range sortedKeys(b.tasks, execName+"/tasks/") {
		if t := b.tasks[tn]; !t.state.terminal() {
			b.finishTaskLocked(t, stateCancelled, "Task did not run: execution was cancelled.", 0)
		}
	}

	b.recountLocked(ex)
	b.stampCompleteLocked(ex)
	view := clone(ex.pb)
	ex.cancel()
	close(ex.done)
	b.mu.Unlock()

	b.finishOp(op, view, view)
}

func (b *InMemoryBackend) stampCompleteLocked(ex *execRec) {
	at, pb := b.ts(), ex.pb
	pb.CompletionTime, pb.UpdateTime, pb.Reconciling = at, at, false

	state, msg := runpb.Condition_CONDITION_SUCCEEDED, ""

	switch {
	case pb.GetCancelledCount() > 0:
		state, msg = runpb.Condition_CONDITION_FAILED, fmt.Sprintf("%d task(s) cancelled.", pb.GetCancelledCount())
	case pb.GetFailedCount() > 0:
		state, msg = runpb.Condition_CONDITION_FAILED, fmt.Sprintf("%d task(s) failed.", pb.GetFailedCount())
	}

	pb.Conditions = []*runpb.Condition{newCondition("Completed", state, msg, at)}

	if job, ok := b.jobs[pb.GetJob()]; ok && job.GetLatestCreatedExecution().GetName() == pb.GetName() {
		job.LatestCreatedExecution.CompletionTime = at
		job.LatestCreatedExecution.CompletionStatus = completionStatus(state)
	}
}

func completionStatus(s runpb.Condition_State) runpb.ExecutionReference_CompletionStatus {
	if s == runpb.Condition_CONDITION_SUCCEEDED {
		return runpb.ExecutionReference_EXECUTION_SUCCEEDED
	}

	return runpb.ExecutionReference_EXECUTION_FAILED
}

// recountLocked refreshes the execution's task counters from its tasks.
func (b *InMemoryBackend) recountLocked(ex *execRec) {
	pb := ex.pb
	pb.RunningCount, pb.SucceededCount, pb.FailedCount, pb.CancelledCount, pb.RetriedCount = 0, 0, 0, 0, 0

	for _, tn := range sortedKeys(b.tasks, pb.GetName()+"/tasks/") {
		t := b.tasks[tn]
		pb.RetriedCount += t.pb.GetRetried()

		switch t.state {
		case stateRunning:
			pb.RunningCount++
		case stateSucceeded:
			pb.SucceededCount++
		case stateFailed:
			pb.FailedCount++
		case stateCancelled:
			pb.CancelledCount++
		case statePending:
		}
	}

	pb.UpdateTime = b.ts()
}

func (b *InMemoryBackend) purgeExecutionsLocked(prefix string) {
	for _, name := range sortedKeys(b.executions, prefix) {
		for _, tn := range sortedKeys(b.tasks, name+"/tasks/") {
			delete(b.tasks, tn)
		}

		delete(b.executions, name)
	}
}

// GetExecution returns the named execution.
func (b *InMemoryBackend) GetExecution(name string) (*runpb.Execution, error) {
	n, err := parseName(name, "jobs", "executions")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetExecution")
	defer b.mu.RUnlock()

	e, ok := b.executions[name]
	if !ok {
		return nil, errNotFound("EXECUTION", n.ids[1], n.location, n.project)
	}

	return clone(e.pb), nil
}

// ListExecutions lists a job's executions, newest name last.
func (b *InMemoryBackend) ListExecutions(in *runpb.ListExecutionsRequest) ([]*runpb.Execution, string, error) {
	n, err := parseName(in.GetParent(), "jobs")
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListExecutions")

	if _, ok := b.jobs[in.GetParent()]; !ok {
		b.mu.RUnlock()

		return nil, "", errNotFound("JOB", n.ids[0], n.location, n.project)
	}

	all := collect(
		b.executions,
		in.GetParent()+"/executions/",
		func(e *execRec) *runpb.Execution { return clone(e.pb) },
	)
	b.mu.RUnlock()

	return paginate(all, in.GetPageToken(), in.GetPageSize())
}

// CancelExecution stops a running execution and returns once its tasks have been cancelled.
func (b *InMemoryBackend) CancelExecution(
	ctx context.Context, in *runpb.CancelExecutionRequest,
) (*longrunningpb.Operation, error) {
	n, err := parseName(in.GetName(), "jobs", "executions")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("CancelExecution")
	ex, ok := b.executions[in.GetName()]

	var (
		done    chan struct{}
		etagBad bool
		over    bool
	)

	if ok {
		done, over = ex.done, ex.pb.GetCompletionTime() != nil
		etagBad = in.GetEtag() != "" && in.GetEtag() != ex.pb.GetEtag()
	}
	b.mu.RUnlock()

	switch {
	case !ok:
		return nil, errNotFound("EXECUTION", n.ids[1], n.location, n.project)
	case etagBad:
		return nil, errPrecondition("The etag %q does not match the execution's current etag.", in.GetEtag())
	case over:
		return nil, errPrecondition("Execution '%s' has already completed.", n.ids[1])
	}

	if in.GetValidateOnly() {
		return b.doneOp(opName(n.parent()), ex.pb, ex.pb)
	}

	ex.cancel()

	if err = waitDone(ctx, done); err != nil {
		return nil, err
	}

	view, err := b.GetExecution(in.GetName())
	if err != nil {
		return nil, err
	}

	return b.doneOp(opName(n.parent()), view, view)
}

func waitDone(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errPrecondition("Timed out waiting for the execution to stop: %v", ctx.Err())
	}
}

// DeleteExecution cancels a running execution and removes it with its tasks.
func (b *InMemoryBackend) DeleteExecution(
	ctx context.Context, in *runpb.DeleteExecutionRequest,
) (*longrunningpb.Operation, error) {
	n, err := parseName(in.GetName(), "jobs", "executions")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("DeleteExecution")
	ex, ok := b.executions[in.GetName()]
	b.mu.RUnlock()

	if !ok {
		return nil, errNotFound("EXECUTION", n.ids[1], n.location, n.project)
	}

	if in.GetEtag() != "" && in.GetEtag() != ex.pb.GetEtag() {
		return nil, errPrecondition("The etag %q does not match the execution's current etag.", in.GetEtag())
	}

	if in.GetValidateOnly() {
		return b.doneOp(opName(n.parent()), ex.pb, ex.pb)
	}

	ex.cancel()

	if err = waitDone(ctx, ex.done); err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteExecution")
	view := clone(ex.pb)
	view.DeleteTime = b.ts()
	b.purgeExecutionsLocked(in.GetName())
	b.mu.Unlock()

	return b.doneOp(opName(n.parent()), view, view)
}

// GetTask returns the named task.
func (b *InMemoryBackend) GetTask(name string) (*runpb.Task, error) {
	n, err := parseName(name, "jobs", "executions", "tasks")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetTask")
	defer b.mu.RUnlock()

	t, ok := b.tasks[name]
	if !ok {
		return nil, errNotFound("TASK", n.ids[2], n.location, n.project)
	}

	return clone(t.pb), nil
}

// ListTasks lists an execution's tasks by index.
func (b *InMemoryBackend) ListTasks(in *runpb.ListTasksRequest) ([]*runpb.Task, string, error) {
	n, err := parseName(in.GetParent(), "jobs", "executions")
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListTasks")

	if _, ok := b.executions[in.GetParent()]; !ok {
		b.mu.RUnlock()

		return nil, "", errNotFound("EXECUTION", n.ids[1], n.location, n.project)
	}

	all := collect(b.tasks, in.GetParent()+"/tasks/", func(t *taskRec) *runpb.Task { return clone(t.pb) })
	b.mu.RUnlock()

	sortTasksByIndex(all)

	return paginate(all, in.GetPageToken(), in.GetPageSize())
}

func sortTasksByIndex(ts []*runpb.Task) {
	slices.SortFunc(ts, func(a, b *runpb.Task) int { return int(a.GetIndex() - b.GetIndex()) })
}
