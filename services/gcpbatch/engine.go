package gcpbatch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

const (
	cleanupBudget  = 15 * time.Second
	logDrainLimit  = 10 * time.Second
	scratchMode    = 0o755
	hostsMode      = 0o644
	scriptWrapper  = `printf '%s' "$1" > /tmp/batch-script && chmod +x /tmp/batch-script && exec /tmp/batch-script`
	exitStartError = 1
)

// execEnv is what a job runner needs, captured when the job starts.
type execEnv struct {
	rt       Runtime
	svc      Services
	script   string
	scratch  string
	hosts    string
	job      string
	project  string
	location string
	vols     []string
	logs     bool
	count    int
}

func (b *InMemoryBackend) startRunnerLocked(r *jobRec) {
	ctx, cancel := context.WithCancel(b.ctx)
	b.cancels[r.pb.GetName()] = cancel

	project, location, _ := parseJobName(r.pb.GetName())
	ex := &execEnv{
		rt: b.rt, svc: b.svc, script: b.script, job: r.pb.GetName(), project: project, location: location,
		count: len(r.Tasks), logs: r.pb.GetLogsPolicy().GetDestination() == batchpb.LogsPolicy_CLOUD_LOGGING,
	}
	g := clone(r.group())
	par := r.parallelism()

	b.wg.Go(func() {
		defer cancel()
		b.runJob(ctx, ex, g, par)
	})
}

func (b *InMemoryBackend) stopRunnerLocked(name string) {
	if c, ok := b.cancels[name]; ok {
		c()
		delete(b.cancels, name)
	}
}

// withJob runs f on the job under the write lock unless the job is gone.
func (b *InMemoryBackend) withJob(name string, f func(r *jobRec)) {
	b.mu.Lock("withJob")
	defer b.mu.Unlock()

	if r, ok := b.jobs[name]; ok {
		f(r)
	}
}

func (b *InMemoryBackend) taskEvent(ex *execEnv, i int, to int32, exit *int32, note string) {
	b.withJob(ex.job, func(r *jobRec) { r.setTask(i, to, b.now(), exit, note) })
}

func (b *InMemoryBackend) runJob(ctx context.Context, ex *execEnv, g *batchpb.TaskGroup, par int) {
	b.withJob(ex.job, func(r *jobRec) {
		now := b.now()
		r.setPhase(jobScheduled, now)
		r.setPhase(jobRunning, now)
	})

	defer func() { _ = os.RemoveAll(ex.scratch) }()

	if err := b.prepare(ex, g); err != nil {
		b.failJob(ex, err.Error())

		return
	}

	var (
		failed atomic.Bool
		wg     sync.WaitGroup
	)

	sem := make(chan struct{}, par)
	inOrder := g.GetSchedulingPolicy() == batchpb.TaskGroup_IN_ORDER

launch:
	for i := range ex.count {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break launch
		}

		if inOrder && failed.Load() {
			<-sem

			break
		}

		wg.Go(func() {
			defer func() { <-sem }()

			if !b.runTask(ctx, ex, g, i) {
				failed.Store(true)
			}
		})
	}

	wg.Wait()
	b.finishJob(ctx, ex)
}

func (b *InMemoryBackend) finishJob(ctx context.Context, ex *execEnv) {
	b.withJob(ex.job, func(r *jobRec) {
		if r.interrupted() || ctx.Err() != nil {
			return
		}

		now, failed := b.now(), false

		for i, t := range r.Tasks {
			if t.State == taskPending || t.State == taskAssigned {
				r.setTask(i, taskUnexecuted, now, nil, "an earlier task failed")
			}

			failed = failed || t.State == taskFailed
		}

		if failed {
			r.setPhase(jobFailed, now)
		} else {
			r.setPhase(jobSucceeded, now)
		}
	})
}

func (b *InMemoryBackend) failJob(ex *execEnv, note string) {
	b.withJob(ex.job, func(r *jobRec) {
		now := b.now()
		for i := range r.Tasks {
			r.setTask(i, taskFailed, now, nil, note)
		}

		r.setPhase(jobFailed, now)
	})
}

// prepare creates the scratch directories behind the task spec's volumes and the hosts file.
func (b *InMemoryBackend) prepare(ex *execEnv, g *batchpb.TaskGroup) error {
	vols := g.GetTaskSpec().GetVolumes()
	if len(vols) == 0 && !g.GetRequireHostsFile() {
		return nil
	}

	dir, err := os.MkdirTemp("", "gcpbatch-")
	if err != nil {
		return fmt.Errorf("create scratch dir: %w", err)
	}

	ex.scratch = dir

	for i, v := range vols {
		p := filepath.Join(dir, fmt.Sprintf("vol%d", i))
		if err = os.MkdirAll(p, scratchMode); err != nil {
			return fmt.Errorf("create volume dir: %w", err)
		}

		if err = os.Chmod(p, scratchMode|0o022); err != nil {
			return fmt.Errorf("chmod volume dir: %w", err)
		}

		ex.vols = append(ex.vols, p+":"+v.GetMountPath())
	}

	if g.GetRequireHostsFile() {
		var sb strings.Builder
		for i := range ex.count {
			sb.WriteString("batch-" + ex.job[strings.LastIndex(ex.job, "/")+1:] + "-" + itoa(i) + "\n")
		}

		ex.hosts = filepath.Join(dir, "hosts")
		if err = os.WriteFile(ex.hosts, []byte(sb.String()), hostsMode); err != nil {
			return fmt.Errorf("write hosts file: %w", err)
		}
	}

	return nil
}

// runTask runs one task through its attempts and reports whether it succeeded.
func (b *InMemoryBackend) runTask(ctx context.Context, ex *execEnv, g *batchpb.TaskGroup, i int) bool {
	b.taskEvent(ex, i, taskAssigned, nil, "")
	b.taskEvent(ex, i, taskRunning, nil, "")

	spec := g.GetTaskSpec()

	for attempt := int32(0); ; attempt++ {
		exit, note, aborted := b.runAttempt(ctx, ex, g, i)
		if aborted {
			return false
		}

		code := int32(exit) //nolint:gosec // exit statuses fit in int32

		if exit == 0 {
			b.taskEvent(ex, i, taskSucceeded, &code, "")

			return true
		}

		if !shouldRetry(spec, exit, attempt) {
			b.taskEvent(ex, i, taskFailed, &code, note)

			return false
		}

		b.withJob(ex.job, func(r *jobRec) {
			t := r.Tasks[i]
			t.Attempts++
			t.Events = append(t.Events, eventRec{
				Time: b.now(), Type: eventRetry, Exit: &code,
				Desc: fmt.Sprintf("Task attempt %d failed with exit code %d; retrying (%d of %d).",
					attempt+1, exit, attempt+1, spec.GetMaxRetryCount()),
			})
		})
	}
}

// shouldRetry applies the task's lifecycle policy: RETRY_TASK retries only the listed exit codes, FAIL_TASK fails
// on them and retries the rest; with no policy any non-zero exit is retried. Retries stop at max_retry_count.
func shouldRetry(spec *batchpb.TaskSpec, exit int, attempt int32) bool {
	if attempt >= spec.GetMaxRetryCount() {
		return false
	}

	if len(spec.GetLifecyclePolicies()) == 0 {
		return true
	}

	p := spec.GetLifecyclePolicies()[0]
	match := false

	for _, c := range p.GetActionCondition().GetExitCodes() {
		match = match || int(c) == exit
	}

	if p.GetAction() == batchpb.LifecyclePolicy_RETRY_TASK {
		return match
	}

	return !match
}

func (b *InMemoryBackend) deadline(
	ctx context.Context, d time.Duration, fired *atomic.Bool,
) (context.Context, context.CancelFunc) {
	ctx2, cancel := context.WithCancel(ctx)
	if d <= 0 {
		return ctx2, cancel
	}

	ch, stop := b.startTimer(d)

	go func() {
		defer stop()

		select {
		case <-ch:
			fired.Store(true)
			cancel()
		case <-ctx2.Done():
		}
	}()

	return ctx2, cancel
}

// attemptState tracks one attempt across its runnables.
type attemptState struct {
	note   string
	bg     []*proc
	exit   int
	failed bool
}

// runAttempt runs the task's runnables once. aborted means the job was cancelled, deleted or shut down.
func (b *InMemoryBackend) runAttempt(
	ctx context.Context, ex *execEnv, g *batchpb.TaskGroup, i int,
) (int, string, bool) {
	var timedOut atomic.Bool

	actx, cancel := b.deadline(ctx, g.GetTaskSpec().GetMaxRunDuration().AsDuration(), &timedOut)
	defer cancel()

	st := &attemptState{}
	defer func() { stopAll(st.bg) }()

	for n, r := range g.GetTaskSpec().GetRunnables() {
		if ctx.Err() != nil {
			return 0, "", true
		}

		if _, isBarrier := r.GetExecutable().(*batchpb.Runnable_Barrier_); isBarrier {
			continue
		}

		if (st.failed || actx.Err() != nil) && !r.GetAlwaysRun() {
			continue
		}

		b.step(actx, ex, g, i, n, r, &timedOut, st)
	}

	if ctx.Err() != nil {
		return 0, "", true
	}

	if timedOut.Load() && !st.failed {
		return exitMaxRuntime, "task exceeded max_run_duration", false
	}

	return st.exit, st.note, false
}

// step runs one runnable and folds its outcome into the attempt.
func (b *InMemoryBackend) step(
	ctx context.Context, ex *execEnv, g *batchpb.TaskGroup, i, n int, r *batchpb.Runnable,
	timedOut *atomic.Bool, st *attemptState,
) {
	p, code, msg := b.launch(ctx, ex, g, i, n, r)
	if p != nil && r.GetBackground() {
		st.bg = append(st.bg, p)

		return
	}

	if p != nil {
		code, msg = p.wait(ctx)
	}

	if timedOut.Load() && code != 0 {
		code, msg = exitMaxRuntime, "task or runnable exceeded its time limit"
	}

	if code != 0 && !r.GetIgnoreExitStatus() && !st.failed {
		st.exit, st.note, st.failed = code, msg, true
	}
}

// proc is a started runnable container.
type proc struct {
	cancel context.CancelFunc
	done   chan struct{}
	rt     Runtime
	id     string
	note   string
	exit   int
	fired  atomic.Bool
}

func stopAll(ps []*proc) {
	for _, p := range ps {
		p.cancel()
		<-p.done
	}
}

// wait blocks until the container exits and removes it. A runnable timeout reports exit 50005.
func (p *proc) wait(ctx context.Context) (int, string) {
	select {
	case <-p.done:
	case <-ctx.Done():
		p.cancel()
		<-p.done
	}

	if p.fired.Load() {
		return exitMaxRuntime, "runnable exceeded its timeout"
	}

	return p.exit, p.note
}

// launch starts one runnable. It returns the process, or an exit code and note when it could not start.
func (b *InMemoryBackend) launch(
	ctx context.Context, ex *execEnv, g *batchpb.TaskGroup, i, n int, r *batchpb.Runnable,
) (*proc, int, string) {
	vars, err := b.resolveEnv(ex.svc, envLayers(g, i, r))
	if err != nil {
		return nil, exitStartError, err.Error()
	}

	predefined(g, i, ex.count, vars)

	spec, err := b.runnableSpec(ex, r, vars)
	if err != nil {
		return nil, exitStartError, err.Error()
	}

	p := &proc{done: make(chan struct{}), rt: ex.rt}
	rctx, cancel := b.deadline(ctx, r.GetTimeout().AsDuration(), &p.fired)
	p.cancel = cancel

	id, err := ex.rt.CreateAndStart(rctx, spec)
	if err != nil {
		cancel()

		return nil, exitStartError, fmt.Sprintf("start runnable %d: %v", n, err)
	}

	p.id = id

	go p.supervise(rctx, ex, i)

	return p, 0, ""
}

// supervise follows the container's logs, waits for it to exit and removes it.
func (p *proc) supervise(ctx context.Context, ex *execEnv, task int) {
	defer close(p.done)
	defer p.remove()

	logDone := make(chan struct{})

	go func() {
		defer close(logDone)

		p.streamLogs(ctx, ex, task)
	}()

	w, ok := p.rt.(container.ExitWaiter)
	if !ok {
		p.exit, p.note = exitStartError, container.ErrWaitUnsupported.Error()

		return
	}

	code, err := w.WaitExit(ctx, p.id)
	p.exit = code

	if err != nil {
		p.exit, p.note = max(code, exitStartError), err.Error()
	}

	t := time.NewTimer(logDrainLimit)
	defer t.Stop()

	select {
	case <-logDone:
	case <-t.C:
	}
}

func (p *proc) remove() {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupBudget)
	defer cancel()

	_ = p.rt.StopAndRemove(ctx, p.id)
}

func (p *proc) streamLogs(ctx context.Context, ex *execEnv, task int) {
	if !ex.logs || ex.svc.Logs == nil {
		return
	}

	sink := taskLogSink{inner: ex.svc.Logs, project: ex.project}
	target := gcpserve.LogTarget{
		Project:  ex.project,
		Resource: taskLogResource(ex),
		Labels:   map[string]string{"job_uid": ex.job[strings.LastIndex(ex.job, "/")+1:], "task_id": itoa(task)},
	}

	_ = gcpserve.StreamLogs(ctx, p.rt, p.id, target, sink)
}

func (b *InMemoryBackend) runnableSpec(
	ex *execEnv,
	r *batchpb.Runnable,
	vars map[string]string,
) (container.Spec, error) {
	mounts := append([]string(nil), ex.vols...)
	if ex.hosts != "" {
		mounts = append(mounts, ex.hosts+":"+HostsFilePath+":ro")
	}

	spec := container.Spec{
		Env: envList(vars), Mounts: mounts,
		Labels: map[string]string{"gopherstack.gcpbatch.job": ex.job},
	}

	switch x := r.GetExecutable().(type) {
	case *batchpb.Runnable_Container_:
		c := x.Container
		spec.Image, spec.Cmd = c.GetImageUri(), c.GetCommands()
		spec.Mounts = append(spec.Mounts, c.GetVolumes()...)

		if c.GetEntrypoint() != "" {
			spec.Entrypoint = []string{c.GetEntrypoint()}
		}

		if c.GetBlockExternalNetwork() {
			spec.Network = "none"
		}
	case *batchpb.Runnable_Script_:
		spec.Image = ex.script

		if p := x.Script.GetPath(); p != "" {
			spec.Entrypoint = []string{p}
		} else {
			spec.Entrypoint = []string{"/bin/sh", "-c", scriptWrapper, "sh", x.Script.GetText()}
		}
	default:
		return spec, errUnsupportedRunnable
	}

	return spec, nil
}
