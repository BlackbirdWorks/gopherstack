package gcpcloudbuild

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	workDirPerm   = 0o777
	cleanupBudget = 30 * time.Second
	auxMount      = "/aux"
)

// runner executes one build.
type runner struct {
	b        *InMemoryBackend
	rt       Runtime
	build    *cloudbuildpb.Build
	log      *buildLog
	timedOut *atomic.Bool
	fail     atomic.Pointer[stepFailure]
	svc      Services
	secrets  map[string]string
	srcDir   string
	work     string
	ref      ref
}

// stepFailure is the first failure that decided the build's outcome.
type stepFailure struct {
	info   *cloudbuildpb.Build_FailureInfo
	detail string
	status cloudbuildpb.Build_Status
}

func (b *InMemoryBackend) execute(ctx context.Context, r ref) {
	snap, ok := b.begin(r)
	if !ok {
		return
	}

	rt, svc, reason := b.services()
	if rt == nil {
		b.finish(r, cloudbuildpb.Build_FAILURE, reason, nil)

		return
	}

	run := &runner{b: b, rt: rt, svc: svc, build: snap, ref: r, timedOut: &atomic.Bool{}}
	run.log = newBuildLog(snap, svc.Logs, b.now)

	work, err := os.MkdirTemp(b.workDir, "gcb-")
	if err == nil {
		err = os.Chmod(work, workDirPerm)
	}

	if err != nil {
		b.finish(
			r,
			cloudbuildpb.Build_INTERNAL_ERROR,
			"failed to create the build workspace: "+err.Error(),
			nil,
		)

		return
	}

	run.work = work

	defer run.cleanup()

	run.run(ctx)
}

func (r *runner) cleanup() {
	if r.work == "" {
		return
	}

	if !r.removeDirs() {
		r.reclaim()
		r.removeDirs()
	}
}

func (r *runner) removeDirs() bool {
	errWork := os.RemoveAll(r.work)
	errAux := os.RemoveAll(r.auxDir())

	return errWork == nil && errAux == nil
}

func (r *runner) auxDir() string { return r.work + "-aux" }

// reclaim makes files that step containers created as another user removable by the host user.
func (r *runner) reclaim() {
	steps := r.build.GetSteps()
	if len(steps) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cleanupBudget)
	defer cancel()

	id, err := r.rt.CreateAndStart(ctx, container.Spec{
		Image: steps[0].GetName(), Entrypoint: []string{"chmod", "-R", "a+rwX", workspace, auxMount},
		Mounts: []string{r.work + ":" + workspace, r.auxDir() + ":" + auxMount},
	})
	if err != nil {
		return
	}

	defer r.remove(id)

	if w, ok := r.rt.(container.ExitWaiter); ok {
		_, _ = w.WaitExit(ctx, id)
	}
}

func (r *runner) run(ctx context.Context) {
	buildCtx, stop := r.deadline(ctx, r.build.GetTimeout().AsDuration(), r.timedOut)
	defer stop()

	r.log.system(fmt.Sprintf("starting build %q", r.ref.id))

	failure := r.prepare(buildCtx)
	if failure == nil {
		r.runSteps(buildCtx)
		failure = r.fail.Load()
	}

	if failure == nil && buildCtx.Err() != nil {
		failure = r.interruption()
	}

	if failure == nil {
		failure = r.publish(buildCtx)
	}

	r.conclude(ctx, failure)
}

// interruption classifies a build whose context ended without a failing step.
func (r *runner) interruption() *stepFailure {
	if r.timedOut.Load() {
		return &stepFailure{status: cloudbuildpb.Build_TIMEOUT, detail: r.timeoutDetail()}
	}

	return &stepFailure{status: cloudbuildpb.Build_INTERNAL_ERROR, detail: "build was interrupted"}
}

func (r *runner) timeoutDetail() string {
	return fmt.Sprintf("build did not complete within %s", r.build.GetTimeout().AsDuration())
}

// prepare fetches the source and resolves secrets.
func (r *runner) prepare(ctx context.Context) *stepFailure {
	r.logPool()
	r.log.system("FETCHSOURCE")

	if err := r.fetchSource(ctx); err != nil {
		return &stepFailure{
			status: cloudbuildpb.Build_FAILURE, detail: "failed to fetch source: " + err.Error(),
			info: &cloudbuildpb.Build_FailureInfo{
				Type: cloudbuildpb.Build_FailureInfo_FETCH_SOURCE_FAILED, Detail: err.Error(),
			},
		}
	}

	if err := r.resolveSecrets(); err != nil {
		return &stepFailure{
			status: cloudbuildpb.Build_FAILURE,
			detail: "failed to resolve secrets: " + err.Error(),
		}
	}

	return nil
}

// conclude uploads the log and records the final status.
func (r *runner) conclude(ctx context.Context, failure *stepFailure) {
	r.log.system("DONE")
	r.uploadLog()

	if failure == nil {
		r.b.finish(r.ref, cloudbuildpb.Build_SUCCESS, "", nil)

		return
	}

	if ctx.Err() != nil && !r.timedOut.Load() &&
		failure.status == cloudbuildpb.Build_INTERNAL_ERROR {
		logger.Load(ctx).DebugContext(ctx, "gcpcloudbuild: build interrupted", "build", r.ref.id)
	}

	r.b.finish(r.ref, failure.status, failure.detail, failure.info)
}

// deadline derives a context that ends after d on the injected timer and sets fired.
func (r *runner) deadline(
	parent context.Context,
	d time.Duration,
	fired *atomic.Bool,
) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	ch, stop := r.b.after(d)

	go func() {
		select {
		case <-ch:
			fired.Store(true)
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, func() {
		stop()
		cancel()
	}
}

// logPool records the worker pool and machine settings the build was scheduled with.
func (r *runner) logPool() {
	opts := r.build.GetOptions()
	if name := opts.GetPool().GetName(); name != "" {
		cfg := r.b.poolConfig(name).GetWorkerConfig()
		r.log.system(fmt.Sprintf("Scheduled on worker pool %s (machine type %q, disk %d GB)",
			name, cfg.GetMachineType(), cfg.GetDiskSizeGb()))

		return
	}

	if opts.GetMachineType() != cloudbuildpb.BuildOptions_UNSPECIFIED || opts.GetDiskSizeGb() > 0 {
		r.log.system(fmt.Sprintf("Scheduled on machine type %s, disk %d GB",
			opts.GetMachineType(), opts.GetDiskSizeGb()))
	}
}

func (b *InMemoryBackend) poolConfig(name string) *cloudbuildpb.PrivatePoolV1Config {
	b.mu.RLock("poolConfig")
	defer b.mu.RUnlock()

	return b.pools[name].GetPrivatePoolV1Config()
}
