package gcpcloudbuild

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	workspace     = "/workspace"
	scriptsMount  = "/gcb-scripts"
	dockerSocket  = "/var/run/docker.sock"
	logDrainLimit = 10 * time.Second
	scriptPerm    = 0o755
	shebang       = "#!/bin/sh\n"
)

// stepResult is what runStep reports to the scheduler.
type stepResult struct {
	failure *stepFailure
	outcome stepOutcome
}

// runStep runs build step i in its own container.
func (r *runner) runStep(ctx context.Context, i int) stepResult {
	step := r.build.GetSteps()[i]
	label := stepLabel(i, step)
	timedOut := &atomic.Bool{}

	stepCtx, stop := r.stepContext(ctx, step, timedOut)
	defer stop()

	spec, err := r.stepSpec(i, step)
	if err != nil {
		return r.stepError(i, step, fmt.Errorf("prepare step: %w", err))
	}

	r.markStep(i, cloudbuildpb.Build_WORKING, 0)
	r.log.system("Starting " + label)
	r.log.step(label, "Pulling image: "+spec.Image)

	id, err := r.rt.CreateAndStart(stepCtx, spec)
	if err != nil {
		return r.stepError(i, step, fmt.Errorf("start step: %w", err))
	}

	defer r.remove(id)

	logDone := r.followLogs(stepCtx, id, label)

	code, err := r.waitExit(stepCtx, id)
	r.drain(logDone)

	switch {
	case timedOut.Load():
		return r.stepTimeout(i, step)
	case err != nil:
		return r.stepEnded(ctx, i, step, err)
	}

	return r.stepExit(i, step, code)
}

// stepContext applies the step's own timeout on the injected timer.
func (r *runner) stepContext(
	ctx context.Context,
	step *cloudbuildpb.BuildStep,
	fired *atomic.Bool,
) (context.Context, func()) {
	if step.GetTimeout() == nil || step.GetTimeout().AsDuration() <= 0 {
		return context.WithCancel(ctx)
	}

	return r.deadline(ctx, step.GetTimeout().AsDuration(), fired)
}

func (r *runner) stepExit(i int, step *cloudbuildpb.BuildStep, code int) stepResult {
	label := stepLabel(i, step)

	if code == 0 || stepAllows(step, code) {
		r.markStep(i, cloudbuildpb.Build_SUCCESS, exitCode32(code))
		r.log.system("Finished " + label)

		return stepResult{outcome: stepOK}
	}

	r.markStep(i, cloudbuildpb.Build_FAILURE, exitCode32(code))

	if step.GetAllowFailure() {
		r.log.system("Finished " + label + " (failure allowed)")

		return stepResult{outcome: stepOK}
	}

	detail := fmt.Sprintf(
		"build step %d %q failed: step exited with non-zero status: %d",
		i,
		step.GetName(),
		code,
	)

	return stepResult{outcome: stepFailed, failure: &stepFailure{
		status: cloudbuildpb.Build_FAILURE, detail: detail,
		info: &cloudbuildpb.Build_FailureInfo{
			Type:   cloudbuildpb.Build_FailureInfo_USER_BUILD_STEP,
			Detail: detail,
		},
	}}
}

func stepAllows(step *cloudbuildpb.BuildStep, code int) bool {
	return slices.Contains(step.GetAllowExitCodes(), exitCode32(code))
}

// exitCode32 narrows a container exit status, which is always within 0-255, to the proto's int32.
func exitCode32(code int) int32 {
	return int32(code) //nolint:gosec // exit statuses fit in int32
}

func (r *runner) stepTimeout(i int, step *cloudbuildpb.BuildStep) stepResult {
	r.markStep(i, cloudbuildpb.Build_TIMEOUT, 0)

	detail := fmt.Sprintf(
		"build step %d %q timed out after %s",
		i,
		step.GetName(),
		step.GetTimeout().AsDuration(),
	)

	return stepResult{
		outcome: stepTimedOut,
		failure: &stepFailure{status: cloudbuildpb.Build_TIMEOUT, detail: detail},
	}
}

// stepEnded handles a step whose container wait ended without an exit code.
func (r *runner) stepEnded(
	ctx context.Context,
	i int,
	step *cloudbuildpb.BuildStep,
	err error,
) stepResult {
	if ctx.Err() != nil {
		r.markStep(i, cloudbuildpb.Build_CANCELLED, 0)

		return stepResult{outcome: stepSkipped}
	}

	return r.stepError(i, step, err)
}

func (r *runner) stepError(i int, step *cloudbuildpb.BuildStep, err error) stepResult {
	r.markStep(i, cloudbuildpb.Build_FAILURE, 0)

	detail := fmt.Sprintf("build step %d %q failed: %v", i, step.GetName(), err)
	r.log.step(stepLabel(i, step), err.Error())

	return stepResult{
		outcome: stepFailed,
		failure: &stepFailure{status: cloudbuildpb.Build_FAILURE, detail: detail},
	}
}

func (r *runner) waitExit(ctx context.Context, id string) (int, error) {
	w, ok := r.rt.(container.ExitWaiter)
	if !ok {
		return 0, container.ErrWaitUnsupported
	}

	return w.WaitExit(
		ctx,
		id,
	) //nolint:wrapcheck // runtime errors are reported as the step failure detail
}

func (r *runner) remove(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupBudget)
	defer cancel()

	_ = r.rt.StopAndRemove(ctx, id)
}

// followLogs streams the container's output into the build log; the channel closes when the stream ends.
func (r *runner) followLogs(ctx context.Context, id, label string) <-chan struct{} {
	done := make(chan struct{})

	ls, ok := r.rt.(container.LogStreamer)
	if !ok {
		close(done)

		return done
	}

	go func() {
		defer close(done)

		out, errw := r.log.writer(label), r.log.writer(label)
		_ = ls.StreamLogs(ctx, id, out, errw)

		out.flush()
		errw.flush()
	}()

	return done
}

func (r *runner) drain(done <-chan struct{}) {
	t := time.NewTimer(logDrainLimit)
	defer t.Stop()

	select {
	case <-done:
	case <-t.C:
	}
}

// stepSpec builds the container specification for step i.
func (r *runner) stepSpec(i int, step *cloudbuildpb.BuildStep) (container.Spec, error) {
	spec := container.Spec{
		Image: step.GetName(), Cmd: step.GetArgs(), Env: r.stepEnv(step), WorkingDir: workingDir(step.GetDir()),
	}

	if step.GetEntrypoint() != "" {
		spec.Entrypoint = []string{step.GetEntrypoint()}
	}

	mounts, err := r.stepMounts(step)
	if err != nil {
		return spec, err
	}

	spec.Mounts = mounts

	if step.GetScript() != "" {
		file, wErr := r.writeScript(i, step.GetScript())
		if wErr != nil {
			return spec, wErr
		}

		spec.Entrypoint, spec.Cmd = []string{file}, nil
	}

	return spec, nil
}

func workingDir(dir string) string {
	if dir == "" {
		return workspace
	}

	if path.IsAbs(dir) {
		return path.Clean(dir)
	}

	return path.Join(workspace, dir)
}

func (r *runner) stepEnv(step *cloudbuildpb.BuildStep) []string {
	env := slices.Clone(r.build.GetOptions().GetEnv())
	env = append(env, step.GetEnv()...)

	for _, name := range step.GetSecretEnv() {
		if v, ok := r.secrets[name]; ok {
			env = append(env, name+"="+v)
		}
	}

	return env
}

func (r *runner) stepMounts(step *cloudbuildpb.BuildStep) ([]string, error) {
	mounts := []string{r.work + ":" + workspace}

	if step.GetScript() != "" {
		mounts = append(mounts, r.scriptsDir()+":"+scriptsMount+":ro")
	}

	if _, err := os.Stat(dockerSocket); err == nil {
		mounts = append(mounts, dockerSocket+":"+dockerSocket)
	}

	vols := append(slices.Clone(r.build.GetOptions().GetVolumes()), step.GetVolumes()...)

	for _, v := range vols {
		dir, err := r.volumeDir(v.GetName())
		if err != nil {
			return nil, err
		}

		mounts = append(mounts, dir+":"+path.Clean(v.GetPath()))
	}

	return mounts, nil
}

func (r *runner) scriptsDir() string { return filepath.Join(r.auxDir(), "scripts") }

func (r *runner) volumeDir(name string) (string, error) {
	dir := filepath.Join(r.auxDir(), "volumes", name)

	if err := os.MkdirAll(dir, workDirPerm); err != nil {
		return "", fmt.Errorf("create volume %q: %w", name, err)
	}

	if err := os.Chmod(dir, workDirPerm); err != nil {
		return "", fmt.Errorf("create volume %q: %w", name, err)
	}

	return dir, nil
}

// writeScript stores a step's script outside the workspace and returns its path inside the container.
func (r *runner) writeScript(i int, script string) (string, error) {
	if err := os.MkdirAll(r.scriptsDir(), workDirPerm); err != nil {
		return "", fmt.Errorf("create script dir: %w", err)
	}

	if len(script) < 2 || script[:2] != "#!" {
		script = shebang + script
	}

	name := "step-" + strconv.Itoa(i) + ".sh"

	if err := os.WriteFile(filepath.Join(r.scriptsDir(), name), []byte(script), scriptPerm); err != nil {
		return "", fmt.Errorf("write script: %w", err)
	}

	return scriptsMount + "/" + name, nil
}
