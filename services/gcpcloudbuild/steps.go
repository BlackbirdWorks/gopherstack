package gcpcloudbuild

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
)

// stepOutcome is how one step ended.
type stepOutcome int

const (
	stepOK stepOutcome = iota
	stepFailed
	stepTimedOut
	stepSkipped
)

// dependencies returns the indexes step i waits for: all earlier steps by default, none for ["-"].
func dependencies(steps []*cloudbuildpb.BuildStep, i int) []int {
	wait := steps[i].GetWaitFor()
	if len(wait) == 0 {
		deps := make([]int, i)
		for j := range i {
			deps[j] = j
		}

		return deps
	}

	if len(wait) == 1 && wait[0] == "-" {
		return nil
	}

	var deps []int

	for j := range i {
		if slices.Contains(wait, steps[j].GetId()) && steps[j].GetId() != "" {
			deps = append(deps, j)
		}
	}

	return deps
}

// runSteps runs every step once its dependencies finished; independent steps run in parallel. The first step to
// fail stops the build.
func (r *runner) runSteps(ctx context.Context) {
	steps := r.build.GetSteps()
	stepsCtx, cancel := context.WithCancel(ctx)

	defer cancel()

	done := make([]chan struct{}, len(steps))
	ok := make([]bool, len(steps))

	for i := range done {
		done[i] = make(chan struct{})
	}

	var wg sync.WaitGroup

	for i := range steps {
		deps := dependencies(steps, i)

		wg.Go(func() {
			defer close(done[i])

			ok[i] = r.runWhenReady(stepsCtx, i, deps, done, ok, cancel)
		})
	}

	wg.Wait()
}

func (r *runner) runWhenReady(
	ctx context.Context,
	i int,
	deps []int,
	done []chan struct{},
	ok []bool,
	stop context.CancelFunc,
) bool {
	for _, d := range deps {
		<-done[d]
	}

	for _, d := range deps {
		if !ok[d] {
			r.markStep(i, cloudbuildpb.Build_CANCELLED, 0)

			return false
		}
	}

	if ctx.Err() != nil {
		r.markStep(i, cloudbuildpb.Build_CANCELLED, 0)

		return false
	}

	res := r.runStep(ctx, i)
	if res.outcome == stepOK {
		return true
	}

	if res.failure != nil && r.fail.CompareAndSwap(nil, res.failure) {
		stop()
	}

	return false
}

func (r *runner) markStep(i int, st cloudbuildpb.Build_Status, code int32) {
	now := r.b.ts()

	r.b.update(r.ref, func(pb *cloudbuildpb.Build) {
		step := pb.GetSteps()[i]
		step.Status, step.ExitCode = st, code

		if st == cloudbuildpb.Build_WORKING {
			step.Timing = &cloudbuildpb.TimeSpan{StartTime: now}

			return
		}

		if step.GetTiming() != nil {
			step.Timing.EndTime = now
		}
	})
}

func stepLabel(i int, st *cloudbuildpb.BuildStep) string {
	if st.GetId() != "" {
		return fmt.Sprintf("Step #%d - %q", i, st.GetId())
	}

	return fmt.Sprintf("Step #%d", i)
}
