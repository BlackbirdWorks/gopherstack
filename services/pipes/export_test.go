package pipes

import (
	"context"
	"testing"
	"time"
)

// PollAllPipesOnce triggers a single synchronous poll cycle for tests.
func PollAllPipesOnce(ctx context.Context, r *Runner) {
	res := r.backend.ListPipes(ListPipesFilter{CurrentState: stateRunning})

	for _, p := range res.Pipes {
		r.pollPipe(ctx, p)
	}
}

// CreatePipeSimple is a test helper that creates a pipe using positional args.
func (b *InMemoryBackend) CreatePipeSimple(
	name, roleARN, source, target, description, desiredState string,
	tags map[string]string,
) (*Pipe, error) {
	return b.CreatePipe(CreatePipeInput{
		Name:         name,
		RoleARN:      roleARN,
		Source:       source,
		Target:       target,
		Description:  description,
		DesiredState: desiredState,
		Tags:         tags,
	})
}

// ListPipesAll returns all pipes without filtering (test convenience).
func (b *InMemoryBackend) ListPipesAll() []*Pipe {
	return b.ListPipes(ListPipesFilter{}).Pipes
}

// WaitPipeRunning waits up to 500ms for a pipe to reach RUNNING state.
func WaitPipeRunning(t *testing.T, b *InMemoryBackend, name string) {
	t.Helper()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		p, err := b.GetPipe(name)
		if err == nil && p.CurrentState == stateRunning {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("pipe %q did not reach RUNNING state within 500ms", name)
}
