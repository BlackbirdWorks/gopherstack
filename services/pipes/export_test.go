package pipes

import "context"

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
