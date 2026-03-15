package pipes

import "context"

// PollAllPipesOnce triggers a single poll cycle on the given Runner.
func PollAllPipesOnce(ctx context.Context, r *Runner) {
	r.pollAllPipes(ctx)
}
