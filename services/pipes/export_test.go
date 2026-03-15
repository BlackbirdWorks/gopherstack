package pipes

import "context"

// PollAllPipesOnce triggers a single poll cycle on the given Runner.
func PollAllPipesOnce(r *Runner, ctx context.Context) {
	r.pollAllPipes(ctx)
}
