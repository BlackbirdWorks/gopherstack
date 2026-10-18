// Package inithooks provides support for running user-defined scripts on Gopherstack startup.
// Scripts are executed sequentially in the order they are listed, using the system shell.
// Any script that exits with a non-zero status is logged as an error but does not abort startup.
package inithooks

import (
	"context"
	"log/slog"
	"os/exec"
	"time"
)

// defaultTimeout is the maximum duration allowed for a single init script to complete.
const defaultTimeout = 30 * time.Second

// Runner executes a list of init scripts on startup.
type Runner struct {
	log     *slog.Logger
	scripts []string
	timeout time.Duration
}

// New creates a new Runner.
// scripts is the ordered list of shell script paths to execute.
// timeout is the per-script execution timeout (zero defaults to 30 s).
// log may be nil (in which case no logging is performed).
func New(scripts []string, timeout time.Duration, log *slog.Logger) *Runner {
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	return &Runner{
		scripts: scripts,
		timeout: timeout,
		log:     log,
	}
}

// Result is the outcome of one init script.
type Result struct {
	Script string
	OK     bool
}

// Run executes each script sequentially and returns one Result per script.
// It blocks until all scripts have completed (or timed out).
// Errors from individual scripts are logged but do not stop subsequent scripts.
func (r *Runner) Run(ctx context.Context) []Result {
	results := make([]Result, 0, len(r.scripts))

	for _, script := range r.scripts {
		results = append(results, Result{Script: script, OK: r.runOne(ctx, script)})
	}

	return results
}

// runOne executes a single script, respecting the per-script timeout.
func (r *Runner) runOne(ctx context.Context, script string) bool {
	scriptCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	cmd := exec.CommandContext(
		scriptCtx,
		"sh",
		"-c",
		script,
	)
	// WaitDelay ensures orphaned subprocesses are also killed when the context expires.
	cmd.WaitDelay = r.timeout

	if r.log != nil {
		r.log.InfoContext(scriptCtx, "init hook: running", "script", script)
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		if r.log != nil {
			r.log.ErrorContext(
				scriptCtx, "init hook: script failed",
				"script", script,
				"error", err,
				"output", string(out),
			)
		}

		return false
	}

	if r.log != nil {
		r.log.InfoContext(scriptCtx, "init hook: script succeeded", "script", script, "output", string(out))
	}

	return true
}
