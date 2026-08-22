package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// gitRunner shells out to the git binary rooted at dir. It is the only
// place this tool touches git, and every method it exposes is read-only
// (cat-file, log, merge-base) -- nothing here mutates repository state.
type gitRunner struct {
	dir string
}

func (g gitRunner) run(args ...string) (string, error) {
	cmd := exec.CommandContext(context.Background(), "git", args...)
	if g.dir != "" {
		cmd.Dir = g.dir
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}

	return strings.TrimSpace(stdout.String()), nil
}

// commitExists reports whether sha resolves to a commit object in the local
// repository. false covers both "never existed here" and "existed once but
// was pruned" -- this tool cannot tell those apart and does not try to.
//
//nolint:gosec // gitRunner intentionally shells out to git with caller-provided shas/refs.
func (g gitRunner) commitExists(sha string) bool {
	cmd := exec.CommandContext(context.Background(), "git", "cat-file", "-e", sha+"^{commit}")
	if g.dir != "" {
		cmd.Dir = g.dir
	}

	return cmd.Run() == nil
}

// commitDate returns sha's committer date (not author date -- committer
// date is what changes on rebase/cherry-pick/squash, so it's the honest
// answer to "when did this object land where it now sits").
func (g gitRunner) commitDate(sha string) (time.Time, error) {
	out, err := g.run("log", "-1", "--format=%cI", sha)
	if err != nil {
		return time.Time{}, err
	}

	t, parseErr := time.Parse(time.RFC3339, out)
	if parseErr != nil {
		return time.Time{}, fmt.Errorf("parse commit date %q: %w", out, parseErr)
	}

	return t, nil
}

var errAncestryUnknown = errors.New("ancestry check failed (not a simple yes/no)")

// isAncestor reports whether sha is an ancestor of (reachable from) ref.
// git merge-base --is-ancestor exits 0 for yes, 1 for no, and >1 for a real
// error (bad ref, etc.) -- only the last case is treated as an error here.
func (g gitRunner) isAncestor(sha, ref string) (bool, error) {
	cmd := exec.CommandContext(context.Background(), "git", "merge-base", "--is-ancestor", sha, ref)
	if g.dir != "" {
		cmd.Dir = g.dir
	}

	err := cmd.Run()
	if err == nil {
		return true, nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}

	return false, fmt.Errorf("%w: %w", errAncestryUnknown, err)
}

// mergeBase returns the merge-base commit of sha and ref: the point where
// the branch sha sat on diverged from ref. For a sha unreachable from ref
// because its branch was squash-merged, this is the branch's actual
// starting point -- stable, on ref, and the correct re-audit diff baseline,
// since it's what the audit itself started from.
func (g gitRunner) mergeBase(sha, ref string) (string, error) {
	return g.run("merge-base", sha, ref)
}

// shortSHA returns git's own preferred abbreviation of sha, matching this
// repo's existing last_audit_commit style rather than picking an arbitrary
// fixed width.
func (g gitRunner) shortSHA(sha string) (string, error) {
	return g.run("rev-parse", "--short", sha)
}
