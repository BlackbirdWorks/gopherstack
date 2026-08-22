package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRepo creates an isolated throwaway git repository in a t.TempDir()
// and returns a gitRunner rooted there. The `git config user.*` calls below
// are local to this disposable repo only -- never the user's global config.
func newTestRepo(t *testing.T) gitRunner {
	t.Helper()

	dir := t.TempDir()
	runGit(t, dir, "init", "-q", "-b", "main")
	runGit(t, dir, "config", "user.email", "stampaudit-test@example.com")
	runGit(t, dir, "config", "user.name", "stampaudit-test")

	return gitRunner{dir: dir}
}

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()

	cmd := exec.Command("git", args...)
	cmd.Dir = dir

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %s: %s", strings.Join(args, " "), out)

	return strings.TrimSpace(string(out))
}

// commitAt creates a commit with a fixed author/committer date (RFC3339)
// on whatever branch is currently checked out, and returns its full sha.
func commitAt(t *testing.T, dir, msg, date string) string {
	t.Helper()

	path := filepath.Join(dir, msg+".txt")
	require.NoError(t, os.WriteFile(path, []byte(msg), 0o600))
	runGit(t, dir, "add", ".")

	cmd := exec.Command("git", "commit", "-q", "-m", msg)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_DATE="+date, "GIT_COMMITTER_DATE="+date)

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	return runGit(t, dir, "rev-parse", "HEAD")
}

func TestGitRunnerCommitExists(t *testing.T) {
	t.Parallel()

	g := newTestRepo(t)
	sha := commitAt(t, g.dir, "a", "2026-07-01T00:00:00Z")

	t.Run("real commit", func(t *testing.T) {
		t.Parallel()
		assert.True(t, g.commitExists(sha))
	})

	t.Run("unknown sha", func(t *testing.T) {
		t.Parallel()
		assert.False(t, g.commitExists("deadbeef"))
	})
}

func TestGitRunnerCommitDate(t *testing.T) {
	t.Parallel()

	g := newTestRepo(t)
	sha := commitAt(t, g.dir, "a", "2026-07-01T12:00:00Z")

	got, err := g.commitDate(sha)

	require.NoError(t, err)
	assert.Equal(t, 2026, got.Year())
	assert.Equal(t, 7, int(got.Month()))
	assert.Equal(t, 1, got.Day())
}

func TestGitRunnerCommitDateUnknownSHA(t *testing.T) {
	t.Parallel()

	g := newTestRepo(t)
	commitAt(t, g.dir, "a", "2026-07-01T00:00:00Z")

	_, err := g.commitDate("deadbeef")

	require.Error(t, err)
}

func TestGitRunnerIsAncestor(t *testing.T) {
	t.Parallel()

	g := newTestRepo(t)
	base := commitAt(t, g.dir, "a", "2026-07-01T00:00:00Z")
	runGit(t, g.dir, "checkout", "-q", "-b", "feature")
	tip := commitAt(t, g.dir, "b", "2026-07-05T00:00:00Z")

	t.Run("on-main commit is ancestor of main", func(t *testing.T) {
		t.Parallel()

		reachable, err := g.isAncestor(base, "main")
		require.NoError(t, err)
		assert.True(t, reachable)
	})

	t.Run("unmerged branch tip is not ancestor of main", func(t *testing.T) {
		t.Parallel()

		reachable, err := g.isAncestor(tip, "main")
		require.NoError(t, err)
		assert.False(t, reachable)
	})

	t.Run("bad ref surfaces as an error", func(t *testing.T) {
		t.Parallel()

		_, err := g.isAncestor(base, "no-such-ref")
		require.Error(t, err)
		assert.ErrorIs(t, err, errAncestryUnknown)
	})
}

func TestGitRunnerMergeBase(t *testing.T) {
	t.Parallel()

	g := newTestRepo(t)
	base := commitAt(t, g.dir, "a", "2026-07-01T00:00:00Z")
	runGit(t, g.dir, "checkout", "-q", "-b", "feature")
	tip := commitAt(t, g.dir, "b", "2026-07-05T00:00:00Z")

	got, err := g.mergeBase(tip, "main")

	require.NoError(t, err)
	assert.Equal(t, base, got)
}

func TestGitRunnerShortSHA(t *testing.T) {
	t.Parallel()

	g := newTestRepo(t)
	sha := commitAt(t, g.dir, "a", "2026-07-01T00:00:00Z")

	got, err := g.shortSHA(sha)

	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(sha, got))
	assert.NotEmpty(t, got)
}
