package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEffectiveRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		opts runOptions
	}{
		{
			name: "default is base dot dot head",
			opts: runOptions{base: "origin/main"},
			want: "origin/main..HEAD",
		},
		{
			name: "full history uses bare base",
			opts: runOptions{base: "origin/main", fullHistory: true},
			want: "origin/main",
		},
		{
			name: "explicit range overrides everything",
			opts: runOptions{base: "origin/main", fullHistory: true, rangeExpr: "a..b"},
			want: "a..b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.opts.effectiveRange())
		})
	}
}

// newTestRepo creates a hermetic git repo with a fixed commit history so
// fetchCommits/run can be exercised end to end without touching this
// project's own history.
func newTestRepo(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()

		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com",
		)

		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "git %v: %s", args, out)
	}

	run("init", "-q", "-b", "main")
	run("commit", "--allow-empty", "-q", "-m", "base commit")

	return dir
}

func commitEmpty(t *testing.T, dir, message string) {
	t.Helper()

	cmd := exec.Command("git", "-C", dir, "commit", "--allow-empty", "-q", "-m", message)
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com",
	)

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git commit: %s", out)
}

func TestFetchCommits(t *testing.T) {
	t.Parallel()

	dir := newTestRepo(t)
	commitEmpty(t, dir, "fix(example): a real bug\n\nCloses gopherstack-aaaa.")
	commitEmpty(t, dir, "chore: unrelated")

	commits, err := fetchCommits(dir, "main")
	require.NoError(t, err)
	require.Len(t, commits, 3)
	assert.Contains(t, commits[0].Body, "unrelated")
	assert.Contains(t, commits[1].Body, "Closes gopherstack-aaaa")
}

func TestFetchCommitsBadRange(t *testing.T) {
	t.Parallel()

	dir := newTestRepo(t)

	_, err := fetchCommits(dir, "does-not-exist")
	require.Error(t, err)
}

func TestRunEndToEnd(t *testing.T) {
	t.Parallel()

	dir := newTestRepo(t)
	commitEmpty(t, dir, "fix(example): a real bug\n\nCloses gopherstack-aaaa.")
	commitEmpty(t, dir, "fix(example): typo target\n\nCloses gopherstack-zzzz.")

	issuesPath := filepath.Join(dir, "issues.jsonl")
	content := `{"id":"gopherstack-aaaa","title":"still open","status":"open"}
`
	require.NoError(t, os.WriteFile(issuesPath, []byte(content), 0o600))

	rep, err := run(runOptions{
		issuesPath:  issuesPath,
		repoRoot:    dir,
		base:        "main",
		fullHistory: true,
		noSuspicion: true,
	})
	require.NoError(t, err)

	assert.Equal(t, 3, rep.CommitsScanned)
	require.Len(t, rep.NotClosed, 1)
	assert.Equal(t, "gopherstack-aaaa", rep.NotClosed[0].IssueID)
	require.Len(t, rep.Typos, 1)
	assert.Equal(t, "gopherstack-zzzz", rep.Typos[0].IssueID)
	assert.Nil(t, rep.Suspicion)
}

func TestRunMissingIssuesFile(t *testing.T) {
	t.Parallel()

	dir := newTestRepo(t)

	_, err := run(runOptions{
		issuesPath: filepath.Join(dir, "nope.jsonl"),
		repoRoot:   dir,
		base:       "main",
	})
	require.Error(t, err)
}
