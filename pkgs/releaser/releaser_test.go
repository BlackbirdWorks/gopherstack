package releaser_test

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/releaser"
)

func TestBuildPrompt(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name     string
		tag      string
		commits  []string
		diff     string
		contains []string
	}

	tests := []testCase{
		{
			name:    "Basic prompt",
			tag:     "v1.0.0",
			commits: []string{"feat: add something", "fix: bug"},
			diff:    "diff --git a/file b/file\n+new line",
			contains: []string{
				"v1.0.0",
				"feat: add something",
				"fix: bug",
				"Features",
				"Fixes",
				"Code Diff",
				"+new line",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := releaser.BuildPrompt(tt.tag, tt.commits, tt.diff)
			for _, c := range tt.contains {
				require.Contains(t, got, c)
			}
		})
	}
}

//nolint:paralleltest // os.Chdir impacts the whole process.
func TestGitOperations(t *testing.T) {
	// Setup a temporary git repo
	tmpDir := t.TempDir()
	t.Chdir(tmpDir)

	run := func(t *testing.T, name string, args ...string) {
		t.Helper()
		cmd := exec.Command(name, args...)
		require.NoError(t, cmd.Run())
	}

	run(t, "git", "init")
	run(t, "git", "config", "user.email", "test@example.com")
	run(t, "git", "config", "user.name", "Test User")
	run(t, "git", "commit", "--allow-empty", "-m", "initial")
	run(t, "git", "tag", "v0.1.0")

	// Commit with actual file changes
	require.NoError(t, os.WriteFile("feat.txt", []byte("new feature"), 0o644))
	run(t, "git", "add", "feat.txt")
	run(t, "git", "commit", "-m", "feat: new feature")

	require.NoError(t, os.WriteFile("fix.txt", []byte("bug fix"), 0o644))
	run(t, "git", "add", "fix.txt")
	run(t, "git", "commit", "-m", "fix: bug fix")

	ctx := t.Context()

	t.Run("GetLatestTag", func(t *testing.T) {
		tag, err := releaser.GetLatestTag(ctx)
		require.NoError(t, err)
		require.Equal(t, "v0.1.0", tag)
	})

	t.Run("GetCommitsSince", func(t *testing.T) {
		commits, err := releaser.GetCommitsSince(ctx, "v0.1.0")
		require.NoError(t, err)
		require.Len(t, commits, 2)
		require.Contains(t, commits[0], "fix: bug fix")
		require.Contains(t, commits[1], "feat: new feature")
	})

	t.Run("GetDiffSince", func(t *testing.T) {
		diff, err := releaser.GetDiffSince(ctx, "v0.1.0")
		require.NoError(t, err)
		require.Contains(t, diff, "new feature")
		require.Contains(t, diff, "bug fix")
	})
}
