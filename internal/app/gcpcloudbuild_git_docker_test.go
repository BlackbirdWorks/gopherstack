package app

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gcpcloudbuildbackend "github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()

	full := make([]string, 0, len(args)+6)
	full = append(full, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false")
	cmd := exec.CommandContext(t.Context(), "git", append(full, args...)...)
	cmd.Dir = dir

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	return strings.TrimSpace(string(out))
}

// makeRepo creates a local repository with two commits, the first tagged v1, and returns the second's SHA.
func makeRepo(t *testing.T) (string, string) {
	t.Helper()

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}

	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "app"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "app", "msg.txt"), []byte("one\n"), 0o644))
	gitIn(t, dir, "add", ".")
	gitIn(t, dir, "commit", "-q", "-m", "one")
	gitIn(t, dir, "tag", "v1")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "app", "msg.txt"), []byte("two\n"), 0o644))
	gitIn(t, dir, "commit", "-q", "-am", "two")

	return dir, gitIn(t, dir, "rev-parse", "HEAD")
}

func TestGCPCloudBuild_Docker_GitSource(t *testing.T) {
	t.Parallel()

	e := newBuildEnv(t)

	if err := e.rt.PullImage(t.Context(), gcpcloudbuildbackend.DefaultGitImage); err != nil {
		t.Skipf("cannot pull %s: %v", gcpcloudbuildbackend.DefaultGitImage, err)
	}

	repo, head := makeRepo(t)

	tests := []struct {
		name     string
		revision string
		want     string
	}{
		{name: "head", want: "two"},
		{name: "tag", revision: "v1", want: "one"},
		{name: "sha", revision: head, want: "two"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			started := e.create(t, "cb-git-"+tt.name, &cloudbuildpb.Build{
				Source: &cloudbuildpb.Source{Source: &cloudbuildpb.Source_GitSource{
					GitSource: &cloudbuildpb.GitSource{Url: "file://" + repo, Revision: tt.revision, Dir: "app"},
				}},
				Steps: []*cloudbuildpb.BuildStep{shStep("read", "echo msg=$(cat msg.txt); pwd")},
			})

			got := e.finished(t, started)
			require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

			lines := e.logLines(t, "cb-git-"+tt.name)
			assert.Contains(t, lines, `Step #0 - "read": msg=`+tt.want)
			assert.Contains(t, lines, `Step #0 - "read": /workspace/app`)
			assert.Len(t, got.GetSourceProvenance().GetResolvedGitSource().GetRevision(), 40)
		})
	}
}
