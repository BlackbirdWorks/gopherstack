package gcpcloudbuild

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	// DefaultGitImage is the image git fetches run in when Services.GitImage is empty.
	DefaultGitImage = "alpine/git:latest"
	sourceRepoURL   = "https://source.developers.google.com/p/%s/r/%s"
	gitSrcMount     = "/gcb-git-src"
	gitCommitFile   = "git-commit"
	fetchLabel      = "Fetch source"
	maxRevisionLen  = 256
	maxCommitLen    = 64

	// Arguments are positional ($1 url, $2 revision), never interpolated.
	gitScript = `set -e
git init -q .
git remote add origin "$1"
git fetch -q --no-tags origin "${2:-HEAD}"
git checkout -q --detach FETCH_HEAD
git rev-parse HEAD > ` + auxMount + `/` + gitCommitFile
)

var (
	errGitScheme   = errors.New("git url must use http, https or file")
	errGitRevision = errors.New("invalid git revision")
	errGitDir      = errors.New("source dir must be relative and stay inside the workspace")
	errGitExit     = errors.New("git exited with status")
	errGitRepo     = errors.New("repo source needs a repo_name")
	errGitFile     = errors.New("file url must name an absolute local directory")
)

// gitTarget is a repository, revision and build directory to check out.
type gitTarget struct {
	repo     *cloudbuildpb.RepoSource
	url      string
	revision string
	dir      string
	mount    string
}

// fetchGit clones a gitSource or repoSource into the workspace and records the resolved commit.
func (r *runner) fetchGit(ctx context.Context, src *cloudbuildpb.Source) error {
	t, err := r.gitTargetOf(src)
	if err != nil {
		return err
	}

	r.log.system(fmt.Sprintf("Fetching git source: %s %s", t.url, t.revision))

	if err = os.MkdirAll(r.auxDir(), workDirPerm); err != nil {
		return fmt.Errorf("create aux dir: %w", err)
	}

	if err = os.Chmod(r.auxDir(), workDirPerm); err != nil {
		return fmt.Errorf("create aux dir: %w", err)
	}

	if err = r.runHelper(ctx, fetchLabel, r.gitSpec(t)); err != nil {
		return err
	}

	commit := r.readCommit()
	r.srcDir = t.dir
	r.recordProvenance(t, commit)

	return nil
}

func (r *runner) gitTargetOf(src *cloudbuildpb.Source) (gitTarget, error) {
	var t gitTarget

	switch s := src.GetSource().(type) {
	case *cloudbuildpb.Source_GitSource:
		t = gitTarget{url: s.GitSource.GetUrl(), revision: s.GitSource.GetRevision(), dir: s.GitSource.GetDir()}
	case *cloudbuildpb.Source_RepoSource:
		rs := s.RepoSource
		if rs.GetRepoName() == "" {
			return t, errGitRepo
		}

		t = gitTarget{
			url: repoURL(r.svc.RepoBase, cmp.Or(rs.GetProjectId(), r.ref.project), rs.GetRepoName()), dir: rs.GetDir(),
			revision: repoRevision(rs), repo: rs,
		}
	}

	return checkGitTarget(t)
}

func repoURL(base, project, repo string) string {
	if base == "" {
		return fmt.Sprintf(sourceRepoURL, project, repo)
	}

	return strings.TrimRight(base, "/") + "/" + project + "/" + repo
}

func repoRevision(rs *cloudbuildpb.RepoSource) string {
	switch rev := rs.GetRevision().(type) {
	case *cloudbuildpb.RepoSource_BranchName:
		return rev.BranchName
	case *cloudbuildpb.RepoSource_TagName:
		return rev.TagName
	case *cloudbuildpb.RepoSource_CommitSha:
		return rev.CommitSha
	}

	return ""
}

// checkGitTarget rejects anything that could turn into a git option or escape the workspace and resolves
// file:// repositories to a read-only mount.
func checkGitTarget(t gitTarget) (gitTarget, error) {
	u, err := url.Parse(t.url)
	if err != nil {
		return t, fmt.Errorf("%w: %w", errGitScheme, err)
	}

	switch u.Scheme {
	case "http", "https":
		if u.Host == "" {
			return t, errGitScheme
		}
	case "file":
		if t.mount, err = localRepo(u); err != nil {
			return t, err
		}

		t.url = "file://" + gitSrcMount
	default:
		return t, errGitScheme
	}

	if len(t.revision) > maxRevisionLen || strings.HasPrefix(t.revision, "-") ||
		strings.ContainsAny(t.revision, " \t\r\n\x00") {
		return t, errGitRevision
	}

	if t.dir != "" {
		clean := path.Clean(t.dir)
		if path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
			return t, errGitDir
		}

		t.dir = clean
	}

	return t, nil
}

func localRepo(u *url.URL) (string, error) {
	p := filepath.Clean(u.Path)
	if u.Host != "" || !filepath.IsAbs(p) {
		return "", errGitFile
	}

	fi, err := os.Stat(p) //nolint:gosec // mounted read-only into the git container
	if err != nil || !fi.IsDir() {
		return "", errGitFile
	}

	return p, nil
}

func (r *runner) gitSpec(t gitTarget) container.Spec {
	img := r.svc.GitImage
	if img == "" {
		img = DefaultGitImage
	}

	mounts := []string{r.work + ":" + workspace, r.auxDir() + ":" + auxMount}
	if t.mount != "" {
		mounts = append(mounts, t.mount+":"+gitSrcMount+":ro")
	}

	return container.Spec{
		Image: img, Entrypoint: []string{"sh"}, WorkingDir: workspace, Mounts: mounts,
		Cmd: []string{"-c", gitScript, "sh", t.url, t.revision},
		Env: []string{
			"GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=safe.directory", "GIT_CONFIG_VALUE_0=*",
		},
	}
}

// runHelper runs a build-internal container to completion, streaming its output into the build log.
func (r *runner) runHelper(ctx context.Context, label string, spec container.Spec) error {
	id, err := r.rt.CreateAndStart(ctx, spec)
	if err != nil {
		return fmt.Errorf("start %s: %w", spec.Image, err)
	}

	defer r.remove(id)

	done := r.followLogs(ctx, id, label)
	code, err := r.waitExit(ctx, id)

	r.drain(done)

	switch {
	case err != nil:
		return fmt.Errorf("%s: %w", label, err)
	case code != 0:
		return fmt.Errorf("%s: %w %d", label, errGitExit, code)
	}

	return nil
}

func (r *runner) readCommit() string {
	data, err := os.ReadFile(filepath.Join(r.auxDir(), gitCommitFile))
	if err != nil {
		return ""
	}

	c := strings.TrimSpace(string(data))
	if len(c) > maxCommitLen || strings.Trim(c, "0123456789abcdef") != "" {
		return ""
	}

	return c
}

func (r *runner) recordProvenance(t gitTarget, commit string) {
	prov := &cloudbuildpb.SourceProvenance{}

	if t.repo != nil {
		prov.ResolvedRepoSource = &cloudbuildpb.RepoSource{
			ProjectId: t.repo.GetProjectId(), RepoName: t.repo.GetRepoName(), Dir: t.dir,
			Revision: &cloudbuildpb.RepoSource_CommitSha{CommitSha: commit},
		}
	} else {
		gs := r.build.GetSource().GetGitSource()
		prov.ResolvedGitSource = &cloudbuildpb.GitSource{Url: gs.GetUrl(), Dir: t.dir, Revision: commit}
	}

	r.b.update(r.ref, func(pb *cloudbuildpb.Build) { pb.SourceProvenance = prov })
}
