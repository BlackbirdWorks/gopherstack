package gcpcloudbuild_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/api/httpbody"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

const (
	gitImage = gcpcloudbuild.DefaultGitImage
	commit   = "0123456789abcdef0123456789abcdef01234567"
)

// mountHost returns the host side of the mount whose container path is dest.
func mountHost(spec container.Spec, dest string) string {
	for _, m := range spec.Mounts {
		host, rest, _ := strings.Cut(m, ":")
		if strings.HasPrefix(rest, dest) {
			return host
		}
	}

	return ""
}

// fakeGit stands in for the git container: it checks the arguments and writes the resolved commit.
func fakeGit(seen *container.Spec) behavior {
	return func(_ context.Context, spec container.Spec, out io.Writer) int {
		*seen = spec

		if aux := mountHost(spec, "/aux"); aux != "" {
			_ = os.WriteFile(filepath.Join(aux, "git-commit"), []byte(commit+"\n"), 0o600)
		}

		_, _ = io.WriteString(out, "cloned\n")

		return 0
	}
}

func gitBuild(src *cloudbuildpb.Source) *cloudbuildpb.Build {
	b := buildOf(step("img", withID("use")))
	b.Source = src

	return b
}

func gitSrc(url, rev, dir string) *cloudbuildpb.Source {
	return &cloudbuildpb.Source{Source: &cloudbuildpb.Source_GitSource{
		GitSource: &cloudbuildpb.GitSource{Url: url, Revision: rev, Dir: dir},
	}}
}

func TestGitSourceFetch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src      *cloudbuildpb.Source
		name     string
		wantURL  string
		wantRev  string
		wantDir  string
		wantProv string
	}{
		{
			name: "git source", src: gitSrc("https://example.com/r.git", "main", "app"),
			wantURL: "https://example.com/r.git", wantRev: "main", wantDir: "/workspace/app", wantProv: "git",
		},
		{
			name: "git source head", src: gitSrc("http://example.com/r.git", "", ""),
			wantURL: "http://example.com/r.git", wantDir: "/workspace", wantProv: "git",
		},
		{
			name: "repo source", src: &cloudbuildpb.Source{Source: &cloudbuildpb.Source_RepoSource{
				RepoSource: &cloudbuildpb.RepoSource{
					RepoName: "web", Dir: "sub", Revision: &cloudbuildpb.RepoSource_TagName{TagName: "v1"},
				},
			}},
			wantURL: "https://git.example.com/" + project + "/web", wantRev: "v1",
			wantDir: "/workspace/sub", wantProv: "repo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var seen container.Spec

			e := newEnv(t, map[string]behavior{gitImage: fakeGit(&seen), "img": exits(0)})
			e.configure(func(s *gcpcloudbuild.Services) { s.RepoBase = "https://git.example.com" })

			got := e.run(t, gitBuild(tt.src))
			require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

			assert.Equal(t, []string{"-c", seen.Cmd[1], "sh", tt.wantURL, tt.wantRev}, seen.Cmd)
			assert.Equal(t, "sh", seen.Entrypoint[0])
			assert.Equal(t, tt.wantDir, e.rt.specOf("img").WorkingDir)

			prov := got.GetSourceProvenance()
			if tt.wantProv == "git" {
				assert.Equal(t, commit, prov.GetResolvedGitSource().GetRevision())
			} else {
				assert.Equal(t, commit, prov.GetResolvedRepoSource().GetCommitSha())
			}
		})
	}
}

func TestGitSourceRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src  *cloudbuildpb.Source
		name string
		want string
	}{
		{name: "ssh url", src: gitSrc("ssh://host/r.git", "", ""), want: "http, https or file"},
		{name: "option as url", src: gitSrc("--upload-pack=x", "", ""), want: "http, https or file"},
		{name: "option as revision", src: gitSrc("https://h/r.git", "--output=x", ""), want: "invalid git revision"},
		{name: "dir escapes", src: gitSrc("https://h/r.git", "", "../x"), want: "stay inside the workspace"},
		{name: "relative file", src: gitSrc("file://rel/r", "", ""), want: "absolute local directory"},
		{name: "missing file", src: gitSrc("file:///no/such/dir", "", ""), want: "absolute local directory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var seen container.Spec

			e := newEnv(t, map[string]behavior{gitImage: fakeGit(&seen), "img": exits(0)})

			got := e.run(t, gitBuild(tt.src))
			assert.Equal(t, cloudbuildpb.Build_FAILURE, got.GetStatus())
			assert.Contains(t, got.GetStatusDetail(), tt.want)
			assert.Equal(t, cloudbuildpb.Build_FailureInfo_FETCH_SOURCE_FAILED, got.GetFailureInfo().GetType())
			assert.Empty(t, seen.Image)
		})
	}
}

func TestGitFailureAndFileMount(t *testing.T) {
	t.Parallel()

	repo := t.TempDir()

	var seen container.Spec

	e := newEnv(t, map[string]behavior{gitImage: fakeGit(&seen), "img": exits(0)})

	got := e.run(t, gitBuild(gitSrc("file://"+repo, "", "")))
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())
	assert.Equal(t, "file:///gcb-git-src", seen.Cmd[3])
	assert.Contains(t, seen.Mounts, repo+":/gcb-git-src:ro")

	failing := newEnv(t, map[string]behavior{gitImage: exits(128, "fatal: repository not found"), "img": exits(0)})
	got = failing.run(t, gitBuild(gitSrc("https://example.com/missing.git", "", "")))
	assert.Equal(t, cloudbuildpb.Build_FAILURE, got.GetStatus())
	assert.Contains(t, got.GetStatusDetail(), "git exited with status 128")
	assert.Contains(t, strings.Join(failing.logs.lines(), "\n"), "fatal: repository not found")
}

func TestTriggerSources(t *testing.T) {
	t.Parallel()

	template := &cloudbuildpb.RepoSource{
		RepoName: "tpl", Revision: &cloudbuildpb.RepoSource_BranchName{BranchName: "main"},
	}

	tests := []struct {
		trigger *cloudbuildpb.BuildTrigger
		run     *cloudbuildpb.RepoSource
		name    string
		wantURL string
		wantRev string
	}{
		{
			name:    "template revision",
			trigger: &cloudbuildpb.BuildTrigger{Name: "t1", TriggerTemplate: template},
			wantURL: "https://git.example.com/" + project + "/tpl", wantRev: "main",
		},
		{
			name:    "run overrides revision",
			trigger: &cloudbuildpb.BuildTrigger{Name: "t2", TriggerTemplate: template},
			run:     &cloudbuildpb.RepoSource{Revision: &cloudbuildpb.RepoSource_TagName{TagName: "v2"}},
			wantURL: "https://git.example.com/" + project + "/tpl", wantRev: "v2",
		},
		{
			name: "github repository",
			trigger: &cloudbuildpb.BuildTrigger{Name: "t3", Github: &cloudbuildpb.GitHubEventsConfig{
				Owner: "acme", Name: "app",
			}},
			run: &cloudbuildpb.RepoSource{
				Revision: &cloudbuildpb.RepoSource_CommitSha{CommitSha: commit},
			},
			wantURL: "https://github.com/acme/app.git", wantRev: commit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var seen container.Spec

			e := newEnv(t, map[string]behavior{gitImage: fakeGit(&seen), "img": exits(0)})
			e.configure(func(s *gcpcloudbuild.Services) { s.RepoBase = "https://git.example.com" })

			tt.trigger.BuildTemplate = &cloudbuildpb.BuildTrigger_Build{Build: buildOf(step("img"))}

			created, err := e.client.CreateBuildTrigger(t.Context(),
				&cloudbuildpb.CreateBuildTriggerRequest{Parent: parent, Trigger: tt.trigger})
			require.NoError(t, err)

			op, err := e.client.RunBuildTrigger(t.Context(),
				&cloudbuildpb.RunBuildTriggerRequest{Name: created.GetResourceName(), Source: tt.run})
			require.NoError(t, err)

			got, err := op.Wait(t.Context())
			require.NoError(t, err)
			e.backend.Wait()
			require.Equal(t, cloudbuildpb.Build_SUCCESS, e.get(t, got.GetName()).GetStatus())

			assert.Equal(t, tt.wantURL, seen.Cmd[3])
			assert.Equal(t, tt.wantRev, seen.Cmd[4])
		})
	}
}

func TestWebhookBindings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		subs    map[string]string
		want    string
		wantErr string
	}{
		{
			name: "nested and indexed", body: `{"repo":{"name":"web"},"commits":[{"id":"abc"}],"n":7,"ok":true}`,
			subs: map[string]string{"_V": "$(body.repo.name)-$(body.commits[0].id)-$(body.n)-$(body.ok)"},
			want: "web-abc-7-true",
		},
		{
			name: "object value", body: `{"a":{"b":1}}`,
			subs: map[string]string{"_V": "$(body.a)"}, want: `{"b":1}`,
		},
		{name: "literal untouched", body: `{}`, subs: map[string]string{"_V": "plain"}, want: "plain"},
		{
			name: "missing path", body: `{"a":1}`, subs: map[string]string{"_V": "$(body.zzz)"},
			wantErr: "not present in the payload",
		},
		{
			name: "not json", body: `nope`, subs: map[string]string{"_V": "$(body.a)"},
			wantErr: "not valid JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, map[string]behavior{"img": exits(0)})

			created, err := e.client.CreateBuildTrigger(t.Context(), &cloudbuildpb.CreateBuildTriggerRequest{
				Parent: parent,
				Trigger: &cloudbuildpb.BuildTrigger{
					Name:          "hook",
					Substitutions: tt.subs,
					BuildTemplate: &cloudbuildpb.BuildTrigger_Build{
						Build: buildOf(step("img", withArgs("$_V"))),
					},
					WebhookConfig: &cloudbuildpb.WebhookConfig{AuthMethod: &cloudbuildpb.WebhookConfig_Secret{
						Secret: "projects/p/secrets/hook/versions/1",
					}},
				},
			})
			require.NoError(t, err)

			_, err = e.client.ReceiveTriggerWebhook(t.Context(), &cloudbuildpb.ReceiveTriggerWebhookRequest{
				Name: created.GetResourceName(), Secret: "whsec", Body: &httpbody.HttpBody{Data: []byte(tt.body)},
			})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			e.backend.Wait()

			b, err := e.client.ListBuilds(t.Context(), &cloudbuildpb.ListBuildsRequest{Parent: parent}).Next()
			require.NoError(t, err)
			assert.Equal(t, []string{tt.want}, b.GetSteps()[0].GetArgs())
		})
	}
}
