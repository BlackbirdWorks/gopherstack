package codebuild

import (
	"encoding/base64"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const gitBuildspec = "version: 0.2\nphases:\n  build:\n    commands: [echo BUILD_CMD]\n"

func gitRun(t *testing.T, dir string, args ...string) string {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), "git", append([]string{
		"-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false",
		"-c", "protocol.file.allow=always", "-c", "init.defaultBranch=main",
	}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL="+os.DevNull)

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	return strings.TrimSpace(string(out))
}

func commitFile(t *testing.T, dir, name, body string) string {
	t.Helper()

	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	gitRun(t, dir, "add", "-A")
	gitRun(t, dir, "commit", "-q", "-m", name+body)

	return gitRun(t, dir, "rev-parse", "HEAD")
}

type testRepo struct {
	dir      string
	first    string
	head     string
	prCommit string
}

// newTestRepo builds a repo with main (two commits), feature, tag v1, a submodule and PR refs.
func newTestRepo(t *testing.T) testRepo {
	t.Helper()

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	sub := t.TempDir()
	gitRun(t, sub, "init", "-q")
	commitFile(t, sub, "SUB", "sub-content")

	dir := t.TempDir()
	gitRun(t, dir, "init", "-q")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "buildspec.yml"), []byte(gitBuildspec), 0o600))

	r := testRepo{dir: dir}
	r.first = commitFile(t, dir, "VERSION", "one")
	gitRun(t, dir, "tag", "v1")
	gitRun(t, dir, "checkout", "-q", "-b", "feature")
	commitFile(t, dir, "VERSION", "feature")
	gitRun(t, dir, "checkout", "-q", "-b", "pr7", "main")
	r.prCommit = commitFile(t, dir, "VERSION", "pr-seven")
	gitRun(t, dir, "checkout", "-q", "main")
	gitRun(t, dir, "submodule", "add", "-q", "file://"+sub, "libs/sub")
	r.head = commitFile(t, dir, "VERSION", "two")

	for _, ref := range []string{"refs/pull/7/head", "refs/merge-requests/7/head", "refs/pull-requests/7/from"} {
		gitRun(t, dir, "update-ref", ref, r.prCommit)
	}

	return r
}

type gitObservation struct {
	lock    *lockmetrics.RWMutex
	version string
	sub     string
	shallow bool
}

func gitObserver() (*gitObservation, fakeRule) {
	obs := &gitObservation{lock: lockmetrics.New("obs")}

	return obs, fakeRule{match: "BUILD_CMD", create: func(dir string) {
		v, _ := os.ReadFile(filepath.Join(dir, "VERSION"))
		s, _ := os.ReadFile(filepath.Join(dir, "libs", "sub", "SUB"))
		_, serr := os.Stat(filepath.Join(dir, ".git", "shallow"))

		obs.lock.Lock("obs")
		obs.version, obs.sub, obs.shallow = string(v), string(s), serr == nil
		obs.lock.Unlock()
	}}
}

func TestEngineGitSources(t *testing.T) {
	t.Parallel()

	repo := newTestRepo(t)

	tests := []struct {
		name       string
		serverType cbtypes.SourceType
		version    string
		wantFile   string
		wantSHA    string
		wantSub    string
		depth      int32
		submodules bool
		wantShort  bool
	}{
		{name: "default_branch", serverType: "GITHUB", wantFile: "two", wantSHA: repo.head},
		{name: "branch", serverType: "GITHUB", version: "feature", wantFile: "feature"},
		{name: "tag", serverType: "GITHUB_ENTERPRISE", version: "v1", wantFile: "one", wantSHA: repo.first},
		{name: "commit", serverType: "BITBUCKET", version: repo.first, wantFile: "one", wantSHA: repo.first},
		{name: "github_pr", serverType: "GITHUB", version: "pr/7", wantFile: "pr-seven", wantSHA: repo.prCommit},
		{name: "gitlab_mr", serverType: "GITLAB", version: "pr/7", wantFile: "pr-seven", wantSHA: repo.prCommit},
		{name: "gitlab_self_pr", serverType: "GITLAB_SELF_MANAGED", version: "pr/7", wantFile: "pr-seven"},
		{name: "bitbucket_pr", serverType: "BITBUCKET", version: "pr/7", wantFile: "pr-seven", wantSHA: repo.prCommit},
		{name: "shallow", serverType: "GITHUB", depth: 1, wantFile: "two", wantShort: true},
		{name: "submodules", serverType: "GITHUB", submodules: true, wantFile: "two", wantSub: "sub-content"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			obs, rule := gitObserver()
			h := newHarnessWith(t, newFakeRuntime(rule), func(c *EngineConfig) { c.AllowFileGitSources = true })
			c := sdkClientFor(t, h.b)

			src := &cbtypes.ProjectSource{
				Type: tt.serverType, Location: aws.String("file://" + repo.dir), GitCloneDepth: aws.Int32(tt.depth),
			}
			if tt.submodules {
				src.GitSubmodulesConfig = &cbtypes.GitSubmodulesConfig{FetchSubmodules: aws.Bool(true)}
			}

			createSDKProject(t, c, sdkProject{source: src})

			in := &codebuildsdk.StartBuildInput{}
			if tt.version != "" {
				in.SourceVersion = aws.String(tt.version)
			}

			b := startAndWait(t, h, c, in)
			require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus, failedPhaseMessage(b))

			obs.lock.RLock("read")
			defer obs.lock.RUnlock()

			assert.Equal(t, tt.wantFile, obs.version)
			assert.Equal(t, tt.wantSub, obs.sub)
			assert.Equal(t, tt.wantShort, obs.shallow)

			if tt.wantSHA != "" {
				assert.Equal(t, tt.wantSHA, aws.ToString(b.ResolvedSourceVersion))
			}
		})
	}
}

func TestEngineGitSourceRejections(t *testing.T) {
	t.Parallel()

	repo := newTestRepo(t)

	tests := []struct {
		name      string
		location  string
		version   string
		wantMsg   string
		allowFile bool
	}{
		{name: "file_disabled", location: "file://" + repo.dir, wantMsg: "file:// sources are disabled"},
		{name: "bare_path", location: repo.dir, allowFile: true, wantMsg: "invalid git source location"},
		{name: "ext_scheme", location: "ext::sh -c id", allowFile: true, wantMsg: "invalid git source location"},
		{name: "dash_location", location: "-oProxy", allowFile: true, wantMsg: "invalid git source location"},
		{name: "dash_ref", location: "file://" + repo.dir, version: "--upload-pack=x", allowFile: true, wantMsg: "ref"},
		{name: "missing_ref", location: "file://" + repo.dir, version: "nope", allowFile: true, wantMsg: "git fetch"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHarnessWith(t, newFakeRuntime(), func(c *EngineConfig) { c.AllowFileGitSources = tt.allowFile })
			c := sdkClientFor(t, h.b)
			createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
				Type: cbtypes.SourceTypeGithub, Location: aws.String(tt.location),
			}})

			in := &codebuildsdk.StartBuildInput{}
			if tt.version != "" {
				in.SourceVersion = aws.String(tt.version)
			}

			b := startAndWait(t, h, c, in)
			assert.Equal(t, cbtypes.StatusTypeFailed, b.BuildStatus)
			assert.Contains(t, failedPhaseMessage(b), tt.wantMsg)
		})
	}
}

// gitHTTPServer serves repo over smart HTTP, demanding basic auth user:token.
func gitHTTPServer(t *testing.T, repo testRepo, user, token string) string {
	t.Helper()

	backend := filepath.Join(gitRun(t, repo.dir, "--exec-path"), "git-http-backend")

	if _, err := os.Stat(backend); err != nil {
		t.Skip("git-http-backend not available")
	}

	root := t.TempDir()
	gitRun(t, root, "clone", "-q", "--bare", "file://"+repo.dir, filepath.Join(root, "repo.git"))

	cgiH := &cgi.Handler{
		Path: backend, Root: "/",
		Env:        []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1"},
		InheritEnv: []string{"PATH"},
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+token))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != want {
			w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)

			return
		}

		cgiH.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	return srv.URL + "/repo.git"
}

func TestEngineGitSourceCredentials(t *testing.T) {
	t.Parallel()

	repo := newTestRepo(t)

	tests := []struct {
		name       string
		serverType cbtypes.ServerType
		authType   cbtypes.AuthType
		importTok  string
		importUser string
		wantUser   string
		wantOK     bool
	}{
		{
			name: "github_pat", serverType: "GITHUB", authType: "PERSONAL_ACCESS_TOKEN",
			importTok: "ghp_secret", wantUser: "x-access-token", wantOK: true,
		},
		{
			name: "gitlab_oauth", serverType: "GITLAB", authType: "OAUTH",
			importTok: "glpat_secret", wantUser: "oauth2", wantOK: true,
		},
		{
			name: "bitbucket_basic", serverType: "BITBUCKET", authType: "BASIC_AUTH",
			importTok: "app_secret", importUser: "alice", wantUser: "alice", wantOK: true,
		},
		{
			name: "wrong_token", serverType: "GITHUB", authType: "PERSONAL_ACCESS_TOKEN",
			importTok: "ghp_wrong", wantUser: "x-access-token",
		},
		{name: "no_credentials", serverType: "GITHUB", wantUser: "x-access-token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			token := "the-real-token"
			if tt.wantOK {
				token = tt.importTok
			}

			location := gitHTTPServer(t, repo, tt.wantUser, token)
			obs, rule := gitObserver()
			h := newHarnessWith(t, newFakeRuntime(rule), nil)
			c := sdkClientFor(t, h.b)

			if tt.authType != "" {
				in := &codebuildsdk.ImportSourceCredentialsInput{
					ServerType: tt.serverType, AuthType: tt.authType, Token: aws.String(tt.importTok),
				}
				if tt.importUser != "" {
					in.Username = aws.String(tt.importUser)
				}

				_, err := c.ImportSourceCredentials(t.Context(), in)
				require.NoError(t, err)
			}

			createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
				Type: cbtypes.SourceType(tt.serverType), Location: aws.String(location),
			}})

			b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})

			for _, secret := range []string{tt.importTok, "the-real-token"} {
				if secret != "" {
					assert.NotContains(t, h.logs.joined(), secret)
					assert.NotContains(t, failedPhaseMessage(b), secret)
				}
			}

			if !tt.wantOK {
				assert.Equal(t, cbtypes.StatusTypeFailed, b.BuildStatus)

				return
			}

			require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus, failedPhaseMessage(b))
			obs.lock.RLock("read")
			assert.Equal(t, "two", obs.version)
			obs.lock.RUnlock()
		})
	}
}

func TestSourceCredentialsTokenNotListed(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	c := sdkClientFor(t, b)

	_, err := c.ImportSourceCredentials(t.Context(), &codebuildsdk.ImportSourceCredentialsInput{
		ServerType: cbtypes.ServerTypeGithub,
		AuthType:   cbtypes.AuthTypePersonalAccessToken,
		Token:      aws.String("tok-123"),
	})
	require.NoError(t, err)

	tok, user, ok := b.sourceToken("GITHUB")
	assert.True(t, ok)
	assert.Equal(t, "tok-123", tok)
	assert.Empty(t, user)

	_, _, ok = b.sourceToken("GITLAB")
	assert.False(t, ok)

	listed, err := c.ListSourceCredentials(t.Context(), &codebuildsdk.ListSourceCredentialsInput{})
	require.NoError(t, err)
	require.Len(t, listed.SourceCredentialsInfos, 1)
	assert.Equal(t, cbtypes.ServerTypeGithub, listed.SourceCredentialsInfos[0].ServerType)
}
