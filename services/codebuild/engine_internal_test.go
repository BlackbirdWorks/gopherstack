package codebuild

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	errNoSuchKey = errors.New("NoSuchKey")
	errNoParam   = errors.New("ParameterNotFound")
	errNoSecret  = errors.New("ResourceNotFoundException")
	errPull      = errors.New("pull access denied")
)

type fakeRule struct {
	create func(hostDir string)
	codeFn func() int
	match  string
	out    string
	code   int
	block  bool
}

type fakeRuntime struct {
	mu        *lockmetrics.RWMutex
	hostDir   string
	createErr error
	started   chan struct{}
	rules     []fakeRule
	scripts   []string
	mounts    []string
	removed   int
}

func newFakeRuntime(rules ...fakeRule) *fakeRuntime {
	return &fakeRuntime{mu: lockmetrics.New("fakert"), rules: rules, started: make(chan struct{}, 8)}
}

func (f *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	if f.createErr != nil {
		return "", f.createErr
	}

	f.mu.Lock("create")
	f.hostDir, _, _ = strings.Cut(spec.Mounts[0], ":")
	f.mounts = append(f.mounts, spec.Mounts...)
	f.mu.Unlock()

	return "cid", nil
}

func (f *fakeRuntime) StopAndRemove(context.Context, string) error {
	f.mu.Lock("rm")
	f.removed++
	f.mu.Unlock()

	return nil
}

func (f *fakeRuntime) Exec(ctx context.Context, _ string, cmd []string) (container.ExecResult, error) {
	script := cmd[len(cmd)-1]

	f.mu.Lock("exec")
	f.scripts = append(f.scripts, script)
	dir := f.hostDir
	f.mu.Unlock()

	for _, r := range f.rules {
		if !strings.Contains(script, r.match) {
			continue
		}

		if r.create != nil {
			r.create(dir)
		}

		if r.block {
			f.started <- struct{}{}
			<-ctx.Done()

			return container.ExecResult{}, ctx.Err()
		}

		code := r.code
		if r.codeFn != nil {
			code = r.codeFn()
		}

		return container.ExecResult{Stdout: r.out, ExitCode: code}, nil
	}

	return container.ExecResult{}, nil
}

func (f *fakeRuntime) ran(substr string) bool {
	f.mu.RLock("ran")
	defer f.mu.RUnlock()

	for _, s := range f.scripts {
		if strings.Contains(s, substr) {
			return true
		}
	}

	return false
}

type fakeStore struct {
	mu      *lockmetrics.RWMutex
	objects map[string][]byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{mu: lockmetrics.New("fakes3"), objects: map[string][]byte{}}
}

func (s *fakeStore) GetObjectBytes(_ context.Context, bucket, key string) ([]byte, error) {
	s.mu.RLock("get")
	defer s.mu.RUnlock()

	if d, ok := s.objects[bucket+"/"+key]; ok {
		return d, nil
	}

	return nil, errNoSuchKey
}

func (s *fakeStore) PutObjectBytes(_ context.Context, bucket, key string, data []byte) error {
	s.mu.Lock("put")
	defer s.mu.Unlock()

	s.objects[bucket+"/"+key] = data

	return nil
}

func (s *fakeStore) keys() []string {
	s.mu.RLock("keys")
	defer s.mu.RUnlock()

	out := make([]string, 0, len(s.objects))
	for k := range s.objects {
		out = append(out, k)
	}

	return out
}

type fakeLogs struct {
	mu    *lockmetrics.RWMutex
	group string
	lines []string
}

func (l *fakeLogs) EnsureLogGroupAndStream(_ context.Context, _, group, _ string) error {
	l.mu.Lock("ensure")
	l.group = group
	l.mu.Unlock()

	return nil
}

func (l *fakeLogs) PutLogLines(_ context.Context, _, _, _ string, lines []string) error {
	l.mu.Lock("put")
	l.lines = append(l.lines, lines...)
	l.mu.Unlock()

	return nil
}

func (l *fakeLogs) joined() string {
	l.mu.RLock("joined")
	defer l.mu.RUnlock()

	return strings.Join(l.lines, "\n")
}

type fakeValues struct{}

func (fakeValues) Parameter(_ context.Context, _, name string) (string, error) {
	if name == "/db/pass" {
		return "s3cr3t-param", nil
	}

	return "", errNoParam
}

func (fakeValues) SecretValue(_ context.Context, _, id, _, _ string) (string, error) {
	if id == "mysecret" {
		return `{"user":"admin","pw":"hunter2"}`, nil
	}

	return "", errNoSecret
}

type engineHarness struct {
	b     *InMemoryBackend
	rt    *fakeRuntime
	store *fakeStore
	logs  *fakeLogs
}

func newHarness(t *testing.T, rt *fakeRuntime) *engineHarness {
	t.Helper()

	return newHarnessWith(t, rt, nil)
}

func newHarnessWith(t *testing.T, rt EngineRuntime, tweak func(*EngineConfig)) *engineHarness {
	t.Helper()

	h := &engineHarness{
		b:     NewInMemoryBackend("000000000000", "us-east-1"),
		store: newFakeStore(),
		logs:  &fakeLogs{mu: lockmetrics.New("fakelogs")},
	}
	if f, ok := rt.(*fakeRuntime); ok {
		h.rt = f
	}

	cfg := EngineConfig{Runtime: rt, WorkDir: t.TempDir(), CacheDir: t.TempDir(), TimeoutUnit: 100 * time.Millisecond}
	if tweak != nil {
		tweak(&cfg)
	}

	h.b.EnableEngine(cfg)
	h.b.SetEngineServices(EngineServices{Store: h.store, Logs: h.logs, Values: fakeValues{}})

	return h
}

func (h *engineHarness) project(t *testing.T, cfg ProjectConfig) {
	t.Helper()

	if cfg.Name == "" {
		cfg.Name = "proj"
	}

	if cfg.Source == nil {
		cfg.Source = &ProjectSource{Type: "NO_SOURCE"}
	}

	if cfg.Artifacts == nil {
		cfg.Artifacts = &ProjectArtifacts{Type: "NO_ARTIFACTS"}
	}

	if cfg.Environment == nil {
		cfg.Environment = &ProjectEnvironment{
			Type: "LINUX_CONTAINER", Image: "aws/codebuild/standard:7.0", ComputeType: "BUILD_GENERAL1_SMALL",
		}
	}

	_, err := h.b.CreateProject(cfg)
	require.NoError(t, err)
}

func (h *engineHarness) run(t *testing.T, cfg StartBuildConfig) *Build {
	t.Helper()

	b, err := h.b.StartBuild("proj", cfg)
	require.NoError(t, err)

	h.b.WaitBuilds()

	got, nf := h.b.BatchGetBuilds([]string{b.ID})
	require.Empty(t, nf)

	return got[0]
}

func phaseStatuses(b *Build) map[string]string {
	out := map[string]string{}
	for _, p := range b.Phases {
		out[p.PhaseType] = p.PhaseStatus
	}

	return out
}

func zipOf(files map[string]string) []byte {
	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, body := range files {
		w, _ := zw.Create(name)
		_, _ = w.Write([]byte(body))
	}

	_ = zw.Close()

	return buf.Bytes()
}

const specAllPhases = `version: 0.2
phases:
  install:
    commands: [echo INSTALL_CMD]
  pre_build:
    commands: [echo PRE_CMD]
  build:
    commands: [echo BUILD_CMD]
  post_build:
    commands: [echo POST_CMD]
`

func TestEnginePhaseSequencing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantPhases map[string]string
		name       string
		spec       string
		wantStatus string
		wantMsg    string
		rules      []fakeRule
		wantRan    []string
		wantNotRan []string
	}{
		{
			name: "all_succeed", spec: specAllPhases, wantStatus: "SUCCEEDED",
			wantPhases: map[string]string{
				"QUEUED": "SUCCEEDED", "PROVISIONING": "SUCCEEDED", "DOWNLOAD_SOURCE": "SUCCEEDED",
				"INSTALL": "SUCCEEDED", "PRE_BUILD": "SUCCEEDED", "BUILD": "SUCCEEDED", "POST_BUILD": "SUCCEEDED",
				"UPLOAD_ARTIFACTS": "SUCCEEDED", "FINALIZING": "SUCCEEDED", "COMPLETED": "",
			},
			wantRan: []string{"INSTALL_CMD", "PRE_CMD", "BUILD_CMD", "POST_CMD"},
		},
		{
			name:       "install_fails_skips_rest",
			spec:       specAllPhases,
			wantStatus: "FAILED",
			rules: []fakeRule{
				{
					match: "INSTALL_CMD",
					code:  2,
					out:   "[Container] t Command did not exit successfully echo INSTALL_CMD exit status 2",
				},
			},
			wantPhases: map[string]string{"INSTALL": "FAILED", "FINALIZING": "SUCCEEDED"},
			wantNotRan: []string{"PRE_CMD", "BUILD_CMD", "POST_CMD"},
			wantMsg:    "Error while executing command: echo INSTALL_CMD. Reason: exit status 2",
		},
		{
			name:       "build_fails_runs_post_build",
			spec:       specAllPhases,
			wantStatus: "FAILED",
			rules:      []fakeRule{{match: "BUILD_CMD", code: 1}},
			wantPhases: map[string]string{
				"BUILD":            "FAILED",
				"POST_BUILD":       "SUCCEEDED",
				"UPLOAD_ARTIFACTS": "SUCCEEDED",
			},
			wantRan: []string{"POST_CMD"},
		},
		{
			name: "build_abort_skips_post_build", wantStatus: "FAILED",
			spec: `version: 0.2
phases:
  build:
    on-failure: ABORT
    commands: [echo BUILD_CMD]
  post_build:
    commands: [echo POST_CMD]
`,
			rules:      []fakeRule{{match: "BUILD_CMD", code: 1}},
			wantNotRan: []string{"POST_CMD"},
		},
		{
			name: "install_continue_runs_next", wantStatus: "FAILED",
			spec: `version: 0.2
phases:
  install:
    on-failure: CONTINUE
    commands: [echo INSTALL_CMD]
  build:
    commands: [echo BUILD_CMD]
`,
			rules:   []fakeRule{{match: "INSTALL_CMD", code: 1}},
			wantRan: []string{"BUILD_CMD"},
		},
		{
			name: "finally_runs_after_failure", wantStatus: "FAILED",
			spec: `version: 0.2
phases:
  build:
    commands: [echo BUILD_CMD]
    finally: [echo FINALLY_CMD]
`,
			rules:   []fakeRule{{match: "BUILD_CMD", code: 1}},
			wantRan: []string{"FINALLY_CMD"},
		},
		{
			name: "bad_buildspec", spec: "version: 9\n", wantStatus: "FAILED",
			wantPhases: map[string]string{"DOWNLOAD_SOURCE": "FAILED"},
			wantNotRan: []string{"INSTALL_CMD"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t, newFakeRuntime(tt.rules...))
			h.project(t, ProjectConfig{Source: &ProjectSource{Type: "NO_SOURCE", Buildspec: tt.spec}})

			b := h.run(t, StartBuildConfig{})

			assert.Equal(t, tt.wantStatus, b.BuildStatus)
			assert.True(t, b.BuildComplete)
			assert.Equal(t, "COMPLETED", b.CurrentPhase)

			got := phaseStatuses(b)
			for k, v := range tt.wantPhases {
				assert.Equal(t, v, got[k], k)
			}

			for _, s := range tt.wantRan {
				assert.True(t, h.rt.ran(s), s)
			}

			for _, s := range tt.wantNotRan {
				assert.False(t, h.rt.ran(s), s)
			}

			if tt.wantMsg != "" {
				for _, p := range b.Phases {
					if p.PhaseStatus == "FAILED" {
						require.NotEmpty(t, p.Contexts)
						assert.Equal(t, tt.wantMsg, p.Contexts[0].Message)
						assert.Equal(t, "COMMAND_EXECUTION_ERROR", p.Contexts[0].StatusCode)
					}
				}
			}

			assert.Positive(t, h.rt.removed)
		})
	}
}

func TestEngineProvisionFailure(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime()
	rt.createErr = errPull
	h := newHarness(t, rt)
	h.project(t, ProjectConfig{Source: &ProjectSource{Type: "NO_SOURCE", Buildspec: specAllPhases}})

	b := h.run(t, StartBuildConfig{})

	assert.Equal(t, "FAILED", b.BuildStatus)
	assert.Equal(t, "FAILED", phaseStatuses(b)["PROVISIONING"])
}

func TestEngineTimeout(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newFakeRuntime(fakeRule{match: "BUILD_CMD", block: true}))
	h.project(
		t,
		ProjectConfig{TimeoutInMinutes: 1, Source: &ProjectSource{Type: "NO_SOURCE", Buildspec: specAllPhases}},
	)

	b := h.run(t, StartBuildConfig{})

	assert.Equal(t, "TIMED_OUT", b.BuildStatus)
	assert.Equal(t, "TIMED_OUT", phaseStatuses(b)["BUILD"])
	assert.False(t, h.rt.ran("POST_CMD"))
}

func TestEngineStopBuild(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newFakeRuntime(fakeRule{match: "BUILD_CMD", block: true}))
	h.project(
		t,
		ProjectConfig{TimeoutInMinutes: 600, Source: &ProjectSource{Type: "NO_SOURCE", Buildspec: specAllPhases}},
	)

	b, err := h.b.StartBuild("proj", StartBuildConfig{})
	require.NoError(t, err)

	<-h.rt.started

	stopped, err := h.b.StopBuild(b.ID)
	require.NoError(t, err)
	assert.Equal(t, "STOPPED", stopped.BuildStatus)

	h.b.WaitBuilds()

	got, _ := h.b.BatchGetBuilds([]string{b.ID})
	assert.Equal(t, "STOPPED", got[0].BuildStatus)
	assert.Equal(t, "STOPPED", phaseStatuses(got[0])["BUILD"])
	assert.False(t, h.rt.ran("POST_CMD"))
	assert.Positive(t, h.rt.removed)
}

func TestEngineEnvAndLogs(t *testing.T) {
	t.Parallel()

	spec := `version: 0.2
env:
  variables:
    FROM_SPEC: spec
    OVERRIDDEN: spec
  parameter-store:
    DB_PASS: /db/pass
  secrets-manager:
    PW: mysecret:pw
phases:
  build:
    commands: [echo BUILD_CMD]
`
	rt := newFakeRuntime(fakeRule{match: "BUILD_CMD", out: "hello s3cr3t-param world\n"})
	h := newHarness(t, rt)
	h.project(t, ProjectConfig{Source: &ProjectSource{Type: "NO_SOURCE", Buildspec: spec}})

	b := h.run(t, StartBuildConfig{EnvVarsOverride: []EnvironmentVariable{
		{Name: "OVERRIDDEN", Value: "override"},
		{Name: "TOKEN", Value: "mysecret:user", Type: "SECRETS_MANAGER"},
	}})

	require.Equal(t, "SUCCEEDED", b.BuildStatus)

	require.True(t, rt.ran("export FROM_SPEC='spec'"))
	require.True(t, rt.ran("export OVERRIDDEN='override'"))
	require.True(t, rt.ran("export DB_PASS='s3cr3t-param'"))
	require.True(t, rt.ran("export PW='hunter2'"))
	require.True(t, rt.ran("export TOKEN='admin'"))
	require.True(t, rt.ran("export CODEBUILD_BUILD_ID="))

	logs := h.logs.joined()
	assert.Contains(t, logs, "Entering phase BUILD")
	assert.Contains(t, logs, "hello **** world")
	assert.NotContains(t, logs, "s3cr3t-param")
	assert.Equal(t, "/aws/codebuild/proj", h.logs.group)
	require.NotNil(t, b.Logs)
	assert.Equal(t, "/aws/codebuild/proj", b.Logs.GroupName)
	assert.NotEmpty(t, b.Logs.StreamName)
	assert.NotEmpty(t, b.Logs.CloudWatchLogsArn)
}

func TestEngineEnvResolveFailure(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newFakeRuntime())
	h.project(t, ProjectConfig{Source: &ProjectSource{Type: "NO_SOURCE", Buildspec: specAllPhases}})

	b := h.run(t, StartBuildConfig{EnvVarsOverride: []EnvironmentVariable{
		{Name: "X", Value: "/missing", Type: "PARAMETER_STORE"},
	}})

	assert.Equal(t, "FAILED", b.BuildStatus)
	assert.Equal(t, "FAILED", phaseStatuses(b)["DOWNLOAD_SOURCE"])
	assert.False(t, h.rt.ran("INSTALL_CMD"))
}

func TestEngineS3SourceAndArtifacts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		artifacts ProjectArtifacts
		spec      string
		wantKeys  []string
	}{
		{
			name: "none_packaging_base_dir",
			artifacts: ProjectArtifacts{
				Type:          "S3",
				Location:      "out",
				Path:          "pfx",
				NamespaceType: "BUILD_ID",
				Name:          "app",
			},
			spec: `version: 0.2
phases:
  build:
    commands: [echo BUILD_CMD]
artifacts:
  base-directory: dist
  files: ['**/*']
`,
			wantKeys: []string{"out/pfx/%s/app/bin/tool", "out/pfx/%s/app/a.txt"},
		},
		{
			name:      "discard_paths",
			artifacts: ProjectArtifacts{Type: "S3", Location: "out", Name: "flat"},
			spec: `version: 0.2
phases:
  build:
    commands: [echo BUILD_CMD]
artifacts:
  discard-paths: yes
  base-directory: dist
  files: ['**/*']
`,
			wantKeys: []string{"out/flat/tool", "out/flat/a.txt"},
		},
		{
			name:      "zip_packaging",
			artifacts: ProjectArtifacts{Type: "S3", Location: "out", Packaging: "ZIP", Name: "bundle.zip"},
			spec: `version: 0.2
phases:
  build:
    commands: [echo BUILD_CMD]
artifacts:
  base-directory: dist
  files: ['a.txt']
`,
			wantKeys: []string{"out/bundle.zip"},
		},
		{
			name:      "secondary",
			artifacts: ProjectArtifacts{Type: "S3", Location: "out", Name: "p"},
			spec: `version: 0.2
phases:
  build:
    commands: [echo BUILD_CMD]
artifacts:
  files: ['src-file.txt']
  secondary-artifacts:
    sec1:
      base-directory: dist
      files: ['a.txt']
`,
			wantKeys: []string{"out/p/src-file.txt", "out2/sec1/a.txt"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFakeRuntime(fakeRule{match: "BUILD_CMD", create: func(dir string) {
				_ = os.MkdirAll(filepath.Join(dir, "dist", "bin"), 0o750)
				_ = os.WriteFile(filepath.Join(dir, "dist", "a.txt"), []byte("A"), 0o600)
				_ = os.WriteFile(filepath.Join(dir, "dist", "bin", "tool"), []byte("T"), 0o600)
			}})
			h := newHarness(t, rt)
			h.store.objects["src/code.zip"] = zipOf(map[string]string{"src-file.txt": "S", "buildspec.yml": tt.spec})

			cfg := ProjectConfig{
				Source:    &ProjectSource{Type: "S3", Location: "src/code.zip"},
				Artifacts: &tt.artifacts,
			}
			if tt.name == "secondary" {
				cfg.SecondaryArtifacts = []ProjectArtifacts{
					{Type: "S3", Location: "out2", ArtifactIdentifier: "sec1", Name: "sec1"},
				}
			}

			h.project(t, cfg)
			b := h.run(t, StartBuildConfig{})
			require.Equal(t, "SUCCEEDED", b.BuildStatus, "%+v", b.Phases)

			uuid := strings.TrimPrefix(b.ID, "proj:")
			keys := h.store.keys()

			for _, w := range tt.wantKeys {
				want := w
				if strings.Contains(w, "%s") {
					want = strings.Replace(w, "%s", uuid, 1)
				}

				assert.Contains(t, keys, want)
			}
		})
	}
}

func TestEngineArtifactsNoMatchFails(t *testing.T) {
	t.Parallel()

	spec := "version: 0.2\nphases:\n  build:\n    commands: [echo BUILD_CMD]\nartifacts:\n  files: ['nothing/**/*']\n"
	h := newHarness(t, newFakeRuntime())
	h.project(t, ProjectConfig{
		Source:    &ProjectSource{Type: "NO_SOURCE", Buildspec: spec},
		Artifacts: &ProjectArtifacts{Type: "S3", Location: "out"},
	})

	b := h.run(t, StartBuildConfig{})

	assert.Equal(t, "FAILED", b.BuildStatus)
	assert.Equal(t, "FAILED", phaseStatuses(b)["UPLOAD_ARTIFACTS"])
}

func TestEngineBuildspecOverrideAndS3Logs(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newFakeRuntime())
	h.project(t, ProjectConfig{
		Source: &ProjectSource{
			Type:      "NO_SOURCE",
			Buildspec: "version: 0.2\nphases:\n  build:\n    commands: [echo FROM_PROJECT]\n",
		},
		LogsConfig: &LogsConfig{
			CloudWatchLogs: CloudWatchLogsConfig{Status: "DISABLED"},
			S3Logs:         S3LogsConfig{Status: "ENABLED", Location: "logbucket/pre"},
		},
	})

	b := h.run(
		t,
		StartBuildConfig{BuildspecOverride: "version: 0.2\nphases:\n  build:\n    commands: [echo FROM_OVERRIDE]\n"},
	)

	require.Equal(t, "SUCCEEDED", b.BuildStatus)
	assert.True(t, h.rt.ran("FROM_OVERRIDE"))
	assert.False(t, h.rt.ran("FROM_PROJECT"))
	assert.Empty(t, h.logs.joined())
	assert.Contains(t, h.store.keys(), "logbucket/pre/"+strings.TrimPrefix(b.ID, "proj:")+".gz")
}

func TestEngineInvalidGitLocationFails(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newFakeRuntime())
	h.project(t, ProjectConfig{Source: &ProjectSource{Type: "GITHUB", Location: "not-a-url"}})

	b := h.run(t, StartBuildConfig{})

	assert.Equal(t, "FAILED", b.BuildStatus)
	assert.Equal(t, "FAILED", phaseStatuses(b)["DOWNLOAD_SOURCE"])
}

func TestStubModeUnchanged(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateProject(ProjectConfig{
		Name:      "proj",
		Source:    &ProjectSource{Type: "NO_SOURCE"},
		Artifacts: &ProjectArtifacts{Type: "NO_ARTIFACTS"},
		Environment: &ProjectEnvironment{
			Type:        "LINUX_CONTAINER",
			Image:       "aws/codebuild/standard:7.0",
			ComputeType: "BUILD_GENERAL1_SMALL",
		},
	})
	require.NoError(t, err)

	build, err := b.StartBuild("proj", StartBuildConfig{})
	require.NoError(t, err)
	assert.Equal(t, "IN_PROGRESS", build.BuildStatus)
	assert.False(t, b.EngineEnabled())

	stopped, err := b.StopBuild(build.ID)
	require.NoError(t, err)
	assert.Equal(t, "STOPPED", stopped.BuildStatus)
}

func TestParseBuildspecFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		spec    string
		wantErr bool
	}{
		{name: "v02", spec: "version: 0.2\nphases:\n  build:\n    commands: [a]\n"},
		{name: "v01", spec: "version: 0.1\nphases:\n  build:\n    commands: [a]\n"},
		{name: "no_version", spec: "phases:\n  build:\n    commands: [a]\n", wantErr: true},
		{name: "bad_version", spec: "version: 3\n", wantErr: true},
		{name: "bad_yaml", spec: "version: [", wantErr: true},
		{name: "unknown_phase", spec: "version: 0.2\nphases:\n  deploy:\n    commands: [a]\n", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, err := parseBuildspecFile(tt.spec)
			if tt.wantErr {
				require.ErrorIs(t, err, errBuildspec)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, f.Phases)
		})
	}
}

func TestCommandScripts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		version    string
		cmds       []string
		wantSubstr []string
		wantN      int
	}{
		{name: "empty", version: "0.2", wantN: 0},
		{
			name:       "v02_one_shell",
			version:    "0.2",
			cmds:       []string{"cd x", "pwd"},
			wantN:      1,
			wantSubstr: []string{"trap", "cd x", "SUCCEEDING=1"},
		},
		{
			name:       "v01_per_command",
			version:    "0.1",
			cmds:       []string{"cd x", "pwd"},
			wantN:      2,
			wantSubstr: []string{"Running command"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := commandScripts(tt.version, tt.cmds, "/src", true)
			require.Len(t, got, tt.wantN)

			for _, s := range tt.wantSubstr {
				assert.Contains(t, got[0], s)
			}
		})
	}
}

func TestSpecVarsAndRefs(t *testing.T) {
	t.Parallel()

	f, err := parseBuildspecFile(
		"version: 0.2\nenv:\n  variables: {B: 2, A: x}\n  parameter-store: {P: /p}\n  secrets-manager: {S: id:key}\n",
	)
	require.NoError(t, err)

	assert.Equal(t, []EnvironmentVariable{
		{Name: "A", Value: "x", Type: "PLAINTEXT"}, {Name: "B", Value: "2", Type: "PLAINTEXT"},
	}, f.specVars())
	assert.Equal(t, []EnvironmentVariable{
		{Name: "P", Value: "/p", Type: "PARAMETER_STORE"}, {Name: "S", Value: "id:key", Type: "SECRETS_MANAGER"},
	}, f.specRefs())
}

func TestGlobMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern, name string
		want          bool
	}{
		{"**/*", "a/b/c.txt", true},
		{"**/*", "c.txt", true},
		{"*", "c.txt", true},
		{"*", "a/c.txt", false},
		{"*.jar", "app.jar", true},
		{"*.jar", "app.war", false},
		{"target/*.jar", "target/app.jar", true},
		{"target/**/*", "target/x/y.class", true},
		{"target/**/*", "other/x.class", false},
		{"./a.txt", "a.txt", true},
		{"a/**/z", "a/z", true},
		{"a/**/z", "a/b/c/z", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, globMatch(tt.pattern, tt.name))
		})
	}
}

func TestMatchFilesDirectoryPattern(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "d", "e"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "d", "e", "f"), []byte("x"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "top"), []byte("x"), 0o600))

	got, err := matchFiles(dir, []string{"d"})
	require.NoError(t, err)
	assert.Equal(t, []string{"d/e/f"}, got)

	got, err = matchFiles(dir, []string{"d/", "top"})
	require.NoError(t, err)
	assert.Equal(t, []string{"d/e/f", "top"}, got)
}

func TestHelpers(t *testing.T) {
	t.Parallel()

	t.Run("resolve_image", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "ubuntu:22.04", resolveImage("aws/codebuild/standard:7.0", "ubuntu:22.04"))
		assert.Equal(t, "ubuntu:22.04", resolveImage("", "ubuntu:22.04"))
		assert.Equal(t, "alpine:3", resolveImage("alpine:3", "ubuntu:22.04"))
	})

	t.Run("secret_ref", func(t *testing.T) {
		t.Parallel()

		id, rest := splitSecretRef("arn:aws:secretsmanager:us-east-1:123:secret:x-abc:key:AWSCURRENT")
		assert.Equal(t, "arn:aws:secretsmanager:us-east-1:123:secret:x-abc", id)
		assert.Equal(t, []string{"key", "AWSCURRENT"}, rest)

		id, rest = splitSecretRef("name:k")
		assert.Equal(t, "name", id)
		assert.Equal(t, []string{"k"}, rest)
	})

	t.Run("s3_location", func(t *testing.T) {
		t.Parallel()

		b, k, ok := splitS3Location("arn:aws:s3:::bkt/a/b.zip")
		assert.True(t, ok)
		assert.Equal(t, "bkt", b)
		assert.Equal(t, "a/b.zip", k)

		_, _, ok = splitS3Location("bkt")
		assert.False(t, ok)
	})

	t.Run("repo_name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(
			t,
			"myrepo",
			repoNameFromLocation("https://git-codecommit.us-east-1.amazonaws.com/v1/repos/myrepo"),
		)
		assert.Equal(t, "myrepo", repoNameFromLocation("myrepo"))
	})

	t.Run("failure_message", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "Error while executing command: false. Reason: exit status 1",
			failureMessage("x\n[Container] t Command did not exit successfully false exit status 1\n", 1))
		assert.Contains(t, failureMessage("boom", 7), "exit status 7")
	})

	t.Run("safe_join", func(t *testing.T) {
		t.Parallel()

		_, err := safeJoin("/w", "../etc/passwd")
		require.ErrorIs(t, err, errUnsafePath)

		p, err := safeJoin("/w", "a/b")
		require.NoError(t, err)
		assert.Equal(t, "/w/a/b", p)
	})

	t.Run("unzip_rejects_escaping_entry", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		zw := zip.NewWriter(&buf)
		w, err := zw.Create("../escape.txt")
		require.NoError(t, err)
		_, err = w.Write([]byte("x"))
		require.NoError(t, err)
		require.NoError(t, zw.Close())

		dir := t.TempDir()
		require.ErrorIs(t, unzipInto(buf.Bytes(), filepath.Join(dir, "src")), errUnsafePath)
		assert.NoFileExists(t, filepath.Join(dir, "escape.txt"))
	})

	t.Run("inline_buildspec", func(t *testing.T) {
		t.Parallel()
		assert.True(t, isInlineBuildspec("version: 0.2"))
		assert.True(t, isInlineBuildspec("a\nb"))
		assert.False(t, isInlineBuildspec("ci/buildspec.yml"))
	})
}
