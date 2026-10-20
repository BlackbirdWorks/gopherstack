package codebuild

import (
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const dockerTestImage = "alpine:3.20"

func dockerEngineHarness(t *testing.T) *engineHarness {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	if out, perr := exec.CommandContext(t.Context(), "docker", "pull", "-q", dockerTestImage).
		CombinedOutput(); perr != nil {
		t.Skipf("cannot pull %s: %v %s", dockerTestImage, perr, out)
	}

	h := &engineHarness{
		b:     NewInMemoryBackend("000000000000", "us-east-1"),
		store: newFakeStore(),
		logs:  &fakeLogs{mu: lockmetrics.New("fakelogs")},
	}
	h.b.EnableEngine(EngineConfig{
		Runtime: rt, DefaultImage: dockerTestImage, WorkDir: t.TempDir(), CacheDir: t.TempDir(),
		TimeoutUnit: time.Minute,
	})
	h.b.SetEngineServices(EngineServices{Store: h.store, Logs: h.logs, Values: fakeValues{}})

	return h
}

const dockerSpec = `version: 0.2
env:
  variables:
    GREETING: hello
  parameter-store:
    DB_PASS: /db/pass
phases:
  install:
    commands:
      - mkdir -p out/nested
      - cd out
      - export FROM_INSTALL=carried
  build:
    commands:
      - echo "$GREETING from $CODEBUILD_BUILD_NUMBER in $(basename $PWD) $FROM_INSTALL"
      - echo "pass=$DB_PASS"
      - echo built > nested/result.txt
    finally:
      - echo FINALLY_RAN
  post_build:
    commands:
      - echo "succeeding=$CODEBUILD_BUILD_SUCCEEDING"
artifacts:
  base-directory: out
  files: ['**/*']
`

func TestDockerEngineEndToEnd(t *testing.T) {
	t.Parallel()

	h := dockerEngineHarness(t)
	h.project(t, ProjectConfig{
		Source:    &ProjectSource{Type: "S3", Location: "src/code.zip"},
		Artifacts: &ProjectArtifacts{Type: "S3", Location: "artifacts", Name: "app"},
	})
	h.store.objects["src/code.zip"] = zipOf(map[string]string{"buildspec.yml": dockerSpec, "README": "x"})

	b := h.run(t, StartBuildConfig{})

	require.Equal(t, "SUCCEEDED", b.BuildStatus, "%+v", b.Phases)

	logs := h.logs.joined()
	assert.Contains(t, logs, "hello from")
	assert.Contains(t, logs, "out carried")
	assert.Contains(t, logs, "pass=****")
	assert.NotContains(t, logs, "s3cr3t-param")
	assert.Contains(t, logs, "FINALLY_RAN")
	assert.Contains(t, logs, "succeeding=1")
	assert.Contains(t, h.store.keys(), "artifacts/app/nested/result.txt")
}

const failSpec = `version: 0.2
phases:
  build:
    commands: [echo before, 'exit 3', echo after]
  post_build:
    commands: ['echo succeeding=$CODEBUILD_BUILD_SUCCEEDING']
`

func TestDockerEngineFailureAndStop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		spec       string
		wantStatus string
		wantLog    string
		stop       bool
	}{
		{
			name:       "failure",
			spec:       failSpec,
			wantStatus: "FAILED",
			wantLog:    "succeeding=0",
		},
		{
			name: "stop", stop: true,
			spec:       "version: 0.2\nphases:\n  build:\n    commands: [echo STARTED, sleep 300]\n",
			wantStatus: "STOPPED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dockerEngineHarness(t)
			h.project(t, ProjectConfig{Source: &ProjectSource{Type: "NO_SOURCE", Buildspec: tt.spec}})

			b, err := h.b.StartBuild("proj", StartBuildConfig{})
			require.NoError(t, err)

			if tt.stop {
				require.Eventually(t, func() bool {
					got, _ := h.b.BatchGetBuilds([]string{b.ID})

					return got[0].CurrentPhase == "BUILD"
				}, 2*time.Minute, 100*time.Millisecond)

				_, err = h.b.StopBuild(b.ID)
				require.NoError(t, err)
			}

			h.b.WaitBuilds()

			got, _ := h.b.BatchGetBuilds([]string{b.ID})
			assert.Equal(t, tt.wantStatus, got[0].BuildStatus)
			assert.Contains(t, h.logs.joined(), tt.wantLog)

			if tt.name == "failure" {
				assert.NotContains(t, h.logs.joined(), "after\n")
				assert.Contains(t, h.logs.joined(), "exit status 3")
			}
		})
	}
}

func TestDockerEngineStreamsLogsWhileRunning(t *testing.T) {
	t.Parallel()

	h := dockerEngineHarness(t)
	h.project(t, ProjectConfig{Source: &ProjectSource{
		Type:      "NO_SOURCE",
		Buildspec: "version: 0.2\nphases:\n  build:\n    commands: [echo STREAMED_LINE, sleep 300]\n",
	}})

	b, err := h.b.StartBuild("proj", StartBuildConfig{})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return strings.Contains(h.logs.joined(), "STREAMED_LINE") },
		2*time.Minute, 100*time.Millisecond)

	got, _ := h.b.BatchGetBuilds([]string{b.ID})
	assert.Equal(t, "IN_PROGRESS", got[0].BuildStatus)

	_, err = h.b.StopBuild(b.ID)
	require.NoError(t, err)
	h.b.WaitBuilds()
}

func TestDockerEngineAbsoluteLocalCache(t *testing.T) {
	t.Parallel()

	spec := `version: 0.2
phases:
  build:
    commands:
      - if [ -f /root/.cachetest/x ]; then echo CACHE_HIT; else echo CACHE_MISS; fi
      - mkdir -p /root/.cachetest && echo hi > /root/.cachetest/x
cache:
  paths:
    - /root/.cachetest/**/*
`
	h := dockerEngineHarness(t)
	h.project(t, ProjectConfig{
		Source: &ProjectSource{Type: "NO_SOURCE", Buildspec: spec},
		Cache:  &ProjectCache{Type: "LOCAL", Modes: []string{"LOCAL_CUSTOM_CACHE"}},
	})

	require.Equal(t, "SUCCEEDED", h.run(t, StartBuildConfig{}).BuildStatus)
	assert.Contains(t, h.logs.joined(), "CACHE_MISS")

	require.Equal(t, "SUCCEEDED", h.run(t, StartBuildConfig{}).BuildStatus)
	assert.Contains(t, h.logs.joined(), "CACHE_HIT")
}
