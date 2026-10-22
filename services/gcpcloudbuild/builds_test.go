package gcpcloudbuild_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestBuildLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{
		"busybox": exits(0, "hello from step"),
		"alpine":  exits(0),
	})

	got := e.run(t, buildOf(
		step("busybox", withID("greet"), withArgs("echo", "hello")),
		step("alpine", withArgs("true")),
	))

	assert.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus())
	assert.Empty(t, got.GetStatusDetail())
	assert.NotNil(t, got.GetStartTime())
	assert.NotNil(t, got.GetFinishTime())
	assert.Equal(t, project, got.GetProjectId())
	assert.True(t, strings.HasPrefix(got.GetName(), parent+"/builds/"))
	assert.Equal(t, 10*time.Minute, got.GetTimeout().AsDuration())
	assert.Equal(t, "gs://"+project+"_cloudbuild", got.GetLogsBucket())

	for _, s := range got.GetSteps() {
		assert.Equal(t, cloudbuildpb.Build_SUCCESS, s.GetStatus())
		assert.NotNil(t, s.GetTiming())
	}

	assert.Equal(t, []string{"QUEUED", "WORKING", "SUCCESS"}, e.pub.statuses())
	assert.Equal(t, "projects/"+project+"/topics/cloud-builds", e.pub.msgs[0].topic)
	assert.Contains(t, e.logs.lines(), `Step #0 - "greet": hello from step`)

	log, ok := e.store.get(
		strings.TrimPrefix(got.GetLogsBucket(), "gs://") + "/log-" + got.GetId() + ".txt",
	)
	require.True(t, ok)
	assert.Contains(t, string(log), `Step #0 - "greet": hello from step`)
	assert.Equal(t, 2, e.rt.removals())

	listed, err := e.client.GetBuild(t.Context(), toGet(got.GetId()))
	require.NoError(t, err)
	assert.Equal(t, got.GetId(), listed.GetId())
}

func toGet(id string) *cloudbuildpb.GetBuildRequest {
	return &cloudbuildpb.GetBuildRequest{ProjectId: project, Id: id}
}

func TestStepOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build      *cloudbuildpb.Build
		name       string
		wantDetail string
		wantSteps  []cloudbuildpb.Build_Status
		wantRan    []string
		wantStatus cloudbuildpb.Build_Status
	}{
		{
			name: "failure cancels later steps",
			build: buildOf(
				step("ok"), step("bad"), step("never"),
			),
			wantStatus: cloudbuildpb.Build_FAILURE,
			wantDetail: `build step 1 "bad" failed: step exited with non-zero status: 3`,
			wantSteps: []cloudbuildpb.Build_Status{
				cloudbuildpb.Build_SUCCESS, cloudbuildpb.Build_FAILURE, cloudbuildpb.Build_CANCELLED,
			},
			wantRan: []string{"ok", "bad"},
		},
		{
			name: "allow failure continues",
			build: buildOf(
				step("bad", func(s *cloudbuildpb.BuildStep) { s.AllowFailure = true }), step("ok"),
			),
			wantStatus: cloudbuildpb.Build_SUCCESS,
			wantSteps: []cloudbuildpb.Build_Status{
				cloudbuildpb.Build_FAILURE,
				cloudbuildpb.Build_SUCCESS,
			},
			wantRan: []string{"bad", "ok"},
		},
		{
			name: "allowed exit code",
			build: buildOf(
				step(
					"bad",
					func(s *cloudbuildpb.BuildStep) { s.AllowExitCodes = []int32{3} },
				),
				step("ok"),
			),
			wantStatus: cloudbuildpb.Build_SUCCESS,
			wantSteps: []cloudbuildpb.Build_Status{
				cloudbuildpb.Build_SUCCESS,
				cloudbuildpb.Build_SUCCESS,
			},
			wantRan: []string{"bad", "ok"},
		},
		{
			name:       "missing image",
			build:      buildOf(step("ghost")),
			wantStatus: cloudbuildpb.Build_FAILURE,
			wantDetail: `build step 0 "ghost" failed: start step: No such image: ghost`,
			wantSteps:  []cloudbuildpb.Build_Status{cloudbuildpb.Build_FAILURE},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(
				t,
				map[string]behavior{"ok": exits(0), "bad": exits(3, "boom"), "never": exits(0)},
			)
			got := e.run(t, tt.build)

			assert.Equal(t, tt.wantStatus, got.GetStatus())
			assert.Equal(t, tt.wantDetail, got.GetStatusDetail())

			statuses := make([]cloudbuildpb.Build_Status, len(got.GetSteps()))
			for i, s := range got.GetSteps() {
				statuses[i] = s.GetStatus()
			}

			assert.Equal(t, tt.wantSteps, statuses)
			assert.Equal(t, tt.wantRan, e.rt.startedImages())
			assert.Equal(t, len(tt.wantRan), e.rt.removals())
		})
	}
}

func TestWaitForOrdering(t *testing.T) {
	t.Parallel()

	// a and b only finish once both are running, which proves they run in parallel; c waits for both.
	var (
		mu      sync.Mutex
		running int
		bothUp  = make(chan struct{})
		order   []string
	)

	record := func(name string) behavior {
		return func(ctx context.Context, _ container.Spec, _ io.Writer) int {
			mu.Lock()
			order = append(order, "start-"+name)

			if running++; running == 2 {
				close(bothUp)
			}
			mu.Unlock()

			if name != "c" {
				select {
				case <-bothUp:
				case <-ctx.Done():
				}
			}

			mu.Lock()
			order = append(order, "end-"+name)
			mu.Unlock()

			return 0
		}
	}

	e := newEnv(t, map[string]behavior{"a": record("a"), "b": record("b"), "c": record("c")})

	got := e.run(t, buildOf(
		step("a", withID("a"), waitFor("-")),
		step("b", withID("b"), waitFor("-")),
		step("c", withID("c"), waitFor("a", "b")),
	))

	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

	mu.Lock()
	defer mu.Unlock()

	idx := func(s string) int {
		for i, v := range order {
			if v == s {
				return i
			}
		}

		return -1
	}

	assert.Greater(t, idx("start-c"), idx("end-a"))
	assert.Greater(t, idx("start-c"), idx("end-b"))
}

func TestWorkspaceSharedBetweenSteps(t *testing.T) {
	t.Parallel()

	write := func(_ context.Context, spec container.Spec, _ io.Writer) int {
		host, _, _ := strings.Cut(spec.Mounts[0], ":")

		return exitCode(os.WriteFile(filepath.Join(host, "out.txt"), []byte("from-writer"), 0o600))
	}

	read := func(_ context.Context, spec container.Spec, out io.Writer) int {
		host, _, _ := strings.Cut(spec.Mounts[0], ":")
		data, err := os.ReadFile(filepath.Join(host, "out.txt"))
		_, _ = io.WriteString(out, string(data)+"\n")

		return exitCode(err)
	}

	e := newEnv(t, map[string]behavior{"writer": write, "reader": read})
	got := e.run(t, buildOf(step("writer"), step("reader")))

	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())
	assert.Contains(t, e.logs.lines(), "Step #1: from-writer")

	spec := e.rt.specOf("writer")
	assert.Equal(t, "/workspace", spec.WorkingDir)
	assert.True(t, strings.HasSuffix(strings.SplitN(spec.Mounts[0], ":", 2)[1], "/workspace"))

	_, statErr := os.Stat(e.rt.workspaceOf("writer"))
	assert.True(t, os.IsNotExist(statErr), "workspace is removed after the build")
}

func exitCode(err error) int {
	if err != nil {
		return 1
	}

	return 0
}

func TestStepSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		step  *cloudbuildpb.BuildStep
		check func(t *testing.T, s container.Spec)
		name  string
	}{
		{
			name: "entrypoint args env dir",
			step: &cloudbuildpb.BuildStep{
				Name: "img", Entrypoint: "bash", Args: []string{"-c", "ls"}, Env: []string{"A=1"}, Dir: "sub/dir",
			},
			check: func(t *testing.T, s container.Spec) {
				t.Helper()

				assert.Equal(t, []string{"bash"}, s.Entrypoint)
				assert.Equal(t, []string{"-c", "ls"}, s.Cmd)
				assert.Equal(t, []string{"A=1"}, s.Env)
				assert.Equal(t, "/workspace/sub/dir", s.WorkingDir)
			},
		},
		{
			name: "absolute dir",
			step: &cloudbuildpb.BuildStep{Name: "img", Dir: "/tmp"},
			check: func(t *testing.T, s container.Spec) {
				t.Helper()

				assert.Equal(t, "/tmp", s.WorkingDir)
			},
		},
		{
			name: "script runs from the scripts mount",
			step: &cloudbuildpb.BuildStep{Name: "img", Script: "echo hi"},
			check: func(t *testing.T, s container.Spec) {
				t.Helper()

				require.Len(t, s.Entrypoint, 1)
				assert.Equal(t, "/gcb-scripts/step-0.sh", s.Entrypoint[0])
				assert.Empty(t, s.Cmd)
			},
		},
		{
			name: "volumes are shared paths",
			step: &cloudbuildpb.BuildStep{
				Name:    "img",
				Volumes: []*cloudbuildpb.Volume{{Name: "v", Path: "/cache"}},
			},
			check: func(t *testing.T, s container.Spec) {
				t.Helper()

				joined := strings.Join(s.Mounts, " ")
				assert.Contains(t, joined, ":/cache")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, map[string]behavior{"img": exits(0)})
			got := e.run(t, buildOf(tt.step))
			require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())
			tt.check(t, e.rt.specOf("img"))
		})
	}
}

func TestCancelBuild(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	e := newEnv(t, map[string]behavior{"slow": blocks(started), "after": exits(0)})

	op, err := e.client.CreateBuild(t.Context(), &cloudbuildpb.CreateBuildRequest{
		Parent: parent, Build: buildOf(step("slow"), step("after")),
	})
	require.NoError(t, err)

	<-started

	meta, err := op.Metadata()
	require.NoError(t, err)

	cancelled, err := e.client.CancelBuild(
		t.Context(),
		&cloudbuildpb.CancelBuildRequest{Name: meta.GetBuild().GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, cloudbuildpb.Build_CANCELLED, cancelled.GetStatus())

	e.backend.Wait()

	got := e.get(t, meta.GetBuild().GetName())
	assert.Equal(t, cloudbuildpb.Build_CANCELLED, got.GetStatus())
	assert.Equal(t, []string{"slow"}, e.rt.startedImages())
	assert.Equal(t, 1, e.rt.removals(), "the running container is removed by id")
	assert.Equal(t, "CANCELLED", e.pub.statuses()[len(e.pub.statuses())-1])

	_, err = e.client.CancelBuild(
		t.Context(),
		&cloudbuildpb.CancelBuildRequest{Name: got.GetName()},
	)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestBuildTimeout(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	e := newEnv(t, map[string]behavior{"slow": blocks(started)})

	limit := 90 * time.Second
	b := buildOf(step("slow"))
	b.Timeout = timeout(limit)

	op, err := e.client.CreateBuild(
		t.Context(),
		&cloudbuildpb.CreateBuildRequest{Parent: parent, Build: b},
	)
	require.NoError(t, err)

	<-started
	require.True(t, e.timers.fire(limit))

	got, err := op.Wait(t.Context())
	require.NoError(t, err)
	e.backend.Wait()

	assert.Equal(t, cloudbuildpb.Build_TIMEOUT, got.GetStatus())
	assert.Contains(t, got.GetStatusDetail(), "did not complete within 1m30s")
	assert.Equal(t, 1, e.rt.removals())
}

func TestStepTimeout(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	e := newEnv(t, map[string]behavior{"slow": blocks(started)})

	limit := 7 * time.Second
	s := step("slow")
	s.Timeout = timeout(limit)

	op, err := e.client.CreateBuild(
		t.Context(),
		&cloudbuildpb.CreateBuildRequest{Parent: parent, Build: buildOf(s)},
	)
	require.NoError(t, err)

	<-started
	require.True(t, e.timers.fire(limit))

	got, err := op.Wait(t.Context())
	require.NoError(t, err)
	e.backend.Wait()

	assert.Equal(t, cloudbuildpb.Build_TIMEOUT, got.GetStatus())
	assert.Equal(t, cloudbuildpb.Build_TIMEOUT, got.GetSteps()[0].GetStatus())
	assert.Contains(t, got.GetStatusDetail(), `build step 0 "slow" timed out`)
}

func TestNoRuntime(t *testing.T) {
	t.Parallel()

	e := newEnv(t, nil)
	e.backend.DisableEngine("engine off for test")

	got := e.run(t, buildOf(step("busybox")))

	assert.Equal(t, cloudbuildpb.Build_FAILURE, got.GetStatus())
	assert.Equal(t, "engine off for test", got.GetStatusDetail())
	assert.Equal(t, []string{"QUEUED", "WORKING", "FAILURE"}, e.pub.statuses())
}
