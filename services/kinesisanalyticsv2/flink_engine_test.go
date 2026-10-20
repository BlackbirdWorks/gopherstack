package kinesisanalyticsv2_test

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

const (
	flinkTestBucket = "arn:aws:s3:::code-bucket"
	flinkTestKey    = "app.jar"
	flinkTestJar    = "fake-jar-bytes"
	flinkSavepoint  = "file:/opt/flink/savepoints/savepoint-abc-123"
)

type fakeFlinkRuntime struct {
	failRun error
	mu      *lockmetrics.RWMutex
	specs   []container.Spec
	props   []string
	removed []string
}

func newFakeFlinkRuntime() *fakeFlinkRuntime {
	return &fakeFlinkRuntime{mu: lockmetrics.New("fakeFlinkRuntime")}
}

func (r *fakeFlinkRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.mu.Lock("create")
	defer r.mu.Unlock()

	if r.failRun != nil {
		return "", r.failRun
	}

	propsHost, _, _ := strings.Cut(spec.Mounts[0], ":")
	raw, err := os.ReadFile(propsHost)
	if err != nil {
		return "", err
	}

	r.specs = append(r.specs, spec)
	r.props = append(r.props, string(raw))

	return "ctr-" + string(rune('0'+len(r.specs))), nil
}

func (r *fakeFlinkRuntime) StopAndRemove(_ context.Context, id string) error {
	r.mu.Lock("remove")
	defer r.mu.Unlock()

	r.removed = append(r.removed, id)

	return nil
}

func (r *fakeFlinkRuntime) snapshot() ([]container.Spec, []string, []string) {
	r.mu.RLock("snapshot")
	defer r.mu.RUnlock()

	return append([]container.Spec(nil), r.specs...), append([]string(nil), r.props...),
		append([]string(nil), r.removed...)
}

type fakeFlinkREST struct {
	mu       *lockmetrics.RWMutex
	state    string
	savepath string
	jar      []byte
	runs     []map[string]any
	calls    []string
}

func newFakeFlinkREST() *fakeFlinkREST {
	return &fakeFlinkREST{mu: lockmetrics.New("fakeFlinkREST"), state: "INITIALIZING", savepath: flinkSavepoint}
}

func (f *fakeFlinkREST) setState(s string) {
	f.mu.Lock("state")
	defer f.mu.Unlock()

	f.state = s
}

func (f *fakeFlinkREST) seen() []string {
	f.mu.RLock("seen")
	defer f.mu.RUnlock()

	return append([]string(nil), f.calls...)
}

func (f *fakeFlinkREST) runRequests() []map[string]any {
	f.mu.RLock("runs")
	defer f.mu.RUnlock()

	return append([]map[string]any(nil), f.runs...)
}

func (f *fakeFlinkREST) Do(req *http.Request) (*http.Response, error) {
	f.mu.Lock("do")
	defer f.mu.Unlock()

	p := req.URL.Path
	f.calls = append(f.calls, req.Method+" "+p)

	switch {
	case p == "/overview":
		return jsonResp(`{}`), nil
	case p == "/jars/upload":
		f.jar = readUpload(req)

		return jsonResp(`{"filename":"/tmp/flink-web/flink-web-upload/uuid_application.jar","status":"success"}`), nil
	case strings.HasSuffix(p, "/run"):
		var body map[string]any

		_ = json.NewDecoder(req.Body).Decode(&body)
		f.runs = append(f.runs, body)

		return jsonResp(`{"jobid":"job1"}`), nil
	case req.Method == http.MethodGet && p == "/jobs/job1":
		return jsonResp(`{"state":"` + f.state + `"}`), nil
	case req.Method == http.MethodPatch && p == "/jobs/job1":
		return jsonResp(``), nil
	case strings.HasSuffix(p, "/savepoints") || strings.HasSuffix(p, "/stop"):
		return jsonResp(`{"request-id":"trig1"}`), nil
	case strings.Contains(p, "/savepoints/trig1"):
		return jsonResp(`{"status":{"id":"COMPLETED"},"operation":{"location":"` + f.savepath + `"}}`), nil
	}

	return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader("nope"))}, nil
}

func jsonResp(body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}
}

func readUpload(req *http.Request) []byte {
	_, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		return nil
	}

	part, err := multipart.NewReader(req.Body, params["boundary"]).NextPart()
	if err != nil {
		return nil
	}

	b, _ := io.ReadAll(part)

	return b
}

type flinkHarness struct {
	b  *kinesisanalyticsv2.InMemoryBackend
	rt *fakeFlinkRuntime
	fl *fakeFlinkREST
}

func newFlinkHarness(t *testing.T) *flinkHarness {
	t.Helper()

	h := &flinkHarness{
		b:  kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1"),
		rt: newFakeFlinkRuntime(),
		fl: newFakeFlinkREST(),
	}

	h.b.EnableFlink(kinesisanalyticsv2.FlinkConfig{
		Runtime:      h.rt,
		HTTP:         h.fl,
		EndpointURL:  "http://172.17.0.1:8000",
		PollInterval: 2 * time.Millisecond,
		StartTimeout: 10 * time.Second,
		StopTimeout:  10 * time.Second,
		Code: func(_ context.Context, bucket, key, _ string) ([]byte, error) {
			if bucket != flinkTestBucket || key != flinkTestKey {
				return nil, io.ErrUnexpectedEOF
			}

			return []byte(flinkTestJar), nil
		},
	})
	t.Cleanup(h.b.Close)

	return h
}

func (h *flinkHarness) createApp(t *testing.T, name string, snapshots bool) {
	t.Helper()

	ctx := context.Background()
	par := int32(3)
	interval := int64(5000)
	enabled := true

	_, err := h.b.CreateApplication(ctx, name, "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)
	require.NoError(t, h.b.SeedApplicationConfiguration(ctx, name, kinesisanalyticsv2.SeedConfig{
		CodeConfig: &kinesisanalyticsv2.ApplicationCodeConfigDesc{
			CodeContentType: "ZIPFILE",
			CodeContentDescription: &kinesisanalyticsv2.CodeContentDescription{
				S3ApplicationCodeLocationDescription: &kinesisanalyticsv2.S3CodeLocationDesc{
					BucketARN: flinkTestBucket, FileKey: flinkTestKey,
				},
			},
		},
		FlinkConfig: &kinesisanalyticsv2.FlinkApplicationConfigDesc{
			ParallelismConfigurationDescription: &kinesisanalyticsv2.ParallelismConfigDesc{Parallelism: &par},
			CheckpointConfigurationDescription: &kinesisanalyticsv2.CheckpointConfigDesc{
				CheckpointingEnabled: &enabled, CheckpointInterval: &interval,
			},
		},
		SnapshotsEnabled: &snapshots,
		EnvironmentPropertyGroups: []kinesisanalyticsv2.PropertyGroup{
			{PropertyGroupID: "Sink", PropertyMap: map[string]string{"stream": "out"}},
		},
	}))
}

func (h *flinkHarness) status(t *testing.T, name string) string {
	t.Helper()

	app, err := h.b.DescribeApplication(context.Background(), name)
	require.NoError(t, err)

	return app.ApplicationStatus
}

func (h *flinkHarness) waitStatus(t *testing.T, name, want string) {
	t.Helper()

	require.Eventually(t, func() bool { return h.status(t, name) == want }, 10*time.Second, 2*time.Millisecond,
		"want %s, have %s", want, h.status(t, name))
}

func (h *flinkHarness) opStatus(t *testing.T, name, opID string) string {
	t.Helper()

	op, err := h.b.DescribeApplicationOperation(context.Background(), name, opID)
	require.NoError(t, err)

	return op.OperationStatus
}

func TestFlinkEngine_StartLaunchesRealJob(t *testing.T) {
	t.Parallel()

	h := newFlinkHarness(t)
	h.createApp(t, "wc", false)

	opID, err := h.b.StartApplication(context.Background(), "wc", nil, nil)
	require.NoError(t, err)

	h.fl.setState("RUNNING")
	h.waitStatus(t, "wc", "RUNNING")

	specs, props, _ := h.rt.snapshot()
	require.Len(t, specs, 1)
	assert.Equal(t, "flink:1.18", specs[0].Image)
	assert.Contains(t, specs[0].Env, "AWS_ENDPOINT_URL=http://172.17.0.1:8000")
	assert.Contains(t, specs[0].Env, "AWS_REGION=us-east-1")
	assert.Contains(t, strings.Join(specs[0].Env, "|"), "taskmanager.numberOfTaskSlots: 3")
	assert.Contains(t, strings.Join(specs[0].Env, "|"), "execution.checkpointing.interval: 5000ms")
	assert.Contains(t, specs[0].Mounts[0], ":/etc/flink/application_properties.json:ro")
	assert.Contains(t, specs[0].Ports[0], ":8081")
	assert.JSONEq(t, `[{"PropertyGroupId":"Sink","PropertyMap":{"stream":"out"}}]`, props[0])

	assert.Equal(t, []byte(flinkTestJar), h.fl.jar)

	runs := h.fl.runRequests()
	require.Len(t, runs, 1)
	assert.InDelta(t, 3, runs[0]["parallelism"], 0)
	assert.Equal(t, "SUCCESSFUL", h.opStatus(t, "wc", opID))
}

func TestFlinkEngine_StopCancelsAndRemoves(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantCall     string
		snapshots    bool
		force        bool
		wantSnapshot bool
	}{
		{name: "cancel without snapshots", snapshots: false, wantCall: "PATCH /jobs/job1"},
		{name: "force skips savepoint", snapshots: true, force: true, wantCall: "PATCH /jobs/job1"},
		{name: "savepoint on stop", snapshots: true, wantSnapshot: true, wantCall: "POST /jobs/job1/stop"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newFlinkHarness(t)
			h.createApp(t, "app", tt.snapshots)
			h.fl.setState("RUNNING")

			_, err := h.b.StartApplication(context.Background(), "app", nil, nil)
			require.NoError(t, err)
			h.waitStatus(t, "app", "RUNNING")

			opID, err := h.b.StopApplication(context.Background(), "app", tt.force)
			require.NoError(t, err)
			h.waitStatus(t, "app", "READY")

			assert.Contains(t, h.fl.seen(), tt.wantCall)
			assert.Equal(t, "SUCCESSFUL", h.opStatus(t, "app", opID))

			_, _, removed := h.rt.snapshot()
			assert.Equal(t, []string{"ctr-1"}, removed)

			snaps, _, err := h.b.ListApplicationSnapshots(context.Background(), "app", "", 0)
			require.NoError(t, err)

			if tt.wantSnapshot {
				assert.Empty(t, snaps, "the stop savepoint is internal, not a listed snapshot")

				_, err = h.b.StartApplication(context.Background(), "app", nil, nil)
				require.NoError(t, err)
				h.waitStatus(t, "app", "RUNNING")
				assert.Equal(t, flinkSavepoint, h.fl.runRequests()[1]["savepointPath"])
			} else {
				assert.Empty(t, snaps)
			}
		})
	}
}

func TestFlinkEngine_SnapshotAndRestore(t *testing.T) {
	t.Parallel()

	h := newFlinkHarness(t)
	h.createApp(t, "app", false)
	h.fl.setState("RUNNING")
	ctx := context.Background()

	_, err := h.b.StartApplication(ctx, "app", nil, nil)
	require.NoError(t, err)
	h.waitStatus(t, "app", "RUNNING")

	_, err = h.b.CreateApplicationSnapshot(ctx, "app", "snap1")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		s, derr := h.b.DescribeApplicationSnapshot(ctx, "app", "snap1")

		return derr == nil && s.SnapshotStatus == "READY"
	}, 10*time.Second, 2*time.Millisecond)

	_, err = h.b.StopApplication(ctx, "app", true)
	require.NoError(t, err)
	h.waitStatus(t, "app", "READY")

	_, err = h.b.StartApplication(ctx, "app", &kinesisanalyticsv2.RunConfigInput{
		ApplicationRestoreConfiguration: &kinesisanalyticsv2.ApplicationRestoreConfig{
			ApplicationRestoreType: "RESTORE_FROM_CUSTOM_SNAPSHOT", SnapshotName: "snap1",
		},
	}, nil)
	require.NoError(t, err)
	h.waitStatus(t, "app", "RUNNING")

	runs := h.fl.runRequests()
	require.Len(t, runs, 2)
	assert.Equal(t, flinkSavepoint, runs[1]["savepointPath"])

	_, err = h.b.StartApplication(ctx, "app", nil, nil)
	require.Error(t, err)
	require.Error(t, h.b.DeleteApplicationSnapshot(ctx, "app", "missing"))
	require.NoError(t, h.b.DeleteApplicationSnapshot(ctx, "app", "snap1"))
}

func TestFlinkEngine_FailureReturnsToReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prep    func(h *flinkHarness)
		wantOp  string
		wantErr bool
	}{
		{name: "job failed", prep: func(h *flinkHarness) { h.fl.setState("FAILED") }, wantOp: "FAILED"},
		{name: "job finished", prep: func(h *flinkHarness) { h.fl.setState("FINISHED") }, wantOp: "SUCCESSFUL"},
		{name: "container error", prep: func(h *flinkHarness) { h.rt.failRun = io.ErrClosedPipe }, wantOp: "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newFlinkHarness(t)
			h.createApp(t, "app", false)
			tt.prep(h)

			opID, err := h.b.StartApplication(context.Background(), "app", nil, nil)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				return h.status(t, "app") == "READY" && h.opStatus(t, "app", opID) == tt.wantOp
			}, 10*time.Second, 2*time.Millisecond)
		})
	}
}

func TestFlinkEngine_DeleteTearsDown(t *testing.T) {
	t.Parallel()

	h := newFlinkHarness(t)
	h.createApp(t, "app", false)
	h.fl.setState("RUNNING")

	_, err := h.b.StartApplication(context.Background(), "app", nil, nil)
	require.NoError(t, err)
	h.waitStatus(t, "app", "RUNNING")

	require.NoError(t, h.b.DeleteApplication(context.Background(), "app", nil))

	require.Eventually(t, func() bool {
		_, _, removed := h.rt.snapshot()

		return len(removed) == 1
	}, 10*time.Second, 2*time.Millisecond)
}

func TestFlinkEngine_NonFlinkRuntimeUnchanged(t *testing.T) {
	t.Parallel()

	h := newFlinkHarness(t)
	_, err := h.b.CreateApplication(context.Background(), "sql", "SQL-1_0", "", "", "", nil)
	require.NoError(t, err)

	_, err = h.b.StartApplication(context.Background(), "sql", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", h.status(t, "sql"))

	specs, _, _ := h.rt.snapshot()
	assert.Empty(t, specs)
}

func TestFlinkEngine_ForceStopWhileStarting(t *testing.T) {
	t.Parallel()

	h := newFlinkHarness(t)
	h.createApp(t, "app", true)

	_, err := h.b.StartApplication(context.Background(), "app", nil, nil)
	require.NoError(t, err)

	_, err = h.b.StopApplication(context.Background(), "app", false)
	require.Error(t, err)

	_, err = h.b.StopApplication(context.Background(), "app", true)
	require.NoError(t, err)
	h.waitStatus(t, "app", "READY")

	require.Eventually(t, func() bool {
		specs, _, removed := h.rt.snapshot()

		return len(specs) == len(removed)
	}, 10*time.Second, 2*time.Millisecond)
}
