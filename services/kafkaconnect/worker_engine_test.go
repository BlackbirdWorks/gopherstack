package kafkaconnect_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/kafkaconnect"
)

type fakeWorkerRuntime struct {
	files   map[string]string
	started []container.Spec
	removed []string
	mu      sync.Mutex
}

func (f *fakeWorkerRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.started = append(f.started, spec)

	if f.files == nil {
		f.files = map[string]string{}
	}

	dir, _, _ := strings.Cut(spec.Mounts[0], ":")
	if b, err := os.ReadFile(filepath.Join(dir, "worker.properties")); err == nil {
		f.files["worker.properties"] = string(b)
	}

	if b, err := os.ReadFile(filepath.Join(dir, "plugins", "myplugin", "conn", "a.txt")); err == nil {
		f.files["plugin"] = string(b)
	}

	return "ctr-" + strconv.Itoa(len(f.started)), nil
}

func (f *fakeWorkerRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *fakeWorkerRuntime) removedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.removed...)
}

// fakeConnectREST answers the Connect REST calls the engine makes.
type fakeConnectREST struct {
	putBody  string
	status   string
	requests []string
	putCode  int
	mu       sync.Mutex
}

func (f *fakeConnectREST) Do(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.requests = append(f.requests, req.Method+" "+req.URL.Path)

	code, body := http.StatusOK, "[]"

	switch {
	case req.Method == http.MethodPut:
		raw, _ := io.ReadAll(req.Body)
		f.putBody = string(raw)
		code, body = f.putCode, "{}"
	case strings.HasSuffix(req.URL.Path, "/status"):
		body = f.status
	}

	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
}

const (
	runningStatus = `{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}`
	failedStatus  = `{"connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"FAILED","trace":"boom\nstack"}]}`
)

func pluginZip(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create("conn/a.txt")
	require.NoError(t, err)
	_, err = w.Write([]byte("jar-bytes"))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestWorkerEngine_ConnectorLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		status    string
		wantState string
		wantCode  string
		putCode   int
	}{
		{name: "running", status: runningStatus, putCode: http.StatusCreated, wantState: "RUNNING"},
		{
			name:      "task_failed",
			status:    failedStatus,
			putCode:   http.StatusCreated,
			wantState: "FAILED",
			wantCode:  "WORKER_FAILED",
		},
		{
			name:      "config_rejected",
			status:    runningStatus,
			putCode:   http.StatusBadRequest,
			wantState: "FAILED",
			wantCode:  "WORKER_FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeWorkerRuntime{}
			rest := &fakeConnectREST{status: tt.status, putCode: tt.putCode}
			b := kafkaconnect.NewInMemoryBackend()
			b.EnableWorkers(kafkaconnect.WorkerConfig{
				Runtime: rt, HTTP: rest, PollInterval: 5 * time.Millisecond, StartTimeout: 5 * time.Second,
				Plugins: func(_, _, _ string) ([]byte, error) { return pluginZip(t), nil },
			})
			t.Cleanup(b.Close)

			plugin, err := b.CreateCustomPlugin("123456789012", "us-east-1", "myplugin", "", "ZIP",
				"arn:aws:s3:::bkt", "p.zip", "", nil)
			require.NoError(t, err)

			wc, err := b.CreateWorkerConfiguration(
				"123456789012",
				"us-east-1",
				"wc",
				"",
				base64.StdEncoding.EncodeToString([]byte("offset.flush.interval.ms=77\n")),
				nil,
			)
			require.NoError(t, err)

			c, err := b.CreateConnector("123456789012", "us-east-1", kafkaconnect.ConnectorSpec{
				Name:                    "conn",
				ConnectorConfiguration:  map[string]string{"connector.class": "X", "topics": "t"},
				ServiceExecutionRoleArn: "arn:aws:iam::123456789012:role/r",
				Capacity: kafkaconnect.Capacity{
					Provisioned: &kafkaconnect.ProvisionedCapacity{McuCount: 1, WorkerCount: 1},
				},
				ApacheKafkaCluster:  kafkaconnect.ApacheKafkaCluster{BootstrapServers: "127.0.0.1:9092"},
				Plugins:             []kafkaconnect.PluginRef{{CustomPluginArn: plugin.ARN, Revision: 1}},
				WorkerConfiguration: &kafkaconnect.WorkerConfigRef{Arn: wc.ARN, Revision: 1},
			})
			require.NoError(t, err)
			assert.Equal(t, "CREATING", c.State)

			require.Eventually(t, func() bool {
				got, derr := b.DescribeConnector(c.ARN)

				return derr == nil && got.State == tt.wantState
			}, 10*time.Second, 5*time.Millisecond)

			got, err := b.DescribeConnector(c.ARN)
			require.NoError(t, err)

			desc := b.ConnectorStateDescription(got.ARN)
			if tt.wantCode == "" {
				assert.Nil(t, desc)
			} else {
				require.NotNil(t, desc)
				assert.Equal(t, tt.wantCode, desc.Code)
			}

			rt.mu.Lock()
			assert.Contains(t, rt.files["worker.properties"], "bootstrap.servers=127.0.0.1:9092")
			assert.Contains(t, rt.files["worker.properties"], "offset.flush.interval.ms=77")
			assert.Equal(t, "jar-bytes", rt.files["plugin"])
			rt.mu.Unlock()

			rest.mu.Lock()
			assert.Contains(t, rest.putBody, `"connector.class":"X"`)
			rest.mu.Unlock()

			_, err = b.DeleteConnector(c.ARN, "")
			require.NoError(t, err)
			require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
		})
	}
}
