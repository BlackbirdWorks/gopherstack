package bedrockagentcore_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

var errCodeGone = errors.New("gone")

type specRecorder struct {
	files map[string]string
	specs []container.Spec
	mu    sync.Mutex
}

func (r *specRecorder) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.specs = append(r.specs, spec)
	r.files = map[string]string{}

	for _, m := range spec.Mounts {
		if dir, _, ok := strings.Cut(m, ":/var/task"); ok {
			raw, _ := os.ReadFile(filepath.Join(dir, "main.py"))
			r.files["main.py"] = string(raw)
			raw, _ = os.ReadFile(filepath.Join(dir, "pkg", "lib.py"))
			r.files["pkg/lib.py"] = string(raw)
		}
	}

	return "c1", nil
}

func (*specRecorder) StopAndRemove(context.Context, string) error { return nil }

func (*specRecorder) Exec(context.Context, string, []string) (container.ExecResult, error) {
	return container.ExecResult{}, nil
}

func zipOf(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, body := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)

		_, err = w.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func TestInvokeCodeRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		runtime   string
		entry     []string
		wantImage string
		wantCmd   []string
	}{
		{
			name: "python with wrapper", runtime: "PYTHON_3_13", entry: []string{"opentelemetry-instrument", "main.py"},
			wantImage: "python:3.13-slim", wantCmd: []string{"opentelemetry-instrument", "python3", "main.py"},
		},
		{
			name: "node", runtime: "NODE_22", entry: []string{"main.py"}, wantImage: "node:22-slim",
			wantCmd: []string{"node", "main.py"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pkg := zipOf(t, map[string]string{"main.py": "print('hi')", "pkg/lib.py": "X = 1"})
			ctl := defaultControl()
			ctl.runtimes[testRuntime+"|DEFAULT"] = bedrockagentcore.RuntimeInfo{
				ID: testRuntime, Protocol: "HTTP",
				Code: &bedrockagentcore.CodeInfo{
					Runtime: tt.runtime, EntryPoint: tt.entry,
					Load: func() ([]byte, error) { return pkg, nil },
				},
			}

			rec := &specRecorder{}
			f := newFixture(t, ctl)
			f.backend.EnableEngine(bedrockagentcore.EngineConfig{Runtime: rec, HTTP: pingOK{}})

			_, err := f.client.InvokeAgentRuntime(t.Context(), &sdk.InvokeAgentRuntimeInput{
				AgentRuntimeArn:  aws.String(runtimeARN),
				RuntimeSessionId: aws.String("session-code-" + strings.Repeat("d", 30)),
				Payload:          []byte(`{}`),
			})
			require.NoError(t, err)

			rec.mu.Lock()
			defer rec.mu.Unlock()

			require.Len(t, rec.specs, 1)
			assert.Equal(t, tt.wantImage, rec.specs[0].Image)
			assert.Equal(t, tt.wantCmd, rec.specs[0].Cmd)
			assert.Equal(t, "/var/task", rec.specs[0].WorkingDir)
			assert.Equal(t, "print('hi')", rec.files["main.py"])
			assert.Equal(t, "X = 1", rec.files["pkg/lib.py"])
		})
	}
}

func TestInvokeCodeRuntimeLoadFailure(t *testing.T) {
	t.Parallel()

	ctl := defaultControl()
	ctl.runtimes[testRuntime+"|DEFAULT"] = bedrockagentcore.RuntimeInfo{
		ID: testRuntime, Protocol: "HTTP",
		Code: &bedrockagentcore.CodeInfo{
			Runtime: "PYTHON_3_13", EntryPoint: []string{"main.py"},
			Load: func() ([]byte, error) { return nil, errCodeGone },
		},
	}

	f := newFixture(t, ctl)
	f.backend.EnableEngine(bedrockagentcore.EngineConfig{Runtime: &specRecorder{}, HTTP: pingOK{}})

	_, err := f.client.InvokeAgentRuntime(t.Context(), &sdk.InvokeAgentRuntimeInput{
		AgentRuntimeArn:  aws.String(runtimeARN),
		RuntimeSessionId: aws.String("session-code-" + strings.Repeat("e", 30)),
		Payload:          []byte(`{}`),
	})
	assert.Equal(t, "InternalServerException", apiCode(err))
}

func TestExtractZipRejectsEscapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		files   map[string]string
		name    string
		wantErr bool
	}{
		{name: "plain", files: map[string]string{"a/b.py": "x"}},
		{name: "parent traversal", files: map[string]string{"../evil.py": "x"}, wantErr: true},
		{name: "nested traversal", files: map[string]string{"a/../../evil.py": "x"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			err := bedrockagentcore.ExtractZip(zipOf(t, tt.files), filepath.Join(dir, "out"))

			if !tt.wantErr {
				require.Error(t, err, "the target directory must exist")
			}

			require.NoError(t, os.MkdirAll(filepath.Join(dir, "out"), 0o750))

			err = bedrockagentcore.ExtractZip(zipOf(t, tt.files), filepath.Join(dir, "out"))
			if tt.wantErr {
				require.Error(t, err)
				assert.NoFileExists(t, filepath.Join(dir, "evil.py"))

				return
			}

			require.NoError(t, err)
			assert.FileExists(t, filepath.Join(dir, "out", "a", "b.py"))
		})
	}
}

func TestCodeImageRuntimeAllowList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runtime string
		want    string
		wantErr bool
	}{
		{runtime: "PYTHON_3_13", want: "python:3.13-slim"},
		{runtime: "NODE_22", want: "node:22-slim"},
		{runtime: "PYTHON_3_9;evil", wantErr: true},
		{runtime: "PYTHON_../x", wantErr: true},
		{runtime: "JAVA_21", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.runtime, func(t *testing.T) {
			t.Parallel()

			got, err := bedrockagentcore.CodeImage(tt.runtime)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
