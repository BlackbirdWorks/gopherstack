package transcribe_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

type fakeRuntime struct {
	execErr   error
	stdout    string
	specs     []container.Spec
	removed   []string
	execStdin []string
	exitCode  int
	mu        sync.Mutex
}

func (f *fakeRuntime) HasImage(context.Context, string) (bool, error) { return true, nil }
func (f *fakeRuntime) PullImage(context.Context, string) error        { return nil }

func (f *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.specs = append(f.specs, spec)

	return "cid", nil
}

func (f *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *fakeRuntime) Exec(_ context.Context, _ string, cmd []string) (container.ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.execStdin = append(f.execStdin, strings.Join(cmd, " "))

	return container.ExecResult{Stdout: f.stdout, Stderr: "boom", ExitCode: f.exitCode}, f.execErr
}

func modelArchive(t *testing.T, entries map[string]string) ([]byte, string) {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, body := range entries {
		w, err := zw.Create(name)
		require.NoError(t, err)
		_, err = w.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	sum := sha256.Sum256(buf.Bytes())

	return buf.Bytes(), hex.EncodeToString(sum[:])
}

func TestVoskEngine_Recognize(t *testing.T) {
	t.Parallel()

	const words = `[{"word":"hello","start":0.5,"end":0.9,"conf":0.97},{"word":"world","start":1,"end":1.4,"conf":1}]`

	tests := []struct {
		entries  map[string]string
		name     string
		stdout   string
		wantErr  string
		exitCode int
		wantLen  int
		badSum   bool
	}{
		{name: "ok", entries: map[string]string{"m1/conf/model.conf": "x"}, stdout: "noise\n" + words, wantLen: 2},
		{name: "checksum", entries: map[string]string{"m1/conf/model.conf": "x"}, badSum: true, wantErr: "checksum"},
		{name: "zip slip", entries: map[string]string{"../evil": "x"}, wantErr: "unsafe model archive"},
		{
			name: "exec failure", entries: map[string]string{"m1/conf/model.conf": "x"}, exitCode: 1,
			wantErr: "recognizer failed",
		},
		{
			name:    "bad output",
			entries: map[string]string{"m1/conf/model.conf": "x"},
			stdout:  "not json",
			wantErr: "unparsable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			archive, sum := modelArchive(t, tt.entries)
			if tt.badSum {
				sum = strings.Repeat("0", len(sum))
			}

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write(archive)
			}))
			t.Cleanup(srv.Close)

			rt := &fakeRuntime{stdout: tt.stdout, exitCode: tt.exitCode}
			root := t.TempDir()
			eng := transcribe.NewVoskEngine(transcribe.VoskConfig{
				Runtime: rt, HTTP: srv.Client(), ModelDir: root, ModelBaseURL: srv.URL + "/",
				Models: map[string]transcribe.VoskModel{"en-US": {Name: "m1", SHA256: sum}},
			})
			t.Cleanup(func() { eng.Close(context.WithoutCancel(t.Context())) })

			assert.True(t, eng.SupportsLanguage("en-US"))
			assert.False(t, eng.SupportsLanguage("fr-FR"))

			got, err := eng.Recognize(t.Context(), "en-US", 16000, make([]byte, 64))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.NoFileExists(t, filepath.Join(root, "evil"))

				return
			}

			require.NoError(t, err)
			require.Len(t, got, tt.wantLen)
			assert.Equal(t, transcribe.Word{Text: "hello", Start: 0.5, End: 0.9, Confidence: 0.97}, got[0])

			_, err = eng.Recognize(t.Context(), "en-US", 16000, make([]byte, 64))
			require.NoError(t, err)

			require.Len(t, rt.specs, 1, "container is reused across calls")
			assert.Equal(t, []string{"sleep", "infinity"}, rt.specs[0].Entrypoint)
			assert.Equal(t, filepath.Join(root, "m1")+":/model:ro", rt.specs[0].Mounts[0])
			assert.FileExists(t, filepath.Join(root, "m1", "conf", "model.conf"))
			require.Len(t, rt.execStdin, 2)
			assert.Contains(t, rt.execStdin[0], "python3 /work/recognize.py /work/job-")

			eng.Close(t.Context())
			assert.Equal(t, []string{"cid"}, rt.removed)

			workDir, _, _ := strings.Cut(rt.specs[0].Mounts[1], ":")
			_, statErr := os.Stat(workDir)
			assert.True(t, os.IsNotExist(statErr), "work dir removed on close")
		})
	}
}
