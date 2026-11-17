package gcpsql

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type recordingVolumes struct {
	removed []string
	created []string
	mu      sync.Mutex
}

func (r *recordingVolumes) CreateVolume(_ context.Context, name string, _ map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.created = append(r.created, name)

	return nil
}

func (r *recordingVolumes) RemoveVolume(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removed = append(r.removed, name)

	return nil
}

func (*recordingVolumes) ListUnusedVolumes(context.Context, string, time.Duration) ([]string, error) {
	return nil, nil
}

func TestArchiveVolumeOwnership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantRemoved []string
		spec        LaunchSpec
		retained    bool
		promoted    bool
	}{
		{name: "writer owns the archive", wantRemoved: []string{"data", "pitr"}},
		{name: "standby borrows it", spec: LaunchSpec{Standby: true}, wantRemoved: []string{"data"}},
		{
			name:        "promoted standby owns it",
			spec:        LaunchSpec{Standby: true},
			promoted:    true,
			wantRemoved: []string{"data", "pitr"},
		},
		{name: "retired writer passed it on", retained: true, wantRemoved: []string{"data"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vols := &recordingVolumes{}
			e := &dockerEngine{
				vols: vols, volData: "data", volPITR: "pitr", spec: tt.spec, retained: tt.retained,
				promoted: tt.promoted, mu: lockmetrics.New("test"),
			}

			e.RemoveVolumes(t.Context())
			assert.Equal(t, tt.wantRemoved, vols.removed)
		})
	}
}

func TestStandbyLaunchKeepsActiveArchive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantRemoved []string
		spec        LaunchSpec
	}{
		{
			name:        "fresh writer wipes stale volumes",
			spec:        LaunchSpec{Fresh: true},
			wantRemoved: []string{"data", "pitr"},
		},
		{
			name:        "fresh standby keeps the archive",
			spec:        LaunchSpec{Fresh: true, Standby: true},
			wantRemoved: []string{"data"},
		},
		{name: "restart wipes nothing", spec: LaunchSpec{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vols := &recordingVolumes{}
			e := &dockerEngine{vols: vols, volData: "data", volPITR: "pitr", spec: tt.spec, mu: lockmetrics.New("test")}

			require.NoError(t, e.createVolumes(t.Context()))
			assert.Equal(t, tt.wantRemoved, vols.removed)
			assert.Equal(t, []string{"data", "pitr"}, vols.created)
		})
	}
}

func TestArchiveCommandAcceptsIdenticalSegment(t *testing.T) {
	t.Parallel()

	assert.Contains(t, pgArchiveCmd, "cmp -s", "a promoted standby re-offers segments the old writer archived")
	assert.NotContains(t, pgArchiveCmd, "'", "the command is embedded in a quoted SQL literal")
}

func TestArchiveCommandNeverOverwrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		existing *string
		name     string
		segment  string
		wantFile string
		wantOK   bool
	}{
		{name: "new segment", segment: "A", wantOK: true, wantFile: "A"},
		{name: "same content", existing: new("A"), segment: "A", wantOK: true, wantFile: "A"},
		{name: "different content", existing: new("A"), segment: "B", wantOK: false, wantFile: "A"},
		{name: "stale partial", existing: nil, segment: "A", wantOK: true, wantFile: "A"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			wal := filepath.Join(root, "wal")
			require.NoError(t, os.MkdirAll(wal, 0o750))

			src := filepath.Join(root, "seg")
			require.NoError(t, os.WriteFile(src, []byte(tt.segment), 0o600))

			if tt.existing != nil {
				require.NoError(t, os.WriteFile(filepath.Join(wal, "000001"), []byte(*tt.existing), 0o600))
			} else {
				require.NoError(t, os.WriteFile(filepath.Join(wal, "000001.part"), []byte("torn"), 0o600))
			}

			cmd := strings.NewReplacer(pitrMount, root, "%f", "000001", "%p", src).Replace(pgArchiveCmd)
			err := exec.CommandContext(t.Context(), "sh", "-c", cmd).Run()
			assert.Equal(t, tt.wantOK, err == nil)

			got, rerr := os.ReadFile(filepath.Join(wal, "000001"))
			require.NoError(t, rerr)
			assert.Equal(t, tt.wantFile, string(got))
			assert.NoFileExists(t, filepath.Join(wal, "000001.part"))
		})
	}
}
