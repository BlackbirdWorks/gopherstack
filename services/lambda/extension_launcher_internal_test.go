package lambda

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestWrapWithExtensionLauncher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		files      map[string]os.FileMode
		entrypoint []string
		want       []string
		wantCount  int
	}{
		{name: "no_extensions", files: map[string]os.FileMode{"lib/x.so": 0o644}},
		{name: "not_executable", files: map[string]os.FileMode{"extensions/e": 0o644}},
		{
			name: "default_entrypoint", files: map[string]os.FileMode{"extensions/e": 0o755},
			want:      []string{"/bin/sh", extensionLauncherPath, baseImageEntrypoint},
			wantCount: 1,
		},
		{
			name: "provided_entrypoint", files: map[string]os.FileMode{"extensions/a": 0o755, "extensions/b": 0o755},
			entrypoint: []string{"/var/task/bootstrap"},
			want:       []string{"/bin/sh", extensionLauncherPath, "/var/task/bootstrap"},
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			for name, mode := range tt.files {
				p := filepath.Join(dir, name)
				require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
				require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
				require.NoError(t, os.Chmod(p, mode))
			}

			assert.Len(t, listLayerExtensions(dir), tt.wantCount)

			spec := container.Spec{Entrypoint: tt.entrypoint}
			wrapWithExtensionLauncher(&spec, dir+":/opt:ro")

			if tt.want == nil {
				assert.Equal(t, tt.entrypoint, spec.Entrypoint)
				assert.NoFileExists(t, filepath.Join(dir, extensionLauncherName))

				return
			}

			assert.Equal(t, tt.want, spec.Entrypoint)
			assert.FileExists(t, filepath.Join(dir, extensionLauncherName))
		})
	}

	spec := container.Spec{}
	wrapWithExtensionLauncher(&spec, "")
	assert.Empty(t, spec.Entrypoint)
}
