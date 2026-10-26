package codebuild

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sourceSpec = "version: 0.2\nphases:\n  build:\n    commands: [echo BUILD_CMD]\n"

func listTree(dir string) []string {
	var out []string

	_ = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			rel, _ := filepath.Rel(dir, p)
			out = append(out, filepath.ToSlash(rel))
		}

		return nil
	})
	sort.Strings(out)

	return out
}

func TestEngineS3FolderAndSecondarySources(t *testing.T) {
	t.Parallel()

	objects := map[string]string{
		"srcb/proj/buildspec.yml": sourceSpec,
		"srcb/proj/a.txt":         "A",
		"srcb/proj/sub/b.txt":     "B",
		"srcb/other/c.txt":        "C",
		"srcb/libs/l1.txt":        "L1",
		"srcb/libs/deep/l2.txt":   "L2",
		"srcb/tool.txt":           "T",
	}

	tests := []struct {
		name          string
		source        ProjectSource
		secondary     []ProjectSource
		versions      []ProjectSourceVersion
		wantPrimary   []string
		wantSecondary map[string][]string
	}{
		{
			name:        "s3_folder_primary",
			source:      ProjectSource{Type: "S3", Location: "srcb/proj/"},
			wantPrimary: []string{"a.txt", "buildspec.yml", "sub/b.txt"},
		},
		{
			name:   "secondary_folder_and_object",
			source: ProjectSource{Type: "NO_SOURCE", Buildspec: sourceSpec},
			secondary: []ProjectSource{
				{Type: "S3", Location: "srcb/libs/", SourceIdentifier: "libs"},
				{Type: "S3", Location: "srcb/tool.txt", SourceIdentifier: "tool"},
			},
			wantPrimary: []string{},
			wantSecondary: map[string][]string{
				"libs": {"deep/l2.txt", "l1.txt"},
				"tool": {"tool.txt"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				mu       sync.Mutex
				primary  []string
				secFiles = map[string][]string{}
				rt       *fakeRuntime
			)

			rt = newFakeRuntime(fakeRule{match: "BUILD_CMD", create: func(dir string) {
				mu.Lock()
				defer mu.Unlock()

				primary = listTree(dir)

				rt.mu.RLock("m")
				defer rt.mu.RUnlock()

				for _, m := range rt.mounts {
					host, cont, _ := strings.Cut(m, ":")
					if id := filepath.Base(cont); id == "libs" || id == "tool" {
						secFiles[id] = listTree(host)
					}
				}
			}})
			h := newHarnessWith(t, rt, nil)

			for k, v := range objects {
				require.NoError(t, h.store.PutObjectBytes(t.Context(), strings.SplitN(k, "/", 2)[0], strings.SplitN(k, "/", 2)[1], []byte(v)))
			}

			h.project(t, ProjectConfig{Source: &tt.source, SecondarySources: tt.secondary})
			b := h.run(t, StartBuildConfig{})
			require.Equal(t, "SUCCEEDED", b.BuildStatus, b.Phases)

			mu.Lock()
			defer mu.Unlock()

			assert.ElementsMatch(t, tt.wantPrimary, primary)

			for id, want := range tt.wantSecondary {
				assert.Equal(t, want, secFiles[id], id)
				assert.True(t, rt.ran("CODEBUILD_SRC_DIR_"+id+"="))
			}
		})
	}
}
