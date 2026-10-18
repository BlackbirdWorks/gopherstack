package guards_test

import (
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepositoryHygieneNoTempFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args struct {
			checkReject func(relPath string, isDir bool) bool
		}
		want struct {
			description string
		}
	}{
		{
			name: "jetbrains safe write temp files",
			args: struct {
				checkReject func(relPath string, isDir bool) bool
			}{
				checkReject: func(relPath string, isDir bool) bool {
					base := filepath.Base(relPath)

					return !isDir && (strings.HasPrefix(base, ".!") || strings.Contains(base, ".!"))
				},
			},
			want: struct {
				description string
			}{
				description: "files starting with or containing .! (JetBrains safe-write)",
			},
		},
		{
			name: "editor swap and backup files",
			args: struct {
				checkReject func(relPath string, isDir bool) bool
			}{
				checkReject: func(relPath string, isDir bool) bool {
					base := filepath.Base(relPath)

					return !isDir && (strings.HasSuffix(base, ".swp") || strings.HasSuffix(base, ".swo") ||
						strings.HasSuffix(base, ".swn") || strings.HasSuffix(base, "~") ||
						(strings.HasPrefix(base, "#") && strings.HasSuffix(base, "#")))
				},
			},
			want: struct {
				description string
			}{
				description: "editor swap or backup files (.swp, .swo, .swn, ~, #*#)",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			var violations []string

			err := filepath.WalkDir(".", func(path string, d fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if d.IsDir() &&
					(d.Name() == ".git" || d.Name() == "node_modules" || d.Name() == ".svelte-kit" || d.Name() == ".dolt") {
					return filepath.SkipDir
				}
				if tt.args.checkReject(path, d.IsDir()) {
					violations = append(violations, path)
				}

				return nil
			})

			require.NoError(t, err)
			assert.Empty(t, violations, "found disallowed files (%s): %v", tt.want.description, violations)
		})
	}
}
