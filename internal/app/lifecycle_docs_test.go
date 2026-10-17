package app

import (
	"github.com/blackbirdworks/gopherstack/internal/cliconfig"

	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var lifecycleFlagRef = regexp.MustCompile(`--lifecycle-[a-z0-9]+(?:-[a-z0-9]+)*`)

func lifecycleFlagNames(t *testing.T) map[string]bool {
	t.Helper()

	names := map[string]bool{}
	typ := reflect.TypeFor[cliconfig.LifecycleSettings]()

	for f := range typ.Fields() {
		name := f.Tag.Get("name")
		require.NotEmpty(t, name, "field %s needs an explicit name tag", f.Name)

		names["--lifecycle-"+name] = true
	}

	return names
}

func TestDocumentedLifecycleFlagsExist(t *testing.T) {
	t.Parallel()

	known := lifecycleFlagNames(t)

	tests := []struct {
		name string
		root string
	}{
		{name: "root docs", root: "."},
		{name: "service docs", root: "services"},
		{name: "docs dir", root: "docs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var bad []string

			walk := func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}

				if d.IsDir() && tt.root == "." && path != "." {
					return fs.SkipDir
				}

				if d.IsDir() || !strings.HasSuffix(path, ".md") {
					return nil
				}

				data, rErr := os.ReadFile(path)
				if rErr != nil {
					return rErr
				}

				for _, flag := range lifecycleFlagRef.FindAllString(string(data), -1) {
					if !known[flag] {
						bad = append(bad, path+": "+flag)
					}
				}

				return nil
			}

			require.NoError(t, filepath.WalkDir(tt.root, walk))
			assert.Empty(t, bad, "documented --lifecycle-* flags that do not exist")
		})
	}
}
