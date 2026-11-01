package guards_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const jsonV1Path = "encoding/json"

// importsJSONV1 reports whether src imports encoding/json (v1); string
// literals containing the path do not count.
func importsJSONV1(name string, src any) (bool, error) {
	f, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly)
	if err != nil {
		return false, err
	}

	for _, imp := range f.Imports {
		if p, uerr := strconv.Unquote(imp.Path.Value); uerr == nil && p == jsonV1Path {
			return true, nil
		}
	}

	return false, nil
}

func TestImportsJSONV1(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want bool
	}{
		{name: "single", src: "package p\nimport \"encoding/json\"\n", want: true},
		{name: "grouped", src: "package p\nimport (\n\t\"fmt\"\n\t\"encoding/json\"\n)\n", want: true},
		{name: "aliased", src: "package p\nimport j \"encoding/json\"\n", want: true},
		{name: "blank", src: "package p\nimport _ \"encoding/json\"\n", want: true},
		{name: "v2", src: "package p\nimport \"encoding/json/v2\"\n", want: false},
		{name: "jsontext", src: "package p\nimport \"encoding/json/jsontext\"\n", want: false},
		{name: "raw string", src: "package p\nconst s = `import \"encoding/json\"`\n", want: false},
		{name: "comment", src: "package p\n// import \"encoding/json\"\n", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := importsJSONV1("x.go", tt.src)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestNoEncodingJSONV1Import forbids encoding/json outside pkgs/jsonx, which
// owns the v1 option set; everything else uses encoding/json/v2 and jsontext.
func TestNoEncodingJSONV1Import(t *testing.T) {
	t.Parallel()

	var offenders []string

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			switch {
			case path == filepath.Join("pkgs", "jsonx"),
				d.Name() == "testdata", d.Name() == "node_modules",
				path != "." && strings.HasPrefix(d.Name(), "."):
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		bad, parseErr := importsJSONV1(path, src)
		if parseErr != nil {
			return nil //nolint:nilerr // unparsable files are the compiler's concern
		}

		if bad {
			offenders = append(offenders, path)
		}

		return nil
	})
	require.NoError(t, err)
	assert.Empty(t, offenders, "import encoding/json/v2 (+ jsontext) and pass jsonx.V1() instead of encoding/json")
}
