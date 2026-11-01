package guards_test

import (
	"bufio"
	"bytes"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// embedUntrackedAllowlist lists generated embed targets that are intentionally untracked.
// dashboard/static/spa/* is built at build time and gitignored (only .keep is tracked).
var embedUntrackedAllowlist = []string{"dashboard/static/spa/"} //nolint:gochecknoglobals // immutable test table

func gitTrackedSet(t *testing.T, root string) map[string]bool {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), "git", "ls-files", "-z")
	cmd.Dir = root

	out, err := cmd.Output()
	if err != nil {
		return nil
	}

	set := map[string]bool{}
	for f := range bytes.SplitSeq(out, []byte{0}) {
		if len(f) > 0 {
			set[string(f)] = true
		}
	}

	return set
}

// splitEmbedPatterns splits a //go:embed argument list, honouring "..." and `...` quoting.
func splitEmbedPatterns(s string) []string {
	var (
		pats  []string
		cur   strings.Builder
		quote rune
		has   bool
	)

	flush := func() {
		if has {
			pats = append(pats, cur.String())
		}

		cur.Reset()

		has = false
	}

	for _, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '"' || r == '`':
			quote, has = r, true
		case r == ' ' || r == '\t':
			flush()
		default:
			cur.WriteRune(r)

			has = true
		}
	}

	flush()

	return pats
}

type embedDirective struct {
	file     string
	patterns []string
}

func findEmbedDirectives(t *testing.T, root string) []embedDirective {
	t.Helper()

	var found []embedDirective

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", ".dolt":
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}

		f, openErr := os.Open(p)
		if openErr != nil {
			return openErr
		}
		defer f.Close()

		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

		for sc.Scan() {
			if rest, ok := strings.CutPrefix(sc.Text(), "//go:embed "); ok {
				found = append(found, embedDirective{file: p, patterns: splitEmbedPatterns(rest)})
			}
		}

		return sc.Err()
	})
	require.NoError(t, err)

	return found
}

// expandEmbed returns slash-relative (to root) files matched by one pattern, or nil if none.
func expandEmbed(t *testing.T, root, pkgDir, pattern string) []string {
	t.Helper()

	all := false
	if rest, ok := strings.CutPrefix(pattern, "all:"); ok {
		pattern, all = rest, true
	}

	matches, err := filepath.Glob(filepath.Join(pkgDir, filepath.FromSlash(pattern)))
	require.NoError(t, err)

	var files []string

	for _, m := range matches {
		err = filepath.WalkDir(m, func(p string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}

			if p != m && !all && (strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "_")) {
				if d.IsDir() {
					return filepath.SkipDir
				}

				return nil
			}

			if !d.IsDir() {
				rel, relErr := filepath.Rel(root, p)
				if relErr != nil {
					return relErr
				}

				files = append(files, filepath.ToSlash(rel))
			}

			return nil
		})
		require.NoError(t, err)
	}

	return files
}

func embedViolations(t *testing.T, root string, tracked map[string]bool, allow []string) []string {
	t.Helper()

	var violations []string

	for _, dir := range findEmbedDirectives(t, root) {
		pkgDir := filepath.Dir(dir.file)

		for _, pat := range dir.patterns {
			files := expandEmbed(t, root, pkgDir, pat)
			if len(files) == 0 {
				violations = append(violations, dir.file+": pattern "+pat+" matches no files")

				continue
			}

			for _, f := range files {
				if tracked[f] || hasAnyPrefix(f, allow) {
					continue
				}

				violations = append(violations, dir.file+": embedded file "+f+" is not tracked by git (gitignored?)")
			}
		}
	}

	sort.Strings(violations)

	return violations
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}

	return false
}

func TestEmbedTargetsTrackedByGit(t *testing.T) {
	t.Parallel()

	tracked := gitTrackedSet(t, ".")
	if tracked == nil {
		t.Skip("not a git checkout")
	}

	violations := embedViolations(t, ".", tracked, embedUntrackedAllowlist)
	assert.Empty(t, violations, "go:embed targets must be committed; check .gitignore")
}

func TestEmbedGuardDetectsViolations(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	tests := []struct {
		name  string
		src   string
		files map[string]string
		track []string
		allow []string
		want  []string
	}{
		{
			name:  "tracked file ok",
			src:   "//go:embed a.txt\n",
			files: map[string]string{"pkg/a.txt": "x"},
			track: []string{"pkg/a.txt"},
		},
		{
			name:  "ignored file flagged",
			src:   "//go:embed a.txt b.txt\n",
			files: map[string]string{"pkg/a.txt": "x", "pkg/b.txt": "y", ".gitignore": "*.txt\n!a.txt\n"},
			track: []string{"pkg/a.txt"},
			want:  []string{"pkg/b.txt"},
		},
		{
			name:  "quoted pattern",
			src:   "//go:embed \"a b.txt\"\n",
			files: map[string]string{"pkg/a b.txt": "x"},
			want:  []string{"pkg/a b.txt"},
		},
		{
			name:  "missing match flagged",
			src:   "//go:embed nope.json\n",
			files: map[string]string{"pkg/other": "x"},
			track: []string{"pkg/other"},
			want:  []string{"matches no files"},
		},
		{
			name:  "dir skips dot and underscore files",
			src:   "//go:embed dir\n",
			files: map[string]string{"pkg/dir/a": "x", "pkg/dir/.h": "x", "pkg/dir/_u": "x"},
			track: []string{"pkg/dir/a"},
		},
		{
			name:  "all prefix includes dot files",
			src:   "//go:embed all:dir\n",
			files: map[string]string{"pkg/dir/a": "x", "pkg/dir/.h": "x"},
			track: []string{"pkg/dir/a"},
			want:  []string{"pkg/dir/.h"},
		},
		{
			name:  "glob pattern",
			src:   "//go:embed m/*.gz\n",
			files: map[string]string{"pkg/m/a.gz": "x", "pkg/m/b.gz": "x"},
			track: []string{"pkg/m/a.gz"},
			want:  []string{"pkg/m/b.gz"},
		},
		{
			name:  "allowlist exempts",
			src:   "//go:embed all:spa\n",
			files: map[string]string{"pkg/spa/app.js": "x"},
			allow: []string{"pkg/spa/"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			for rel, content := range tt.files {
				full := filepath.Join(root, filepath.FromSlash(rel))
				require.NoError(t, os.MkdirAll(path.Dir(full), 0o755))
				require.NoError(t, os.WriteFile(full, []byte(content), 0o644))
			}

			require.NoError(t, os.MkdirAll(filepath.Join(root, "pkg"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(root, "pkg", "x.go"),
				[]byte("package pkg\n\nimport _ \"embed\"\n\n"+tt.src+"var v string\n"), 0o644))

			run := func(args ...string) {
				cmd := exec.CommandContext(t.Context(), "git", args...)
				cmd.Dir = root
				out, err := cmd.CombinedOutput()
				require.NoError(t, err, string(out))
			}

			run("init", "-q")
			run("add", "-f", "--intent-to-add", "--", "pkg/x.go")

			if len(tt.track) > 0 {
				run(append([]string{"add", "-f", "--"}, tt.track...)...)
			}

			tracked := gitTrackedSet(t, root)
			require.NotNil(t, tracked)

			got := embedViolations(t, root, tracked, tt.allow)
			require.Len(t, got, len(tt.want), "%v", got)

			for i, w := range tt.want {
				assert.Contains(t, got[i], w)
			}
		})
	}
}
