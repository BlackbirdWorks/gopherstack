package guards_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp/syntax"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// maxEagerRepeatCost bounds ranges-in-class x repeat-count for a package-level
// regexp.MustCompile. Unicode classes (\p{L}) inside {1,256} make regexp's
// one-pass compiler allocate megabytes at process start; such patterns belong
// behind sync.OnceValue.
const maxEagerRepeatCost = 20000

func repeatCost(re *syntax.Regexp) int {
	worst := 0

	if re.Op == syntax.OpRepeat && re.Max > 1 {
		worst = classRanges(re) * re.Max
	}

	for _, sub := range re.Sub {
		worst = max(worst, repeatCost(sub))
	}

	return worst
}

func classRanges(re *syntax.Regexp) int {
	n := 0
	if re.Op == syntax.OpCharClass {
		n = len(re.Rune) / 2
	}

	for _, sub := range re.Sub {
		n = max(n, classRanges(sub))
	}

	return n
}

func regexpProgramSize(t *testing.T, pattern string) int {
	t.Helper()

	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return 0
	}

	return repeatCost(re)
}

func eagerRegexpOffenders(t *testing.T, file string) []string {
	t.Helper()

	f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	require.NoError(t, err)

	var offenders []string

	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}

		for _, spec := range gen.Specs {
			for _, val := range spec.(*ast.ValueSpec).Values {
				call, isCall := val.(*ast.CallExpr)
				if !isCall || len(call.Args) != 1 {
					continue
				}

				sel, isSel := call.Fun.(*ast.SelectorExpr)
				if !isSel || sel.Sel.Name != "MustCompile" {
					continue
				}

				lit, isLit := call.Args[0].(*ast.BasicLit)
				if !isLit || lit.Kind != token.STRING {
					continue
				}

				pattern, uerr := strconv.Unquote(lit.Value)
				if uerr != nil {
					continue
				}

				if n := regexpProgramSize(t, pattern); n > maxEagerRepeatCost {
					offenders = append(offenders, file+": "+strconv.Itoa(n)+" repeat cost: "+pattern)
				}
			}
		}
	}

	return offenders
}

func TestNoHeavyPackageLevelRegexp(t *testing.T) {
	t.Parallel()

	var offenders []string

	for _, root := range []string{"services", "pkgs", "internal", "dashboard"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() && (d.Name() == "node_modules" || d.Name() == "testdata") {
				return filepath.SkipDir
			}

			if !d.IsDir() && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
				offenders = append(offenders, eagerRegexpOffenders(t, path)...)
			}

			return nil
		})
		require.NoError(t, err)
	}

	assert.Empty(t, offenders, "wrap in sync.OnceValue: package-level regexps this large allocate MBs at startup")
}
