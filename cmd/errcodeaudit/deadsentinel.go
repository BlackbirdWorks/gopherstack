package main

import (
	"bytes"
	"go/ast"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const reasonDeadSentinel = "sentinel is declared but never referenced by any non-test source, so it is never raised"

// pkgDeadSentinels returns sentinel candidates whose variable has no
// reference in the package's non-test files other than its declaration.
func pkgDeadSentinels(files []*ast.File, cands []candidate) map[int]string {
	decls := collectSentinelDecls(files)
	refs := identRefs(files)
	dead := map[int]string{}

	for i, c := range cands {
		if c.Mechanism != mechAwserrNew && c.Mechanism != mechStdlibErr {
			continue
		}

		decl, ok := decls[c.pos]
		if !ok {
			continue
		}

		if len(refs[decl]) <= 1 {
			dead[i] = decl
		}
	}

	return dead
}

// referencedElsewhere reports which "pkg.Name" selectors appear in any
// non-test Go file under root outside skipDir.
func referencedElsewhere(root, skipDir string, wanted map[string]bool) map[string]bool {
	found := map[string]bool{}
	fsys := os.DirFS(root)

	skip, relErr := filepath.Rel(root, skipDir)
	if relErr != nil {
		skip = skipDir
	}

	_ = fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fs.SkipDir
		}

		if d.IsDir() {
			if path == filepath.ToSlash(skip) || d.Name() == ".git" || d.Name() == "node_modules" {
				return fs.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		markSelectors(fsys, path, wanted, found)

		return nil
	})

	return found
}

func markSelectors(fsys fs.FS, path string, wanted, found map[string]bool) {
	data, err := fs.ReadFile(fsys, path)
	if err != nil {
		return
	}

	for sel := range wanted {
		if bytes.Contains(data, []byte(sel)) {
			found[sel] = true
		}
	}
}

func applyDeadSentinelDemotions(files []*ast.File, dir, repoRoot string, cands []candidate) {
	dead := pkgDeadSentinels(files, cands)
	if len(dead) == 0 {
		return
	}

	pkg := filepath.Base(dir)
	wanted := map[string]bool{}

	for _, name := range dead {
		wanted[pkg+"."+name] = true
	}

	external := referencedElsewhere(repoRoot, dir, wanted)

	for i, name := range dead {
		if !external[pkg+"."+name] && cands[i].DemoteReason == "" {
			cands[i].DemoteReason = reasonDeadSentinel
		}
	}
}
