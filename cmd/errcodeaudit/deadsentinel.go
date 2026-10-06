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

const reasonSetupSentinel = "sentinel is raised only from Init/New* setup functions, never from a request handler"

func isSetupFunc(name string) bool {
	return name == "Init" || strings.HasPrefix(name, "New") || strings.HasPrefix(name, "init")
}

// pkgSetupOnlySentinels returns sentinel candidates referenced, besides
// their declaration, only inside Init/New*/init* functions.
func pkgSetupOnlySentinels(files []*ast.File, cands []candidate) map[int]string {
	decls := collectSentinelDecls(files)
	total := map[string]int{}
	setup := map[string]int{}

	for _, f := range files {
		for _, d := range f.Decls {
			fd, isFn := d.(*ast.FuncDecl)
			inSetup := isFn && isSetupFunc(fd.Name.Name)

			ast.Inspect(d, func(n ast.Node) bool {
				if id, isID := n.(*ast.Ident); isID {
					total[id.Name]++

					if inSetup {
						setup[id.Name]++
					}
				}

				return true
			})
		}
	}

	out := map[int]string{}

	for i, c := range cands {
		if c.Mechanism != mechAwserrNew && c.Mechanism != mechStdlibErr {
			continue
		}

		if name, ok := decls[c.pos]; ok && setup[name] > 0 && total[name]-setup[name] == 1 {
			out[i] = name
		}
	}

	return out
}

func applyDeadSentinelDemotions(files []*ast.File, dir, repoRoot string, cands []candidate) {
	reasons := map[int]string{}
	names := map[int]string{}

	for i, name := range pkgDeadSentinels(files, cands) {
		reasons[i], names[i] = reasonDeadSentinel, name
	}

	for i, name := range pkgSetupOnlySentinels(files, cands) {
		if _, already := reasons[i]; !already {
			reasons[i], names[i] = reasonSetupSentinel, name
		}
	}

	if len(reasons) == 0 {
		return
	}

	pkg := filepath.Base(dir)
	wanted := map[string]bool{}

	for _, name := range names {
		wanted[pkg+"."+name] = true
	}

	external := referencedElsewhere(repoRoot, dir, wanted)

	for i, name := range names {
		if !external[pkg+"."+name] && cands[i].DemoteReason == "" {
			cands[i].DemoteReason = reasons[i]
		}
	}
}
