package main

import (
	"go/ast"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var identTokenRe = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)

// funcRefs maps each FuncDecl name to the identifiers its body mentions,
// and collects names mentioned outside any function body (always reached).
func funcRefs(files []*ast.File) (map[string]map[string]bool, map[string]bool) {
	inFunc := map[string]map[string]bool{}
	outside := map[string]bool{}

	for _, f := range files {
		for _, d := range f.Decls {
			fd, isFn := d.(*ast.FuncDecl)
			if !isFn {
				markOutsideRefs(d, outside)

				continue
			}

			set := inFunc[fd.Name.Name]
			if set == nil {
				set = map[string]bool{}
				inFunc[fd.Name.Name] = set
			}

			ast.Inspect(fd, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id != fd.Name && id.Name != fd.Name.Name {
					set[id.Name] = true
				}

				return true
			})
		}
	}

	return inFunc, outside
}

func markIdents(n ast.Node, set map[string]bool) {
	if n == nil {
		return
	}

	ast.Inspect(n, func(c ast.Node) bool {
		if id, ok := c.(*ast.Ident); ok {
			set[id.Name] = true
		}

		return true
	})
}

// markOutsideRefs records identifiers in a non-function declaration, leaving
// out var/const names and interface method names, which declare not reference.
func markOutsideRefs(d ast.Decl, outside map[string]bool) {
	ast.Inspect(d, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ValueSpec:
			for _, v := range x.Values {
				markIdents(v, outside)
			}

			if x.Type != nil {
				markIdents(x.Type, outside)
			}

			return false
		case *ast.InterfaceType:
			for _, m := range x.Methods.List {
				markIdents(m.Type, outside)
			}

			return false
		case *ast.Ident:
			outside[x.Name] = true
		}

		return true
	})
}

// reachedFuncs closes roots over the in-package reference graph.
func reachedFuncs(inFunc map[string]map[string]bool, roots map[string]bool) map[string]bool {
	reached := map[string]bool{}
	queue := []string{}

	for name := range roots {
		if _, isFn := inFunc[name]; isFn {
			reached[name] = true
			queue = append(queue, name)
		}
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		for ref := range inFunc[cur] {
			if _, isFn := inFunc[ref]; isFn && !reached[ref] {
				reached[ref] = true
				queue = append(queue, ref)
			}
		}
	}

	return reached
}

var (
	identIndexMu    sync.Mutex                                //nolint:gochecknoglobals // guards the cache
	identIndexCache = map[string]map[string]map[string]bool{} //nolint:gochecknoglobals // per-root scan cache
)

// identIndex maps each identifier token in non-test Go files under root to
// the directories mentioning it; built once per root.
func identIndex(root string) map[string]map[string]bool {
	identIndexMu.Lock()
	defer identIndexMu.Unlock()

	if idx, ok := identIndexCache[root]; ok {
		return idx
	}

	idx := map[string]map[string]bool{}
	fsys := os.DirFS(root)

	_ = fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fs.SkipDir
		}

		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "node_modules" {
				return fs.SkipDir
			}

			return nil
		}

		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			indexFile(fsys, path, filepath.Join(root, filepath.Dir(path)), idx)
		}

		return nil
	})

	identIndexCache[root] = idx

	return idx
}

func indexFile(fsys fs.FS, path, dir string, idx map[string]map[string]bool) {
	data, err := fs.ReadFile(fsys, path)
	if err != nil {
		return
	}

	for _, tok := range identTokenRe.FindAll(data, -1) {
		dirs := idx[string(tok)]
		if dirs == nil {
			dirs = map[string]bool{}
			idx[string(tok)] = dirs
		}

		dirs[dir] = true
	}
}

// externalIdents returns which of names appear as an identifier token in a
// non-test Go file under root outside skipDir.
func externalIdents(root, skipDir string, names map[string]bool) map[string]bool {
	idx := identIndex(root)
	found := map[string]bool{}

	for n := range names {
		for dir := range idx[n] {
			if dir != skipDir {
				found[n] = true

				break
			}
		}
	}

	return found
}

func isEntryName(name string) bool {
	return name == "main" || name == "init" || name == "TestMain"
}

// sentinelRaisers maps a sentinel var name to the functions that reference it.
func sentinelRaisers(files []*ast.File, sentinels map[string]bool) map[string]map[string]bool {
	inFunc, _ := funcRefs(files)
	raisers := map[string]map[string]bool{}

	for fn, refs := range inFunc {
		for s := range sentinels {
			if refs[s] {
				if raisers[s] == nil {
					raisers[s] = map[string]bool{}
				}

				raisers[s][fn] = true
			}
		}
	}

	return raisers
}

// reachedWithRoots computes the functions reached from package-level
// references, entry points and identifiers other packages use.
func reachedWithRoots(files []*ast.File, dir, repoRoot string) (map[string]bool, map[string]bool) {
	inFunc, outside := funcRefs(files)
	roots := map[string]bool{}
	exported := map[string]bool{}

	for n := range outside {
		roots[n] = true
	}

	for fn := range inFunc {
		if isEntryName(fn) {
			roots[fn] = true
		}

		if ast.IsExported(fn) {
			exported[fn] = true
		}
	}

	for n := range externalIdents(repoRoot, dir, exported) {
		roots[n] = true
	}

	return reachedFuncs(inFunc, roots), outside
}

func sentinelNames(files []*ast.File, cands []candidate) (map[int]string, map[string]bool) {
	decls := collectSentinelDecls(files)
	names := map[int]string{}
	sentinels := map[string]bool{}

	for i, c := range cands {
		if c.Mechanism != mechAwserrNew && c.Mechanism != mechStdlibErr {
			continue
		}

		if n, ok := decls[c.pos]; ok {
			names[i], sentinels[n] = n, true
		}
	}

	return names, sentinels
}

func allUnreached(raisers, reached map[string]bool) bool {
	for fn := range raisers {
		if reached[fn] {
			return false
		}
	}

	return len(raisers) > 0
}

// pkgUnroutedSentinels returns sentinels whose every raiser is unreachable from in-package
// callers, package-level references and outside-package identifier uses.
func pkgUnroutedSentinels(files []*ast.File, dir, repoRoot string, cands []candidate) map[int]string {
	names, sentinels := sentinelNames(files, cands)
	if len(sentinels) == 0 {
		return nil
	}

	raisers := sentinelRaisers(files, sentinels)
	usedElsewhere := externalIdents(repoRoot, dir, sentinels)
	reached, outside := reachedWithRoots(files, dir, repoRoot)
	out := map[int]string{}

	for i, n := range names {
		if !outside[n] && !usedElsewhere[n] && allUnreached(raisers[n], reached) {
			out[i] = n
		}
	}

	return out
}

func applyUnroutedSentinels(files []*ast.File, dir, repoRoot string, cands []candidate) {
	for i := range pkgUnroutedSentinels(files, dir, repoRoot, cands) {
		if cands[i].DemoteReason == "" {
			cands[i].Kind, cands[i].DemoteReason = kindUnroutedDead, reasonUnrouted
		}
	}
}
