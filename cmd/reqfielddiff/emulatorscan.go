package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// packageIndex is the structural result of scanning one services/<dir>'s
// non-test .go files: every locally-declared struct type, function and
// method, and every recognised dispatch-table entry -- everything resolveOp
// needs to answer "what does the emulator declare for operation X".
type packageIndex struct {
	ctx      handlerResolveCtx
	dispatch map[string]ast.Expr
}

func parseDirFiles(dir string) ([]*ast.File, *token.FileSet, error) {
	fset := token.NewFileSet()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}

	var files []*ast.File

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}

		f, perr := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, 0)
		if perr != nil {
			return nil, nil, perr
		}

		files = append(files, f)
	}

	return files, fset, nil
}

func buildPackageIndex(dir string) (*packageIndex, error) {
	files, fset, err := parseDirFiles(dir)
	if err != nil {
		return nil, err
	}

	return buildPackageIndexFromFiles(files, fset), nil
}

// buildPackageIndexFromFiles is the testable core of buildPackageIndex --
// fixtures in reqfielddiff_test.go call this directly with parser.ParseFile
// output built from in-memory source, no services/ directory required.
func buildPackageIndexFromFiles(files []*ast.File, fset *token.FileSet) *packageIndex {
	structs := collectStructTypes(files, fset)
	methods, funcs := collectFuncs(files)
	consts := collectPackageStringConsts(files)
	wrappers := collectLocalWrapOpWrappers(files)
	funcTypeNames := collectLocalFuncTypeNames(files)

	ctx := handlerResolveCtx{
		fset: fset, structs: structs, methods: methods, funcs: funcs, wrapOpWrappers: wrappers,
	}

	return &packageIndex{ctx: ctx, dispatch: collectDispatchEntries(files, consts, funcTypeNames)}
}

// resolveOps resolves every op in opNames against this package.
func (p *packageIndex) resolveOps(opNames []string) map[string]opResolution {
	out := make(map[string]opResolution, len(opNames))
	for _, op := range opNames {
		out[op] = resolveOp(op, p.dispatch, p.ctx)
	}

	return out
}
