package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
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

	return buildPackageIndexFromFiles(files, fset, dir), nil
}

// buildPackageIndexFromFiles is the testable core of buildPackageIndex --
// fixtures in reqfielddiff_test.go call this directly with parser.ParseFile
// output built from in-memory source, no services/ directory required (dir
// is passed as "" in that case, which simply yields no in-repo
// subpackages -- see collectInRepoSubPackages).
func buildPackageIndexFromFiles(files []*ast.File, fset *token.FileSet, dir string) *packageIndex {
	structs := collectStructTypes(files, fset)
	methods, funcs := collectFuncs(files)
	consts := collectPackageStringConsts(files)
	wrappers := collectLocalWrapOpWrappers(files)
	funcTypeNames := collectLocalFuncTypeNames(files)
	namedMapTypes := collectNamedDispatchMapTypes(files, funcTypeNames)

	ctx := handlerResolveCtx{
		fset:                  fset,
		structs:               structs,
		methods:               methods,
		funcs:                 funcs,
		wrapOpWrappers:        wrappers,
		genericDecodeWrappers: collectGenericDecodeWrapperFuncs(files),
		decodeDstWrappers:     collectLocalDecodeDstWrappers(files),
		queryAccessorWrappers: collectQueryAccessorWrappers(files),
		subPackages:           buildSubPackageIndexes(files, dir),
		pkgConsts:             consts,
	}

	return &packageIndex{ctx: ctx, dispatch: collectDispatchEntries(files, consts, funcTypeNames, namedMapTypes)}
}

// buildSubPackageIndexes parses every in-repo subpackage dir imports (see
// collectInRepoSubPackages) and builds the same structural index for each
// that buildPackageIndexFromFiles builds for the service package itself --
// dynamodb/models is the confirmed instance (gopherstack-99nj/xhu2t slice
// 1): its handleOp[...] generic dispatch wrapper decodes into a WireIn
// type resolved from models.ToSDK<Op>Input's own parameter type, which
// lives in this subpackage, not the service package being scanned. A
// subpackage that fails to parse is skipped rather than failing the whole
// scan -- the same "loud warning, not a hard failure" posture this tool
// takes toward every other resolution gap.
func buildSubPackageIndexes(files []*ast.File, dir string) map[string]subPackageIndex {
	out := map[string]subPackageIndex{}

	if dir == "" {
		return out
	}

	for alias, subDir := range collectInRepoSubPackages(files, dir) {
		subFiles, subFset, err := parseDirFiles(subDir)
		if err != nil {
			continue
		}

		_, subFuncs := collectFuncs(subFiles)

		out[alias] = subPackageIndex{
			structs: collectStructTypes(subFiles, subFset),
			funcs:   subFuncs,
		}
	}

	return out
}

// collectInRepoSubPackages finds every import in files whose path is
// rooted at "/services/<dir's own basename>/<one more segment>" -- i.e. a
// direct subpackage of the service currently being scanned, never an
// arbitrary unrelated package. Scoping the match to THIS service's own
// path (rather than, say, any import whose last segment happens to match
// some subdirectory name) is what keeps this safe: a random import
// wouldn't also need to be textually rooted under this exact service's own
// import path to be mistaken for one of its subpackages.
func collectInRepoSubPackages(files []*ast.File, dir string) map[string]string {
	out := map[string]string{}
	marker := "/services/" + filepath.Base(dir) + "/"

	for _, f := range files {
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}

			_, sub, found := strings.Cut(path, marker)
			if !found || sub == "" || strings.Contains(sub, "/") {
				continue
			}

			local := sub
			if imp.Name != nil {
				local = imp.Name.Name
			}

			if _, exists := out[local]; exists {
				continue
			}

			out[local] = filepath.Join(dir, sub)
		}
	}

	return out
}

// resolveOps resolves every op in ops against this package. Takes the full
// sdkOp (not just its name) because form-read matching needs each
// operation's own SDK field names to scope its candidate key set -- see
// formreads.go.
func (p *packageIndex) resolveOps(ops []sdkOp) map[string]opResolution {
	out := make(map[string]opResolution, len(ops))
	for _, op := range ops {
		out[op.Name] = resolveOp(op, p.dispatch, p.ctx)
	}

	return out
}
