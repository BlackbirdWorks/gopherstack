package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

// fieldDef is one exported field of a request struct, as declared.
type fieldDef struct {
	Name string
	Tag  string
	File string
	Line int
}

// structDef is one locally-declared struct type this scan can resolve
// request fields for.
type structDef struct {
	Name   string
	File   string
	Fields []fieldDef
	Line   int
}

// Anchor values for dispatchEntry.
const (
	anchorWrapOp     = "wrapop"
	anchorLiteral    = "literal"
	anchorUnresolved = "unresolved"
)

// dispatchEntry is one operation in a service's dispatch table.
type dispatchEntry struct {
	Op      string `json:"op"`
	File    string `json:"file"`
	Anchor  string `json:"anchor"`
	ReqType string `json:"reqType"`
	Reason  string `json:"reason"`
	Line    int    `json:"line"`
}

// literalSite is one `json.Unmarshal(body, &x)` call whose target x's type
// was resolved from its own declaration in the same function.
type literalSite struct {
	FuncName string
	ReqType  string
	File     string
	Line     int
}

// packageScan is the full structural result of scanning one service
// directory's non-test .go files.
type packageScan struct {
	Structs   map[string]structDef
	Coverage  map[coverageKey]coverageInfo
	Dispatch  []dispatchEntry
	Literal   []literalSite
	StaticOps []string
}

func scanServiceDir(dir string) (*packageScan, error) {
	files, fset, err := parseDirFiles(dir)
	if err != nil {
		return nil, err
	}

	return scanFiles(files, fset), nil
}

func scanFiles(files []*ast.File, fset *token.FileSet) *packageScan {
	structs := collectStructTypes(files, fset)
	methods, funcs := collectFuncs(files)
	pkgConsts := collectPackageStringConsts(files)

	wrapOpFuncs := collectWrapOpFuncNames(files, fset, structs, methods, funcs)
	literal := collectLiteralSites(files, fset, structs)

	denom := collectStaticOpList(files, pkgConsts)
	if len(denom) == 0 {
		denom = collectDispatchMapKeys(files, pkgConsts)
	}

	dispatch := resolveDispatchTable(denom, wrapOpFuncs, literal)

	return &packageScan{
		Structs:   structs,
		Coverage:  collectFieldCoverage(files, fset, structs),
		Dispatch:  dispatch,
		Literal:   literal,
		StaticOps: denom,
	}
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

func collectStructTypes(files []*ast.File, fset *token.FileSet) map[string]structDef {
	out := map[string]structDef{}

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}

			for _, spec := range gd.Specs {
				addStructTypeSpec(spec, fset, out)
			}
		}
	}

	return out
}

func addStructTypeSpec(spec ast.Spec, fset *token.FileSet, out map[string]structDef) {
	ts, ok := spec.(*ast.TypeSpec)
	if !ok {
		return
	}

	st, ok := ts.Type.(*ast.StructType)
	if !ok {
		return
	}

	pos := fset.Position(ts.Pos())
	out[ts.Name.Name] = structDef{
		Name:   ts.Name.Name,
		File:   pos.Filename,
		Line:   pos.Line,
		Fields: collectFields(st, fset),
	}
}

// collectFields skips embedded (anonymous) fields -- no field identity to
// key coverage by without one -- and any field tagged `json:"-"`, a
// disclosed blind spot documented in the package doc.
func collectFields(st *ast.StructType, fset *token.FileSet) []fieldDef {
	var out []fieldDef

	if st.Fields == nil {
		return out
	}

	for _, f := range st.Fields.List {
		if len(f.Names) == 0 {
			continue
		}

		tag := jsonTagOf(f)
		if tag == "-" {
			continue
		}

		pos := fset.Position(f.Pos())

		for _, n := range f.Names {
			if n.Name == "_" {
				continue
			}

			out = append(out, fieldDef{Name: n.Name, Tag: tag, File: pos.Filename, Line: pos.Line})
		}
	}

	return out
}

func jsonTagOf(f *ast.Field) string {
	if f.Tag == nil {
		return ""
	}

	unquoted, err := strconv.Unquote(f.Tag.Value)
	if err != nil {
		return ""
	}

	tag, _, _ := strings.Cut(reflect.StructTag(unquoted).Get("json"), ",")

	return tag
}

func collectFuncs(files []*ast.File) (map[string][]*ast.FuncDecl, map[string]*ast.FuncDecl) {
	methods := map[string][]*ast.FuncDecl{}
	funcs := map[string]*ast.FuncDecl{}

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}

			if fd.Recv != nil {
				methods[fd.Name.Name] = append(methods[fd.Name.Name], fd)
			} else {
				funcs[fd.Name.Name] = fd
			}
		}
	}

	return methods, funcs
}

func collectPackageStringConsts(files []*ast.File) map[string]string {
	out := map[string]string{}

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}

			for _, spec := range gd.Specs {
				addStringValueSpec(spec, out)
			}
		}
	}

	return out
}

func addStringValueSpec(spec ast.Spec, out map[string]string) {
	vs, ok := spec.(*ast.ValueSpec)
	if !ok || len(vs.Names) != 1 || len(vs.Values) != 1 {
		return
	}

	lit, ok := vs.Values[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return
	}

	if v, err := strconv.Unquote(lit.Value); err == nil {
		out[vs.Names[0].Name] = v
	}
}
