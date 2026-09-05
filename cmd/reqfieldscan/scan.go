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
	Structs        map[string]structDef
	Coverage       map[coverageKey]coverageInfo
	Dispatch       []dispatchEntry
	Literal        []literalSite
	StaticOps      []string
	UsesJSONOpFunc bool
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
	wrapOpWrappers := collectLocalWrapOpWrappers(files)

	ctx := handlerResolveCtx{
		fset:           fset,
		structs:        structs,
		methods:        methods,
		funcs:          funcs,
		wrapOpWrappers: wrapOpWrappers,
	}

	wrapOpFuncs := collectWrapOpFuncNames(files, ctx)
	literal := collectLiteralSites(files, fset, structs)
	tableEntries := collectDispatchTableEntries(files, pkgConsts)

	denom := collectStaticOpList(files, pkgConsts)
	if len(denom) == 0 {
		denom = dispatchTableOpNames(tableEntries)
	}

	dispatch := resolveDispatchTable(denom, tableEntries, wrapOpFuncs, literal, ctx)

	return &packageScan{
		Structs:        structs,
		Coverage:       collectFieldCoverage(files, fset, structs),
		Dispatch:       dispatch,
		Literal:        literal,
		StaticOps:      denom,
		UsesJSONOpFunc: packageMentionsJSONOpFunc(files),
	}
}

// packageMentionsJSONOpFunc reports whether the package refers to
// service.JSONOpFunc anywhere at all, regardless of shape. It gates the
// coverage guard in report.go: a package that never mentions this type
// uses some other dispatch mechanism entirely (REST routing, CBOR, or a
// Query/XML-protocol service's own action-function type) and a zero or low
// dispatch-table resolution there is expected, not suspicious -- this
// scan's documented ground truth was never meant to cover it. A package
// that DOES mention it but still resolves low is exactly the false-clean-
// verdict failure mode gopherstack-43o8 was filed for.
func packageMentionsJSONOpFunc(files []*ast.File) bool {
	for _, f := range files {
		found := false

		ast.Inspect(f, func(n ast.Node) bool {
			if found {
				return false
			}

			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == jsonOpFuncTypeName {
				found = true

				return false
			}

			return true
		})

		if found {
			return true
		}
	}

	return false
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

// aliasSpec is a `type X = Y` or `type X Y` TypeSpec whose Type is a bare
// identifier rather than its own struct literal -- glue's
// `type updateJobFromSourceControlInput = jobSourceControlInput`
// (handler_jobs.go:386) reaches its request struct only through this
// indirection.
type aliasSpec struct {
	Name   string
	Target string
}

func collectStructTypes(files []*ast.File, fset *token.FileSet) map[string]structDef {
	out := map[string]structDef{}

	var aliases []aliasSpec

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}

			for _, spec := range gd.Specs {
				addTypeSpec(spec, fset, out, &aliases)
			}
		}
	}

	resolveStructAliases(aliases, out)
	collectAnonReqStructs(files, fset, out)

	return out
}

// collectAnonReqStructs registers a request struct declared inline as
// `var req struct{...}` rather than as a named local type -- opsworks's
// shape (e.g. handler_instances.go's handleAssignInstance and 73 other
// handlers in that package): every handler there IS a service.JSONOpFunc
// directly, with no service.WrapOp call anywhere, decoding its own body
// into an anonymous struct literal that otherwise never gets a name for
// this scan's struct collector -- or the literal-decode-site linker
// (collectLiteralSites) that already exists for exactly this
// outside-WrapOp shape -- to key coverage by. Keyed by file:line so
// recordVarDeclBindings can recompute the identical key later, when it
// binds the declared identifier to it.
func collectAnonReqStructs(files []*ast.File, fset *token.FileSet, out map[string]structDef) {
	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			ast.Inspect(fd.Body, func(n ast.Node) bool {
				vs, st, isAnon := anonStructVarSpec(n)
				if !isAnon {
					return true
				}

				name := anonStructName(fset, vs)
				pos := fset.Position(vs.Pos())
				out[name] = structDef{Name: name, File: pos.Filename, Line: pos.Line, Fields: collectFields(st, fset)}

				return true
			})
		}
	}
}

func anonStructVarSpec(n ast.Node) (*ast.ValueSpec, *ast.StructType, bool) {
	ds, ok := n.(*ast.DeclStmt)
	if !ok {
		return nil, nil, false
	}

	gd, ok := ds.Decl.(*ast.GenDecl)
	if !ok || gd.Tok != token.VAR || len(gd.Specs) != 1 {
		return nil, nil, false
	}

	vs, ok := gd.Specs[0].(*ast.ValueSpec)
	if !ok || len(vs.Names) != 1 {
		return nil, nil, false
	}

	st, ok := vs.Type.(*ast.StructType)
	if !ok {
		return nil, nil, false
	}

	return vs, st, true
}

// anonStructName is purely a function of source position, so it can be
// recomputed identically at bind time (recordVarDeclBindings) without any
// shared counter or call-order dependency between the two passes.
func anonStructName(fset *token.FileSet, vs *ast.ValueSpec) string {
	pos := fset.Position(vs.Pos())

	return "anon@" + filepath.Base(pos.Filename) + ":" + strconv.Itoa(pos.Line)
}

func addTypeSpec(spec ast.Spec, fset *token.FileSet, out map[string]structDef, aliases *[]aliasSpec) {
	ts, ok := spec.(*ast.TypeSpec)
	if !ok {
		return
	}

	switch t := ts.Type.(type) {
	case *ast.StructType:
		pos := fset.Position(ts.Pos())
		out[ts.Name.Name] = structDef{
			Name:   ts.Name.Name,
			File:   pos.Filename,
			Line:   pos.Line,
			Fields: collectFields(t, fset),
		}
	case *ast.Ident:
		*aliases = append(*aliases, aliasSpec{Name: ts.Name.Name, Target: t.Name})
	}
}

// resolveStructAliases registers every alias whose target is a known
// request struct (transitively, in case one alias targets another) so a
// WrapOp handler's *aliasName parameter resolves like any other local
// struct type. An alias whose target is never a struct -- e.g. glue's own
// `type iterableFormItemsMap = map[...]...` -- is silently left
// unregistered, same as any other non-struct type.
func resolveStructAliases(aliases []aliasSpec, out map[string]structDef) {
	for range aliases {
		changed := false

		for _, a := range aliases {
			if _, known := out[a.Name]; known {
				continue
			}

			if def, ok := out[a.Target]; ok {
				out[a.Name] = structDef{Name: a.Name, File: def.File, Line: def.Line, Fields: def.Fields}
				changed = true
			}
		}

		if !changed {
			break
		}
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
