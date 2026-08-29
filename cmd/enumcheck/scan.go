package main

import (
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	kindLiteral = "literal-value"
	kindReuse   = "cross-enum-reuse"
)

// finding is one enumcheck result. CONFIDENT findings (kindLiteral) show a
// statically-resolved value that is provably not a member of the enum its
// wire key deserializes into. NEEDS REVIEW findings (kindReuse) show the
// same dynamic value source feeding two wire keys whose real SDK enums have
// different declared member sets -- structurally suspicious, but the actual
// runtime values are never inspected, so this is never promoted to
// confident. See scan.go's doc comments for why.
type finding struct {
	File      string `json:"file"`
	Kind      string `json:"kind"`
	Key       string `json:"key"`
	Enum      string `json:"enum"`
	Value     string `json:"value,omitempty"`
	OtherKey  string `json:"otherKey,omitempty"`
	OtherEnum string `json:"otherEnum,omitempty"`
	Line      int    `json:"line"`
	OtherLine int    `json:"otherLine,omitempty"`
	Confident bool   `json:"confident"`
}

// scanPackage checks every non-test .go file directly in dir (no recursion
// into subpackages -- see the package doc comment for why) against reg and
// wireKeys, returning every finding sorted by file:line.
func scanPackage(dir string, reg *enumRegistry, wireKeys map[string]wireKeyFact, repoRoot string) ([]finding, error) {
	fset := token.NewFileSet()

	files, err := parseDirFiles(fset, dir)
	if err != nil {
		return nil, err
	}

	pkgConsts := packageStringConsts(files)

	var out []finding

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			localConsts := localStringConsts(fd)
			out = append(out, checkLiteralsInFunc(fd, fset, reg, wireKeys, localConsts, pkgConsts, repoRoot)...)
		}
	}

	out = append(out, checkCrossEnumReuse(files, fset, reg, wireKeys, pkgConsts, repoRoot)...)

	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}

		return out[i].Line < out[j].Line
	})

	return out, nil
}

func parseDirFiles(fset *token.FileSet, dir string) ([]*ast.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []*ast.File

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}

		f, perr := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, 0)
		if perr != nil {
			return nil, perr
		}

		files = append(files, f)
	}

	return files, nil
}

func relPath(repoRoot, path string) string {
	rel, err := filepath.Rel(repoRoot, path)
	if err != nil {
		return path
	}

	return rel
}

// isStringAnyMapType reports whether expr is an explicit map[string]any (or
// map[string]interface{}) type expression. Only explicitly-typed composite
// literals are matched -- an elided inner literal in []map[string]any{{...}}
// has a nil Type and is out of scope, a disclosed approximation.
func isStringAnyMapType(expr ast.Expr) bool {
	mt, ok := expr.(*ast.MapType)
	if !ok {
		return false
	}

	keyIdent, ok := mt.Key.(*ast.Ident)
	if !ok || keyIdent.Name != "string" {
		return false
	}

	switch v := mt.Value.(type) {
	case *ast.InterfaceType:
		return v.Methods == nil || len(v.Methods.List) == 0
	case *ast.Ident:
		return v.Name == "any"
	default:
		return false
	}
}

// packageStringConsts collects every single-name, single-value, string
// literal top-level const across files.
func packageStringConsts(files []*ast.File) map[string]string {
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

// localStringConsts collects every `name := "literal"` binding in fd whose
// name is never assigned to again anywhere else in fd -- a single-hop alias
// resolution, not general dataflow.
func localStringConsts(fd *ast.FuncDecl) map[string]string {
	vals := map[string]string{}
	assignCount := map[string]int{}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != len(as.Rhs) {
			return true
		}

		for i, lhs := range as.Lhs {
			recordLocalAssign(lhs, as, i, vals, assignCount)
		}

		return true
	})

	for name, count := range assignCount {
		if count > 1 {
			delete(vals, name)
		}
	}

	return vals
}

func recordLocalAssign(lhs ast.Expr, as *ast.AssignStmt, i int, vals map[string]string, assignCount map[string]int) {
	id, ok := lhs.(*ast.Ident)
	if !ok || id.Name == "_" {
		return
	}

	assignCount[id.Name]++

	if as.Tok != token.DEFINE {
		return
	}

	lit, ok := as.Rhs[i].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return
	}

	if v, err := strconv.Unquote(lit.Value); err == nil {
		vals[id.Name] = v
	}
}

// resolveConstString statically resolves expr to a concrete string, or
// reports false when it depends on a runtime value this scan can't pin down
// (a decoded request field, an unresolvable variable, ...) -- that is the
// common, correct case and produces no finding, not an error.
func resolveConstString(expr ast.Expr, localConsts, pkgConsts map[string]string, reg *enumRegistry) (string, bool) {
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return resolveConstString(e.X, localConsts, pkgConsts, reg)
	case *ast.BasicLit:
		return resolveBasicLitString(e)
	case *ast.Ident:
		return resolveIdentString(e, localConsts, pkgConsts)
	case *ast.SelectorExpr:
		return resolveSDKEnumSelector(e, reg)
	case *ast.CallExpr:
		return resolveEnumConversionCall(e, localConsts, pkgConsts, reg)
	default:
		return "", false
	}
}

func resolveBasicLitString(lit *ast.BasicLit) (string, bool) {
	if lit.Kind != token.STRING {
		return "", false
	}

	v, err := strconv.Unquote(lit.Value)

	return v, err == nil
}

func resolveIdentString(id *ast.Ident, localConsts, pkgConsts map[string]string) (string, bool) {
	if v, ok := localConsts[id.Name]; ok {
		return v, true
	}

	v, ok := pkgConsts[id.Name]

	return v, ok
}

// resolveSDKEnumSelector resolves a `types.SomeEnumMember` selector to its
// real declared value, matching this repo's universal SDK import
// convention of an unaliased "types" package name.
func resolveSDKEnumSelector(e *ast.SelectorExpr, reg *enumRegistry) (string, bool) {
	pkgIdent, ok := e.X.(*ast.Ident)
	if !ok || pkgIdent.Name != sdkTypesPkgName {
		return "", false
	}

	c, ok := reg.constByIdent[e.Sel.Name]

	return c.value, ok
}

func resolveEnumConversionCall(
	e *ast.CallExpr, localConsts, pkgConsts map[string]string, reg *enumRegistry,
) (string, bool) {
	sel, ok := e.Fun.(*ast.SelectorExpr)
	if !ok || len(e.Args) != 1 {
		return "", false
	}

	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok || pkgIdent.Name != sdkTypesPkgName {
		return "", false
	}

	return resolveConstString(e.Args[0], localConsts, pkgConsts, reg)
}

// checkLiteralsInFunc is CONFIDENT check A: a map[string]any entry whose key
// resolves to a wire key with known enum candidates, and whose value
// statically resolves to a string that is not a member of any candidate.
func checkLiteralsInFunc(
	fd *ast.FuncDecl, fset *token.FileSet, reg *enumRegistry,
	wireKeys map[string]wireKeyFact, localConsts, pkgConsts map[string]string, repoRoot string,
) []finding {
	var out []finding

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok || cl.Type == nil || !isStringAnyMapType(cl.Type) {
			return true
		}

		for _, elt := range cl.Elts {
			if f, found := checkLiteralElt(elt, fset, reg, wireKeys, localConsts, pkgConsts, repoRoot); found {
				out = append(out, f)
			}
		}

		return true
	})

	return out
}

func checkLiteralElt(
	elt ast.Expr, fset *token.FileSet, reg *enumRegistry,
	wireKeys map[string]wireKeyFact, localConsts, pkgConsts map[string]string, repoRoot string,
) (finding, bool) {
	kv, ok := elt.(*ast.KeyValueExpr)
	if !ok {
		return finding{}, false
	}

	key, ok := resolveConstString(kv.Key, localConsts, pkgConsts, reg)
	if !ok {
		return finding{}, false
	}

	fact, known := wireKeys[key]
	// Require an UNAMBIGUOUS single enum candidate, and reject a Polymorphic
	// key (deserializes as a plain, non-enum string in at least one other
	// struct this scan can't tell apart from the enum-typed sense). Both
	// restrictions came from hand-checking this scan's own early findings
	// against the pinned SDK: a key with 2+ real enum candidates (apigateway
	// export.go's OpenAPI-document "type", unrelated to any of API
	// Gateway's own DocumentationPartType/AuthorizerType/IntegrationType;
	// securityhub's "ErrorCode", really an int32 field on the struct this
	// code actually builds) was wrong every single time checked by hand --
	// see the package doc comment.
	if !known || len(fact.Enums) != 1 || fact.Polymorphic {
		return finding{}, false
	}

	val, ok := resolveConstString(kv.Value, localConsts, pkgConsts, reg)
	// "" is this repo's overwhelming placeholder for "no backend/not set" (a
	// degenerate fallback response, not a copy-pasted wrong enum value --
	// confirmed live at services/cloudwatchlogs/handler_integrations.go:181,
	// a nil-backend fallback), never a real enum-typed field's intended
	// value -- excluded to avoid flagging every such placeholder as a bug.
	if !ok || val == "" || reg.isMemberOfAny(val, fact.Enums) {
		return finding{}, false
	}

	pos := fset.Position(kv.Value.Pos())

	return finding{
		File:      relPath(repoRoot, pos.Filename),
		Line:      pos.Line,
		Kind:      kindLiteral,
		Key:       key,
		Enum:      strings.Join(fact.Enums, "|"),
		Value:     val,
		Confident: true,
	}, true
}

func exprText(fset *token.FileSet, e ast.Expr) string {
	var sb strings.Builder
	if err := format.Node(&sb, fset, e); err != nil {
		return ""
	}

	return sb.String()
}
