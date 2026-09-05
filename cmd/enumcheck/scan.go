package main

import (
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	kindLiteral      = "literal-value"
	kindReuse        = "cross-enum-reuse"
	kindAmbiguousKey = "ambiguous-key"
	kindPhantomField = "phantom-field"
)

// finding is one enumcheck result. CONFIDENT findings (kindLiteral) show a
// statically-resolved value that is provably not a member of the enum its
// wire key deserializes into. NEEDS REVIEW findings come in three kinds:
// kindReuse shows the same dynamic value source feeding two wire keys whose
// real SDK enums have different declared member sets -- structurally
// suspicious, but the actual runtime values are never inspected, so this is
// never promoted to confident. kindAmbiguousKey shows a statically-resolved
// value under a wire key with 2+ real SDK enum candidates (or a Polymorphic
// one, also a plain non-enum string somewhere) that fails membership in at
// least one candidate -- real, but which candidate sense actually applies at
// this emission site is unknown, so this can never be confident either.
// kindPhantomField (gopherstack-7fps) shows a gopherstack response struct
// field whose real same-named SDK type has NO field under this wire key at
// all -- the enum a naive key-name match would apply belongs to some
// entirely unrelated real operation, so this is never a "wrong value" claim
// and never confident; Enum carries the struct type name (not an enum type)
// for this kind. See scan.go's and structresp.go's doc comments for why.
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
	structFields := collectStructFields(files)

	var out []finding

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			localConsts := localStringConsts(fd)
			maps.Copy(localConsts, localFieldConsts(fd, localConsts, pkgConsts, reg))

			out = append(out, checkLiteralsInFunc(fd, fset, reg, wireKeys, localConsts, pkgConsts, repoRoot)...)
			out = append(out, checkIndexAssignsInFunc(fd, fset, reg, wireKeys, localConsts, pkgConsts, repoRoot)...)
			out = append(
				out,
				checkStructResponsesInFunc(fd, fset, reg, wireKeys, localConsts, pkgConsts, structFields, repoRoot)...,
			)
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
// resolution, not general dataflow. Traversal stops at nested *ast.FuncLit
// boundaries: a closure's own local bindings are a distinct scope, and Go
// permits a closure-local `status := "INVALID"` to shadow an outer runtime
// parameter of the same name. Without this boundary, a nested binding would
// pollute the enclosing function's vals map and could resolve an outer
// map[string]any{"status": status} literal to the closure's constant
// instead of leaving the outer runtime value unresolved.
func localStringConsts(fd *ast.FuncDecl) map[string]string {
	vals := map[string]string{}
	assignCount := map[string]int{}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}

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

// localFieldConsts collects every single-assignment `structVar.Field = <expr>`
// binding in fd whose RHS statically resolves (via identConsts/pkgConsts/reg,
// the SAME single-hop resolution checkLiteralElt itself uses), keyed by
// "structVar.Field" -- identity is the (local variable, field name) pair,
// never the bare field name, so two different local structs that both happen
// to declare a "Status" field (gopherstack-3dzb's comprehend shape: this repo's
// dominant pattern is a domain struct field set once and marshalled later)
// never collide within one function. A field assigned more than once is
// dropped, same discipline as localStringConsts -- ambiguous dataflow
// resolves to nothing, never a guess.
func localFieldConsts(fd *ast.FuncDecl, identConsts, pkgConsts map[string]string, reg *enumRegistry) map[string]string {
	vals := map[string]string{}
	assignCount := map[string]int{}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}

		as, ok := n.(*ast.AssignStmt)
		if !ok || as.Tok != token.ASSIGN || len(as.Lhs) != len(as.Rhs) {
			return true
		}

		for i, lhs := range as.Lhs {
			recordFieldAssign(lhs, as.Rhs[i], identConsts, pkgConsts, reg, vals, assignCount)
		}

		return true
	})

	for key, count := range assignCount {
		if count > 1 {
			delete(vals, key)
		}
	}

	return vals
}

func recordFieldAssign(
	lhs, rhs ast.Expr, identConsts, pkgConsts map[string]string, reg *enumRegistry,
	vals map[string]string, assignCount map[string]int,
) {
	sel, ok := lhs.(*ast.SelectorExpr)
	if !ok {
		return
	}

	varIdent, ok := sel.X.(*ast.Ident)
	if !ok || varIdent.Name == sdkTypesPkgName {
		return
	}

	key := varIdent.Name + "." + sel.Sel.Name
	assignCount[key]++

	if v, resolved := resolveConstString(rhs, identConsts, pkgConsts, reg); resolved {
		vals[key] = v
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
		return resolveSelectorString(e, localConsts, reg)
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

// resolveSelectorString resolves a SelectorExpr as either a
// `types.SomeEnumMember` (resolveSDKEnumSelector) or, failing that, a
// `structVar.Field` read of a field this function's own single-hop
// localFieldConsts resolved earlier -- the struct-field blind spot
// gopherstack-3dzb exists for.
func resolveSelectorString(e *ast.SelectorExpr, localConsts map[string]string, reg *enumRegistry) (string, bool) {
	if v, ok := resolveSDKEnumSelector(e, reg); ok {
		return v, true
	}

	varIdent, ok := e.X.(*ast.Ident)
	if !ok {
		return "", false
	}

	v, ok := localConsts[varIdent.Name+"."+e.Sel.Name]

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

	return evalKeyValue(key, kv.Value, fset, reg, wireKeys, localConsts, pkgConsts, repoRoot)
}

// checkIndexAssignsInFunc is CONFIDENT check A's sibling: an `out["wireKey"]
// = value` index-assignment statement AFTER a map was already built --
// this repo's other dominant map-mutation idiom (services/comprehend's
// resourceMap: `out := cloneMap(resource.Configuration); out["Status"] =
// resource.Status`, the real gopherstack-3dzb/8f6239230 bug's own shape),
// invisible to checkLiteralsInFunc since nothing here is a composite-literal
// element at all. Restricted to an Ident base with a statically
// string-resolvable index -- in this repo's map[string]any convention, only
// a map is ever indexed by a resolvable string literal (a slice/array index
// is an int expression), so this cannot mistake a slice index for a map key.
func checkIndexAssignsInFunc(
	fd *ast.FuncDecl, fset *token.FileSet, reg *enumRegistry,
	wireKeys map[string]wireKeyFact, localConsts, pkgConsts map[string]string, repoRoot string,
) []finding {
	var out []finding

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || as.Tok != token.ASSIGN || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}

		idx, ok := as.Lhs[0].(*ast.IndexExpr)
		if !ok {
			return true
		}

		if _, isIdent := idx.X.(*ast.Ident); !isIdent {
			return true
		}

		key, ok := resolveConstString(idx.Index, localConsts, pkgConsts, reg)
		if !ok {
			return true
		}

		if f, found := evalKeyValue(key, as.Rhs[0], fset, reg, wireKeys, localConsts, pkgConsts, repoRoot); found {
			out = append(out, f)
		}

		return true
	})

	return out
}

// evalKeyValue is the CONFIDENT/ambiguous-key decision shared by
// checkLiteralElt (a composite-literal entry) and checkIndexAssignsInFunc
// (an index-assignment statement): key is already resolved, valueExpr is
// resolved here the same single-hop way (literal, const, SSK enum
// selector/conversion, or -- gopherstack-3dzb -- a single-hop struct field
// read via localConsts).
func evalKeyValue(
	key string, valueExpr ast.Expr, fset *token.FileSet, reg *enumRegistry,
	wireKeys map[string]wireKeyFact, localConsts, pkgConsts map[string]string, repoRoot string,
) (finding, bool) {
	fact, known := wireKeys[key]
	if !known {
		return finding{}, false
	}

	val, ok := resolveConstString(valueExpr, localConsts, pkgConsts, reg)
	// "" is this repo's overwhelming placeholder for "no backend/not set" (a
	// degenerate fallback response, not a copy-pasted wrong enum value --
	// confirmed live at services/cloudwatchlogs/handler_integrations.go:181,
	// a nil-backend fallback), never a real enum-typed field's intended
	// value -- excluded to avoid flagging every such placeholder as a bug.
	if !ok || val == "" {
		return finding{}, false
	}

	pos := fset.Position(valueExpr.Pos())
	base := finding{
		File: relPath(repoRoot, pos.Filename), Line: pos.Line,
		Key: key, Enum: strings.Join(fact.Enums, "|"), Value: val,
	}

	// An UNAMBIGUOUS single enum candidate with no Polymorphic plain-string
	// sighting is CONFIDENT: the emitted value's own enum type is known for
	// certain, so a non-member value is sound proof of a bug -- UNLESS that
	// one candidate's own SDK module is not native to this directory (see
	// enumRegistry.confidentModuleOK): gopherstack-7fps's ec2/outposts
	// contamination, where the sole candidate came from a module this
	// directory's own production code never imports at all.
	if len(fact.Enums) == 1 && !fact.Polymorphic {
		if reg.isMemberOfAny(val, fact.Enums) {
			return finding{}, false
		}

		if !reg.confidentModuleOK(key, fact.Enums[0]) {
			return finding{}, false
		}

		base.Kind, base.Confident = kindLiteral, true

		return base, true
	}

	// Otherwise the key is ambiguous (2+ real enum candidates SDK-wide,
	// e.g. inspector2's "status" spanning 13 unrelated *Status enums) or
	// Polymorphic (also a plain, non-enum string somewhere) -- this scan
	// cannot tell which sense applies at this emission site, so it is never
	// CONFIDENT. But when the value fails membership in at least one
	// candidate, at least one real sense of this key would reject it --
	// worth a human's judgement even though the scan can't prove which sense
	// is the true one. Confirmed live: inspector2's rescanDurationState
	// reused statusEnabled ("ENABLED") under "status", valid only for
	// Status/DelegatedAdminStatus, never for the EcrRescanDurationStatus
	// (SUCCESS/PENDING/FAILED) actually in play there -- a real bug the
	// prior all-or-nothing filter dropped silently.
	if reg.isMemberOfAll(val, fact.Enums) {
		return finding{}, false
	}

	base.Kind = kindAmbiguousKey

	return base, true
}

func exprText(fset *token.FileSet, e ast.Expr) string {
	var sb strings.Builder
	if err := format.Node(&sb, fset, e); err != nil {
		return ""
	}

	return sb.String()
}
