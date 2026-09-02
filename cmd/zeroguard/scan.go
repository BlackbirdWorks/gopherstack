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
	kindConfident    = "zero-guard-drops-explicit-zero"
	kindTypeMismatch = "pointer-mismatch"
)

// updatePrefixes are the operation-name prefixes this scan considers an
// Update/Put/Modify handler -- the shape where an omitted-vs-explicit-zero
// distinction on an existing resource actually matters. Create ops take a
// fresh resource with no prior state to preserve, so the same guard there
// is not this bug class.
var updatePrefixes = []string{"Update", "Put", "Modify"} //nolint:gochecknoglobals // read-only lookup table

// finding is one zeroguard result. CONFIDENT (kindConfident) shows a
// gopherstack Input-struct field declared as a plain predeclared scalar
// where the real pinned SDK member is a pointer to that same scalar type
// (signal A), AND a zero-guard in the handler that gates whether the field
// is applied at all (signal B) -- the exact shape fixed for
// apigatewayv2.UpdateAuthorizer in 406c1dcc3. NEEDS REVIEW (kindTypeMismatch)
// is signal A alone: the type mismatch is real, but no zero-guard was found
// gating its use, so whether it is actually reachable as a bug is unproven.
type finding struct {
	File      string `json:"file"`
	Kind      string `json:"kind"`
	Op        string `json:"op"`
	Field     string `json:"field"`
	SDKField  string `json:"sdkField"`
	SDKType   string `json:"sdkType"`
	Line      int    `json:"line"`
	Confident bool   `json:"confident"`
}

// scanPackage checks every non-test .go file directly in dir against the
// real SDK Input fields resolvable from mods (no recursion into
// subpackages, matching the sibling cmd tools' disclosed scope).
func scanPackage(dir, repoRoot string, mods []sdkModule, fieldCache *sdkOpFieldCache) ([]finding, error) {
	fset := token.NewFileSet()

	files, err := parseDirFiles(fset, dir)
	if err != nil {
		return nil, err
	}

	structTypes := map[string]*ast.StructType{}
	for _, f := range files {
		maps.Copy(structTypes, topLevelStructs(f))
	}

	var out []finding

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			found, scanErr := checkHandlerFunc(fd, fset, structTypes, mods, fieldCache, repoRoot)
			if scanErr != nil {
				return nil, scanErr
			}

			out = append(out, found...)
		}
	}

	out = dedupeFindings(out)

	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}

		return out[i].Line < out[j].Line
	})

	return out, nil
}

// dedupeFindings drops exact repeats: the same Input struct field examined
// through more than one handler function (e.g. a routing wrapper and the
// backend method it calls, both taking the same *XInput) reports the same
// field at the same struct-declaration line once per function otherwise.
func dedupeFindings(in []finding) []finding {
	type key struct {
		file, op, field, kind string
		line                  int
	}

	seen := map[key]bool{}
	out := make([]finding, 0, len(in))

	for _, f := range in {
		k := key{file: f.File, op: f.Op, field: f.Field, kind: f.Kind, line: f.Line}
		if seen[k] {
			continue
		}

		seen[k] = true

		out = append(out, f)
	}

	return out
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

func topLevelStructs(f *ast.File) map[string]*ast.StructType {
	out := map[string]*ast.StructType{}

	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}

		for _, spec := range gd.Specs {
			ts, isSpec := spec.(*ast.TypeSpec)
			if !isSpec {
				continue
			}

			if st, isStruct := ts.Type.(*ast.StructType); isStruct {
				out[ts.Name.Name] = st
			}
		}
	}

	return out
}

// checkHandlerFunc examines one candidate Update/Put/Modify handler: its
// Input-struct parameter's plain-scalar fields against the real pinned SDK
// operation of the same name, then its body for a zero-guard on any
// mismatched field.
func checkHandlerFunc(
	fd *ast.FuncDecl, fset *token.FileSet, structTypes map[string]*ast.StructType,
	mods []sdkModule, fieldCache *sdkOpFieldCache, repoRoot string,
) ([]finding, error) {
	paramName, structName, ok := inputParam(fd)
	if !ok {
		return nil, nil
	}

	opName, ok := updateOpName(structName)
	if !ok {
		return nil, nil
	}

	st, ok := structTypes[structName]
	if !ok || st.Fields == nil {
		return nil, nil
	}

	sdkFields, ok, err := resolveOpFields(mods, fieldCache, opName)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	var out []finding

	for _, field := range st.Fields.List {
		f, hit := checkField(fd, fset, field, paramName, opName, sdkFields, repoRoot)
		if hit {
			out = append(out, f)
		}
	}

	return out, nil
}

func resolveOpFields(
	mods []sdkModule, fieldCache *sdkOpFieldCache, opName string,
) (map[string]sdkInputField, bool, error) {
	for _, mod := range mods {
		fields, ok, err := fieldCache.fieldsFor(mod.path, opName)
		if err != nil {
			return nil, false, err
		}

		if ok {
			return fields, true, nil
		}
	}

	return nil, false, nil
}

// inputParam returns the name and struct-type name of fd's first parameter
// whose type is `T` or `*T` with T's name ending "Input".
func inputParam(fd *ast.FuncDecl) (string, string, bool) {
	if fd.Type.Params == nil {
		return "", "", false
	}

	for _, field := range fd.Type.Params.List {
		name, ok := inputStructName(field.Type)
		if !ok || len(field.Names) == 0 {
			continue
		}

		return field.Names[0].Name, name, true
	}

	return "", "", false
}

func inputStructName(t ast.Expr) (string, bool) {
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}

	id, ok := t.(*ast.Ident)
	if !ok || !strings.HasSuffix(id.Name, "Input") {
		return "", false
	}

	return id.Name, true
}

// updateOpName derives the real AWS operation name from a gopherstack Input
// struct name (its "Input" suffix stripped) and reports whether it is an
// Update/Put/Modify shaped op -- see updatePrefixes.
func updateOpName(structName string) (string, bool) {
	op := strings.TrimSuffix(structName, "Input")

	for _, p := range updatePrefixes {
		if strings.HasPrefix(op, p) {
			return op, true
		}
	}

	return "", false
}

func checkField(
	fd *ast.FuncDecl, fset *token.FileSet, field *ast.Field, paramName, opName string,
	sdkFields map[string]sdkInputField, repoRoot string,
) (finding, bool) {
	id, ok := plainScalarField(field)
	if !ok {
		return finding{}, false
	}

	sdkField, matched := matchSDKField(sdkFields, id.Name)
	if !matched || !sdkField.isPointerScalar || sdkField.baseType != scalarIdentName(field.Type) {
		return finding{}, false
	}

	base := finding{
		Op: opName, Field: id.Name, SDKField: sdkField.name,
		SDKType: "*" + sdkField.baseType,
	}

	if line, hasGuard := findZeroGuard(fd, fset, paramName, id.Name, sdkField.baseType); hasGuard {
		base.Kind, base.Confident, base.Line = kindConfident, true, line
		base.File = relPath(repoRoot, fset.Position(fd.Pos()).Filename)

		return base, true
	}

	base.Kind = kindTypeMismatch
	base.Line = fset.Position(id.Pos()).Line
	base.File = relPath(repoRoot, fset.Position(id.Pos()).Filename)

	return base, true
}

// plainScalarField returns field's single name identifier when field's type
// is a bare predeclared scalar identifier (not a pointer, slice, map or
// named/enum type).
func plainScalarField(field *ast.Field) (*ast.Ident, bool) {
	if len(field.Names) != 1 {
		return nil, false
	}

	t, ok := field.Type.(*ast.Ident)
	if !ok || !scalarBaseTypes[t.Name] {
		return nil, false
	}

	return field.Names[0], true
}

func scalarIdentName(t ast.Expr) string {
	id, ok := t.(*ast.Ident)
	if !ok {
		return ""
	}

	return id.Name
}

// matchSDKField looks up name against sdkFields case-insensitively --
// gopherstack and the pinned SDK sometimes differ only in the casing of a
// common abbreviation (AuthorizerResultTTLInSeconds vs.
// AuthorizerResultTtlInSeconds), which strings.EqualFold treats as equal
// since they differ solely in letter case, not letter count.
func matchSDKField(sdkFields map[string]sdkInputField, name string) (sdkInputField, bool) {
	if f, ok := sdkFields[name]; ok {
		return f, true
	}

	for sdkName, f := range sdkFields {
		if strings.EqualFold(sdkName, name) {
			return f, true
		}
	}

	return sdkInputField{}, false
}

func relPath(repoRoot, path string) string {
	rel, err := filepath.Rel(repoRoot, path)
	if err != nil {
		return path
	}

	return rel
}

func exprText(fset *token.FileSet, e ast.Expr) string {
	var sb strings.Builder
	if err := format.Node(&sb, fset, e); err != nil {
		return ""
	}

	return sb.String()
}

// findZeroGuard walks fd's body for an if-statement whose condition tests
// paramName.fieldName against its zero value (or, for a bool field, tests it
// directly for truthiness) and whose body references that same field --
// the exact shape of the pre-fix apigatewayv2.UpdateAuthorizer guards this
// tool is validated against (406c1dcc3).
func findZeroGuard(fd *ast.FuncDecl, fset *token.FileSet, paramName, fieldName, baseType string) (int, bool) {
	selText := paramName + "." + fieldName

	line, found := 0, false

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if found {
			return false
		}

		ifStmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}

		if guardMatchesField(fset, ifStmt.Cond, selText, baseType) && bodyReferencesField(fset, ifStmt.Body, selText) {
			found = true
			line = fset.Position(ifStmt.Pos()).Line

			return false
		}

		return true
	})

	return line, found
}

func guardMatchesField(fset *token.FileSet, cond ast.Expr, selText, baseType string) bool {
	switch c := cond.(type) {
	case *ast.ParenExpr:
		return guardMatchesField(fset, c.X, selText, baseType)
	case *ast.BinaryExpr:
		if c.Op != token.NEQ {
			return false
		}

		if exprText(fset, c.X) == selText && isZeroLiteral(c.Y, baseType) {
			return true
		}

		return exprText(fset, c.Y) == selText && isZeroLiteral(c.X, baseType)
	case *ast.SelectorExpr:
		return baseType == "bool" && exprText(fset, c) == selText
	default:
		return false
	}
}

func isZeroLiteral(expr ast.Expr, baseType string) bool {
	lit, ok := expr.(*ast.BasicLit)
	if !ok {
		return false
	}

	if baseType == "string" {
		v, err := strconv.Unquote(lit.Value)

		return lit.Kind == token.STRING && err == nil && v == ""
	}

	if lit.Kind != token.INT && lit.Kind != token.FLOAT {
		return false
	}

	f, err := strconv.ParseFloat(lit.Value, 64)

	return err == nil && f == 0
}

// bodyReferencesField reports whether block contains a reference to
// selText anywhere -- confirming the guard actually gates a use of the
// field, not an unrelated check with an empty or dead body.
func bodyReferencesField(fset *token.FileSet, block *ast.BlockStmt, selText string) bool {
	found := false

	ast.Inspect(block, func(n ast.Node) bool {
		if found {
			return false
		}

		sel, ok := n.(*ast.SelectorExpr)
		if ok && exprText(fset, sel) == selText {
			found = true

			return false
		}

		return true
	})

	return found
}
