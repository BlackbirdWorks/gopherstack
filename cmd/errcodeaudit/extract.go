package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// mechanism identifies which syntactic shape produced a candidate emitted
// code, matching one of the four emission mechanisms this tool's brief
// identified by reading services/ecs, services/iam, services/lambda and
// services/cloudformation's handler.go files (a shared awserr sentinel, a
// stdlib errors.New sentinel whose message IS the code, a literal argument
// at each call site, and a mapping table), plus two narrower structural
// extensions (a code-named variable, and a return statement inside an
// error-code classifier function) found while reading those same files.
type mechanism string

const (
	mechAwserrNew  mechanism = "awserr.New/Newf arg"
	mechStdlibErr  mechanism = "errors.New arg"
	mechErrorCall  mechanism = "*Error()-suffixed call arg"
	mechFieldLit   mechanism = "code/type field literal"
	mechFieldIdent mechanism = "code/type field via resolved const"
	mechCodeVar    mechanism = "code-named var/const"
	mechReturnStmt mechanism = "return in *Error*-named func"
)

// candidate is one emitted-code sighting. Indirect marks a value reached
// through one hop of same-package identifier resolution (a package-level
// const/var), never more -- mirroring cmd/enumcheck's single-hop discipline
// (resolveConstString's Ident case): a value assembled through more
// indirection than that resolves to nothing and produces no candidate,
// never a wrong one.
type candidate struct {
	File      string
	Code      string
	Mechanism mechanism
	Line      int
	Indirect  bool
}

// codeShapeRe is the filter that separates an AWS-style error code
// ("ResourceNotFoundException", "NoSuchEntity", "ValidationError") from
// every other string literal these extraction rules' call/field/var shapes
// also incidentally reach: a human-readable message ("StackName is
// required"), a format string ("%w: %s"), an already-interpolated detail
// ("unknown action: "+action, not even a literal), a JSON/XML field name.
// PascalCase-or-SCREAMING, no spaces or punctuation, at least 4 characters
// -- exactly the shape every real AWS error code in this tool's ground
// truth and every one of the eleven pre-fix ecs codes shares.
var codeShapeRe = regexp.MustCompile(`^[A-Z][A-Za-z0-9]{2,}$`)

func looksLikeCode(s string) bool {
	return codeShapeRe.MatchString(s)
}

// looksLikeCodeVarName reports whether an identifier's own name marks it
// as an error-code variable/const, not merely any name that happens to
// contain "code" -- services/ce's handlerCurrencyCode ("USD") and
// services/comprehend's fieldLanguageCode ("LanguageCode") both contain
// "code" as a substring but are not error codes at all, and were false
// positives before this narrowing. A name starting with "code"
// (services/iam's codeNoSuchEntity, cloudformation's local `code :=`) or
// containing both "err" and "code" (cloudformation's errCodeValidation)
// is the pattern actually observed at real error-code declaration sites --
// EXCEPT a "key"/"field" prefix, this repo's own naming convention for a
// wire KEY-NAME constant (services/quicksight and services/securityhub's
// own `keyErrorCode = "ErrorCode"`, the JSON field name "ErrorCode" itself,
// not a code value -- both false positives before this exclusion).
func looksLikeCodeVarName(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "key") || strings.HasPrefix(lower, "field") {
		return false
	}

	if strings.HasPrefix(lower, "code") {
		return true
	}

	return strings.Contains(lower, "err") && strings.Contains(lower, "code")
}

// extractCandidates scans every non-test .go file directly in dir (no
// subpackage recursion, matching cmd/enumcheck and cmd/xmlitemwrap's own
// disclosed scope) for emitted error-code candidates.
func extractCandidates(dir, repoRoot string) ([]candidate, error) {
	fset := token.NewFileSet()

	files, err := parseNonTestDirFiles(fset, dir)
	if err != nil {
		return nil, err
	}

	structTypes := map[string]*ast.StructType{}
	pkgStrings := map[string]string{}

	for _, f := range files {
		fillElidedCompositeTypes(f)
		collectTopLevelStructs(f, structTypes)
		collectPackageStrings(f, pkgStrings)
	}

	sinkPositions := buildSinkPositions(files)

	var out []candidate

	for _, f := range files {
		out = append(
			out,
			extractFromFile(f, fset, repoRoot, structTypes, pkgStrings, sinkPositions)...)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}

		if out[i].Line != out[j].Line {
			return out[i].Line < out[j].Line
		}

		return out[i].Code < out[j].Code
	})

	return out, nil
}

func parseNonTestDirFiles(fset *token.FileSet, dir string) ([]*ast.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []*ast.File

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") ||
			strings.HasSuffix(e.Name(), "_test.go") {
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

func collectTopLevelStructs(f *ast.File, out map[string]*ast.StructType) {
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
}

// collectPackageStrings collects every top-level (package-scope) single
// name, single value, string-literal const or var -- the only identifiers
// this tool ever resolves through (one hop, same package), matching
// cmd/enumcheck's packageStringConsts but extended to var since this
// repo's error-code tables key on both (services/iam's codeNoSuchEntity is
// a const, services/ecs's keyTypeField is also a const, but nothing in
// principle rules out a var elsewhere).
func collectPackageStrings(f *ast.File, out map[string]string) {
	for _, decl := range f.Decls {
		gd, isGD := decl.(*ast.GenDecl)
		if !isGD || (gd.Tok != token.CONST && gd.Tok != token.VAR) {
			continue
		}

		for _, spec := range gd.Specs {
			collectValueSpecStrings(spec, out)
		}
	}
}

func collectValueSpecStrings(spec ast.Spec, out map[string]string) {
	vs, isVS := spec.(*ast.ValueSpec)
	if !isVS || len(vs.Names) != len(vs.Values) {
		return
	}

	for i, name := range vs.Names {
		lit, isLit := vs.Values[i].(*ast.BasicLit)
		if !isLit || lit.Kind != token.STRING {
			continue
		}

		if v, err := strconv.Unquote(lit.Value); err == nil {
			out[name.Name] = v
		}
	}
}

// fillElidedCompositeTypes mutates f's own parsed AST (never written back to
// disk -- this process's private copy) so a slice/map literal's ELIDED
// inner composite literal type ([]T{{...}, {...}}) carries T explicitly,
// the same way an explicit T{...} would. Without this, matchCompositeLit's
// error-shaped-type-name requirement can never see past a nil Type and
// silently drops every finding inside a slice-of-struct table -- confirmed
// live: services/networkmanager's []CoreNetworkPolicyError{{ErrorCode:
// "InvalidPolicyDocument", ...}} and services/xray's own
// []unprocessedSegment{{ErrorCode: "InvalidSegment", ...}} both vanished
// from this tool's own output the run this qualifier was added, before
// this fill existed to compensate.
func fillElidedCompositeTypes(f *ast.File) {
	ast.Inspect(f, func(n ast.Node) bool {
		cl, isCL := n.(*ast.CompositeLit)
		if !isCL {
			return true
		}

		switch t := cl.Type.(type) {
		case *ast.ArrayType:
			fillElidedArrayElts(cl, t)
		case *ast.MapType:
			fillElidedMapValues(cl, t)
		}

		return true
	})
}

func fillElidedArrayElts(cl *ast.CompositeLit, t *ast.ArrayType) {
	for _, elt := range cl.Elts {
		if ce, isCL := elt.(*ast.CompositeLit); isCL && ce.Type == nil {
			ce.Type = t.Elt
		}
	}
}

func fillElidedMapValues(cl *ast.CompositeLit, t *ast.MapType) {
	for _, elt := range cl.Elts {
		kv, isKV := elt.(*ast.KeyValueExpr)
		if !isKV {
			continue
		}

		if ce, isCL := kv.Value.(*ast.CompositeLit); isCL && ce.Type == nil {
			ce.Type = t.Value
		}
	}
}

func extractFromFile(
	f *ast.File,
	fset *token.FileSet,
	repoRoot string,
	structTypes map[string]*ast.StructType,
	pkgStrings map[string]string,
	sinkPositions map[string]map[int]bool,
) []candidate {
	var out []candidate

	ast.Inspect(f, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			out = append(out, matchCallExpr(node, fset, repoRoot, sinkPositions)...)
		case *ast.CompositeLit:
			out = append(out, matchCompositeLit(node, fset, repoRoot, structTypes, pkgStrings)...)
		case *ast.AssignStmt:
			out = append(out, matchAssign(node, fset, repoRoot)...)
		case *ast.GenDecl:
			out = append(out, matchGenDecl(node, fset, repoRoot)...)
		case *ast.FuncDecl:
			out = append(out, matchReturnLiterals(node, fset, repoRoot)...)
		}

		return true
	})

	return out
}

func newCandidate(
	fset *token.FileSet,
	repoRoot string,
	pos token.Pos,
	code string,
	m mechanism,
	indirect bool,
) candidate {
	p := fset.Position(pos)

	file, err := filepath.Rel(repoRoot, p.Filename)
	if err != nil {
		file = p.Filename
	}

	return candidate{File: file, Line: p.Line, Code: code, Mechanism: m, Indirect: indirect}
}

// matchCallExpr covers three of the four handler.go mechanisms directly:
// awserr.New/Newf(code, sentinel) (ecs's mechanism), stdlib errors.New(code)
// (lambda's mechanism, where the sentinel's own message IS the code), and
// a code-shaped literal argument at a known SINK POSITION of a call to a
// function/method named "...Error" (never "...Errorf") -- covers
// writeError(status, "Code", message) (lambda) and xmlError(c, "Code",
// message) (cloudformation). Which position is a sink is resolved by
// sink.go's buildSinkPositions, not by argument order alone: an
// unclassified "...Error" call (its own definition never writes a
// parameter into a Code/Type-labeled field) contributes nothing, which is
// what keeps an action-name argument like
// handleBackendError(ctx, c, "CreateApp", err) out -- see sink.go's doc
// comment for the false-positive this closed.
func matchCallExpr(
	call *ast.CallExpr, fset *token.FileSet, repoRoot string, sinkPositions map[string]map[int]bool,
) []candidate {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if ok {
		pkgIdent, isPkg := sel.X.(*ast.Ident)

		switch {
		case isPkg && pkgIdent.Name == "awserr" && (sel.Sel.Name == "New" || sel.Sel.Name == "Newf"):
			return literalArgCandidates(
				call.Args[:min(1, len(call.Args))],
				fset,
				repoRoot,
				mechAwserrNew,
			)
		case isPkg && pkgIdent.Name == "errors" && sel.Sel.Name == "New":
			return literalArgCandidates(call.Args, fset, repoRoot, mechStdlibErr)
		case strings.HasSuffix(sel.Sel.Name, "Error"):
			return sinkArgCandidates(call.Args, sinkPositions[sel.Sel.Name], fset, repoRoot)
		}

		return nil
	}

	if ident, isIdent := call.Fun.(*ast.Ident); isIdent && strings.HasSuffix(ident.Name, "Error") {
		return sinkArgCandidates(call.Args, sinkPositions[ident.Name], fset, repoRoot)
	}

	return nil
}

func literalArgCandidates(
	args []ast.Expr,
	fset *token.FileSet,
	repoRoot string,
	m mechanism,
) []candidate {
	var out []candidate

	for _, arg := range args {
		lit, ok := arg.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}

		v, err := strconv.Unquote(lit.Value)
		if err != nil || !looksLikeCode(v) {
			continue
		}

		out = append(out, newCandidate(fset, repoRoot, lit.Pos(), v, m, false))
	}

	return out
}

func sinkArgCandidates(
	args []ast.Expr,
	sinkPos map[int]bool,
	fset *token.FileSet,
	repoRoot string,
) []candidate {
	if len(sinkPos) == 0 {
		return nil
	}

	var out []candidate

	for i, arg := range args {
		if !sinkPos[i] {
			continue
		}

		lit, ok := arg.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}

		v, err := strconv.Unquote(lit.Value)
		if err != nil || !looksLikeCode(v) {
			continue
		}

		out = append(out, newCandidate(fset, repoRoot, lit.Pos(), v, mechErrorCall, false))
	}

	return out
}

// matchCompositeLit covers the fourth mechanism: a mapping table, either a
// map[string]string{keyTypeField: "Code", ...} (ecs) or a slice of a
// locally-declared struct with a code field, keyed (IAMError{Code: code})
// or positional (iamErrorMapping{ErrX, codeY, status}, resolved against the
// struct's own declared field order). Field-name matching uses the same
// narrowFieldNameMatches sink.go uses -- see its doc comment for why a
// bare "Type" field name is not enough on its own -- with one further
// narrowing: when the SAME literal also keys a "Code"/"ErrorCode" field,
// its "Type" field (if any) is never a candidate, full stop. Confirmed
// live: services/autoscaling and services/docdb's own
// autoscalingError{Code: code, Message: message, Type: "Sender"} -- the
// classic AWS Query protocol's <Error><Type>Sender/Receiver</Type></Error>
// fault-role field, not a second error code -- was a false positive this
// suppression fixes; "Sender"/"Receiver" are never listed by name because
// the same reasoning would fail to protect against a novel one.
func matchCompositeLit(
	cl *ast.CompositeLit, fset *token.FileSet, repoRoot string,
	structTypes map[string]*ast.StructType, pkgStrings map[string]string,
) []candidate {
	litTypeName := compositeLitTypeName(cl.Type)
	fieldNames := positionalFieldNames(cl.Type, structTypes)
	suppressType := compositeHasCodeField(cl, fieldNames)

	var out []candidate

	for i, elt := range cl.Elts {
		kv, keyed := elt.(*ast.KeyValueExpr)

		var matched bool

		var valueExpr ast.Expr

		switch {
		case keyed:
			matched, valueExpr = compositeKeyMatches(
				kv.Key,
				litTypeName,
				pkgStrings,
				suppressType,
			), kv.Value
		case i < len(fieldNames):
			matched, valueExpr = fieldMatches(fieldNames[i], litTypeName, suppressType), elt
		default:
			continue
		}

		if !matched {
			continue
		}

		if c, ok := resolveFieldValue(valueExpr, fset, repoRoot, pkgStrings); ok {
			out = append(out, c)
		}
	}

	return out
}

func compositeHasCodeField(cl *ast.CompositeLit, fieldNames []string) bool {
	for i, elt := range cl.Elts {
		if kv, keyed := elt.(*ast.KeyValueExpr); keyed {
			if id, isIdent := kv.Key.(*ast.Ident); isIdent && isExactCodeLabel(id.Name) {
				return true
			}

			continue
		}

		if i < len(fieldNames) && isExactCodeLabel(fieldNames[i]) {
			return true
		}
	}

	return false
}

func isExactCodeLabel(name string) bool {
	lower := strings.ToLower(name)

	return lower == labelCode || lower == labelErrorCode
}

// fieldMatches is narrowFieldNameMatches with suppressType additionally
// ruling out the "Type" family when a Code field sits alongside it.
func fieldMatches(name, litTypeName string, suppressType bool) bool {
	if suppressType && !isExactCodeLabel(name) {
		return false
	}

	return narrowFieldNameMatches(name, litTypeName)
}

// compositeKeyMatches handles a keyed composite-literal element: a struct
// field name (Code/Type/...) via fieldMatches directly, or a map key
// identifier resolved one hop through pkgStrings to its literal value
// (ecs's map[string]string{keyTypeField: ...}, where keyTypeField resolves
// to the wire discriminator "__type").
func compositeKeyMatches(
	key ast.Expr,
	litTypeName string,
	pkgStrings map[string]string,
	suppressType bool,
) bool {
	switch k := key.(type) {
	case *ast.Ident:
		if fieldMatches(k.Name, litTypeName, suppressType) {
			return true
		}

		if v, ok := pkgStrings[k.Name]; ok {
			return narrowLiteralKeyMatches(v)
		}

		return false
	case *ast.BasicLit:
		if k.Kind == token.STRING {
			if v, err := strconv.Unquote(k.Value); err == nil {
				return narrowLiteralKeyMatches(v)
			}
		}
	}

	return false
}

func narrowLiteralKeyMatches(v string) bool {
	lower := strings.ToLower(v)

	return lower == labelWireType || lower == labelCode || lower == labelErrorCode
}

// positionalFieldNames resolves a composite literal's type expression to
// its struct's declared field names in order (multi-name fields expanded),
// for the unkeyed-element case. A type this scan can't resolve (an
// imported type, a slice/map element type, a built-in) yields nil, which
// only ever skips a positional match -- never produces a wrong one.
func positionalFieldNames(typeExpr ast.Expr, structTypes map[string]*ast.StructType) []string {
	st := resolveStructType(typeExpr, structTypes)
	if st == nil || st.Fields == nil {
		return nil
	}

	var names []string

	for _, field := range st.Fields.List {
		for _, id := range field.Names {
			names = append(names, id.Name)
		}
	}

	return names
}

func resolveStructType(expr ast.Expr, structTypes map[string]*ast.StructType) *ast.StructType {
	switch e := expr.(type) {
	case *ast.StructType:
		return e
	case *ast.Ident:
		return structTypes[e.Name]
	case *ast.ArrayType:
		return resolveStructType(e.Elt, structTypes)
	case *ast.StarExpr:
		return resolveStructType(e.X, structTypes)
	default:
		return nil
	}
}

func resolveFieldValue(
	expr ast.Expr, fset *token.FileSet, repoRoot string, pkgStrings map[string]string,
) (candidate, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return candidate{}, false
		}

		v, err := strconv.Unquote(e.Value)
		if err != nil || !looksLikeCode(v) {
			return candidate{}, false
		}

		return newCandidate(fset, repoRoot, e.Pos(), v, mechFieldLit, false), true
	case *ast.Ident:
		v, ok := pkgStrings[e.Name]
		if !ok || !looksLikeCode(v) {
			return candidate{}, false
		}

		return newCandidate(fset, repoRoot, e.Pos(), v, mechFieldIdent, true), true
	default:
		return candidate{}, false
	}
}

// matchAssign covers `code := "ValidationError"` / `code =
// "StackRefactorNotFoundException"` -- a code-shaped literal assigned
// directly to a variable whose own name marks it as an error code, the
// shape services/cloudformation's handler_stack_refactors.go and
// handler_stack_sets.go's stackInstancesErrorCode use.
func matchAssign(as *ast.AssignStmt, fset *token.FileSet, repoRoot string) []candidate {
	if len(as.Lhs) != len(as.Rhs) {
		return nil
	}

	var out []candidate

	for i, lhs := range as.Lhs {
		id, ok := lhs.(*ast.Ident)
		if !ok || !looksLikeCodeVarName(id.Name) {
			continue
		}

		lit, ok := as.Rhs[i].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}

		v, err := strconv.Unquote(lit.Value)
		if err != nil || !looksLikeCode(v) {
			continue
		}

		out = append(out, newCandidate(fset, repoRoot, lit.Pos(), v, mechCodeVar, false))
	}

	return out
}

// matchGenDecl covers `const codeNoSuchEntity = "NoSuchEntity"` /
// `errCodeValidation = "ValidationError"` -- the const/var declaration form
// of the same code-named-identifier signal matchAssign reads for plain
// assignments.
func matchGenDecl(gd *ast.GenDecl, fset *token.FileSet, repoRoot string) []candidate {
	if gd.Tok != token.CONST && gd.Tok != token.VAR {
		return nil
	}

	var out []candidate

	for _, spec := range gd.Specs {
		vs, isVS := spec.(*ast.ValueSpec)
		if !isVS || len(vs.Names) != len(vs.Values) {
			continue
		}

		for i, name := range vs.Names {
			if !looksLikeCodeVarName(name.Name) {
				continue
			}

			lit, isLit := vs.Values[i].(*ast.BasicLit)
			if !isLit || lit.Kind != token.STRING {
				continue
			}

			v, err := strconv.Unquote(lit.Value)
			if err != nil || !looksLikeCode(v) {
				continue
			}

			out = append(out, newCandidate(fset, repoRoot, lit.Pos(), v, mechCodeVar, false))
		}
	}

	return out
}

// matchReturnLiterals covers services/cloudformation's mapCreateStackError/
// stackInstancesErrorCode shape: a function whose name marks it as an
// error-code classifier (contains "Error", case-insensitive -- excludes
// unrelated functions the same way codeFieldLabel's "code" substring check
// does) returning a code-shaped literal directly. Always NEEDS REVIEW (see
// scan.go): the function-name heuristic is the weakest signal this tool
// uses, since a function merely named "...Error..." can return any string,
// not necessarily a wire error code.
func matchReturnLiterals(fd *ast.FuncDecl, fset *token.FileSet, repoRoot string) []candidate {
	if fd.Body == nil || !strings.Contains(strings.ToLower(fd.Name.Name), "error") {
		return nil
	}

	var out []candidate

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		ret, isRet := n.(*ast.ReturnStmt)
		if !isRet {
			return true
		}

		for _, result := range ret.Results {
			lit, isLit := result.(*ast.BasicLit)
			if !isLit || lit.Kind != token.STRING {
				continue
			}

			v, err := strconv.Unquote(lit.Value)
			if err != nil || !looksLikeCode(v) {
				continue
			}

			out = append(out, newCandidate(fset, repoRoot, lit.Pos(), v, mechReturnStmt, true))
		}

		return true
	})

	return out
}
