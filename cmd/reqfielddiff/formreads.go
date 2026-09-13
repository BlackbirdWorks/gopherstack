package main

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

// formFieldKeys builds the candidate key set a query-protocol form-read is
// allowed to match for ONE operation: each of its own SDK Input field
// names, normalized, plus a singular variant for the query-protocol
// convention where a plural field (KeyNames) is read from singular indexed
// member keys (KeyName.1, KeyName.2, ...). Restricting the candidate set to
// one operation's own fields is what makes matching a bare url.Values.Get
// or helper call safe -- see matchFormReadCall's doc and the package doc's
// "targeted, not blanket" reasoning. The map value is the field's own
// (unnormalized) SDK name, so a match can be recorded keyed the way
// findMissing looks it up.
func formFieldKeys(fields []sdkField) map[string]string {
	out := make(map[string]string, len(fields)*2) //nolint:mnd // rough capacity hint, not a meaningful constant

	for _, f := range fields {
		out[normalizeWireName(f.Name)] = f.Name

		if sing, ok := singularVariant(f.Name); ok {
			key := normalizeWireName(sing)
			if _, exists := out[key]; !exists {
				out[key] = f.Name
			}
		}
	}

	return out
}

// singularVariant strips a common English plural suffix from an SDK field
// name. Deliberately simple -- English pluralization has more shapes than
// this covers (irregular plurals like "Children"), a disclosed scope
// limit, not a claim of completeness.
func singularVariant(name string) (string, bool) {
	const minStrippable = 1

	switch {
	case strings.HasSuffix(name, "ies") && len(name) > len("ies"):
		return name[:len(name)-len("ies")] + "y", true
	case strings.HasSuffix(name, "ses") && len(name) > len("ses"):
		return name[:len(name)-len("es")], true
	case strings.HasSuffix(name, "s") && !strings.HasSuffix(name, "ss") && len(name) > minStrippable:
		return name[:len(name)-1], true
	default:
		return "", false
	}
}

// urlValuesParamNames returns the names of every url.Values-holding local
// in fl: a direct PARAMETER of that type (`vals url.Values`, `form
// url.Values`, `q url.Values`), and a local reassigned from one via
// `q := c.Request().URL.Query()` (lambda's durable-execution family reads
// this way: `q := c.Request().URL.Query(); ... q.Get("ReverseOrder")`,
// even indexing it directly as `q["Statuses"]` -- matchFormReadCall's
// helper-call path still only fires for a genuine url.Values PARAMETER,
// since a package-level helper's own signature can't have been written
// against a local this scan only discovers by reading the caller).
func urlValuesParamNames(fl funcLike) map[string]bool {
	out := map[string]bool{}

	addURLValuesParams(fl.Params, out)
	addURLValuesReassignments(fl.Body, out)

	return out
}

// addURLValuesParams records the names of fl's direct url.Values-typed
// parameters into out. Split out of urlValuesParamNames to keep that
// function's cognitive complexity under the gocognit limit.
func addURLValuesParams(params *ast.FieldList, out map[string]bool) {
	if params == nil {
		return
	}

	for _, field := range params.List {
		if !isURLValuesType(field.Type) {
			continue
		}

		for _, n := range field.Names {
			out[n.Name] = true
		}
	}
}

// addURLValuesReassignments records the names of locals in body reassigned
// from a `.Query()` call (e.g. `q := c.Request().URL.Query()`) into out.
// Split out of urlValuesParamNames to keep that function's cognitive
// complexity under the gocognit limit.
func addURLValuesReassignments(body *ast.BlockStmt, out map[string]bool) {
	if body == nil {
		return
	}

	ast.Inspect(body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}

		call, ok := as.Rhs[0].(*ast.CallExpr)
		if !ok || !isURLQueryCall(call) {
			return true
		}

		if id, isIdent := as.Lhs[0].(*ast.Ident); isIdent && id.Name != "_" {
			out[id.Name] = true
		}

		return true
	})
}

func isURLValuesType(t ast.Expr) bool {
	sel, ok := t.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	id, ok := sel.X.(*ast.Ident)

	return ok && id.Name == "url" && sel.Sel.Name == "Values"
}

// isURLQueryCall reports whether call is a zero-argument `.Query()` call --
// net/url's `(*url.URL).Query() url.Values` accessor, reached in this
// repo as `c.Request().URL.Query()` / `r.URL.Query()` -- matched
// structurally (any zero-arg call named exactly "Query") rather than by
// fully resolving the receiver chain's type. Used both to recognise a
// reassigned local (urlValuesParamNames above) and a fully inline chained
// read with no intermediate variable at all (matchFormGetCall's second
// case below -- apigatewayv2's `c.Request().URL.Query().Get("basepath")`).
func isURLQueryCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)

	return ok && sel.Sel.Name == "Query" && len(call.Args) == 0
}

// matchFormReadCall recognises a query-protocol form read keyed by op's own
// SDK field names, restricted to two shapes: (1) `vals.Get("Name")` where
// vals is a url.Values-typed parameter of the function being scanned, and
// (2) a call to a package-level helper whose own first parameter is
// url.Values-typed (this repo's many differently-named indexed-list/prefix
// helpers -- parseMemberList, extractIndexedList, parseIndexedValues,
// parseSESMemberList, ... -- recognised structurally by their own
// signature, not by name) passed one of the scanned function's own
// url.Values parameters, with a PascalCase string-literal argument. Both
// are scoped to formKeys -- op's own SDK field names, singular and plural
// -- so an unrelated map/cache Get() call, or a helper call carrying an
// unrelated literal, can only ever produce a spurious match if it happens
// to spell one of THIS operation's own field names on a receiver this scan
// has independently confirmed is that operation's own url.Values -- the
// collision risk the tool's author judged acceptable once narrowed this
// far (see package doc).
func matchFormReadCall(
	call *ast.CallExpr,
	urlValuesNames map[string]bool,
	formKeys map[string]string,
	ctx handlerResolveCtx,
	res *opResolution,
) {
	if len(formKeys) == 0 {
		return
	}

	if matchFormGetCall(call, urlValuesNames, formKeys, res) {
		return
	}

	if len(urlValuesNames) == 0 {
		return
	}

	matchFormHelperCall(call, urlValuesNames, formKeys, ctx, res)
}

// matchFormGetCall matches `vals.Get("Name")` -- either vals is a
// url.Values-typed parameter/reassigned-local of the function being
// scanned (urlValuesNames, PascalCase query-protocol convention -- keeps
// addFormReadLiteral's uppercase-first-letter gate), or the receiver IS
// itself the `.Query()` call (apigatewayv2's fully inline
// `c.Request().URL.Query().Get("basepath")`, no intermediate variable at
// all -- this repo's non-query-protocol services spell these camelCase, so
// that gate is dropped for this branch; formKeys' own per-op scoping is
// still the real safety net either way).
func matchFormGetCall(
	call *ast.CallExpr,
	urlValuesNames map[string]bool,
	formKeys map[string]string,
	res *opResolution,
) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Get" || len(call.Args) == 0 {
		return false
	}

	switch recv := sel.X.(type) {
	case *ast.Ident:
		if !urlValuesNames[recv.Name] {
			return false
		}

		return addFormReadLiteral(call.Args[0], formKeys, res)
	case *ast.CallExpr:
		if !isURLQueryCall(recv) {
			return false
		}

		return matchWireLiteral(call.Args[0], formKeys, res, false)
	default:
		return false
	}
}

// matchFormHelperCall matches a call to a package-level helper whose own
// first parameter is url.Values -- ec2's `parseMemberList(vals, "KeyName")`
// shape, and its equivalents across the other affected services.
func matchFormHelperCall(
	call *ast.CallExpr,
	urlValuesNames map[string]bool,
	formKeys map[string]string,
	ctx handlerResolveCtx,
	res *opResolution,
) {
	fn, ok := call.Fun.(*ast.Ident)
	if !ok {
		return
	}

	fd, ok := ctx.funcs[fn.Name]
	if !ok || fd.Type == nil || fd.Type.Params == nil || len(fd.Type.Params.List) == 0 {
		return
	}

	if !isURLValuesType(fd.Type.Params.List[0].Type) {
		return
	}

	passesURLValues := false

	for _, arg := range call.Args {
		if id, isIdent := arg.(*ast.Ident); isIdent && urlValuesNames[id.Name] {
			passesURLValues = true

			break
		}
	}

	if !passesURLValues {
		return
	}

	for _, arg := range call.Args {
		addFormReadLiteral(arg, formKeys, res)
	}
}

// addFormReadLiteral checks a call-argument expression for a string literal
// matching one of formKeys, either whole (a scalar field, or a plural
// field's singular member prefix: "KeyName" matching declared "KeyNames")
// or by its first dot-segment (a nested-prefix read like
// "AssociationTarget.InstanceId", matched against the top-level
// "AssociationTarget" field this scan is scoped to -- see the package doc,
// this tool only ever compares top-level Input fields). Requires an
// uppercase-ASCII first letter, since every AWS wire/query-param name in
// this repo's query-protocol services is PascalCase; a lowercase literal is
// never a wire key and is excluded before it can collide with anything.
func addFormReadLiteral(arg ast.Expr, formKeys map[string]string, res *opResolution) bool {
	return matchWireLiteral(arg, formKeys, res, true)
}

// matchWireLiteral is addFormReadLiteral's shared core, parameterized on
// whether an uppercase first letter is required -- dropped for the
// non-query-protocol read shapes (a fully chained `.Query().Get(lit)` with
// no url.Values receiver, a header read) whose camelCase/mixed-case
// conventions would never pass that gate at all; formKeys' own per-op
// field-name scoping remains the actual safety net regardless.
func matchWireLiteral(arg ast.Expr, formKeys map[string]string, res *opResolution, requireUpper bool) bool {
	lit, ok := arg.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}

	s, err := strconv.Unquote(lit.Value)
	if err != nil || s == "" {
		return false
	}

	if requireUpper && (s[0] < 'A' || s[0] > 'Z') {
		return false
	}

	matched := false

	if canonical, whole := formKeys[normalizeWireName(s)]; whole {
		recordFormRead(res, canonical, s)

		matched = true
	}

	if head, _, found := strings.Cut(s, "."); found {
		if canonical, prefix := formKeys[normalizeWireName(head)]; prefix {
			recordFormRead(res, canonical, s)

			matched = true
		}
	}

	return matched
}

func recordFormRead(res *opResolution, canonicalSDKName, literal string) {
	res.Fields[normalizeWireName(canonicalSDKName)] = emuField{WireName: literal, GoName: canonicalSDKName}
	res.HasSignal = true
}

// headerPrefixesToStrip are HTTP header prefixes this repo's own header
// reads carry that a plain SDK field name never does -- "X-Amz-Acl" reads
// s3's ACL field (gopherstack-7fve's confirmed instance). Checked
// case-insensitively, longest-first so "x-amz-" is tried before the bare
// "x-" it would otherwise also match.
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as sdkfields.go's dirModuleOverride
var headerPrefixesToStrip = []string{"x-amz-", "x-"}

func stripHeaderPrefix(name string) string {
	lower := strings.ToLower(name)

	for _, p := range headerPrefixesToStrip {
		if strings.HasPrefix(lower, p) {
			return name[len(p):]
		}
	}

	return name
}

// isHeaderGetCall reports whether call is a `<expr>.Header.Get(...)`
// chain -- gopherstack-7fve's confirmed shape (s3's PutBucketAcl/
// PutObjectAcl read their canned-ACL field off the X-Amz-Acl request
// header). Matched structurally (any ".Header" selector immediately
// followed by ".Get(...)") rather than by resolving the full receiver
// chain's type -- a field/method pair spelled exactly "Header"+"Get" for
// an unrelated reason is not a shape this repo's HTTP handlers exhibit.
func isHeaderGetCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Get" {
		return false
	}

	inner, ok := sel.X.(*ast.SelectorExpr)

	return ok && inner.Sel.Name == "Header"
}

// matchHeaderReadCall recognises a header read keyed by op's own SDK field
// names (formKeys, the same per-op candidate set formreads.go's
// query-protocol matching uses) after stripping a known header prefix --
// "X-Amz-Acl" strips to "Acl", which normalizes to match a declared "ACL"
// field.
func matchHeaderReadCall(call *ast.CallExpr, formKeys map[string]string, res *opResolution) {
	if len(formKeys) == 0 || !isHeaderGetCall(call) || len(call.Args) == 0 {
		return
	}

	lit, ok := call.Args[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return
	}

	s, err := strconv.Unquote(lit.Value)
	if err != nil || s == "" {
		return
	}

	if canonical, matched := formKeys[normalizeWireName(stripHeaderPrefix(s))]; matched {
		recordFormRead(res, canonical, s)
	}
}

// isScalarParamType reports whether t is one of this repo's plain
// REST-path-value parameter types -- string, or a numeric/bool scalar.
// Deliberately excludes anything else (structs, pointers, slices,
// *echo.Context, ...) so matchOwnParamNames can never accidentally credit
// a non-scalar parameter as if its own name were a wire value.
func isScalarParamType(t ast.Expr) bool {
	id, ok := t.(*ast.Ident)
	if !ok {
		return false
	}

	switch id.Name {
	case goStringType, "int", "int32", "int64", "bool", "float64":
		return true
	default:
		return false
	}
}

// matchOwnParamNames matches fl's own scalar parameter names against
// formKeys -- a REST-path value an upstream dispatcher already extracted
// and threaded in as a plain function argument, with no read call of its
// own anywhere in THIS function at all (apigatewayv2's
// handleUpdateStage(c, apiID, stageName string): stageName IS StageName,
// declared purely because the parameter's own name matches the op's own
// field set -- "a label is required to route", so its value necessarily
// reaches the handler somehow even when no call site names it). Gated to
// hop 0 by scanBody's caller: a deeper hop's parameter names belong to some
// unrelated helper, not this operation's wire values.
func matchOwnParamNames(fl funcLike, formKeys map[string]string, res *opResolution) {
	if len(formKeys) == 0 || fl.Params == nil {
		return
	}

	for _, field := range fl.Params.List {
		if !isScalarParamType(field.Type) {
			continue
		}

		for _, n := range field.Names {
			if canonical, ok := formKeys[normalizeWireName(n.Name)]; ok {
				recordFormRead(res, canonical, n.Name)
			}
		}
	}
}

// isPathSegmentAccessorSig reports whether ft is a `func([]string, T)
// string`-shaped path-segment accessor -- quicksight's
// `seg(segs []string, i int) string`, called as `seg(segs, segResID)` with
// a positional CONSTANT, not a literal name, so nothing at the call site
// itself spells the wire name (see matchPathSegmentLocalNames, which
// relies on the ASSIGNED LOCAL's own name instead). T is deliberately
// unconstrained -- an int literal, a named enum constant's type, whatever
// this repo's index parameter happens to be -- since the real gate is
// downstream: the resulting local's own name must independently match one
// of THIS op's own SDK field names.
func isPathSegmentAccessorSig(ft *ast.FuncType) bool {
	if ft.Params == nil || len(ft.Params.List) == 0 {
		return false
	}

	arr, ok := ft.Params.List[0].Type.(*ast.ArrayType)
	if !ok || arr.Len != nil {
		return false
	}

	elt, ok := arr.Elt.(*ast.Ident)
	if !ok || elt.Name != goStringType {
		return false
	}

	if ft.Results == nil || len(ft.Results.List) != 1 {
		return false
	}

	resType, ok := ft.Results.List[0].Type.(*ast.Ident)

	return ok && resType.Name == goStringType
}

// matchPathSegmentLocalNames matches a local variable's own name against
// formKeys when it was assigned from a path-segment accessor call
// (isPathSegmentAccessorSig) -- quicksight's `namespace := seg(segs,
// segResID)`, declaring Namespace the same way matchOwnParamNames declares
// a parameter, just one assignment removed. Gated to hop 0 by scanBody's
// caller for the same reason matchOwnParamNames is.
func matchPathSegmentLocalNames(fl funcLike, ctx handlerResolveCtx, formKeys map[string]string, res *opResolution) {
	if len(formKeys) == 0 || fl.Body == nil {
		return
	}

	ast.Inspect(fl.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || as.Tok != token.DEFINE || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}

		call, ok := as.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}

		id, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}

		fd, ok := ctx.funcs[id.Name]
		if !ok || !isPathSegmentAccessorSig(fd.Type) {
			return true
		}

		lhsID, isIdent := as.Lhs[0].(*ast.Ident)
		if !isIdent || lhsID.Name == "_" {
			return true
		}

		if canonical, matched := formKeys[normalizeWireName(lhsID.Name)]; matched {
			recordFormRead(res, canonical, lhsID.Name)
		}

		return true
	})
}
