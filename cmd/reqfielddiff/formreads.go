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

// urlValuesParamNames returns the names of fl's own parameters declared
// with type url.Values -- this repo's uniform query-protocol form-read
// receiver (`vals url.Values`, `form url.Values`, `q url.Values`). Only
// direct parameters are recognised, deliberately: a local variable
// reassigned from one (`q := c.Request().URL.Query()`) is not followed,
// the same single-hop-from-a-known-binding discipline this file uses
// elsewhere to bound false "declared" matches -- see the package doc's
// form-read coverage note for what this leaves undetected.
func urlValuesParamNames(fl funcLike) map[string]bool {
	out := map[string]bool{}

	if fl.Params == nil {
		return out
	}

	for _, field := range fl.Params.List {
		if !isURLValuesType(field.Type) {
			continue
		}

		for _, n := range field.Names {
			out[n.Name] = true
		}
	}

	return out
}

func isURLValuesType(t ast.Expr) bool {
	sel, ok := t.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	id, ok := sel.X.(*ast.Ident)

	return ok && id.Name == "url" && sel.Sel.Name == "Values"
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
	if len(formKeys) == 0 || len(urlValuesNames) == 0 {
		return
	}

	if matchFormGetCall(call, urlValuesNames, formKeys, res) {
		return
	}

	matchFormHelperCall(call, urlValuesNames, formKeys, ctx, res)
}

// matchFormGetCall matches `vals.Get("Name")` -- the direct read shape most
// scalar query-protocol fields use.
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

	recv, ok := sel.X.(*ast.Ident)
	if !ok || !urlValuesNames[recv.Name] {
		return false
	}

	return addFormReadLiteral(call.Args[0], formKeys, res)
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
	lit, ok := arg.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}

	s, err := strconv.Unquote(lit.Value)
	if err != nil || s == "" || s[0] < 'A' || s[0] > 'Z' {
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
