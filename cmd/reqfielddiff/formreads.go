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
	localLits map[string]string,
	chainVisited map[*ast.FuncDecl]bool,
) {
	if len(formKeys) == 0 {
		return
	}

	if matchFormGetCall(call, urlValuesNames, formKeys, res, localLits) {
		return
	}

	if len(urlValuesNames) == 0 {
		return
	}

	matchFormHelperCall(call, urlValuesNames, formKeys, ctx, res, localLits, chainVisited)
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
	localLits map[string]string,
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

		return addFormReadLiteral(call.Args[0], formKeys, res, localLits)
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
// shape, and its equivalents across the other affected services. When the
// helper resolves, its own body is chased too (scanURLValuesFuncBody) --
// rds's handleDescribeDBInstances calls the generic paginateDescribe(vals,
// ...) directly, which itself calls parseDescribePagination(vals), whose
// OWN body reads `vals.Get("MaxRecords")`: two calls from the handler,
// past scanBody's single-hop cap on every OTHER decode signal. Chasing
// this chain has no depth limit (chainVisited only guards against a
// cycle) because, unlike scanBody's return-type struct resolution --
// capped at one hop specifically to avoid gopherstack-id70's
// same-named-different-receiver hazard -- every step here is gated by
// three independent conditions regardless of depth: the callee's own
// first parameter must be url.Values (structural), the caller must pass
// one of its OWN already-confirmed url.Values locals into it (dataflow),
// and a match still only counts against THIS operation's own SDK field
// names (formKeys).
func matchFormHelperCall(
	call *ast.CallExpr,
	urlValuesNames map[string]bool,
	formKeys map[string]string,
	ctx handlerResolveCtx,
	res *opResolution,
	localLits map[string]string,
	chainVisited map[*ast.FuncDecl]bool,
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
		addFormReadLiteral(arg, formKeys, res, localLits)
	}

	scanURLValuesFuncBody(fd, ctx, formKeys, res, chainVisited)
}

// scanURLValuesFuncBody walks fd's own body for Get() calls and further
// url.Values-forwarding helper calls, keyed by fd's OWN url.Values
// parameter names rather than the original caller's -- see
// matchFormHelperCall's doc for why following this chain to any depth is
// safe. chainVisited prevents infinite recursion through a call cycle.
func scanURLValuesFuncBody(
	fd *ast.FuncDecl,
	ctx handlerResolveCtx,
	formKeys map[string]string,
	res *opResolution,
	chainVisited map[*ast.FuncDecl]bool,
) {
	if fd == nil || fd.Body == nil || fd.Type == nil || chainVisited[fd] {
		return
	}

	chainVisited[fd] = true

	ownNames := map[string]bool{}
	addURLValuesParams(fd.Type.Params, ownNames)

	if len(ownNames) == 0 {
		return
	}

	localLits := map[string]string{}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if as, ok := n.(*ast.AssignStmt); ok {
			recordLocalPrefixAssign(as, localLits)

			return true
		}

		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		matchFormGetCall(call, ownNames, formKeys, res, localLits)
		matchFormHelperCall(call, ownNames, formKeys, ctx, res, localLits, chainVisited)

		return true
	})
}

// addFormReadLiteral checks a call-argument expression for a wire-name
// prefix matching one of formKeys, either whole (a scalar field, or a
// plural field's singular member prefix: "KeyName" matching declared
// "KeyNames") or by its first dot-segment (a nested-prefix read like
// "AssociationTarget.InstanceId", matched against the top-level
// "AssociationTarget" field this scan is scoped to -- see the package doc,
// this tool only ever compares top-level Input fields). Requires an
// uppercase-ASCII first letter, since every AWS wire/query-param name in
// this repo's query-protocol services is PascalCase; a lowercase literal is
// never a wire key and is excluded before it can collide with anything.
// The argument need not be a plain string literal -- see
// resolveLiteralPrefix.
func addFormReadLiteral(arg ast.Expr, formKeys map[string]string, res *opResolution, localLits map[string]string) bool {
	return matchExprLiteral(arg, formKeys, res, true, localLits)
}

// matchExprLiteral is addFormReadLiteral's shared core: resolve arg to a
// literal prefix (resolveLiteralPrefix, which -- unlike a plain
// *ast.BasicLit check -- follows an fmt.Sprintf format string or a local
// variable built from string concatenation) and match it the same way
// matchWireLiteral does.
func matchExprLiteral(
	arg ast.Expr,
	formKeys map[string]string,
	res *opResolution,
	requireUpper bool,
	localLits map[string]string,
) bool {
	s, ok := resolveLiteralPrefix(arg, localLits)
	if !ok {
		return false
	}

	return matchLiteralString(cutAtFormatVerb(s), formKeys, res, requireUpper)
}

// resolveLiteralPrefix resolves a wire-name-bearing prefix out of an
// expression more general than a single string literal -- the two shapes
// this repo's indexed-list/nested-prefix form-read helpers build their
// AWS query-protocol keys with: a string concatenation chain rooted in a
// literal (`"ImageCriterion." + strconv.Itoa(i)`, ec2's
// parseImageCriteria; `"DhcpConfiguration." + strconv.Itoa(i) + ".Key"`,
// ec2's parseDhcpConfigurations), and an fmt.Sprintf format string
// (`fmt.Sprintf("Filter.%d.Name", i)`, ec2's parseEC2Filters;
// `fmt.Sprintf("Tags.Tag.%d.Key", i)`, rds's parseTagEntries). localLits
// carries every local variable in the SAME function body already resolved
// to a literal prefix earlier in this same walk (recordLocalPrefixAssign
// populates it in source order, so `prefix := "ImageCriterion." +
// strconv.Itoa(i)` used two lines later as `vals.Get(prefix + ".Name")`
// resolves too). Anything else -- an unrelated function call, a
// concatenation operand with no known local -- resolves to "", false,
// deliberately: an unresolved expression is left as a finding, never
// guessed at.
func resolveLiteralPrefix(expr ast.Expr, localLits map[string]string) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}

		s, err := strconv.Unquote(e.Value)
		if err != nil {
			return "", false
		}

		return s, true
	case *ast.Ident:
		s, ok := localLits[e.Name]

		return s, ok
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}

		if s, ok := resolveLiteralPrefix(e.X, localLits); ok {
			return s, true
		}

		return resolveLiteralPrefix(e.Y, localLits)
	case *ast.CallExpr:
		if !isSprintfCall(e) || len(e.Args) == 0 {
			return "", false
		}

		return resolveLiteralPrefix(e.Args[0], localLits)
	default:
		return "", false
	}
}

// isSprintfCall reports whether call is fmt.Sprintf(...), matched
// structurally against the selector rather than an imported-package
// identity check -- consistent with this file's other structural call
// recognisers (isURLQueryCall, isHeaderGetCall).
func isSprintfCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	id, ok := sel.X.(*ast.Ident)

	return ok && id.Name == "fmt" && sel.Sel.Name == "Sprintf"
}

// cutAtFormatVerb truncates a resolved fmt.Sprintf format string at its
// first verb ("Filter.%d.Name" -> "Filter.") so the trailing "%d"/"%s"
// never reaches normalizeWireName or the dot-segment split as noise.
func cutAtFormatVerb(s string) string {
	head, _, _ := strings.Cut(s, "%")

	return head
}

// recordLocalPrefixAssign updates localLits when stmt assigns a single
// local from an expression resolveLiteralPrefix can already resolve
// (using localLits' current contents, so a later local can build on an
// earlier one defined previously in the same body). Source order makes
// this valid: ast.Inspect visits a block's statements in the order they
// appear, so the defining assignment is always visited before a later
// read of the same local.
func recordLocalPrefixAssign(as *ast.AssignStmt, localLits map[string]string) {
	if len(as.Lhs) != 1 || len(as.Rhs) != 1 {
		return
	}

	id, isIdent := as.Lhs[0].(*ast.Ident)
	if !isIdent || id.Name == "_" {
		return
	}

	if s, resolved := resolveLiteralPrefix(as.Rhs[0], localLits); resolved {
		localLits[id.Name] = s
	}
}

// matchWireLiteral is matchLiteralString's plain-*ast.BasicLit-only
// entry point, used where an fmt.Sprintf/concatenation prefix is not a
// shape this repo exhibits (a header read, the fully chained
// `.Query().Get(lit)` case) -- resolveLiteralPrefix's Ident/BinaryExpr/
// Sprintf following is deliberately not used here, since neither call
// site has a localLits map of its own to resolve against.
func matchWireLiteral(arg ast.Expr, formKeys map[string]string, res *opResolution, requireUpper bool) bool {
	lit, ok := arg.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}

	s, err := strconv.Unquote(lit.Value)
	if err != nil || s == "" {
		return false
	}

	return matchLiteralString(s, formKeys, res, requireUpper)
}

// matchLiteralString is matchWireLiteral's and matchExprLiteral's shared
// matching core, parameterized on whether an uppercase first letter is
// required -- dropped for the non-query-protocol read shapes (a fully
// chained `.Query().Get(lit)` with no url.Values receiver, a header read)
// whose camelCase/mixed-case conventions would never pass that gate at
// all; formKeys' own per-op field-name scoping remains the actual safety
// net regardless.
func matchLiteralString(s string, formKeys map[string]string, res *opResolution, requireUpper bool) bool {
	if s == "" {
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

// formLoopRanges reports whether fl's body ranges directly over one of
// urlValuesNames -- a dynamic per-key loop (`for key := range vals { if
// strings.HasPrefix(key, someExpr) ... }`) with no literal wire name at
// the range statement itself for this scan to resolve. Unlike every other
// recogniser in this file, this one never declares a field: a loop keyed
// off a runtime prefix this scan can't statically resolve is real,
// unmeasured surface, not a false positive to wave through silently (see
// the package doc's orphan lesson) -- resolveOp records it as
// FormLoopUnresolved instead, and callers report it as a separate
// "form-loop (unresolved)" count so it stays visible.
func formLoopRanges(fl funcLike, urlValuesNames map[string]bool) bool {
	if fl.Body == nil || len(urlValuesNames) == 0 {
		return false
	}

	found := false

	ast.Inspect(fl.Body, func(n ast.Node) bool {
		rs, ok := n.(*ast.RangeStmt)
		if !ok {
			return true
		}

		if id, isIdent := rs.X.(*ast.Ident); isIdent && urlValuesNames[id.Name] {
			found = true
		}

		return true
	})

	return found
}
