package main

import (
	"go/ast"
	"go/token"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// maxHop bounds how far body scanning follows a handler's own calls into
// other package-local functions before giving up -- one hop, matching
// cmd/reqfieldscan's disclosed single-hop discipline (see that package's
// doc, "does NOT follow a field through further indirection"). Hop 0 is the
// resolved handler itself; hop 1 is a function or *Handler method it calls
// directly. This is what reaches cloudfront's real shape: handleX calls
// decodeXBody(c), a plain package func that builds and returns a named
// local struct.
const maxHop = 1

// decodeCallVerbs is matched case-insensitively against a CallExpr's own
// selector/ident name to recognise a decode call: json.Unmarshal,
// xml.Unmarshal, echo's c.Bind, and this repo's local readJSON/ReadJSON
// helpers (omics) all match "unmarshal" or "bind" or "readjson".
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as sdkfields.go's dirModuleOverride
var decodeCallVerbs = []string{"unmarshal", "bind", "readjson"}

// queryParamSelectors is matched exactly against a CallExpr's selector name
// to harvest a wire-declared name straight from a literal string argument,
// for handlers with no decode struct at all -- apigateway's real shape
// (resources.go's getResourcesAction reads three named fields off a decoded
// struct, but many other services take individual echo query/path params
// directly with no struct in between).
//
//nolint:gochecknoglobals // read-only lookup table, same pattern as sdkfields.go's dirModuleOverride
var queryParamSelectors = map[string]bool{
	"QueryParam": true,
	"Param":      true,
	"FormValue":  true,
}

// opResolution is what one operation's emulator-side declaration search
// found.
type opResolution struct {
	Fields      map[string]emuField
	FromHandler string
	StructsUsed []string
	Found       bool
	HasSignal   bool
}

// resolveOp finds the emulator's declared field set for op op. It tries
// BOTH the package's own dispatch table AND a name-convention search for a
// handler function/method, and unions whatever each finds, rather than
// picking one and stopping -- a dispatch-table value this scan can't
// resolve (an unrecognised call shape) should not suppress a
// "handle"+op-named handler sitting right there in the same package.
// Deliberately over-inclusive: a spurious extra field lowers one finding's
// rank (or produces a stray "declared" match a human dismisses in seconds);
// a spurious MISSING resolution manufactures a finding out of a tool
// failure, which is the worse mistake for a scan whose whole premise is
// "an undeclared field is real, not a resolution gap".
func resolveOp(op sdkOp, dispatch map[string]ast.Expr, ctx handlerResolveCtx) opResolution {
	res := opResolution{Fields: map[string]emuField{}}
	formKeys := formFieldKeys(op.Fields)

	if expr, ok := dispatch[op.Name]; ok {
		if dres, resolved := resolveDispatchValue(expr, ctx, formKeys); resolved {
			mergeResolution(&res, dres)
		}
	}

	if fd, _ := findHandlerByName(op.Name, ctx); fd != nil {
		mergeResolution(&res, scanTopLevel(fromFuncDecl(fd), ctx, funcKey(fd), formKeys))
	}

	return res
}

func mergeResolution(dst *opResolution, src opResolution) {
	if src.Found {
		dst.Found = true
	}

	if src.HasSignal {
		dst.HasSignal = true
	}

	if dst.FromHandler == "" {
		dst.FromHandler = src.FromHandler
	}

	dst.StructsUsed = append(dst.StructsUsed, src.StructsUsed...)

	maps.Copy(dst.Fields, src.Fields)
}

// resolveDispatchValue unwraps a dispatch-table value expression -- a
// direct WrapOp/wrapper call, a func literal whose first return forwards to
// one, or a func literal with real logic of its own -- to an opResolution.
func resolveDispatchValue(expr ast.Expr, ctx handlerResolveCtx, formKeys map[string]string) (opResolution, bool) {
	expr = unwrapParen(expr)

	if lit, isLit := expr.(*ast.FuncLit); isLit {
		if ret := firstReturnExpr(lit.Body); ret != nil {
			if res, ok := resolveCallLikeValue(ret, ctx, formKeys); ok {
				return res, true
			}
		}
		// No single clean forwarding return (or it didn't resolve): the
		// closure itself may still contain real decode logic (e.g. one
		// that extracts a path segment before calling a handler with
		// extra arguments) -- scan its own body directly rather than
		// giving up.
		return scanTopLevel(fromFuncLit(lit), ctx, "", formKeys), true
	}

	return resolveCallLikeValue(expr, ctx, formKeys)
}

func resolveCallLikeValue(expr ast.Expr, ctx handlerResolveCtx, formKeys map[string]string) (opResolution, bool) {
	if reqType, ok := resolveWrapOpReqType(expr, ctx); ok {
		def := ctx.structs[reqType]

		return opResolution{
			Fields:      fieldMap(def),
			StructsUsed: []string{reqType},
			Found:       true,
			HasSignal:   true,
		}, true
	}

	switch v := expr.(type) {
	case *ast.CallExpr:
		return resolveCalleeBody(v.Fun, ctx, formKeys)
	case *ast.SelectorExpr:
		return resolveCalleeBody(v, ctx, formKeys)
	case *ast.Ident:
		return resolveCalleeBody(v, ctx, formKeys)
	default:
		return opResolution{}, false
	}
}

// resolveCalleeBody resolves fn (a selector or ident naming a method or
// package func) to its FuncDecl and scans its body.
func resolveCalleeBody(fn ast.Expr, ctx handlerResolveCtx, formKeys map[string]string) (opResolution, bool) {
	fd := lookupFuncDecl(fn, ctx)
	if fd == nil || fd.Body == nil {
		return opResolution{Found: true}, true
	}

	return scanTopLevel(fromFuncDecl(fd), ctx, funcKey(fd), formKeys), true
}

func lookupFuncDecl(fn ast.Expr, ctx handlerResolveCtx) *ast.FuncDecl {
	switch v := fn.(type) {
	case *ast.SelectorExpr:
		if cands, ok := ctx.methods[v.Sel.Name]; ok && len(cands) > 0 {
			return cands[0]
		}
	case *ast.Ident:
		if fd, ok := ctx.funcs[v.Name]; ok {
			return fd
		}
	}

	return nil
}

func funcKey(fd *ast.FuncDecl) string {
	pos := fd.Name.Name
	if fd.Recv != nil {
		pos = "(recv)." + pos
	}

	return pos
}

// scanTopLevel scans fl's own body (hop 0) for decode signals.
func scanTopLevel(fl funcLike, ctx handlerResolveCtx, label string, formKeys map[string]string) opResolution {
	res := opResolution{Fields: map[string]emuField{}, Found: true, FromHandler: label}
	scanBody(fl, ctx, 0, map[*ast.FuncDecl]bool{}, &res, formKeys)

	return res
}

// scanBody walks fl's body for: (1) a decode call binding a known struct's
// worth of fields, (2) an echo query/path/form param read with a literal
// name, (3) a call whose own return type resolves to a known struct
// (cloudfront's decodeXBody(c) shape), (4) a query-protocol form read keyed
// by op's own SDK field names (formKeys -- see formreads.go), and (5) at
// hop 0 only, one hop of recursion into a *Handler method or bare package
// func it calls directly -- never into h.Backend.X or any other selector
// chain, so backend-internal field names never leak in as false "declared"
// matches.
func scanBody(
	fl funcLike,
	ctx handlerResolveCtx,
	hop int,
	visited map[*ast.FuncDecl]bool,
	res *opResolution,
	formKeys map[string]string,
) {
	if fl.Body == nil {
		return
	}

	bindings := collectLocalBindings(fl, ctx.fset, ctx.structs)
	urlValuesNames := urlValuesParamNames(fl)

	ast.Inspect(fl.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		matchDecodeCall(call, bindings, ctx, res)
		matchQueryParamCall(call, res)
		matchReturnsStructCall(call, ctx, res)
		matchFormReadCall(call, urlValuesNames, formKeys, ctx, res)

		if hop < maxHop {
			matchRecursableCall(call, ctx, hop, visited, res, formKeys)
		}

		return true
	})
}

// matchDecodeCall recognises json.Unmarshal(body, &x) / xml.Unmarshal /
// c.Bind(&x) / readJSON(c, &x) -- any call whose name matches a decode verb
// and has an `&ident` argument bound to a known struct type.
func matchDecodeCall(call *ast.CallExpr, bindings map[string]string, ctx handlerResolveCtx, res *opResolution) {
	if !isDecodeVerb(callName(call.Fun)) {
		return
	}

	for _, arg := range call.Args {
		unary, ok := arg.(*ast.UnaryExpr)
		if !ok || unary.Op != token.AND {
			continue
		}

		id, ok := unwrapExpr(unary.X).(*ast.Ident)
		if !ok {
			continue
		}

		typeName, ok := bindings[id.Name]
		if !ok {
			continue
		}

		addStructFields(typeName, ctx, res)
	}
}

func isDecodeVerb(name string) bool {
	lower := strings.ToLower(name)
	for _, v := range decodeCallVerbs {
		if strings.Contains(lower, v) {
			return true
		}
	}

	return false
}

func callName(fn ast.Expr) string {
	switch v := fn.(type) {
	case *ast.SelectorExpr:
		return v.Sel.Name
	case *ast.Ident:
		return v.Name
	default:
		return ""
	}
}

// matchQueryParamCall harvests `c.QueryParam("embed")`-shaped calls: the
// literal string argument becomes a declared wire field, keyed and named
// identically (no struct backs it, so GoName is left equal to WireName).
func matchQueryParamCall(call *ast.CallExpr, res *opResolution) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !queryParamSelectors[sel.Sel.Name] || len(call.Args) == 0 {
		return
	}

	lit, ok := call.Args[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return
	}

	name, err := strconv.Unquote(lit.Value)
	if err != nil || name == "" {
		return
	}

	res.Fields[normalizeWireName(name)] = emuField{WireName: name, GoName: name}
	res.HasSignal = true
}

// matchReturnsStructCall recognises a call to a package func, or a method on
// the handler receiver itself, whose single declared return type is a known
// struct -- cloudfront's decodeListDistributionsByRealtimeLogConfigBody(c)
// helper, which returns a named local struct with no decode-verb call
// anywhere in the caller at all. Deliberately gated the same way
// matchRecursableCall is gated (bare func, or `h.<method>()` with the
// receiver ident literally "h"): lookupFuncDecl's SelectorExpr branch
// resolves a method by NAME ONLY, ignoring the receiver's actual type, so an
// ungated call to any other selector -- a backend/business-logic call like
// `lambdaBk.UpdateFunctionURLConfig(...)` -- can match a same-named method on
// a completely different receiver whose return type happens to be some other
// known struct (lambda's UpdateFunctionURLConfig backend method returns
// *FunctionURLConfig, the RESPONSE struct, whose InvokeMode field then
// registered as a falsely "declared" REQUEST field and hid the real
// UpdateFunctionUrlConfig.InvokeMode gap end to end -- gopherstack-id70).
func matchReturnsStructCall(call *ast.CallExpr, ctx handlerResolveCtx, res *opResolution) {
	if !isBareOrHandlerCall(call.Fun) {
		return
	}

	fd := lookupFuncDecl(call.Fun, ctx)
	if fd == nil || fd.Type.Results == nil || len(fd.Type.Results.List) == 0 {
		return
	}

	resultType := fd.Type.Results.List[0].Type

	typeName := underlyingIdentType(resultType)
	if typeName == "" {
		if id, ok := resultType.(*ast.Ident); ok {
			typeName = id.Name
		}
	}

	if _, known := ctx.structs[typeName]; !known {
		return
	}

	addStructFields(typeName, ctx, res)
}

// isBareOrHandlerCall reports whether fn is a bare package-function
// reference or a selector call on a receiver ident literally "h" -- the same
// boundary matchRecursableCall enforces, so a struct-returning call can never
// be resolved through an arbitrary receiver (h.Backend.X, a locally-typed
// backend variable, a third-party client, ...) whose method merely shares a
// name with something else in the package.
func isBareOrHandlerCall(fn ast.Expr) bool {
	switch v := fn.(type) {
	case *ast.Ident:
		return true
	case *ast.SelectorExpr:
		recv, ok := v.X.(*ast.Ident)

		return ok && recv.Name == "h"
	default:
		return false
	}
}

// matchRecursableCall follows a call to `h.<method>(...)` (receiver ident
// literally "h", this repo's uniform Handler receiver name) or a bare
// package function, one hop, merging what that callee's own body declares.
// Any other selector chain (h.Backend.X, a third-party client, ...) is
// deliberately never followed -- see the package doc's disclosed blind
// spots for why that boundary matters.
func matchRecursableCall(
	call *ast.CallExpr,
	ctx handlerResolveCtx,
	hop int,
	visited map[*ast.FuncDecl]bool,
	res *opResolution,
	formKeys map[string]string,
) {
	var fd *ast.FuncDecl

	switch fn := call.Fun.(type) {
	case *ast.Ident:
		fd = ctx.funcs[fn.Name]
	case *ast.SelectorExpr:
		recv, isRecvIdent := fn.X.(*ast.Ident)
		if !isRecvIdent || recv.Name != "h" {
			return
		}

		if cands, found := ctx.methods[fn.Sel.Name]; found && len(cands) > 0 {
			fd = cands[0]
		}
	default:
		return
	}

	if fd == nil || fd.Body == nil || visited[fd] {
		return
	}

	visited[fd] = true
	scanBody(fromFuncDecl(fd), ctx, hop+1, visited, res, formKeys)
}

func addStructFields(typeName string, ctx handlerResolveCtx, res *opResolution) {
	def, ok := ctx.structs[typeName]
	if !ok {
		return
	}

	res.StructsUsed = append(res.StructsUsed, typeName)
	res.HasSignal = true

	maps.Copy(res.Fields, fieldMap(def))
}

func fieldMap(def structDef) map[string]emuField {
	out := make(map[string]emuField, len(def.Fields))
	for _, f := range def.Fields {
		out[normalizeWireName(f.WireName)] = f
	}

	return out
}

// findHandlerByName is the name-convention fallback for a service whose
// dispatch shape this scan doesn't recognise at all (a REST-path-keyed
// route table this scan can't statically resolve, ...): search every
// FuncDecl in the package for "handle"+op, then the suffixed variants this
// repo is known to use (handle<Op>Full/Accurate/WithOpts -- see
// cmd/reqfieldscan's package doc, blind spot 3), then this repo's other
// observed conventions -- lowerCamel(op)+"Action" (apigateway's shape) and
// bare lowerCamel(op) with no prefix at all (appsync's shape:
// createGraphqlAPI for CreateGraphqlApi) -- then case-insensitively against
// EITHER "handle"+op or bare op, so a casing quirk in how this repo
// capitalizes an AWS acronym (GraphqlAPI vs GraphqlApi, IPAddress vs
// Ipaddress) never blocks a match cmd/reqfieldscan's own lowerKeyedHandlers
// fallback already relies on for the same reason.
//
// Returns the resolved handler and, only when resolution fell all the way
// through to findHandlerByNameFold's case-insensitive scan, every name that
// scan matched (nil whenever an exact-name candidate above resolved it, or
// nothing matched at all) -- see findHandlerByNameFold for why that second
// value exists at all.
func findHandlerByName(op string, ctx handlerResolveCtx) (*ast.FuncDecl, []string) {
	candidates := []string{
		"handle" + op,
		"handle" + op + "Full",
		"handle" + op + "Accurate",
		"handle" + op + "WithOpts",
		lowerFirst(op) + "Action",
		op + "Action",
		lowerFirst(op),
	}

	for _, name := range candidates {
		if fd := lookupByExactName(name, ctx); fd != nil {
			return fd, nil
		}
	}

	return findHandlerByNameFold(op, ctx)
}

// foldCandidate is one match found by findHandlerByNameFold's
// case-insensitive scan, kept alongside enough of its own shape to apply the
// tie-break rule and, for gopherstack-fr30's census, to be reported back to
// a caller that wants to know when a name was genuinely ambiguous.
type foldCandidate struct {
	fd       *ast.FuncDecl
	name     string
	rank     int
	isMethod bool
}

// findHandlerByNameFold is the last-resort case-insensitive scan behind
// findHandlerByName's exact-name candidates: a casing quirk in how this repo
// capitalizes an AWS acronym (GraphqlAPI vs GraphqlApi, IPAddress vs
// Ipaddress) that none of those exact spellings covers.
//
// It used to return whichever match Go's map iteration produced first --
// RANDOMIZED per process, so a service with 2+ case-insensitive matches for
// the same op resolved a different handler body (and so a different field
// count) from one run to the next (gopherstack-fr30). It now collects EVERY
// case-insensitive match and picks among them by a stated, deterministic
// rule:
//
//  1. prefer a match against "handle"+op over bare op -- "handle"+X is this
//     repo's dominant handler-naming convention; the bare-op convention
//     (apigateway's shape) is already caught, when spelled exactly, by the
//     lowerFirst(op) candidate in findHandlerByName above, so a fold match
//     against bare op is the weaker signal of the two.
//  2. prefer an UNEXPORTED name over an exported one. This is not an
//     arbitrary tie-break: gopherstack-fr30's own census of every fold
//     ambiguity in this repo (177 operations, 26 services) found every
//     single bare-vs-bare collision is the SAME shape -- an exported
//     PascalCase method on a Backend/InMemoryBackend (appsync's
//     `(b *InMemoryBackend) CreateAPI`, s3's `(b *InMemoryBackend)
//     GetBucketACL`) colliding with the real unexported dispatch handler
//     spelled identically but for case (appsync's `(h *Handler) createAPI`,
//     s3's `(h *S3Handler) getBucketACL`). This repo's real handlers are
//     uniformly unexported; picking the exported name here would silently
//     resolve to backend business logic instead of the decode site, in
//     every observed instance.
//  3. prefer a method over a package func -- methods are this repo's
//     overwhelming convention for real handlers.
//  4. prefer the shorter name, then break any remaining tie
//     lexicographically -- both arbitrary but stated, and neither depends on
//     iteration order.
//
// The second return value is every matched name (deduplicated, sorted), for
// a caller that wants to know whether this op's fallback was genuinely
// ambiguous -- more than one candidate is the seventh inherited blind spot
// (a second in-package dispatch table behind colliding names) surfacing in
// practice rather than staying theoretical; see this package's doc comment.
func findHandlerByNameFold(op string, ctx handlerResolveCtx) (*ast.FuncDecl, []string) {
	handleTarget := strings.ToLower("handle" + op)
	bareTarget := strings.ToLower(op)

	var cands []foldCandidate

	for name, fds := range ctx.methods {
		if len(fds) == 0 {
			continue
		}

		if rank, ok := foldRank(name, handleTarget, bareTarget); ok {
			cands = append(cands, foldCandidate{name: name, fd: fds[0], isMethod: true, rank: rank})
		}
	}

	for name, fd := range ctx.funcs {
		if rank, ok := foldRank(name, handleTarget, bareTarget); ok {
			cands = append(cands, foldCandidate{name: name, fd: fd, isMethod: false, rank: rank})
		}
	}

	if len(cands) == 0 {
		return nil, nil
	}

	slices.SortFunc(cands, compareFoldCandidates)

	return cands[0].fd, foldCandidateNames(cands)
}

func foldRank(name, handleTarget, bareTarget string) (int, bool) {
	switch strings.ToLower(name) {
	case handleTarget:
		return 0, true
	case bareTarget:
		return 1, true
	default:
		return 0, false
	}
}

func compareFoldCandidates(a, b foldCandidate) int {
	if a.rank != b.rank {
		return a.rank - b.rank
	}

	if aExp, bExp := ast.IsExported(a.name), ast.IsExported(b.name); aExp != bExp {
		if aExp {
			return 1
		}

		return -1
	}

	if a.isMethod != b.isMethod {
		if a.isMethod {
			return -1
		}

		return 1
	}

	if len(a.name) != len(b.name) {
		return len(a.name) - len(b.name)
	}

	return strings.Compare(a.name, b.name)
}

func foldCandidateNames(cands []foldCandidate) []string {
	seen := map[string]bool{}

	var names []string

	for _, c := range cands {
		if seen[c.name] {
			continue
		}

		seen[c.name] = true

		names = append(names, c.name)
	}

	sort.Strings(names)

	return names
}

func lookupByExactName(name string, ctx handlerResolveCtx) *ast.FuncDecl {
	if cands, ok := ctx.methods[name]; ok && len(cands) > 0 {
		return cands[0]
	}

	if fd, ok := ctx.funcs[name]; ok {
		return fd
	}

	return nil
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}

	return strings.ToLower(s[:1]) + s[1:]
}
