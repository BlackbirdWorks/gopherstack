package main

import (
	"go/ast"
	"go/token"
	"maps"
	"regexp"
	"strconv"
)

// pkgErrors is the stdlib "errors" package identifier this file and emit.go
// both match against (errors.Is, errors.New) when recognizing a call's
// package qualifier.
const pkgErrors = "errors"

// codeShapeRe separates an AWS-style error code ("ResourceNotFoundException",
// "ConflictException") from every other string literal a mapper branch's
// body might contain (a human-readable message, a header name) --
// PascalCase-or-SCREAMING, no spaces/punctuation, at least 3 characters.
// Identical filter to cmd/errcodeaudit/extract.go's codeShapeRe.
var codeShapeRe = regexp.MustCompile(`^[A-Z][A-Za-z0-9]{2,}$`)

func looksLikeCode(s string) bool {
	return codeShapeRe.MatchString(s)
}

// classifiers is the package-wide map from an error-emission SOURCE (a
// sentinel variable's name, or a constructor function's name) to the wire
// code it renders as -- built once per service and shared across every
// operation's emission walk (emit.go). See this package's doc comment for
// why this table, not a per-call-site literal, is the right ground truth
// for most of this repo's real shape: services/bedrock and services/iot
// emit a SENTINEL (ErrAlreadyExists, ErrThingNotFound, ...), never a code
// literal, at the actual bug site -- the literal only ever appears once,
// in a shared mapper function every operation in the package funnels
// through.
type classifiers struct {
	Sentinels    map[string]string
	ByFunc       map[string]map[string]string
	Funcs        map[string]string
	Overrides    map[string]overrideFunc
	Constructors []*ast.FuncDecl
}

// overrideFunc is a helper like services/iot's respondAsInvalidRequest(c,
// err, sentinel error) -- a function that takes the COMPARISON sentinel as
// its OWN parameter rather than a fixed identifier, and emits Code
// specifically when errors.Is(err, thatParam) holds. ParamIndex is the
// flattened parameter position of the comparison argument, so a call site
// passing a literal sentinel there can be resolved without knowing the
// helper's implementation.
type overrideFunc struct {
	Code       string
	ParamIndex int
}

// buildClassifiers finds the package's own errors.Is-to-code mapper(s)
// (sentinelCodes) and propagates through one hop of constructor-function
// indirection (funcCodes) -- services/networkmanager's real shape:
// notFoundError(...) never mentions a code literal itself, it builds
// &apiError{cause: errNotFoundSentinel, ...}, and errNotFoundSentinel is
// what the package's real mapper (classifyError) associates with
// "ResourceNotFoundException". A constructor that wraps ANOTHER constructor,
// rather than a sentinel directly, is not resolved -- disclosed in the
// package doc as a blind spot, matching this repo's other tools' one-hop
// discipline.
//
// opNames excludes every function whose OWN name matches a real ground-truth
// operation name from constructor candidacy -- an ordinary backend method
// (`func (b *Backend) DeleteThing(id string) error`) also returns bare
// `error` and would otherwise be misread as a small error-builder helper,
// double-counting its own hop-1 emission under a second mechanism AND, worse,
// bypassing emit.go's per-op override suppression entirely (that helper's
// code is baked in at buildClassifiers time, before any op-specific override
// is known). A real constructor (notFoundError, validationError,
// conflictError) is never itself named after an AWS operation; a backend
// method implementing one always is -- confirmed as a false positive on a
// synthetic CancelJob-shaped fixture during this tool's own test-writing.
func buildClassifiers(idx *pkgIndex, opNames map[string]bool) *classifiers {
	byFunc := funcSentinelCodes(idx)
	flat := flattenSentinelCodes(byFunc)

	c := &classifiers{
		Sentinels: flat,
		ByFunc:    byFunc,
		Overrides: detectOverrideFuncs(idx),
	}

	for _, f := range idx.Files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil || opNames[fd.Name.Name] || !returnsOnlyError(fd) {
				continue
			}

			c.Constructors = append(c.Constructors, fd)
		}
	}

	c.Funcs = resolveConstructorCodes(c.Constructors, flat)

	return c
}

func resolveConstructorCodes(candidates []*ast.FuncDecl, sentinels map[string]string) map[string]string {
	out := map[string]string{}

	for _, fd := range candidates {
		if code, found := constructorCode(fd, sentinels); found {
			out[fd.Name.Name] = code
		}
	}

	return out
}

// funcSentinelCodes scans every switch statement and if-statement in the
// package for an errors.Is(<err>, <sentinel>) condition whose branch body
// contains a code-shaped literal, associating the sentinel's own name with
// that code -- SCOPED per enclosing mapper function (gopherstack-0yva),
// unlike a single package-wide table: services/eks's real shape has two
// mapper functions, handleError and handleTagError, that both branch on the
// SAME identifier ErrNotFound to DIFFERENT codes (ResourceNotFoundException
// vs NotFoundException, a real, deliberate difference between the two
// tagging-API families' own deserializers), and a flat table keyed by
// identifier alone can only record one winner -- silently misattributing
// every operation reachable through the LOSING mapper. Every switch/if found
// inside one FuncDecl's body contributes to THAT function's own table;
// flattenSentinelCodes below builds the package-wide fallback used only when
// a call site's own mapper cannot be determined (emit.go's
// localMapperScope).
func funcSentinelCodes(idx *pkgIndex) map[string]map[string]string {
	out := map[string]map[string]string{}

	collect := func(name string, body *ast.BlockStmt) {
		if body == nil {
			return
		}

		table := map[string]string{}

		ast.Inspect(body, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.SwitchStmt:
				addSwitchSentinelCodes(v, idx, table)
			case *ast.IfStmt:
				addIfSentinelCodes(v, idx, table)
			}

			return true
		})

		if len(table) == 0 {
			return
		}

		if existing, ok := out[name]; ok {
			maps.Copy(existing, table)
		} else {
			out[name] = table
		}
	}

	for _, fd := range idx.Funcs {
		collect(fd.Name.Name, fd.Body)
	}

	for name, fds := range idx.Methods {
		for _, fd := range fds {
			collect(name, fd.Body)
		}
	}

	return out
}

// flattenSentinelCodes merges every mapper function's own table (built by
// funcSentinelCodes) into one package-wide fallback -- used only when an
// operation's own call path cannot be pinned to a specific mapper
// (emit.go's localMapperScope finds none reachable). When two DIFFERENT
// mapper functions map the SAME identifier to DIFFERENT codes, that
// identifier is a COLLISION: dropped from the flat map entirely, never
// silently resolved to whichever mapper this scan happened to visit
// first -- this is deterministic regardless of map iteration order, because
// any two DIFFERING values for the same identifier mark it a collision
// however the functions are visited (verified in
// TestFlattenSentinelCodes_CollisionOmitted). gopherstack-0yva's other,
// preferred resolution -- resolving through the mapper an operation's OWN
// call path actually reaches -- lives in emit.go's localMapperScope, and
// wins over this fallback whenever it finds one.
func flattenSentinelCodes(byFunc map[string]map[string]string) map[string]string {
	out := map[string]string{}
	collide := map[string]bool{}

	for _, table := range byFunc {
		for ident, code := range table {
			prev, seen := out[ident]
			if !seen {
				out[ident] = code

				continue
			}

			if prev != code {
				collide[ident] = true
			}
		}
	}

	for ident := range collide {
		delete(out, ident)
	}

	return out
}

// sentinelCodes is funcSentinelCodes's flat, package-wide view -- kept as
// its own entry point because it is the shape most of this file's own
// resolution (constructorCode's default candidacy, this package's tests)
// needs, and because a package with exactly one mapper (the common case)
// never triggers the ambiguity flattenSentinelCodes exists to catch.
func sentinelCodes(idx *pkgIndex) map[string]string {
	return flattenSentinelCodes(funcSentinelCodes(idx))
}

func addSwitchSentinelCodes(sw *ast.SwitchStmt, idx *pkgIndex, out map[string]string) {
	for _, stmt := range sw.Body.List {
		cc, ok := stmt.(*ast.CaseClause)
		if !ok {
			continue
		}

		names := map[string]bool{}
		for _, expr := range cc.List {
			collectErrorsIsSentinels(expr, idx.Sentinels, names)
		}

		if len(names) == 0 {
			continue
		}

		code, found := firstCodeLiteral(&ast.BlockStmt{List: cc.Body}, idx, 0)
		if !found {
			continue
		}

		for name := range names {
			out[name] = code
		}
	}
}

func addIfSentinelCodes(ifs *ast.IfStmt, idx *pkgIndex, out map[string]string) {
	names := map[string]bool{}
	collectErrorsIsSentinels(ifs.Cond, idx.Sentinels, names)

	if len(names) == 0 || ifs.Body == nil {
		return
	}

	code, found := firstCodeLiteral(ifs.Body, idx, 0)
	if !found {
		return
	}

	for name := range names {
		out[name] = code
	}
}

// collectErrorsIsSentinels finds every errors.Is(<x>, <sentinel>) call
// reachable in expr (an entire case-list entry, or an if's -- possibly
// &&/||-combined -- condition) whose second argument is a known sentinel
// identifier.
func collectErrorsIsSentinels(expr ast.Expr, sentinels map[string]bool, out map[string]bool) {
	ast.Inspect(expr, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}

		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Is" {
			return true
		}

		pkgIdent, ok := sel.X.(*ast.Ident)
		if !ok || pkgIdent.Name != pkgErrors {
			return true
		}

		id, ok := call.Args[1].(*ast.Ident)
		if ok && sentinels[id.Name] {
			out[id.Name] = true
		}

		return true
	})
}

// maxLiteralHop bounds how far firstCodeLiteral follows a mapper branch's
// own call into another package-local function before giving up --
// services/iot's real shape needs exactly one: writeIoTError's
// ResourceNotFoundException branch is `return respondNotFound(c,
// err.Error())`, and the literal "ResourceNotFoundException" lives inside
// respondNotFound's OWN body, not the branch that calls it.
const maxLiteralHop = 1

// firstCodeLiteral returns the first code-shaped value found anywhere in n,
// in AST traversal order: a direct string literal, a bare identifier
// resolving to a package-level string const (services/iot's
// errTypeInvalidRequest), or -- up to maxLiteralHop -- a call to a
// package-local function/method, recursed into for the same two shapes.
func firstCodeLiteral(n ast.Node, idx *pkgIndex, hop int) (string, bool) {
	var found string

	var ok bool

	ast.Inspect(n, func(node ast.Node) bool {
		if ok {
			return false
		}

		if code, matched := codeLiteralAtNode(node, idx, hop); matched {
			found, ok = code, true

			return false
		}

		return true
	})

	return found, ok
}

// codeLiteralAtNode checks node itself (not its children -- ast.Inspect's
// own traversal covers those) for one of firstCodeLiteral's three shapes.
func codeLiteralAtNode(node ast.Node, idx *pkgIndex, hop int) (string, bool) {
	switch v := node.(type) {
	case *ast.BasicLit:
		return literalCode(v)
	case *ast.Ident:
		if code, matched := idx.PkgConsts[v.Name]; matched && looksLikeCode(code) {
			return code, true
		}
	case *ast.CallExpr:
		if hop < maxLiteralHop {
			return firstCalleeCodeLiteral(v.Fun, idx, hop)
		}
	}

	return "", false
}

func firstCalleeCodeLiteral(fn ast.Expr, idx *pkgIndex, hop int) (string, bool) {
	for _, fd := range calleeFuncDecls(fn, idx) {
		if fd.Body == nil {
			continue
		}

		if code, matched := firstCodeLiteral(fd.Body, idx, hop+1); matched {
			return code, true
		}
	}

	return "", false
}

func literalCode(lit *ast.BasicLit) (string, bool) {
	if lit.Kind != token.STRING {
		return "", false
	}

	v, err := strconv.Unquote(lit.Value)
	if err != nil || !looksLikeCode(v) {
		return "", false
	}

	return v, true
}

// returnsOnlyError reports whether fd declares EXACTLY one result, the
// built-in `error` type -- the shape every constructor in this repo's
// mapper-adjacent files (notFoundError, validationError, conflictError, ...)
// shares. Deliberately narrower than "last result is error": an ordinary
// backend method (`func (b *InMemoryBackend) CancelJob(...) (*Job, error)`)
// also ends in error but is not a constructor, and treating it as one
// double-counted a finding through both the "constructor classifier" and
// "sentinel reference" mechanisms during this tool's own validation pass,
// confirmed on services/iot's CancelJob before this narrowing.
func returnsOnlyError(fd *ast.FuncDecl) bool {
	if fd.Type.Results == nil || len(fd.Type.Results.List) != 1 {
		return false
	}

	field := fd.Type.Results.List[0]
	if len(field.Names) > 1 {
		return false
	}

	id, ok := field.Type.(*ast.Ident)

	return ok && id.Name == "error"
}

// constructorCode inspects fd's own return statements (including nested
// composite-literal field values and fmt.Errorf's %w slot) for a bare
// reference to a known sentinel, one hop of indirection past sentinelCodes
// itself.
func constructorCode(fd *ast.FuncDecl, sentinelCodes map[string]string) (string, bool) {
	var found string

	var ok bool

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if ok {
			return false
		}

		ret, isRet := n.(*ast.ReturnStmt)
		if !isRet {
			return true
		}

		for _, result := range ret.Results {
			if code, matched := sentinelRefCode(result, sentinelCodes); matched {
				found, ok = code, true

				return false
			}
		}

		return true
	})

	return found, ok
}

// sentinelRefCode reports whether expr is, or directly carries, a bare
// reference to a known sentinel: the expression itself, a unary `&`, a
// composite literal's own field values (services/networkmanager's
// `&apiError{cause: errNotFoundSentinel, ...}` shape, recursed into nested
// composite literals), or an argument to fmt.Errorf specifically (the
// `fmt.Errorf("%w: ...", ErrX, ...)` wrap idiom). It deliberately does NOT
// descend into the arguments of any OTHER call: services/iot's real
// post-fix shape, `respondAsInvalidRequest(c, err, ErrInvalidStateTransition)`,
// passes a sentinel as a COMPARISON target (errors.Is(err, thatParam)
// inside the callee), not as the value being emitted -- a confirmed false
// positive during this tool's own validation pass before this exclusion was
// added (classifiers.go's own doc comment records the concrete instance).
func sentinelRefCode(expr ast.Expr, sentinelCodes map[string]string) (string, bool) {
	switch e := expr.(type) {
	case *ast.Ident:
		if code, ok := sentinelCodes[e.Name]; ok {
			return code, true
		}
	case *ast.UnaryExpr:
		if e.Op == token.AND {
			return sentinelRefCode(e.X, sentinelCodes)
		}
	case *ast.CompositeLit:
		return sentinelRefCodeInElts(e.Elts, sentinelCodes)
	case *ast.CallExpr:
		if isFmtErrorfCall(e) {
			return sentinelRefCodeInArgs(e.Args, sentinelCodes)
		}
	}

	return "", false
}

func sentinelRefCodeInElts(elts []ast.Expr, sentinelCodes map[string]string) (string, bool) {
	for _, elt := range elts {
		v := elt
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			v = kv.Value
		}

		if code, ok := sentinelRefCode(v, sentinelCodes); ok {
			return code, true
		}
	}

	return "", false
}

func sentinelRefCodeInArgs(args []ast.Expr, sentinelCodes map[string]string) (string, bool) {
	for _, a := range args {
		if code, ok := sentinelRefCode(a, sentinelCodes); ok {
			return code, true
		}
	}

	return "", false
}

func isFmtErrorfCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	pkgIdent, ok := sel.X.(*ast.Ident)

	return ok && pkgIdent.Name == "fmt" && sel.Sel.Name == "Errorf"
}

// detectOverrideFuncs finds every package-level function shaped like
// services/iot's respondAsInvalidRequest(c, err, sentinel error): it takes
// the comparison sentinel as ITS OWN parameter (rather than a fixed package
// identifier) and, in an `if errors.Is(<x>, <thatParam>) { ... }` branch,
// emits a fixed code. Detecting this matters for PRECISION, not recall: a
// service that only ever uses such a helper post-fix (this repo's own
// pattern for the fix commits this tool validates against) would otherwise
// have its call sites misread as still emitting the PRE-fix, general
// mapper's code -- confirmed as a false positive on services/iot's
// (post-fix) CancelJob/DeleteThing during this tool's own validation pass,
// before this detector was added.
func detectOverrideFuncs(idx *pkgIndex) map[string]overrideFunc {
	out := map[string]overrideFunc{}

	for _, f := range idx.Files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			params := flattenParamNames(fd.Type.Params)
			if ov, found := findOverrideShape(fd.Body, idx, params); found {
				out[fd.Name.Name] = ov
			}
		}
	}

	return out
}

func flattenParamNames(fl *ast.FieldList) []string {
	if fl == nil {
		return nil
	}

	var out []string

	for _, f := range fl.List {
		if len(f.Names) == 0 {
			out = append(out, "")

			continue
		}

		for _, n := range f.Names {
			out = append(out, n.Name)
		}
	}

	return out
}

func findOverrideShape(body *ast.BlockStmt, idx *pkgIndex, params []string) (overrideFunc, bool) {
	var result overrideFunc

	var found bool

	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}

		ifs, ok := n.(*ast.IfStmt)
		if !ok || ifs.Body == nil {
			return true
		}

		paramIdx, condOK := errorsIsParamIndex(ifs.Cond, params)
		if !condOK {
			return true
		}

		code, codeOK := firstCodeLiteral(ifs.Body, idx, 0)
		if !codeOK {
			return true
		}

		result, found = overrideFunc{ParamIndex: paramIdx, Code: code}, true

		return false
	})

	return result, found
}

// errorsIsParamIndex reports whether cond contains an errors.Is(<x>, <y>)
// call where y names one of fd's own parameters, returning that
// parameter's flattened index.
func errorsIsParamIndex(cond ast.Expr, params []string) (int, bool) {
	var result int

	var found bool

	ast.Inspect(cond, func(n ast.Node) bool {
		if found {
			return false
		}

		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}

		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Is" {
			return true
		}

		pkgIdent, ok := sel.X.(*ast.Ident)
		if !ok || pkgIdent.Name != pkgErrors {
			return true
		}

		id, ok := call.Args[1].(*ast.Ident)
		if !ok {
			return true
		}

		for i, p := range params {
			if p == id.Name {
				result, found = i, true

				return false
			}
		}

		return true
	})

	return result, found
}
