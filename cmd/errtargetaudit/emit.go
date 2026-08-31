package main

import (
	"go/ast"
	"go/token"
	"maps"
	"strconv"
	"strings"
)

// maxEmitHop bounds how far this scan follows a resolved handler's own
// calls into other package-local functions before giving up: hop 0 is the
// resolved root itself, hop 1 is any function or method it calls directly
// -- ANY receiver, not only this repo's uniform "h" Handler receiver name,
// because the real bug site in three of the four validated commits
// (d7149d0f8, 19f3d65f0) sits in the BACKEND method a handler calls, one
// hop away, never in the handler itself. This is deliberately WIDER than
// cmd/reqfieldscan/cmd/reqfielddiff's own single-hop discipline, which
// restricts recursion to "h.<Method>" specifically to keep a backend's
// internal FIELD names from leaking in as false "declared wire field"
// matches -- that hazard does not apply here: a backend method's own
// sentinel-error return IS exactly the site this tool exists to find.
const maxEmitHop = 1

// emission is one candidate error-code emission found reachable from an
// operation's resolved root(s).
type emission struct {
	Code      string
	Mechanism string
	Pos       token.Pos
}

// walkOpEmissions finds every emission reachable from roots (hop 0 each
// root's own body, hop 1 any function/method call it makes directly),
// deduplicated by source position. Before walking, it looks for an
// override-mapper call (cls.Overrides) at hop 0 ONLY -- the handler's own
// body, where a call like services/iot's `respondAsInvalidRequest(c, err,
// ErrInvalidStateTransition)` sits -- and builds a PER-OP effective sentinel
// table so a hop-1 backend return of that same sentinel resolves to the
// override's code rather than the general mapper's, matching what this
// operation's real response actually renders.
func walkOpEmissions(roots []opRoot, idx *pkgIndex, cls *classifiers) []emission {
	effective := effectiveClassifiers(roots, idx, cls)

	visited := map[*ast.BlockStmt]bool{}

	out := make([]emission, 0, len(roots))

	for _, r := range roots {
		out = append(out, scanBodyEmissions(r.Body, idx, effective, 0, visited)...)
	}

	return dedupEmissions(out)
}

func effectiveClassifiers(hop0Roots []opRoot, idx *pkgIndex, cls *classifiers) *classifiers {
	overrides := localSentinelOverrides(hop0Roots, idx, cls.Overrides)
	if len(overrides) == 0 {
		return cls
	}

	sentinels := make(map[string]string, len(cls.Sentinels)+len(overrides))
	maps.Copy(sentinels, cls.Sentinels)
	maps.Copy(sentinels, overrides)

	return &classifiers{Sentinels: sentinels, Funcs: cls.Funcs, Overrides: cls.Overrides}
}

// localSentinelOverrides scans hop0Roots' OWN bodies (never recursing) for
// a call to a known override function, reading the actual sentinel argument
// passed at that call site.
func localSentinelOverrides(hop0Roots []opRoot, idx *pkgIndex, overrides map[string]overrideFunc) map[string]string {
	out := map[string]string{}

	for _, r := range hop0Roots {
		if r.Body == nil {
			continue
		}

		ast.Inspect(r.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			name, ok := calleeSimpleName(call.Fun)
			if !ok {
				return true
			}

			ov, known := overrides[name]
			if !known || ov.ParamIndex >= len(call.Args) {
				return true
			}

			id, ok := call.Args[ov.ParamIndex].(*ast.Ident)
			if ok && idx.Sentinels[id.Name] {
				out[id.Name] = ov.Code
			}

			return true
		})
	}

	return out
}

func dedupEmissions(in []emission) []emission {
	seen := map[token.Pos]bool{}

	var out []emission

	for _, e := range in {
		if seen[e.Pos] {
			continue
		}

		seen[e.Pos] = true

		out = append(out, e)
	}

	return out
}

func scanBodyEmissions(
	body *ast.BlockStmt,
	idx *pkgIndex,
	cls *classifiers,
	hop int,
	visited map[*ast.BlockStmt]bool,
) []emission {
	if body == nil || visited[body] {
		return nil
	}

	visited[body] = true

	var out []emission

	ast.Inspect(body, func(n ast.Node) bool {
		out = append(out, nodeEmissions(n, cls)...)

		if hop < maxEmitHop {
			out = append(out, recurseCallEmissions(n, idx, cls, hop, visited)...)
		}

		return true
	})

	return out
}

func nodeEmissions(n ast.Node, cls *classifiers) []emission {
	switch v := n.(type) {
	case *ast.ReturnStmt:
		return returnStmtEmissions(v, cls)
	case *ast.CallExpr:
		return callExprEmissions(v, cls)
	case *ast.CompositeLit:
		return compositeLitEmissions(v)
	case *ast.AssignStmt:
		return assignEmissions(v)
	case *ast.GenDecl:
		return genDeclEmissions(v)
	default:
		return nil
	}
}

// returnStmtEmissions catches a bare sentinel return (`return ErrX` /
// `return nil, ErrX`) and a wrapped one (`return fmt.Errorf("%w: ...", ErrX,
// ...)`) uniformly, via the same deep sentinel scan classifiers.go uses to
// resolve a constructor function's own code.
func returnStmtEmissions(ret *ast.ReturnStmt, cls *classifiers) []emission {
	var out []emission

	for _, res := range ret.Results {
		if code, ok := sentinelRefCode(res, cls.Sentinels); ok {
			out = append(out, emission{Code: code, Mechanism: "sentinel reference", Pos: res.Pos()})
		}
	}

	return out
}

// callExprEmissions catches a call to a known constructor classifier
// (services/networkmanager's notFoundError/validationError shape) and the
// direct-literal mechanisms this repo also uses outside the sentinel-mapper
// pattern (awserr.New/Newf).
func callExprEmissions(call *ast.CallExpr, cls *classifiers) []emission {
	var out []emission

	if name, ok := calleeSimpleName(call.Fun); ok {
		if code, known := cls.Funcs[name]; known {
			out = append(out, emission{Code: code, Mechanism: "constructor classifier: " + name, Pos: call.Pos()})
		}
	}

	out = append(out, awserrLiteralEmissions(call)...)

	return out
}

func calleeSimpleName(fn ast.Expr) (string, bool) {
	switch v := fn.(type) {
	case *ast.Ident:
		return v.Name, true
	case *ast.SelectorExpr:
		return v.Sel.Name, true
	default:
		return "", false
	}
}

// awserrLiteralEmissions covers services/ecs's own direct mechanism:
// awserr.New("Code", sentinel) / awserr.Newf("Code", format, args...) and
// stdlib errors.New("Code") where the sentinel's message IS the code --
// cmd/errcodeaudit's mechAwserrNew/mechStdlibErr, reimplemented narrowly (no
// sink-position table, see this package's doc comment for what that costs).
func awserrLiteralEmissions(call *ast.CallExpr) []emission {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}

	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok {
		return nil
	}

	switch {
	case pkgIdent.Name == "awserr" && (sel.Sel.Name == "New" || sel.Sel.Name == "Newf"):
		return literalArgEmissions(call.Args, 1, "awserr."+sel.Sel.Name+" arg")
	case pkgIdent.Name == pkgErrors && sel.Sel.Name == "New":
		return literalArgEmissions(call.Args, len(call.Args), "errors.New arg")
	default:
		return nil
	}
}

func literalArgEmissions(args []ast.Expr, limit int, mech string) []emission {
	var out []emission

	for i, arg := range args {
		if i >= limit {
			break
		}

		lit, ok := arg.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}

		v, err := strconv.Unquote(lit.Value)
		if err != nil || !looksLikeCode(v) {
			continue
		}

		out = append(out, emission{Code: v, Mechanism: mech, Pos: lit.Pos()})
	}

	return out
}

// compositeLitEmissions covers a mapping-table row: a struct/map composite
// literal's Code/Type/ErrorCode-labeled field holding a code-shaped literal
// -- services/iam and services/ecs's own mechanism, narrowed to keyed
// elements only (no positional-field struct-order resolution, unlike
// cmd/errcodeaudit's fuller version -- see this package's doc for the cost).
func compositeLitEmissions(cl *ast.CompositeLit) []emission {
	var out []emission

	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		id, ok := kv.Key.(*ast.Ident)
		if !ok || !isCodeFieldLabel(id.Name) {
			continue
		}

		lit, ok := kv.Value.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}

		v, err := strconv.Unquote(lit.Value)
		if err != nil || !looksLikeCode(v) {
			continue
		}

		out = append(out, emission{Code: v, Mechanism: "composite literal field: " + id.Name, Pos: lit.Pos()})
	}

	return out
}

// isCodeFieldLabel deliberately excludes a bare "Code" label: this repo's
// AWS-shaped batch operations (BatchDeleteXError{JobIdentifier, Code,
// Message}) legitimately carry a per-ITEM result code as part of a 200 OK
// response, not a wire error envelope -- a confirmed false positive on
// services/bedrock's BatchDeleteAdvancedPromptOptimizationJob during this
// tool's own validation pass, before this narrowing. "ErrorCode" and "Type"
// (the classic AWS Query <Error><Type>Sender</Type></Error> label, and
// services/iam/services/ecs's own field name) are narrow enough in practice
// that neither has produced that failure mode.
func isCodeFieldLabel(name string) bool {
	lower := strings.ToLower(name)

	return lower == "errorcode" || lower == "type"
}

// assignEmissions/genDeclEmissions cover `code := "ValidationError"` /
// `const errCodeValidation = "ValidationError"` -- a code-shaped literal
// assigned to an identifier whose own name marks it as an error code,
// services/cloudformation's mechanism.
func assignEmissions(as *ast.AssignStmt) []emission {
	if len(as.Lhs) != len(as.Rhs) {
		return nil
	}

	var out []emission

	for i, lhs := range as.Lhs {
		id, ok := lhs.(*ast.Ident)
		if !ok || !looksLikeCodeVarName(id.Name) {
			continue
		}

		if e, found := codeLitEmission(as.Rhs[i], "code-named var"); found {
			out = append(out, e)
		}
	}

	return out
}

func genDeclEmissions(gd *ast.GenDecl) []emission {
	if gd.Tok != token.CONST && gd.Tok != token.VAR {
		return nil
	}

	var out []emission

	for _, spec := range gd.Specs {
		vs, ok := spec.(*ast.ValueSpec)
		if !ok || len(vs.Names) != len(vs.Values) {
			continue
		}

		for i, name := range vs.Names {
			if !looksLikeCodeVarName(name.Name) {
				continue
			}

			if e, found := codeLitEmission(vs.Values[i], "code-named const/var"); found {
				out = append(out, e)
			}
		}
	}

	return out
}

func codeLitEmission(expr ast.Expr, mech string) (emission, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return emission{}, false
	}

	v, err := strconv.Unquote(lit.Value)
	if err != nil || !looksLikeCode(v) {
		return emission{}, false
	}

	return emission{Code: v, Mechanism: mech, Pos: lit.Pos()}, true
}

// looksLikeCodeVarName mirrors cmd/errcodeaudit/extract.go's function of the
// same name: a name starting with "code", "errtype"/"errortype", or
// containing both "err" and "code" -- excluding a "key"/"field" prefix,
// this repo's own convention for a wire KEY-NAME constant rather than a
// code value.
func looksLikeCodeVarName(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "key") || strings.HasPrefix(lower, "field") {
		return false
	}

	if strings.HasPrefix(lower, "code") || strings.HasPrefix(lower, "errtype") ||
		strings.HasPrefix(lower, "errortype") {
		return true
	}

	return strings.Contains(lower, "err") && strings.Contains(lower, "code")
}

func recurseCallEmissions(
	n ast.Node,
	idx *pkgIndex,
	cls *classifiers,
	hop int,
	visited map[*ast.BlockStmt]bool,
) []emission {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return nil
	}

	var out []emission

	for _, fd := range calleeFuncDecls(call.Fun, idx) {
		out = append(out, scanBodyEmissions(fd.Body, idx, cls, hop+1, visited)...)
	}

	return out
}

func calleeFuncDecls(fn ast.Expr, idx *pkgIndex) []*ast.FuncDecl {
	switch v := fn.(type) {
	case *ast.SelectorExpr:
		return idx.Methods[v.Sel.Name]
	case *ast.Ident:
		if fd, ok := idx.Funcs[v.Name]; ok {
			return []*ast.FuncDecl{fd}
		}
	}

	return nil
}
