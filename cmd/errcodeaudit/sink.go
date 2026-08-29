package main

import (
	"go/ast"
	"strings"
)

// The lowercased field/key labels narrowFieldNameMatches and
// extract.go's compositeKeyMatches/isExactCodeLabel treat as an
// error-code/type discriminator.
const (
	labelCode      = "code"
	labelErrorCode = "errorcode"
	labelType      = "type"
	labelErrType   = "errtype"
	labelErrorType = "errortype"
	labelWireType  = "__type"
)

// paramInfo is one flattened function/method parameter (a `status, code
// string` group expands to two entries).
type paramInfo struct {
	name     string
	isString bool
}

type errFuncInfo struct {
	body   *ast.BlockStmt
	params []paramInfo
}

// buildSinkPositions finds, for every "...Error"-suffixed function/method
// declared directly in dir's files, which of its string parameter
// POSITIONS are actually written into a code-shaped struct field somewhere
// in its own body -- directly (round 1: a composite literal keyed "Code",
// or "Type"/"ErrType"/"ErrorType" on a struct whose own type name contains
// "Error") or transitively (round 2: passed at a round-1 sink position into
// another such function this same pass classified).
//
// This is what separates services/lambda's writeError(status, errType,
// message) -- errType lands in &Error{Type: errType}, so writeError's
// position 2 is a real sink -- and services/cloudformation's xmlError(c,
// code, message) -- code lands in xmlErrBody{Code: code} -- from e.g.
// services/amplify's handleBackendError(ctx, c, "CreateApp", err): its
// action-name parameter never reaches any such field (the real code comes
// from classifying err, not from that parameter), so it is never
// classified as a sink. Confirmed live: without this check,
// "CreateApp"/"GetApp"/"ListApps"/... (an AWS *operation* name, not an
// error code, but just as PascalCase-shaped) were the single largest
// confident-tier false-positive source on this tool's first repo-wide
// pass -- 457 of 806 confident hits came from unfiltered "...Error"-suffixed
// call arguments before this registry existed.
//
// Two rounds, never more, matching this tool's single-hop discipline
// elsewhere (cmd/enumcheck's own same-package-const resolution is exactly
// one hop too).
func buildSinkPositions(files []*ast.File) map[string]map[int]bool {
	funcs := collectErrFuncs(files)
	sinks := map[string]map[int]bool{}

	for name, fi := range funcs {
		markDirectSinks(name, fi, sinks)
	}

	for name, fi := range funcs {
		markTransitiveSinks(name, fi, sinks)
	}

	return sinks
}

func collectErrFuncs(files []*ast.File) map[string]*errFuncInfo {
	out := map[string]*errFuncInfo{}

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil || fd.Type.Params == nil {
				continue
			}

			if !strings.HasSuffix(fd.Name.Name, "Error") {
				continue
			}

			out[fd.Name.Name] = &errFuncInfo{body: fd.Body, params: flattenParams(fd.Type.Params)}
		}
	}

	return out
}

func flattenParams(fl *ast.FieldList) []paramInfo {
	var out []paramInfo

	for _, field := range fl.List {
		isString := isStringType(field.Type)

		if len(field.Names) == 0 {
			out = append(out, paramInfo{isString: isString})

			continue
		}

		for _, id := range field.Names {
			out = append(out, paramInfo{name: id.Name, isString: isString})
		}
	}

	return out
}

func isStringType(expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)

	return ok && id.Name == "string"
}

func markDirectSinks(name string, fi *errFuncInfo, sinks map[string]map[int]bool) {
	paramIndex := stringParamIndex(fi.params)

	ast.Inspect(fi.body, func(n ast.Node) bool {
		cl, isCL := n.(*ast.CompositeLit)
		if !isCL {
			return true
		}

		markCompositeLitSinks(cl, paramIndex, name, sinks)

		return true
	})
}

// stringParamIndex maps each named string parameter's own name to its
// position in the flattened parameter list.
func stringParamIndex(params []paramInfo) map[string]int {
	idx := map[string]int{}

	for i, p := range params {
		if p.isString && p.name != "" && p.name != "_" {
			idx[p.name] = i
		}
	}

	return idx
}

// markCompositeLitSinks marks fi's caller-visible sink positions for every
// keyed element of cl whose field name matches narrowFieldNameMatches and
// whose value is a parameter identifier.
func markCompositeLitSinks(
	cl *ast.CompositeLit,
	paramIndex map[string]int,
	name string,
	sinks map[string]map[int]bool,
) {
	litTypeName := compositeLitTypeName(cl.Type)

	for _, elt := range cl.Elts {
		kv, keyed := elt.(*ast.KeyValueExpr)
		if !keyed {
			continue
		}

		key, isIdentKey := kv.Key.(*ast.Ident)
		if !isIdentKey || !narrowFieldNameMatches(key.Name, litTypeName) {
			continue
		}

		valID, isIdentVal := kv.Value.(*ast.Ident)
		if !isIdentVal {
			continue
		}

		if idx, known := paramIndex[valID.Name]; known {
			markSink(sinks, name, idx)
		}
	}
}

func markTransitiveSinks(name string, fi *errFuncInfo, sinks map[string]map[int]bool) {
	paramIndex := stringParamIndex(fi.params)

	ast.Inspect(fi.body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		calleeName := calleeIdentName(call.Fun)

		calleeSinks, known := sinks[calleeName]
		if !known {
			return true
		}

		for i, arg := range call.Args {
			if !calleeSinks[i] {
				continue
			}

			id, isID := arg.(*ast.Ident)
			if !isID {
				continue
			}

			if idx, paramKnown := paramIndex[id.Name]; paramKnown {
				markSink(sinks, name, idx)
			}
		}

		return true
	})
}

func calleeIdentName(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	default:
		return ""
	}
}

func markSink(sinks map[string]map[int]bool, name string, idx int) {
	if sinks[name] == nil {
		sinks[name] = map[int]bool{}
	}

	sinks[name][idx] = true
}

// narrowFieldNameMatches reports whether a struct field name marks its
// value as an error-code discriminator. The exact names "ErrorCode" and
// "ErrorType" are unambiguous enough to trust unconditionally (confirmed
// live: services/xray's unprocessedSegment{ErrorCode: "InvalidSegment"} --
// a locally function-scoped struct type that is not itself "Error"-named,
// yet is exactly the wire shape this tool exists to check). The bare,
// heavily-overloaded "Code"/"Type"/"ErrType" only qualify when the
// composite literal's own type name ALSO contains "Err" -- "Type" alone is
// far too common a field name across this repo's 161 services (resource
// types, record types, MFA types, ...) to trust by itself: confirmed live,
// services/acm's DNS challenge record Type field ("CNAME") was this tool's
// first false positive. Bare "Code" needs the same qualifier: confirmed
// live, services/textract's Money{Code: "USD"} currency-code field was a
// second one. "Err", not the fuller "Error", is the qualifier: every real
// emission mechanism this tool was built from names its containing struct
// with "Error"/"Err" in it already -- iamErrorMapping, IAMError, APIError,
// and services/cloudformation's own xmlErrBody, whose name spells "Err"
// but never the full "Error" -- confirmed live: requiring the fuller
// "Error" substring here made this tool blind to cloudformation's own
// xmlError sink (see collectErrFuncs) despite cloudformation being one of
// the four handler.go files this tool was explicitly built to cover.
func narrowFieldNameMatches(name, litTypeName string) bool {
	lower := strings.ToLower(name)
	if lower == labelErrorCode || lower == labelErrorType {
		return true
	}

	if !strings.Contains(strings.ToLower(litTypeName), "err") {
		return false
	}

	return lower == labelCode || lower == labelType || lower == labelErrType
}

func compositeLitTypeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.StarExpr:
		return compositeLitTypeName(e.X)
	default:
		return ""
	}
}
