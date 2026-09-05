package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
)

// applyMapperDetection finds every sentinel error this service dir declares
// (errors.New("Lit") / awserr.New("Lit", ...) / awserr.Newf("Lit", ...) at
// package scope) whose own declared literal is never itself written to the
// wire -- only matched by identity via errors.Is somewhere in this dir's
// own source, with the ACTUAL wire code coming from a separate literal
// decided at the match site. rds's rdsErrorCode, neptune's
// neptuneErrorCode, fis's classifyError, cloudfront's notFoundCodeCore/
// errCodeMapping, and elasticache's per-call-site `if errors.Is(err, ErrX)
// { ...xmlError(c, status, "SomeOtherLiteral", msg) }` guards are the same
// shape wearing five different syntaxes: a sentinel value flows in, a
// DIFFERENT string flows out.
//
// It mutates cands in place, setting MapperReason on every candidate that
// is exactly one of these sentinel declarations -- scan.go's buildFinding
// treats a non-empty MapperReason as an override, forcing needs-review
// rather than confident. This never drops a finding (it still prints,
// demoted). It also returns new candidates for every mapper-table row's
// OUTPUT literal this function finds directly (a struct populated with
// both an error-typed field and a string-typed field, keyed or positional,
// is unambiguously a mapper row -- narrowFieldNameMatches's field-name
// allowlist exists to rule OUT a Code/Type field this scan reaches
// incidentally elsewhere, which does not apply here, and its own
// "err-in-the-type-name" requirement otherwise blinds the scan to an
// ANONYMOUS row struct like cloudfront's `[]struct{ err error; code
// string; status int }{...}`, whose composite-literal type name is empty).
func applyMapperDetection(
	files []*ast.File,
	structTypes map[string]*ast.StructType,
	pkgStrings map[string]string,
	fset *token.FileSet,
	repoRoot string,
	cands []candidate,
) []candidate {
	decls := collectSentinelDecls(files)

	consumed, outputs := scanMappers(files, structTypes, pkgStrings, fset, repoRoot)
	if len(decls) == 0 || len(consumed) == 0 {
		return outputs
	}

	for i := range cands {
		c := &cands[i]
		if c.Mechanism != mechAwserrNew && c.Mechanism != mechStdlibErr {
			continue
		}

		name, isDecl := decls[c.pos]
		if !isDecl || !consumed[name] {
			continue
		}

		c.MapperReason = fmt.Sprintf(
			"sentinel %s's own literal is matched only via errors.Is identity by a "+
				"central error-code mapper in this service; it is never itself written "+
				"to the wire -- check the mapper's OUTPUT code (the mapper's other "+
				"literal/table-row/switch-case value) instead",
			name,
		)
	}

	return outputs
}

// collectSentinelDecls maps the position of the message literal in every
// package-scoped `X = errors.New("Lit")` / `X = awserr.New("Lit", ...)` /
// `X = awserr.Newf("Lit", ...)` declaration to X's own name -- the exact
// position extract.go's mechStdlibErr/mechAwserrNew rules build their
// candidate from, so a mapper-consumption verdict lands on the very same
// finding.
func collectSentinelDecls(files []*ast.File) map[token.Pos]string {
	out := map[token.Pos]string{}

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, isGD := decl.(*ast.GenDecl)
			if !isGD || gd.Tok != token.VAR {
				continue
			}

			for _, spec := range gd.Specs {
				collectSentinelValueSpec(spec, out)
			}
		}
	}

	return out
}

func collectSentinelValueSpec(spec ast.Spec, out map[token.Pos]string) {
	vs, isVS := spec.(*ast.ValueSpec)
	if !isVS || len(vs.Names) != len(vs.Values) {
		return
	}

	for i, name := range vs.Names {
		if pos, ok := sentinelCallLiteralPos(vs.Values[i]); ok {
			out[pos] = name.Name
		}
	}
}

// sentinelCallLiteralPos reports the position of expr's message-literal
// argument when expr is a call to awserr.New/awserr.Newf/errors.New --
// mirrors matchCallExpr's own recognition of these three call shapes.
func sentinelCallLiteralPos(expr ast.Expr) (token.Pos, bool) {
	call, isCall := expr.(*ast.CallExpr)
	if !isCall || len(call.Args) == 0 {
		return 0, false
	}

	sel, isSel := call.Fun.(*ast.SelectorExpr)
	if !isSel {
		return 0, false
	}

	pkgIdent, isPkg := sel.X.(*ast.Ident)
	if !isPkg {
		return 0, false
	}

	isSentinelCall := (pkgIdent.Name == pkgAwserr && (sel.Sel.Name == fnSentinelNew || sel.Sel.Name == fnAwserrNewf)) ||
		(pkgIdent.Name == pkgErrors && sel.Sel.Name == fnSentinelNew)
	if !isSentinelCall {
		return 0, false
	}

	lit, isLit := call.Args[0].(*ast.BasicLit)
	if !isLit || lit.Kind != token.STRING {
		return 0, false
	}

	return lit.Pos(), true
}

// scanMappers returns every identifier name this service dir's own source
// uses to reach a sentinel by IDENTITY rather than by reading its message
// text, via either of two shapes, and the mapper-table shape's own OUTPUT
// literal candidates:
//
//   - a direct errors.Is(_, S) call anywhere -- fis's classifyError switch
//     and elasticache's per-call-site `if errors.Is(err, ErrX) { ... }`
//     guards both spell S directly as an argument.
//   - S populating the error-typed field of a mapping-table row: a
//     composite literal of a struct with both an error field and a string
//     field (rds/neptune's local `type errorMapping struct { sentinel
//     error; code string }`, cloudfront's package-level anonymous-struct
//     `errCodeMapping`), keyed or positional -- markMapperTableConsumed
//     reads the row's OTHER field (the code) directly in the same pass,
//     rather than relying on matchCompositeLit's separate field-name
//     filter (see applyMapperDetection's doc comment for why that filter
//     alone misses an anonymous row struct).
func scanMappers(
	files []*ast.File,
	structTypes map[string]*ast.StructType,
	pkgStrings map[string]string,
	fset *token.FileSet,
	repoRoot string,
) (map[string]bool, []candidate) {
	consumed := map[string]bool{}

	var outputs []candidate

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				markErrorsIsConsumed(node, consumed)
			case *ast.CompositeLit:
				if c, ok := markMapperTableConsumed(node, structTypes, pkgStrings, fset, repoRoot, consumed); ok {
					outputs = append(outputs, c)
				}
			}

			return true
		})
	}

	return consumed, outputs
}

func markErrorsIsConsumed(call *ast.CallExpr, consumed map[string]bool) {
	if !isErrorsIsCall(call) {
		return
	}

	for _, arg := range call.Args {
		if id, isIdent := arg.(*ast.Ident); isIdent {
			consumed[id.Name] = true
		}
	}
}

func isErrorsIsCall(call *ast.CallExpr) bool {
	sel, isSel := call.Fun.(*ast.SelectorExpr)
	if !isSel {
		return false
	}

	pkgIdent, isPkg := sel.X.(*ast.Ident)

	return isPkg && pkgIdent.Name == pkgErrors && sel.Sel.Name == "Is"
}

// containsErrorsIsCall reports whether body calls errors.Is anywhere --
// extract.go's matchReturnLiterals uses this as a second, name-independent
// gate onto a function it should treat as an error-code classifier: a
// function that branches on errors.Is at all is doing exactly the
// sentinel-identity-to-code-literal mapping this tool exists to see through
// (services/cloudfront's notFoundCodeCore is named nothing like "error" but
// is exactly this shape).
func containsErrorsIsCall(body *ast.BlockStmt) bool {
	found := false

	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}

		if call, isCall := n.(*ast.CallExpr); isCall && isErrorsIsCall(call) {
			found = true

			return false
		}

		return true
	})

	return found
}

// markMapperTableConsumed records cl's error-field identifier (if any) into
// consumed and, when cl also carries a code-shaped literal or one-hop
// resolvable const in its string field, returns that as a new direct
// candidate (mechMapperOutput) -- the mapper's own OUTPUT for this row.
func markMapperTableConsumed(
	cl *ast.CompositeLit,
	structTypes map[string]*ast.StructType,
	pkgStrings map[string]string,
	fset *token.FileSet,
	repoRoot string,
	consumed map[string]bool,
) (candidate, bool) {
	st := resolveStructType(cl.Type, structTypes)
	if st == nil || st.Fields == nil {
		return candidate{}, false
	}

	errField, strField := errorAndStringFieldNames(st)
	if errField == "" || strField == "" {
		return candidate{}, false
	}

	fieldNames := positionalFieldNames(cl.Type, structTypes)

	var sentinelSeen bool

	var codeExpr ast.Expr

	for i, elt := range cl.Elts {
		fieldName, valueExpr, ok := mapperRowElement(elt, i, fieldNames)
		if !ok {
			continue
		}

		switch fieldName {
		case errField:
			if id, isIdent := valueExpr.(*ast.Ident); isIdent {
				consumed[id.Name] = true
				sentinelSeen = true
			}
		case strField:
			codeExpr = valueExpr
		}
	}

	if !sentinelSeen || codeExpr == nil {
		return candidate{}, false
	}

	return mapperOutputCandidate(codeExpr, pkgStrings, fset, repoRoot)
}

func mapperOutputCandidate(
	expr ast.Expr, pkgStrings map[string]string, fset *token.FileSet, repoRoot string,
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

		return newCandidate(fset, repoRoot, e.Pos(), v, mechMapperOutput, false), true
	case *ast.Ident:
		v, ok := pkgStrings[e.Name]
		if !ok || !looksLikeCode(v) {
			return candidate{}, false
		}

		return newCandidate(fset, repoRoot, e.Pos(), v, mechMapperOutput, true), true
	default:
		return candidate{}, false
	}
}

func mapperRowElement(elt ast.Expr, i int, fieldNames []string) (string, ast.Expr, bool) {
	if kv, keyed := elt.(*ast.KeyValueExpr); keyed {
		id, isIdent := kv.Key.(*ast.Ident)
		if !isIdent {
			return "", nil, false
		}

		return id.Name, kv.Value, true
	}

	if i < len(fieldNames) {
		return fieldNames[i], elt, true
	}

	return "", nil, false
}

// errorAndStringFieldNames returns the names of st's first field of type
// `error` and first field of type `string`, the shape every mapper-table
// row struct this tool was built from uses (rds/neptune's `sentinel error;
// code string`).
func errorAndStringFieldNames(st *ast.StructType) (string, string) {
	var errField, strField string

	for _, field := range st.Fields.List {
		id, isIdent := field.Type.(*ast.Ident)
		if !isIdent {
			continue
		}

		for _, name := range field.Names {
			switch id.Name {
			case "error":
				if errField == "" {
					errField = name.Name
				}
			case "string":
				if strField == "" {
					strField = name.Name
				}
			}
		}
	}

	return errField, strField
}
