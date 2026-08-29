package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

// wireKeyFact is what wireEnumKeys learned about one wire key across an
// entire pinned SDK: every real enum type it deserializes into somewhere
// (Enums, possibly more than one struct sharing the name), and whether the
// SAME key name ALSO deserializes as a plain, non-enum string in some other
// struct (Polymorphic).
//
// Polymorphic matters because this scan has no way to tell, at a service
// emission site, which struct's sense of the key applies (see the package
// doc comment) -- confirmed live: comprehend's "ErrorCode" (plain *string on
// a batch-item error struct, types.PageBasedErrorCode on an unrelated
// Textract-page struct), xray's "State" (plain *string on a Service graph
// node, types.InsightState on an Insight), transfer's "Status" (plain
// *string on TestConnectionOutput, several *Status enums elsewhere),
// s3tables's "status" (plain *string on PutTableReplicationOutput) all
// produced false CONFIDENT findings under this key's Enums before
// Polymorphic was tracked and checked by callers. A CONFIDENT check must
// refuse a Polymorphic key entirely; the weaker cross-enum-reuse check
// (reuse.go) still uses Enums even when Polymorphic, since it never claims
// certainty about the value in the first place.
type wireKeyFact struct {
	Enums       []string
	Polymorphic bool
}

// wireEnumKeys parses a pinned SDK's deserializers.go and returns, for every
// wire key with at least one enum sighting, a wireKeyFact.
//
// The signal is codegen-structural, not a name guess: every JSON-family
// protocol this repo pins (restjson1, awsjson1.0/1.1 -- confirmed against
// guardduty@v1.85.4, a restjson1 service) generates
//
//	case "wireKey":
//	    ...
//	    sv.Field = types.SomeEnum(jtv)
//
// inside a `switch key { ... }` keyed off a decoded map[string]interface{}.
// A CaseClause's own literal string(s) are the real wire key(s); an
// AssignStmt in that case's body whose RHS is a call converting to a type
// already known (loadEnumRegistry) to be a declared SDK enum is exactly "this
// key deserializes into that enum". A key seen with NO such assignment
// anywhere (a nested object, a plain string, a number, ...) never appears in
// the result at all -- there is nothing to check it against.
//
// query/EC2-query/REST-XML protocols use an xml.Decoder with no
// map[string]interface{} switch at all, so this parses zero cases for them
// -- same disclosed scope as cmd/keycheck.
func wireEnumKeys(deserializersGoPath string, reg *enumRegistry) (map[string]wireKeyFact, error) {
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, deserializersGoPath, nil, 0)
	if err != nil {
		return nil, err
	}

	enums := map[string]map[string]bool{}
	polymorphic := map[string]bool{}

	ast.Inspect(f, func(n ast.Node) bool {
		sw, ok := n.(*ast.SwitchStmt)
		if !ok {
			return true
		}

		collectSwitchEnumCases(sw, reg, enums, polymorphic)

		return true
	})

	result := make(map[string]wireKeyFact, len(enums))

	for key, types := range enums {
		list := make([]string, 0, len(types))
		for t := range types {
			list = append(list, t)
		}

		result[key] = wireKeyFact{Enums: list, Polymorphic: polymorphic[key]}
	}

	return result, nil
}

func collectSwitchEnumCases(
	sw *ast.SwitchStmt, reg *enumRegistry, enums map[string]map[string]bool, polymorphic map[string]bool,
) {
	if sw.Body == nil {
		return
	}

	for _, stmt := range sw.Body.List {
		cc, ok := stmt.(*ast.CaseClause)
		if !ok {
			continue
		}

		recordCaseClauseKeys(cc, reg, enums, polymorphic)
	}
}

func recordCaseClauseKeys(
	cc *ast.CaseClause, reg *enumRegistry, enums map[string]map[string]bool, polymorphic map[string]bool,
) {
	enumType := caseBodyEnumAssign(cc.Body, reg)
	plain := caseBodyIsPlainString(cc.Body)

	if enumType == "" && !plain {
		return
	}

	for _, expr := range cc.List {
		lit, ok := expr.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}

		key, err := strconv.Unquote(lit.Value)
		if err != nil {
			continue
		}

		if plain {
			polymorphic[key] = true
		}

		if enumType != "" {
			if enums[key] == nil {
				enums[key] = map[string]bool{}
			}

			enums[key][enumType] = true
		}
	}
}

// caseBodyEnumAssign finds the first `sv.Field = types.SomeEnum(x)`
// assignment anywhere in body -- real codegen nests it inside `if value !=
// nil { ... }`, never as a direct top-level statement -- and returns
// "SomeEnum" if SomeEnum is a known SDK enum type, else "".
func caseBodyEnumAssign(body []ast.Stmt, reg *enumRegistry) string {
	found := ""

	for _, stmt := range body {
		if found != "" {
			break
		}

		ast.Inspect(stmt, func(n ast.Node) bool {
			if found != "" {
				return false
			}

			as, isAssign := n.(*ast.AssignStmt)
			if !isAssign || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
				return true
			}

			if _, isSel := as.Lhs[0].(*ast.SelectorExpr); !isSel {
				return true
			}

			found = enumConversionType(as.Rhs[0], reg)

			return true
		})
	}

	return found
}

// caseBodyIsPlainString reports whether body contains an `sv.Field =
// ptr.String(jtv)` or `sv.Field = jtv` assignment -- the codegen shape for a
// plain, non-enum string member deserialized from the same `jtv, ok :=
// value.(string)` this scan also reads the enum-conversion case from.
func caseBodyIsPlainString(body []ast.Stmt) bool {
	found := false

	for _, stmt := range body {
		if found {
			break
		}

		ast.Inspect(stmt, func(n ast.Node) bool {
			if found {
				return false
			}

			as, isAssign := n.(*ast.AssignStmt)
			if !isAssign || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
				return true
			}

			if _, isSel := as.Lhs[0].(*ast.SelectorExpr); !isSel {
				return true
			}

			found = isPlainStringRHS(as.Rhs[0])

			return true
		})
	}

	return found
}

func isPlainStringRHS(expr ast.Expr) bool {
	if _, ok := expr.(*ast.Ident); ok {
		return true
	}

	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return false
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)

	return ok && sel.Sel.Name == "String"
}

// enumConversionType reports the enum type name of expr if expr is a
// `types.SomeEnum(...)` conversion call and SomeEnum is a declared SDK enum.
func enumConversionType(expr ast.Expr, reg *enumRegistry) string {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return ""
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}

	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok || pkgIdent.Name != sdkTypesPkgName {
		return ""
	}

	if _, known := reg.membersByType[sel.Sel.Name]; !known {
		return ""
	}

	return sel.Sel.Name
}
