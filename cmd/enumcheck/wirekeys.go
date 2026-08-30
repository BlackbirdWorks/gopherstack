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
//
// wireGroundTruth also returns, in the same parse pass, every real SDK
// type's own wire-key field set (typeWireFields) -- gopherstack-7fps's
// phantom-field ground truth, read from the same deserializers.go so this
// never parses the file twice.
func wireGroundTruth(
	deserializersGoPath string, reg *enumRegistry,
) (map[string]wireKeyFact, map[string]map[string]bool, error) {
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, deserializersGoPath, nil, 0)
	if err != nil {
		return nil, nil, err
	}

	enums := map[string]map[string]bool{}
	polymorphic := map[string]bool{}
	fields := map[string]map[string]bool{}

	for _, decl := range f.Decls {
		fd, isFunc := decl.(*ast.FuncDecl)
		if !isFunc || fd.Body == nil || fd.Recv != nil {
			continue
		}

		collectFuncEnumCases(fd, reg, enums, polymorphic)
		collectFuncWireFields(fd, fields)
	}

	return wireKeyFactsFromEnums(enums, polymorphic), fields, nil
}

func collectFuncEnumCases(
	fd *ast.FuncDecl,
	reg *enumRegistry,
	enums map[string]map[string]bool,
	polymorphic map[string]bool,
) {
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		sw, isSwitch := n.(*ast.SwitchStmt)
		if !isSwitch {
			return true
		}

		collectSwitchEnumCases(sw, reg, enums, polymorphic)

		return true
	})
}

func collectFuncWireFields(fd *ast.FuncDecl, fields map[string]map[string]bool) {
	typeName, ok := deserializeDocumentTargetType(fd)
	if !ok {
		return
	}

	keys := collectAllCaseKeys(fd.Body)
	if len(keys) == 0 {
		return
	}

	if fields[typeName] == nil {
		fields[typeName] = map[string]bool{}
	}

	for k := range keys {
		fields[typeName][k] = true
	}
}

func wireKeyFactsFromEnums(enums map[string]map[string]bool, polymorphic map[string]bool) map[string]wireKeyFact {
	result := make(map[string]wireKeyFact, len(enums))

	for key, types := range enums {
		list := make([]string, 0, len(types))
		for t := range types {
			list = append(list, t)
		}

		result[key] = wireKeyFact{Enums: list, Polymorphic: polymorphic[key]}
	}

	return result
}

// deserializeDocumentTargetType reports the real SDK type name fd decodes
// into, read from its own first parameter's static type (**types.TypeName)
// -- every deserializeDocument<Type> function in this codegen shape takes
// exactly this signature, structural ground truth rather than a name guess
// off the function identifier (whose prefix varies by protocol:
// awsAwsjson11_, awsRestjson1_, ...).
func deserializeDocumentTargetType(fd *ast.FuncDecl) (string, bool) {
	if fd.Type.Params == nil || len(fd.Type.Params.List) == 0 {
		return "", false
	}

	star1, ok := fd.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return "", false
	}

	star2, ok := star1.X.(*ast.StarExpr)
	if !ok {
		return "", false
	}

	sel, ok := star2.X.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}

	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok || pkgIdent.Name != sdkTypesPkgName {
		return "", false
	}

	return sel.Sel.Name, true
}

// collectAllCaseKeys returns every case-clause literal string of any switch
// statement in body, regardless of what the case assigns -- ground truth
// for "this real type has A FIELD under this wire key at all", not just its
// enum-typed fields.
func collectAllCaseKeys(body *ast.BlockStmt) map[string]bool {
	out := map[string]bool{}

	ast.Inspect(body, func(n ast.Node) bool {
		sw, isSwitch := n.(*ast.SwitchStmt)
		if !isSwitch || sw.Body == nil {
			return true
		}

		for _, stmt := range sw.Body.List {
			cc, isCase := stmt.(*ast.CaseClause)
			if !isCase {
				continue
			}

			for _, expr := range cc.List {
				lit, isLit := expr.(*ast.BasicLit)
				if !isLit || lit.Kind != token.STRING {
					continue
				}

				if v, err := strconv.Unquote(lit.Value); err == nil {
					out[v] = true
				}
			}
		}

		return true
	})

	return out
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

// loadNestedTypeRefs parses a pinned SDK's types/types.go and returns, for
// every top-level `type X struct { ... }`, the set of other locally
// declared type names referenced by X's own field types (through *T, []T,
// or map[K]T, unwrapped to their base named type) -- ground truth for
// expandOneHopNestedFields's one-hop flattening tolerance: gopherstack
// routinely flattens a real API's parent+child nesting into one local
// struct -- confirmed live, amplify's real Job wraps `Steps []Step` and
// `Summary *JobSummary`; Job's own Status/Type fields actually live on the
// nested JobSummary, not on Job itself, so without this a locally-flattened
// gopherstack Job{Status: ...} was wrongly flagged phantom. An embedded
// field (no Names, e.g. the generated noSmithyDocumentSerde marker) is
// skipped -- same discipline collectStructFieldWireNames already applies to
// gopherstack's own structs.
func loadNestedTypeRefs(typesGoPath string) (map[string][]string, error) {
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, typesGoPath, nil, 0)
	if err != nil {
		return nil, err
	}

	out := map[string][]string{}

	for _, decl := range f.Decls {
		gd, isGenDecl := decl.(*ast.GenDecl)
		if !isGenDecl || gd.Tok != token.TYPE {
			continue
		}

		for _, spec := range gd.Specs {
			addStructTypeRefs(spec, out)
		}
	}

	return out, nil
}

func addStructTypeRefs(spec ast.Spec, out map[string][]string) {
	ts, isType := spec.(*ast.TypeSpec)
	if !isType {
		return
	}

	st, isStruct := ts.Type.(*ast.StructType)
	if !isStruct || st.Fields == nil {
		return
	}

	var refs []string

	for _, field := range st.Fields.List {
		if len(field.Names) == 0 {
			continue
		}

		if name, ok := namedTypeRef(field.Type); ok {
			refs = append(refs, name)
		}
	}

	if len(refs) > 0 {
		out[ts.Name.Name] = refs
	}
}

// namedTypeRef unwraps expr's pointer/slice/map wrapping to its base type
// and reports its name if that base type is an exported identifier (a
// locally declared struct type this SDK module might independently know
// wire fields for) -- an unexported/builtin type (string, int32, a
// lowercase-named type) is never a struct this scan tracks, so it is
// excluded by IsExported alone, no separate builtin list needed.
func namedTypeRef(expr ast.Expr) (string, bool) {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return namedTypeRef(e.X)
	case *ast.ArrayType:
		return namedTypeRef(e.Elt)
	case *ast.MapType:
		return namedTypeRef(e.Value)
	case *ast.Ident:
		if e.IsExported() {
			return e.Name, true
		}

		return "", false
	default:
		return "", false
	}
}

// expandOneHopNestedFields returns direct's wire-field sets each unioned,
// one hop only, with the wire-field sets of every type its own struct
// fields reference (refs) -- see loadNestedTypeRefs's doc comment. Only
// expands a type that already has SOME direct wire-field ground truth of
// its own (from its own deserializeDocument<Type> function); a type with
// no direct ground truth at all gains none here either, same "resolves to
// nothing new" discipline as the rest of this scan.
func expandOneHopNestedFields(direct map[string]map[string]bool, refs map[string][]string) map[string]map[string]bool {
	out := make(map[string]map[string]bool, len(direct))

	for typeName, fields := range direct {
		merged := map[string]bool{}
		for k := range fields {
			merged[k] = true
		}

		for _, refType := range refs[typeName] {
			for k := range direct[refType] {
				merged[k] = true
			}
		}

		out[typeName] = merged
	}

	return out
}
