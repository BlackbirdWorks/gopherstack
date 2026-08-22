package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
)

type class string

const (
	classWrapped    class = "wrapped"
	classFlat       class = "flat/payload"
	classHeaderOnly class = "header-only"
	classVoid       class = "void"
	classUnknown    class = "unknown"
)

// outputIdent is the name smithy-go always gives the *<Op>Output local
// variable in HandleDeserialize -- the identifier classifyArg matches
// against to tell a whole-output call from a field-address or bare-output
// one.
const outputIdent = "output"

type opResult struct {
	Op       string   `json:"op"`
	Protocol string   `json:"protocol"`
	Class    class    `json:"class"`
	Member   string   `json:"member,omitempty"`
	Callee   string   `json:"callee,omitempty"`
	Detail   string   `json:"detail,omitempty"`
	Keys     []string `json:"keys,omitempty"`
}

// jsonProtocolPrefixes are the deserializer prefixes this classifier's logic
// has been verified against (see cmd/bodyclass doc comment): both route the
// body through "var shape interface{}; json.Decode(&shape)" then either the
// whole-output OpDocument helper or a direct single-field decode call. Every
// other protocol (restxml, ec2query, awsquery, rpcv2cbor, ...) deserializes
// differently and is reported unknown rather than guessed at.
func supportedProtocol(prefix string) bool {
	return strings.HasPrefix(prefix, "awsRestjson") || strings.HasPrefix(prefix, "awsAwsjson")
}

func classifyOp(idx *serviceIndex, op string) opResult {
	protocol := idx.protocolByOp[op]
	res := opResult{Op: op, Protocol: protocol}

	hd := idx.handleDeser[op]
	if hd == nil || hd.Body == nil {
		res.Class = classUnknown
		res.Detail = "no HandleDeserialize method found"

		return res
	}

	if !supportedProtocol(protocol) {
		res.Class = classUnknown
		res.Detail = "protocol " + protocol + " not covered by this classifier"

		return res
	}

	outputFields, err := outputFieldCount(idx.fset, idx.outputFieldsDir, op)
	if err != nil {
		res.Class = classUnknown
		res.Detail = "could not read " + op + "Output struct: " + err.Error()

		return res
	}

	// Void is structural: an Output with no real members can't be wrapped or
	// flat no matter what HandleDeserialize calls -- a still-generated
	// OpDocument helper for a void op has a key switch with only a "default"
	// clause, which would otherwise misreport as unknown.
	if outputFields == 0 {
		res.Class = classVoid

		return res
	}

	dc := findDocumentCall(hd)
	if dc == nil {
		return classifyNoDocumentCall(res, hd, outputFields)
	}

	switch dc.shape {
	case argWholeAddr:
		return classifyWrapped(res, dc, idx.funcsByName)
	case argFieldAddr:
		res.Class = classFlat
		res.Member = dc.field
		res.Callee = calleeName(dc.expr)

		return res
	case argBareOutput:
		return classifyPayload(res, dc, idx.funcsByName)
	default:
		res.Class = classUnknown
		res.Detail = "unrecognized first argument shape calling " + calleeName(dc.expr)

		return res
	}
}

type argShape int

const (
	argOther argShape = iota
	argWholeAddr
	argFieldAddr
	argBareOutput
)

type documentCall struct {
	expr  *ast.CallExpr
	field string
	shape argShape
}

// findDocumentCall locates the "err = <fn>(...)" call in hd's body whose
// callee name contains "Document" -- the only call shape smithy-go uses for
// a function that consumes the decoded response body (as opposed to
// deserializeOpHttpBindings..., which reads headers, or
// deserializeOpError..., which handles non-2xx responses). There is at most
// one such call per operation.
func findDocumentCall(hd *ast.FuncDecl) *documentCall {
	var found *documentCall

	ast.Inspect(hd.Body, func(n ast.Node) bool {
		if found != nil {
			return false
		}

		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}

		call := documentCallFromAssign(assign)
		if call != nil {
			found = call

			return false
		}

		return true
	})

	return found
}

func documentCallFromAssign(assign *ast.AssignStmt) *documentCall {
	if !assignsErr(assign) || len(assign.Rhs) != 1 {
		return nil
	}

	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok || len(call.Args) == 0 {
		return nil
	}

	fn, ok := call.Fun.(*ast.Ident)
	if !ok || !strings.Contains(fn.Name, "Document") {
		return nil
	}

	shape, field := classifyArg(call.Args[0])
	if shape == argOther {
		return nil
	}

	return &documentCall{expr: call, shape: shape, field: field}
}

func assignsErr(assign *ast.AssignStmt) bool {
	for _, lhs := range assign.Lhs {
		if id, ok := lhs.(*ast.Ident); ok && id.Name == "err" {
			return true
		}
	}

	return false
}

// classifyArg reads the first argument of the body-consuming call:
// "&output" (whole Output pointer, the shape a live OpDocument wrapper
// takes) vs. "&output.Field" (one field's address, the shape a dead
// wrapper's live replacement takes) vs. bare "output" (the shape a
// payload-bound helper -- one that reads response.Body/ContentLength
// directly instead of a decoded JSON value -- takes).
func classifyArg(arg ast.Expr) (argShape, string) {
	unary, isUnary := arg.(*ast.UnaryExpr)
	if !isUnary {
		if id, isIdent := arg.(*ast.Ident); isIdent && id.Name == outputIdent {
			return argBareOutput, ""
		}

		return argOther, ""
	}

	if unary.Op != token.AND {
		return argOther, ""
	}

	switch x := unary.X.(type) {
	case *ast.Ident:
		if x.Name == outputIdent {
			return argWholeAddr, ""
		}
	case *ast.SelectorExpr:
		if base, isIdent := x.X.(*ast.Ident); isIdent && base.Name == outputIdent {
			return argFieldAddr, x.Sel.Name
		}
	}

	return argOther, ""
}

func calleeName(call *ast.CallExpr) string {
	if id, ok := call.Fun.(*ast.Ident); ok {
		return id.Name
	}

	return ""
}

// classifyWrapped confirms a whole-output call is genuinely wrapped by
// reading the callee's body for its key switch, rather than trusting the
// call shape alone -- per gopherstack-cnhp, only the callee's actual content
// is reliable.
func classifyWrapped(res opResult, dc *documentCall, funcsByName map[string]*ast.FuncDecl) opResult {
	name := calleeName(dc.expr)
	res.Callee = name

	callee, ok := funcsByName[name]
	if !ok || callee.Body == nil {
		res.Class = classUnknown
		res.Detail = "callee " + name + " not found in deserializers.go"

		return res
	}

	keys := extractSwitchKeys(callee)
	if len(keys) == 0 {
		res.Class = classUnknown
		res.Detail = "callee " + name + " has a whole-output signature but no key switch was found in its body"

		return res
	}

	res.Class = classWrapped
	res.Keys = keys

	return res
}

// extractSwitchKeys returns the case-label strings of the first switch
// statement found in fd's body (the generated "for key, value := range
// shape { switch key { case \"foo\": ... } }" pattern), skipping the default
// clause.
func extractSwitchKeys(fd *ast.FuncDecl) []string {
	var keys []string

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		sw, ok := n.(*ast.SwitchStmt)
		if !ok {
			return true
		}

		keys = caseLabels(sw)

		return false
	})

	return keys
}

func caseLabels(sw *ast.SwitchStmt) []string {
	var keys []string

	for _, stmt := range sw.Body.List {
		cc, ok := stmt.(*ast.CaseClause)
		if !ok || cc.List == nil {
			continue
		}

		for _, expr := range cc.List {
			if lit, isString := expr.(*ast.BasicLit); isString && lit.Kind == token.STRING {
				keys = append(keys, trimQuotes(lit.Value))
			}
		}
	}

	return keys
}

// classifyPayload confirms a bare-output (payload-bound) call assigns
// exactly one field of its receiver by reading the callee's body, matching
// the polly/mediastoredata/appconfigdata shape from gopherstack-cnhp.
func classifyPayload(res opResult, dc *documentCall, funcsByName map[string]*ast.FuncDecl) opResult {
	name := calleeName(dc.expr)
	res.Callee = name

	callee, ok := funcsByName[name]
	if !ok || callee.Body == nil || callee.Type.Params == nil || len(callee.Type.Params.List) == 0 {
		res.Class = classUnknown
		res.Detail = "payload callee " + name + " not found or has no parameters"

		return res
	}

	paramName := firstParamName(callee)
	if paramName == "" {
		res.Class = classUnknown
		res.Detail = "payload callee " + name + "'s first parameter has no name"

		return res
	}

	fields := assignedFields(callee, paramName)

	switch len(fields) {
	case 0:
		res.Class = classUnknown
		res.Detail = "payload callee " + name + " assigns no field of " + paramName
	case 1:
		res.Class = classFlat
		res.Member = fields[0]
	default:
		res.Class = classUnknown
		res.Detail = fmt.Sprintf("payload callee %s assigns %d fields (%s) -- expected exactly one",
			name, len(fields), strings.Join(fields, ", "))
	}

	return res
}

func firstParamName(fd *ast.FuncDecl) string {
	names := fd.Type.Params.List[0].Names
	if len(names) == 0 {
		return ""
	}

	return names[0].Name
}

// assignedFields returns, in first-seen order, the distinct field names
// assigned as "<paramName>.Field = ..." anywhere in fd's body.
func assignedFields(fd *ast.FuncDecl, paramName string) []string {
	seen := map[string]bool{}

	var out []string

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}

		for _, lhs := range assign.Lhs {
			if field, isField := selectorField(lhs, paramName); isField && !seen[field] {
				seen[field] = true
				out = append(out, field)
			}
		}

		return true
	})

	return out
}

func selectorField(expr ast.Expr, paramName string) (string, bool) {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}

	base, ok := sel.X.(*ast.Ident)
	if !ok || base.Name != paramName {
		return "", false
	}

	return sel.Sel.Name, true
}

func hasHTTPBindingsCall(hd *ast.FuncDecl) bool {
	found := false

	ast.Inspect(hd.Body, func(n ast.Node) bool {
		if found {
			return false
		}

		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		if id, isIdent := call.Fun.(*ast.Ident); isIdent && strings.Contains(id.Name, "HttpBindings") {
			found = true

			return false
		}

		return true
	})

	return found
}

func classifyNoDocumentCall(res opResult, hd *ast.FuncDecl, outputFields int) opResult {
	if hasHTTPBindingsCall(hd) {
		res.Class = classHeaderOnly

		return res
	}

	res.Class = classUnknown
	res.Detail = fmt.Sprintf(
		"no document-decode or HTTP-bindings call found in HandleDeserialize, but Output has %d field(s)",
		outputFields)

	return res
}
