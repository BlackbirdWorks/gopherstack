package main

import (
	"go/ast"
	"go/token"
	"regexp"
	"strings"
)

// Kinds of finding that are not an error envelope code at all.
const (
	kindResponseField = "response field"
	kindHeaderValue   = "header value"
	kindUnroutedDead  = "dead sentinel"
)

const (
	reasonResponseField = "code of a per-item struct inside a success response body, not an error envelope"
	reasonHeaderValue   = "value of a response header classifier, not an error code"
	reasonUnrouted      = "sentinel is raised only by functions no handler or dispatch path reaches"
)

var (
	headerDocRe     = regexp.MustCompile(`(?i)\bheader\b`)
	errTypeHeaderRe = regexp.MustCompile(`(?i)error-?type|__type`)
)

// isEnvelopeField reports a field an error envelope carries beside its code.
func isEnvelopeField(name string) bool {
	switch strings.ToLower(name) {
	case "code", "errorcode", "failurecode", "message", "errormessage", "type", "errortype",
		"status", "statuscode", "requestid", "resource", "hostid", "details", "detail",
		"error", "xmlname", "xmlns", "reason":
		return true
	}

	return false
}

func isItemCodeLabel(name string) bool {
	switch strings.ToLower(name) {
	case labelCode, labelErrorCode, "failurecode":
		return true
	}

	return false
}

// isItemStruct reports a struct with a code field plus payload fields an
// error envelope never carries; a type with an Error method is an envelope.
func isItemStruct(st *ast.StructType, hasErrorMethod bool) bool {
	if st == nil || st.Fields == nil || hasErrorMethod {
		return false
	}

	hasCode, hasPayload := false, false

	for _, fl := range st.Fields.List {
		for _, n := range fl.Names {
			switch {
			case isItemCodeLabel(n.Name):
				hasCode = true
			case !isEnvelopeField(n.Name):
				hasPayload = true
			}
		}
	}

	return hasCode && hasPayload
}

func errorMethodTypes(files []*ast.File) map[string]bool {
	out := map[string]bool{}

	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || fd.Name.Name != "Error" || len(fd.Recv.List) == 0 {
				continue
			}

			t := fd.Recv.List[0].Type
			if s, isStar := t.(*ast.StarExpr); isStar {
				t = s.X
			}

			if id, isID := t.(*ast.Ident); isID {
				out[id.Name] = true
			}
		}
	}

	return out
}

type kindCtx struct {
	parents   map[ast.Node]ast.Node
	structs   map[string]*ast.StructType
	errMethod map[string]bool
	funcs     map[string]*ast.FuncDecl
}

// itemLiteralValue reports whether n is the value of a code-labeled field in
// a composite literal of an item struct.
func (k *kindCtx) itemLiteralValue(n ast.Node) bool {
	kv, ok := k.parents[n].(*ast.KeyValueExpr)
	if !ok || kv.Value != n {
		return false
	}

	key, isID := kv.Key.(*ast.Ident)
	if !isID || !isItemCodeLabel(key.Name) {
		return false
	}

	cl, isCL := k.parents[kv].(*ast.CompositeLit)
	if !isCL {
		return false
	}

	if k.passedToErrorWriter(cl) {
		return false
	}

	id, named := cl.Type.(*ast.Ident)
	if !named {
		return isItemStruct(resolveStructType(cl.Type, k.structs), false)
	}

	return isItemStruct(k.structs[id.Name], k.errMethod[id.Name])
}

// passedToErrorWriter reports a literal handed straight to an *Error*-named call.
func (k *kindCtx) passedToErrorWriter(cl *ast.CompositeLit) bool {
	call, ok := skipUnary(cl, k.parents).(*ast.CallExpr)
	if !ok {
		return false
	}

	name := ""

	switch f := call.Fun.(type) {
	case *ast.Ident:
		name = f.Name
	case *ast.SelectorExpr:
		name = f.Sel.Name
	}

	return strings.Contains(strings.ToLower(name), "error")
}

// itemUse reports whether a reference is an item-struct code value, directly
// or as an argument whose parameter is one inside the callee.
func (k *kindCtx) itemUse(ref *ast.Ident) bool {
	if k.itemLiteralValue(ref) {
		return true
	}

	call, ok := k.parents[ref].(*ast.CallExpr)
	if !ok {
		return false
	}

	var callee string

	switch f := call.Fun.(type) {
	case *ast.Ident:
		callee = f.Name
	case *ast.SelectorExpr:
		callee = f.Sel.Name
	}

	fd := k.funcs[callee]
	if fd == nil || fd.Body == nil {
		return false
	}

	param := paramNameAt(fd, call.Args, ref)
	if param == "" {
		return false
	}

	found := false

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if id, isID := n.(*ast.Ident); isID && id.Name == param && k.itemLiteralValue(id) {
			found = true
		}

		return !found
	})

	return found
}

func paramNameAt(fd *ast.FuncDecl, args []ast.Expr, ref *ast.Ident) string {
	idx := -1

	for i, a := range args {
		if a == ast.Expr(ref) {
			idx = i
		}
	}

	if idx < 0 {
		return ""
	}

	i := 0

	for _, fl := range fd.Type.Params.List {
		for _, n := range fl.Names {
			if i == idx {
				return n.Name
			}

			i++
		}
	}

	return ""
}

func (k *kindCtx) responseFieldKind(c candidate, ident, decl *ast.Ident, refs map[string][]*ast.Ident) bool {
	if (c.Mechanism == mechFieldLit || c.Mechanism == mechFieldIdent) && ident != nil {
		return k.itemLiteralValue(ident)
	}

	if c.Mechanism != mechCodeVar || decl == nil {
		return false
	}

	uses := 0

	for _, r := range refs[decl.Name] {
		if r == decl {
			continue
		}

		uses++

		if !k.itemUse(r) {
			return false
		}
	}

	return uses > 0
}

func nodeAtPos(files []*ast.File) map[token.Pos]ast.Expr {
	out := map[token.Pos]ast.Expr{}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BasicLit:
				if x.Kind == token.STRING {
					out[x.Pos()] = x
				}
			case *ast.Ident:
				out[x.Pos()] = x
			}

			return true
		})
	}

	return out
}

// isHeaderValueDoc reports a doc about a response header whose value is not
// the error type itself (X-Amzn-Errortype carries a real code).
func isHeaderValueDoc(doc string) bool {
	return headerDocRe.MatchString(doc) && !errTypeHeaderRe.MatchString(doc)
}

func markReturnedLiterals(body *ast.BlockStmt, out map[token.Pos]bool) {
	ast.Inspect(body, func(n ast.Node) bool {
		rs, isRet := n.(*ast.ReturnStmt)
		if !isRet {
			return true
		}

		for _, r := range rs.Results {
			if lit, isLit := r.(*ast.BasicLit); isLit {
				out[lit.Pos()] = true
			}
		}

		return true
	})
}

func headerFuncReturns(files []*ast.File) map[token.Pos]bool {
	out := map[token.Pos]bool{}

	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if ok && fd.Doc != nil && fd.Body != nil && isHeaderValueDoc(fd.Doc.Text()) {
				markReturnedLiterals(fd.Body, out)
			}
		}
	}

	return out
}

// applyKindClassification marks candidates that are not error envelope
// codes: per-item response fields and response-header values.
func applyKindClassification(
	files []*ast.File, structTypes map[string]*ast.StructType, cands []candidate,
) {
	k := &kindCtx{
		parents:   parentMap(files),
		structs:   structTypes,
		errMethod: errorMethodTypes(files),
		funcs:     map[string]*ast.FuncDecl{},
	}

	for _, f := range files {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok {
				k.funcs[fd.Name.Name] = fd
			}
		}
	}

	nodes := nodeAtPos(files)
	decls := declaredStringConsts(files)
	refs := identRefs(files)
	headers := headerFuncReturns(files)

	for i := range cands {
		c := &cands[i]

		if c.Mechanism == mechReturnStmt && headers[c.pos] {
			c.Kind, c.DemoteReason = kindHeaderValue, reasonHeaderValue

			continue
		}

		var ident *ast.Ident

		if id, ok := nodes[c.pos].(*ast.Ident); ok {
			ident = id
		}

		var node ast.Node
		if n, ok := nodes[c.pos]; ok {
			node = n
		}

		if node != nil && c.Mechanism == mechFieldLit && k.itemLiteralValue(node) {
			c.Kind, c.DemoteReason = kindResponseField, reasonResponseField

			continue
		}

		if k.responseFieldKind(*c, ident, decls[c.pos], refs) {
			c.Kind, c.DemoteReason = kindResponseField, reasonResponseField
		}
	}
}
