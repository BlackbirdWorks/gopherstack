package main

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

func parentMap(files []*ast.File) map[ast.Node]ast.Node {
	parents := map[ast.Node]ast.Node{}

	for _, f := range files {
		var stack []ast.Node

		ast.Inspect(f, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]

				return true
			}

			if len(stack) > 0 {
				parents[n] = stack[len(stack)-1]
			}

			stack = append(stack, n)

			return true
		})
	}

	return parents
}

func stringLitPositions(files []*ast.File) map[token.Pos]*ast.BasicLit {
	out := map[token.Pos]*ast.BasicLit{}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				out[lit.Pos()] = lit
			}

			return true
		})
	}

	return out
}

func skipUnary(n ast.Node, parents map[ast.Node]ast.Node) ast.Node {
	for {
		u, ok := parents[n].(*ast.UnaryExpr)
		if !ok || u.Op != token.AND {
			return parents[n]
		}

		n = u
	}
}

// isMemberLiteral reports a composite literal appended to a list or held
// as a field value of another literal: a per-item member, not an envelope.
func isMemberLiteral(cl *ast.CompositeLit, parents map[ast.Node]ast.Node) bool {
	switch p := skipUnary(cl, parents).(type) {
	case *ast.CallExpr:
		id, ok := p.Fun.(*ast.Ident)

		return ok && id.Name == "append" && len(p.Args) > 1 && p.Args[0] != ast.Expr(cl)
	case *ast.KeyValueExpr:
		_, nested := parents[p].(*ast.CompositeLit)

		return nested && p.Value != nil
	case *ast.CompositeLit:
		return isNestedSliceElement(p, parents)
	}

	return false
}

func isNestedSliceElement(outer *ast.CompositeLit, parents map[ast.Node]ast.Node) bool {
	if _, isArr := outer.Type.(*ast.ArrayType); !isArr {
		return false
	}

	if _, isArg := skipUnary(outer, parents).(*ast.CallExpr); isArg {
		return true
	}

	kv, ok := skipUnary(outer, parents).(*ast.KeyValueExpr)
	if !ok {
		return false
	}

	_, nested := parents[kv].(*ast.CompositeLit)

	return nested
}

func enclosingLiteral(n ast.Node, parents map[ast.Node]ast.Node) *ast.CompositeLit {
	for cur := parents[n]; cur != nil; cur = parents[cur] {
		if cl, ok := cur.(*ast.CompositeLit); ok {
			return cl
		}
	}

	return nil
}

func isCodeLabel(label string) bool {
	switch strings.ToLower(label) {
	case labelCode, labelErrorCode, labelType, labelErrType, labelErrorType, labelWireType, labelWireError:
		return true
	}

	return false
}

func labelOf(key ast.Expr, pkgStrings map[string]string) string {
	switch k := key.(type) {
	case *ast.Ident:
		if v, ok := pkgStrings[k.Name]; ok {
			return v
		}

		return k.Name
	case *ast.SelectorExpr:
		return k.Sel.Name
	case *ast.BasicLit:
		v, _ := strconv.Unquote(k.Value)

		return v
	}

	return ""
}

// refUseReason classifies one reference to a code-named const: a value
// stored under a non-code key, or a field of a per-item list member.
func refUseReason(
	ref *ast.Ident, parents map[ast.Node]ast.Node, pkgStrings map[string]string,
) string {
	switch p := parents[ref].(type) {
	case *ast.KeyValueExpr:
		if p.Value != ast.Expr(ref) {
			return ""
		}

		if !isCodeLabel(labelOf(p.Key, pkgStrings)) {
			return "stored under a non-error field"
		}

		if cl := enclosingLiteral(p, parents); cl != nil && isMemberLiteral(cl, parents) {
			return "per-item field of a success-response list member"
		}
	case *ast.AssignStmt:
		for i, rhs := range p.Rhs {
			if rhs != ast.Expr(ref) || i >= len(p.Lhs) {
				continue
			}

			if !isCodeLabel(lhsLabel(p.Lhs[i], pkgStrings)) {
				return "assigned to a non-error field"
			}
		}
	}

	return ""
}

func lhsLabel(lhs ast.Expr, pkgStrings map[string]string) string {
	switch l := lhs.(type) {
	case *ast.IndexExpr:
		return labelOf(l.Index, pkgStrings)
	case *ast.SelectorExpr:
		return l.Sel.Name
	}

	return "code"
}

func declaredStringConsts(files []*ast.File) map[token.Pos]*ast.Ident {
	out := map[token.Pos]*ast.Ident{}

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || (gd.Tok != token.CONST && gd.Tok != token.VAR) {
				continue
			}

			for _, spec := range gd.Specs {
				vs, isVS := spec.(*ast.ValueSpec)
				if !isVS || len(vs.Names) != len(vs.Values) {
					continue
				}

				for i, v := range vs.Values {
					out[v.Pos()] = vs.Names[i]
				}
			}
		}
	}

	return out
}

func identRefs(files []*ast.File) map[string][]*ast.Ident {
	out := map[string][]*ast.Ident{}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				out[id.Name] = append(out[id.Name], id)
			}

			return true
		})
	}

	return out
}

// constUseReason demotes a code-named const that is never referenced, or
// only ever used as a non-error value or per-item list member field.
func constUseReason(
	decl *ast.Ident, refs map[string][]*ast.Ident, parents map[ast.Node]ast.Node, pkgStrings map[string]string,
) string {
	var uses []*ast.Ident

	for _, r := range refs[decl.Name] {
		if r != decl {
			uses = append(uses, r)
		}
	}

	if len(uses) == 0 {
		return "declared but never referenced"
	}

	reason := ""

	for _, u := range uses {
		r := refUseReason(u, parents, pkgStrings)
		if r == "" {
			return ""
		}

		reason = r
	}

	return "every use is " + reason + ", not an error envelope code"
}

func memberLiteralReason(lit *ast.BasicLit, parents map[ast.Node]ast.Node) string {
	if lit == nil {
		return ""
	}

	cl := enclosingLiteral(lit, parents)
	if cl != nil && isMemberLiteral(cl, parents) {
		return "value of a per-item member inside a response body, not an error envelope code"
	}

	return ""
}

// applyUseSiteDemotions sets DemoteReason on candidates whose literal never
// reaches an error envelope: per-item response members and unused consts.
func applyUseSiteDemotions(
	files []*ast.File, pkgStrings map[string]string, cands []candidate,
) {
	parents := parentMap(files)
	lits := stringLitPositions(files)
	decls := declaredStringConsts(files)
	refs := identRefs(files)

	for i := range cands {
		c := &cands[i]
		if c.DemoteReason != "" {
			continue
		}

		if c.Mechanism == mechFieldLit {
			c.DemoteReason = memberLiteralReason(lits[c.pos], parents)
		} else if decl, ok := decls[c.pos]; ok && c.Mechanism == mechCodeVar {
			c.DemoteReason = constUseReason(decl, refs, parents, pkgStrings)
		}
	}
}
