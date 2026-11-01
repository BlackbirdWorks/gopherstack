package main

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
)

const (
	v1Path       = "encoding/json"
	v2Path       = "encoding/json/v2"
	jsontextPath = "encoding/json/jsontext"
	jsonxPath    = "github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	localPrefix  = "github.com/blackbirdworks/gopherstack"
	optV1        = "jsonx.V1()"
)

var errOverlap = errors.New("overlapping edits")

type finding struct {
	msg  string
	line int
}

type result struct {
	out      []byte
	findings []finding
}

type edit struct {
	text       string
	start, end int
}

type stream struct {
	call    *ast.CallExpr
	block   *ast.BlockStmt
	name    string
	opts    []string
	edits   []edit
	decodes []*ast.CallExpr
	decoder bool
	bad     bool
}

type rewriter struct {
	fset     *token.FileSet
	file     *ast.File
	src      []byte
	jsonName string
	consumed map[ast.Node]bool
	streams  map[string]*stream
	edits    []edit
	findings []finding
}

func (r *rewriter) off(p token.Pos) int { return r.fset.Position(p).Offset }

func (r *rewriter) text(a, b ast.Node) string { return string(r.src[r.off(a.Pos()):r.off(b.End())]) }

func (r *rewriter) add(start, end int, text string) {
	r.edits = append(r.edits, edit{start: start, end: end, text: text})
}

func (r *rewriter) reportf(p token.Pos, format string, a ...any) {
	r.findings = append(r.findings, finding{line: r.fset.Position(p).Line, msg: fmt.Sprintf(format, a...)})
}

func (r *rewriter) isJSON(e ast.Expr, name string) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	id, ok := sel.X.(*ast.Ident)

	return ok && id.Name == r.jsonName && (name == "" || sel.Sel.Name == name)
}

func (r *rewriter) hasComment(start, end int) bool {
	for _, cg := range r.file.Comments {
		if r.off(cg.Pos()) < end && r.off(cg.End()) > start {
			return true
		}
	}

	return false
}

func rewrite(path string, src []byte) (*result, error) {
	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	name, ok := jsonImportName(file)
	if !ok {
		return &result{}, nil
	}

	r := &rewriter{fset: fset, file: file, src: src, jsonName: name, consumed: map[ast.Node]bool{}}

	for _, d := range file.Decls {
		r.streams = map[string]*stream{}
		r.collectStreams(d)
		r.rewriteStreamUses(d)
		r.rewriteCalls(d)
		r.rewriteSelectors(d)
		r.emitStreams()
	}

	out, err := r.apply()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	out, err = fixImports(path, out, name)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	res := &result{findings: r.findings}
	if !bytes.Equal(out, src) {
		res.out = out
	}

	return res, nil
}

func jsonImportName(file *ast.File) (string, bool) {
	for _, imp := range file.Imports {
		if p, _ := strconv.Unquote(imp.Path.Value); p != v1Path {
			continue
		}

		if imp.Name == nil {
			return "json", true
		}

		if imp.Name.Name == "_" || imp.Name.Name == "." {
			return "", false
		}

		return imp.Name.Name, true
	}

	return "", false
}

func (r *rewriter) collectStreams(d ast.Decl) {
	register := func(lhs ast.Expr, rhs ast.Expr, block *ast.BlockStmt) {
		id, ok := lhs.(*ast.Ident)
		call, isCall := rhs.(*ast.CallExpr)

		if !ok || !isCall || len(call.Args) != 1 {
			return
		}

		switch {
		case r.isJSON(call.Fun, "NewDecoder"):
			r.streams[id.Name] = &stream{name: id.Name, call: call, decoder: true, block: block}
		case r.isJSON(call.Fun, "NewEncoder"):
			r.streams[id.Name] = &stream{name: id.Name, call: call, block: block}
		default:
			return
		}

		r.consumed[call.Fun] = true
	}

	ast.Inspect(d, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			if len(x.Lhs) == 1 && len(x.Rhs) == 1 {
				register(x.Lhs[0], x.Rhs[0], enclosing(d, x))
			}
		case *ast.ValueSpec:
			if len(x.Names) == 1 && len(x.Values) == 1 {
				register(x.Names[0], x.Values[0], enclosing(d, x))
			}
		}

		return true
	})
}

// enclosing returns the innermost block statement of d containing n.
func enclosing(d ast.Decl, n ast.Node) *ast.BlockStmt {
	var best *ast.BlockStmt

	ast.Inspect(d, func(c ast.Node) bool {
		if b, ok := c.(*ast.BlockStmt); ok && b.Pos() <= n.Pos() && n.End() <= b.End() {
			best = b
		}

		return true
	})

	return best
}

func (r *rewriter) declIdents(d ast.Decl) map[*ast.Ident]bool {
	out := map[*ast.Ident]bool{}

	ast.Inspect(d, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				if id, ok := l.(*ast.Ident); ok && r.streams[id.Name] != nil {
					out[id] = true
				}
			}
		case *ast.ValueSpec:
			for _, id := range x.Names {
				out[id] = true
			}
		}

		return true
	})

	return out
}

func (r *rewriter) rewriteStreamUses(d ast.Decl) {
	if len(r.streams) == 0 {
		return
	}

	decls := r.declIdents(d)

	var stack []ast.Node

	ast.Inspect(d, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]

			return true
		}

		stack = append(stack, n)

		if id, ok := n.(*ast.Ident); ok && !decls[id] && r.streams[id.Name] != nil {
			r.streamUse(r.streams[id.Name], id, stack)
		}

		return true
	})
}

// Stack offsets from the identifier at the top: selector, call, statement, block.
const (
	stackSel   = 2
	stackCall  = 3
	stackStmt  = 4
	stackBlock = 5
)

func (r *rewriter) streamUse(s *stream, id *ast.Ident, stack []ast.Node) {
	n := len(stack)
	if n < stackCall {
		s.bad = true

		return
	}

	sel, isSel := stack[n-stackSel].(*ast.SelectorExpr)
	call, isCall := stack[n-stackCall].(*ast.CallExpr)

	if !isSel || sel.X != id || !isCall || call.Fun != sel {
		s.bad = true

		r.reportf(id.Pos(), "%s escapes (not a plain method call); rewrite by hand", s.name)

		return
	}

	var stmt *ast.ExprStmt

	if n >= stackBlock && stack[n-stackBlock] == ast.Node(s.block) {
		stmt, _ = stack[n-stackStmt].(*ast.ExprStmt)
	}

	var handled bool
	if s.decoder {
		handled = r.decoderMethod(s, call, sel.Sel.Name, stmt)
	} else {
		handled = r.encoderMethod(s, call, sel.Sel.Name, stmt)
	}

	if !handled {
		s.bad = true

		r.reportf(call.Pos(), "%s.%s has no mechanical v2 equivalent; rewrite by hand", s.name, sel.Sel.Name)
	}
}

func (r *rewriter) decoderMethod(s *stream, call *ast.CallExpr, method string, stmt *ast.ExprStmt) bool {
	argc := len(call.Args)

	switch {
	case method == "Decode" && argc == 1:
		s.decodes = append(s.decodes, call)
	case method == "UseNumber" && argc == 0 && stmt != nil:
		s.opts = append(s.opts, "jsonx.UseNumber()")
		s.edits = append(s.edits, r.stmtEdit(stmt))
	case method == "DisallowUnknownFields" && argc == 0 && stmt != nil:
		s.opts = append(s.opts, r.jsonName+".RejectUnknownMembers(true)")
		s.edits = append(s.edits, r.stmtEdit(stmt))
	default:
		return false
	}

	return true
}

func (r *rewriter) encoderMethod(s *stream, call *ast.CallExpr, method string, stmt *ast.ExprStmt) bool {
	argc := len(call.Args)

	switch {
	case method == "Encode" && argc == 1:
		s.edits = append(s.edits, edit{
			start: r.off(call.Pos()), end: r.off(call.Args[0].Pos()),
			text: r.jsonName + ".MarshalEncode(" + s.name + ", ",
		})
	case method == "SetEscapeHTML" && argc == 1 && stmt != nil:
		s.opts = append(s.opts, "jsontext.EscapeForHTML("+r.text(call.Args[0], call.Args[0])+")")
		s.edits = append(s.edits, r.stmtEdit(stmt))
	case method == "SetIndent" && argc == 2 && stmt != nil:
		s.opts = append(s.opts, r.indentOpts(call.Args[0], call.Args[1])...)
		s.edits = append(s.edits, r.stmtEdit(stmt))
	default:
		return false
	}

	return true
}

func (r *rewriter) indentOpts(prefix, indent ast.Expr) []string {
	opts := []string{"jsontext.Multiline(true)"}

	if lit, ok := prefix.(*ast.BasicLit); !ok || lit.Value != `""` {
		opts = append(opts, "jsontext.WithIndentPrefix("+r.text(prefix, prefix)+")")
	}

	if lit, ok := indent.(*ast.BasicLit); !ok || lit.Value != `""` {
		opts = append(opts, "jsontext.WithIndent("+r.text(indent, indent)+")")
	}

	return opts
}

func (r *rewriter) stmtEdit(s *ast.ExprStmt) edit {
	start, end := r.off(s.Pos()), r.off(s.End())

	for start > 0 && (r.src[start-1] == ' ' || r.src[start-1] == '\t') {
		start--
	}

	if end < len(r.src) && r.src[end] == '\n' {
		end++
	}

	return edit{start: start, end: end}
}

func (r *rewriter) emitStreams() {
	for _, s := range r.streams {
		fun, ok := s.call.Fun.(*ast.SelectorExpr)
		if s.bad || !ok {
			continue
		}

		r.edits = append(r.edits, s.edits...)

		opts := ", " + optV1
		if len(s.opts) > 0 {
			opts += ", " + strings.Join(s.opts, ", ")
		}

		if !s.decoder {
			r.add(r.off(fun.Pos()), r.off(fun.End()), "jsontext.NewEncoder")
			end := r.off(s.call.Args[0].End())
			r.add(end, end, opts)

			continue
		}

		r.add(r.off(fun.Pos()), r.off(fun.End()), "jsontext.NewDecoder")

		for _, c := range s.decodes {
			r.add(r.off(c.Pos()), r.off(c.Args[0].Pos()), r.jsonName+".UnmarshalDecode("+s.name+", ")
			r.add(r.off(c.Args[0].End()), r.off(c.End()), opts+")")
		}
	}
}

func (r *rewriter) rewriteCalls(d ast.Decl) {
	ast.Inspect(d, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		r.callExpr(call)

		return true
	})
}

func (r *rewriter) callExpr(call *ast.CallExpr) {
	argc := len(call.Args)

	switch {
	case r.isJSON(call.Fun, "Marshal") && argc == 1, r.isJSON(call.Fun, "Unmarshal") && argc == 2:
		r.consumed[call.Fun] = true
		end := r.off(call.Args[len(call.Args)-1].End())
		r.add(end, end, ", "+optV1)
	case r.isJSON(call.Fun, "MarshalIndent") && argc == 3:
		r.marshalIndent(call)
	case r.isJSON(call.Fun, "Valid") && argc == 1:
		r.consumed[call.Fun] = true
		r.add(r.off(call.Pos()), r.off(call.Args[0].Pos()), "jsontext.Value(")
		r.add(r.off(call.Args[0].End()), r.off(call.End()), ").IsValid("+optV1+")")
	default:
		r.chain(call)
	}
}

func (r *rewriter) marshalIndent(call *ast.CallExpr) {
	for _, arg := range call.Args[1:] {
		lit, isLit := arg.(*ast.BasicLit)
		if !isLit {
			continue
		}

		if s, err := strconv.Unquote(lit.Value); err == nil && strings.Trim(s, " \t\r\n") != "" {
			r.reportf(call.Pos(), "MarshalIndent with non-whitespace indent %s; v2 panics, rewrite by hand", lit.Value)

			return
		}
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}

	r.consumed[call.Fun] = true
	r.add(r.off(sel.Sel.Pos()), r.off(sel.Sel.End()), "Marshal")

	opts := append([]string{optV1}, r.indentOpts(call.Args[1], call.Args[2])...)
	r.add(r.off(call.Args[0].End()), r.off(call.Args[2].End()), ", "+strings.Join(opts, ", "))
}

func (r *rewriter) chain(call *ast.CallExpr) {
	outer, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}

	inner, ok := outer.X.(*ast.CallExpr)
	if !ok || len(inner.Args) != 1 || len(call.Args) != 1 {
		return
	}

	start, end := r.off(inner.Args[0].End()), r.off(call.Args[0].Pos())

	switch {
	case r.isJSON(inner.Fun, "NewDecoder") && outer.Sel.Name == "Decode":
		if r.hasComment(r.off(call.Pos()), r.off(call.End())) {
			r.reportf(call.Pos(), "comment inside NewDecoder(...).Decode chain; rewrite by hand")

			return
		}

		r.consumed[inner.Fun] = true
		r.add(r.off(call.Pos()), r.off(inner.Args[0].Pos()), r.jsonName+".UnmarshalDecode(jsontext.NewDecoder(")
		r.add(start, end, "), ")
		r.add(r.off(call.Args[0].End()), r.off(call.End()), ", "+optV1+")")
	case r.isJSON(inner.Fun, "NewEncoder") && outer.Sel.Name == "Encode":
		if r.hasComment(r.off(call.Pos()), r.off(call.End())) {
			r.reportf(call.Pos(), "comment inside NewEncoder(...).Encode chain; rewrite by hand")

			return
		}

		r.consumed[inner.Fun] = true
		r.add(r.off(call.Pos()), r.off(inner.Args[0].Pos()), r.jsonName+".MarshalEncode(jsontext.NewEncoder(")
		r.add(start, end, ", "+optV1+"), ")
		r.add(r.off(call.Args[0].End()), r.off(call.End()), ")")
	}
}

func (r *rewriter) rewriteSelectors(d ast.Decl) {
	ast.Inspect(d, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || r.consumed[sel] || !r.isJSON(sel, "") {
			return true
		}

		switch sel.Sel.Name {
		case "RawMessage":
			r.add(r.off(sel.Pos()), r.off(sel.End()), "jsontext.Value")
		case "Number":
			r.add(r.off(sel.Pos()), r.off(sel.End()), "jsonx.Number")
		case "Marshaler", "Unmarshaler":
		default:
			r.reportf(sel.Pos(), "%s.%s has no mechanical v2 equivalent; rewrite by hand", r.jsonName, sel.Sel.Name)
		}

		return true
	})
}

func (r *rewriter) apply() ([]byte, error) {
	sort.SliceStable(r.edits, func(i, j int) bool { return r.edits[i].start > r.edits[j].start })

	out := append([]byte(nil), r.src...)
	prev := len(out) + 1

	for _, e := range r.edits {
		if e.end > prev {
			return nil, fmt.Errorf("%w at offset %d", errOverlap, e.start)
		}

		out = append(out[:e.start], append([]byte(e.text), out[e.end:]...)...)
		prev = e.start
	}

	return out, nil
}

type importInfo struct {
	v1Spec     *ast.ImportSpec
	firstLocal *ast.ImportSpec
	last       *ast.ImportSpec
	have       map[string]bool
	used       map[string]bool
}

func scanImports(file *ast.File) importInfo {
	info := importInfo{have: map[string]bool{}, used: map[string]bool{}}

	for _, imp := range file.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		info.have[p] = true
		info.last = imp

		if p == v1Path {
			info.v1Spec = imp
		}

		if info.firstLocal == nil && strings.HasPrefix(p, localPrefix) {
			info.firstLocal = imp
		}
	}

	ast.Inspect(file, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if id, isID := sel.X.(*ast.Ident); isID {
				info.used[id.Name] = true
			}
		}

		return true
	})

	return info
}

// fixImports swaps the v1 import for v2 (or drops it when unused after the
// rewrite) and adds jsontext / jsonx when referenced.
func fixImports(path string, src []byte, jsonName string) ([]byte, error) {
	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("rewritten file does not parse: %w", err)
	}

	info := scanImports(file)
	off := func(p token.Pos) int { return fset.Position(p).Offset }
	spec := info.v1Spec

	if decl := importDecl(file, spec); !decl.Lparen.IsValid() {
		block := "import (\n\t" + string(src[off(spec.Pos()):off(spec.End())]) + "\n)"
		out := append(append(append([]byte(nil), src[:off(decl.Pos())]...), block...), src[off(decl.End()):]...)

		return fixImports(path, out, jsonName)
	}

	var edits []edit

	if info.used[jsonName] {
		edits = append(edits, edit{start: off(spec.Path.Pos()), end: off(spec.Path.End()), text: strconv.Quote(v2Path)})
	} else {
		start, end := off(spec.Pos()), off(spec.End())
		for start > 0 && (src[start-1] == ' ' || src[start-1] == '\t') {
			start--
		}

		if end < len(src) && src[end] == '\n' {
			end++
		}

		edits = append(edits, edit{start: start, end: end})
	}

	if info.used["jsontext"] && !info.have[jsontextPath] {
		edits = append(
			edits,
			edit{start: off(spec.End()), end: off(spec.End()), text: "\n\t" + strconv.Quote(jsontextPath)},
		)
	}

	if info.used["jsonx"] && !info.have[jsonxPath] {
		at, text := off(info.last.End()), "\n\n\t"+strconv.Quote(jsonxPath)
		if info.firstLocal != nil {
			at, text = off(info.firstLocal.Pos()), strconv.Quote(jsonxPath)+"\n\t"
		}

		edits = append(edits, edit{start: at, end: at, text: text})
	}

	sort.SliceStable(edits, func(i, j int) bool { return edits[i].start > edits[j].start })

	out := append([]byte(nil), src...)
	for _, e := range edits {
		out = append(out[:e.start], append([]byte(e.text), out[e.end:]...)...)
	}

	return format.Source(out)
}

func importDecl(file *ast.File, spec *ast.ImportSpec) *ast.GenDecl {
	for _, d := range file.Decls {
		if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.IMPORT && g.Pos() <= spec.Pos() && spec.End() <= g.End() {
			return g
		}
	}

	return nil
}
