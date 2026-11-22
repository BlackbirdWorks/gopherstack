package timestreamquery

import (
	"slices"
	"strings"
	"time"
)

type column struct {
	rel  string
	name string
	typ  tsType
}

type scope struct {
	vars  map[string]tsType
	outer *scope
	cols  []column
}

func (s *scope) withVars(vars map[string]tsType) *scope {
	return &scope{vars: vars, outer: s}
}

type outCol struct {
	name string
	typ  tsType
}

// inferrer computes static types for a parsed query and collects the text edits that depend on them.
type inferrer struct {
	tables      loadedSet
	vars        map[string]string
	rules       map[string]funcRule
	now         time.Time
	scans       []pruneScan
	ctes        []map[string][]outCol
	edits       []typedEdit
	params      []outCol
	nanRoots    map[*node]bool
	unsupported []string
	steps       int
	tryDepth    int
}

const maxInferSteps = 200000

func newInferrer(tables loadedSet, now time.Time, vars map[string]string) *inferrer {
	return &inferrer{tables: tables, rules: funcRules(), now: now, vars: vars}
}

func (inf *inferrer) unsupportedf(reason string) tsType {
	inf.unsupported = append(inf.unsupported, reason)

	return tsType{}
}

func (inf *inferrer) edit(text string, start, end int) {
	inf.edits = append(inf.edits, typedEdit{text: text, start: start, end: end, seq: len(inf.edits)})
}

func (inf *inferrer) closeEdit(text string, pos int) {
	inf.edits = append(inf.edits, typedEdit{text: text, start: pos, end: pos, seq: len(inf.edits), closing: true})
}

// checkedEdit is an edit that only routes arithmetic or a cast through a checked function; it can be dropped.
func (inf *inferrer) checkedEdit(text string, start, end int) {
	inf.edits = append(inf.edits, typedEdit{text: text, start: start, end: end, seq: len(inf.edits), checked: true})
}

func (inf *inferrer) checkedClose(text string, pos int) {
	inf.edits = append(inf.edits, typedEdit{
		text: text, start: pos, end: pos, seq: len(inf.edits), closing: true, checked: true,
	})
}

func fieldType(t tsType, name string) tsType {
	if t.kind != tyRow {
		return tsType{}
	}

	for _, f := range t.fields {
		if strings.EqualFold(f.name, name) {
			return f.typ
		}
	}

	return tsType{}
}

func (s *scope) lookupVar(name string) (tsType, bool) {
	for c := s; c != nil; c = c.outer {
		for k, v := range c.vars {
			if strings.EqualFold(k, name) {
				return v, true
			}
		}
	}

	return tsType{}, false
}

func (s *scope) lookupColumn(rel, name string) (tsType, bool) {
	for c := s; c != nil; c = c.outer {
		for _, col := range c.cols {
			if strings.EqualFold(col.name, name) && (rel == "" || strings.EqualFold(col.rel, rel)) {
				return col.typ, true
			}
		}
	}

	return tsType{}, false
}

func (inf *inferrer) resolveColumn(parts []string, sc *scope) tsType {
	if t, ok := sc.lookupVar(parts[0]); ok {
		return fieldChain(t, parts[1:])
	}

	if len(parts) >= argsTwo {
		if t, ok := sc.lookupColumn(parts[len(parts)-argsTwo], parts[len(parts)-1]); ok && len(parts) <= argsThree {
			return t
		}
	}

	if t, ok := sc.lookupColumn("", parts[0]); ok {
		return fieldChain(t, parts[1:])
	}

	if len(parts) == 1 {
		return bareFunctionType(parts[0])
	}

	return tsType{}
}

func fieldChain(t tsType, fields []string) tsType {
	for _, f := range fields {
		t = fieldType(t, f)
	}

	return t
}

func (inf *inferrer) inferAll(nodes []*node, sc *scope) []tsType {
	out := make([]tsType, len(nodes))
	for i, n := range nodes {
		out[i] = inf.infer(n, sc)
	}

	return out
}

func (inf *inferrer) infer(n *node, sc *scope) tsType {
	if n == nil {
		return tsType{}
	}

	if inf.steps++; inf.steps > maxInferSteps {
		return tsType{}
	}

	switch n.kind {
	case nLiteral, nInterval:
		return n.typ
	case nTypedLit:
		inf.rewriteTypedLiteral(n)

		return n.typ
	case nVar:
		inf.noteParam(n.name)

		if strings.EqualFold(n.name, "@scheduled_runtime") {
			return scalar(tyTimestamp)
		}

		return inf.unsupportedf("query variable " + n.name)
	case nParam:
		return inf.unsupportedf("positional parameter")
	case nColumn:
		return inf.resolveColumn(n.parts, sc)
	case nStar:
		return tsType{}
	case nCall:
		return inf.inferCall(n, sc)
	case nBinary, nUnary:
		return inf.inferOperator(n, sc)
	case nLambda:
		return inf.unsupportedf("lambda outside a higher-order function")
	default:
		return inf.inferCompound(n, sc)
	}
}

func (inf *inferrer) inferCompound(n *node, sc *scope) tsType {
	switch n.kind {
	case nCase:
		return inf.inferCase(n, sc)
	case nCast:
		if n.typ.kind != tyDouble {
			inf.keepNaN(n.args[0])
		}

		inf.rewriteCast(n, inf.infer(n.args[0], sc))

		return n.typ
	case nBetween, nInList, nLike:
		inf.bindParams(n.args, inf.inferAll(n.args, sc))

		return scalar(tyBoolean)
	case nIsNull:
		inf.inferAll(n.args, sc)

		return scalar(tyBoolean)
	case nInSub, nExists:
		inf.inferAll(n.args, sc)
		inf.inferQuery(n.sub, sc)

		return scalar(tyBoolean)
	case nSubquery:
		cols := inf.inferQuery(n.sub, sc)
		if len(cols) != 1 {
			return tsType{}
		}

		return cols[0].typ
	case nArrayCtor:
		return inf.inferArrayCtor(n, sc)
	case nRowCtor:
		return inf.inferRowCtor(n, sc)
	case nSubscript:
		return inf.inferSubscript(n, sc)
	case nField:
		return fieldType(inf.infer(n.args[0], sc), n.name)
	default:
		return tsType{}
	}
}

func (inf *inferrer) inferSubscript(n *node, sc *scope) tsType {
	base := inf.infer(n.args[0], sc)
	inf.infer(n.args[1], sc)

	switch base.kind {
	case tyArray:
		return *base.elem
	case tyRow:
		return rowSubscript(base, n.args[1])
	default:
		return tsType{}
	}
}

// rowSubscript types row[i] for an integer literal i, counting fields from one.
func rowSubscript(row tsType, idx *node) tsType {
	i, ok := atoiSafe(idx.name)
	if idx.kind != nLiteral || !ok || i < 1 || i > len(row.fields) {
		return tsType{}
	}

	return row.fields[i-1].typ
}

func (inf *inferrer) inferCase(n *node, sc *scope) tsType {
	args := inf.inferAll(n.args, sc)

	skip := 0
	if n.operand {
		skip = 1
	}

	res := scalar(tyUnknown)
	pairs := len(args) - skip

	if n.hasElse {
		pairs--
		res = args[len(args)-1]
	}

	for i := skip + 1; i < skip+1+pairs; i += argsTwo {
		res = supertype(res, args[i])
	}

	return res
}

func (inf *inferrer) inferArrayCtor(n *node, sc *scope) tsType {
	res := scalar(tyUnknown)
	for _, t := range inf.inferAll(n.args, sc) {
		res = supertype(res, t)
	}

	if res.kind == tyInvalid {
		return tsType{}
	}

	return arrayOf(res)
}

func (inf *inferrer) inferRowCtor(n *node, sc *scope) tsType {
	fields := make([]tsField, len(n.args))
	for i, t := range inf.inferAll(n.args, sc) {
		fields[i] = tsField{typ: t}
	}

	return rowOf(fields)
}

func (inf *inferrer) inferOperator(n *node, sc *scope) tsType {
	inf.markNaNOperands(n)

	args := inf.inferAll(n.args, sc)
	inf.bindParams(n.args, args)

	if n.kind == nUnary {
		if n.name == opNot {
			return scalar(tyBoolean)
		}

		inf.rewriteNegate(n, args[0])

		return args[0]
	}

	switch n.name {
	case opAnd, "OR", "=", "<>", "!=", "<", ">", "<=", ">=", "IS DISTINCT":
		inf.rewriteCompare(n, args[0], args[1])

		return scalar(tyBoolean)
	case "||":
		return concatType(args[0], args[1])
	}

	if n.name == "-" && args[0].kind == tyTimestamp && args[1].kind == tyTimestamp {
		inf.edit("ts_tsdiff(", n.start, n.start)
		inf.edit(",", n.opStart, n.opEnd)
		inf.closeEdit(")", n.end)

		return scalar(tyIntervalDS)
	}

	res := arithType(n.name, args[0], args[1])
	inf.rewriteIntervalArith(n, args[0], args[1])
	inf.rewriteArith(n, args[0], args[1], res)

	return res
}

func concatType(a, b tsType) tsType {
	if a.kind == tyArray || b.kind == tyArray {
		return supertype(a, b)
	}

	if a.kind == tyVarchar || b.kind == tyVarchar || a.kind == tyUnknown && b.kind == tyUnknown {
		return scalar(tyVarchar)
	}

	return tsType{}
}

func arithType(op string, a, b tsType) tsType {
	switch {
	case a.kind == tyInvalid || b.kind == tyInvalid:
		return tsType{}
	case a.isNumeric() && b.isNumeric():
		return supertype(a, b)
	case a.kind == tyUnknown && (b.isNumeric() || b.isInterval()):
		return b
	case b.kind == tyUnknown && (a.isNumeric() || a.isInterval()):
		return a
	}

	return intervalArith(op, a, b)
}

func intervalArith(op string, a, b tsType) tsType {
	addSub := op == "+" || op == "-"

	switch {
	case a.isTemporal() && b.isInterval() && addSub:
		return a
	case a.isInterval() && b.isTemporal() && op == "+":
		return b
	case a.isInterval() && b.isInterval() && addSub:
		return a
	}

	return scaleOrDiff(op, a, b)
}

func scaleOrDiff(op string, a, b tsType) tsType {
	switch {
	case a.isInterval() && b.isNumeric() && (op == "*" || op == "/"):
		return a
	case a.isNumeric() && b.isInterval() && op == "*":
		return b
	case a.kind == b.kind && (a.kind == tyDate || a.kind == tyTime) && op == "-":
		return scalar(tyIntervalDS)
	}

	return tsType{}
}

// inferQuery types every output column of a query expression; clauses are visited for their edits.
func (inf *inferrer) inferQuery(qn *queryNode, outer *scope) []outCol {
	frame := map[string][]outCol{}
	inf.ctes = append(inf.ctes, frame)

	defer func() { inf.ctes = inf.ctes[:len(inf.ctes)-1] }()

	for _, c := range qn.ctes {
		cols := inf.inferQuery(c.query, outer)
		frame[strings.ToLower(c.name)] = renameCols(cols, c.cols)
	}

	var cols []outCol

	if qn.setOp != "" {
		cols = inf.inferSetOp(qn, outer)
	} else {
		cols = inf.inferSelect(qn.sel, qn, outer)
	}

	return cols
}

func renameCols(cols []outCol, names []string) []outCol {
	out := slices.Clone(cols)
	for i := range names {
		if i < len(out) {
			out[i].name = names[i]
		}
	}

	return out
}

func (inf *inferrer) inferSetOp(qn *queryNode, outer *scope) []outCol {
	l, r := inf.inferQuery(qn.left, outer), inf.inferQuery(qn.right, outer)
	out := slices.Clone(l)

	for i := range out {
		if i >= len(r) {
			out[i].typ = tsType{}

			continue
		}

		out[i].typ = supertype(l[i].typ, r[i].typ)
	}

	inf.orderBy(qn, &scope{outer: outer}, out)

	return out
}

func (inf *inferrer) orderBy(qn *queryNode, sc *scope, out []outCol) {
	if len(qn.orderBy) == 0 && len(qn.limit) == 0 {
		return
	}

	alias := &scope{outer: sc}
	for _, c := range out {
		if c.name != "" {
			alias.cols = append(alias.cols, column{name: c.name, typ: c.typ})
		}
	}

	for _, o := range qn.orderBy {
		inf.infer(o.expr, alias)
	}
}

func (inf *inferrer) inferSelect(sel *selectSpec, qn *queryNode, outer *scope) []outCol {
	sc := &scope{outer: outer}
	if sel.from != nil {
		sc.cols = inf.relationColumns(sel.from, outer)
		inf.recordScans(sel)
	}

	inf.infer(sel.where, sc)

	for _, g := range sel.groupBy {
		inf.infer(g, sc)
	}

	inf.infer(sel.having, sc)

	out := make([]outCol, 0, len(sel.items))

	for _, it := range sel.items {
		out = append(out, inf.inferItem(it, sc)...)
	}

	inf.orderBy(qn, sc, out)

	return out
}

func itemName(it selectItem) string {
	if it.alias != "" {
		return it.alias
	}

	if it.expr != nil && it.expr.kind == nColumn {
		return it.expr.parts[len(it.expr.parts)-1]
	}

	return ""
}

func (inf *inferrer) inferItem(it selectItem, sc *scope) []outCol {
	if !it.star {
		if len(inf.ctes) == 1 {
			inf.keepNaN(it.expr)
		}

		return []outCol{{name: itemName(it), typ: inf.infer(it.expr, sc)}}
	}

	var out []outCol

	for _, c := range sc.cols {
		if len(it.qual) == 0 || strings.EqualFold(c.rel, it.qual[len(it.qual)-1]) {
			out = append(out, outCol{name: c.name, typ: c.typ})
		}
	}

	return out
}

func (inf *inferrer) lookupCTE(name string) ([]outCol, bool) {
	for _, frame := range slices.Backward(inf.ctes) {
		if cols, ok := frame[strings.ToLower(name)]; ok {
			return cols, true
		}
	}

	return nil, false
}

func qualify(rel string, cols []outCol, aliases []string) []column {
	out := make([]column, len(cols))
	for i, c := range renameCols(cols, aliases) {
		out[i] = column{rel: rel, name: c.name, typ: c.typ}
	}

	return out
}

func (inf *inferrer) relationColumns(r *relation, outer *scope) []column {
	switch r.kind {
	case relTable:
		return inf.tableColumns(r)
	case relSubquery:
		return qualify(r.alias, inf.inferQuery(r.sub, outer), r.colAliases)
	case relJoin:
		return inf.joinColumns(r, outer)
	case relUnnest:
		return inf.unnestColumns(r, outer)
	case relValues:
		return inf.valuesColumns(r, outer)
	}

	return nil
}

func (inf *inferrer) tableColumns(r *relation) []column {
	rel := r.alias
	if rel == "" {
		rel = r.ref[len(r.ref)-1]
	}

	if len(r.ref) == 1 {
		if cols, ok := inf.lookupCTE(r.ref[0]); ok {
			return qualify(rel, cols, r.colAliases)
		}

		inf.unsupportedf("unresolved relation " + r.ref[0])

		return nil
	}

	t, ok := inf.tables[tableKey(r.ref[0], r.ref[1])]
	if !ok || len(r.ref) != argsTwo {
		inf.unsupportedf("unresolved table")

		return nil
	}

	cols := make([]outCol, len(t.schema.cols))
	for i, c := range t.schema.cols {
		cols[i] = outCol{name: c.name, typ: typeFromDecl(c.typ)}
	}

	return qualify(rel, cols, r.colAliases)
}

func (inf *inferrer) joinColumns(r *relation, outer *scope) []column {
	left, right := inf.relationColumns(r.left, outer), inf.relationColumns(r.right, outer)
	all := slices.Concat(left, right)
	inf.infer(r.on, &scope{cols: all, outer: outer})

	if len(r.using) == 0 {
		return all
	}

	var merged []column

	isUsing := func(c column) bool {
		return slices.ContainsFunc(r.using, func(u string) bool { return strings.EqualFold(u, c.name) })
	}

	for _, u := range r.using {
		for _, c := range left {
			if strings.EqualFold(c.name, u) {
				merged = append(merged, c)

				break
			}
		}
	}

	for _, c := range left {
		if !isUsing(c) {
			merged = append(merged, c)
		}
	}

	for _, c := range right {
		if !isUsing(c) {
			merged = append(merged, c)
		}
	}

	return merged
}

func (inf *inferrer) unnestColumns(r *relation, outer *scope) []column {
	var cols []outCol

	sc := &scope{outer: outer}

	for _, a := range r.args {
		t := inf.infer(a, sc)

		switch t.kind {
		case tyArray:
			cols = append(cols, outCol{typ: *t.elem})
		case tyTimeseries:
			cols = append(cols, outCol{name: colTime, typ: scalar(tyTimestamp)}, outCol{name: colValue, typ: *t.elem})
		default:
			cols = append(cols, outCol{})
		}
	}

	if r.ordinality {
		cols = append(cols, outCol{name: "ordinality", typ: scalar(tyBigint)})
	}

	return qualify(r.alias, cols, r.colAliases)
}

func (inf *inferrer) valuesColumns(r *relation, outer *scope) []column {
	var cols []outCol

	sc := &scope{outer: outer}

	for _, row := range r.rows {
		for i, t := range inf.inferAll(row, sc) {
			if i >= len(cols) {
				cols = append(cols, outCol{typ: t})

				continue
			}

			cols[i].typ = supertype(cols[i].typ, t)
		}
	}

	return qualify(r.alias, cols, r.colAliases)
}

func (inf *inferrer) inferCall(n *node, sc *scope) tsType {
	lw := strings.ToLower(n.name)

	if t, ok := inf.inferHigherOrder(lw, n, sc); ok {
		return t
	}

	if lw == "try" {
		inf.tryDepth++
		defer func() { inf.tryDepth-- }()
	}

	if isFloatClassFunc(lw) && len(n.args) == 1 {
		inf.keepNaN(n.args[0])
	}

	args := inf.inferAll(n.args, sc)

	if lw == "typeof" && len(args) == 1 && !n.over {
		return inf.inferTypeof(n, args[0])
	}

	rule, ok := inf.rules[lw]
	if !ok {
		return inf.unsupportedf("function " + lw)
	}

	res := rule(&callCtx{inf: inf, n: n, args: args})
	inf.rewriteCheckedCall(lw, n, args)

	return res
}

func (inf *inferrer) inferTypeof(n *node, arg tsType) tsType {
	if arg.known() {
		inf.edit("(CASE WHEN 0 THEN ", n.start, n.start+len(n.name))
		inf.closeEdit(" ELSE '"+strings.ReplaceAll(arg.typeofName(), "'", "''")+"' END)", n.end)
	}

	return scalar(tyVarchar)
}

func lambdaParam(t tsType) tsType {
	if t.kind == tyTimeseries {
		return rowOf([]tsField{{name: colTime, typ: scalar(tyTimestamp)}, {name: colValue, typ: *t.elem}})
	}

	if t.kind == tyArray {
		return *t.elem
	}

	return tsType{}
}

func (inf *inferrer) lambdaBody(l *node, sc *scope, params ...tsType) tsType {
	if l.kind != nLambda || len(l.params) != len(params) || len(l.args) == 0 {
		return tsType{}
	}

	vars := map[string]tsType{}
	for i, t := range params {
		if i < len(l.params) {
			vars[l.params[i]] = t
		}
	}

	return inf.infer(l.arg(0), sc.withVars(vars))
}

// inferHigherOrder types filter/transform/reduce and the *_match functions, whose lambdas see the element type.
func (inf *inferrer) inferHigherOrder(lw string, n *node, sc *scope) (tsType, bool) {
	switch lw {
	case fnFilter, "transform", "any_match", "all_match", "none_match":
		if len(n.args) != argsTwo {
			return tsType{}, false
		}

		subject := inf.infer(n.args[0], sc)
		body := inf.lambdaBody(n.args[1], sc, lambdaParam(subject))

		return inf.higherOrderResult(lw, n, subject, body), true
	case "reduce":
		if len(n.args) != argsFour {
			return tsType{}, false
		}

		return inf.inferReduce(n, sc), true
	case "zip_with":
		if len(n.args) != argsThree {
			return tsType{}, false
		}

		return inf.inferZipWith(n, sc), true
	case "regexp_replace":
		if len(n.args) != argsThree || n.args[2].kind != nLambda {
			return tsType{}, false
		}

		inf.inferAll(n.args[:argsTwo], sc)
		inf.lambdaBody(n.args[2], sc, arrayOf(scalar(tyVarchar)))

		return scalar(tyVarchar), true
	}

	return tsType{}, false
}

func (inf *inferrer) inferZipWith(n *node, sc *scope) tsType {
	left, right := inf.infer(n.args[0], sc), inf.infer(n.args[1], sc)
	body := inf.lambdaBody(n.args[2], sc, lambdaParam(left), lambdaParam(right))

	if left.kind == tyArray && right.kind == tyArray && body.known() {
		return arrayOf(body)
	}

	return tsType{}
}

func (inf *inferrer) higherOrderResult(lw string, n *node, subject, body tsType) tsType {
	switch lw {
	case fnFilter:
		if subject.kind == tyTimeseries && isLambda(n.args[1], 1) {
			inf.edit("ts_", n.start, n.start)
		}

		return subject
	case "transform":
		if subject.kind == tyArray && body.known() {
			return arrayOf(body)
		}

		return tsType{}
	}

	return scalar(tyBoolean)
}

const argsFour = 4

func isLambda(n *node, params int) bool { return n.kind == nLambda && len(n.params) == params }

func (inf *inferrer) inferReduce(n *node, sc *scope) tsType {
	subject, init := inf.infer(n.args[0], sc), inf.infer(n.args[1], sc)
	elem := lambdaParam(subject)

	if subject.kind == tyTimeseries && isLambda(n.args[2], argsTwo) && isLambda(n.args[3], 1) {
		inf.edit("ts_", n.start, n.start)
	}

	state := init
	if step := inf.lambdaBody(n.args[2], sc, init, elem); step.known() {
		state = supertype(init, step)
	}

	return inf.lambdaBody(n.args[3], sc, state)
}

// bareFunctionType types the parenthesis-free SQL functions such as current_date.
func bareFunctionType(name string) tsType {
	switch strings.ToLower(name) {
	case "current_date":
		return scalar(tyDate)
	case "current_time", "localtime":
		return scalar(tyTime)
	case fnCurTS, fnLocalTS:
		return scalar(tyTimestamp)
	}

	return tsType{}
}
