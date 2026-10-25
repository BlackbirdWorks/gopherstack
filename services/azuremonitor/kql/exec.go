package kql

import (
	"slices"
	"strconv"
	"strings"
	"time"
)

// Options configures a query execution.
type Options struct {
	Now      time.Time
	Resolver Resolver
}

// Parse checks that src is syntactically valid and uses only supported operators.
func Parse(src string) error {
	_, err := parseQuery(src)

	return err
}

// Execute runs a query and returns its single result table.
func Execute(src string, opt Options) (*Rel, error) {
	q, err := parseQuery(src)
	if err != nil {
		return nil, err
	}

	if opt.Now.IsZero() {
		opt.Now = time.Now().UTC()
	}

	e := &env{now: opt.Now, res: opt.Resolver, lets: map[string]*letDef{}}
	for _, l := range q.lets {
		e.lets[l.name] = &letDef{def: l.def}
	}

	if _, isIdent := q.body.(*identNode); !isTabularNode(q.body) && !isIdent {
		return nil, semErr("a query must be a tabular expression")
	}

	rel, err := e.exec(q.body)
	if err != nil {
		return nil, err
	}

	return rel, nil
}

func (e *env) exec(n node) (*Rel, error) {
	switch x := n.(type) {
	case *pipeNode:
		rel, err := e.exec(x.src)
		if err != nil {
			return nil, err
		}

		for _, op := range x.ops {
			if rel, err = e.apply(op, rel); err != nil {
				return nil, err
			}
		}

		return rel, nil
	case *tableRef:
		return e.table(x.workspace, x.name)
	case *identNode:
		return e.execIdent(x)
	case *unionNode:
		return e.union(nil, x.tables)
	case *datatableNode:
		return e.datatable(x)
	case *rangeNode:
		return e.rangeTable(x)
	case *printNode:
		return e.printTable(x)
	}

	return nil, semErr("a tabular expression was expected")
}

func (e *env) execIdent(x *identNode) (*Rel, error) {
	if def, ok := e.lets[x.name]; ok {
		if e.depth > maxLetDepth {
			return nil, semErr("let statement '%s' is recursive", x.name)
		}

		e.depth++
		defer func() { e.depth-- }()

		if !isTabularNode(def.def) {
			if _, isIdent := def.def.(*identNode); !isIdent {
				return nil, semErr("'%s' is a scalar and cannot be used as a table", x.name)
			}
		}

		return e.exec(def.def)
	}

	return e.table("", x.name)
}

func (e *env) table(ws, name string) (*Rel, error) {
	if e.res != nil {
		if t, ok := e.res.Table(ws, name); ok {
			return t, nil
		}
	}

	return nil, semErr("Failed to resolve table or column expression named '%s'", name)
}

func (e *env) union(base *Rel, tables []node) (*Rel, error) {
	var parts []*Rel
	if base != nil {
		parts = append(parts, base)
	}

	for _, t := range tables {
		r, err := e.exec(t)
		if err != nil {
			return nil, err
		}

		parts = append(parts, r)
	}

	return unionRels(parts)
}

func unionRels(parts []*Rel) (*Rel, error) {
	out := &Rel{}
	index := map[string]int{}

	for _, p := range parts {
		if err := mergeSchema(out, index, p.Cols); err != nil {
			return nil, err
		}
	}

	for _, p := range parts {
		for _, row := range p.Rows {
			out.Rows = append(out.Rows, widenRow(out.Cols, index, p.Cols, row))
		}
	}

	return out, nil
}

func mergeSchema(out *Rel, index map[string]int, cols []Col) error {
	for _, c := range cols {
		i, seen := index[c.Name]
		if !seen {
			index[c.Name] = len(out.Cols)
			out.Cols = append(out.Cols, c)

			continue
		}

		if out.Cols[i].Type != c.Type {
			return unsupported("union of columns with conflicting types ('" + c.Name + "')")
		}
	}

	return nil
}

// widenRow places a row of one union input into the merged schema (missing strings are empty).
func widenRow(merged []Col, index map[string]int, cols []Col, row []any) []any {
	nr := make([]any, len(merged))

	for j, c := range merged {
		if c.Type == TString {
			nr[j] = ""
		}
	}

	for j, c := range cols {
		nr[index[c.Name]] = row[j]
	}

	return nr
}

func (e *env) constScope() *scope { return &scope{env: e} }

func (e *env) datatable(x *datatableNode) (*Rel, error) {
	sc := e.constScope()
	out := &Rel{Cols: x.cols}

	for i := 0; i < len(x.vals); i += len(x.cols) {
		row := make([]any, len(x.cols))

		for j := range x.cols {
			s, err := sc.compile(x.vals[i+j])
			if err != nil {
				return nil, err
			}

			v, err := s.fn(nil)
			if err != nil {
				return nil, err
			}

			row[j] = coerceTo(x.cols[j].Type, v)
		}

		out.Rows = append(out.Rows, row)
	}

	return out, nil
}

func coerceTo(t Type, v any) any {
	if v == nil {
		return nil
	}

	switch t { //nolint:exhaustive // other types are stored as written
	case TReal:
		if f, ok := toFloat(v); ok {
			return f
		}
	case TLong, TInt:
		if n, ok := toInt(v); ok {
			return n
		}
	case TDatetime:
		if d, ok := parseIfString(v, ParseDatetime); ok {
			return d
		}
	case TTimespan:
		if d, ok := parseIfString(v, ParseTimespan); ok {
			return d
		}
	}

	return v
}

func parseIfString[T any](v any, parse func(string) (T, bool)) (T, bool) {
	s, ok := v.(string)
	if !ok {
		var zero T

		return zero, false
	}

	return parse(s)
}

const maxRangeRows = 1_000_000

const rangeArgs = 3

func (e *env) rangeTable(x *rangeNode) (*Rel, error) {
	sc := e.constScope()
	vals := make([]any, rangeArgs)

	for i, n := range []node{x.from, x.to, x.step} {
		s, err := sc.compile(n)
		if err != nil {
			return nil, err
		}

		if vals[i], err = s.fn(nil); err != nil {
			return nil, err
		}
	}

	switch from := vals[0].(type) {
	case int64:
		return rangeLong(x.name, from, intArg(vals[1]), intArg(vals[2]))
	case time.Time:
		to, ok1 := vals[1].(time.Time)
		step, ok2 := vals[2].(time.Duration)

		if !ok1 || !ok2 || step <= 0 {
			return nil, semErr("range over datetime needs a datetime end and a positive timespan step")
		}

		return rangeTime(x.name, from, to, step)
	}

	return nil, semErr("range supports long and datetime values")
}

func rangeLong(name string, from, to, step int64) (*Rel, error) {
	if step == 0 {
		return nil, semErr("range step must not be zero")
	}

	out := &Rel{Cols: []Col{{Name: name, Type: TLong}}}

	for v := from; (step > 0 && v <= to) || (step < 0 && v >= to); v += step {
		if len(out.Rows) >= maxRangeRows {
			return nil, semErr("range produces too many rows")
		}

		out.Rows = append(out.Rows, []any{v})
	}

	return out, nil
}

func rangeTime(name string, from, to time.Time, step time.Duration) (*Rel, error) {
	out := &Rel{Cols: []Col{{Name: name, Type: TDatetime}}}

	for v := from; !v.After(to); v = v.Add(step) {
		if len(out.Rows) >= maxRangeRows {
			return nil, semErr("range produces too many rows")
		}

		out.Rows = append(out.Rows, []any{v})
	}

	return out, nil
}

func (e *env) printTable(x *printNode) (*Rel, error) {
	sc := e.constScope()
	out := &Rel{}
	row := make([]any, 0, len(x.items))

	for i, it := range x.items {
		s, err := sc.compile(it.e)
		if err != nil {
			return nil, err
		}

		v, err := s.fn(nil)
		if err != nil {
			return nil, err
		}

		name := it.name
		if name == "" {
			name = "print_" + strconv.Itoa(i)
		}

		out.Cols = append(out.Cols, Col{Name: name, Type: s.typ})
		row = append(row, v)
	}

	out.Rows = [][]any{row}

	return out, nil
}

func (e *env) apply(op node, in *Rel) (*Rel, error) {
	sc := &scope{env: e, cols: in.Cols}

	if rel, handled, err := applyColumnOp(sc, in, op); handled {
		return rel, err
	}

	switch x := op.(type) {
	case *summarizeOp:
		return applySummarize(sc, in, x)
	case *sortOp:
		return applySort(sc, in, x.keys)
	case *topOp:
		return applyTop(sc, in, x)
	case *takeOp:
		return applyTake(sc, in, x)
	case *distinctOp:
		return applyDistinct(sc, in, x)
	case *countOp:
		return &Rel{Cols: []Col{{Name: x.name, Type: TLong}}, Rows: [][]any{{int64(len(in.Rows))}}}, nil
	case *unionOp:
		return e.union(in, x.tables)
	case *renderOp:
		return in, nil
	case *getschemaOp:
		return getSchema(in), nil
	}

	return nil, unsupported("this operator")
}

// applyColumnOp runs the operators that filter rows or reshape columns.
func applyColumnOp(sc *scope, in *Rel, op node) (*Rel, bool, error) {
	var (
		rel *Rel
		err error
	)

	switch x := op.(type) {
	case *whereOp:
		rel, err = applyWhere(sc, in, x)
	case *projectOp:
		rel, err = applyProject(sc, in, x.items, false)
	case *extendOp:
		rel, err = applyProject(sc, in, x.items, true)
	case *projectAwayOp:
		rel = selectCols(in, x.names, false)
	case *projectKeepOp:
		rel = selectCols(in, x.names, true)
	case *projectOrderOp:
		rel = reorderCols(in, x.names)
	case *projectRenOp:
		rel, err = renameCols(in, x.pairs)
	default:
		return nil, false, nil
	}

	return rel, true, err
}

func getSchema(in *Rel) *Rel {
	out := &Rel{Cols: []Col{
		{Name: "ColumnName", Type: TString}, {Name: "ColumnOrdinal", Type: TInt},
		{Name: "DataType", Type: TString}, {Name: "ColumnType", Type: TString},
	}}

	for i, c := range in.Cols {
		out.Rows = append(out.Rows, []any{c.Name, int64(i), dotnetType(c.Type), string(c.Type)})
	}

	return out
}

func dotnetType(t Type) string {
	switch t { //nolint:exhaustive // unknown maps to object
	case TString:
		return "System.String"
	case TLong:
		return "System.Int64"
	case TInt:
		return "System.Int32"
	case TReal:
		return "System.Double"
	case TBool:
		return "System.SByte"
	case TDatetime:
		return "System.DateTime"
	case TTimespan:
		return "System.TimeSpan"
	case TGUID:
		return "System.Guid"
	}

	return "System.Object"
}

func applyWhere(sc *scope, in *Rel, x *whereOp) (*Rel, error) {
	pred, err := sc.compile(x.e)
	if err != nil {
		return nil, err
	}

	if pred.typ != TBool && pred.typ != TDynamic && pred.typ != "" {
		return nil, semErr("where: a boolean predicate was expected but found %s", pred.typ)
	}

	out := &Rel{Cols: in.Cols}

	for _, row := range in.Rows {
		v, evalErr := pred.fn(row)
		if evalErr != nil {
			return nil, evalErr
		}

		if b, _ := v.(bool); b {
			out.Rows = append(out.Rows, row)
		}
	}

	return out, nil
}

func uniqueName(name string, used map[string]bool) string {
	if !used[name] {
		return name
	}

	for i := 1; ; i++ {
		cand := name + strconv.Itoa(i)
		if !used[cand] {
			return cand
		}
	}
}

func applyProject(sc *scope, in *Rel, items []namedExpr, extend bool) (*Rel, error) {
	if extend {
		return applyExtend(sc, in, items)
	}

	type outCol struct {
		s    scalar
		name string
	}

	used := map[string]bool{}
	counter := 0
	computed := make([]outCol, 0, len(items))

	for _, it := range items {
		s, err := sc.compile(it.e)
		if err != nil {
			return nil, err
		}

		name := uniqueName(outputName(it, &counter), used)
		used[name] = true
		computed = append(computed, outCol{name: name, s: s})
	}

	out := &Rel{Cols: make([]Col, len(computed)), Rows: make([][]any, 0, len(in.Rows))}
	for i, c := range computed {
		out.Cols[i] = Col{Name: c.name, Type: c.s.typ}
	}

	for _, row := range in.Rows {
		nr := make([]any, len(computed))

		for i, c := range computed {
			v, err := c.s.fn(row)
			if err != nil {
				return nil, err
			}

			nr[i] = v
		}

		out.Rows = append(out.Rows, nr)
	}

	return out, nil
}

// outputName is the explicit, derived or generated (ColumnN) name of a projected expression.
func outputName(it namedExpr, counter *int) string {
	if it.name != "" {
		return it.name
	}

	if name := defaultExprName(it.e, ""); name != "" {
		return name
	}

	*counter++

	return "Column" + strconv.Itoa(*counter)
}

func applyExtend(sc *scope, in *Rel, items []namedExpr) (*Rel, error) {
	counter := 0
	cur := &Rel{Cols: in.Cols, Rows: in.Rows}

	for _, it := range items {
		s, err := (&scope{env: sc.env, cols: cur.Cols}).compile(it.e)
		if err != nil {
			return nil, err
		}

		if cur, err = extendOne(cur, outputName(it, &counter), s); err != nil {
			return nil, err
		}
	}

	return cur, nil
}

func extendOne(in *Rel, name string, s scalar) (*Rel, error) {
	pos := slices.IndexFunc(in.Cols, func(c Col) bool { return c.Name == name })
	cols := slices.Clone(in.Cols)
	col := Col{Name: name, Type: s.typ}

	if pos >= 0 {
		cols[pos] = col
	} else {
		cols = append(cols, col)
	}

	out := &Rel{Cols: cols, Rows: make([][]any, 0, len(in.Rows))}

	for _, row := range in.Rows {
		v, err := s.fn(row)
		if err != nil {
			return nil, err
		}

		var nr []any
		if pos >= 0 {
			nr = slices.Clone(row)
			nr[pos] = v
		} else {
			nr = append(slices.Clone(row), v)
		}

		out.Rows = append(out.Rows, nr)
	}

	return out, nil
}

func matchName(pattern, name string) bool {
	if p, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(name, p)
	}

	return pattern == name
}

func selectCols(in *Rel, names []string, keep bool) *Rel {
	var idx []int

	for i, c := range in.Cols {
		matched := slices.ContainsFunc(names, func(p string) bool { return matchName(p, c.Name) })
		if matched == keep {
			idx = append(idx, i)
		}
	}

	return pickCols(in, idx)
}

func pickCols(in *Rel, idx []int) *Rel {
	out := &Rel{Cols: make([]Col, len(idx)), Rows: make([][]any, len(in.Rows))}
	for i, j := range idx {
		out.Cols[i] = in.Cols[j]
	}

	for r, row := range in.Rows {
		nr := make([]any, len(idx))
		for i, j := range idx {
			nr[i] = row[j]
		}

		out.Rows[r] = nr
	}

	return out
}

func reorderCols(in *Rel, names []string) *Rel {
	var idx []int

	taken := map[int]bool{}

	for _, p := range names {
		for i, c := range in.Cols {
			if !taken[i] && matchName(p, c.Name) {
				idx = append(idx, i)
				taken[i] = true
			}
		}
	}

	for i := range in.Cols {
		if !taken[i] {
			idx = append(idx, i)
		}
	}

	return pickCols(in, idx)
}

func renameCols(in *Rel, pairs []namedExpr) (*Rel, error) {
	cols := slices.Clone(in.Cols)

	for _, p := range pairs {
		old := defaultExprName(p.e, "")
		if p.name == "" || old == "" {
			return nil, syntaxErr("project-rename expects new = old pairs")
		}

		i := slices.IndexFunc(cols, func(c Col) bool { return c.Name == old })
		if i < 0 {
			return nil, semErr("Failed to resolve scalar expression named '%s'", old)
		}

		cols[i].Name = p.name
	}

	return &Rel{Cols: cols, Rows: in.Rows}, nil
}

func applyTake(sc *scope, in *Rel, x *takeOp) (*Rel, error) {
	n, err := constInt(sc, x.n)
	if err != nil {
		return nil, err
	}

	return &Rel{Cols: in.Cols, Rows: in.Rows[:min(int(max(n, 0)), len(in.Rows))]}, nil
}

func constInt(sc *scope, n node) (int64, error) {
	s, err := (&scope{env: sc.env}).compile(n)
	if err != nil {
		return 0, err
	}

	v, err := s.fn(nil)
	if err != nil {
		return 0, err
	}

	i, ok := v.(int64)
	if !ok {
		return 0, semErr("an integer constant was expected")
	}

	return i, nil
}

type sortCol struct {
	s         scalar
	desc      bool
	nullsLast bool
}

func compileSortKeys(sc *scope, keys []sortKey) ([]sortCol, error) {
	out := make([]sortCol, len(keys))

	for i, k := range keys {
		s, err := sc.compile(k.e)
		if err != nil {
			return nil, err
		}

		out[i] = sortCol{s: s, desc: k.desc, nullsLast: k.desc}
		if k.nullsLast != nil {
			out[i].nullsLast = *k.nullsLast
		}
	}

	return out, nil
}

type keyedRow struct {
	row  []any
	keys []any
}

// compareSortKey orders two key values: nulls first or last, then by value (descending negates).
func compareSortKey(c sortCol, x, y any) int {
	switch {
	case x == nil && y == nil:
		return 0
	case x == nil:
		return nullOrder(c.nullsLast)
	case y == nil:
		return -nullOrder(c.nullsLast)
	}

	cmp, ok := compareValues(x, y)
	if !ok {
		return 0
	}

	if c.desc {
		return -cmp
	}

	return cmp
}

func nullOrder(nullsLast bool) int {
	if nullsLast {
		return 1
	}

	return -1
}

func sortRows(in *Rel, cols []sortCol) ([][]any, error) {
	items := make([]keyedRow, len(in.Rows))

	for i, row := range in.Rows {
		ks := make([]any, len(cols))

		for j, c := range cols {
			v, err := c.s.fn(row)
			if err != nil {
				return nil, err
			}

			ks[j] = v
		}

		items[i] = keyedRow{row: row, keys: ks}
	}

	slices.SortStableFunc(items, func(a, b keyedRow) int {
		for j, c := range cols {
			if r := compareSortKey(c, a.keys[j], b.keys[j]); r != 0 {
				return r
			}
		}

		return 0
	})

	rows := make([][]any, len(items))
	for i, it := range items {
		rows[i] = it.row
	}

	return rows, nil
}

func applySort(sc *scope, in *Rel, keys []sortKey) (*Rel, error) {
	cols, err := compileSortKeys(sc, keys)
	if err != nil {
		return nil, err
	}

	rows, err := sortRows(in, cols)
	if err != nil {
		return nil, err
	}

	return &Rel{Cols: in.Cols, Rows: rows}, nil
}

func applyTop(sc *scope, in *Rel, x *topOp) (*Rel, error) {
	n, err := constInt(sc, x.n)
	if err != nil {
		return nil, err
	}

	sorted, err := applySort(sc, in, []sortKey{x.key})
	if err != nil {
		return nil, err
	}

	sorted.Rows = sorted.Rows[:min(int(max(n, 0)), len(sorted.Rows))]

	return sorted, nil
}

func applyDistinct(sc *scope, in *Rel, x *distinctOp) (*Rel, error) {
	items := x.items

	if x.all {
		for _, c := range in.Cols {
			items = append(items, namedExpr{e: &identNode{name: c.Name}})
		}
	}

	proj, err := applyProject(sc, in, items, false)
	if err != nil {
		return nil, err
	}

	seen := map[string]bool{}
	out := &Rel{Cols: proj.Cols}

	for _, row := range proj.Rows {
		var sb strings.Builder
		for _, v := range row {
			sb.WriteString(groupKey(v))
			sb.WriteByte(0)
		}

		if k := sb.String(); !seen[k] {
			seen[k] = true
			out.Rows = append(out.Rows, row)
		}
	}

	return out, nil
}

type group struct {
	keys   []any
	states []aggState
}

type summaryItem struct {
	s     scalar
	cols  []Col
	slot  int
	multi bool
}

type summarizePlan struct {
	ac     *aggCollector
	used   map[string]bool
	by     []scalar
	byCols []Col
	items  []summaryItem
	count  int
}

func (p *summarizePlan) unnamed() string {
	p.count++

	return "Column" + strconv.Itoa(p.count)
}

func (p *summarizePlan) claim(name string) string {
	name = uniqueName(name, p.used)
	p.used[name] = true

	return name
}

func (p *summarizePlan) planBy(sc *scope, x *summarizeOp) error {
	for _, b := range x.by {
		s, err := sc.compile(b.e)
		if err != nil {
			return err
		}

		name := b.name
		if name == "" {
			name = defaultExprName(b.e, "")
		}

		if name == "" {
			name = p.unnamed()
		}

		name = p.claim(name)
		p.ac.byIdx[name] = true
		p.by = append(p.by, s)
		p.byCols = append(p.byCols, Col{Name: name, Type: s.typ})
	}

	return nil
}

func isArgAggregate(n node) (*callNode, bool) {
	call, ok := n.(*callNode)
	if !ok {
		return nil, false
	}

	return call, strings.EqualFold(call.name, aggArgMax) || strings.EqualFold(call.name, aggArgMin)
}

func (p *summarizePlan) planAggs(sc *scope, x *summarizeOp) error {
	outer := &scope{env: sc.env, agg: p.ac}

	for _, a := range x.aggs {
		if call, ok := isArgAggregate(a.e); ok {
			cols, err := p.ac.registerArg(call)
			if err != nil {
				return err
			}

			p.items = append(p.items, summaryItem{cols: cols, multi: true, slot: p.ac.specs[len(p.ac.specs)-1].slot})

			continue
		}

		s, err := outer.compile(a.e)
		if err != nil {
			return err
		}

		name := a.name
		if call, isCall := a.e.(*callNode); name == "" && isCall {
			name = defaultAggName(call)
		}

		if name == "" {
			name = p.unnamed()
		}

		p.items = append(p.items, summaryItem{s: s, cols: []Col{{Name: p.claim(name), Type: s.typ}}})
	}

	return nil
}

func (p *summarizePlan) outputCols() []Col {
	out := slices.Clone(p.byCols)

	for _, it := range p.items {
		for _, c := range it.cols {
			if it.multi {
				c.Name = p.claim(c.Name)
			}

			out = append(out, c)
		}
	}

	return out
}

func (p *summarizePlan) row(g *group) ([]any, error) {
	aggRow := make([]any, 0, p.ac.width)

	for _, st := range g.states {
		aggRow = append(aggRow, st.result()...)
	}

	row := slices.Clone(g.keys)

	for _, it := range p.items {
		if it.multi {
			row = append(row, aggRow[it.slot:it.slot+len(it.cols)]...)

			continue
		}

		v, err := it.s.fn(aggRow)
		if err != nil {
			return nil, err
		}

		row = append(row, v)
	}

	return row, nil
}

func applySummarize(sc *scope, in *Rel, x *summarizeOp) (*Rel, error) {
	p := &summarizePlan{
		ac:   &aggCollector{in: &scope{env: sc.env, cols: in.Cols}, byIdx: map[string]bool{}},
		used: map[string]bool{},
	}

	if err := p.planBy(sc, x); err != nil {
		return nil, err
	}

	if err := p.planAggs(sc, x); err != nil {
		return nil, err
	}

	groups, order, err := runGroups(in, p.by, p.ac)
	if err != nil {
		return nil, err
	}

	out := &Rel{Cols: p.outputCols()}

	for _, k := range order {
		row, rowErr := p.row(groups[k])
		if rowErr != nil {
			return nil, rowErr
		}

		out.Rows = append(out.Rows, row)
	}

	return out, nil
}

func runGroups(in *Rel, by []scalar, ac *aggCollector) (map[string]*group, []string, error) {
	groups := map[string]*group{}

	var order []string

	newGroup := func(keys []any) *group {
		g := &group{keys: keys}
		for _, sp := range ac.specs {
			g.states = append(g.states, sp.newState())
		}

		return g
	}

	for _, row := range in.Rows {
		keys := make([]any, len(by))

		var sb strings.Builder

		for i, b := range by {
			v, err := b.fn(row)
			if err != nil {
				return nil, nil, err
			}

			keys[i] = v

			sb.WriteString(groupKey(v))
			sb.WriteByte(0)
		}

		k := sb.String()

		g, ok := groups[k]
		if !ok {
			g = newGroup(keys)
			groups[k] = g
			order = append(order, k)
		}

		for _, st := range g.states {
			if err := st.add(row); err != nil {
				return nil, nil, err
			}
		}
	}

	if len(by) == 0 && len(groups) == 0 {
		groups[""] = newGroup(nil)
		order = append(order, "")
	}

	return groups, order, nil
}
