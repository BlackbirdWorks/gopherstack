package bqsql

import (
	"database/sql"
	"math"
	"slices"
	"strconv"
	"strings"
)

// TableRef names a table.
type TableRef struct {
	Project string
	Dataset string
	Table   string
}

// String renders the legacy project:dataset.table form BigQuery error messages use.
func (r TableRef) String() string { return r.Project + ":" + r.Dataset + "." + r.Table }

// TableData is a table's schema and rows as the catalog holds them.
type TableData struct {
	Ref       TableRef
	ViewQuery string
	Fields    []Field
	Rows      [][]any
}

// Catalog resolves tables for the compiler.
type Catalog interface {
	Table(ref TableRef) (*TableData, bool)
	DatasetExists(project, dataset string) bool
}

// Param is a query parameter; Name is empty for positional parameters and Value is in stored form.
type Param struct {
	Value any
	Name  string
	Field Field
}

// Input configures one statement run.
type Input struct {
	Catalog Catalog
	Project string
	Dataset string
	Params  []Param
}

type colInfo struct {
	name   string
	sql    string
	typ    Type
	hidden bool
}

type relation struct {
	alias string
	cols  []*colInfo
	value bool
}

type scope struct {
	parent      *scope
	aliasItems  map[string]*gx
	rels        []*relation
	preferAlias bool
}

type gx struct {
	lit *node
	sql string
	typ Type
}

type loadedTable struct {
	td   *TableData
	name string
}

type cteDef struct {
	cols []*colInfo
}

type compiledQuery struct {
	sql     string
	cols    []Column
	aliases []string
}

type compiler struct {
	in        *Input
	loadIdx   map[TableRef]*loadedTable
	fns       map[string]*fnDef
	project   string
	dataset   string
	loads     []*loadedTable
	ctes      []map[string]*cteDef
	bound     map[*Param]string
	binds     []any
	seq       int
	viewDepth int
}

const maxViewDepth = 16

func newCompiler(in *Input) *compiler {
	c := &compiler{
		in: in, loadIdx: map[TableRef]*loadedTable{}, bound: map[*Param]string{},
		project: in.Project, dataset: in.Dataset,
	}
	c.fns = functionTable()

	return c
}

// bind registers p as a named SQLite parameter and returns its placeholder.
func (c *compiler) bind(p *Param) string {
	if name, ok := c.bound[p]; ok {
		return name
	}

	name := "@bqp" + strconv.Itoa(len(c.bound))
	v := ToSQLite(p.Field, p.Value)

	if f, isF := v.(float64); isF && math.IsNaN(f) {
		v = nil
	}

	c.bound[p] = name
	c.binds = append(c.binds, sql.Named(name[1:], v))

	return name
}

func (c *compiler) uniq(prefix string) string {
	c.seq++

	return prefix + strconv.Itoa(c.seq)
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func quoteStr(s string) string { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }

func (c *compiler) resolveRef(path []string) (TableRef, error) {
	switch len(path) {
	case 1:
		if c.dataset == "" {
			return TableRef{}, invalid("Table %q must be qualified with a dataset (e.g. dataset.table).", path[0])
		}

		return TableRef{Project: c.project, Dataset: c.dataset, Table: path[0]}, nil
	case 2: //nolint:mnd // dataset.table
		return TableRef{Project: c.project, Dataset: path[0], Table: path[1]}, nil
	case 3: //nolint:mnd // project.dataset.table
		return TableRef{Project: path[0], Dataset: path[1], Table: path[2]}, nil
	}

	return TableRef{}, invalid("Invalid table name %q", strings.Join(path, "."))
}

func (c *compiler) findCTE(name string) *cteDef {
	for _, defs := range slices.Backward(c.ctes) {
		for k, d := range defs {
			if strings.EqualFold(k, name) {
				return d
			}
		}
	}

	return nil
}

func (c *compiler) lookupTable(ref TableRef) (*TableData, error) {
	td, ok := c.in.Catalog.Table(ref)
	if !ok {
		if !c.in.Catalog.DatasetExists(ref.Project, ref.Dataset) {
			return nil, notFound("Not found: Dataset %s:%s was not found in location US", ref.Project, ref.Dataset)
		}

		return nil, notFound("Not found: Table %s was not found in location US", ref)
	}

	return td, nil
}

// compileQuery compiles q; outer is the enclosing scope for correlated subqueries.
func (c *compiler) compileQuery(q *Query, outer *scope) (*compiledQuery, error) {
	defs := map[string]*cteDef{}
	c.ctes = append(c.ctes, defs)

	defer func() { c.ctes = c.ctes[:len(c.ctes)-1] }()

	prefix, err := c.compileWith(q.with, defs)
	if err != nil {
		return nil, err
	}

	cq, sel, err := c.compileSetExpr(q.body, outer)
	if err != nil {
		return nil, err
	}

	if err = c.applyOrderLimit(cq, sel, q); err != nil {
		return nil, err
	}

	cq.sql = prefix + cq.sql

	return cq, nil
}

func (c *compiler) compileWith(with []cte, defs map[string]*cteDef) (string, error) {
	if len(with) == 0 {
		return "", nil
	}

	parts := make([]string, 0, len(with))

	for _, w := range with {
		cq, err := c.compileQuery(w.query, nil)
		if err != nil {
			return "", err
		}

		names := make([]string, len(cq.cols))
		for i, col := range cq.cols {
			names[i] = col.Name
		}

		if len(w.cols) > 0 {
			if len(w.cols) != len(cq.cols) {
				return "", invalid("WITH %s has %d columns but %d names", w.name, len(cq.cols), len(w.cols))
			}

			names = w.cols
		}

		def := &cteDef{}
		sqlNames := uniqueNames(names)

		for i, col := range cq.cols {
			def.cols = append(def.cols, &colInfo{name: names[i], typ: col.Type, sql: quoteIdent(sqlNames[i])})
		}

		defs[w.name] = def

		parts = append(parts, quoteIdent(w.name)+"("+quoteList(sqlNames)+") AS ("+cq.sql+")")
	}

	return "WITH " + strings.Join(parts, ", ") + " ", nil
}

func quoteList(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = quoteIdent(n)
	}

	return strings.Join(q, ", ")
}

func uniqueNames(names []string) []string {
	seen := map[string]int{}
	out := make([]string, len(names))

	for i, n := range names {
		l := strings.ToLower(n)
		seen[l]++

		out[i] = n
		if seen[l] > 1 {
			out[i] = n + "_" + strconv.Itoa(seen[l]-1)
		}
	}

	return out
}

func (c *compiler) compileSetExpr(s *setExpr, outer *scope) (*compiledQuery, *selOut, error) {
	switch {
	case s.sel != nil:
		so, err := c.compileSelect(s.sel, outer)
		if err != nil {
			return nil, nil, err
		}

		return &compiledQuery{sql: so.sql, cols: so.cols, aliases: so.aliases}, so, nil
	case s.sub != nil:
		cq, err := c.compileQuery(s.sub, outer)

		return cq, nil, err
	}

	return c.compileSetOp(s, outer)
}

func setOpSQL() map[string]string {
	return map[string]string{
		"UNION ALL": "UNION ALL", "UNION DISTINCT": litUnion, "INTERSECT DISTINCT": litIntersect,
		"EXCEPT DISTINCT": litExcept,
	}
}

func (c *compiler) compileSetOp(s *setExpr, outer *scope) (*compiledQuery, *selOut, error) {
	left, _, err := c.compileSetExpr(s.left, outer)
	if err != nil {
		return nil, nil, err
	}

	right, _, err := c.compileSetExpr(s.right, outer)
	if err != nil {
		return nil, nil, err
	}

	if len(left.cols) != len(right.cols) {
		return nil, nil, invalid(
			"Queries in %s have mismatched column count; query 1 has %d columns, query 2 has %d columns",
			s.op,
			len(left.cols),
			len(right.cols),
		)
	}

	if s.op == "INTERSECT ALL" || s.op == "EXCEPT ALL" {
		return nil, nil, unsupported("%s", s.op)
	}

	cols := make([]Column, len(left.cols))

	for i := range cols {
		cols[i] = Column{Name: left.cols[i].Name, Type: commonType(left.cols[i].Type, right.cols[i].Type)}
	}

	sql := "SELECT * FROM (" + left.sql + ") " + setOpSQL()[s.op] + " SELECT * FROM (" + right.sql + ")"

	return &compiledQuery{sql: sql, cols: cols, aliases: left.aliases}, nil, nil
}

func (c *compiler) applyOrderLimit(cq *compiledQuery, sel *selOut, q *Query) error {
	var tail strings.Builder

	if len(q.order) > 0 {
		parts := make([]string, 0, len(q.order))

		for _, o := range q.order {
			s, err := c.orderExpr(o.expr, cq, sel)
			if err != nil {
				return err
			}

			if o.desc {
				s += litDesc
			}

			parts = append(parts, s)
		}

		tail.WriteString(" ORDER BY " + strings.Join(parts, ", "))
	}

	if q.limit != nil {
		lim, err := c.constInt(q.limit)
		if err != nil {
			return err
		}

		tail.WriteString(" LIMIT " + lim)

		if q.offset != nil {
			off, offErr := c.constInt(q.offset)
			if offErr != nil {
				return offErr
			}

			tail.WriteString(" OFFSET " + off)
		}
	}

	cq.sql += tail.String()

	return nil
}

func (c *compiler) constInt(n *node) (string, error) {
	g, err := c.expr(n, nil)
	if err != nil {
		return "", err
	}

	if g.typ.Kind != KindInt {
		return "", invalid("LIMIT/OFFSET must be an INT64 expression")
	}

	return g.sql, nil
}

// orderExpr compiles one ORDER BY expression: an ordinal, an output column name or an expression over the FROM scope.
func (c *compiler) orderExpr(n *node, cq *compiledQuery, sel *selOut) (string, error) {
	if n.kind == nInt {
		i, _ := strconv.Atoi(n.str)
		if i < 1 || i > len(cq.cols) {
			return "", invalid("ORDER BY column number %d is out of range", i)
		}

		return strconv.Itoa(i), nil
	}

	if sel != nil {
		sc := sel.sc
		sc.preferAlias = true

		g, err := c.expr(n, sc)
		if err != nil {
			return "", err
		}

		return g.sql, nil
	}

	if n.kind == nPath && len(n.parts) == 1 {
		for i, col := range cq.cols {
			if strings.EqualFold(col.Name, n.parts[0]) {
				return quoteIdent(cq.aliases[i]), nil
			}
		}
	}

	return "", invalid("ORDER BY on a set operation must name an output column or position")
}

type selOut struct {
	sc      *scope
	sql     string
	cols    []Column
	aliases []string
}

func (c *compiler) compileSelect(s *selectStmt, outer *scope) (*selOut, error) {
	sc := &scope{parent: outer, aliasItems: map[string]*gx{}}

	fromSQL := ""

	if s.from != nil {
		var err error

		if fromSQL, err = c.compileFrom(s.from, sc); err != nil {
			return nil, err
		}
	}

	items, err := c.compileItems(s, sc)
	if err != nil {
		return nil, err
	}

	if s.asStruct {
		items = []*outItem{asStructItem(items)}
	}

	for _, it := range items {
		sc.aliasItems[strings.ToLower(it.name)] = &gx{sql: it.sql, typ: it.typ}
	}

	return c.finishSelect(s, sc, items, fromSQL)
}

func (c *compiler) finishSelect(s *selectStmt, sc *scope, items []*outItem, fromSQL string) (*selOut, error) {
	names := make([]string, len(items))
	cols := make([]Column, len(items))

	for i, it := range items {
		names[i] = it.name
		cols[i] = Column{Name: it.name, Type: it.typ}
	}

	aliases := uniqueNames(names)
	parts := make([]string, len(items))

	for i, it := range items {
		parts[i] = it.sql + " AS " + quoteIdent(aliases[i])
	}

	var sb strings.Builder

	sb.WriteString("SELECT ")

	if s.distinct {
		sb.WriteString("DISTINCT ")
	}

	sb.WriteString(strings.Join(parts, ", "))

	if err := c.selectClauses(&sb, s, sc, items, fromSQL); err != nil {
		return nil, err
	}

	out := &selOut{sc: sc, sql: sb.String(), cols: cols, aliases: aliases}

	if s.qualify != nil {
		return c.applyQualify(out, s, sc, items)
	}

	return out, nil
}

func (c *compiler) selectClauses(
	sb *strings.Builder,
	s *selectStmt,
	sc *scope,
	items []*outItem,
	fromSQL string,
) error {
	if fromSQL != "" {
		sb.WriteString(" FROM " + fromSQL)
	}

	if s.where != nil {
		g, err := c.expr(s.where, sc)
		if err != nil {
			return err
		}

		sb.WriteString(" WHERE " + g.sql)
	}

	if len(s.groupBy) > 0 {
		parts, err := c.groupItems(s.groupBy, sc, items)
		if err != nil {
			return err
		}

		sb.WriteString(" GROUP BY " + strings.Join(parts, ", "))
	}

	if s.having != nil {
		g, err := c.expr(s.having, sc)
		if err != nil {
			return err
		}

		sb.WriteString(" HAVING " + g.sql)
	}

	return nil
}

func (c *compiler) groupItems(group []*node, sc *scope, items []*outItem) ([]string, error) {
	parts := make([]string, 0, len(group))
	sc.preferAlias = true

	defer func() { sc.preferAlias = false }()

	for _, n := range group {
		if n.kind == nInt {
			i, _ := strconv.Atoi(n.str)
			if i < 1 || i > len(items) {
				return nil, invalid("GROUP BY position %d is out of range", i)
			}

			parts = append(parts, items[i-1].sql)

			continue
		}

		g, err := c.expr(n, sc)
		if err != nil {
			return nil, err
		}

		parts = append(parts, g.sql)
	}

	return parts, nil
}

func (c *compiler) applyQualify(out *selOut, s *selectStmt, sc *scope, items []*outItem) (*selOut, error) {
	g, err := c.expr(s.qualify, sc)
	if err != nil {
		return nil, err
	}

	_ = items
	inner := strings.Replace(out.sql, " FROM ", ", "+g.sql+" AS \"__qualify\" FROM ", 1)
	if !strings.Contains(out.sql, " FROM ") {
		return nil, invalid("QUALIFY requires a FROM clause")
	}

	out.sql = "SELECT " + quoteList(out.aliases) + " FROM (" + inner + ") WHERE \"__qualify\""

	return out, nil
}

type outItem struct {
	name string
	sql  string
	typ  Type
}

func (c *compiler) compileItems(s *selectStmt, sc *scope) ([]*outItem, error) {
	var items []*outItem

	for i, it := range s.items {
		if it.expr.kind == nStar {
			stars, err := c.expandStar(it.expr, sc)
			if err != nil {
				return nil, err
			}

			items = append(items, stars...)

			continue
		}

		g, err := c.expr(it.expr, sc)
		if err != nil {
			return nil, err
		}

		items = append(items, &outItem{name: itemName(it, i), sql: g.sql, typ: defaultType(g.typ)})
	}

	if len(items) == 0 {
		return nil, invalid("SELECT list must not be empty")
	}

	return items, nil
}

func defaultType(t Type) Type {
	if t.Kind == KindUnknown {
		return scalar(KindString)
	}

	return t
}

func itemName(it selectItem, idx int) string {
	switch {
	case it.alias != "":
		return it.alias
	case it.expr.kind == nPath:
		return it.expr.parts[len(it.expr.parts)-1]
	case it.expr.kind == nField:
		return it.expr.name
	}

	return "f" + strconv.Itoa(idx) + "_"
}

func asStructItem(items []*outItem) *outItem {
	cols := make([]Column, len(items))
	args := make([]string, 0, len(items)*2) //nolint:mnd // name and value per member

	for i, it := range items {
		cols[i] = Column{Name: it.name, Type: it.typ}
		args = append(args, quoteStr(it.name), jsonVal(&gx{sql: it.sql, typ: it.typ}))
	}

	return &outItem{
		name: "f0_",
		sql:  "json_object(" + strings.Join(args, ", ") + ")",
		typ:  Type{Kind: KindStruct, Fields: cols},
	}
}

func (c *compiler) expandStar(star *node, sc *scope) ([]*outItem, error) {
	var out []*outItem

	except := map[string]bool{}
	for _, e := range star.except {
		except[strings.ToLower(e)] = true
	}

	repl := map[string]*selectItem{}
	for i := range star.replace {
		repl[strings.ToLower(star.replace[i].alias)] = &star.replace[i]
	}

	found := false

	for _, rel := range sc.rels {
		if star.name != "" && !strings.EqualFold(star.name, rel.alias) {
			continue
		}

		found = true

		items, err := c.relationStar(rel, except, repl, sc)
		if err != nil {
			return nil, err
		}

		out = append(out, items...)
	}

	if !found {
		if star.name != "" {
			return nil, invalid("Unrecognized name: %s", star.name)
		}

		return nil, invalid("SELECT * must have a FROM clause")
	}

	return out, nil
}

func (c *compiler) relationStar(
	rel *relation, except map[string]bool, repl map[string]*selectItem, sc *scope,
) ([]*outItem, error) {
	var out []*outItem

	for _, col := range c.starColumns(rel) {
		if except[strings.ToLower(col.name)] {
			continue
		}

		it := &outItem{name: col.name, sql: col.sql, typ: col.typ}

		if r, ok := repl[strings.ToLower(col.name)]; ok {
			g, err := c.expr(r.expr, sc)
			if err != nil {
				return nil, err
			}

			it.sql, it.typ = g.sql, defaultType(g.typ)
		}

		out = append(out, it)
	}

	return out, nil
}

func (c *compiler) starColumns(rel *relation) []*colInfo {
	if rel.value && len(rel.cols) == 1 && rel.cols[0].typ.Kind == KindStruct {
		base := rel.cols[0]
		out := make([]*colInfo, len(base.typ.Fields))

		for i, f := range base.typ.Fields {
			out[i] = &colInfo{name: f.Name, typ: f.Type, sql: fieldSQL(base.sql, f)}
		}

		return out
	}

	var out []*colInfo

	for _, col := range rel.cols {
		if !col.hidden {
			out = append(out, col)
		}
	}

	return out
}

func (c *compiler) compileFrom(f *fromItem, sc *scope) (string, error) {
	switch f.kind {
	case fromTable:
		return c.fromTable(f, sc)
	case fromSubquery:
		cq, err := c.compileQuery(f.query, sc.parent)
		if err != nil {
			return "", err
		}

		alias := f.alias
		if alias == "" {
			alias = c.uniq("_q")
		}

		sc.rels = append(sc.rels, relFromCompiled(alias, cq))

		return "(" + cq.sql + ") AS " + quoteIdent(alias), nil
	case fromUnnest:
		return c.fromUnnest(f, sc)
	case fromJoin:
		return c.fromJoin(f, sc)
	}

	return "", invalid("unsupported FROM item")
}

func relFromCompiled(alias string, cq *compiledQuery) *relation {
	rel := &relation{alias: alias}

	for i, col := range cq.cols {
		rel.cols = append(
			rel.cols,
			&colInfo{name: col.Name, typ: col.Type, sql: quoteIdent(alias) + "." + quoteIdent(cq.aliases[i])},
		)
	}

	return rel
}

func (c *compiler) fromTable(f *fromItem, sc *scope) (string, error) {
	if len(f.path) == 1 {
		if def := c.findCTE(f.path[0]); def != nil {
			alias := f.alias
			if alias == "" {
				alias = f.path[0]
			}

			rel := &relation{alias: alias}
			for _, col := range def.cols {
				rel.cols = append(
					rel.cols,
					&colInfo{name: col.name, typ: col.typ, sql: quoteIdent(alias) + "." + col.sql},
				)
			}

			sc.rels = append(sc.rels, rel)

			return quoteIdent(f.path[0]) + " AS " + quoteIdent(alias), nil
		}
	}

	ref, err := c.resolveRef(f.path)
	if err != nil {
		return "", err
	}

	td, err := c.lookupTable(ref)
	if err != nil {
		return "", err
	}

	alias := f.alias
	if alias == "" {
		alias = ref.Table
	}

	if td.ViewQuery != "" {
		return c.fromView(td, alias, sc)
	}

	lt := c.load(td)
	rel := &relation{alias: alias}

	for _, fl := range td.Fields {
		rel.cols = append(
			rel.cols,
			&colInfo{name: fl.Name, typ: fl.SQLType(), sql: quoteIdent(alias) + "." + quoteIdent(fl.Name)},
		)
	}

	sc.rels = append(sc.rels, rel)

	return quoteIdent(lt.name) + " AS " + quoteIdent(alias), nil
}

func (c *compiler) fromView(td *TableData, alias string, sc *scope) (string, error) {
	if c.viewDepth >= maxViewDepth {
		return "", invalid("View %s is nested too deeply", td.Ref)
	}

	stmt, err := ParseStatement(td.ViewQuery)
	if err != nil || stmt.query == nil {
		return "", invalid("View %s has an invalid query", td.Ref)
	}

	saveP, saveD := c.project, c.dataset
	c.project, c.dataset = td.Ref.Project, td.Ref.Dataset
	c.viewDepth++

	defer func() { c.project, c.dataset = saveP, saveD; c.viewDepth-- }()

	saved := c.ctes
	c.ctes = nil

	cq, err := c.compileQuery(stmt.query, nil)
	c.ctes = saved

	if err != nil {
		return "", err
	}

	sc.rels = append(sc.rels, relFromCompiled(alias, cq))

	return "(" + cq.sql + ") AS " + quoteIdent(alias), nil
}

func (c *compiler) load(td *TableData) *loadedTable {
	if lt, ok := c.loadIdx[td.Ref]; ok {
		return lt
	}

	lt := &loadedTable{td: td, name: "t_" + strconv.Itoa(len(c.loads))}
	c.loads = append(c.loads, lt)
	c.loadIdx[td.Ref] = lt

	return lt
}

func (c *compiler) fromUnnest(f *fromItem, sc *scope) (string, error) {
	g, err := c.expr(f.unnest, sc)
	if err != nil {
		return "", err
	}

	if g.typ.Kind != KindArray || g.typ.Elem == nil {
		return "", invalid("Values referenced in UNNEST must be arrays")
	}

	je := c.uniq("_je")
	elem := *g.typ.Elem
	rel := &relation{alias: f.alias, value: f.alias != ""}
	val := quoteIdent(je) + ".\"value\""

	switch {
	case f.alias != "":
		rel.cols = append(rel.cols, &colInfo{name: f.alias, typ: elem, sql: val})
	case elem.Kind == KindStruct:
		for _, fl := range elem.Fields {
			rel.cols = append(rel.cols, &colInfo{name: fl.Name, typ: fl.Type, sql: fieldSQL(val, fl)})
		}
	default:
		rel.cols = append(rel.cols, &colInfo{name: "f0_", typ: elem, sql: val})
	}

	if f.withOffset {
		name := f.offsetAlias
		if name == "" {
			name = "offset"
		}

		rel.cols = append(rel.cols, &colInfo{name: name, typ: scalar(KindInt), sql: quoteIdent(je) + ".\"key\""})
	}

	sc.rels = append(sc.rels, rel)

	return "json_each(" + g.sql + ") AS " + quoteIdent(je), nil
}

func (c *compiler) fromJoin(f *fromItem, sc *scope) (string, error) {
	leftSQL, err := c.compileFrom(f.left, sc)
	if err != nil {
		return "", err
	}

	nLeft := len(sc.rels)

	rightSQL, err := c.compileFrom(f.right, sc)
	if err != nil {
		return "", err
	}

	kw := f.joinType + " JOIN"
	if f.joinType == litInner && f.on == nil && len(f.using) == 0 {
		kw = "CROSS JOIN"
	}

	sql := leftSQL + " " + kw + " " + rightSQL

	switch {
	case f.on != nil:
		g, onErr := c.expr(f.on, sc)
		if onErr != nil {
			return "", onErr
		}

		sql += " ON " + g.sql
	case len(f.using) > 0:
		cond, useErr := c.usingJoin(f, sc, nLeft)
		if useErr != nil {
			return "", useErr
		}

		sql += " ON " + cond
	case f.right.kind == fromUnnest && (f.joinType == litLeft):
		sql += " ON 1"
	}

	return sql, nil
}

func (c *compiler) usingJoin(f *fromItem, sc *scope, nLeft int) (string, error) {
	conds := make([]string, 0, len(f.using))

	for _, u := range f.using {
		l := findCol(sc.rels[:nLeft], u)
		r := findCol(sc.rels[nLeft:], u)

		if l == nil || r == nil {
			return "", invalid("USING column %s must exist on both sides of the join", u)
		}

		conds = append(conds, l.sql+" = "+r.sql)

		if f.joinType == litRight || f.joinType == "FULL" {
			l.sql = "COALESCE(" + l.sql + ", " + r.sql + ")"
		}

		r.hidden = true
	}

	return strings.Join(conds, " AND "), nil
}

func findCol(rels []*relation, name string) *colInfo {
	for _, rel := range rels {
		for _, col := range rel.cols {
			if strings.EqualFold(col.name, name) {
				return col
			}
		}
	}

	return nil
}

func equalFold(a, b string) bool { return strings.EqualFold(a, b) }
