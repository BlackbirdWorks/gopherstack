package cosmosdb

import (
	"fmt"
	"strconv"
	"strings"
)

// sql_exec.go runs a parsed sqlSelectStmt: source tuples (container rows x JOINs) -> WHERE -> GROUP BY /
// aggregates -> projection -> DISTINCT -> ORDER BY -> OFFSET/LIMIT or TOP. Expression evaluation (sql_eval.go)
// follows Cosmos's three-valued logic: a missing field is undefined, undefined never flips under NOT, and only
// a definite true keeps a row.

// QueryParameter is one "@name"/value binding from a query request body.
type QueryParameter struct {
	Value any    `json:"value"`
	Name  string `json:"name"`
}

type sqlTriState int

const (
	sqlUndefined sqlTriState = iota
	sqlFalse
	sqlTrue
)

func boolToTri(b bool) sqlTriState {
	if b {
		return sqlTrue
	}

	return sqlFalse
}

func triAnd(a, b sqlTriState) sqlTriState {
	if a == sqlFalse || b == sqlFalse {
		return sqlFalse
	}

	if a == sqlTrue && b == sqlTrue {
		return sqlTrue
	}

	return sqlUndefined
}

func triOr(a, b sqlTriState) sqlTriState {
	if a == sqlTrue || b == sqlTrue {
		return sqlTrue
	}

	if a == sqlFalse && b == sqlFalse {
		return sqlFalse
	}

	return sqlUndefined
}

func triNot(a sqlTriState) sqlTriState {
	switch a {
	case sqlTrue:
		return sqlFalse
	case sqlFalse:
		return sqlTrue
	default:
		return sqlUndefined
	}
}

// QueryResult is the output of one query execution; Scanned counts container rows examined.
type QueryResult struct {
	Rows    []any
	Visited []*storedDocument
	Matched []*storedDocument
	Scanned int
	Loaded  float64
}

// retrievalStats records which stored documents a query touched, for the request charge.
type retrievalStats struct {
	visited map[*storedDocument]struct{}
	matched map[*storedDocument]struct{}
}

func newRetrievalStats() *retrievalStats {
	return &retrievalStats{visited: map[*storedDocument]struct{}{}, matched: map[*storedDocument]struct{}{}}
}

func (s *retrievalStats) visit(r *sqlRow) {
	if s != nil && r.doc != nil {
		s.visited[r.doc] = struct{}{}
	}
}

func (s *retrievalStats) match(env []any) {
	if s == nil || len(env) == 0 {
		return
	}

	if r, ok := env[0].(*sqlRow); ok && r.doc != nil {
		s.matched[r.doc] = struct{}{}
	}
}

func docSet(m map[*storedDocument]struct{}) []*storedDocument {
	out := make([]*storedDocument, 0, len(m))
	for d := range m {
		out = append(out, d)
	}

	return out
}

// ExecuteQuery parses and runs query against docs. It never panics.
func ExecuteQuery(query string, params []QueryParameter, docs []DocumentInfo) ([]any, error) {
	stmt, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	rows := make(mapRows, len(docs))
	for i, doc := range docs {
		rows[i] = documentAsMap(doc)
	}

	res, err := executeStatement(stmt, params, rows)

	return res.Rows, err
}

// queryRows is the container row source of a top-level statement.
type queryRows interface {
	count() int
	row(i int) sqlRow
}

type mapRows []map[string]any

func (r mapRows) count() int       { return len(r) }
func (r mapRows) row(i int) sqlRow { return sqlRow{m: r[i]} }

func executeStatement(stmt *sqlSelectStmt, params []QueryParameter, rs queryRows) (QueryResult, error) {
	return executeStatementWith(stmt, params, rs, nil, nil)
}

// executeStatementWith is executeStatement with the container's user-defined functions available.
func executeStatementWith(
	stmt *sqlSelectStmt, params []QueryParameter, rs queryRows, udf *udfRuntime, coll *queryColl,
) (QueryResult, error) {
	ec := newEvalCtx(params)
	if udf != nil {
		ec.udf = udf
	}

	if coll != nil {
		ec.geometry = coll.geometry
		coll.rows, ec.coll = rs, coll
		if err := coll.checkStmt(ec, stmt); err != nil {
			return QueryResult{}, err
		}
	}

	if _, stored := rs.(storedRows); stored {
		ec.stats = newRetrievalStats()
	}

	rows, err := ec.runStmt(stmt, nil, rs)
	if err != nil {
		return QueryResult{}, err
	}

	if ec.udf.err != nil {
		return QueryResult{}, ec.udf.err
	}

	res := QueryResult{Rows: rows, Scanned: rs.count()}
	if len(stmt.sources) == 0 {
		res.Scanned = 0
	}
	if ec.stats != nil {
		res.Visited, res.Matched = docSet(ec.stats.visited), docSet(ec.stats.matched)
	}

	return res, nil
}

type outRow struct {
	val  any
	keys []any
}

// limits are the resolved TOP/OFFSET/LIMIT values; negative means unset.
type limits struct{ top, offset, limit int }

func (ec *evalCtx) resolveCount(x sqlExpr, what string) (int, error) {
	if x == nil {
		return -1, nil
	}

	n, ok := toNumber(ec.eval(x, nil))
	if !ok {
		return 0, fmt.Errorf("%w: %s value must be a non-negative integer", ErrQueryParse, what)
	}

	i, err := strconv.ParseInt(string(n), 10, 32)
	if err != nil || i < 0 {
		return 0, fmt.Errorf("%w: %s value must be a non-negative integer", ErrQueryParse, what)
	}

	return int(i), nil
}

func (ec *evalCtx) resolveLimits(stmt *sqlSelectStmt) (limits, error) {
	var (
		l   limits
		err error
	)

	if l.top, err = ec.resolveCount(stmt.top, "TOP"); err != nil {
		return l, err
	}

	if l.offset, err = ec.resolveCount(stmt.offset, "OFFSET"); err != nil {
		return l, err
	}

	l.limit, err = ec.resolveCount(stmt.limit, "LIMIT")

	return l, err
}

// window is how many rows the statement can ever emit before ordering is considered (-1 = unbounded).
func (l limits) window() int {
	switch {
	case l.limit >= 0:
		return l.offset + l.limit
	case l.top >= 0:
		return l.top
	default:
		return -1
	}
}

func (l limits) apply(rows []any) []any {
	if l.limit >= 0 || l.offset > 0 {
		rows = rows[min(l.offset, len(rows)):]

		if l.limit >= 0 && l.limit < len(rows) {
			rows = rows[:l.limit]
		}

		return rows
	}

	if l.top >= 0 && l.top < len(rows) {
		rows = rows[:l.top]
	}

	return rows
}

type runState struct {
	ec     *evalCtx
	stmt   *sqlSelectStmt
	seen   map[string]struct{}
	outs   []outRow
	lim    limits
	nsrc   int
	stream bool
}

func (rs *runState) emit(env []any) bool {
	val := rs.ec.project(rs.stmt, env, rs.nsrc)
	if isUndef(val) {
		return true
	}

	if rs.stmt.distinct {
		key := canonicalJSON(val)
		if _, dup := rs.seen[key]; dup {
			return true
		}

		rs.seen[key] = struct{}{}
	}

	out := outRow{val: val}

	if rs.stmt.rank {
		for _, comp := range rankComponents(rs.stmt) {
			out.keys = append(out.keys, rs.ec.eval(comp, env))
		}
	} else if len(rs.stmt.orderBy) > 0 {
		out.keys = make([]any, len(rs.stmt.orderBy))
		for i, k := range rs.stmt.orderBy {
			out.keys[i] = rs.ec.eval(k.expr, env)
		}
	}

	rs.outs = append(rs.outs, out)

	w := rs.lim.window()

	stop := rs.stream && w >= 0 && len(rs.outs) >= w

	return !stop
}

// runStmt executes stmt. outer is the enclosing env for subqueries; rs supplies container rows for a top-level
// statement.
func (ec *evalCtx) runStmt(stmt *sqlSelectStmt, outer []any, rows queryRows) ([]any, error) {
	lim, err := ec.resolveLimits(stmt)
	if err != nil {
		return nil, err
	}

	if lim.top == 0 || lim.limit == 0 {
		return []any{}, nil
	}

	st := &runState{ec: ec, stmt: stmt, lim: lim, nsrc: len(stmt.sources)}
	rows = sourceRows(stmt, rows)
	if stmt.distinct {
		st.seen = make(map[string]struct{})
	}

	grouped := stmt.hasAgg || len(stmt.groupBy) > 0
	st.stream = !grouped && len(stmt.orderBy) == 0

	if len(outer) > maxEnvSlots || len(stmt.sources) > maxEnvSlots {
		return nil, fmt.Errorf("%w: query nests too many sources", ErrQueryParse)
	}

	env := make([]any, len(outer), len(outer)+len(stmt.sources))
	copy(env, outer)

	if grouped {
		ec.runGrouped(st, env, rows)
	} else {
		ec.forEachTuple(stmt, env, rows, func(e []any) bool {
			if stmt.where != nil && ec.truth(stmt.where, e) != sqlTrue {
				return true
			}

			ec.stats.match(e)

			return st.emit(e)
		})
	}

	switch {
	case stmt.rank:
		ec.rankOutputs(st.outs, stmt)
	case len(stmt.orderBy) > 0:
		sortOutputs(st.outs, ec.effectiveOrder(stmt.orderBy))
	}

	vals := make([]any, len(st.outs))
	for i, o := range st.outs {
		vals[i] = o.val
	}

	return lim.apply(vals), nil
}

// groupKeySeparator is the ASCII unit separator between GROUP BY key parts.
const groupKeySeparator = 0x1f

const maxEnvSlots = 1 << 12

type tupleGroup struct {
	tuples [][]any
}

func (ec *evalCtx) runGrouped(st *runState, env []any, rows queryRows) {
	stmt := st.stmt
	groups := make(map[string]*tupleGroup)

	var order []*tupleGroup

	ec.forEachTuple(stmt, env, rows, func(e []any) bool {
		if stmt.where != nil && ec.truth(stmt.where, e) != sqlTrue {
			return true
		}

		ec.stats.match(e)

		key := ec.groupKey(stmt.groupBy, e)

		g, ok := groups[key]
		if !ok {
			g = &tupleGroup{}
			groups[key] = g
			order = append(order, g)
		}

		g.tuples = append(g.tuples, cloneTuple(e))

		return true
	})

	if len(order) == 0 && len(stmt.groupBy) == 0 {
		order = append(order, &tupleGroup{})
	}

	for _, g := range order {
		genv := emptyTuple(len(env) + st.nsrc)
		if len(g.tuples) > 0 {
			genv = g.tuples[0]
		}

		ec.group, ec.grouped = g.tuples, true
		ok := st.emit(genv)
		ec.group, ec.grouped = nil, false

		if !ok {
			return
		}
	}
}

// emptyTuple is an env of n undefined slots, used for an aggregate over no rows.
func emptyTuple(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = undef{}
	}

	return out
}

func cloneTuple(e []any) []any {
	out := make([]any, len(e))

	for i, v := range e {
		if r, ok := v.(*sqlRow); ok {
			rc := *r
			out[i] = &rc

			continue
		}

		out[i] = v
	}

	return out
}

func (ec *evalCtx) groupKey(groupBy []sqlExpr, env []any) string {
	if len(groupBy) == 0 {
		return ""
	}

	var sb strings.Builder

	for _, g := range groupBy {
		v := ec.eval(g, env)
		if isUndef(v) {
			sb.WriteString("\x00u")
		} else {
			sb.WriteString(canonicalJSON(v))
		}

		sb.WriteByte(groupKeySeparator)
	}

	return sb.String()
}

// forEachTuple enumerates source tuples, calling fn with a reused env slice until fn returns false.
func (ec *evalCtx) forEachTuple(stmt *sqlSelectStmt, env []any, rows queryRows, fn func([]any) bool) {
	if rows == nil {
		ec.expandSources(stmt, 0, env, fn)

		return
	}

	row := &sqlRow{}
	env = append(env, row)

	for i := range rows.count() {
		*row = rows.row(i)
		ec.stats.visit(row)

		if !ec.expandSources(stmt, 1, env, fn) {
			return
		}
	}
}

// expandSources binds sources[k:] by iterating each source's array, then calls fn per full tuple.
func (ec *evalCtx) expandSources(stmt *sqlSelectStmt, k int, env []any, fn func([]any) bool) bool {
	if k >= len(stmt.sources) {
		return fn(env)
	}

	src := stmt.sources[k]
	base := len(env)

	if _, isVar := src.src.(*exprVar); isVar {
		return ec.expandSources(stmt, k+1, append(env[:base:base+1], ec.evalRaw(src.src, env)), fn)
	}

	arr, ok := ec.eval(src.src, env).([]any)
	if !ok {
		return true
	}

	for _, item := range arr {
		if !ec.expandSources(stmt, k+1, append(env[:base], item), fn) {
			return false
		}
	}

	return true
}

// project builds one output value for env.
func (ec *evalCtx) project(stmt *sqlSelectStmt, env []any, nsrc int) any {
	switch {
	case stmt.isStar:
		base := len(env) - nsrc
		if base < 0 || base >= len(env) {
			return undef{}
		}

		if r, ok := env[base].(*sqlRow); ok {
			return r.full()
		}

		return env[base]
	case stmt.selectValue:
		return ec.eval(stmt.items[0].expr, env)
	default:
		obj := make(map[string]any, len(stmt.items))

		for _, it := range stmt.items {
			if v := ec.eval(it.expr, env); !isUndef(v) {
				obj[it.name] = v
			}
		}

		return obj
	}
}

// sourceRows is the container row source of stmt: none for a statement without a FROM clause.
func sourceRows(stmt *sqlSelectStmt, rows queryRows) queryRows {
	if len(stmt.sources) == 0 {
		return nil
	}

	return rows
}
