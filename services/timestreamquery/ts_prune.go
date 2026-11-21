package timestreamquery

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// pruneScan is what one table reference of a SELECT constrains: bounds on time and allowed values of equality-filtered
// columns.
type pruneScan struct {
	lo       *int64
	hi       *int64
	eq       map[string][]string
	database string
	table    string
}

// tableScanInsight is the per-table result reported through QueryInsights.
type tableScanInsight struct {
	database string
	table    string
	keys     []string
	rangeNs  int64
	coverage float64
}

type tableInst struct {
	data *tableData
	rel  string
}

func (s *pruneScan) lower(t time.Time) {
	if v := t.UnixNano(); s.lo == nil || v > *s.lo {
		s.lo = &v
	}
}

func (s *pruneScan) upper(t time.Time) {
	if v := t.UnixNano(); s.hi == nil || v < *s.hi {
		s.hi = &v
	}
}

func (s *pruneScan) restrict(col string, vals []string) {
	if s.eq == nil {
		s.eq = map[string][]string{}
	}

	prev, seen := s.eq[col]
	if !seen {
		s.eq[col] = vals

		return
	}

	s.eq[col] = slices.DeleteFunc(slices.Clone(prev), func(v string) bool { return !slices.Contains(vals, v) })
}

func (t tableInst) hasColumn(col string) bool {
	return slices.ContainsFunc(t.data.schema.cols, func(c tsColumn) bool { return strings.EqualFold(c.name, col) })
}

func collectInstances(r *relation, tables loadedSet, insts *[]tableInst, conds *[]*node) {
	switch r.kind {
	case relTable:
		if t, ok := tables[tableKey(refPart(r.ref, 0), refPart(r.ref, 1))]; ok && len(r.ref) == qualifiedParts {
			rel := r.alias
			if rel == "" {
				rel = r.ref[1]
			}

			*insts = append(*insts, tableInst{data: t, rel: rel})
		}
	case relJoin:
		collectInstances(r.left, tables, insts, conds)
		collectInstances(r.right, tables, insts, conds)

		if r.on != nil {
			*conds = append(*conds, splitAnd(r.on)...)
		}
	case relSubquery, relUnnest, relValues:
	}
}

func refPart(ref []string, i int) string {
	if i < len(ref) {
		return ref[i]
	}

	return ""
}

func splitAnd(n *node) []*node {
	if n == nil {
		return nil
	}

	if n.kind == nBinary && n.name == opAnd {
		return append(splitAnd(n.args[0]), splitAnd(n.args[1])...)
	}

	return []*node{n}
}

func splitOr(n *node) []*node {
	if n.kind == nBinary && n.name == "OR" {
		return append(splitOr(n.args[0]), splitOr(n.args[1])...)
	}

	return []*node{n}
}

// recordScans notes the pruning predicates of every real table in sel's FROM clause.
func (inf *inferrer) recordScans(sel *selectSpec) {
	if len(inf.tables) == 0 {
		return
	}

	var (
		insts []tableInst
		conds = splitAnd(sel.where)
	)

	collectInstances(sel.from, inf.tables, &insts, &conds)

	for _, inst := range insts {
		scan := pruneScan{database: inst.data.database, table: inst.data.table}

		for _, c := range conds {
			inf.applyConjunct(&scan, inst, insts, c)
		}

		inf.scans = append(inf.scans, scan)
	}
}

// columnOf returns the lower-cased column of inst that n references, or "".
func columnOf(n *node, inst tableInst, insts []tableInst) string {
	if n.kind != nColumn {
		return ""
	}

	col := n.parts[len(n.parts)-1]

	switch {
	case len(n.parts) == 1:
		for _, other := range insts {
			if other.data != inst.data && other.hasColumn(col) {
				return ""
			}
		}
	case len(n.parts) <= argsThree && strings.EqualFold(n.parts[len(n.parts)-argsTwo], inst.rel):
	default:
		return ""
	}

	if !inst.hasColumn(col) {
		return ""
	}

	return strings.ToLower(col)
}

func (inf *inferrer) applyConjunct(scan *pruneScan, inst tableInst, insts []tableInst, c *node) {
	switch c.kind {
	case nBetween:
		if !c.neg && columnOf(c.args[0], inst, insts) == colTime {
			inf.applyBounds(scan, ">=", c.args[1])
			inf.applyBounds(scan, "<=", c.args[argsTwo])
		}
	case nBinary:
		inf.applyComparison(scan, inst, insts, c)
	case nInList:
		if col := columnOf(c.args[0], inst, insts); !c.neg && col != "" && col != colTime {
			if vals, ok := literalStrings(c.args[1:]); ok {
				scan.restrict(col, vals)
			}
		}
	default:
	}
}

func (inf *inferrer) applyComparison(scan *pruneScan, inst tableInst, insts []tableInst, c *node) {
	if c.name == "OR" {
		inf.applyDisjunction(scan, inst, insts, c)

		return
	}

	left, right := columnOf(c.args[0], inst, insts), columnOf(c.args[1], inst, insts)
	op, expr, col := c.name, c.args[1], left

	if left == "" && right != "" {
		op, expr, col = flipComparison(c.name), c.args[0], right
	}

	switch {
	case col == colTime:
		inf.applyBounds(scan, op, expr)
	case col != "" && op == "=":
		if v, ok := literalString(expr); ok {
			scan.restrict(col, []string{v})
		}
	}
}

func (inf *inferrer) applyDisjunction(scan *pruneScan, inst tableInst, insts []tableInst, c *node) {
	var (
		col  string
		vals []string
	)

	for _, d := range splitOr(c) {
		if d.kind != nBinary || d.name != "=" {
			return
		}

		dc, lit := columnOf(d.args[0], inst, insts), d.args[1]
		if dc == "" {
			dc, lit = columnOf(d.args[1], inst, insts), d.args[0]
		}

		v, ok := literalString(lit)
		if dc == "" || dc == colTime || !ok || (col != "" && col != dc) {
			return
		}

		col = dc
		vals = append(vals, v)
	}

	scan.restrict(col, vals)
}

func flipComparison(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	}

	return op
}

func (inf *inferrer) applyBounds(scan *pruneScan, op string, expr *node) {
	t, ok := inf.constTime(expr)
	if !ok {
		return
	}

	switch op {
	case ">", ">=":
		scan.lower(t)
	case "<", "<=":
		scan.upper(t)
	case "=":
		scan.lower(t)
		scan.upper(t)
	}
}

func literalString(n *node) (string, bool) {
	if n.kind == nLiteral && n.typ.kind == tyVarchar {
		return n.name, true
	}

	return "", false
}

func literalStrings(nodes []*node) ([]string, bool) {
	out := make([]string, 0, len(nodes))

	for _, n := range nodes {
		v, ok := literalString(n)
		if !ok {
			return nil, false
		}

		out = append(out, v)
	}

	return out, true
}

// constTime evaluates a time expression made of now(), ago(), literals and interval arithmetic.
func (inf *inferrer) constTime(n *node) (time.Time, bool) {
	switch n.kind {
	case nCall:
		return inf.constTimeCall(n)
	case nColumn:
		return inf.bareTime(n)
	case nTypedLit:
		if n.typ.kind == tyTimestamp || n.typ.kind == tyDate {
			return parseTSValue(n.name)
		}
	case nLiteral:
		if n.typ.kind == tyVarchar {
			return parseTSValue(n.name)
		}
	case nCast:
		if n.typ.kind == tyTimestamp {
			return inf.constTime(n.args[0])
		}
	case nVar:
		if v, ok := inf.vars[n.name]; ok {
			return parseTSValue(v)
		}
	case nBinary:
		return inf.constTimeArith(n)
	default:
	}

	return time.Time{}, false
}

func (inf *inferrer) bareTime(n *node) (time.Time, bool) {
	if len(n.parts) != 1 {
		return time.Time{}, false
	}

	switch strings.ToLower(n.parts[0]) {
	case fnCurTS, fnLocalTS:
		return inf.now.UTC(), true
	case "current_date":
		return inf.now.UTC().Truncate(hoursInDay * time.Hour), true
	}

	return time.Time{}, false
}

func (inf *inferrer) constTimeCall(n *node) (time.Time, bool) {
	switch strings.ToLower(n.name) {
	case "now", fnCurTS, fnLocalTS:
		return inf.now.UTC(), true
	case "ago":
		d, ok := constDuration(n.arg(0))

		return inf.now.UTC().Add(-time.Duration(d)), ok && n.arg(0) != nil
	case "bin":
		t, okT := inf.constTime(n.arg(0))
		d, okD := constDuration(n.arg(1))

		if !okT || !okD || d <= 0 {
			return time.Time{}, false
		}

		ns := t.UnixNano()

		return time.Unix(0, ns-floorMod(ns, d)).UTC(), true
	}

	return time.Time{}, false
}

func (inf *inferrer) constTimeArith(n *node) (time.Time, bool) {
	if n.name != "+" && n.name != "-" {
		return time.Time{}, false
	}

	if t, ok := inf.constTime(n.args[0]); ok {
		d, okD := constDuration(n.args[1])
		if n.name == "-" {
			d = -d
		}

		return t.Add(time.Duration(d)), okD
	}

	if n.name == "+" {
		d, okD := constDuration(n.args[0])
		t, okT := inf.constTime(n.args[1])

		return t.Add(time.Duration(d)), okD && okT
	}

	return time.Time{}, false
}

// constDuration evaluates an INTERVAL literal, a duration literal such as 15m or parse_duration('1d') to nanoseconds.
func constDuration(n *node) (int64, bool) {
	if n == nil {
		return 0, false
	}

	switch {
	case n.kind == nLiteral && n.typ.kind == tyIntervalDS:
		return durationText(n.name)
	case n.kind == nInterval && n.typ.kind == tyIntervalDS && len(n.parts) == 1:
		return durationText(n.name + n.parts[0])
	case n.kind == nCall && strings.EqualFold(n.name, "parse_duration") && n.arg(0) != nil:
		if v, ok := literalString(n.args[0]); ok {
			return durationText(v)
		}
	}

	return 0, false
}

func durationText(s string) (int64, bool) {
	m := reDurationText.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}

	f, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, false
	}

	return int64(f * float64(unitNanos(m[argsTwo]))), true
}

// partitionKeys returns the table's partition key columns: its customer-defined keys, else measure_name.
func partitionKeys(src RecordSource, region, db, table string) []string {
	if ks, ok := src.(PartitionKeySource); ok {
		if keys := ks.TablePartitionKeys(region, db, table); len(keys) > 0 {
			return keys
		}
	}

	return []string{colMeasureName}
}

func partitionTuple(r SourceRecord, keys []string) string {
	vals := make([]string, len(keys))

	for i, k := range keys {
		if strings.EqualFold(k, colMeasureName) {
			vals[i] = r.MeasureName

			continue
		}

		for _, d := range r.Dimensions {
			if strings.EqualFold(d.Name, k) {
				vals[i] = d.Value
			}
		}
	}

	return strings.Join(vals, "\x00")
}

func spatialCoverage(scan pruneScan, keys []string, recs []SourceRecord) float64 {
	constrained := false

	for _, k := range keys {
		if _, ok := scan.eq[strings.ToLower(k)]; ok {
			constrained = true
		}
	}

	if !constrained {
		return 1
	}

	total, matched := map[string]bool{}, map[string]bool{}

	for _, r := range recs {
		tuple := partitionTuple(r, keys)
		total[tuple] = true

		if tupleAllowed(scan, keys, strings.Split(tuple, "\x00")) {
			matched[tuple] = true
		}
	}

	if len(total) == 0 {
		return 0
	}

	return float64(len(matched)) / float64(len(total))
}

func tupleAllowed(scan pruneScan, keys, vals []string) bool {
	for i, k := range keys {
		if allowed, ok := scan.eq[strings.ToLower(k)]; ok && !slices.Contains(allowed, vals[i]) {
			return false
		}
	}

	return true
}

func temporalRange(scan pruneScan, recs []SourceRecord, now time.Time) int64 {
	lo, hi := scan.lo, scan.hi

	if lo == nil && len(recs) > 0 {
		first := recs[0].Time.UnixNano()
		lo = &first
	}

	if hi == nil {
		upper := now.UnixNano()
		hi = &upper
	}

	if lo == nil || *hi < *lo {
		return 0
	}

	return *hi - *lo
}

// scanInsights computes the temporal range and spatial coverage of each table reference in a query.
func (b *InMemoryBackend) scanInsights(
	region string, scans []pruneScan, tables loadedSet, now time.Time, src RecordSource,
) []tableScanInsight {
	out := make([]tableScanInsight, 0, len(scans))

	for _, s := range scans {
		t, ok := tables[tableKey(s.database, s.table)]
		if !ok {
			continue
		}

		keys := partitionKeys(src, region, s.database, s.table)

		out = append(out, tableScanInsight{
			database: s.database, table: s.table, keys: keys,
			rangeNs:  temporalRange(s, t.records, now),
			coverage: spatialCoverage(s, keys, t.records),
		})
	}

	return out
}

func (b *InMemoryBackend) tableARN(region string, t tableScanInsight) string {
	return arn.Build("timestream", region, b.accountID, "database/"+t.database+"/table/"+t.table)
}
