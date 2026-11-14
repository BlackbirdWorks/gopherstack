package athena

import (
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

const (
	qualifiedColLen = 3
	icebergLong     = "long"
	opEq            = "="
	opLT            = "<"
	opLE            = "<="
	opGT            = ">"
	opGE            = ">="
	opIsNull        = "isnull"
	opNotNull       = "notnull"
	opIn            = "in"
	opBetween       = "between"
)

// filterTerm is one top-level WHERE conjunct of the form column <op> literal(s).
type filterTerm struct {
	col  string
	op   string
	lits []token
}

// whereTerms extracts the prunable conjuncts of a single-table SELECT's WHERE clause;
// anything it does not understand is skipped, so the result never over-prunes.
func whereTerms(sqlText string) []filterTerm {
	toks, err := tokenize(sqlText)
	if err != nil {
		return nil
	}

	sig := significant(toks)
	start, end := whereBounds(sig)

	if start < 0 {
		return nil
	}

	var terms []filterTerm

	for _, conj := range splitAnd(sig[start:end]) {
		if t, ok := parseTerm(conj); ok {
			terms = append(terms, t)
		}
	}

	return terms
}

// whereBounds returns the WHERE clause span of a flat SELECT; start<0 when the query has
// joins, unions, subqueries in FROM, or top-level OR.
func whereBounds(sig []token) (int, int) {
	if !isFlatSelect(sig) {
		return -1, -1
	}

	depth, start := 0, -1

	for i, t := range sig {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && t.isWord(kwWhere) && start < 0:
			start = i + 1
		case depth == 0 && start >= 0 && t.kind == tkWord && isClauseEnder(strings.ToLower(t.text)):
			return start, i
		}
	}

	return start, len(sig)
}

// isFlatSelect rejects queries whose WHERE could bind to something other than one table's columns.
func isFlatSelect(sig []token) bool {
	depth, selects := 0, 0

	for _, t := range sig {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case t.isWord("select"):
			if selects++; selects > 1 {
				return false
			}
		case depth == 0 && isCompoundWord(t):
			return false
		}
	}

	return true
}

func isCompoundWord(t token) bool {
	return slices.ContainsFunc([]string{"join", kwUnion, "intersect", "except", "with", "or"}, t.isWord)
}

func splitAnd(toks []token) [][]token {
	var (
		out     [][]token
		depth   int
		between bool
	)

	cur := make([]token, 0, len(toks))

	for _, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && t.isWord(kwBetween):
			between = true
		case depth == 0 && t.isWord("and") && between:
			between = false
		case depth == 0 && t.isWord("and"):
			out, cur = append(out, cur), make([]token, 0, len(toks))

			continue
		}

		cur = append(cur, t)
	}

	return append(out, cur)
}

func colName(toks []token) (string, int, bool) {
	if len(toks) == 0 || (toks[0].kind != tkWord && toks[0].kind != tkQuoted) {
		return "", 0, false
	}

	if len(toks) >= 3 && toks[1].isPunct(".") && (toks[2].kind == tkWord || toks[2].kind == tkQuoted) {
		return toks[2].ident(), qualifiedColLen, true
	}

	return toks[0].ident(), 1, true
}

func literalTokens(toks []token) ([]token, bool) {
	switch {
	case len(toks) == 1 && (toks[0].kind == tkString || toks[0].kind == tkNumber):
		return toks, true
	case len(toks) == 2 && toks[0].isPunct("-") && toks[1].kind == tkNumber:
		return []token{{kind: tkNumber, text: "-" + toks[1].text}}, true
	case len(toks) == 2 && (toks[0].isWord("date") || toks[0].isWord("timestamp")) && toks[1].kind == tkString:
		return toks[1:], true
	default:
		return nil, false
	}
}

func parseTerm(c []token) (filterTerm, bool) {
	col, n, ok := colName(c)
	if !ok || n >= len(c) {
		return filterTerm{}, false
	}

	rest := c[n:]

	if t, handled := parseKeywordTerm(col, rest); handled {
		return t, t.op != ""
	}

	op, operand := rest[0].text, rest[1:]
	if (op == opLT || op == opGT) && len(operand) > 0 && operand[0].isPunct("=") {
		op, operand = op+"=", operand[1:]
	}

	switch op {
	case opEq, opLT, opLE, opGT, opGE:
	default:
		return filterTerm{}, false
	}

	lit, ok := literalTokens(operand)
	if !ok {
		return filterTerm{}, false
	}

	return filterTerm{col: col, op: op, lits: lit}, true
}

// parseKeywordTerm handles IS [NOT] NULL, IN (...) and BETWEEN; handled=false means a symbolic comparison.
func parseKeywordTerm(col string, rest []token) (filterTerm, bool) {
	switch {
	case len(rest) == 2 && rest[0].isWord("is") && rest[1].isWord("null"):
		return filterTerm{col: col, op: opIsNull}, true
	case len(rest) == 3 && rest[0].isWord("is") && rest[1].isWord("not") && rest[2].isWord("null"):
		return filterTerm{col: col, op: opNotNull}, true
	case rest[0].isWord("in") && len(rest) > 2 && rest[1].isPunct("(") && rest[len(rest)-1].isPunct(")"):
		t, _ := parseInList(col, rest[2:len(rest)-1])

		return t, true
	case rest[0].isWord(kwBetween):
		t, _ := parseBetween(col, rest[1:])

		return t, true
	default:
		return filterTerm{}, false
	}
}

func parseInList(col string, toks []token) (filterTerm, bool) {
	t := filterTerm{col: col, op: opIn}

	for _, part := range splitCommas(toks) {
		lit, ok := literalTokens(part)
		if !ok {
			return filterTerm{}, false
		}

		t.lits = append(t.lits, lit...)
	}

	return t, len(t.lits) > 0
}

// parseBetween turns "a AND b" into a >= a conjunct; the upper side is handled by a second term.
func parseBetween(col string, toks []token) (filterTerm, bool) {
	for i, t := range toks {
		if !t.isWord("and") {
			continue
		}

		lo, ok1 := literalTokens(toks[:i])
		hi, ok2 := literalTokens(toks[i+1:])

		if ok1 && ok2 {
			return filterTerm{col: col, op: opBetween, lits: []token{lo[0], hi[0]}}, true
		}
	}

	return filterTerm{}, false
}

// boundOf converts a literal to the comparable form of an Iceberg primitive type.
func boundOf(icebergType string, t token) (iceberg.Bound, bool) {
	switch icebergType {
	case typeInt, icebergLong:
		n, err := strconv.ParseInt(literalText(t), 10, 64)

		return n, err == nil
	case typeDouble:
		f, err := strconv.ParseFloat(literalText(t), 64)

		return f, err == nil
	case columnTypeString:
		return literalText(t), t.kind == tkString
	case typeDate:
		d, err := time.Parse(dateLayout, strings.TrimSpace(literalText(t)))

		return d.Unix() / secondsPerDayInt, err == nil && t.kind == tkString
	case typeTimestamp:
		ts, err := time.Parse("2006-01-02 15:04:05.999999999", strings.TrimSpace(literalText(t)))

		return ts.UnixMicro(), err == nil && t.kind == tkString
	default:
		return nil, false
	}
}

func literalText(t token) string {
	if t.kind == tkString {
		return stringLiteral(t)
	}

	return t.text
}

// fileStats are the per-column facts a data file carries for pruning.
type fileStats struct {
	lo, hi        iceberg.Bound
	nulls, values int64
	hasNulls      bool
	hasCount      bool
}

func statsFor(f fileEntry, field iceberg.Field, spec iceberg.PartitionSpec, ptype string) fileStats {
	df := f.entry.DataFile

	var st fileStats

	if raw, ok := df.LowerBounds[field.ID]; ok {
		st.lo, _ = iceberg.DecodeBound(ptype, raw)
	}

	if raw, ok := df.UpperBounds[field.ID]; ok {
		st.hi, _ = iceberg.DecodeBound(ptype, raw)
	}

	if n, ok := df.NullCounts[field.ID]; ok {
		st.nulls, st.hasNulls = n, true
	}

	if n, ok := df.ValueCounts[field.ID]; ok {
		st.values, st.hasCount = n, true
	}

	for _, pf := range spec.Fields {
		if pf.SourceID != field.ID || !strings.EqualFold(pf.Transform, "identity") {
			continue
		}

		if v, has := df.Partition[pf.Name]; has && v != nil {
			if pv, ok := partitionBound(ptype, v); ok {
				st.lo, st.hi = pv, pv
			}
		}
	}

	return st
}

func partitionBound(ptype string, v any) (iceberg.Bound, bool) {
	switch ptype {
	case typeInt, icebergLong, typeDate, typeTimestamp:
		return toInt64(v), true
	case columnTypeString:
		s, ok := v.(string)

		return s, ok
	default:
		return nil, false
	}
}

// pruned reports whether the file provably holds no row satisfying every term.
func (tbl *icebergTable) pruned(terms []filterTerm, schema iceberg.Schema, f fileEntry) bool {
	spec, _ := tbl.meta.SpecByID(f.spec)

	for _, t := range terms {
		for _, fld := range schema.Fields {
			if !strings.EqualFold(fld.Name, t.col) {
				continue
			}

			ptype := iceberg.PrimitiveName(fld.Type)
			if ptype == "" {
				continue
			}

			if termExcludes(t, ptype, statsFor(f, fld, spec, ptype)) {
				return true
			}
		}
	}

	return false
}

func termExcludes(t filterTerm, ptype string, st fileStats) bool {
	if t.op == opIsNull || t.op == opNotNull {
		return nullTermExcludes(t.op, st)
	}

	if st.lo == nil || st.hi == nil {
		return false
	}

	lits, ok := comparableLits(t, ptype, st)
	if !ok {
		return false
	}

	switch t.op {
	case opIn:
		for _, b := range lits {
			if !outside(b, st) {
				return false
			}
		}

		return true
	case opBetween:
		return cmpBound(st.hi, lits[0]) < 0 || cmpBound(st.lo, lits[1]) > 0
	default:
		return rangeExcludes(t.op, lits[0], st)
	}
}

func nullTermExcludes(op string, st fileStats) bool {
	if op == opIsNull {
		return st.hasNulls && st.nulls == 0
	}

	return st.hasNulls && st.hasCount && st.values > 0 && st.nulls == st.values
}

func rangeExcludes(op string, lit iceberg.Bound, st fileStats) bool {
	switch op {
	case opEq:
		return outside(lit, st)
	case opLT:
		return cmpBound(st.lo, lit) >= 0
	case opLE:
		return cmpBound(st.lo, lit) > 0
	case opGT:
		return cmpBound(st.hi, lit) <= 0
	case opGE:
		return cmpBound(st.hi, lit) < 0
	default:
		return false
	}
}

// comparableLits converts the literals, failing when any is not comparable with the file's bounds.
func comparableLits(t filterTerm, ptype string, st fileStats) ([]iceberg.Bound, bool) {
	lits := make([]iceberg.Bound, 0, len(t.lits))

	for _, l := range t.lits {
		b, ok := boundOf(ptype, l)
		if !ok {
			return nil, false
		}

		for _, edge := range []iceberg.Bound{st.lo, st.hi} {
			if _, same := iceberg.CompareBound(edge, b); !same {
				return nil, false
			}
		}

		lits = append(lits, b)
	}

	return lits, len(lits) > 0
}

func outside(b iceberg.Bound, st fileStats) bool {
	return cmpBound(b, st.lo) < 0 || cmpBound(b, st.hi) > 0
}

// cmpBound orders two bounds; incomparable pairs compare equal so nothing is pruned on them.
func cmpBound(a, b iceberg.Bound) int {
	c, ok := iceberg.CompareBound(a, b)
	if !ok {
		return 0
	}

	return c
}

// columnBounds computes the lower and upper bound of every primitive column the pruner understands.
func columnBounds(res *sqlResult, rows []int, cols []outCol, schema iceberg.Schema) (map[int][]byte, map[int][]byte) {
	lower, upper := map[int][]byte{}, map[int][]byte{}

	for i, c := range cols {
		ptype := iceberg.PrimitiveName(schema.Fields[i].Type)

		lo, hi, ok := columnRange(res, rows, c.src, ptype)
		if !ok {
			continue
		}

		loB, err1 := iceberg.EncodeBound(ptype, lo)
		hiB, err2 := iceberg.EncodeBound(ptype, hi)

		if err1 == nil && err2 == nil {
			lower[c.id], upper[c.id] = loB, hiB
		}
	}

	return lower, upper
}

// columnRange is the smallest and largest non-null value of a column; ok=false when it has none or one is unparseable.
func columnRange(res *sqlResult, rows []int, src int, ptype string) (iceberg.Bound, iceberg.Bound, bool) {
	var lo, hi iceberg.Bound

	for _, r := range rows {
		v, null := res.cell(r, src)
		if null {
			continue
		}

		b, ok := cellBound(ptype, v)
		if !ok {
			return nil, nil, false
		}

		if lo == nil || cmpBound(b, lo) < 0 {
			lo = b
		}

		if hi == nil || cmpBound(b, hi) > 0 {
			hi = b
		}
	}

	return lo, hi, lo != nil
}

func cellBound(ptype, v string) (iceberg.Bound, bool) {
	switch ptype {
	case typeInt, icebergLong:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)

		return n, err == nil
	case typeDouble:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)

		return f, err == nil && !math.IsNaN(f)
	case columnTypeString:
		return v, true
	case typeDate:
		d, err := time.Parse(dateLayout, strings.TrimSpace(v))

		return d.Unix() / secondsPerDayInt, err == nil
	case typeTimestamp:
		ts, err := time.Parse("2006-01-02 15:04:05.999999999", strings.TrimSpace(v))

		return ts.UnixMicro(), err == nil
	default:
		return nil, false
	}
}
