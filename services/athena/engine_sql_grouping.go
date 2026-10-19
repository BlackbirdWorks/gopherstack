package athena

import (
	"slices"
	"strconv"
	"strings"
)

// groupExpr is one GROUP BY expression; key is its normalised token text.
type groupExpr struct {
	key  string
	text string
}

type groupSet []groupExpr

func exprOf(toks []token) groupExpr {
	sig := significant(toks)
	parts := make([]string, len(sig))

	for i, t := range sig {
		parts[i] = strings.ToLower(t.text)
	}

	return groupExpr{key: strings.Join(parts, " "), text: strings.TrimSpace(joinTokens(toks))}
}

func errGrouping(msg string) error { return &syntaxError{"line 1:1: " + msg} }

// groupingBlock is a SELECT split around its GROUP BY clause.
type groupingBlock struct {
	prefix   []token
	distinct string
	items    [][]token
	from     []token
	groupBy  [][]token
	having   []token
	tail     []token
}

func depthZeroWord(toks []token, from int, words ...string) int {
	depth := 0

	for i := from; i < len(toks); i++ {
		switch {
		case toks[i].isPunct("("):
			depth++
		case toks[i].isPunct(")"):
			depth--
		case depth == 0 && toks[i].kind == tkWord:
			for _, w := range words {
				if strings.EqualFold(toks[i].text, w) {
					return i
				}
			}
		}
	}

	return -1
}

func hasGroupingExtension(by [][]token) bool {
	for _, el := range by {
		sig := significant(el)
		if len(sig) > 1 && sig[1].isPunct("(") && (sig[0].isWord("rollup") || sig[0].isWord("cube")) {
			return true
		}

		if len(sig) > argsTwo && sig[0].isWord("grouping") && sig[1].isWord("sets") {
			return true
		}
	}

	return false
}

func firstAtOrEnd(toks []token, from int, words ...string) int {
	if i := depthZeroWord(toks, from, words...); i >= 0 {
		return i
	}

	return len(toks)
}

// splitGroupingBlock splits a depth-zero SELECT; ok=false when it has no ROLLUP/CUBE/GROUPING SETS.
func splitGroupingBlock(toks []token) (groupingBlock, bool, error) {
	g := depthZeroWord(toks, 0, kwGroup)
	if g < 0 {
		return groupingBlock{}, false, nil
	}

	by := nextSig(toks, g)
	if by < 0 || !toks[by].isWord("by") {
		return groupingBlock{}, false, nil
	}

	end := firstAtOrEnd(toks, by+1, kwHaving, "order", kwLimit, kwOffset, kwFetch, "window")
	groupBy := splitArgs(toks[by+1 : end])

	if !hasGroupingExtension(groupBy) {
		return groupingBlock{}, false, nil
	}

	if depthZeroWord(toks, 0, "union", kwIntersect, kwExcept) >= 0 {
		return groupingBlock{}, false, errGrouping("GROUPING SETS combined with set operations is not supported")
	}

	sel := depthZeroWord(toks, 0, "select")
	if sel < 0 || sel > g {
		return groupingBlock{}, false, errGrouping("mismatched input 'GROUP'")
	}

	return fillGroupingBlock(toks, sel, g, end, groupBy), true, nil
}

func fillGroupingBlock(toks []token, sel, g, end int, groupBy [][]token) groupingBlock {
	b := groupBy
	blk := groupingBlock{prefix: toks[:sel], groupBy: b}

	from := depthZeroWord(toks[:g], sel, kwFrom)
	itemsEnd := g

	if from >= 0 {
		itemsEnd = from
		blk.from = toks[from:g]
	}

	body := toks[sel+1 : itemsEnd]
	if n := nextSig(body, -1); n >= 0 && (body[n].isWord("distinct") || body[n].isWord("all")) {
		blk.distinct = body[n].text + " "
		body = body[n+1:]
	}

	blk.items = splitArgs(body)

	hav := depthZeroWord(toks, end, kwHaving)
	tailStart := end

	if hav == end {
		tailStart = firstAtOrEnd(toks, hav+1, "order", kwLimit, kwOffset, kwFetch, "window")
		blk.having = toks[hav+1 : tailStart]
	}

	blk.tail = toks[tailStart:]

	return blk
}

// splitAlias separates `expr [AS] alias` into its parts.
func splitAlias(item []token) ([]token, string) {
	sig := significant(item)
	if len(sig) >= argsThree && sig[len(sig)-2].isWord("as") {
		idx := lastTopLevelWord(item, "as")

		return item[:idx], sig[len(sig)-1].ident()
	}

	return item, ""
}

func trimSpaceTokens(toks []token) []token {
	for len(toks) > 0 && toks[0].kind == tkSpace {
		toks = toks[1:]
	}

	for len(toks) > 0 && toks[len(toks)-1].kind == tkSpace {
		toks = toks[:len(toks)-1]
	}

	return toks
}

// parenInner returns the tokens inside a fully parenthesised group.
func parenInner(toks []token) ([]token, bool) {
	t := trimSpaceTokens(toks)
	if len(t) < argsTwo || !t[0].isPunct("(") || matchParen(t, 0) != len(t)-1 {
		return nil, false
	}

	return t[1 : len(t)-1], true
}

func unitsOf(arg []token) []groupExpr {
	inner, ok := parenInner(arg)
	if !ok {
		return []groupExpr{exprOf(arg)}
	}

	if len(significant(inner)) == 0 {
		return nil
	}

	parts := splitArgs(inner)
	out := make([]groupExpr, 0, len(parts))

	for _, p := range parts {
		out = append(out, exprOf(p))
	}

	return out
}

// callInner returns the argument tokens of `kw(...)` or `kw2 kw(...)`, matching the leading words.
func callInner(el []token, words ...string) ([]token, bool) {
	t := trimSpaceTokens(el)
	i := -1

	for _, w := range words {
		i = nextSig(t, i)
		if i < 0 || !t[i].isWord(w) {
			return nil, false
		}
	}

	open := nextSig(t, i)
	if open < 0 || !t[open].isPunct("(") || matchParen(t, open) != len(t)-1 {
		return nil, false
	}

	return t[open+1 : len(t)-1], true
}

func groupingSetsOf(el []token) []groupSet {
	if inner, ok := callInner(el, "rollup"); ok {
		return rollupSets(unitList(inner))
	}

	if inner, ok := callInner(el, "cube"); ok {
		return cubeSets(unitList(inner))
	}

	if inner, ok := callInner(el, "grouping", "sets"); ok {
		var sets []groupSet
		for _, a := range splitArgs(inner) {
			sets = append(sets, groupSet(unitsOf(a)))
		}

		return sets
	}

	return []groupSet{groupSet(unitsOf(el))}
}

func unitList(toks []token) [][]groupExpr {
	args := splitArgs(toks)
	units := make([][]groupExpr, 0, len(args))

	for _, a := range args {
		units = append(units, unitsOf(a))
	}

	return units
}

func rollupSets(units [][]groupExpr) []groupSet {
	sets := make([]groupSet, 0, len(units)+1)

	for k := len(units); k >= 0; k-- {
		var s groupSet
		for _, u := range units[:k] {
			s = append(s, u...)
		}

		sets = append(sets, s)
	}

	return sets
}

func cubeSets(units [][]groupExpr) []groupSet {
	total := 1 << len(units)
	sets := make([]groupSet, 0, total)

	for mask := total - 1; mask >= 0; mask-- {
		var s groupSet

		for i, u := range units {
			if mask&(1<<(len(units)-1-i)) != 0 {
				s = append(s, u...)
			}
		}

		sets = append(sets, s)
	}

	return sets
}

func crossSets(parts [][]groupSet) []groupSet {
	acc := []groupSet{nil}

	for _, p := range parts {
		next := make([]groupSet, 0, len(acc)*len(p))

		for _, a := range acc {
			for _, b := range p {
				merged := make(groupSet, 0, len(a)+len(b))
				merged = append(merged, a...)
				next = append(next, appendUnique(merged, b))
			}
		}

		acc = next
	}

	return acc
}

func appendUnique(s groupSet, more groupSet) groupSet {
	for _, e := range more {
		dup := false

		for _, have := range s {
			dup = dup || have.key == e.key
		}

		if !dup {
			s = append(s, e)
		}
	}

	return s
}

func (b *groupingBlock) allExprs(sets []groupSet) []groupExpr {
	var all groupSet
	for _, s := range sets {
		all = appendUnique(all, s)
	}

	return all
}

// resolveRef maps ordinals and select aliases in a grouping expression to the select item it names.
func (b *groupingBlock) resolveRef(e groupExpr) groupExpr {
	if n, err := strconv.Atoi(e.key); err == nil && n >= 1 && n <= len(b.items) {
		expr, _ := splitAlias(b.items[n-1])

		return exprOf(expr)
	}

	for _, it := range b.items {
		expr, alias := splitAlias(it)
		if alias != "" && alias == e.key {
			return exprOf(expr)
		}
	}

	return e
}

func isAggregateName(lw string) bool {
	switch lw {
	case "sum", fnCount, "avg", "min", "max", "array_agg", "map_agg", "histogram", "count_if", "bool_and",
		"bool_or", "every", "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp", "arbitrary",
		"any_value", "max_by", "min_by", "corr", "covar_pop", "covar_samp", "geometric_mean", "checksum":
		return true
	default:
		return strings.HasPrefix(lw, "approx_")
	}
}

func matchesAt(toks []token, i int, key string) int {
	var parts []string

	j := i
	for ; j < len(toks); j++ {
		if toks[j].kind == tkSpace {
			continue
		}

		parts = append(parts, strings.ToLower(toks[j].text))
		cur := strings.Join(parts, " ")

		if cur == key {
			return j
		}

		if !strings.HasPrefix(key, cur+" ") {
			return -1
		}
	}

	return -1
}

type branch struct {
	present map[string]bool
	absent  []groupExpr
	all     []groupExpr
}

func (br *branch) groupingValue(args []token) (int, error) {
	v := 0

	for _, a := range splitArgs(args) {
		e := exprOf(a)
		known := false

		for _, g := range br.all {
			known = known || g.key == e.key
		}

		if !known {
			return 0, errGrouping("arguments to GROUPING() must be expressions referenced by GROUP BY")
		}

		v <<= 1
		if !br.present[e.key] {
			v |= 1
		}
	}

	return v, nil
}

// subst walks one expression, NULLing absent grouping expressions and folding grouping() calls.
type subst struct {
	br    *branch
	toks  []token
	sb    strings.Builder
	aggs  []bool
	inAgg int
}

// substitute NULLs out the absent grouping expressions and folds grouping() calls.
func (br *branch) substitute(toks []token) (string, error) {
	s := &subst{br: br, toks: toks}

	for i := 0; i < len(toks); i++ {
		next, err := s.step(i)
		if err != nil {
			return "", err
		}

		i = next
	}

	return s.sb.String(), nil
}

func (s *subst) trackParens(i int) {
	switch t := s.toks[i]; {
	case t.isPunct("("):
		pv := prevSig(s.toks, i)
		agg := pv >= 0 && s.toks[pv].kind == tkWord && isAggregateName(strings.ToLower(s.toks[pv].text))

		if agg {
			s.inAgg++
		}

		s.aggs = append(s.aggs, agg)
	case t.isPunct(")") && len(s.aggs) > 0:
		if s.aggs[len(s.aggs)-1] {
			s.inAgg--
		}

		s.aggs = s.aggs[:len(s.aggs)-1]
	}
}

func (s *subst) groupingCall(i int) (int, bool, error) {
	open := nextSig(s.toks, i)
	if !s.toks[i].isWord("grouping") || open < 0 || !s.toks[open].isPunct("(") {
		return i, false, nil
	}

	closeIdx := matchParen(s.toks, open)
	if closeIdx < 0 {
		return i, false, errGrouping("mismatched input in GROUPING()")
	}

	v, err := s.br.groupingValue(s.toks[open+1 : closeIdx])
	if err != nil {
		return i, false, err
	}

	s.sb.WriteString(strconv.Itoa(v))

	return closeIdx, true, nil
}

func (s *subst) step(i int) (int, error) {
	t := s.toks[i]
	s.trackParens(i)

	if s.inAgg == 0 {
		if next, ok, err := s.groupingCall(i); ok || err != nil {
			return next, err
		}

		if t.kind != tkSpace && !prevIsDot(s.toks, i) {
			if end, ok := s.br.absentAt(s.toks, i); ok {
				s.sb.WriteString("NULL")

				return end, nil
			}
		}
	}

	s.sb.WriteString(t.text)

	return i, nil
}

func prevIsDot(toks []token, i int) bool {
	p := prevSig(toks, i)

	return p >= 0 && toks[p].isPunct(".")
}

func (br *branch) absentAt(toks []token, i int) (int, bool) {
	best, found := -1, false

	for _, g := range br.absent {
		if end := matchesAt(toks, i, g.key); end > best {
			best, found = end, true
		}
	}

	return best, found
}

func (b *groupingBlock) branchSQL(br *branch, set groupSet) (string, error) {
	items := make([]string, len(b.items))

	for i, it := range b.items {
		s, err := b.itemSQL(br, it)
		if err != nil {
			return "", err
		}

		items[i] = s
	}

	sql := "SELECT " + b.distinct + strings.Join(items, ", ") + " " + strings.TrimSpace(joinTokens(b.from))

	if len(set) > 0 {
		exprs := make([]string, len(set))
		for i, e := range set {
			exprs[i] = e.text
		}

		sql += " GROUP BY " + strings.Join(exprs, ", ")
	}

	if len(significant(b.having)) > 0 {
		h, err := br.substitute(b.having)
		if err != nil {
			return "", err
		}

		sql += " HAVING " + h
	}

	if len(set) == 0 && !b.hasAggregate() {
		sql = "SELECT * FROM (" + sql + " LIMIT 1)"
	}

	return sql, nil
}

func hasAggregateCall(toks []token) bool {
	for i, t := range toks {
		if t.kind != tkWord || !isAggregateName(strings.ToLower(t.text)) {
			continue
		}

		if n := nextSig(toks, i); n >= 0 && toks[n].isPunct("(") {
			return true
		}
	}

	return false
}

// hasAggregate reports whether the grand-total branch aggregates; if not it must still yield one row.
func (b *groupingBlock) hasAggregate() bool {
	return slices.ContainsFunc(b.items, hasAggregateCall) || hasAggregateCall(b.having)
}

func (b *groupingBlock) itemSQL(br *branch, item []token) (string, error) {
	expr, alias := splitAlias(item)
	key := exprOf(expr).key

	for _, g := range br.absent {
		if g.key != key {
			continue
		}

		if alias == "" {
			sig := significant(expr)
			alias = sig[len(sig)-1].ident()
		}

		return "NULL AS " + quoteIdent(alias), nil
	}

	out, err := br.substitute(item)

	return strings.TrimSpace(out), err
}

// expandGroupingSets rewrites GROUP BY ROLLUP/CUBE/GROUPING SETS into a UNION
// ALL of plain GROUP BY branches (SQLite has no grouping sets).
func expandGroupingSets(toks []token) ([]token, error) {
	toks, err := expandNested(toks)
	if err != nil {
		return nil, err
	}

	blk, ok, err := splitGroupingBlock(toks)
	if err != nil || !ok {
		return toks, err
	}

	parts := make([][]groupSet, len(blk.groupBy))

	for i, el := range blk.groupBy {
		parts[i] = groupingSetsOf(el)
	}

	sets := crossSets(parts)
	for _, s := range sets {
		for j := range s {
			s[j] = blk.resolveRef(s[j])
		}
	}

	all := blk.allExprs(sets)
	branches := make([]string, len(sets))

	for i, s := range sets {
		br := branchFor(all, s)

		branches[i], err = blk.branchSQL(br, s)
		if err != nil {
			return nil, err
		}
	}

	sql := joinTokens(
		blk.prefix,
	) + "SELECT * FROM (" + strings.Join(
		branches,
		" UNION ALL ",
	) + ") " + joinTokens(
		blk.tail,
	)

	return tokenize(sql)
}

func branchFor(all []groupExpr, set groupSet) *branch {
	br := &branch{present: map[string]bool{}, all: all}
	for _, e := range set {
		br.present[e.key] = true
	}

	for _, g := range all {
		if !br.present[g.key] {
			br.absent = append(br.absent, g)
		}
	}

	return br
}

// expandNested rewrites parenthesised sub-selects first.
func expandNested(toks []token) ([]token, error) {
	out := make([]token, 0, len(toks))

	for i := 0; i < len(toks); i++ {
		if !toks[i].isPunct("(") {
			out = append(out, toks[i])

			continue
		}

		closeIdx := matchParen(toks, i)
		if closeIdx < 0 {
			return toks, nil
		}

		inner := toks[i+1 : closeIdx]

		if n := nextSig(inner, -1); n >= 0 && (inner[n].isWord("select") || inner[n].isWord("with")) {
			var err error
			if inner, err = expandGroupingSets(inner); err != nil {
				return nil, err
			}
		}

		out = append(out, toks[i])
		out = append(out, inner...)
		out = append(out, toks[closeIdx])
		i = closeIdx
	}

	return out, nil
}
