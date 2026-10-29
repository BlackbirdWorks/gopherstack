package athena

import (
	"errors"
	"fmt"
	"strings"
)

var errMultiStatement = errors.New("line 1:1: only a single statement is supported")

// resolvedRef is a table reference mapped to an attached SQLite schema.
type resolvedRef struct {
	schema     string
	name       string
	display    string
	structCols []string
	found      bool
}

type translator struct {
	resolve       func(parts []string) resolvedRef
	ctes          map[string]bool
	err           error
	unnest        map[string]unnestCol
	qualifiers    map[string]bool
	structCols    map[string]bool
	unresolved    []string
	unnestAliases []string
	nUnnest       int
}

type scope struct {
	opener   string
	fromList bool
}

func renamedFunc(lw string) string {
	switch lw {
	case "date_trunc":
		return "athena_date_trunc"
	case "date_add":
		return "athena_date_add"
	case "date_diff":
		return "athena_date_diff"
	case unitYear:
		return "athena_year"
	case unitMonth:
		return "athena_month"
	case unitDay, "day_of_month":
		return "athena_day"
	case unitHour:
		return "athena_hour"
	case "regexp_like":
		return "athena_regexp_like"
	case "strpos":
		return "instr"
	case "if":
		return "iif"
	default:
		return renamedAlias(lw)
	}
}

func renamedAlias(lw string) string {
	switch lw {
	case "json_extract":
		return "athena_json_extract"
	case "greatest":
		return "max"
	case "least":
		return "min"
	case "ceiling":
		return "ceil"
	case fnTruncate:
		return "trunc"
	default:
		return ""
	}
}

func isClauseEnder(lw string) bool {
	switch lw {
	case kwWhere, kwGroup, "order", kwHaving, kwLimit, "union", kwIntersect, kwExcept,
		"window", kwOffset, "on", "using", kwFetch:
		return true
	default:
		return false
	}
}

func isReservedWord(lw string) bool {
	switch lw {
	case "select", kwFrom, "where", "group", "order", "having", "limit", kwJoin, "on", "union", "as":
		return true
	default:
		return false
	}
}

func isFromInFunc(opener string) bool {
	switch opener {
	case "extract", "substring", "trim", "overlay":
		return true
	default:
		return false
	}
}

func extractFormat(field string) (string, bool) {
	switch field {
	case unitYear:
		return "%Y", true
	case unitMonth:
		return "%m", true
	case unitDay:
		return "%d", true
	case unitHour:
		return "%H", true
	case unitMinute:
		return "%M", true
	case unitSecond:
		return "%S", true
	default:
		return "", false
	}
}

func nextSig(toks []token, i int) int {
	for j := i + 1; j < len(toks); j++ {
		if toks[j].kind != tkSpace {
			return j
		}
	}

	return -1
}

func prevSig(toks []token, i int) int {
	for j := i - 1; j >= 0; j-- {
		if toks[j].kind != tkSpace {
			return j
		}
	}

	return -1
}

// collectCTEs records the names introduced by WITH name AS ( ... ), name AS ( ... ).
func collectCTEs(toks []token) map[string]bool {
	names := map[string]bool{}

	for i, t := range toks {
		if t.kind != tkWord && t.kind != tkQuoted {
			continue
		}

		a := nextSig(toks, i)
		if a < 0 || !toks[a].isWord("as") {
			continue
		}

		p := nextSig(toks, a)
		if p < 0 || !toks[p].isPunct("(") {
			continue
		}

		if pv := prevSig(toks, i); pv >= 0 && (toks[pv].isWord("with") || toks[pv].isWord("recursive") ||
			toks[pv].isPunct(",")) {
			names[t.ident()] = true
		}
	}

	return names
}

// translateSQL rewrites Presto/Trino SQL into SQLite SQL and resolves every
// table reference through resolve.
func translateSQL(sql string, resolve func(parts []string) resolvedRef) (string, []string, error) {
	toks, err := tokenize(sql)
	if err != nil {
		return "", nil, err
	}

	toks, err = expandGroupingSets(toks)
	if err != nil {
		return "", nil, err
	}

	toks = addNullsLast(toks)

	x := &translator{
		resolve: resolve, ctes: collectCTEs(toks),
		qualifiers: map[string]bool{}, structCols: map[string]bool{},
	}
	out := x.rewriteStructFields(x.rewriteUnnestRefs(x.translate(toks)))

	if x.err != nil {
		return "", nil, x.err
	}

	return strings.TrimSpace(joinTokens(out)), x.unresolved, nil
}

const fnJSONArray = "athena_array"

func isArrayBracket(t token, out []token, top *scope) bool {
	return t.isPunct("[") && lastWordIs(out, fnJSONArray) || t.isPunct("]") && top.opener == fnJSONArray
}

// arrayBracket turns ARRAY[ ... ] into json_array( ... ).
func arrayBracket(t token, out []token, stack []scope) ([]token, []scope) {
	if t.isPunct("[") {
		return append(out, token{kind: tkPunct, text: "("}), append(stack, scope{opener: fnJSONArray})
	}

	return append(out, token{kind: tkPunct, text: ")"}), stack[:len(stack)-1]
}

func openerOf(toks []token, i int) string {
	if p := prevSig(toks, i); p >= 0 && toks[p].kind == tkWord {
		return strings.ToLower(toks[p].text)
	}

	return ""
}

func lastWordIs(out []token, w string) bool {
	if p := prevSig(out, len(out)); p >= 0 {
		return out[p].isWord(w)
	}

	return false
}

func (x *translator) translate(toks []token) []token {
	out := make([]token, 0, len(toks)+tokenSlackPerCall)
	stack := []scope{{}}

	for i := 0; i < len(toks); i++ {
		t := toks[i]
		top := &stack[len(stack)-1]

		switch {
		case t.kind == tkWord:
			var next int

			out, next, stack = x.word(toks, i, out, stack)
			i = next
		case t.isPunct(","):
			out = append(out, t)

			if top.fromList {
				var next int

				out, next = x.tableRef(toks, i, out)
				i = next
			}
		case isArrayBracket(t, out, top):
			out, stack = arrayBracket(t, out, stack)
		case t.isPunct("["):
			var next int

			out, next = x.subscript(toks, i, out)
			i = next
		case t.isPunct("("):
			stack = append(stack, scope{opener: openerOf(toks, i)})
			out = append(out, t)
		case t.isPunct(")"):
			if len(stack) > 1 {
				stack = stack[:len(stack)-1]
			}

			out = append(out, t)
		case t.isPunct(";"):
			if n := nextSig(toks, i); n >= 0 {
				x.err = errMultiStatement
			}
		default:
			out = append(out, t)
		}
	}

	return out
}

// plainWord handles words that need no scope tracking: dotted members and ARRAY literals.
func plainWord(toks []token, i, nxt int, lw string) (token, bool) {
	switch {
	case i > 0 && toks[i-1].isPunct("."):
		return toks[i], true
	case lw == "array" && nxt >= 0 && toks[nxt].isPunct("["):
		return token{kind: tkWord, text: fnJSONArray}, true
	default:
		return token{}, false
	}
}

// preWord handles plain words and INTERVAL literals ahead of scope tracking.
func (x *translator) preWord(toks []token, i, nxt int, lw string, out []token) ([]token, int, bool) {
	if res, ok := plainWord(toks, i, nxt, lw); ok {
		return append(out, res), i, true
	}

	if lw == "interval" && nxt >= 0 && toks[nxt].kind == tkString {
		res, end := x.interval(toks, i, out)

		return res, end, true
	}

	return nil, 0, false
}

// word handles one word token and returns the new output, last consumed index and scope stack.
func (x *translator) word(toks []token, i int, out []token, stack []scope) ([]token, int, []scope) {
	t := toks[i]
	lw := strings.ToLower(t.text)
	nxt := nextSig(toks, i)
	callNext := nxt >= 0 && toks[nxt].isPunct("(")

	if res, end, ok := x.preWord(toks, i, nxt, lw, out); ok {
		return res, end, stack
	}

	if callNext {
		if res, end, newStack, ok := x.rewriteCall(toks, i, nxt, lw, stack); ok {
			return append(out, res...), end, newStack
		}
	}

	top := &stack[len(stack)-1]

	switch {
	case lw == kwFrom && isFromInFunc(top.opener):
	case lw == kwFrom || lw == kwJoin:
		top.fromList = true

		var end int

		out, end = x.tableRef(toks, i, append(out, t))

		return out, end, stack
	case isClauseEnder(lw):
		top.fromList = false
	case (lw == typeDate || lw == typeTimestamp) && nxt >= 0 && toks[nxt].kind == tkString:
		return out, i, stack
	}

	return append(out, t), i, stack
}

// rewriteCall handles function calls that need translation; ok=false leaves the word alone.
func (x *translator) rewriteCall(
	toks []token, i, open int, lw string, stack []scope,
) ([]token, int, []scope, bool) {
	if res, end, ok := x.callWord(toks, lw, open); ok {
		return res, end, stack, true
	}

	if lw == "approx_distinct" {
		res := make([]token, 0, open-i+argsTwo)
		res = append(res, token{kind: tkWord, text: fnCount})
		res = append(res, toks[i+1:open]...)
		res = append(res, toks[open], token{kind: tkWord, text: "DISTINCT "})

		return res, open, append(stack, scope{opener: "count"}), true
	}

	if r := renamedFunc(lw); r != "" {
		return []token{{kind: tkWord, text: r}}, i, stack, true
	}

	return nil, 0, stack, false
}

// callWord rewrites CAST and EXTRACT calls; ok=false leaves the call to normal handling.
func (x *translator) callWord(toks []token, lw string, open int) ([]token, int, bool) {
	var (
		res []token
		end int
	)

	switch lw {
	case "cast", "try_cast":
		res, end = x.castCall(toks, open)
	case "extract":
		res, end = x.extractCall(toks, open)
	default:
		res, end = x.lambdaCall(toks, lw, open)
	}

	return res, end, end > 0
}

// tableRef consumes the table reference following the token at i (a FROM, JOIN
// or comma) and appends its rewritten form to out.
func (x *translator) tableRef(toks []token, i int, out []token) ([]token, int) {
	j := nextSig(toks, i)
	if j < 0 {
		return out, i
	}

	out = append(out, toks[i+1:j]...)

	if toks[j].kind != tkWord && toks[j].kind != tkQuoted {
		return out, j - 1
	}

	if n := nextSig(toks, j); n >= 0 && toks[n].isPunct("(") {
		return x.callRef(toks, j, out)
	}

	if toks[j].kind == tkWord && isReservedWord(toks[j].ident()) {
		x.err = &syntaxError{fmt.Sprintf("line 1:%d: mismatched input '%s'", j+1, toks[j].text)}

		return out, j - 1
	}

	parts, end := qualifiedRef(toks, j)

	if len(parts) == 1 && x.ctes[parts[0]] {
		return append(out, toks[j:end+1]...), end
	}

	resolveParts := parts

	if tr, tend := x.travelClause(toks, end); tr != "" {
		resolveParts = append(append([]string{}, parts...), travelSentinel+tr)
		end = tend
	}

	ref := x.resolve(resolveParts)
	if !ref.found {
		x.unresolved = append(x.unresolved, orDefault(ref.display, strings.Join(parts, ".")))

		return append(out, toks[j:end+1]...), end
	}

	x.noteTable(toks, end, parts[len(parts)-1], ref)

	return append(out,
		token{kind: tkQuoted, text: quoteIdent(ref.schema)},
		token{kind: tkPunct, text: "."},
		token{kind: tkQuoted, text: quoteIdent(ref.name)},
	), end
}

// qualifiedRef reads a dotted table reference starting at j and returns its parts and last token index.
func qualifiedRef(toks []token, j int) ([]string, int) {
	parts := []string{toks[j].ident()}
	end := j

	for end+2 < len(toks) && toks[end+1].isPunct(".") &&
		(toks[end+2].kind == tkWord || toks[end+2].kind == tkQuoted) {
		parts = append(parts, toks[end+2].ident())
		end += 2
	}

	return parts, end
}

// noteTable records the qualifiers (table name and alias) and ROW columns of a resolved table.
func (x *translator) noteTable(toks []token, end int, name string, ref resolvedRef) {
	x.qualifiers[name] = true

	for _, c := range ref.structCols {
		x.structCols[c] = true
	}

	n := nextSig(toks, end)
	if n >= 0 && toks[n].isWord("as") {
		n = nextSig(toks, n)
	}

	if n >= 0 && (toks[n].kind == tkWord || toks[n].kind == tkQuoted) && !isAliasStopper(toks[n]) {
		x.qualifiers[toks[n].ident()] = true
	}
}

func isAliasStopper(t token) bool {
	if t.kind != tkWord {
		return false
	}

	lw := strings.ToLower(t.text)
	if isClauseEnder(lw) {
		return true
	}

	switch lw {
	case "join", "left", "right", "inner", "outer", "cross", "full", "natural", "tablesample", "with":
		return true
	default:
		return false
	}
}

// callRef handles a function-style table reference: UNNEST is rewritten, others pass through.
func (x *translator) callRef(toks []token, j int, out []token) ([]token, int) {
	if toks[j].isWord("unnest") {
		return x.unnestRef(toks, j, out)
	}

	return out, j - 1
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// castCall rewrites CAST(expr AS type); open is the index of '('. It returns
// the rewritten tokens and the closing paren index, or end=0 to leave the call alone.
func (x *translator) castCall(toks []token, open int) ([]token, int) {
	closeIdx := matchParen(toks, open)
	if closeIdx < 0 {
		return nil, 0
	}

	inner := toks[open+1 : closeIdx]
	asIdx := lastTopLevelWord(inner, "as")

	if asIdx < 0 {
		return nil, 0
	}

	expr := joinTokens(x.translate(inner[:asIdx]))
	typ := strings.ToLower(strings.Join(tokenTexts(significant(inner[asIdx+1:])), " "))
	typ = strings.ReplaceAll(strings.ReplaceAll(typ, " (", "("), " , ", ",")

	var sql string

	switch base := typeBase(typ); base {
	case typeDate:
		sql = "date(" + expr + ")"
	case typeTimestamp:
		sql = "datetime(" + expr + ")"
	default:
		sql = "CAST(" + expr + " AS " + sqliteCastType(base, typ) + ")"
	}

	return []token{{kind: tkWord, text: sql}}, closeIdx
}

func tokenTexts(toks []token) []string {
	out := make([]string, len(toks))
	for i, t := range toks {
		out[i] = t.text
	}

	return out
}

func typeBase(typ string) string {
	base, _, _ := strings.Cut(typ, "(")

	return strings.TrimSpace(base)
}

func sqliteCastType(base, full string) string {
	switch base {
	case typeVarchar, "char", columnTypeString:
		return "TEXT"
	case typeTinyint, typeSmallint, typeInteger, typeInt, typeBigint, typeBoolean:
		return sqlInteger
	case typeDouble, typeReal, typeFloat, "double precision":
		return "REAL"
	case typeDecimal, typeNumeric:
		return "NUMERIC"
	default:
		return full
	}
}

func lastTopLevelWord(toks []token, w string) int {
	depth, idx := 0, -1

	for i, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && t.isWord(w):
			idx = i
		}
	}

	return idx
}

func firstTopLevelWord(toks []token, w string) int {
	depth := 0

	for i, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && t.isWord(w):
			return i
		}
	}

	return -1
}

// extractCall rewrites EXTRACT(field FROM expr) onto strftime.
func (x *translator) extractCall(toks []token, open int) ([]token, int) {
	closeIdx := matchParen(toks, open)
	if closeIdx < 0 {
		return nil, 0
	}

	inner := toks[open+1 : closeIdx]
	fromIdx := firstTopLevelWord(inner, kwFrom)

	if fromIdx < 0 {
		return nil, 0
	}

	field := strings.ToLower(strings.TrimSpace(joinTokens(inner[:fromIdx])))

	format, ok := extractFormat(field)
	if !ok {
		return nil, 0
	}

	expr := joinTokens(x.translate(inner[fromIdx+1:]))

	return []token{{kind: tkWord, text: "CAST(strftime('" + format + "', " + expr + ") AS INTEGER)"}}, closeIdx
}
