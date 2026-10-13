package dynamodb

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	ddbexpr "github.com/blackbirdworks/gopherstack/services/dynamodb/expr"
)

var (
	partiqlAliasRefRe  = regexp.MustCompile(`#_a\d+`)
	partiqlReturningRe = regexp.MustCompile(`(?i)\s+RETURNING\s+(ALL|MODIFIED)\s+(OLD|NEW)\s+\*\s*$`)
	partiqlNumberRe    = regexp.MustCompile(`-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?`)
	partiqlKeywordRe   = regexp.MustCompile(`(?i)\b(true|false|null)\b`)
	partiqlSetFuncRe   = regexp.MustCompile(
		`(?i)^\s*([\w.#]+)\s*=\s*(set_add|set_delete)\s*\(\s*([\w.#]+)\s*,\s*(:\w+)\s*\)\s*$`,
	)
)

// partiqlSplitReturning strips a trailing RETURNING clause and maps it to the
// UpdateItem/DeleteItem ReturnValues enum ("" when absent).
func partiqlSplitReturning(stmt string) (string, string) {
	m := partiqlReturningRe.FindStringSubmatch(stmt)
	if m == nil {
		return stmt, ""
	}

	stripped := stmt[:len(stmt)-len(m[0])]

	switch strings.ToUpper(m[1] + " " + m[2]) {
	case "ALL OLD":
		return stripped, "ALL_OLD"
	case "ALL NEW":
		return stripped, "ALL_NEW"
	case "MODIFIED OLD":
		return stripped, "UPDATED_OLD"
	default:
		return stripped, "UPDATED_NEW"
	}
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isWordChar(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

func skipSpaces(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}

	return i
}

const (
	kwSet    = "SET"
	kwRemove = "REMOVE"
	kwUpdate = "UPDATE"
	kwFrom   = "FROM"
	kwInto   = "INTO"
	kwWhere  = "WHERE"
	kwIn     = "IN"

	// quotePairLen is the length of a closing quote plus the opening one that precedes it.
	quotePairLen = 2
)

// partiqlNormalizer double-quotes bare table/index identifiers after
// FROM/INTO/UPDATE, rewrites IN [..] lists as IN (..) and joins repeated
// SET/REMOVE clauses, leaving string literals and quoted identifiers untouched.
type partiqlNormalizer struct {
	src        string
	prevWord   string
	lastClause string
	out        strings.Builder
	i          int
	firstWord  bool
	isUpdate   bool
}

func partiqlNormalize(stmt string) string {
	n := &partiqlNormalizer{src: stmt, firstWord: true}

	for n.i < len(n.src) {
		switch c := n.src[n.i]; {
		case c == '\'':
			end := advancePastStringLiteral(n.src, n.i)
			n.out.WriteString(n.src[n.i:end])
			n.i = end
			n.prevWord = ""
		case c == '"':
			if !n.copyQuotedIdent() {
				return n.out.String()
			}
		case isIdentStart(c):
			n.word()
		default:
			n.out.WriteByte(c)
			n.i++

			if c != ' ' && c != '\t' && c != '\n' {
				n.prevWord = ""
			}
		}
	}

	return n.out.String()
}

// copyQuotedIdent copies a "quoted identifier"; false means it was unterminated.
func (n *partiqlNormalizer) copyQuotedIdent() bool {
	end := strings.IndexByte(n.src[n.i+1:], '"')
	if end < 0 {
		n.out.WriteString(n.src[n.i:])

		return false
	}

	n.out.WriteString(n.src[n.i : n.i+end+quotePairLen])
	n.i += end + quotePairLen
	n.prevWord = ""

	return true
}

func (n *partiqlNormalizer) word() {
	j := n.i
	for j < len(n.src) && isWordChar(n.src[j]) {
		j++
	}

	word := n.src[n.i:j]
	upper := strings.ToUpper(word)

	switch {
	case n.isUpdate && (upper == kwSet || upper == kwRemove):
		n.clauseKeyword(word, upper, j)

		return
	case (n.prevWord == kwFrom || n.prevWord == kwInto || n.prevWord == kwUpdate) && !isPartiQLKeyword(upper):
		j = quoteQualifiedName(&n.out, n.src, n.i)
	case upper == kwIn:
		j = n.inList(word, j)
	default:
		n.out.WriteString(word)
	}

	if upper == kwWhere {
		n.lastClause = ""
	}

	n.advance(upper, j)
}

func (n *partiqlNormalizer) advance(upper string, j int) {
	n.i = j

	switch {
	case upper == kwUpdate && n.firstWord:
		n.prevWord = kwUpdate
		n.isUpdate = true
	case upper == kwFrom || upper == kwInto:
		n.prevWord = upper
	default:
		n.prevWord = ""
	}

	n.firstWord = false
}

// clauseKeyword emits SET/REMOVE, or a comma when the same clause repeats.
func (n *partiqlNormalizer) clauseKeyword(word, upper string, j int) {
	if n.lastClause == upper && n.prevWord != upper+"KW" {
		n.out.WriteString(",")
	} else {
		n.out.WriteString(word)
	}

	n.lastClause = upper
	n.i = j
	n.prevWord = upper + "KW"
	n.firstWord = false
}

// inList rewrites `IN [a, b]` to `IN (a, b)`, returning the next source index.
func (n *partiqlNormalizer) inList(word string, j int) int {
	n.out.WriteString(word)

	k := skipSpaces(n.src, j)
	if k < len(n.src) && n.src[k] == '[' {
		if closeIdx := matchingBracket(n.src, k); closeIdx > 0 {
			n.out.WriteString(" (" + n.src[k+1:closeIdx] + ")")

			return closeIdx + 1
		}
	}

	return j
}

func isPartiQLKeyword(upper string) bool {
	return upper == "SELECT" || upper == "VALUE" || upper == kwSet || upper == kwWhere
}

// quoteQualifiedName writes `"name"` (and `."index"` when dotted) for the
// identifier starting at stmt[i], returning the index after it.
func quoteQualifiedName(out *strings.Builder, stmt string, i int) int {
	j := i
	for j < len(stmt) && isWordChar(stmt[j]) {
		j++
	}

	out.WriteString(`"` + stmt[i:j] + `"`)

	k := skipSpaces(stmt, j)
	if k >= len(stmt) || stmt[k] != '.' {
		return j
	}

	k = skipSpaces(stmt, k+1)

	switch {
	case k < len(stmt) && stmt[k] == '"':
		if end := strings.IndexByte(stmt[k+1:], '"'); end >= 0 {
			out.WriteString("." + stmt[k:k+end+quotePairLen])

			return k + end + quotePairLen
		}
	case k < len(stmt) && isIdentStart(stmt[k]):
		m := k
		for m < len(stmt) && isWordChar(stmt[m]) {
			m++
		}

		out.WriteString(`."` + stmt[k:m] + `"`)

		return m
	}

	return j
}

// matchingBracket returns the index of the ']' matching the '[' at s[open], or -1.
func matchingBracket(s string, open int) int {
	depth := 0

	for i := open; i < len(s); i++ {
		switch s[i] {
		case '\'':
			i = advancePastStringLiteral(s, i) - 1
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// partiqlSubstituteCollections replaces {..}, [..] and <<..>> literals that
// appear in value position with :_cN placeholders.
func partiqlSubstituteCollections(expr string, eav map[string]any) (string, map[string]any, error) {
	if !strings.ContainsAny(expr, "{[<") {
		return expr, eav, nil
	}

	var out strings.Builder

	n := len(eav)
	prev := byte(0)

	for i := 0; i < len(expr); {
		c := expr[i]

		if c == '\'' {
			end := advancePastStringLiteral(expr, i)
			out.WriteString(expr[i:end])
			i = end
			prev = '\''

			continue
		}

		end := collectionLiteralEnd(expr, i, prev)
		if end < 0 {
			out.WriteByte(c)
			i++

			if c != ' ' && c != '\t' {
				prev = c
			}

			continue
		}

		idx := 0
		val, err := parseValueToken(expr[i:end], nil, &idx, eav)
		if err != nil {
			return "", nil, err
		}

		if eav == nil {
			eav = make(map[string]any)
		}

		key := fmt.Sprintf(":_c%d", n)
		n++
		eav[key] = val
		out.WriteString(key)
		i = end
		prev = 'x'
	}

	return out.String(), eav, nil
}

// collectionLiteralEnd returns the end offset of a collection literal opening
// at expr[i], or -1 when expr[i] does not start one in value position.
func collectionLiteralEnd(expr string, i int, prev byte) int {
	if prev != 0 && strings.IndexByte(",(=<>", prev) < 0 {
		return -1
	}

	switch {
	case strings.HasPrefix(expr[i:], "<<"):
		if end := strings.Index(expr[i:], ">>"); end >= 0 {
			return i + end + quotePairLen
		}
	case expr[i] == '[':
		if end := matchingBracket(expr, i); end > 0 {
			return end + 1
		}
	case expr[i] == '{':
		return matchingBrace(expr, i)
	}

	return -1
}

// matchingBrace returns the offset just past the '}' matching the '{' at s[open], or -1.
func matchingBrace(s string, open int) int {
	depth := 0

	for j := open; j < len(s); j++ {
		switch s[j] {
		case '\'':
			j = advancePastStringLiteral(s, j) - 1
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return j + 1
			}
		}
	}

	return -1
}

// partiqlSubstituteScalars replaces numeric, TRUE/FALSE and NULL literals
// (outside placeholders and identifiers) with :_vN / :_kN placeholders.
func partiqlSubstituteScalars(expr string, eav map[string]any) (string, map[string]any) {
	if eav == nil {
		eav = make(map[string]any)
	}

	expr = replaceOutsideQuotes(expr, partiqlNumberRe, func(s string, start, end int) (string, bool) {
		if !standaloneToken(s, start, end, ":.[") {
			return "", false
		}

		text := s[start:end]
		if text[0] == '-' && !signIsUnary(s, start) {
			return "", false
		}

		key := fmt.Sprintf(":_v%d", len(eav))
		eav[key] = map[string]any{"N": text}

		return key, true
	})

	expr = replaceOutsideQuotes(expr, partiqlKeywordRe, func(s string, start, end int) (string, bool) {
		if start > 0 && strings.IndexByte(":.#", s[start-1]) >= 0 {
			return "", false
		}

		key := fmt.Sprintf(":_k%d", len(eav))
		eav[key] = keywordLiteral(s[start:end])

		return key, true
	})

	return expr, eav
}

// standaloneToken reports whether s[start:end] is not glued to an identifier
// or number: the preceding byte must not be a word char or in extraPrev, and
// the following byte must not be a word char or '.'.
func standaloneToken(s string, start, end int, extraPrev string) bool {
	if start > 0 && (isWordChar(s[start-1]) || strings.IndexByte(extraPrev, s[start-1]) >= 0) {
		return false
	}

	return end >= len(s) || (!isWordChar(s[end]) && s[end] != '.')
}

func keywordLiteral(word string) map[string]any {
	switch strings.ToLower(word) {
	case "true":
		return map[string]any{typeBOOL: true}
	case "false":
		return map[string]any{typeBOOL: false}
	default:
		return map[string]any{typeNULL: true}
	}
}

func signIsUnary(s string, pos int) bool {
	j := pos - 1
	for j >= 0 && (s[j] == ' ' || s[j] == '\t') {
		j--
	}

	if j < 0 {
		return true
	}

	return strings.IndexByte("=<>(,", s[j]) >= 0
}

// replaceOutsideQuotes applies re to expr, skipping matches inside double-quoted
// identifiers; repl returns the replacement and whether to use it.
func replaceOutsideQuotes(
	expr string, re *regexp.Regexp, repl func(s string, start, end int) (string, bool),
) string {
	var out strings.Builder

	last := 0

	for _, loc := range re.FindAllStringIndex(expr, -1) {
		if strings.Count(expr[:loc[0]], `"`)%2 == 1 {
			continue
		}

		text, ok := repl(expr, loc[0], loc[1])
		if !ok {
			continue
		}

		out.WriteString(expr[last:loc[0]])
		out.WriteString(text)
		last = loc[1]
	}

	out.WriteString(expr[last:])

	return out.String()
}

// parseValueToken parses one PartiQL value: ?, :placeholder, string, number,
// TRUE/FALSE/NULL, <<set>>, [list] or {map}.
func parseValueToken(
	token string, params []map[string]any, paramIdx *int, eav map[string]any,
) (map[string]any, error) {
	token = strings.TrimSpace(token)

	switch {
	case strings.HasPrefix(token, "<<") && strings.HasSuffix(token, ">>"):
		return parseSetLiteral(token[2:len(token)-2], params, paramIdx, eav)
	case strings.HasPrefix(token, "[") && strings.HasSuffix(token, "]"):
		return parseListLiteral(token[1:len(token)-1], params, paramIdx, eav)
	case strings.HasPrefix(token, "{") && strings.HasSuffix(token, "}"):
		return parseMapLiteral(token[1:len(token)-1], params, paramIdx, eav)
	case strings.HasPrefix(token, ":") && eav != nil:
		if v, ok := eav[token].(map[string]any); ok {
			return v, nil
		}
	}

	return partiqlParseScalar(token, params, paramIdx)
}

func parseListLiteral(
	body string, params []map[string]any, paramIdx *int, eav map[string]any,
) (map[string]any, error) {
	elems := []any{}

	for _, part := range splitTopLevelCommas(body) {
		if strings.TrimSpace(part) == "" {
			continue
		}

		v, err := parseValueToken(part, params, paramIdx, eav)
		if err != nil {
			return nil, err
		}

		elems = append(elems, v)
	}

	return map[string]any{"L": elems}, nil
}

func parseMapLiteral(
	body string, params []map[string]any, paramIdx *int, eav map[string]any,
) (map[string]any, error) {
	m := map[string]any{}

	for _, pair := range splitTopLevelCommas(body) {
		if strings.TrimSpace(pair) == "" {
			continue
		}

		rawKey, rawVal, found := strings.Cut(pair, ":")
		if !found {
			return nil, fmt.Errorf("%w: invalid key:value pair in map literal: %q", ErrInvalidStatement, pair)
		}

		v, err := parseValueToken(rawVal, params, paramIdx, eav)
		if err != nil {
			return nil, err
		}

		m[strings.Trim(strings.TrimSpace(rawKey), `'"`)] = v
	}

	return map[string]any{"M": m}, nil
}

func parseSetLiteral(
	body string, params []map[string]any, paramIdx *int, eav map[string]any,
) (map[string]any, error) {
	var strs, nums []any

	for _, part := range splitTopLevelCommas(body) {
		if strings.TrimSpace(part) == "" {
			continue
		}

		v, err := parseValueToken(part, params, paramIdx, eav)
		if err != nil {
			return nil, err
		}

		str, isStr := v["S"].(string)
		num, isNum := v["N"].(string)

		switch {
		case isStr:
			strs = append(strs, str)
		case isNum:
			if _, perr := strconv.ParseFloat(num, 64); perr != nil {
				return nil, fmt.Errorf("%w: invalid number %q in set literal", ErrInvalidStatement, num)
			}

			nums = append(nums, num)
		default:
			return nil, fmt.Errorf("%w: set literals may only contain strings or numbers", ErrInvalidStatement)
		}
	}

	switch {
	case len(strs) > 0 && len(nums) == 0:
		return map[string]any{"SS": strs}, nil
	case len(nums) > 0 && len(strs) == 0:
		return map[string]any{"NS": nums}, nil
	default:
		return nil, fmt.Errorf("%w: set literal must be a non-empty set of one type", ErrInvalidStatement)
	}
}

// partiqlRewriteSetFunctions turns `a = set_add(a, :v)` / `set_delete` SET
// assignments into ADD / DELETE update actions, returning the remaining SET
// assignments plus the extra actions.
func partiqlRewriteSetFunctions(setClause string) (string, string, string) {
	var keep, add, del []string

	for _, part := range splitTopLevelCommas(setClause) {
		m := partiqlSetFuncRe.FindStringSubmatch(part)
		if m == nil || m[1] != m[3] {
			keep = append(keep, strings.TrimSpace(part))

			continue
		}

		if strings.EqualFold(m[2], "set_add") {
			add = append(add, m[1]+" "+m[4])
		} else {
			del = append(del, m[1]+" "+m[4])
		}
	}

	return strings.Join(keep, ", "), strings.Join(add, ", "), strings.Join(del, ", ")
}

func isPartiQLOperatorWord(upper string) bool {
	switch upper {
	case "AND", "OR", "NOT", "BETWEEN", kwIn, "IS", typeNULL, "MISSING":
		return true
	default:
		return false
	}
}

type partiqlAliaser struct {
	eav map[string]any
	out strings.Builder
}

// alias returns the #_aN alias for name, creating it on first use.
func (a *partiqlAliaser) alias(name string) string {
	for k, v := range a.eav {
		if s, ok := v.(string); ok && strings.HasPrefix(k, "#_a") && s == name {
			return k
		}
	}

	key := fmt.Sprintf("#_a%d", len(a.eav))
	a.eav[key] = name

	return key
}

// partiqlAliasNames rewrites attribute names that DynamoDB expressions reject
// as reserved words (and double-quoted names) to #_aN aliases, recorded in eav
// as string entries. PartiQL itself has no such restriction.
func partiqlAliasNames(text string, eav map[string]any) (string, map[string]any) {
	if text == "" {
		return text, eav
	}

	if eav == nil {
		eav = make(map[string]any)
	}

	a := &partiqlAliaser{eav: eav}

	for i := 0; i < len(text); {
		i = a.step(text, i)
	}

	return a.out.String(), eav
}

// step consumes one token of text starting at i and returns the next offset.
func (a *partiqlAliaser) step(text string, i int) int {
	c := text[i]

	switch {
	case c == '\'':
		end := advancePastStringLiteral(text, i)
		a.out.WriteString(text[i:end])

		return end
	case c == '"':
		end := strings.IndexByte(text[i+1:], '"')
		if end < 0 {
			a.out.WriteString(text[i:])

			return len(text)
		}

		a.out.WriteString(a.alias(text[i+1 : i+1+end]))

		return i + end + quotePairLen
	case c == ':' || c == '#':
		j := wordEnd(text, i+1)
		a.out.WriteString(text[i:j])

		return j
	case isIdentStart(c):
		j := wordEnd(text, i)
		word := text[i:j]
		next := skipSpaces(text, j)
		isFunc := next < len(text) && text[next] == '('

		if !isPartiQLOperatorWord(strings.ToUpper(word)) && !isFunc && isReservedAttribute(word) {
			a.out.WriteString(a.alias(word))
		} else {
			a.out.WriteString(word)
		}

		return j
	default:
		a.out.WriteByte(c)

		return i + 1
	}
}

func wordEnd(s string, i int) int {
	for i < len(s) && isWordChar(s[i]) {
		i++
	}

	return i
}

// partiqlResolveName maps a #_aN alias back to the attribute name it stands for.
func partiqlResolveName(name string, eav map[string]any) string {
	if strings.HasPrefix(name, "#") {
		if s, ok := eav[name].(string); ok {
			return s
		}
	}

	return name
}

// partiqlNamesFor returns the alias entries of eav referenced by exprs.
func partiqlNamesFor(eav map[string]any, exprs ...string) map[string]string {
	var names map[string]string

	for _, ref := range partiqlAliasRefRe.FindAllString(strings.Join(exprs, " "), -1) {
		s, ok := eav[ref].(string)
		if !ok {
			continue
		}

		if names == nil {
			names = make(map[string]string)
		}

		names[ref] = s
	}

	return names
}

func isReservedAttribute(name string) bool { return ddbexpr.IsReservedWord(name) }
