package cloudwatchlogs

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// compileJSONFilterPattern compiles an AWS CloudWatch Logs JSON selector pattern
// ("{ $.field = "x" && $.n > 5 }") into a message matcher. A message matches when
// it parses as JSON and satisfies the selector expression. A pattern that fails
// to parse never matches (AWS treats malformed patterns as non-matching).
func compileJSONFilterPattern(pattern string) func(string) bool {
	body := strings.TrimSpace(pattern)
	body = strings.TrimPrefix(body, "{")
	body = strings.TrimSuffix(body, "}")

	toks := lexJSONPattern(body)
	p := &jsonPatParser{toks: toks, pos: 0}

	node, ok := p.parseOr()
	if !ok || p.pos != len(p.toks) {
		return func(string) bool { return false }
	}

	return func(message string) bool {
		var root any
		if err := json.Unmarshal([]byte(strings.TrimSpace(message)), &root); err != nil {
			return false
		}

		return node.evalJSON(root)
	}
}

// compileJSONFilterPatternExtract returns a field extractor for a JSON selector pattern.
// fieldRef is a selector path with its leading "$" already stripped by the caller (e.g.
// ".bytes" for a MetricValue of "$.bytes"); parseSelectorPath tolerates the remaining
// leading "." itself. Returns the leaf's string form, or false if message isn't valid
// JSON, the path resolves to nothing, or the leaf is a non-scalar (array/object).
func compileJSONFilterPatternExtract(_ string) func(message, fieldRef string) (string, bool) {
	return func(message, fieldRef string) (string, bool) {
		var root any
		if err := json.Unmarshal([]byte(strings.TrimSpace(message)), &root); err != nil {
			return "", false
		}

		leaves := resolveJSONSelector(root, parseSelectorPath(fieldRef))
		if len(leaves) == 0 {
			return "", false
		}

		switch leaves[0].(type) {
		case string, float64, bool:
			return jsonLeafString(leaves[0]), true
		default:
			return "", false
		}
	}
}

// jsonPatternSelectors returns the distinct "$.path" selector tokens referenced anywhere
// in a JSON selector pattern, in first-seen order.
func jsonPatternSelectors(pattern string) []string {
	body := strings.TrimSpace(pattern)
	body = strings.TrimPrefix(body, "{")
	body = strings.TrimSuffix(body, "}")

	seen := make(map[string]bool)
	var out []string
	for _, t := range lexJSONPattern(body) {
		if t.kind == jtSelector && !seen[t.text] {
			seen[t.text] = true
			out = append(out, t.text)
		}
	}

	return out
}

// jtKind enumerates JSON-pattern token kinds.
type jtKind int

const (
	jtSelector jtKind = iota
	jtNumber
	jtString
	jtIdent
	jtOp
	jtAnd
	jtOr
	jtLParen
	jtRParen
)

type jsonTok struct {
	text string
	kind jtKind
}

// lexJSONPattern lexes a JSON-selector pattern body into tokens.
func lexJSONPattern(s string) []jsonTok {
	var toks []jsonTok
	i := 0

	for i < len(s) {
		ch := s[i]
		switch ch {
		case ' ', '\t', '\n':
			i++
		case '(':
			toks = append(toks, jsonTok{text: "(", kind: jtLParen})
			i++
		case ')':
			toks = append(toks, jsonTok{text: ")", kind: jtRParen})
			i++
		default:
			var tok jsonTok
			tok, i = lexJSONToken(s, i)
			toks = append(toks, tok)
		}
	}

	return toks
}

// lexJSONToken lexes a single non-trivial token starting at i.
func lexJSONToken(s string, i int) (jsonTok, int) {
	switch s[i] {
	case '&':
		return jsonTok{text: "&&", kind: jtAnd}, skipDouble(s, i, '&')
	case '|':
		return jsonTok{text: "||", kind: jtOr}, skipDouble(s, i, '|')
	case '$':
		return lexJSONSelector(s, i)
	case '"', '\'':
		return lexJSONString(s, i)
	case '!', '=', '<', '>':
		return lexJSONOperator(s, i)
	default:
		return lexJSONWord(s, i)
	}
}

// skipDouble advances past a doubled operator char (e.g. "&&").
func skipDouble(s string, i int, ch byte) int {
	if i+1 < len(s) && s[i+1] == ch {
		return i + twoCharToken
	}

	return i + 1
}

// lexJSONSelector lexes a "$.path" selector token.
func lexJSONSelector(s string, start int) (jsonTok, int) {
	i := start + 1
	for i < len(s) && !isJSONBreak(s[i]) {
		i++
	}

	return jsonTok{text: s[start:i], kind: jtSelector}, i
}

// isJSONBreak reports whether ch terminates a selector token.
func isJSONBreak(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '(', ')', '=', '!', '<', '>', '&', '|':
		return true
	default:
		return false
	}
}

// lexJSONString lexes a quoted string (wildcards preserved).
func lexJSONString(s string, start int) (jsonTok, int) {
	quote := s[start]
	var sb strings.Builder

	for i := start + 1; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			sb.WriteByte(s[i+1])
			i++

			continue
		}

		if s[i] == quote {
			return jsonTok{text: sb.String(), kind: jtString}, i + 1
		}

		sb.WriteByte(s[i])
	}

	return jsonTok{text: sb.String(), kind: jtString}, len(s)
}

// lexJSONOperator lexes a comparison operator.
func lexJSONOperator(s string, start int) (jsonTok, int) {
	if start+1 < len(s) && s[start+1] == '=' {
		return jsonTok{text: s[start : start+twoCharToken], kind: jtOp}, start + twoCharToken
	}

	return jsonTok{text: string(s[start]), kind: jtOp}, start + 1
}

// lexJSONWord lexes a bare word: number, ident, or wildcard literal.
func lexJSONWord(s string, start int) (jsonTok, int) {
	i := start
	for i < len(s) && !isJSONBreak(s[i]) {
		i++
	}

	word := s[start:i]
	if _, err := strconv.ParseFloat(word, 64); err == nil {
		return jsonTok{text: word, kind: jtNumber}, i
	}

	return jsonTok{text: word, kind: jtIdent}, i
}

// jsonPatNode is a node in a JSON-selector pattern AST.
type jsonPatNode interface {
	evalJSON(root any) bool
}

type jsonBinNode struct {
	left  jsonPatNode
	right jsonPatNode
	isOr  bool
}

func (n *jsonBinNode) evalJSON(root any) bool {
	if n.isOr {
		return n.left.evalJSON(root) || n.right.evalJSON(root)
	}

	return n.left.evalJSON(root) && n.right.evalJSON(root)
}

// jsonCondNode is a single selector comparison or existence test.
type jsonCondNode struct {
	segs   []jsonSeg
	op     string
	val    jsonVal
	exists bool
}

func (n *jsonCondNode) evalJSON(root any) bool {
	leaves := resolveJSONSelector(root, n.segs)
	if n.exists {
		return len(leaves) > 0
	}

	for _, leaf := range leaves {
		if matchJSONLeaf(leaf, n.op, n.val) {
			return true
		}
	}

	return false
}

// jsonPatParser is a recursive-descent parser for JSON-selector patterns.
type jsonPatParser struct {
	toks []jsonTok
	pos  int
}

func (p *jsonPatParser) parseOr() (jsonPatNode, bool) {
	left, ok := p.parseAnd()
	if !ok {
		return nil, false
	}

	for p.pos < len(p.toks) && p.toks[p.pos].kind == jtOr {
		p.pos++

		right, rok := p.parseAnd()
		if !rok {
			return nil, false
		}

		left = &jsonBinNode{left: left, right: right, isOr: true}
	}

	return left, true
}

func (p *jsonPatParser) parseAnd() (jsonPatNode, bool) {
	left, ok := p.parseUnary()
	if !ok {
		return nil, false
	}

	for p.pos < len(p.toks) && p.toks[p.pos].kind == jtAnd {
		p.pos++

		right, rok := p.parseUnary()
		if !rok {
			return nil, false
		}

		left = &jsonBinNode{left: left, right: right, isOr: false}
	}

	return left, true
}

func (p *jsonPatParser) parseUnary() (jsonPatNode, bool) {
	if p.pos >= len(p.toks) {
		return nil, false
	}

	t := p.toks[p.pos]
	if t.kind == jtLParen {
		return p.parseParen()
	}

	if t.kind != jtSelector {
		return nil, false
	}

	return p.parseCondition(t)
}

func (p *jsonPatParser) parseParen() (jsonPatNode, bool) {
	p.pos++ // consume (

	inner, ok := p.parseOr()
	if !ok {
		return nil, false
	}

	if p.pos >= len(p.toks) || p.toks[p.pos].kind != jtRParen {
		return nil, false
	}

	p.pos++ // consume )

	return inner, true
}

// parseCondition parses "selector [op value]" or a bare existence selector.
func (p *jsonPatParser) parseCondition(sel jsonTok) (jsonPatNode, bool) {
	p.pos++ // consume selector
	segs := parseSelectorPath(sel.text)

	if p.pos >= len(p.toks) || p.toks[p.pos].kind != jtOp {
		return &jsonCondNode{segs: segs, op: "", val: jsonVal{}, exists: true}, true
	}

	op := p.toks[p.pos].text
	p.pos++

	if p.pos >= len(p.toks) {
		return nil, false
	}

	val := jsonValFromToken(p.toks[p.pos])
	p.pos++

	return &jsonCondNode{segs: segs, op: op, val: val, exists: false}, true
}

// jsonVal is a parsed comparison operand from a JSON pattern.
type jsonVal struct {
	str    string
	num    float64
	isNum  bool
	isBool bool
	boolV  bool
	isNull bool
}

// jsonValFromToken builds a comparison operand from a token.
func jsonValFromToken(t jsonTok) jsonVal {
	if t.kind == jtNumber {
		f, _ := strconv.ParseFloat(t.text, 64)

		return jsonVal{str: t.text, num: f, isNum: true, isBool: false, boolV: false, isNull: false}
	}

	switch strings.ToLower(t.text) {
	case literalTrue:
		return jsonVal{str: literalTrue, num: 0, isNum: false, isBool: true, boolV: true, isNull: false}
	case literalFalse:
		return jsonVal{str: literalFalse, num: 0, isNum: false, isBool: true, boolV: false, isNull: false}
	case literalNull:
		return jsonVal{str: literalNull, num: 0, isNum: false, isBool: false, boolV: false, isNull: true}
	default:
		return jsonVal{str: t.text, num: 0, isNum: false, isBool: false, boolV: false, isNull: false}
	}
}

// matchJSONLeaf compares a resolved JSON leaf against the operand using op.
func matchJSONLeaf(leaf any, op string, val jsonVal) bool {
	switch op {
	case "=":
		return jsonEquals(leaf, val)
	case "!=":
		return !jsonEquals(leaf, val)
	default:
		return jsonNumericCompare(leaf, op, val)
	}
}

// jsonEquals implements "=" including numeric, boolean, null, and wildcard string.
func jsonEquals(leaf any, val jsonVal) bool {
	switch {
	case val.isNull:
		return leaf == nil
	case val.isBool:
		b, ok := leaf.(bool)

		return ok && b == val.boolV
	case val.isNum:
		f, ok := leaf.(float64)

		return ok && f == val.num
	default:
		return jsonStringEquals(leaf, val.str)
	}
}

// jsonStringEquals compares a leaf's string form against a possibly-wildcard value.
func jsonStringEquals(leaf any, pattern string) bool {
	s := jsonLeafString(leaf)
	if strings.Contains(pattern, "*") {
		return wildcardMatch(pattern, s)
	}

	return s == pattern
}

// jsonNumericCompare implements <, <=, >, >= on numeric leaves.
func jsonNumericCompare(leaf any, op string, val jsonVal) bool {
	f, ok := leaf.(float64)
	if !ok || !val.isNum {
		return false
	}

	switch op {
	case "<":
		return f < val.num
	case "<=":
		return f <= val.num
	case ">":
		return f > val.num
	case ">=":
		return f >= val.num
	default:
		return false
	}
}

// jsonLeafString renders a JSON leaf as a string for equality/wildcard tests.
func jsonLeafString(leaf any) string {
	switch t := leaf.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return ""
	}
}

// wildcardMatch reports whether s matches a pattern containing "*" wildcards.
func wildcardMatch(pattern, s string) bool {
	parts := strings.Split(pattern, "*")
	escaped := make([]string, len(parts))
	for i, p := range parts {
		escaped[i] = regexp.QuoteMeta(p)
	}

	re, err := regexp.Compile("^" + strings.Join(escaped, ".*") + "$")
	if err != nil {
		return false
	}

	return re.MatchString(s)
}

// jsonSeg is one segment of a JSON selector path.
type jsonSeg struct {
	key      string
	index    int
	isIndex  bool
	wildcard bool
}

// parseSelectorPath parses "$.a.b[0].c" / "$.a[*].b" into path segments.
func parseSelectorPath(sel string) []jsonSeg {
	sel = strings.TrimPrefix(sel, "$")
	sel = strings.TrimPrefix(sel, ".")
	replaced := strings.NewReplacer("[", ".[", "]", "").Replace(sel)

	var segs []jsonSeg
	for part := range strings.SplitSeq(replaced, ".") {
		if part == "" {
			continue
		}

		segs = append(segs, parseSelectorSeg(part))
	}

	return segs
}

// parseSelectorSeg parses one selector segment into a jsonSeg.
func parseSelectorSeg(part string) jsonSeg {
	if inner, ok := strings.CutPrefix(part, "["); ok {
		if inner == "*" {
			return jsonSeg{key: "", index: 0, isIndex: false, wildcard: true}
		}

		idx, _ := strconv.Atoi(inner)

		return jsonSeg{key: "", index: idx, isIndex: true, wildcard: false}
	}

	return jsonSeg{key: part, index: 0, isIndex: false, wildcard: false}
}

// resolveJSONSelector resolves a selector path to all matching leaf values.
func resolveJSONSelector(root any, segs []jsonSeg) []any {
	cur := []any{root}

	for _, seg := range segs {
		next := make([]any, 0, len(cur))
		for _, node := range cur {
			next = append(next, stepJSONSeg(node, seg)...)
		}

		cur = next
		if len(cur) == 0 {
			return nil
		}
	}

	return cur
}

// stepJSONSeg descends one segment, returning all matching child values.
func stepJSONSeg(node any, seg jsonSeg) []any {
	switch {
	case seg.wildcard:
		return jsonChildren(node)
	case seg.isIndex:
		if arr, ok := node.([]any); ok && seg.index >= 0 && seg.index < len(arr) {
			return []any{arr[seg.index]}
		}

		return nil
	default:
		if obj, ok := node.(map[string]any); ok {
			if v, present := obj[seg.key]; present {
				return []any{v}
			}
		}

		return nil
	}
}

// jsonChildren returns all element/values of an array or object node.
func jsonChildren(node any) []any {
	switch t := node.(type) {
	case []any:
		return t
	case map[string]any:
		out := make([]any, 0, len(t))
		for _, v := range t {
			out = append(out, v)
		}

		return out
	default:
		return nil
	}
}
