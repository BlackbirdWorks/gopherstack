package glue

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Grammar: https://docs.aws.amazon.com/glue/latest/dg/dqdl.html and dqdl-rule-types-<Rule>.html.
// Validation only; no data is evaluated.

const (
	dqTokEOF = iota
	dqTokIdent
	dqTokString
	dqTokNumber
	dqTokConst
	dqTokRegex
	dqTokSym
)

const (
	maxDQDLLabels = 10
	dqWordBetween = "between"
	dqWordMatches = "matches"
	dqWordNull    = "null"
	dqWordNot     = "not"
	dqSymLen2     = 2
)

var errDQDL = errors.New("invalid DQDL")

type dqTok struct {
	text string
	pos  int
	kind int
}

type dqRuleSpec struct {
	minParams int
	maxParams int
	expr      dqExprPolicy
}

type dqExprPolicy int

const (
	dqExprNone dqExprPolicy = iota
	dqExprRequired
	dqExprOptional
)

const dqUnbounded = 1 << 20

func dqRuleSpecs() map[string]dqRuleSpec {
	return map[string]dqRuleSpec{
		"ColumnCount":             {0, 0, dqExprRequired},
		"RowCount":                {0, 0, dqExprRequired},
		"ColumnExists":            {1, 1, dqExprNone},
		"IsComplete":              {1, 1, dqExprNone},
		"IsUnique":                {1, dqUnbounded, dqExprNone},
		"IsPrimaryKey":            {1, dqUnbounded, dqExprNone},
		"Completeness":            {1, 1, dqExprRequired},
		"Uniqueness":              {1, dqUnbounded, dqExprRequired},
		"ColumnValues":            {1, 1, dqExprRequired},
		"ColumnLength":            {1, 1, dqExprRequired},
		"Mean":                    {1, 1, dqExprRequired},
		"Sum":                     {1, 1, dqExprRequired},
		"StandardDeviation":       {1, 1, dqExprRequired},
		"Entropy":                 {1, 1, dqExprRequired},
		"DistinctValuesCount":     {1, 1, dqExprRequired},
		"UniqueValueRatio":        {1, 1, dqExprRequired},
		"DataFreshness":           {1, 1, dqExprRequired},
		"CustomSql":               {1, 1, dqExprOptional},
		"ReferentialIntegrity":    {2, 2, dqExprRequired},
		"ColumnCorrelation":       {2, 2, dqExprRequired},
		"ColumnDataType":          {1, 1, dqExprRequired},
		"ColumnNamesMatchPattern": {1, 1, dqExprNone},
		"AggregateMatch":          {2, 2, dqExprRequired},
		"DatasetMatch":            {2, 3, dqExprRequired},
		"RowCountMatch":           {1, 1, dqExprRequired},
		"SchemaMatch":             {1, 1, dqExprRequired},
		"DetectAnomalies":         {1, dqUnbounded, dqExprNone},
		"FileFreshness":           {0, 1, dqExprRequired},
		"FileSize":                {0, 1, dqExprRequired},
		"FileUniqueness":          {0, 1, dqExprRequired},
		"FileMatch":               {0, 2, dqExprOptional},
	}
}

func dqAnalyzerSpecs() map[string]dqRuleSpec {
	return map[string]dqRuleSpec{
		"RowCount":            {0, 0, dqExprNone},
		"ColumnCount":         {0, 0, dqExprNone},
		"Completeness":        {1, 1, dqExprNone},
		"Uniqueness":          {1, dqUnbounded, dqExprNone},
		"Mean":                {1, 1, dqExprNone},
		"Sum":                 {1, 1, dqExprNone},
		"StandardDeviation":   {1, 1, dqExprNone},
		"Entropy":             {1, 1, dqExprNone},
		"DistinctValuesCount": {1, 1, dqExprNone},
		"UniqueValueRatio":    {1, 1, dqExprNone},
		"ColumnLength":        {1, 1, dqExprNone},
		"Distribution":        {1, 1, dqExprNone},
	}
}

func dqIsKeyword(s string) bool {
	switch strings.ToLower(s) {
	case "and", "or", "where", "with", "labels", dqWordBetween, dqWordNot, "in", dqWordMatches:
		return true
	}

	return false
}

func dqIsAggFunc(s string) bool {
	switch s {
	case "avg", "median", "max", "min", "sum", "std", "abs":
		return true
	}

	return false
}

func dqIsValueKeyword(s string) bool {
	switch s {
	case "NULL", dqWordNull, "EMPTY", "empty", "WHITESPACES_ONLY", "whitespaces_only", "true", "false":
		return true
	}

	return false
}

// ValidateDQDL reports whether text is a syntactically valid DQDL document.
func ValidateDQDL(text string) error {
	toks, err := dqLex(text)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrValidation, err)
	}

	p := &dqParser{
		toks: toks, consts: map[string]struct{}{}, rules: dqRuleSpecs(), analyze: dqAnalyzerSpecs(),
	}
	if err = p.parseDocument(); err != nil {
		return fmt.Errorf("%w: %w", ErrValidation, err)
	}

	return nil
}

func dqLex(src string) ([]dqTok, error) {
	var toks []dqTok

	for i := 0; i < len(src); {
		tok, n, err := dqLexOne(src, i, toks)
		if err != nil {
			return nil, err
		}

		if tok.kind != dqTokEOF {
			toks = append(toks, tok)
		}

		i += n
	}

	return append(toks, dqTok{kind: dqTokEOF, pos: len(src)}), nil
}

// dqLexOne lexes the token at src[i:]; whitespace and comments yield an EOF-kind placeholder.
func dqLexOne(src string, i int, prev []dqTok) (dqTok, int, error) {
	if n := dqSkip(src, i); n > 0 {
		return dqTok{}, n, nil
	}

	c := src[i]

	switch {
	case c == '"' || c == '\'':
		end, err := dqScanQuoted(src, i, c)
		if err != nil {
			return dqTok{}, 0, err
		}

		return dqTok{kind: dqTokString, text: src[i+1 : end], pos: i}, end + 1 - i, nil
	case c == '/' && len(prev) > 0 && prev[len(prev)-1].text == dqWordMatches:
		return dqLexRegex(src, i)
	case c == '$':
		return dqLexConst(src, i)
	case isDQDigit(c) || isDQLetter(c) || c == '_':
		return dqLexWord(src, i)
	}

	return dqLexSymbol(src, i)
}

func dqLexWord(src string, i int) (dqTok, int, error) {
	if isDQDigit(src[i]) {
		j := dqSpan(src, i, func(b byte) bool { return isDQDigit(b) || b == '.' })

		return dqTok{kind: dqTokNumber, text: src[i:j], pos: i}, j - i, nil
	}

	j := dqSpan(src, i, func(b byte) bool { return isDQLetter(b) || isDQDigit(b) || b == '_' || b == '.' })

	return dqTok{kind: dqTokIdent, text: src[i:j], pos: i}, j - i, nil
}

// dqSkip returns the length of the whitespace or comment at src[i:], or 0.
func dqSkip(src string, i int) int {
	switch src[i] {
	case ' ', '\t', '\n', '\r':
		return 1
	case '#':
		return dqSpan(src, i, func(b byte) bool { return b != '\n' }) - i
	}

	return 0
}

func dqLexConst(src string, i int) (dqTok, int, error) {
	j := dqSpan(src, i+1, func(b byte) bool { return isDQLetter(b) || isDQDigit(b) || b == '_' })
	if j == i+1 {
		return dqTok{}, 0, fmt.Errorf("%w: offset %d: constant reference needs a name", errDQDL, i)
	}

	return dqTok{kind: dqTokConst, text: src[i+1 : j], pos: i}, j - i, nil
}

func dqSpan(src string, from int, in func(byte) bool) int {
	j := from
	for j < len(src) && in(src[j]) {
		j++
	}

	return j
}

func dqLexRegex(src string, i int) (dqTok, int, error) {
	end, err := dqScanQuoted(src, i, '/')
	if err != nil {
		return dqTok{}, 0, err
	}

	j := dqSpan(src, end+1, isDQLetter)

	return dqTok{kind: dqTokRegex, text: src[i:j], pos: i}, j - i, nil
}

func dqLexSymbol(src string, i int) (dqTok, int, error) {
	if i+1 < len(src) {
		two := src[i : i+dqSymLen2]
		if two == "!=" || two == ">=" || two == "<=" || two == "<>" {
			return dqTok{kind: dqTokSym, text: two, pos: i}, dqSymLen2, nil
		}
	}

	if strings.ContainsRune("=<>[](),+-*/", rune(src[i])) {
		return dqTok{kind: dqTokSym, text: src[i : i+1], pos: i}, 1, nil
	}

	return dqTok{}, 0, fmt.Errorf("%w: offset %d: unexpected character %q", errDQDL, i, src[i])
}

func dqScanQuoted(src string, start int, quote byte) (int, error) {
	for j := start + 1; j < len(src); j++ {
		if src[j] == '\\' {
			j++

			continue
		}

		if src[j] == quote {
			return j, nil
		}
	}

	return 0, fmt.Errorf("%w: offset %d: unterminated literal", errDQDL, start)
}

func isDQLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

func isDQDigit(c byte) bool { return c >= '0' && c <= '9' }

type dqParser struct {
	rules    map[string]dqRuleSpec
	analyze  map[string]dqRuleSpec
	consts   map[string]struct{}
	toks     []dqTok
	i        int
	aggDepth int
}

func (p *dqParser) peek() dqTok { return p.toks[p.i] }

func (p *dqParser) next() dqTok {
	t := p.toks[p.i]
	if t.kind != dqTokEOF {
		p.i++
	}

	return t
}

func (p *dqParser) isSym(s string) bool {
	t := p.peek()

	return t.kind == dqTokSym && t.text == s
}

func (p *dqParser) isWord(s string) bool {
	t := p.peek()

	return t.kind == dqTokIdent && strings.EqualFold(t.text, s) && dqIsKeyword(t.text)
}

func (p *dqParser) errf(format string, args ...any) error {
	return fmt.Errorf("%w: offset %d: %s", errDQDL, p.peek().pos, fmt.Sprintf(format, args...))
}

func (p *dqParser) expectSym(s string) error {
	if !p.isSym(s) {
		return p.errf("expected %q", s)
	}

	p.i++

	return nil
}

func (p *dqParser) parseDocument() error {
	seen := map[string]bool{}

	for p.peek().kind != dqTokEOF {
		name := p.next()
		if name.kind != dqTokIdent {
			return fmt.Errorf("%w: offset %d: expected a section name or constant", errDQDL, name.pos)
		}

		if err := p.expectSym("="); err != nil {
			return err
		}

		if err := p.parseSection(name.text, seen); err != nil {
			return err
		}
	}

	if !seen["Rules"] && !seen["Analyzers"] {
		return fmt.Errorf("%w: a DQDL document must contain a Rules = [ ... ] list", errDQDL)
	}

	return nil
}

func (p *dqParser) parseSection(name string, seen map[string]bool) error {
	switch name {
	case "Rules", "Analyzers", "DefaultLabels":
		if seen[name] {
			return fmt.Errorf("%w: duplicate %s section", errDQDL, name)
		}

		seen[name] = true
	}

	switch name {
	case "Rules":
		return p.parseList(func() error { return p.parseRule() })
	case "Analyzers":
		return p.parseList(p.parseAnalyzer)
	case "DefaultLabels":
		return p.parseLabelList()
	default:
		t := p.next()
		if t.kind != dqTokString {
			return fmt.Errorf("%w: offset %d: constant %q must be assigned a quoted string", errDQDL, t.pos, name)
		}

		p.consts[name] = struct{}{}

		return nil
	}
}

func (p *dqParser) parseList(item func() error) error {
	if err := p.expectSym("["); err != nil {
		return err
	}

	if p.isSym("]") {
		p.i++

		return nil
	}

	for {
		if err := item(); err != nil {
			return err
		}

		if p.isSym(",") {
			p.i++

			if p.isSym("]") {
				break
			}

			continue
		}

		break
	}

	return p.expectSym("]")
}

func (p *dqParser) parseLabelList() error {
	if err := p.expectSym("["); err != nil {
		return err
	}

	n := 0

	for {
		k := p.next()
		if k.kind != dqTokString {
			return fmt.Errorf("%w: offset %d: label key must be a quoted string", errDQDL, k.pos)
		}

		if err := p.expectSym("="); err != nil {
			return err
		}

		if v := p.next(); v.kind != dqTokString {
			return fmt.Errorf("%w: offset %d: label value must be a quoted string", errDQDL, v.pos)
		}

		n++

		if !p.isSym(",") {
			break
		}

		p.i++
	}

	if n > maxDQDLLabels {
		return fmt.Errorf("%w: a rule can have at most %d labels, got %d", errDQDL, maxDQDLLabels, n)
	}

	return p.expectSym("]")
}

func (p *dqParser) parseRule() error {
	units, err := p.parseChain(false)
	if err != nil {
		return err
	}

	if p.peek().kind == dqTokIdent && p.peek().text == "labels" {
		if units > 1 {
			return p.errf("labels on a composite rule must follow the whole parenthesized rule")
		}

		p.i++

		if err = p.expectSym("="); err != nil {
			return err
		}

		return p.parseLabelList()
	}

	return nil
}

func (p *dqParser) parseChain(inGroup bool) (int, error) {
	units := 0
	bare := false

	for {
		paren, err := p.parseUnit()
		if err != nil {
			return 0, err
		}

		units++
		bare = bare || !paren

		if !p.isWord("and") && !p.isWord("or") {
			break
		}

		p.i++
	}

	if units > 1 && bare && !inGroup {
		return 0, p.errf("each rule in a composite rule must be surrounded by parentheses")
	}

	return units, nil
}

func (p *dqParser) parseUnit() (bool, error) {
	if !p.isSym("(") {
		return false, p.parseSimpleRule(p.rules, true)
	}

	p.i++

	if _, err := p.parseChain(true); err != nil {
		return false, err
	}

	return true, p.expectSym(")")
}

func (p *dqParser) parseAnalyzer() error {
	return p.parseSimpleRule(p.analyze, false)
}

func (p *dqParser) parseSimpleRule(specs map[string]dqRuleSpec, isRule bool) error {
	t := p.next()

	spec, ok := specs[t.text]
	if t.kind != dqTokIdent || !ok {
		return fmt.Errorf("%w: offset %d: unknown rule type %q", errDQDL, t.pos, t.text)
	}

	params, err := p.parseParams()
	if err != nil {
		return err
	}

	if params < spec.minParams || params > spec.maxParams {
		return fmt.Errorf(
			"%w: offset %d: %s takes %s parameter(s), got %d",
			errDQDL,
			t.pos,
			t.text,
			dqArity(spec),
			params,
		)
	}

	hasExpr := p.atExpression()
	if hasExpr {
		if spec.expr == dqExprNone {
			return fmt.Errorf("%w: offset %d: %s does not take an expression", errDQDL, t.pos, t.text)
		}

		if err = p.parseExpression(); err != nil {
			return err
		}
	} else if spec.expr == dqExprRequired {
		return fmt.Errorf("%w: offset %d: %s requires an expression", errDQDL, t.pos, t.text)
	}

	return p.parseModifiers(t, isRule, hasExpr)
}

func dqArity(s dqRuleSpec) string {
	switch {
	case s.maxParams == dqUnbounded:
		return strconv.Itoa(s.minParams) + " or more"
	case s.minParams == s.maxParams:
		return strconv.Itoa(s.minParams)
	default:
		return fmt.Sprintf("%d to %d", s.minParams, s.maxParams)
	}
}

func (p *dqParser) parseParams() (int, error) {
	n := 0

	for {
		t := p.peek()

		switch {
		case t.kind == dqTokString:
		case t.kind == dqTokConst:
			if _, ok := p.consts[t.text]; !ok {
				return 0, fmt.Errorf("%w: offset %d: undefined constant $%s", errDQDL, t.pos, t.text)
			}
		case t.kind == dqTokIdent && !dqIsKeyword(t.text):
		default:
			return n, nil
		}

		p.i++
		n++
	}
}

func (p *dqParser) atExpression() bool {
	t := p.peek()
	if t.kind == dqTokSym {
		switch t.text {
		case "=", "!=", "<>", ">", ">=", "<", "<=":
			return true
		}

		return false
	}

	if t.kind != dqTokIdent {
		return false
	}

	switch strings.ToLower(t.text) {
	case dqWordBetween, "in", dqWordMatches, dqWordNot:
		return true
	}

	return false
}

func (p *dqParser) parseExpression() error {
	t := p.next()

	if t.kind == dqTokSym {
		return p.parseOperand()
	}

	word := strings.ToLower(t.text)
	if word == dqWordNot {
		t = p.next()
		word = strings.ToLower(t.text)

		if word != dqWordBetween && word != "in" && word != dqWordMatches {
			return fmt.Errorf("%w: offset %d: expected between, in or matches after not", errDQDL, t.pos)
		}
	}

	switch word {
	case dqWordBetween:
		if err := p.parseOperand(); err != nil {
			return err
		}

		if !p.isWord("and") {
			return p.errf("expected \"and\" in between expression")
		}

		p.i++

		return p.parseOperand()
	case "in":
		return p.parseValueList()
	default:
		r := p.next()
		if r.kind != dqTokString && r.kind != dqTokRegex && r.kind != dqTokConst {
			return fmt.Errorf("%w: offset %d: matches needs a quoted pattern or /regex/", errDQDL, r.pos)
		}

		return nil
	}
}

func (p *dqParser) parseValueList() error {
	if err := p.expectSym("["); err != nil {
		return err
	}

	if p.isSym("]") {
		return p.errf("in [...] needs at least one value")
	}

	for {
		if err := p.parseOperand(); err != nil {
			return err
		}

		if !p.isSym(",") {
			break
		}

		p.i++
	}

	return p.expectSym("]")
}

func (p *dqParser) parseOperand() error {
	if err := p.parsePrimary(); err != nil {
		return err
	}

	for p.isSym("+") || p.isSym("-") || p.isSym("*") || p.isSym("/") {
		p.i++

		if err := p.parsePrimary(); err != nil {
			return err
		}
	}

	return nil
}

func (p *dqParser) parsePrimary() error {
	t := p.next()

	switch t.kind {
	case dqTokNumber:
		if _, err := strconv.ParseFloat(t.text, 64); err != nil {
			return fmt.Errorf("%w: offset %d: invalid number %q", errDQDL, t.pos, t.text)
		}

		if n := p.peek(); n.kind == dqTokIdent && !dqIsKeyword(n.text) {
			p.i++
		}

		return nil
	case dqTokString, dqTokConst:
		return nil
	case dqTokSym:
		return p.parseSymPrimary(t)
	case dqTokIdent:
		return p.parseIdentPrimary(t)
	}

	return fmt.Errorf("%w: offset %d: expected a value", errDQDL, t.pos)
}

func (p *dqParser) parseSymPrimary(t dqTok) error {
	switch t.text {
	case "-":
		return p.parsePrimary()
	case "(":
		if err := p.parseOperand(); err != nil {
			return err
		}

		return p.expectSym(")")
	}

	return fmt.Errorf("%w: offset %d: expected a value, got %q", errDQDL, t.pos, t.text)
}

func (p *dqParser) parseIdentPrimary(t dqTok) error {
	if dqIsValueKeyword(t.text) {
		return nil
	}

	switch t.text {
	case "now":
		if err := p.expectSym("("); err != nil {
			return err
		}

		return p.expectSym(")")
	case "last":
		return p.parseLast()
	case "index":
		return p.parseIndex()
	}

	if dqIsAggFunc(t.text) {
		if err := p.expectSym("("); err != nil {
			return err
		}

		p.aggDepth++
		err := p.parseOperand()
		p.aggDepth--

		if err != nil {
			return err
		}

		return p.expectSym(")")
	}

	return fmt.Errorf("%w: offset %d: unexpected %q in expression", errDQDL, t.pos, t.text)
}

func (p *dqParser) parseLast() error {
	if err := p.expectSym("("); err != nil {
		return err
	}

	if n := p.peek(); n.kind == dqTokNumber {
		k, err := strconv.Atoi(n.text)
		if err != nil || k < 1 {
			return fmt.Errorf("%w: offset %d: last(k) needs a natural number k >= 1", errDQDL, n.pos)
		}

		if k > 1 && p.aggDepth == 0 {
			return fmt.Errorf(
				"%w: offset %d: last(%d) must be reduced by an aggregation function such as avg()",
				errDQDL,
				n.pos,
				k,
			)
		}

		p.i++
	}

	return p.expectSym(")")
}

func (p *dqParser) parseIndex() error {
	if err := p.expectSym("("); err != nil {
		return err
	}

	p.aggDepth++
	err := p.parseOperand()
	p.aggDepth--

	if err != nil {
		return err
	}

	if err = p.expectSym(","); err != nil {
		return err
	}

	if n := p.next(); n.kind != dqTokNumber {
		return fmt.Errorf("%w: offset %d: index(last(k), i) needs a numeric position", errDQDL, n.pos)
	}

	return p.expectSym(")")
}

func (p *dqParser) parseModifiers(rule dqTok, isRule, hasExpr bool) error {
	if p.isWord("where") {
		p.i++

		w := p.next()
		if w.kind != dqTokString || strings.TrimSpace(w.text) == "" || !dqBalanced(w.text) {
			return fmt.Errorf("%w: offset %d: rule %s has an invalid where clause", errDQDL, w.pos, rule.text)
		}
	}

	for p.isWord("with") {
		p.i++

		if err := p.parseWith(rule, isRule, hasExpr); err != nil {
			return err
		}
	}

	return nil
}

func (p *dqParser) parseWith(rule dqTok, isRule, hasExpr bool) error {
	opt := p.next()
	if opt.kind != dqTokIdent {
		return fmt.Errorf("%w: offset %d: expected an option name after with", errDQDL, opt.pos)
	}

	if opt.text == "threshold" {
		if !isRule || !hasExpr {
			return fmt.Errorf("%w: offset %d: with threshold is not valid on %s", errDQDL, opt.pos, rule.text)
		}

		if !p.atExpression() {
			return p.errf("with threshold needs a comparison expression")
		}

		return p.parseExpression()
	}

	for {
		if err := p.expectSym("="); err != nil {
			return err
		}

		if p.isSym("[") {
			if err := p.parseValueList(); err != nil {
				return err
			}
		} else if err := p.parseOperand(); err != nil {
			return err
		}

		if !p.isSym(",") {
			return nil
		}

		p.i++

		if opt = p.next(); opt.kind != dqTokIdent {
			return fmt.Errorf("%w: offset %d: expected an option name", errDQDL, opt.pos)
		}
	}
}

func dqBalanced(s string) bool {
	depth := 0

	var quote rune

	for _, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"' || r == '`':
			quote = r
		case r == '(':
			depth++
		case r == ')':
			depth--
			if depth < 0 {
				return false
			}
		}
	}

	return depth == 0 && quote == 0
}
