package cloudwatch

import (
	"strings"
	"unicode"
)

// evaluateAlarmRule evaluates a composite alarm rule expression against the current alarm state resolver.
// The resolver returns the StateValue ("ALARM", "OK", "INSUFFICIENT_DATA") for a named alarm.
// Returns the resulting StateValue: "ALARM", "OK", or "INSUFFICIENT_DATA".
func evaluateAlarmRule(rule string, resolveState func(name string) string) string {
	p := &ruleParser{tokens: tokenizeRule(rule), pos: 0, resolve: resolveState}
	result, ok := p.parseExpr()
	if !ok {
		return alarmStateInsufficientData
	}
	if result {
		return alarmStateAlarm
	}

	return alarmStateOK
}

// tokenizeRule splits the AlarmRule string into tokens.
func tokenizeRule(rule string) []string {
	var tokens []string
	i := 0
	for i < len(rule) {
		ch := rune(rule[i])
		if unicode.IsSpace(ch) {
			i++

			continue
		}
		if ch == '(' || ch == ')' {
			tokens = append(tokens, string(ch))
			i++

			continue
		}
		if ch == '"' {
			tok, nextPos := tokenizeQuotedString(rule, i)
			tokens = append(tokens, tok)
			i = nextPos

			continue
		}
		tok, nextPos := tokenizeWord(rule, i)
		if nextPos > i {
			tokens = append(tokens, tok)
			i = nextPos
		} else {
			i++
		}
	}

	return tokens
}

// tokenizeQuotedString scans a quoted string starting at pos in rule.
// Returns the token and the next position to continue scanning.
// If the closing quote is missing, returns an empty-string token and advances to end-of-input.
func tokenizeQuotedString(rule string, pos int) (string, int) {
	j := pos + 1
	for j < len(rule) && rule[j] != '"' {
		j++
	}
	if j >= len(rule) {
		// unterminated quote: signal a parse error by returning an empty-string token
		return `""`, j
	}
	nextPos := j + 1

	return rule[pos:nextPos], nextPos
}

// tokenizeWord scans a non-quoted word token starting at pos in rule.
// Returns the token and the next position to continue scanning.
func tokenizeWord(rule string, pos int) (string, int) {
	nextPos := pos
	for nextPos < len(rule) && !unicode.IsSpace(rune(rule[nextPos])) && rule[nextPos] != '(' && rule[nextPos] != ')' && rule[nextPos] != '"' {
		nextPos++
	}

	return rule[pos:nextPos], nextPos
}

type ruleParser struct {
	resolve func(name string) string
	tokens  []string
	pos     int
}

func (p *ruleParser) peek() string {
	if p.pos >= len(p.tokens) {
		return ""
	}

	return p.tokens[p.pos]
}

func (p *ruleParser) consume() string {
	t := p.peek()
	p.pos++

	return t
}

// parseExpr parses the top-level expression (OR-level).
func (p *ruleParser) parseExpr() (bool, bool) {
	return p.parseOrExpr()
}

func (p *ruleParser) parseOrExpr() (bool, bool) {
	left, ok := p.parseAndExpr()
	if !ok {
		return false, false
	}
	for strings.EqualFold(p.peek(), "OR") {
		p.consume()
		right, ok2 := p.parseAndExpr()
		if !ok2 {
			return false, false
		}
		left = left || right
	}

	return left, true
}

func (p *ruleParser) parseAndExpr() (bool, bool) {
	left, ok := p.parseNotExpr()
	if !ok {
		return false, false
	}
	for strings.EqualFold(p.peek(), "AND") {
		p.consume()
		right, ok2 := p.parseNotExpr()
		if !ok2 {
			return false, false
		}
		left = left && right
	}

	return left, true
}

func (p *ruleParser) parseNotExpr() (bool, bool) {
	if strings.EqualFold(p.peek(), "NOT") {
		p.consume()
		val, ok := p.parseNotExpr()
		if !ok {
			return false, false
		}

		return !val, true
	}

	return p.parseAtom()
}

func (p *ruleParser) parseAtom() (bool, bool) {
	tok := p.peek()
	if tok == "(" {
		p.consume()
		val, ok := p.parseExpr()
		if !ok {
			return false, false
		}
		if p.peek() != ")" {
			return false, false
		}
		p.consume()

		return val, true
	}

	// State function: ALARM("name"), OK("name"), INSUFFICIENT_DATA("name")
	upper := strings.ToUpper(tok)
	if upper == "ALARM" || upper == "OK" || upper == "INSUFFICIENT_DATA" {
		p.consume()
		if p.peek() != "(" {
			return false, false
		}
		p.consume()
		name := p.consume()
		// strip surrounding quotes
		name = strings.Trim(name, `"`)
		if p.peek() != ")" {
			return false, false
		}
		p.consume()

		state := p.resolve(name)

		return state == upper, true
	}

	return false, false
}
