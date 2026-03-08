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
		return "INSUFFICIENT_DATA"
	}
	if result {
		return "ALARM"
	}

	return "OK"
}

// EvaluateAlarmRuleForTest is a test-visible wrapper around evaluateAlarmRule.
func EvaluateAlarmRuleForTest(rule string, states map[string]string) string {
	return evaluateAlarmRule(rule, func(name string) string {
		if s, ok := states[name]; ok {
			return s
		}

		return alarmStateInsufficientData
	})
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
			// quoted string
			j := i + 1
			for j < len(rule) && rule[j] != '"' {
				j++
			}
			tokens = append(tokens, rule[i:j+1])
			i = j + 1

			continue
		}
		// word token
		j := i
		for j < len(rule) && !unicode.IsSpace(rune(rule[j])) && rule[j] != '(' && rule[j] != ')' && rule[j] != '"' {
			j++
		}
		if j > i {
			tokens = append(tokens, rule[i:j])
			i = j
		} else {
			i++
		}
	}

	return tokens
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
		if p.peek() == ")" {
			p.consume()
		}

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
