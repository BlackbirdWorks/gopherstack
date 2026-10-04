package iot

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	errTemplate          = errors.New("invalid substitution template")
	errTemplateUndefined = errors.New("substitution template resolved to undefined")
	errSQLFunction       = errors.New("sql function failed")
)

// ruleMessage is the message a rule matched, as substitution templates see it.
type ruleMessage struct {
	received  time.Time
	fatal     error
	hook      *ruleHook
	vars      map[string]any
	calls     map[string]int
	props     *mqttProps
	topic     string
	clientID  string
	region    string
	account   string
	ruleARN   string
	principal string
	sourceIP  string
	traceID   string
	payload   []byte
	original  []byte
	hops      int
	ingest    bool
}

// fail aborts the rule: a failed SQL function runs no actions.
func (m *ruleMessage) fail(cause error, fn string) {
	if m.fatal == nil {
		m.fatal = fmt.Errorf("%w: %s", cause, fn)
	}
}

// callOnce reports whether fn may run; some functions are limited to one call per statement.
func (m *ruleMessage) callOnce(fn string) bool {
	if m.calls == nil {
		m.calls = map[string]int{}
	}

	m.calls[fn]++

	return m.calls[fn] == 1
}

// expand resolves every ${expression} in tmpl against the original message.
func (m *ruleMessage) expand(tmpl string) (string, error) {
	var sb strings.Builder

	for {
		start := strings.Index(tmpl, "${")
		if start < 0 {
			sb.WriteString(tmpl)

			return sb.String(), nil
		}

		end := strings.Index(tmpl[start:], "}")
		if end < 0 {
			return "", fmt.Errorf("%w: unterminated expression", errTemplate)
		}

		val, err := m.eval(strings.TrimSpace(tmpl[start+2 : start+end]))
		if err != nil {
			return "", err
		}

		sb.WriteString(tmpl[:start])
		sb.WriteString(val)

		tmpl = tmpl[start+end+1:]
	}
}

// eval evaluates one template expression with the full SQL expression grammar.
func (m *ruleMessage) eval(expr string) (string, error) {
	p := newSQLParser(expr, true)
	n := p.parseExpr()

	if p.err == nil && p.tok.kind != tokEOF {
		p.fail("unexpected " + p.tok.text)
	}

	if p.err != nil {
		return "", fmt.Errorf("%w: %w", errTemplate, p.err)
	}

	v := n.eval(newSQLCtx(m, sqlVersion2016, true))

	if err := m.fatal; err != nil {
		m.fatal = nil

		return "", err
	}

	return renderTemplateValue(v)
}

func renderTemplateValue(v any) (string, error) {
	if v == nil || isUndef(v) {
		return "", errTemplateUndefined
	}

	if s, ok := toStringConv(v); ok {
		return s, nil
	}

	return "", errTemplateUndefined
}
