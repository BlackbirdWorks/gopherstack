package iot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	errTemplate          = errors.New("invalid substitution template")
	errTemplateUndefined = errors.New("substitution template resolved to undefined")
)

// ruleMessage is the message a rule matched, as substitution templates see it.
type ruleMessage struct {
	received time.Time
	topic    string
	clientID string
	region   string
	account  string
	payload  []byte
	hops     int
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

func (m *ruleMessage) eval(expr string) (string, error) {
	open := strings.Index(expr, "(")
	if open > 0 && strings.HasSuffix(expr, ")") {
		return m.call(strings.ToLower(expr[:open]), strings.TrimSpace(expr[open+1:len(expr)-1]))
	}

	return m.field(expr)
}

func (m *ruleMessage) call(name, arg string) (string, error) {
	switch name {
	case "topic":
		return m.topicSegment(arg)
	case "timestamp":
		return strconv.FormatInt(m.received.UnixMilli(), 10), nil
	case "clientid":
		return m.clientID, nil
	case "newuuid":
		return uuid.NewString(), nil
	case "accountid":
		return m.account, nil
	default:
		return "", fmt.Errorf("%w: unsupported function %s()", errTemplate, name)
	}
}

func (m *ruleMessage) topicSegment(arg string) (string, error) {
	if arg == "" {
		return m.topic, nil
	}

	n, err := strconv.Atoi(arg)
	if err != nil || n < 1 {
		return "", fmt.Errorf("%w: topic() takes a segment number from 1", errTemplate)
	}

	segments := strings.Split(m.topic, "/")
	if n > len(segments) {
		return "", fmt.Errorf("%w: topic(%d)", errTemplateUndefined, n)
	}

	return segments[n-1], nil
}

// field resolves a dotted payload path such as a.b[0].c.
func (m *ruleMessage) field(path string) (string, error) {
	cur, err := m.decoded()
	if err != nil {
		return "", err
	}

	for seg := range strings.SplitSeq(path, ".") {
		name, idx, hasIdx, perr := splitIndex(seg)
		if perr != nil {
			return "", perr
		}

		if name != "" {
			obj, ok := cur.(map[string]any)
			if !ok {
				return "", errTemplateUndefined
			}

			if cur, ok = obj[name]; !ok {
				return "", errTemplateUndefined
			}
		}

		if hasIdx {
			arr, ok := cur.([]any)
			if !ok || idx >= len(arr) {
				return "", errTemplateUndefined
			}

			cur = arr[idx]
		}
	}

	return renderValue(cur)
}

func (m *ruleMessage) decoded() (any, error) {
	dec := json.NewDecoder(bytes.NewReader(m.payload))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, errTemplateUndefined
	}

	return v, nil
}

func splitIndex(seg string) (string, int, bool, error) {
	open := strings.Index(seg, "[")
	if open < 0 {
		return seg, 0, false, nil
	}

	if !strings.HasSuffix(seg, "]") {
		return "", 0, false, fmt.Errorf("%w: bad index in %q", errTemplate, seg)
	}

	idx, err := strconv.Atoi(seg[open+1 : len(seg)-1])
	if err != nil || idx < 0 {
		return "", 0, false, fmt.Errorf("%w: bad index in %q", errTemplate, seg)
	}

	return seg[:open], idx, true, nil
}

func renderValue(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case json.Number:
		return t.String(), nil
	case bool:
		return strconv.FormatBool(t), nil
	case nil:
		return "", errTemplateUndefined
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return "", fmt.Errorf("%w: %w", errTemplate, err)
		}

		return string(b), nil
	}
}
