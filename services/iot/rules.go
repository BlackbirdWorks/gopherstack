package iot

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrCannotParseFloat is returned when a numeric value cannot be parsed.
var ErrCannotParseFloat = errors.New("cannot parse value as float")

// ParsedRule holds the parsed components of an IoT SQL rule statement.
type ParsedRule struct {
	// TopicPattern is the MQTT topic pattern extracted from the FROM clause.
	TopicPattern string
	// Condition is the optional WHERE clause predicate.
	Condition string
}

// ParseRuleSQL parses a simplified AWS IoT SQL rule statement.
// Supported format: SELECT * FROM 'topic/pattern' [WHERE condition].
func ParseRuleSQL(sql string) (*ParsedRule, error) {
	sql = strings.TrimSpace(sql)

	fromIdx := strings.Index(strings.ToUpper(sql), " FROM ")
	if fromIdx < 0 {
		return &ParsedRule{}, nil
	}

	rest := strings.TrimSpace(sql[fromIdx+6:])

	var topicPattern, condition string

	if len(rest) > 0 && rest[0] == '\'' {
		end := strings.Index(rest[1:], "'")
		if end >= 0 {
			topicPattern = rest[1 : end+1]
			after := strings.TrimSpace(rest[end+2:])

			if whereIdx := strings.Index(strings.ToUpper(after), "WHERE "); whereIdx >= 0 {
				condition = strings.TrimSpace(after[whereIdx+6:])
			}
		}
	}

	return &ParsedRule{
		TopicPattern: topicPattern,
		Condition:    condition,
	}, nil
}

// MatchesTopic reports whether the MQTT topic matches the given pattern.
// Wildcard semantics:
//   - # matches zero or more levels (must be the last segment)
//   - + matches exactly one level
func MatchesTopic(topicPattern, topic string) bool {
	if topicPattern == "#" {
		return true
	}

	return matchParts(
		strings.Split(topicPattern, "/"),
		strings.Split(topic, "/"),
	)
}

func matchParts(pattern, topic []string) bool {
	if len(pattern) == 0 {
		return len(topic) == 0
	}

	if pattern[0] == "#" {
		return true
	}

	if len(topic) == 0 {
		return false
	}

	if pattern[0] != "+" && pattern[0] != topic[0] {
		return false
	}

	return matchParts(pattern[1:], topic[1:])
}

// EvaluateRule reports whether the given topic and payload satisfy the rule's SQL predicate.
func EvaluateRule(rule *TopicRule, topic string, payload []byte) bool {
	if rule == nil || !rule.Enabled {
		return false
	}

	parsed, err := ParseRuleSQL(rule.SQL)
	if err != nil || parsed.TopicPattern == "" {
		return false
	}

	if !MatchesTopic(parsed.TopicPattern, topic) {
		return false
	}

	if parsed.Condition == "" {
		return true
	}

	return evaluateCondition(parsed.Condition, payload)
}

// evaluateCondition evaluates a simple WHERE condition against a JSON payload.
// Supported operators: >, >=, <, <=, =, !=.
func evaluateCondition(condition string, payload []byte) bool {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return false
	}

	condition = strings.TrimSpace(condition)

	// Try operators in longest-first order to avoid ">=" being parsed as ">".
	for _, op := range []string{">=", "<=", "!=", ">", "<", "="} {
		before, after, ok := strings.Cut(condition, op)
		if !ok {
			continue
		}

		fieldName := strings.TrimSpace(before)
		rawVal := strings.Trim(strings.TrimSpace(after), "'\"")

		fieldVal, exists := data[fieldName]
		if !exists {
			return false
		}

		return compareValues(fieldVal, rawVal, op)
	}

	return false
}

func compareValues(fieldVal any, rawVal, op string) bool {
	switch v := fieldVal.(type) {
	case float64:
		var target float64
		if err := parseFloat(rawVal, &target); err != nil {
			return false
		}

		switch op {
		case ">":
			return v > target
		case ">=":
			return v >= target
		case "<":
			return v < target
		case "<=":
			return v <= target
		case "=":
			return v == target
		case "!=":
			return v != target
		}
	case string:
		switch op {
		case "=":
			return v == rawVal
		case "!=":
			return v != rawVal
		}
	case bool:
		target := rawVal == "true"
		switch op {
		case "=":
			return v == target
		case "!=":
			return v != target
		}
	}

	return false
}

func parseFloat(s string, out *float64) error {
	var f float64

	if n, err := fmt.Sscanf(s, "%f", &f); n == 1 && err == nil {
		*out = f

		return nil
	}

	return fmt.Errorf("%w: %q", ErrCannotParseFloat, s)
}
