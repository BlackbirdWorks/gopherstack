package iot

import (
	"strings"
)

const (
	sqlVersion2015 = "2015-10-08"
	sqlVersion2016 = "2016-03-23"
	sqlVersionBeta = "beta"
)

// ParsedRule is a compiled IoT SQL rule statement.
type ParsedRule struct {
	stmt *selectStmt
	// TopicPattern is the MQTT topic filter from the FROM clause.
	TopicPattern string
	version      string
	v2016        bool
}

// ParseRuleSQL compiles a rule statement under the latest SQL version.
func ParseRuleSQL(sql string) (*ParsedRule, error) {
	return ParseRuleSQLVersion(sql, sqlVersion2016)
}

// ParseRuleSQLVersion compiles a rule statement under the given awsIotSqlVersion.
func ParseRuleSQLVersion(sql, version string) (*ParsedRule, error) {
	v2016 := version != sqlVersion2015 && version != ""

	if strings.TrimSpace(sql) == "" {
		return nil, ErrSQLParse
	}

	p := newSQLParser(sql, v2016)
	stmt, topic := p.parseTopStatement()

	if p.err != nil {
		return nil, p.err
	}

	return &ParsedRule{stmt: stmt, TopicPattern: topic, version: effectiveVersion(version), v2016: v2016}, nil
}

func effectiveVersion(v string) string {
	if v == "" {
		return sqlVersion2015
	}

	return v
}

// validSQLVersion reports whether v is an accepted awsIotSqlVersion.
func validSQLVersion(v string) bool {
	return v == sqlVersion2015 || v == sqlVersion2016 || v == sqlVersionBeta
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
		// # must be the last segment; only matches if no more pattern segments follow.
		return len(pattern) == 1
	}

	if len(topic) == 0 {
		return false
	}

	if pattern[0] != "+" && pattern[0] != topic[0] {
		return false
	}

	return matchParts(pattern[1:], topic[1:])
}

// EvaluateRule reports whether a message on topic with payload fires the rule.
func EvaluateRule(rule *TopicRule, topic string, payload []byte) bool {
	return rule.fire(&ruleMessage{topic: topic, payload: payload, original: payload})
}

// fire reports whether msg triggers the rule and, if so, sets msg.payload to the SELECT result.
func (r *TopicRule) fire(msg *ruleMessage) bool {
	if r == nil || !r.Enabled {
		return false
	}

	parsed, err := ParseRuleSQLVersion(r.SQL, r.AWSIoTSQLVersion)
	if err != nil || !MatchesTopic(parsed.TopicPattern, msg.topic) {
		return false
	}

	out, ok := parsed.apply(msg)
	if !ok {
		return false
	}

	msg.payload = out

	return true
}

// apply evaluates WHERE then SELECT against the message's original payload.
func (p *ParsedRule) apply(msg *ruleMessage) ([]byte, bool) {
	c := newSQLCtx(msg, p.version, p.v2016)

	if !p.stmt.passes(c) {
		return nil, false
	}

	if p.stmt.loneStar() {
		return msg.original, true
	}

	return marshalResult(p.stmt.project(c)), true
}
