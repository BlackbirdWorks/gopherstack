package wafv2

import (
	"fmt"
	"regexp"
)

// validEvaluationWindowSecs contains the allowed EvaluationWindowSec values.
var validEvaluationWindowSecs = map[int64]bool{ //nolint:gochecknoglobals // package-level lookup table
	60: true, 120: true, 300: true, 600: true,
	1800: true, 3600: true, 7200: true, 21600: true,
}

// maxStatementNestingDepth is the maximum allowed nesting depth for rule statements.
const maxStatementNestingDepth = 3

// nestedStatementKeys are statement wrapper keys that may contain nested statements.
var nestedStatementKeys = []string{ //nolint:gochecknoglobals // package-level lookup table
	"AndStatement", "OrStatement", "NotStatement",
	"ManagedRuleGroupStatement", "RuleGroupReferenceStatement",
}

// validateStatement performs basic structural validation on a rule statement map.
// It handles RateBasedStatement and RegexPatternReferenceStatement recursively (depth-limited).
// The nested-wrapper and regex-pattern checks are split into their own helpers below to
// keep this function's cognitive complexity low instead of suppressing the linter.
func validateStatement(stmt map[string]any, depth int) error {
	if depth > maxStatementNestingDepth {
		return fmt.Errorf(
			"%w: statement nesting exceeds maximum depth of %d",
			errInvalidRequest,
			maxStatementNestingDepth,
		)
	}

	if rbs, isRBS := stmt["RateBasedStatement"].(map[string]any); isRBS {
		return validateRateBasedStatement(rbs)
	}

	if err := validateNestedStatements(stmt, depth); err != nil {
		return err
	}

	return validateRegexPatternStatement(stmt)
}

// validateNestedStatements recurses into every statement wrapper key present in stmt
// (AndStatement, OrStatement, NotStatement, ManagedRuleGroupStatement,
// RuleGroupReferenceStatement), validating each nested Statement/Statements payload.
func validateNestedStatements(stmt map[string]any, depth int) error {
	for _, key := range nestedStatementKeys {
		nested, isNested := stmt[key].(map[string]any)
		if !isNested {
			continue
		}

		if err := validateNestedStatementWrapper(nested, depth); err != nil {
			return err
		}
	}

	return nil
}

// validateNestedStatementWrapper validates the single-Statement form (NotStatement,
// ManagedRuleGroupStatement, RuleGroupReferenceStatement) and the plural-Statements form
// (AndStatement, OrStatement) of a nested statement wrapper.
func validateNestedStatementWrapper(nested map[string]any, depth int) error {
	if inner, hasInner := nested["Statement"].(map[string]any); hasInner {
		if err := validateStatement(inner, depth+1); err != nil {
			return err
		}
	}

	stmts, hasStmts := nested["Statements"].([]any)
	if !hasStmts {
		return nil
	}

	for _, s := range stmts {
		sm, isSM := s.(map[string]any)
		if !isSM {
			continue
		}

		if err := validateStatement(sm, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// validateRegexPatternStatement validates the RegexString of a
// RegexPatternSetReferenceStatement, if present.
func validateRegexPatternStatement(stmt map[string]any) error {
	rps, hasRPS := stmt["RegexPatternSetReferenceStatement"].(map[string]any)
	if !hasRPS {
		return nil
	}

	pattern, hasPattern := rps["RegexString"].(string)
	if !hasPattern {
		return nil
	}

	if _, err := regexp.Compile(pattern); err != nil {
		return fmt.Errorf("%w: invalid regex pattern %q: %w", errInvalidRequest, pattern, err)
	}

	return nil
}
func validateRateBasedStatement(rbs map[string]any) error {
	limit, _ := toInt64(rbs["Limit"])
	if limit < minRateLimit || limit > maxRateLimit {
		return fmt.Errorf(
			"%w: RateBasedStatement.Limit must be between %d and %d",
			errInvalidRequest,
			minRateLimit,
			maxRateLimit,
		)
	}

	if ewsRaw, ok := rbs["EvaluationWindowSec"]; ok {
		ews, _ := toInt64(ewsRaw)
		if !validEvaluationWindowSecs[ews] {
			return fmt.Errorf(
				"%w: RateBasedStatement.EvaluationWindowSec must be one of {60,120,300,600,1800,3600,7200,21600}",
				errInvalidRequest,
			)
		}
	}

	return nil
}

// validateRules validates a slice of rules for a WebACL or RuleGroup.
func validateRules(rules []map[string]any) error {
	priorities := make(map[int64]bool)
	names := make(map[string]bool)

	for _, rule := range rules {
		if err := validateSingleRule(rule, priorities, names); err != nil {
			return err
		}
	}

	return nil
}

// validateSingleRule validates a single rule map and updates the seen priority
// and name sets. Extracted to keep validateRules below the cognitive complexity
// threshold.
func validateSingleRule(rule map[string]any, priorities map[int64]bool, names map[string]bool) error {
	name, _ := rule["Name"].(string)
	if name == "" {
		return fmt.Errorf("%w: rule Name is required", errInvalidRequest)
	}

	if names[name] {
		return fmt.Errorf("%w: duplicate rule Name %q in rules", errInvalidRequest, name)
	}

	names[name] = true

	priorityRaw, hasPriority := rule["Priority"]
	if !hasPriority {
		return fmt.Errorf("%w: rule %q is missing Priority", errInvalidRequest, name)
	}

	priority, ok := toInt64(priorityRaw)
	if !ok {
		return fmt.Errorf("%w: rule %q Priority must be an integer", errInvalidRequest, name)
	}

	if priority < 0 || priority > maxRulePriority {
		return fmt.Errorf(
			"%w: rule %q Priority must be between 0 and %d, got %d",
			errInvalidRequest,
			name,
			maxRulePriority,
			priority,
		)
	}

	if priorities[priority] {
		return fmt.Errorf("%w: duplicate Priority %d in rules", errInvalidRequest, priority)
	}

	priorities[priority] = true

	if _, hasStatement := rule["Statement"]; !hasStatement {
		return fmt.Errorf("%w: rule %q is missing Statement", errInvalidRequest, name)
	}

	if stmt, isMap := rule["Statement"].(map[string]any); isMap {
		if err := validateStatement(stmt, 0); err != nil {
			return err
		}
	}

	if _, hasVC := rule["VisibilityConfig"]; !hasVC {
		return fmt.Errorf("%w: rule %q is missing VisibilityConfig", errInvalidRequest, name)
	}

	return nil
}
