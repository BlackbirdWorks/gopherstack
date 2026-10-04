package cloudwatch

import (
	"encoding/json"
	"fmt"
	"strings"
)

// maxContributionKeys is Contributor Insights' documented limit on
// Contribution.Keys entries.
const maxContributionKeys = 4

// maxInsightRuleDefinitionLen is PutInsightRule.RuleDefinition's documented max length.
const maxInsightRuleDefinitionLen = 8192

// validateInsightRuleDefinition validates a PutInsightRule RuleDefinition against the
// Contributor Insights rule syntax (ContributorInsights-RuleSyntax.html).
func validateInsightRuleDefinition(definition string) error {
	trimmed := strings.TrimSpace(definition)
	if trimmed == "" {
		return fmt.Errorf("%w: RuleDefinition parameter is required", ErrValidation)
	}

	if len(definition) > maxInsightRuleDefinitionLen {
		return fmt.Errorf("%w: RuleDefinition exceeds %d characters", ErrValidation, maxInsightRuleDefinitionLen)
	}

	var raw any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return fmt.Errorf("%w: RuleDefinition is not valid JSON: %s", ErrValidation, err.Error())
	}

	obj, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: RuleDefinition must be a JSON object", ErrValidation)
	}

	if err := validateInsightRuleSchema(obj); err != nil {
		return err
	}

	return validateInsightRuleSpec(trimmed)
}

// validateInsightRuleSchema enforces the structural rules documented in
// validateInsightRuleDefinition's doc comment.
func validateInsightRuleSchema(obj map[string]any) error {
	name, err := validateInsightRuleSchemaBlock(obj)
	if err != nil {
		return err
	}

	logFormat, _ := obj["LogFormat"].(string)
	if logFormat != "JSON" && logFormat != "CLF" {
		return fmt.Errorf("%w: RuleDefinition.LogFormat must be JSON or CLF", ErrValidation)
	}

	if !isNonEmptyStringArray(obj["LogGroupNames"]) {
		return fmt.Errorf("%w: RuleDefinition.LogGroupNames must be a non-empty array of strings", ErrValidation)
	}

	contribution, ok := obj["Contribution"].(map[string]any)
	if !ok {
		return fmt.Errorf("%w: RuleDefinition.Contribution is required", ErrValidation)
	}

	if keysErr := validateContributionKeys(contribution); keysErr != nil {
		return keysErr
	}

	return validateAggregateOn(obj, contribution, name)
}

// validateInsightRuleSchemaBlock validates RuleDefinition.Schema.{Name,Version}
// and returns the validated Name.
func validateInsightRuleSchemaBlock(obj map[string]any) (string, error) {
	schema, ok := obj["Schema"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("%w: RuleDefinition.Schema is required", ErrValidation)
	}

	name, _ := schema["Name"].(string)
	if name != "CloudWatchLogRule" && name != "CloudWatchLogRule2" {
		return "", fmt.Errorf(
			"%w: RuleDefinition.Schema.Name must be CloudWatchLogRule or CloudWatchLogRule2", ErrValidation,
		)
	}

	const schemaVersion = 1

	version, ok := schema["Version"].(float64)
	if !ok || version != schemaVersion {
		return "", fmt.Errorf("%w: RuleDefinition.Schema.Version must be 1", ErrValidation)
	}

	return name, nil
}

// validateContributionKeys validates Contribution.Keys is 1-4 string entries.
func validateContributionKeys(contribution map[string]any) error {
	keysRaw, ok := contribution["Keys"].([]any)
	if ok && len(keysRaw) > maxContributionKeys {
		return fmt.Errorf(
			"%w: RuleDefinition.Contribution.Keys allows at most %d entries",
			ErrInsightRuleLimit,
			maxContributionKeys,
		)
	}

	if !ok || len(keysRaw) == 0 {
		return fmt.Errorf(
			"%w: RuleDefinition.Contribution.Keys must be an array of 1-%d strings",
			ErrValidation, maxContributionKeys,
		)
	}

	for _, k := range keysRaw {
		if _, isString := k.(string); !isString {
			return fmt.Errorf("%w: RuleDefinition.Contribution.Keys entries must be strings", ErrValidation)
		}
	}

	return nil
}

// validateAggregateOn checks the AggregateOn enum and that Sum has a ValueOf.
func validateAggregateOn(obj map[string]any, contribution map[string]any, _ string) error {
	aggOn, hasAggOn := obj["AggregateOn"]
	if !hasAggOn {
		return nil
	}

	aggOnStr, _ := aggOn.(string)
	if aggOnStr != aggregateCount && aggOnStr != aggregateSum {
		return fmt.Errorf("%w: RuleDefinition.AggregateOn must be Count or Sum", ErrValidation)
	}

	if aggOnStr == aggregateSum {
		if _, ok := contribution["ValueOf"].(string); !ok {
			return fmt.Errorf(
				"%w: RuleDefinition.Contribution.ValueOf is required when AggregateOn is Sum", ErrValidation,
			)
		}
	}

	return nil
}

// isNonEmptyStringArray reports whether v is a JSON array of one or more strings.
func isNonEmptyStringArray(v any) bool {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return false
	}

	for _, e := range arr {
		if _, isString := e.(string); !isString {
			return false
		}
	}

	return true
}
