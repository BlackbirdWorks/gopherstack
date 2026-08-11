package cloudwatch

import (
	"encoding/json"
	"fmt"
	"strings"
)

// maxContributionKeys is Contributor Insights' documented limit on
// Contribution.Keys entries.
const maxContributionKeys = 4

// validateInsightRuleDefinition checks that a RuleDefinition parameter is present,
// well-formed JSON, and matches the structural rules of the Contributor Insights
// Rule Syntax, matching real CloudWatch's server-side validation (RuleDefinition
// is a required, JSON-object parameter per the API model even though the SDK
// types it as an opaque string — AWS itself parses and validates it, rejecting
// malformed bodies with InvalidParameterValue before a rule is ever created or
// updated).
//
// The Contributor Insights Rule Syntax is not part of the generated SDK model
// (RuleDefinition is opaque there too, no typed struct to field-diff against) —
// verified instead against AWS's published syntax reference
// (https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/ContributorInsights-RuleSyntax.html).
// Enforced: Schema.Name (CloudWatchLogRule/CloudWatchLogRule2)/Version (1),
// LogFormat (JSON/CLF), LogGroupNames (non-empty string array),
// Contribution.Keys (1-4 string entries), and AggregateOn's Count/Sum enum
// with the required Contribution.ValueOf when summing. NOT enforced
// (deliberately, to avoid diverging from real AWS on rules this pass could
// not verify against a generated type): whether AggregateOn is restricted to
// a specific Schema.Name (an integration test exercises AggregateOn=Count
// against the base CloudWatchLogRule schema successfully, so this is not
// cross-checked), Contribution.Filters' per-match-type field shape, and
// CLF's Fields position-mapping requirement.
func validateInsightRuleDefinition(definition string) error {
	trimmed := strings.TrimSpace(definition)
	if trimmed == "" {
		return fmt.Errorf("%w: RuleDefinition parameter is required", ErrValidation)
	}

	var raw any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return fmt.Errorf("%w: RuleDefinition is not valid JSON: %s", ErrValidation, err.Error())
	}

	obj, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: RuleDefinition must be a JSON object", ErrValidation)
	}

	return validateInsightRuleSchema(obj)
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
	if !ok || len(keysRaw) == 0 || len(keysRaw) > maxContributionKeys {
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

// validateAggregateOn validates AggregateOn/Contribution.ValueOf pairing.
// AggregateOn's enum (Count/Sum) is enforced unconditionally; ValueOf is
// required alongside AggregateOn=Sum. The schema-name field is accepted but
// deliberately NOT cross-checked against AggregateOn here -- AWS's own
// documentation on which Schema.Name values accept AggregateOn is not
// verifiable against a generated SDK type (RuleDefinition is opaque there),
// and this backend has observed AggregateOn used successfully together with
// the base CloudWatchLogRule schema, contradicting an earlier, stricter draft
// of this check that required CloudWatchLogRule2 -- reverted rather than risk
// rejecting valid real-world documents on unverifiable doc-derived rules.
func validateAggregateOn(obj map[string]any, contribution map[string]any, _ string) error {
	aggOn, hasAggOn := obj["AggregateOn"]
	if !hasAggOn {
		return nil
	}

	aggOnStr, _ := aggOn.(string)
	if aggOnStr != "Count" && aggOnStr != "Sum" {
		return fmt.Errorf("%w: RuleDefinition.AggregateOn must be Count or Sum", ErrValidation)
	}

	if aggOnStr == "Sum" {
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
