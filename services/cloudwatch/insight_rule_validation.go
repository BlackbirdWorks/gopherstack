package cloudwatch

import (
	"encoding/json"
	"fmt"
	"strings"
)

// validateInsightRuleDefinition checks that a RuleDefinition parameter is present
// and is well-formed JSON, matching real CloudWatch's server-side validation of
// the Contributor Insights rule syntax (RuleDefinition is a required, JSON-object
// parameter per the API model even though the SDK types it as an opaque string —
// AWS itself parses and validates it, rejecting malformed or non-object bodies
// with InvalidParameterValue before a rule is ever created or updated).
//
// This intentionally does not attempt to enforce the full Contributor Insights
// rule schema depth (Schema/LogFormat/Contribution/Filters field-level rules) —
// that schema is not part of the generated SDK model (RuleDefinition is opaque
// there too) and re-deriving it exactly from documentation risks diverging from
// real AWS in ways a wire-shape diff can't catch. JSON-shape validation closes
// the concrete "accepted verbatim" gap: a non-JSON or non-object RuleDefinition
// is rejected instead of being silently stored.
func validateInsightRuleDefinition(definition string) error {
	trimmed := strings.TrimSpace(definition)
	if trimmed == "" {
		return fmt.Errorf("%w: RuleDefinition parameter is required", ErrValidation)
	}

	var raw any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return fmt.Errorf("%w: RuleDefinition is not valid JSON: %s", ErrValidation, err.Error())
	}

	if _, ok := raw.(map[string]any); !ok {
		return fmt.Errorf("%w: RuleDefinition must be a JSON object", ErrValidation)
	}

	return nil
}
