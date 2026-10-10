package awsconfig

import (
	"fmt"
	"slices"
)

const (
	maxConfigRuleNameLen        = 128
	maxConfigRuleDescriptionLen = 256
	maxConfigRuleParamsLen      = 1024
)

func ruleExecutionFrequencies() []string {
	return []string{"One_Hour", "Three_Hours", "Six_Hours", "Twelve_Hours", "TwentyFour_Hours"}
}

func ruleMessageTypes() []string {
	return []string{
		"ConfigurationItemChangeNotification",
		"ConfigurationSnapshotDeliveryCompleted",
		"OversizedConfigurationItemChangeNotification",
		"ScheduledNotification",
	}
}

func ruleEvaluationModes() []string { return []string{"DETECTIVE", "PROACTIVE"} }

func invalidRuleParam(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidParameterValue, fmt.Sprintf(format, args...))
}

// validateConfigRule applies PutConfigRule's documented limits and enums. AWS managed
// SourceIdentifiers are not checked: the emulator models only a handful of the catalogue.
func validateConfigRule(r *ConfigRule) error {
	if len(r.ConfigRuleName) > maxConfigRuleNameLen {
		return invalidRuleParam("ConfigRuleName must be at most %d characters", maxConfigRuleNameLen)
	}

	if len(r.Description) > maxConfigRuleDescriptionLen {
		return invalidRuleParam("Description must be at most %d characters", maxConfigRuleDescriptionLen)
	}

	if len(r.InputParameters) > maxConfigRuleParamsLen {
		return invalidRuleParam("InputParameters must be at most %d characters", maxConfigRuleParamsLen)
	}

	if r.MaximumExecutionFrequency != "" && !slices.Contains(ruleExecutionFrequencies(), r.MaximumExecutionFrequency) {
		return invalidRuleParam("invalid MaximumExecutionFrequency %q", r.MaximumExecutionFrequency)
	}

	for _, m := range r.EvaluationModes {
		if !slices.Contains(ruleEvaluationModes(), m.Mode) {
			return invalidRuleParam("invalid evaluation mode %q", m.Mode)
		}
	}

	return validateRuleSource(r.Source)
}

func validateRuleSource(src *ConfigRuleSource) error {
	if src == nil {
		return nil
	}

	switch src.Owner {
	case ruleOwnerAWS:
		if src.SourceIdentifier == "" {
			return invalidRuleParam("SourceIdentifier is required for an AWS managed rule")
		}
	case ruleOwnerCustomLambda:
	case ruleOwnerCustomPolicy:
		if src.CustomPolicyDetails == nil || src.CustomPolicyDetails.PolicyText == "" {
			return invalidRuleParam("CustomPolicyDetails with PolicyText is required for a CUSTOM_POLICY rule")
		}
	default:
		return invalidRuleParam("invalid Source Owner %q", src.Owner)
	}

	for _, d := range src.SourceDetails {
		if d.MessageType != "" && !slices.Contains(ruleMessageTypes(), d.MessageType) {
			return invalidRuleParam("invalid SourceDetail MessageType %q", d.MessageType)
		}

		if d.MaximumExecutionFrequency != "" &&
			!slices.Contains(ruleExecutionFrequencies(), d.MaximumExecutionFrequency) {
			return invalidRuleParam("invalid SourceDetail MaximumExecutionFrequency %q", d.MaximumExecutionFrequency)
		}
	}

	return nil
}
