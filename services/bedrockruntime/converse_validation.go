package bedrockruntime

import (
	"encoding/json"
	"regexp"
	"slices"
)

const maxRequestMetadataEntries = 16

var (
	requestMetadataKeyRE   = regexp.MustCompile(`^[a-zA-Z0-9\s:_@$#=/+,-.]{1,256}$`)
	requestMetadataValueRE = regexp.MustCompile(`^[a-zA-Z0-9\s:_@$#=/+,-.]{0,256}$`)
	guardrailIdentifierRE  = regexp.MustCompile(
		`^(|([a-z0-9]+)|(arn:aws(-[^:]+)?:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:guardrail/[a-z0-9]+))$`,
	)
	guardrailVersionRE = regexp.MustCompile(`^(|([1-9][0-9]{0,7})|(DRAFT))$`)
)

type guardrailConfigBody struct {
	GuardrailIdentifier  string `json:"guardrailIdentifier"`
	GuardrailVersion     string `json:"guardrailVersion"`
	Trace                string `json:"trace"`
	StreamProcessingMode string `json:"streamProcessingMode"`
}

// validateRequestMetadata applies the constraints in API_runtime_Converse.html (max 16 entries, key/value patterns).
func validateRequestMetadata(md map[string]string) string {
	if len(md) > maxRequestMetadataEntries {
		return "requestMetadata can have at most 16 entries"
	}

	for k, v := range md {
		if !requestMetadataKeyRE.MatchString(k) {
			return "requestMetadata key " + k + " does not satisfy the required pattern"
		}

		if !requestMetadataValueRE.MatchString(v) {
			return "requestMetadata value for key " + k + " does not satisfy the required pattern"
		}
	}

	return ""
}

// validateGuardrailConfig applies the patterns in API_runtime_GuardrailConfiguration.html and
// API_runtime_GuardrailStreamConfiguration.html.
func validateGuardrailConfig(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}

	var gc guardrailConfigBody
	if json.Unmarshal(raw, &gc) != nil {
		return ""
	}

	if !guardrailIdentifierRE.MatchString(gc.GuardrailIdentifier) {
		return "guardrailConfig.guardrailIdentifier does not satisfy the required pattern"
	}

	if !guardrailVersionRE.MatchString(gc.GuardrailVersion) {
		return "guardrailConfig.guardrailVersion must be a number or DRAFT"
	}

	if gc.Trace != "" && !slices.Contains([]string{"enabled", "disabled", "enabled_full"}, gc.Trace) {
		return "guardrailConfig.trace must be one of enabled, disabled, enabled_full"
	}

	if gc.StreamProcessingMode != "" && !slices.Contains([]string{"sync", "async"}, gc.StreamProcessingMode) {
		return "guardrailConfig.streamProcessingMode must be one of sync, async"
	}

	return ""
}
