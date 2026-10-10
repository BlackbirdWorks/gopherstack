package bedrockruntime

import (
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"strings"
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

var (
	toolNameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
	modelIDRE  = regexp.MustCompile(`^[a-zA-Z0-9_:.-]+$`)
)

const (
	maxModelIDLen = 2048
	roleUser      = "user"
)

// validateModelID applies the ConversationalModelId/InvokeModelIdentifier length and
// character constraints; ARNs are accepted as-is.
func validateModelID(id string) string {
	if len(id) > maxModelIDLen {
		return "modelId must be at most 2048 characters"
	}

	if strings.HasPrefix(id, "arn:") || modelIDRE.MatchString(id) {
		return ""
	}

	return "The provided model identifier is invalid."
}

// validateConverseMessages enforces the conversation shape Bedrock requires: roles are
// user/assistant, the first message is from the user, roles alternate, and no message is empty.
func validateConverseMessages(messages []converseMessage) string {
	for i, m := range messages {
		if m.Role != roleUser && m.Role != roleAssistant {
			return "messages." + strconv.Itoa(i) + ".role must be one of user, assistant"
		}

		if len(m.Content) == 0 {
			return "The content field in the Message object at messages." + strconv.Itoa(i) +
				" is empty. Add a ContentBlock object to the content field and try again."
		}

		if i == 0 && m.Role != roleUser {
			return "A conversation must start with a user message. " +
				"Try again with a conversation that starts with a user message."
		}

		if i > 0 && m.Role == messages[i-1].Role {
			return "A conversation must alternate between user and assistant roles. " +
				"Make sure the conversation alternates between user and assistant roles and try again."
		}
	}

	return ""
}

type inferenceConfigBody struct {
	MaxTokens     *int     `json:"maxTokens"`
	Temperature   *float64 `json:"temperature"`
	TopP          *float64 `json:"topP"`
	StopSequences []string `json:"stopSequences"`
}

// validateInferenceConfig applies the InferenceConfiguration bounds.
func validateInferenceConfig(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}

	var ic inferenceConfigBody
	if json.Unmarshal(raw, &ic) != nil {
		return ""
	}

	switch {
	case ic.MaxTokens != nil && *ic.MaxTokens < 1:
		return "inferenceConfig.maxTokens must be at least 1"
	case ic.Temperature != nil && (*ic.Temperature < 0 || *ic.Temperature > 1):
		return "inferenceConfig.temperature must be between 0 and 1"
	case ic.TopP != nil && (*ic.TopP < 0 || *ic.TopP > 1):
		return "inferenceConfig.topP must be between 0 and 1"
	}

	if slices.Contains(ic.StopSequences, "") {
		return "inferenceConfig.stopSequences entries must not be empty"
	}

	return ""
}

type toolConfigBody struct {
	ToolChoice map[string]json.RawMessage `json:"toolChoice"`
	Tools      []struct {
		ToolSpec *struct {
			Name        string          `json:"name"`
			InputSchema json.RawMessage `json:"inputSchema"`
		} `json:"toolSpec"`
	} `json:"tools"`
}

type specificToolChoice struct {
	Name string `json:"name"`
}

// validateToolConfig applies the ToolConfiguration shape: at least one tool, valid tool names,
// a single-member toolChoice union, and a specific choice that names a declared tool.
func validateToolConfig(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}

	var tc toolConfigBody
	if json.Unmarshal(raw, &tc) != nil {
		return ""
	}

	if len(tc.Tools) == 0 {
		return "toolConfig.tools must contain at least 1 tool"
	}

	names := make(map[string]bool, len(tc.Tools))

	for i, t := range tc.Tools {
		if t.ToolSpec == nil {
			continue
		}

		if !toolNameRE.MatchString(t.ToolSpec.Name) {
			return "toolConfig.tools." + strconv.Itoa(i) + ".toolSpec.name must match [a-zA-Z0-9_-]{1,64}"
		}

		if len(t.ToolSpec.InputSchema) == 0 {
			return "toolConfig.tools." + strconv.Itoa(i) + ".toolSpec.inputSchema is required"
		}

		names[t.ToolSpec.Name] = true
	}

	return validateToolChoice(tc.ToolChoice, names)
}

func validateToolChoice(choice map[string]json.RawMessage, declared map[string]bool) string {
	if len(choice) == 0 {
		return ""
	}

	if len(choice) != 1 {
		return "toolConfig.toolChoice must set exactly one of auto, any, tool"
	}

	if raw, ok := choice["tool"]; ok {
		var st specificToolChoice
		if json.Unmarshal(raw, &st) == nil && !declared[st.Name] {
			return "toolConfig.toolChoice.tool.name " + st.Name + " does not match a declared tool"
		}

		return ""
	}

	if _, auto := choice["auto"]; !auto {
		if _, anyChoice := choice["any"]; !anyChoice {
			return "toolConfig.toolChoice must set exactly one of auto, any, tool"
		}
	}

	return ""
}
