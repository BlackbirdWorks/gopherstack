package bedrockruntime

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// applyGuardrailRequest represents the parsed ApplyGuardrail request body.
type applyGuardrailRequest struct {
	Source  string                 `json:"source"`
	Content []guardrailContentItem `json:"content"`
}

// guardrailContentItem represents a single item in the guardrail content list.
type guardrailContentItem struct {
	Text *guardrailTextBlock `json:"text,omitempty"`
}

// guardrailTextBlock holds the text for a guardrail content item.
type guardrailTextBlock struct {
	Text string `json:"text"`
}

// guardrailBlockPatterns are keywords that trigger BLOCKED action in the guardrail mock.
// Deterministic behavior allows tests to verify BLOCKED vs NONE outcomes.
const (
	guardrailPatternBlocked = "blocked"
	guardrailPatternHarmful = "harmful"
	guardrailPatternToxic   = "toxic"
	guardrailPatternUnsafe  = "unsafe"
)

// evaluateGuardrailAction returns "BLOCKED" if the content matches known trigger patterns.
func evaluateGuardrailAction(content []guardrailContentItem) string {
	for _, item := range content {
		if item.Text == nil {
			continue
		}

		lower := strings.ToLower(item.Text.Text)

		for _, kw := range []string{
			guardrailPatternBlocked,
			guardrailPatternHarmful,
			guardrailPatternToxic,
			guardrailPatternUnsafe,
		} {
			if strings.Contains(lower, kw) {
				return "BLOCKED"
			}
		}
	}

	return "NONE"
}

// handleApplyGuardrail handles POST /guardrail/{guardrailIdentifier}/version/{guardrailVersion}/apply.
func (h *Handler) handleApplyGuardrail(
	c *echo.Context,
	path string,
	body []byte,
) error {
	guardrailID, guardrailVersion := extractGuardrailIDAndVersion(path)

	if guardrailID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "guardrailIdentifier is required"))
	}

	if guardrailVersion == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "guardrailVersion is required"))
	}

	var req applyGuardrailRequest
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	action := evaluateGuardrailAction(req.Content)

	resp := map[string]any{
		"action":      action,
		"assessments": []map[string]any{},
		"outputs":     buildGuardrailOutputs(req),
		keyUsage: map[string]any{
			"topicPolicyUnits":                    0,
			"contentPolicyUnits":                  0,
			"wordPolicyUnits":                     0,
			"sensitiveInformationPolicyUnits":     0,
			"sensitiveInformationPolicyFreeUnits": 0,
			"contextualGroundingPolicyUnits":      0,
		},
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	h.Backend.RecordInvocation("ApplyGuardrail", guardrailID+"/"+guardrailVersion, string(body), string(out))

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusOK, out)
}

// buildGuardrailOutputs reflects filtered content back in the outputs list.
// For NONE action, outputs mirror the input content unchanged.
func buildGuardrailOutputs(req applyGuardrailRequest) []map[string]any {
	if len(req.Content) == 0 {
		return []map[string]any{}
	}

	outputs := make([]map[string]any, 0, len(req.Content))

	for _, item := range req.Content {
		if item.Text != nil {
			outputs = append(outputs, map[string]any{
				keyText: item.Text.Text,
			})
		}
	}

	return outputs
}
