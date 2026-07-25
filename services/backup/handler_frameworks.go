package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// controlInputParameterJSON is ControlInputParameter's wire shape: a single
// {ParameterName, ParameterValue} pair, NOT a map -- a control can repeat
// the same parameter name is not expected, but the wire type is an array
// exactly like AWS's own types.ControlInputParameter.
type controlInputParameterJSON struct {
	ParameterName  string `json:"ParameterName"`
	ParameterValue string `json:"ParameterValue"`
}

// controlScopeJSON is ControlScope's wire shape: a struct, NOT a free-form
// map -- matches AWS's types.ControlScope exactly.
type controlScopeJSON struct {
	Tags                    map[string]string `json:"Tags,omitempty"`
	ComplianceResourceIDs   []string          `json:"ComplianceResourceIds,omitempty"`
	ComplianceResourceTypes []string          `json:"ComplianceResourceTypes,omitempty"`
}

type frameworkControlJSON struct {
	ControlScope           *controlScopeJSON           `json:"ControlScope,omitempty"`
	ControlName            string                      `json:"ControlName"`
	ControlInputParameters []controlInputParameterJSON `json:"ControlInputParameters,omitempty"`
}

func controlScopeFromJSON(cs *controlScopeJSON) *ControlScope {
	if cs == nil {
		return nil
	}

	return &ControlScope{
		ComplianceResourceIDs:   cs.ComplianceResourceIDs,
		ComplianceResourceTypes: cs.ComplianceResourceTypes,
		Tags:                    cs.Tags,
	}
}

func controlScopeToJSON(cs *ControlScope) *controlScopeJSON {
	if cs == nil {
		return nil
	}

	return &controlScopeJSON{
		ComplianceResourceIDs:   cs.ComplianceResourceIDs,
		ComplianceResourceTypes: cs.ComplianceResourceTypes,
		Tags:                    cs.Tags,
	}
}

func frameworkControlsFromJSON(in []frameworkControlJSON) []FrameworkControl {
	out := make([]FrameworkControl, 0, len(in))
	for _, fc := range in {
		params := make([]ControlInputParameter, 0, len(fc.ControlInputParameters))
		for _, p := range fc.ControlInputParameters {
			params = append(params, ControlInputParameter(p))
		}
		out = append(out, FrameworkControl{
			ControlName:            fc.ControlName,
			ControlInputParameters: params,
			ControlScope:           controlScopeFromJSON(fc.ControlScope),
		})
	}

	return out
}

func frameworkControlsToJSON(in []FrameworkControl) []frameworkControlJSON {
	out := make([]frameworkControlJSON, 0, len(in))
	for _, fc := range in {
		params := make([]controlInputParameterJSON, 0, len(fc.ControlInputParameters))
		for _, p := range fc.ControlInputParameters {
			params = append(params, controlInputParameterJSON(p))
		}
		out = append(out, frameworkControlJSON{
			ControlName:            fc.ControlName,
			ControlInputParameters: params,
			ControlScope:           controlScopeToJSON(fc.ControlScope),
		})
	}

	return out
}

type createFrameworkBody struct {
	FrameworkName        string                 `json:"FrameworkName"`
	FrameworkDescription string                 `json:"FrameworkDescription,omitempty"`
	IdempotencyToken     string                 `json:"IdempotencyToken,omitempty"`
	FrameworkControls    []frameworkControlJSON `json:"FrameworkControls,omitempty"`
}

func (h *Handler) handleCreateFramework(c *echo.Context, body []byte) error {
	var in createFrameworkBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", "invalid request body"))
	}

	if in.FrameworkName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "FrameworkName is required"),
		)
	}

	controls := frameworkControlsFromJSON(in.FrameworkControls)
	f, err := h.Backend.CreateFramework(in.FrameworkName, in.FrameworkDescription, controls)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFrameworkArn:  f.FrameworkArn,
		keyFrameworkName: f.FrameworkName,
	})
}

func (h *Handler) handleDescribeFramework(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "FrameworkName is required"),
		)
	}

	f, err := h.Backend.DescribeFramework(name)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyFrameworkArn:        f.FrameworkArn,
		keyFrameworkName:       f.FrameworkName,
		"FrameworkDescription": f.FrameworkDescription,
		"FrameworkStatus":      f.FrameworkStatus,
		"DeploymentStatus":     f.DeploymentStatus,
		keyCreationTime:        epochSeconds(f.CreationTime),
	}
	if len(f.FrameworkControls) > 0 {
		resp["FrameworkControls"] = frameworkControlsToJSON(f.FrameworkControls)
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListFrameworks(c *echo.Context) error {
	frameworks := h.Backend.ListFrameworks()
	items := make([]map[string]any, 0, len(frameworks))

	for _, f := range frameworks {
		items = append(items, map[string]any{
			keyFrameworkArn:        f.FrameworkArn,
			keyFrameworkName:       f.FrameworkName,
			"FrameworkDescription": f.FrameworkDescription,
			keyCreationTime:        epochSeconds(f.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Frameworks": items,
	})
}

type updateFrameworkBody struct {
	FrameworkDescription string                 `json:"FrameworkDescription,omitempty"`
	IdempotencyToken     string                 `json:"IdempotencyToken,omitempty"`
	FrameworkControls    []frameworkControlJSON `json:"FrameworkControls,omitempty"`
}

func (h *Handler) handleUpdateFramework(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "FrameworkName is required"),
		)
	}

	var in updateFrameworkBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterValueException", "invalid request body"),
			)
		}
	}

	// Real AWS: FrameworkControls is optional on Update -- an omitted field
	// leaves the framework's existing controls untouched (nil here signals
	// "no change" to UpdateFramework, distinct from an explicit empty list).
	var controls *[]FrameworkControl
	if in.FrameworkControls != nil {
		fc := frameworkControlsFromJSON(in.FrameworkControls)
		controls = &fc
	}

	f, err := h.Backend.UpdateFramework(name, in.FrameworkDescription, controls)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFrameworkArn:  f.FrameworkArn,
		keyFrameworkName: f.FrameworkName,
	})
}

func (h *Handler) handleDeleteFramework(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("MissingParameterValueException", "FrameworkName is required"),
		)
	}

	if err := h.Backend.DeleteFramework(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Report plan read/update/delete handlers ---
