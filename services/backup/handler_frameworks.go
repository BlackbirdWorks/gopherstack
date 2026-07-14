package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type frameworkControlJSON struct {
	ControlInputParameters map[string]string `json:"ControlInputParameters,omitempty"`
	ControlScope           map[string]any    `json:"ControlScope,omitempty"`
	ControlName            string            `json:"ControlName"`
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
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.FrameworkName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "FrameworkName is required"),
		)
	}

	controls := make([]FrameworkControl, 0, len(in.FrameworkControls))
	for _, fc := range in.FrameworkControls {
		controls = append(controls, FrameworkControl(fc))
	}
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
			errResp("ValidationException", "FrameworkName is required"),
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
		controls := make([]map[string]any, 0, len(f.FrameworkControls))
		for _, fc := range f.FrameworkControls {
			c2 := map[string]any{"ControlName": fc.ControlName}
			if len(fc.ControlInputParameters) > 0 {
				c2["ControlInputParameters"] = fc.ControlInputParameters
			}
			if fc.ControlScope != nil {
				c2["ControlScope"] = fc.ControlScope
			}
			controls = append(controls, c2)
		}
		resp["FrameworkControls"] = controls
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
	FrameworkDescription string `json:"FrameworkDescription,omitempty"`
	IdempotencyToken     string `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleUpdateFramework(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "FrameworkName is required"),
		)
	}

	var in updateFrameworkBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	f, err := h.Backend.UpdateFramework(name, in.FrameworkDescription)
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
			errResp("ValidationException", "FrameworkName is required"),
		)
	}

	if err := h.Backend.DeleteFramework(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Report plan read/update/delete handlers ---
