package securityhub

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func classifyActionTargetsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/actionTargets":
		return opCreateActionTarget, ""
	case method == http.MethodPost && path == "/actionTargets/get":
		return opDescribeActionTargets, ""
	case strings.HasPrefix(path, "/actionTargets/") && method == http.MethodPatch:
		return opUpdateActionTarget, strings.TrimPrefix(path, "/actionTargets/")
	case strings.HasPrefix(path, "/actionTargets/") && method == http.MethodDelete:
		return opDeleteActionTarget, strings.TrimPrefix(path, "/actionTargets/")
	}

	return opUnknown, ""
}

func (h *Handler) handleCreateActionTarget(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	description, _ := body["Description"].(string)
	id, _ := body["Id"].(string)

	if name == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "Name is required"})
	}

	if description == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "Description is required"})
	}

	if id == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "Id is required"})
	}

	arn, err := h.Backend.CreateActionTarget(name, description, id)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubNotEnabled})
		}

		return c.JSON(http.StatusConflict, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{keyActionTargetArn: arn})
}

func (h *Handler) handleDescribeActionTargets(c *echo.Context, body map[string]any) error {
	rawArns, _ := body["ActionTargetArns"].([]any)

	var arns []string

	for _, a := range rawArns {
		if s, ok := a.(string); ok {
			arns = append(arns, s)
		}
	}

	nextToken, _ := body["NextToken"].(string)
	maxResults := intFromBody(body)

	targets, nextOut := h.Backend.DescribeActionTargets(arns, nextToken, maxResults)
	items := make([]map[string]any, len(targets))

	for i, t := range targets {
		items[i] = map[string]any{
			keyActionTargetArn: t.ActionTargetArn,
			keyName:            t.Name,
			keyDescription:     t.Description,
		}
	}

	resp := map[string]any{"ActionTargets": items}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateActionTarget(c *echo.Context, actionTargetArn string, body map[string]any) error {
	name, _ := body["Name"].(string)
	description, _ := body["Description"].(string)

	if err := h.Backend.UpdateActionTarget(actionTargetArn, name, description); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "ActionTarget not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteActionTarget(c *echo.Context, actionTargetArn string) error {
	deletedArn, err := h.Backend.DeleteActionTarget(actionTargetArn)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "ActionTarget not found"})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{keyActionTargetArn: deletedArn})
}

// actionTargetsOpHandlers returns the Action Targets operation dispatch
// table for handleREST.
func (h *Handler) actionTargetsOpHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opCreateActionTarget:    func() error { return h.handleCreateActionTarget(c, body) },
		opDescribeActionTargets: func() error { return h.handleDescribeActionTargets(c, body) },
		opUpdateActionTarget:    func() error { return h.handleUpdateActionTarget(c, resource, body) },
		opDeleteActionTarget:    func() error { return h.handleDeleteActionTarget(c, resource) },
	}
}
