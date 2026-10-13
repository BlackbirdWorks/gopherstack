package securityhub

import (
	"errors"
	"net/http"
	"regexp"
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
		return typedErrorResponse(c, http.StatusBadRequest, "InvalidInputException", msgNameRequired)
	}

	if description == "" {
		return typedErrorResponse(c, http.StatusBadRequest, "InvalidInputException", "Description is required")
	}

	if id == "" {
		return typedErrorResponse(c, http.StatusBadRequest, "InvalidInputException", "Id is required")
	}

	if msg := validateActionTargetInput(name, id); msg != "" {
		return typedErrorResponse(c, http.StatusBadRequest, "InvalidInputException", msg)
	}

	arn, err := h.Backend.CreateActionTarget(name, description, id)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return hubNotSubscribed(c)
		}

		if errors.Is(err, ErrAlreadyExists) {
			return typedErrorResponse(c, http.StatusConflict, "ResourceConflictException", err.Error())
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalException", err.Error())
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
		if errors.Is(err, ErrHubNotEnabled) {
			return hubNotSubscribed(c)
		}

		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "ActionTarget not found")
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalException", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteActionTarget(c *echo.Context, actionTargetArn string) error {
	deletedArn, err := h.Backend.DeleteActionTarget(actionTargetArn)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return hubNotSubscribed(c)
		}

		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "ActionTarget not found")
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalException", err.Error())
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

const maxActionTargetField = 20

var actionTargetIDRe = regexp.MustCompile(`^[A-Za-z0-9]+$`)

// validateActionTargetInput applies CreateActionTarget's documented limits: a name of at most 20 characters and an
// alphanumeric Id of at most 20 characters (api_op_CreateActionTarget.go).
func validateActionTargetInput(name, id string) string {
	if len(name) > maxActionTargetField {
		return "Name can contain up to 20 characters"
	}

	if len(id) > maxActionTargetField || !actionTargetIDRe.MatchString(id) {
		return "Id can contain up to 20 alphanumeric characters"
	}

	return ""
}
