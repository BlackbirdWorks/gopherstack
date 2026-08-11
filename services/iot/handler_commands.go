package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func resolveCommandOps(path, method string) string {
	if path == "/commands" && method == http.MethodGet {
		return opListCommands
	}
	if rest, ok := strings.CutPrefix(path, "/commands/"); ok {
		return resolveCommandSubPathOps(strings.SplitN(rest, "/", pathSplitThree), method)
	}

	return unknownOperation
}

// resolveCommandSubPathOps resolves the /commands/{commandId}[/executions[/{executionId}]]
// sub-routes once the "/commands/" prefix has been stripped and split on "/".
func resolveCommandSubPathOps(parts []string, method string) string {
	switch len(parts) {
	case 1:
		switch method {
		case http.MethodPut:

			return opCreateCommand
		case http.MethodGet:

			return opGetCommand
		case http.MethodDelete:

			return opDeleteCommand
		case http.MethodPatch:

			return opUpdateCommand
		}
	case pathSplitTwo:
		if parts[1] == pathSegmentExecutions && method == http.MethodGet {
			return opListCommandExecutions
		}
	case pathSplitThree:
		if parts[1] == pathSegmentExecutions {
			switch method {
			case http.MethodGet:
				return opGetCommandExecution
			case http.MethodDelete:
				return opDeleteCommandExecution
			}
		}
	}

	return unknownOperation
}

func (h *Handler) handleCreateCommand(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	var req struct {
		Payload     map[string]any `json:"payload"`
		DisplayName string         `json:"displayName"`
		Description string         `json:"description"`
		Namespace   string         `json:"namespace"`
		Tags        []tags.KV      `json:"tags"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	cmd, err := h.Backend.CreateCommand(
		id, req.DisplayName, req.Description, req.Namespace, req.Payload, tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"commandId":  cmd.CommandID,
		"commandArn": cmd.CommandARN,
	})
}

func (h *Handler) handleGetCommand(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	cmd, err := h.Backend.GetCommand(id)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, cmd)
}

func (h *Handler) handleUpdateCommand(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	var req struct {
		DisplayName string `json:"displayName"`
		Description string `json:"description"`
		Deprecated  bool   `json:"deprecated"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateCommand(id, req.DisplayName, req.Description, req.Deprecated); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteCommand(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	if err := h.Backend.DeleteCommand(id); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListCommands(c *echo.Context) error {
	items := h.Backend.ListCommands()

	return c.JSON(http.StatusOK, map[string]any{"commands": items})
}

func (h *Handler) handleGetCommandExecution(c *echo.Context) error {
	// /commands/{commandId}/executions/{executionId}
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	parts := strings.SplitN(trimmed, "/executions/", pathSplitTwo)
	if len(parts) != pathSplitTwo {
		return respondNotFound(c, "command execution not found")
	}
	ex, err := h.Backend.GetCommandExecution(parts[0], parts[1])
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, ex)
}

func (h *Handler) handleListCommandExecutions(c *echo.Context) error {
	// /commands/{commandId}/executions
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
	commandID := strings.TrimSuffix(trimmed, "/executions")
	items := h.Backend.ListCommandExecutions(commandID)

	return c.JSON(http.StatusOK, map[string]any{"executions": items})
}

func (h *Handler) handleDeleteCommandExecution(c *echo.Context) error {
	executionID := strings.TrimPrefix(c.Request().URL.Path, pathCommandExecutions+"/")
	targetARN := c.QueryParam("targetArn")

	if err := h.Backend.DeleteCommandExecution(executionID, targetARN); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) dispatchCommandOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateCommand:
		return true, h.handleCreateCommand(c)
	case opGetCommand:
		return true, h.handleGetCommand(c)
	case opUpdateCommand:
		return true, h.handleUpdateCommand(c)
	case opDeleteCommand:
		return true, h.handleDeleteCommand(c)
	case opListCommands:
		return true, h.handleListCommands(c)
	case opGetCommandExecution:
		return true, h.handleGetCommandExecution(c)
	case opListCommandExecutions:
		return true, h.handleListCommandExecutions(c)
	}

	return false, nil
}
