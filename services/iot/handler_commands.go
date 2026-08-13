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
	// Real ListCommandExecutions route (iot@v1.77.4 serializers.go:13785):
	// POST /command-executions, filters travel in the JSON body. The
	// nested "/commands/{commandId}/executions" case below predates this
	// and stays wired for backward compatibility with existing callers.
	if path == pathCommandExecutions && method == http.MethodPost {
		return opListCommandExecutions
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
		"commandId":   cmd.CommandID,
		keyCommandArn: cmd.CommandARN,
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

// commandSummaryFields renders the fields of cmd that types.CommandSummary
// (types.go:1504-1527, iot@v1.77.4) declares: CommandArn, CommandId,
// CreatedAt, Deprecated, DisplayName, LastUpdatedAt, PendingDeletion.
func commandSummaryFields(cmd *IoTCommand) map[string]any {
	return map[string]any{
		keyCommandArn:     cmd.CommandARN,
		"commandId":       cmd.CommandID,
		"createdAt":       cmd.CreationDate,
		"deprecated":      cmd.Deprecated,
		"displayName":     cmd.DisplayName,
		"lastUpdatedAt":   cmd.LastUpdated,
		"pendingDeletion": cmd.PendingDeletion,
	}
}

func (h *Handler) handleListCommands(c *echo.Context) error {
	items := h.Backend.ListCommands()
	out := make([]map[string]any, 0, len(items))
	for _, cmd := range items {
		out = append(out, commandSummaryFields(cmd))
	}

	return c.JSON(http.StatusOK, map[string]any{"commands": out})
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

// commandExecutionSummaryFields renders the fields of ex that
// types.CommandExecutionSummary (types.go:1327-1352, iot@v1.77.4) declares:
// CommandArn, CompletedAt, CreatedAt, ExecutionId, StartedAt, Status,
// TargetArn. CompletedAt and StartedAt are deliberately left absent: this
// backend has no StartCommandExecution/UpdateCommandExecution control-plane
// op (executions only arrive via AddCommandExecutionInternal test seeding,
// or DeleteCommandExecution/GetCommandExecution reads), so there is no
// honest source for a start time or completion time distinct from
// CreatedAt.
func commandExecutionSummaryFields(ex *IoTCommandExecution) map[string]any {
	return map[string]any{
		keyCommandArn: ex.CommandARN,
		"executionId": ex.ExecutionID,
		"targetArn":   ex.ThingARN,
		keyStatus:     ex.Status,
		"createdAt":   ex.CreationDate,
	}
}

func (h *Handler) handleListCommandExecutions(c *echo.Context) error {
	var items []*IoTCommandExecution

	if c.Request().URL.Path == pathCommandExecutions {
		// Real route: POST /command-executions, filters in the JSON body
		// (iot@v1.77.4 serializers.go:13840, awsRestjson1_serializeOpDocumentListCommandExecutionsInput).
		var body struct {
			CommandArn string `json:"commandArn"`
			TargetArn  string `json:"targetArn"`
			Status     string `json:"status"`
		}
		if err := readBody(c, &body); err != nil {
			return err
		}
		items = h.Backend.ListCommandExecutionsByFilter(body.CommandArn, body.TargetArn, body.Status)
	} else {
		// Legacy nested route: /commands/{commandId}/executions.
		trimmed := strings.TrimPrefix(c.Request().URL.Path, "/commands/")
		commandID := strings.TrimSuffix(trimmed, "/executions")
		items = h.Backend.ListCommandExecutions(commandID)
	}

	out := make([]map[string]any, 0, len(items))
	for _, ex := range items {
		out = append(out, commandExecutionSummaryFields(ex))
	}

	return c.JSON(http.StatusOK, map[string]any{"commandExecutions": out})
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
