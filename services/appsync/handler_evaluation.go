package appsync

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleDataplaneEvaluations handles /v1/dataplane-evaluations/{template|code}.
func (h *Handler) handleDataplaneEvaluations(ctx context.Context, c *echo.Context, segs []string) error {
	if len(segs) != pathSegsAPIID {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	if c.Request().Method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	switch segs[2] {
	case "template":
		return h.evaluateMappingTemplate(ctx, c)
	case keyCode:
		return h.evaluateCode(ctx, c)
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// evaluateMappingTemplate handles POST /v1/dataplane-evaluations/template.
func (h *Handler) evaluateMappingTemplate(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Template string `json:"template"`
		Context  string `json:"context"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	out, logs, evalErr := h.Backend.EvaluateMappingTemplateLogs(input.Template, input.Context)
	if re, ok := errors.AsType[*resolverError](evalErr); ok {
		return evalErrorResponse(c, re, logs)
	}

	if evalErr != nil {
		return h.handleError(ctx, c, "EvaluateMappingTemplate", evalErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"evaluationResult": out, "logs": nonNilLogs(logs)})
}

// evaluateCode handles POST /v1/dataplane-evaluations/code.
func (h *Handler) evaluateCode(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Code     string `json:"code"`
		Context  string `json:"context"`
		Function string `json:"function"`
		Runtime  struct {
			Name string `json:"name"`
		} `json:"runtime"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	out, logs, evalErr := h.Backend.EvaluateCodeLogs(input.Code, input.Context, input.Function, input.Runtime.Name)
	if re, ok := errors.AsType[*resolverError](evalErr); ok {
		return evalErrorResponse(c, re, logs)
	}

	if evalErr != nil {
		return h.handleError(ctx, c, "EvaluateCode", evalErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"evaluationResult": out, "logs": nonNilLogs(logs)})
}

// evalErrorResponse reports a template/code error the way the Evaluate APIs do:
// a 200 with an error member rather than an HTTP failure.
func evalErrorResponse(c *echo.Context, re *resolverError, logs []string) error {
	return c.JSON(http.StatusOK, map[string]any{
		rtMsgError: map[string]any{keyMessage: re.Message}, "logs": nonNilLogs(logs),
	})
}

func nonNilLogs(logs []string) []string {
	if logs == nil {
		return []string{}
	}

	return logs
}
