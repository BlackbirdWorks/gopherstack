package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Function handlers ---

func (h *Handler) handlePutFunction(c *echo.Context, functionID string, body map[string]any) error {
	functionType, _ := body["FunctionType"].(string)
	description, _ := body["Description"].(string)
	tags := extractTags(body)

	fn, err := h.Backend.PutFunction(functionID, functionType, description, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toFunctionOutput(fn))
}

func (h *Handler) handleGetFunction(c *echo.Context, functionID string) error {
	fn, err := h.Backend.GetFunction(functionID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toFunctionOutput(fn))
}

func (h *Handler) handleDeleteFunction(c *echo.Context, functionID string) error {
	if err := h.Backend.DeleteFunction(functionID); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListFunctions(c *echo.Context) error {
	maxResults, nextToken := extractPaginationParams(c)

	summaries, nextToken, err := h.Backend.ListFunctions(maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			"FunctionId":   s.FunctionID,
			"FunctionType": s.FunctionType,
			keyArn:         s.ARN,
			keyTags:        nilToEmpty(s.Tags),
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toFunctionOutput(fn *Function) map[string]any {
	return map[string]any{
		"FunctionId":   fn.FunctionID,
		"FunctionType": fn.FunctionType,
		keyArn:         fn.ARN,
		"Description":  fn.Description,
		keyTags:        nilToEmpty(fn.Tags),
	}
}
