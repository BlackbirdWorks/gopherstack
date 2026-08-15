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
	customOutput, _ := body["CustomOutputConfiguration"].(map[string]any)
	httpRequest, _ := body["HttpRequestConfiguration"].(map[string]any)
	sequentialExecutor, _ := body["SequentialExecutorConfiguration"].(map[string]any)

	fn, err := h.Backend.PutFunction(
		functionID, functionType, description, customOutput, httpRequest, sequentialExecutor, tags,
	)
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
		// ListFunctionsOutput.Items is []types.Function, the same full type
		// GetFunction returns, so Description and all three FunctionType
		// configs belong on every list item too.
		out = append(out, toFunctionOutput(&Function{
			FunctionID:                      s.FunctionID,
			FunctionType:                    s.FunctionType,
			ARN:                             s.ARN,
			Description:                     s.Description,
			Tags:                            s.Tags,
			CustomOutputConfiguration:       s.CustomOutputConfiguration,
			HTTPRequestConfiguration:        s.HTTPRequestConfiguration,
			SequentialExecutorConfiguration: s.SequentialExecutorConfiguration,
		}))
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toFunctionOutput(fn *Function) map[string]any {
	out := map[string]any{
		"FunctionId":   fn.FunctionID,
		"FunctionType": fn.FunctionType,
		keyArn:         fn.ARN,
		"Description":  fn.Description,
		keyTags:        nilToEmpty(fn.Tags),
	}

	if fn.CustomOutputConfiguration != nil {
		out["CustomOutputConfiguration"] = fn.CustomOutputConfiguration
	}

	if fn.HTTPRequestConfiguration != nil {
		out["HttpRequestConfiguration"] = fn.HTTPRequestConfiguration
	}

	if fn.SequentialExecutorConfiguration != nil {
		out["SequentialExecutorConfiguration"] = fn.SequentialExecutorConfiguration
	}

	return out
}
