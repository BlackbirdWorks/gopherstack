package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Batch handlers ---

// toBatchResultOutput mirrors BatchStartOutput/BatchStopOutput/
// BatchDeleteOutput exactly (wrapper keys "successful"/"failed", item
// keys "arn"/"id"/"state" and "arn"/"id"/"code"/"message").
func toBatchResultOutput(result *BatchResult) map[string]any {
	successful := make([]map[string]any, 0, len(result.Successful))
	for _, s := range result.Successful {
		successful = append(
			successful,
			map[string]any{keyArn: s.Arn, keyID: s.ID, keyState: s.State},
		)
	}
	failed := make([]map[string]any, 0, len(result.Failed))
	for _, f := range result.Failed {
		failed = append(
			failed,
			map[string]any{keyArn: f.Arn, keyID: f.ID, "code": f.Code, keyLowerMessage: f.Message},
		)
	}

	return map[string]any{"successful": successful, "failed": failed}
}

// handleBatchStart parses BatchStartInput. Real field names, verified
// against aws-sdk-go-v2/service/medialive's api_op_BatchStart.go and
// serializers.go: channelIds, multiplexIds. There is NO inputIds field.
func (h *Handler) handleBatchStart(c *echo.Context, body map[string]any) error {
	channelIDs := extractStringSlice(body, "channelIds")
	multiplexIDs := extractStringSlice(body, "multiplexIds")
	result, err := h.Backend.BatchStart(channelIDs, multiplexIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toBatchResultOutput(result))
}

// handleBatchStop parses BatchStopInput -- same shape as BatchStartInput
// (channelIds, multiplexIds; no inputIds).
func (h *Handler) handleBatchStop(c *echo.Context, body map[string]any) error {
	channelIDs := extractStringSlice(body, "channelIds")
	multiplexIDs := extractStringSlice(body, "multiplexIds")
	result, err := h.Backend.BatchStop(channelIDs, multiplexIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toBatchResultOutput(result))
}

// handleBatchDelete parses BatchDeleteInput. Unlike BatchStart/BatchStop,
// this shape has all four ID lists: channelIds, inputIds, multiplexIds,
// AND inputSecurityGroupIds (verified against api_op_BatchDelete.go).
func (h *Handler) handleBatchDelete(c *echo.Context, body map[string]any) error {
	channelIDs := extractStringSlice(body, "channelIds")
	inputIDs := extractStringSlice(body, "inputIds")
	multiplexIDs := extractStringSlice(body, "multiplexIds")
	inputSecurityGroupIDs := extractStringSlice(body, "inputSecurityGroupIds")
	result, err := h.Backend.BatchDelete(channelIDs, inputIDs, multiplexIDs, inputSecurityGroupIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toBatchResultOutput(result))
}
