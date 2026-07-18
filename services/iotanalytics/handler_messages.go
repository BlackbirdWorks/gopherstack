package iotanalytics

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleBatchPutMessage(c *echo.Context, body []byte) error {
	var req batchPutMessageRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if req.ChannelName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "channelName is required")
	}

	if len(req.Messages) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "messages must not be empty")
	}

	if len(req.Messages) > maxBatchMessages {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException",
			"messages count exceeds limit of "+strconv.Itoa(maxBatchMessages))
	}

	for i, msg := range req.Messages {
		if msg.MessageID == "" {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"messageId is required for message at index "+strconv.Itoa(i),
			)
		}

		if len(msg.Payload) == 0 {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"payload must not be empty for message "+msg.MessageID,
			)
		}
	}

	errs, err := h.Backend.BatchPutMessage(req.ChannelName, req.Messages)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalFailureException", err.Error())
	}

	return c.JSON(http.StatusOK, batchPutMessageResponse{BatchPutMessageErrorEntries: errs})
}
