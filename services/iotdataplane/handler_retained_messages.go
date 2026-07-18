package iotdataplane

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// handleGetRetainedMessage processes GET /retainedMessage/{topic} requests.
func (h *Handler) handleGetRetainedMessage(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}

	topic := strings.TrimPrefix(c.Request().URL.Path, retainedMessagePathSlash)
	if topic == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "topic is required"})
	}

	msg, err := h.Backend.GetRetainedMessage(topic)
	if err != nil {
		return h.handleError(c, err)
	}

	// Typed response per AWS RetainedMessage shape; payload is base64 encoded by json.Marshal.
	resp := map[string]any{
		"topic":            msg.Topic,
		"payload":          msg.Payload,
		"qos":              msg.Qos,
		"lastModifiedTime": msg.LastModifiedTime,
	}

	return c.JSON(http.StatusOK, resp)
}

// handleListRetainedMessages processes GET /retainedMessage requests.
// Pagination: pageSize (primary) or maxResults (alias); default 25, max 100.
// AWS RetainedMessageSummary does NOT include qos.
func (h *Handler) handleListRetainedMessages(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.JSON(http.StatusMethodNotAllowed, map[string]string{keyError: errMethodNotAllowed})
	}

	msgs, err := h.Backend.ListRetainedMessages()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{keyError: err.Error()})
	}

	q := c.Request().URL.Query()
	nextTokenIn := q.Get("nextToken")
	pageSize := parsePageSize(q)

	// Extract topic strings for cursor lookup.
	topics := make([]string, len(msgs))
	for i, m := range msgs {
		topics[i] = m.Topic
	}

	startIdx := findCursorIndex(topics, nextTokenIn)
	end := min(startIdx+pageSize, len(msgs))

	page := msgs[startIdx:end]

	// AWS RetainedMessageSummary: {topic, payloadSize, lastModifiedTime} — qos excluded.
	summaries := make([]map[string]any, 0, len(page))
	for _, msg := range page {
		summaries = append(summaries, map[string]any{
			"topic":            msg.Topic,
			"payloadSize":      int64(len(msg.Payload)),
			"lastModifiedTime": msg.LastModifiedTime,
		})
	}

	resp := map[string]any{
		"retainedTopics": summaries,
	}

	if end < len(msgs) {
		resp["nextToken"] = msgs[end].Topic
	}

	return c.JSON(http.StatusOK, resp)
}
