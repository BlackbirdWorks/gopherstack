package directoryservice

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleRegisterEventTopic(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: keyTopicName,
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.RegisterEventTopic(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleDeregisterEventTopic(c *echo.Context) error {
	return h.handleTwoFieldOp(c, twoFieldOp{
		secondKey: keyTopicName,
		invoke: func(ctx context.Context, dirID, second string) error {
			return h.Backend.DeregisterEventTopic(ctx, dirID, second)
		},
	})
}

func (h *Handler) handleDescribeEventTopics(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string   `json:"DirectoryId"`
		TopicNames  []string `json:"TopicNames"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	topics, descErr := h.Backend.DescribeEventTopics(h.contextWithRegion(c), req.DirectoryID, req.TopicNames)
	if descErr != nil {
		return h.mapError(c, descErr)
	}

	topicList := make([]map[string]any, 0, len(topics))
	for _, t := range topics {
		topicList = append(topicList, map[string]any{
			keyDirectoryID:    t.DirectoryID,
			"TopicName":       t.TopicName,
			"TopicArn":        t.TopicARN,
			keyStatus:         t.Status,
			"CreatedDateTime": awstime.Epoch(t.CreatedDateTime), //nolint:goconst // existing issue.
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"EventTopics": topicList})
}
