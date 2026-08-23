package iotanalytics

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateChannel(c *echo.Context, body []byte) error {
	var req createChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if req.ChannelName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "channelName is required")
	}

	if err := validateTags(req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	tags := tagsToMap(req.Tags)

	ch, err := h.Backend.CreateChannel(
		c.Request().Context(), req.ChannelName, tags, req.ChannelStorage, req.RetentionPeriod)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createChannelResponse{
		ChannelName:     ch.Name,
		ChannelARN:      ch.ARN,
		RetentionPeriod: ch.RetentionPeriod,
	})
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	maxResults, cursor := parsePagination(c)
	channels := h.Backend.ListChannels()

	summaries := make([]channelSummary, 0, len(channels))
	var nextToken *string

	count := 0

	for _, ch := range channels {
		if cursor != "" && ch.Name <= cursor {
			continue
		}

		if count >= maxResults {
			tok := encodeNextToken(summaries[len(summaries)-1].ChannelName)
			nextToken = &tok

			break
		}

		summaries = append(summaries, channelSummary{
			ChannelName:            ch.Name,
			ChannelStorage:         ch.Storage,
			Status:                 ch.Status,
			CreationTime:           ch.CreationTime,
			LastUpdateTime:         ch.LastUpdate,
			LastMessageArrivalTime: ch.LastMessageArrivalTime,
		})
		count++
	}

	return c.JSON(http.StatusOK, listChannelsResponse{
		ChannelSummaries: summaries,
		NextToken:        nextToken,
	})
}

func (h *Handler) handleDescribeChannel(c *echo.Context, name string) error {
	ch, err := h.Backend.DescribeChannel(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	detail := channelDetail{
		Storage:                ch.Storage,
		RetentionPeriod:        ch.RetentionPeriod,
		Tags:                   mapToTagsSorted(ch.Tags),
		Name:                   ch.Name,
		ARN:                    ch.ARN,
		Status:                 ch.Status,
		CreationTime:           ch.CreationTime,
		LastUpdateTime:         ch.LastUpdate,
		LastMessageArrivalTime: ch.LastMessageArrivalTime,
	}

	resp := describeChannelResponse{Channel: detail}

	if c.Request().URL.Query().Get("includeStatistics") == "true" {
		resp.Statistics = &channelStatistics{
			Size: &channelStatisticsSize{
				EstimatedSizeInBytes: 0,
				EstimatedOn:          ch.LastUpdate,
			},
		}
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateChannel(c *echo.Context, name string, body []byte) error {
	var req updateChannelRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"invalid request body: "+err.Error(),
			)
		}
	}

	if err := h.Backend.UpdateChannel(name, req.ChannelStorage, req.RetentionPeriod); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteChannel(c *echo.Context, name string) error {
	if err := h.Backend.DeleteChannel(name); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleSampleChannelData(c *echo.Context, channelName string) error {
	maxMessages := defaultSampleMessages

	if s := c.Request().URL.Query().Get("maxMessages"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxMessages = n
		}
	}

	payloads, err := h.Backend.SampleChannelData(channelName, maxMessages)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, sampleChannelDataResponse{Payloads: payloads})
}
