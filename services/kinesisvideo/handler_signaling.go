package kinesisvideo

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) buildSignalingOps() map[string]handlerFunc {
	return map[string]handlerFunc{
		pathCreateSignalingChannel:   h.handleCreateSignalingChannel,
		pathDescribeSignalingChannel: h.handleDescribeSignalingChannel,
		pathListSignalingChannels:    h.handleListSignalingChannels,
		pathUpdateSignalingChannel:   h.handleUpdateSignalingChannel,
		pathDeleteSignalingChannel:   h.handleDeleteSignalingChannel,
	}
}

func (h *Handler) handleCreateSignalingChannel(c *echo.Context, body []byte) error {
	var req createSignalingChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.ChannelName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "ChannelName is required")
	}

	var ttl int32
	if req.SingleMasterConfiguration != nil {
		ttl = req.SingleMasterConfiguration.MessageTTLSeconds
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	ch, err := h.Backend.CreateSignalingChannel(
		h.AccountID, regionFromRequest(c, h.DefaultRegion), req.ChannelName, req.ChannelType, ttl, tags)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, createSignalingChannelResponse{ChannelARN: ch.ARN})
}

func (h *Handler) handleDescribeSignalingChannel(c *echo.Context, body []byte) error {
	var req describeSignalingChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	ch, err := h.Backend.DescribeSignalingChannel(req.ChannelName, req.ChannelARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, describeSignalingChannelResponse{ChannelInfo: channelInfoFromChannel(ch)})
}

func (h *Handler) handleListSignalingChannels(c *echo.Context, body []byte) error {
	var req listSignalingChannelsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	var cond *ChannelNameCondition
	if req.ChannelNameCondition != nil {
		cond = &ChannelNameCondition{
			ComparisonOperator: req.ChannelNameCondition.ComparisonOperator,
			ComparisonValue:    req.ChannelNameCondition.ComparisonValue,
		}
	}

	infos, next, err := listAndConvert(
		func() ([]*Channel, string, error) {
			return h.Backend.ListSignalingChannels(req.NextToken, int(req.MaxResults), cond)
		},
		channelInfoFromChannel,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, listSignalingChannelsResponse{ChannelInfoList: infos, NextToken: next})
}

func (h *Handler) handleUpdateSignalingChannel(c *echo.Context, body []byte) error {
	var req updateSignalingChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.ChannelARN == "" || req.CurrentVersion == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidArgumentException",
			"ChannelARN and CurrentVersion are required",
		)
	}

	var ttl *int32
	if req.SingleMasterConfiguration != nil {
		v := req.SingleMasterConfiguration.MessageTTLSeconds
		ttl = &v
	}

	if err := h.Backend.UpdateSignalingChannel(req.ChannelARN, req.CurrentVersion, ttl); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleDeleteSignalingChannel(c *echo.Context, body []byte) error {
	var req deleteSignalingChannelRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.ChannelARN == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "ChannelARN is required")
	}

	if err := h.Backend.DeleteSignalingChannel(req.ChannelARN, req.CurrentVersion); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}
