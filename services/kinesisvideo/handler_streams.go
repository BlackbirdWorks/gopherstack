package kinesisvideo

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) buildStreamOps() map[string]handlerFunc {
	return map[string]handlerFunc{
		pathCreateStream:        h.handleCreateStream,
		pathDescribeStream:      h.handleDescribeStream,
		pathListStreams:         h.handleListStreams,
		pathUpdateStream:        h.handleUpdateStream,
		pathDeleteStream:        h.handleDeleteStream,
		pathUpdateDataRetention: h.handleUpdateDataRetention,
		pathGetDataEndpoint:     h.handleGetDataEndpoint,
	}
}

func (h *Handler) handleCreateStream(c *echo.Context, body []byte) error {
	var req createStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.StreamName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "StreamName is required")
	}

	defaultStorageTier := ""
	if req.StreamStorageConfiguration != nil {
		defaultStorageTier = req.StreamStorageConfiguration.DefaultStorageTier
	}

	s, err := h.Backend.CreateStream(
		h.AccountID, regionFromRequest(c, h.DefaultRegion), req.StreamName, req.DeviceName, req.MediaType,
		req.KmsKeyID, defaultStorageTier, req.DataRetentionInHours, req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, createStreamResponse{StreamARN: s.ARN})
}

func (h *Handler) handleDescribeStream(c *echo.Context, body []byte) error {
	var req describeStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	s, err := h.Backend.DescribeStream(req.StreamName, req.StreamARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, describeStreamResponse{StreamInfo: streamInfoFromStream(s)})
}

func (h *Handler) handleListStreams(c *echo.Context, body []byte) error {
	var req listStreamsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	var cond *StreamNameCondition
	if req.StreamNameCondition != nil {
		cond = &StreamNameCondition{
			ComparisonOperator: req.StreamNameCondition.ComparisonOperator,
			ComparisonValue:    req.StreamNameCondition.ComparisonValue,
		}
	}

	infos, next, err := listAndConvert(
		func() ([]*Stream, string, error) {
			return h.Backend.ListStreams(req.NextToken, int(req.MaxResults), cond)
		},
		streamInfoFromStream,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, listStreamsResponse{StreamInfoList: infos, NextToken: next})
}

func (h *Handler) handleUpdateStream(c *echo.Context, body []byte) error {
	var req updateStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.CurrentVersion == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "CurrentVersion is required")
	}

	if err := h.Backend.UpdateStream(
		req.StreamName,
		req.StreamARN,
		req.CurrentVersion,
		req.DeviceName,
		req.MediaType,
	); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleDeleteStream(c *echo.Context, body []byte) error {
	var req deleteStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.StreamARN == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "StreamARN is required")
	}

	if err := h.Backend.DeleteStream(req.StreamARN, req.CurrentVersion); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleUpdateDataRetention(c *echo.Context, body []byte) error {
	var req updateDataRetentionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.CurrentVersion == "" || req.Operation == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidArgumentException",
			"CurrentVersion and Operation are required",
		)
	}

	err := h.Backend.UpdateDataRetention(
		req.StreamName, req.StreamARN, req.CurrentVersion, req.Operation, req.DataRetentionChangeInHours)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleGetDataEndpoint(c *echo.Context, body []byte) error {
	var req getDataEndpointRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if req.APIName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "APIName is required")
	}

	endpoint, err := h.Backend.GetDataEndpoint(
		req.StreamName, req.StreamARN, req.APIName, regionFromRequest(c, h.DefaultRegion))
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, getDataEndpointResponse{DataEndpoint: endpoint})
}

func regionFromRequest(c *echo.Context, defaultRegion string) string {
	return httputils.ExtractRegionFromRequest(c.Request(), defaultRegion)
}
