package dsql

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// dispatchStreamOps handles cluster change-data-capture stream operations.
func (h *Handler) dispatchStreamOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case opCreateStream:
		return true, h.handleCreateStream(c, resource, body)
	case opGetStream:
		return true, h.handleGetStream(c, resource)
	case opDeleteStream:
		return true, h.handleDeleteStream(c, resource)
	case opListStreams:
		return true, h.handleListStreams(c, resource)
	}

	return false, nil
}

func (h *Handler) handleCreateStream(c *echo.Context, clusterIdentifier string, body []byte) error {
	var req createStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeInvalidBody(c)
	}

	in := CreateStreamInput{
		Format:   req.Format,
		Ordering: req.Ordering,
		Tags:     req.Tags,
		Target:   targetFromDTO(req.TargetDefinition),
	}

	stream, err := h.Backend.CreateStream(clusterIdentifier, in)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, streamResponseFromStream(stream, false))
}

func (h *Handler) handleGetStream(c *echo.Context, key string) error {
	clusterIdentifier, streamIdentifier := splitStreamKey(key)

	stream, err := h.Backend.GetStream(clusterIdentifier, streamIdentifier)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, streamResponseFromStream(stream, true))
}

func (h *Handler) handleDeleteStream(c *echo.Context, key string) error {
	clusterIdentifier, streamIdentifier := splitStreamKey(key)

	stream, err := h.Backend.DeleteStream(clusterIdentifier, streamIdentifier)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteStreamResponseFromStream(stream))
}

func (h *Handler) handleListStreams(c *echo.Context, clusterIdentifier string) error {
	q := c.Request().URL.Query()

	maxResults := 0
	if v := q.Get("max-results"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			maxResults = n
		}
	}

	streams, next, err := h.Backend.ListStreams(clusterIdentifier, q.Get("next-token"), maxResults)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]streamSummaryDTO, 0, len(streams))
	for _, s := range streams {
		summaries = append(summaries, streamSummaryFromStream(s))
	}

	return c.JSON(http.StatusOK, listStreamsResponse{NextToken: next, Streams: summaries})
}
