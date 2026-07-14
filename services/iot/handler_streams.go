package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func resolveStreamOps(path, method string) string {
	switch {
	case path == "/streams" && method == http.MethodGet:
		return opListStreams
	case strings.HasPrefix(path, "/streams/") && method == http.MethodPost:
		return opCreateStream
	case strings.HasPrefix(path, "/streams/") && method == http.MethodGet:
		return opDescribeStream
	case strings.HasPrefix(path, "/streams/") && method == http.MethodPut:
		return opUpdateStream
	case strings.HasPrefix(path, "/streams/") && method == http.MethodDelete:
		return opDeleteStream
	}

	return unknownOperation
}

func (h *Handler) handleCreateStream(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/streams/")
	var input CreateStreamInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.StreamID = id
	s, err := h.Backend.CreateStream(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyStreamID:     s.StreamID,
		keyStreamARN:    s.StreamARN,
		"description":   s.Description,
		"streamVersion": s.StreamVersion,
	})
}

func (h *Handler) handleDescribeStream(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/streams/")
	s, err := h.Backend.DescribeStream(id)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"streamInfo": s})
}

func (h *Handler) handleListStreams(c *echo.Context) error {
	streams := h.Backend.ListStreams()
	summaries := make([]map[string]any, len(streams))
	for i, s := range streams {
		summaries[i] = map[string]any{
			keyStreamID:  s.StreamID,
			keyStreamARN: s.StreamARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"streams": summaries})
}

func (h *Handler) handleUpdateStream(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/streams/")
	var req struct {
		Description string       `json:"description,omitempty"`
		RoleARN     string       `json:"roleArn,omitempty"`
		Files       []StreamFile `json:"files,omitempty"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	s, err := h.Backend.UpdateStream(id, req.Description, req.RoleARN, req.Files)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyStreamID:     s.StreamID,
		keyStreamARN:    s.StreamARN,
		"streamVersion": s.StreamVersion,
	})
}

func (h *Handler) handleDeleteStream(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/streams/")
	if err := h.Backend.DeleteStream(id); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) dispatchStreamOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateStream:
		return true, h.handleCreateStream(c)
	case opDescribeStream:
		return true, h.handleDescribeStream(c)
	case opListStreams:
		return true, h.handleListStreams(c)
	case opUpdateStream:
		return true, h.handleUpdateStream(c)
	case opDeleteStream:
		return true, h.handleDeleteStream(c)
	}

	return false, nil
}
