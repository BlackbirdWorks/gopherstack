package iot

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func shadowThingAndName(c *echo.Context) (string, string) {
	after := strings.TrimPrefix(c.Request().URL.Path, "/things/")
	thingName := strings.TrimSuffix(after, "/shadow")
	shadowName := c.Request().URL.Query().Get("name")

	return thingName, shadowName
}

func (h *Handler) handleGetThingShadow(c *echo.Context) error {
	thingName, shadowName := shadowThingAndName(c)
	s, err := h.Backend.GetThingShadow(thingName, shadowName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"state":      s.State,
		keyVersion:   s.Version,
		keyTimestamp: 0,
	})
}

func (h *Handler) handleUpdateThingShadow(c *echo.Context) error {
	thingName, shadowName := shadowThingAndName(c)

	var body struct {
		State map[string]any `json:"state"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	s, err := h.Backend.UpdateThingShadow(thingName, shadowName, body.State)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"state":      s.State,
		keyVersion:   s.Version,
		keyTimestamp: 0,
	})
}

func (h *Handler) handleDeleteThingShadow(c *echo.Context) error {
	thingName, shadowName := shadowThingAndName(c)
	if err := h.Backend.DeleteThingShadow(thingName, shadowName); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVersion: 0, keyTimestamp: 0})
}

func (h *Handler) handleListNamedShadowsForThing(c *echo.Context) error {
	thingName := strings.TrimPrefix(
		c.Request().URL.Path, "/api/things/shadow/ListNamedShadowsForThing/",
	)
	names, err := h.Backend.ListNamedShadowsForThing(thingName)
	if err != nil {
		return h.handleError(c, err)
	}

	if names == nil {
		names = []string{}
	}

	return c.JSON(http.StatusOK, map[string]any{"results": names, keyTimestamp: 0})
}
