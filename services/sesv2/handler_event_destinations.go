package sesv2

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

type createConfigurationSetEventDestinationInput struct {
	EventDestinationName string `json:"EventDestinationName"`
	EventDestination     struct {
		MatchingEventTypes []string `json:"MatchingEventTypes"`
		Enabled            bool     `json:"Enabled"`
	} `json:"EventDestination"`
}

func (h *Handler) handleCreateConfigurationSetEventDestination(
	c *echo.Context,
	configSetName string,
) (any, error) {
	var in createConfigurationSetEventDestinationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateConfigurationSetEventDestination(
		configSetName,
		in.EventDestinationName,
		in.EventDestination.Enabled,
		in.EventDestination.MatchingEventTypes,
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// configuration set event destination handlers

func (h *Handler) handleGetConfigurationSetEventDestinations(configSetName string) (any, error) {
	dests, err := h.Backend.GetConfigurationSetEventDestinations(configSetName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"EventDestinations": toEventDestinationOutputs(dests)}, nil
}

func (h *Handler) handleDeleteConfigurationSetEventDestination(
	c *echo.Context,
	configSetName string,
) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid event destination path", ErrInvalidInput)
	}

	destName := segments[3]

	if decoded, err := url.PathUnescape(destName); err == nil {
		destName = decoded
	}

	if err := h.Backend.DeleteConfigurationSetEventDestination(configSetName, destName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateConfigurationSetEventDestinationInput struct {
	EventDestination struct {
		MatchingEventTypes []string `json:"MatchingEventTypes"`
		Enabled            bool     `json:"Enabled"`
	} `json:"EventDestination"`
}

func (h *Handler) handleUpdateConfigurationSetEventDestination(
	c *echo.Context,
	configSetName string,
) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid event destination path", ErrInvalidInput)
	}

	destName := segments[3]

	if decoded, err := url.PathUnescape(destName); err == nil {
		destName = decoded
	}

	var in updateConfigurationSetEventDestinationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateConfigurationSetEventDestination(
		configSetName, destName, in.EventDestination.Enabled, in.EventDestination.MatchingEventTypes,
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}
