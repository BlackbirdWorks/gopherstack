package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

// suppressed destination handlers

type putSuppressedDestinationInput struct {
	EmailAddress string `json:"EmailAddress"`
	Reason       string `json:"Reason"`
}

func (h *Handler) handlePutSuppressedDestination(c *echo.Context) (any, error) {
	var in putSuppressedDestinationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.PutSuppressedDestination(in.EmailAddress, in.Reason); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleGetSuppressedDestination(email string) (any, error) {
	dest, err := h.Backend.GetSuppressedDestination(email)
	if err != nil {
		return nil, err
	}

	return map[string]any{"SuppressedDestination": toSuppressedDestinationOutput(dest)}, nil
}

func (h *Handler) handleDeleteSuppressedDestination(email string) (any, error) {
	if err := h.Backend.DeleteSuppressedDestination(email); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListSuppressedDestinations(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListSuppressedDestinations(nextToken, 0)

	items := make([]suppressedDestinationOutput, 0, len(pg.Data))
	for _, d := range pg.Data {
		items = append(items, toSuppressedDestinationOutput(d))
	}

	return map[string]any{
		"SuppressedDestinationSummaries": items,
		keyNextToken:                     pg.Next,
	}, nil
}
