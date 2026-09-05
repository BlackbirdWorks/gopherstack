package sesv2

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

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

// parseSESv2QueryDate parses a query-string date in the smithy DateTime
// format real clients send (e.g. 2006-01-02T15:04:05.999Z), which
// time.RFC3339 also accepts. An empty or unparseable value yields nil,
// leaving that bound unconstrained.
func parseSESv2QueryDate(v string) *time.Time {
	if v == "" {
		return nil
	}

	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return nil
	}

	return &t
}

func (h *Handler) handleListSuppressedDestinations(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	pageSize := 0
	if v := c.QueryParam("PageSize"); v != "" {
		pageSize, _ = strconv.Atoi(v)
	}

	reasons := c.Request().URL.Query()["Reason"]
	startDate := parseSESv2QueryDate(c.QueryParam("StartDate"))
	endDate := parseSESv2QueryDate(c.QueryParam("EndDate"))

	pg := h.Backend.ListSuppressedDestinations(reasons, startDate, endDate, nextToken, pageSize)

	items := make([]suppressedDestinationOutput, 0, len(pg.Data))
	for _, d := range pg.Data {
		items = append(items, toSuppressedDestinationOutput(d))
	}

	return map[string]any{
		"SuppressedDestinationSummaries": items,
		keyNextToken:                     pg.Next,
	}, nil
}
