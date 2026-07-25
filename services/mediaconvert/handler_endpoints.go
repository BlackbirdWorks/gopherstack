package mediaconvert

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Endpoints handler ---

type describeEndpointsInput struct {
	Mode       string `json:"mode,omitempty"`
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type endpointsOutput struct {
	NextToken string          `json:"nextToken,omitempty"`
	Endpoints []endpointEntry `json:"endpoints"`
}

type endpointEntry struct {
	URL string `json:"url"`
}

func (h *Handler) handleDescribeEndpoints(c *echo.Context, body []byte) error {
	var in describeEndpointsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
		}
	}

	r := c.Request()
	scheme := "http"

	if r.TLS != nil {
		scheme = "https"
	}

	// gopherstack always has exactly one endpoint (this host), so both
	// DEFAULT (create-if-absent) and GET_ONLY (existing-only) modes return
	// it; in.Mode is parsed and accepted for wire accuracy but doesn't
	// change the result.
	endpoints := []endpointEntry{{URL: scheme + "://" + r.Host}}
	if in.MaxResults > 0 && in.MaxResults < len(endpoints) {
		endpoints = endpoints[:in.MaxResults]
	}

	// Only one endpoint ever exists, so there is never a next page.
	out := endpointsOutput{Endpoints: endpoints}

	return c.JSON(http.StatusOK, out)
}
