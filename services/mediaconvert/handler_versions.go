package mediaconvert

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- ListVersions handler ---

type listVersionsOutput struct {
	NextToken string             `json:"nextToken,omitempty"`
	Versions  []jobEngineVersion `json:"versions"`
}

// jobEngineVersion mirrors the AWS MediaConvert JobEngineVersion type.
type jobEngineVersion struct {
	ExpirationDate *float64 `json:"expirationDate,omitempty"`
	Version        string   `json:"version"`
}

func (h *Handler) handleListVersions(c *echo.Context) error {
	q := c.Request().URL.Query()
	if page.ValidateToken(q.Get("nextToken")) != nil {
		return h.writeError(c, fmt.Errorf("%w: invalid nextToken", ErrValidation))
	}

	pg := page.New(
		[]jobEngineVersion{{Version: "2017-08-29"}},
		q.Get("nextToken"), parseMaxResults(q.Get("maxResults")), defaultListPageSize,
	)

	return c.JSON(http.StatusOK, listVersionsOutput{Versions: pg.Data, NextToken: pg.Next})
}
