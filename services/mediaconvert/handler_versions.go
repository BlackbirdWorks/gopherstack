package mediaconvert

import (
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
	token, maxResults, err := listPageArgs(q)
	if err != nil {
		return h.writeError(c, err)
	}

	pg := page.New([]jobEngineVersion{{Version: "2017-08-29"}}, token, maxResults, defaultListPageSize)

	return c.JSON(http.StatusOK, listVersionsOutput{Versions: pg.Data, NextToken: pg.Next})
}
