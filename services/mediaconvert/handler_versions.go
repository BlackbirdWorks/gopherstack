package mediaconvert

import (
	"net/http"

	"github.com/labstack/echo/v5"
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
	out := listVersionsOutput{
		Versions: []jobEngineVersion{
			{Version: "2017-08-29"},
		},
	}

	return c.JSON(http.StatusOK, out)
}
