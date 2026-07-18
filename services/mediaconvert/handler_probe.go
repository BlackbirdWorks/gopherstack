package mediaconvert

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Probe handler ---

type probeInput struct {
	InputFiles []map[string]any `json:"inputFiles,omitempty"`
}

type probeOutput struct {
	ProbeResults []map[string]any `json:"probeResults"`
}

func (h *Handler) handleProbe(c *echo.Context, body []byte) error {
	var in probeInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	results := make([]map[string]any, 0, len(in.InputFiles))
	for _, f := range in.InputFiles {
		result := map[string]any{
			"probeResult": map[string]any{
				"container": map[string]any{"format": "mp4"},
				"inputFile": f,
			},
		}
		results = append(results, result)
	}

	return c.JSON(http.StatusOK, probeOutput{ProbeResults: results})
}
