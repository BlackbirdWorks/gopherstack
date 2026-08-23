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

	// ProbeOutput.ProbeResults (mediaconvert@v1.97.1 api_op_Probe.go) is
	// []types.ProbeResult directly -- each item is the Container/Metadata/
	// TrackMappings object itself, not wrapped under a "probeResult" key, and
	// there is no "inputFile" echo member.
	results := make([]map[string]any, 0, len(in.InputFiles))
	for range in.InputFiles {
		results = append(results, map[string]any{
			"container": map[string]any{"format": "mp4"},
		})
	}

	return c.JSON(http.StatusOK, probeOutput{ProbeResults: results})
}
