package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handlePutAccountVdmAttributes(c *echo.Context) (any, error) {
	var in struct {
		VdmAttributes map[string]any `json:"VdmAttributes"`
	}

	_ = json.NewDecoder(c.Request().Body).Decode(&in)

	if err := h.Backend.PutAccountVdmAttributes(in.VdmAttributes); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handlePutConfigurationSetVdmOptions(
	c *echo.Context,
	name string,
) (any, error) {
	var in struct {
		VdmOptions struct {
			DashboardOptions map[string]any `json:"DashboardOptions"`
			GuardianOptions  map[string]any `json:"GuardianOptions"`
		} `json:"VdmOptions"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetVdmOptions(
		name,
		in.VdmOptions.DashboardOptions,
		in.VdmOptions.GuardianOptions,
	)
}
