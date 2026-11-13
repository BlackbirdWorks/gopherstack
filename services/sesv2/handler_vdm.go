package sesv2

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func (h *Handler) handlePutAccountVdmAttributes(c *echo.Context) (any, error) {
	var in struct {
		VdmAttributes map[string]any `json:"VdmAttributes"`
	}

	_ = json.UnmarshalDecode(jsontext.NewDecoder(c.Request().Body), &in, jsonx.V1())

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

	if err := json.UnmarshalDecode(jsontext.NewDecoder(c.Request().Body), &in, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return &emptyDeleteOutput{}, h.Backend.PutConfigurationSetVdmOptions(
		name,
		in.VdmOptions.DashboardOptions,
		in.VdmOptions.GuardianOptions,
	)
}
