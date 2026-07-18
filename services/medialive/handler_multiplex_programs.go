package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- MultiplexProgram handlers ---

type serviceDescriptorOutput struct {
	ProviderName string `json:"providerName"`
	ServiceName  string `json:"serviceName"`
}

type multiplexProgramSettingsOutput struct {
	ServiceDescriptor        serviceDescriptorOutput `json:"serviceDescriptor"`
	PreferredChannelPipeline string                  `json:"preferredChannelPipeline"`
	ProgramNumber            int                     `json:"programNumber"`
}

// ProgramName and ChannelID first: reduces GC pointer scan.
type multiplexProgramOutput struct {
	ProgramName              string                         `json:"programName"`
	ChannelID                string                         `json:"channelId"`
	MultiplexProgramSettings multiplexProgramSettingsOutput `json:"multiplexProgramSettings"`
}

func toMultiplexProgramOutput(p *MultiplexProgram) multiplexProgramOutput {
	return multiplexProgramOutput{
		ProgramName: p.ProgramName,
		ChannelID:   p.ChannelID,
		MultiplexProgramSettings: multiplexProgramSettingsOutput{
			ProgramNumber:            p.Settings.ProgramNumber,
			PreferredChannelPipeline: p.Settings.PreferredChannelPipeline,
			ServiceDescriptor: serviceDescriptorOutput{
				ProviderName: p.Settings.ServiceDescriptor.ProviderName,
				ServiceName:  p.Settings.ServiceDescriptor.ServiceName,
			},
		},
	}
}

func extractMultiplexProgramSettings(body map[string]any) MultiplexProgramSettings {
	programName, _ := body["programName"].(string)

	raw, _ := body["multiplexProgramSettings"].(map[string]any)
	if raw == nil {
		return MultiplexProgramSettings{ProgramName: programName}
	}

	var sd ServiceDescriptor
	if sdRaw, ok := raw["serviceDescriptor"].(map[string]any); ok {
		sd.ProviderName, _ = sdRaw["providerName"].(string)
		sd.ServiceName, _ = sdRaw["serviceName"].(string)
	}

	preferred, _ := raw["preferredChannelPipeline"].(string)

	return MultiplexProgramSettings{
		ProgramName:              programName,
		ProgramNumber:            intFromAny(raw["programNumber"]),
		PreferredChannelPipeline: preferred,
		ServiceDescriptor:        sd,
	}
}

func (h *Handler) handleCreateMultiplexProgram(
	c *echo.Context,
	multiplexID string,
	body map[string]any,
) error {
	prog := extractMultiplexProgramSettings(body)

	p, err := h.Backend.CreateMultiplexProgram(multiplexID, prog)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(
		http.StatusCreated,
		map[string]any{"multiplexProgram": toMultiplexProgramOutput(p)},
	)
}

func (h *Handler) handleDescribeMultiplexProgram(c *echo.Context, resource string) error {
	multiplexID, programName := splitMultiplexProgram(resource)

	p, err := h.Backend.DescribeMultiplexProgram(multiplexID, programName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexProgramOutput(p))
}

func (h *Handler) handleUpdateMultiplexProgram(
	c *echo.Context,
	resource string,
	body map[string]any,
) error {
	multiplexID, programName := splitMultiplexProgram(resource)

	prog := extractMultiplexProgramSettings(body)
	prog.ProgramName = programName

	p, err := h.Backend.UpdateMultiplexProgram(multiplexID, prog)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"multiplexProgram": toMultiplexProgramOutput(p)})
}

func (h *Handler) handleDeleteMultiplexProgram(c *echo.Context, resource string) error {
	multiplexID, programName := splitMultiplexProgram(resource)

	p, err := h.Backend.DeleteMultiplexProgram(multiplexID, programName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexProgramOutput(p))
}

func (h *Handler) handleListMultiplexPrograms(c *echo.Context, multiplexID string) error {
	summaries, nextToken, err := h.Backend.ListMultiplexPrograms(multiplexID, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			"programName": s.ProgramName,
			"channelId":   s.ChannelID,
		})
	}

	resp := map[string]any{"multiplexPrograms": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
