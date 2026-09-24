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

type videoSettingsOutput struct {
	ConstantBitrate int32 `json:"constantBitrate"`
}

type multiplexProgramSettingsOutput struct {
	ServiceDescriptor        *serviceDescriptorOutput `json:"serviceDescriptor,omitempty"`
	VideoSettings            *videoSettingsOutput     `json:"videoSettings,omitempty"`
	PreferredChannelPipeline string                   `json:"preferredChannelPipeline"`
	ProgramNumber            int                      `json:"programNumber"`
}

// ProgramName and ChannelID first: reduces GC pointer scan.
type multiplexProgramOutput struct {
	ProgramName              string                         `json:"programName"`
	ChannelID                string                         `json:"channelId"`
	MultiplexProgramSettings multiplexProgramSettingsOutput `json:"multiplexProgramSettings"`
}

func toMultiplexProgramOutput(p *MultiplexProgram) multiplexProgramOutput {
	settings := multiplexProgramSettingsOutput{
		ProgramNumber:            p.Settings.ProgramNumber,
		PreferredChannelPipeline: p.Settings.PreferredChannelPipeline,
	}

	if p.Settings.HasServiceDescriptor {
		settings.ServiceDescriptor = &serviceDescriptorOutput{
			ProviderName: p.Settings.ServiceDescriptor.ProviderName,
			ServiceName:  p.Settings.ServiceDescriptor.ServiceName,
		}
	}

	if p.Settings.VideoConstantBitrate != nil {
		settings.VideoSettings = &videoSettingsOutput{ConstantBitrate: *p.Settings.VideoConstantBitrate}
	}

	return multiplexProgramOutput{
		ProgramName:              p.ProgramName,
		ChannelID:                p.ChannelID,
		MultiplexProgramSettings: settings,
	}
}

func extractMultiplexProgramSettings(body map[string]any) MultiplexProgramSettings {
	programName, _ := body["programName"].(string)

	raw, _ := body["multiplexProgramSettings"].(map[string]any)
	if raw == nil {
		return MultiplexProgramSettings{ProgramName: programName}
	}

	var sd ServiceDescriptor

	sdRaw, hasSD := raw["serviceDescriptor"].(map[string]any)
	if hasSD {
		sd.ProviderName, _ = sdRaw["providerName"].(string)
		sd.ServiceName, _ = sdRaw["serviceName"].(string)
	}

	var videoConstantBitrate *int32

	if vsRaw, ok := raw["videoSettings"].(map[string]any); ok {
		if cb, hasCB := vsRaw["constantBitrate"]; hasCB {
			v := int32FromAny(cb)
			videoConstantBitrate = &v
		}
	}

	preferred, _ := raw["preferredChannelPipeline"].(string)

	return MultiplexProgramSettings{
		ProgramName:              programName,
		ProgramNumber:            intFromAny(raw["programNumber"]),
		PreferredChannelPipeline: preferred,
		ServiceDescriptor:        sd,
		HasServiceDescriptor:     hasSD,
		VideoConstantBitrate:     videoConstantBitrate,
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
	maxResults, nextTokenParam := paginationParams(c)
	summaries, nextToken, err := h.Backend.ListMultiplexPrograms(multiplexID, maxResults, nextTokenParam)
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
