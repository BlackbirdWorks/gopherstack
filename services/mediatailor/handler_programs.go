package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Program handlers ---

func (h *Handler) handleCreateProgram(
	c *echo.Context,
	channelName, programName string,
	body map[string]any,
) error {
	sourceLocationName, _ := body["SourceLocationName"].(string)
	vodSourceName, _ := body[keyVodSourceName].(string)
	liveSourceName, _ := body[keyLiveSourceName].(string)
	tags := extractTags(body)

	prog, err := h.Backend.CreateProgram(
		channelName,
		programName,
		sourceLocationName,
		vodSourceName,
		liveSourceName,
		tags,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toProgramOutput(prog))
}

func (h *Handler) handleDescribeProgram(c *echo.Context, channelName, programName string) error {
	prog, err := h.Backend.DescribeProgram(channelName, programName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toProgramOutput(prog))
}

func (h *Handler) handleUpdateProgram(c *echo.Context, channelName, programName string) error {
	prog, err := h.Backend.UpdateProgram(channelName, programName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toProgramOutput(prog))
}

func (h *Handler) handleDeleteProgram(c *echo.Context, channelName, programName string) error {
	if err := h.Backend.DeleteProgram(channelName, programName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetChannelSchedule(c *echo.Context, channelName string) error {
	maxResults, nextToken := extractPaginationParams(c)
	entries, nextToken, err := h.Backend.GetChannelSchedule(channelName, maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, map[string]any{
			keyArn:         e.ARN,
			keyChannelName: e.ChannelName,
			"ProgramName":  e.ProgramName,
		})
	}

	resp := map[string]any{keyItems: out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func toProgramOutput(prog *Program) map[string]any {
	return map[string]any{
		keyArn:               prog.ARN,
		keyChannelName:       prog.ChannelName,
		"ProgramName":        prog.ProgramName,
		"SourceLocationName": prog.SourceLocationName,
		keyVodSourceName:     prog.VodSourceName,
		keyLiveSourceName:    prog.LiveSourceName,
		keyTags:              nilToEmpty(prog.Tags),
	}
}
