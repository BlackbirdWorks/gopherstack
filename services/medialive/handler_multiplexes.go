package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Multiplex handlers ---

// JSON tags are lowerCamelCase to match the real DescribeMultiplexOutput /
// MultiplexSettings wire shape (see channelOutput's doc comment for why
// case matters to the real SDK client's decoder).
type multiplexSettingsOutput struct {
	TransportStreamBitrate              int `json:"transportStreamBitrate"`
	TransportStreamID                   int `json:"transportStreamId"`
	TransportStreamReservedBitrate      int `json:"transportStreamReservedBitrate"`
	MaximumVideoBufferDelayMilliseconds int `json:"maximumVideoBufferDelayMilliseconds"`
}

// Tags and AvailabilityZones first: reduces GC pointer scan.
type multiplexOutput struct {
	Tags                  map[string]string       `json:"tags"`
	Arn                   string                  `json:"arn"`
	ID                    string                  `json:"id"`
	Name                  string                  `json:"name"`
	State                 string                  `json:"state"`
	AvailabilityZones     []string                `json:"availabilityZones"`
	MultiplexSettings     multiplexSettingsOutput `json:"multiplexSettings"`
	PipelinesRunningCount int32                   `json:"pipelinesRunningCount"`
	ProgramCount          int32                   `json:"programCount"`
}

// multiplexPipelinesRunningCount mirrors pipelinesRunningCount for
// Multiplex: AWS Elemental multiplexes always run as a 2-pipeline hitless
// pair, so a RUNNING multiplex reports 2 healthy pipelines and every other
// state reports 0.
func multiplexPipelinesRunningCount(state string) int32 {
	if state != stateRunning {
		return 0
	}

	return pipelinesRunningCountStandard
}

func toMultiplexOutput(m *Multiplex) multiplexOutput {
	tags := m.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	zones := m.AvailabilityZones
	if zones == nil {
		zones = []string{}
	}

	return multiplexOutput{
		Tags:              tags,
		AvailabilityZones: zones,
		Arn:               m.ARN,
		ID:                m.ID,
		Name:              m.Name,
		State:             m.State,
		MultiplexSettings: multiplexSettingsOutput{
			TransportStreamBitrate:              m.Settings.TransportStreamBitrate,
			TransportStreamID:                   m.Settings.TransportStreamID,
			TransportStreamReservedBitrate:      m.Settings.TransportStreamReservedBitrate,
			MaximumVideoBufferDelayMilliseconds: m.Settings.MaximumVideoBufferDelayMilliseconds,
		},
		PipelinesRunningCount: multiplexPipelinesRunningCount(m.State),
		ProgramCount:          int32(m.ProgramCount), //nolint:gosec // program count is always small
	}
}

func extractMultiplexSettings(body map[string]any) MultiplexSettings {
	raw, _ := body["multiplexSettings"].(map[string]any)
	if raw == nil {
		return MultiplexSettings{}
	}

	return MultiplexSettings{
		TransportStreamBitrate:              intFromAny(raw["transportStreamBitrate"]),
		TransportStreamID:                   intFromAny(raw["transportStreamId"]),
		TransportStreamReservedBitrate:      intFromAny(raw["transportStreamReservedBitrate"]),
		MaximumVideoBufferDelayMilliseconds: intFromAny(raw["maximumVideoBufferDelayMilliseconds"]),
	}
}

func (h *Handler) handleCreateMultiplex(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	settings := extractMultiplexSettings(body)
	tags := extractTags(body)

	var zones []string
	if raw, ok := body["availabilityZones"].([]any); ok {
		for _, z := range raw {
			if s, isStr := z.(string); isStr {
				zones = append(zones, s)
			}
		}
	}

	m, err := h.Backend.CreateMultiplex(name, zones, settings, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"multiplex": toMultiplexOutput(m)})
}

func (h *Handler) handleDescribeMultiplex(c *echo.Context, multiplexID string) error {
	m, err := h.Backend.DescribeMultiplex(multiplexID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexOutput(m))
}

func (h *Handler) handleUpdateMultiplex(
	c *echo.Context,
	multiplexID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	settings := extractMultiplexSettings(body)

	m, err := h.Backend.UpdateMultiplex(multiplexID, name, settings)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"multiplex": toMultiplexOutput(m)})
}

func (h *Handler) handleDeleteMultiplex(c *echo.Context, multiplexID string) error {
	m, err := h.Backend.DeleteMultiplex(multiplexID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexOutput(m))
}

func (h *Handler) handleListMultiplexes(c *echo.Context) error {
	maxResults, nextTokenParam := paginationParams(c)
	summaries, nextToken, err := h.Backend.ListMultiplexes(maxResults, nextTokenParam)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		zones := s.AvailabilityZones
		if zones == nil {
			zones = []string{}
		}
		tags := s.Tags
		if tags == nil {
			tags = map[string]string{}
		}

		out = append(out, map[string]any{
			keyArn:                  s.ARN,
			keyID:                   s.ID,
			keyName:                 s.Name,
			keyState:                s.State,
			"availabilityZones":     zones,
			"pipelinesRunningCount": multiplexPipelinesRunningCount(s.State),
			"programCount":          int32(s.ProgramCount), //nolint:gosec // program count is always small
			"multiplexSettings": map[string]any{
				"transportStreamBitrate": s.TransportStreamBitrate,
			},
			keyTags: tags,
		})
	}

	resp := map[string]any{"multiplexes": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStartMultiplex(c *echo.Context, multiplexID string) error {
	m, err := h.Backend.StartMultiplex(multiplexID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexOutput(m))
}

func (h *Handler) handleStopMultiplex(c *echo.Context, multiplexID string) error {
	m, err := h.Backend.StopMultiplex(multiplexID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toMultiplexOutput(m))
}

func (h *Handler) handleListMultiplexAlerts(c *echo.Context, multiplexID string) error {
	alerts, err := h.Backend.ListMultiplexAlerts(multiplexID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAlerts: alerts})
}
