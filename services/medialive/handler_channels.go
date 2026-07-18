package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Channel handlers ---

// Tags first: reduces GC pointer scan from 104 to 96 bytes.
//
// JSON tags are lowerCamelCase to match the real DescribeChannelOutput wire
// shape (aws-sdk-go-v2/service/medialive deserializers.go switches on
// exact-case keys "arn"/"id"/"name"/... -- a PascalCase key like "Arn" is
// silently ignored by the real SDK client's decoder, leaving every field at
// its zero value).
type channelOutput struct {
	Tags                  map[string]string `json:"tags"`
	Arn                   string            `json:"arn"`
	ID                    string            `json:"id"`
	Name                  string            `json:"name"`
	ChannelClass          string            `json:"channelClass"`
	RoleArn               string            `json:"roleArn"`
	State                 string            `json:"state"`
	PipelinesRunningCount int32             `json:"pipelinesRunningCount"`
}

// pipelinesRunningCount derives the number of currently healthy pipelines
// from a channel's State and ChannelClass, matching AWS: only RUNNING
// channels report running pipelines (2 for STANDARD, 1 for
// SINGLE_PIPELINE); every other state (IDLE, STARTING, STOPPING, etc.)
// reports 0.
func pipelinesRunningCount(state, channelClass string) int32 {
	if state != stateRunning {
		return 0
	}

	if channelClass == channelClassSinglePipeline {
		return pipelinesRunningCountSinglePipeline
	}

	return pipelinesRunningCountStandard
}

func toChannelOutput(ch *Channel) channelOutput {
	tags := ch.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return channelOutput{
		Tags:                  tags,
		Arn:                   ch.ARN,
		ID:                    ch.ID,
		Name:                  ch.Name,
		ChannelClass:          ch.ChannelClass,
		RoleArn:               ch.RoleARN,
		State:                 ch.State,
		PipelinesRunningCount: pipelinesRunningCount(ch.State, ch.ChannelClass),
	}
}

func (h *Handler) handleCreateChannel(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	channelClass, _ := body["channelClass"].(string)
	roleArn, _ := body["roleArn"].(string)
	tags := extractTags(body)

	ch, err := h.Backend.CreateChannel(name, channelClass, roleArn, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyChannel: toChannelOutput(ch)})
}

func (h *Handler) handleDescribeChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.DescribeChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleUpdateChannel(
	c *echo.Context,
	channelID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	roleArn, _ := body["roleArn"].(string)

	ch, err := h.Backend.UpdateChannel(channelID, name, roleArn)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyChannel: toChannelOutput(ch)})
}

func (h *Handler) handleDeleteChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.DeleteChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListChannels(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, map[string]any{
			keyArn:                  s.ARN,
			keyID:                   s.ID,
			keyName:                 s.Name,
			"channelClass":          s.ChannelClass,
			keyState:                s.State,
			"pipelinesRunningCount": pipelinesRunningCount(s.State, s.ChannelClass),
		})
	}

	resp := map[string]any{"channels": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStartChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.StartChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleStopChannel(c *echo.Context, channelID string) error {
	ch, err := h.Backend.StopChannel(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

// --- Alert and version handlers ---

func (h *Handler) handleListAlerts(c *echo.Context, channelID string) error {
	alerts, err := h.Backend.ListAlerts(channelID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAlerts: alerts})
}

func (h *Handler) handleListVersions(c *echo.Context) error {
	versions := h.Backend.ListVersions()

	out := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		item := map[string]any{"version": v.Version}
		// expirationDate is __timestampIso8601 on the real wire
		// (smithytime.ParseDateTime) -- omit rather than emit "", which a
		// real SDK client would fail to parse.
		if v.ExpirationDate != "" {
			item["expirationDate"] = v.ExpirationDate
		}
		out = append(out, item)
	}

	return c.JSON(http.StatusOK, map[string]any{"versions": out})
}

// --- Channel lifecycle extra handlers ---

func (h *Handler) handleUpdateChannelClass(
	c *echo.Context,
	channelID string,
	body map[string]any,
) error {
	channelClass, _ := body["channelClass"].(string)

	ch, err := h.Backend.UpdateChannelClass(channelID, channelClass)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyChannel: toChannelOutput(ch)})
}

func (h *Handler) handleRestartChannelPipelines(c *echo.Context, channelID string) error {
	pipelineIDs := []string{}

	ch, err := h.Backend.RestartChannelPipelines(channelID, pipelineIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleDescribeThumbnails(c *echo.Context, channelID string) error {
	if _, err := h.Backend.DescribeThumbnails(channelID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"ThumbnailDetails": []map[string]any{}})
}
