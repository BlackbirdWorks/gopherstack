package mediapackage

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// --- channel output helpers ---

type ingestEndpointOutput struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type hlsIngestOutput struct {
	IngestEndpoints []ingestEndpointOutput `json:"ingestEndpoints"`
}

// logGroupOutput mirrors the AWS EgressAccessLogs/IngressAccessLogs shape:
// a single-member object wrapping the configured CloudWatch Logs group name.
type logGroupOutput struct {
	LogGroupName string `json:"logGroupName"`
}

type channelOutput struct {
	EgressAccessLogs  *logGroupOutput `json:"egressAccessLogs,omitempty"`
	IngressAccessLogs *logGroupOutput `json:"ingressAccessLogs,omitempty"`
	Tags              map[string]any  `json:"tags"`
	Arn               string          `json:"arn"`
	ID                string          `json:"id"`
	Description       string          `json:"description"`
	CreatedAt         string          `json:"createdAt"`
	HlsIngest         hlsIngestOutput `json:"hlsIngest"`
}

func toChannelOutput(ch *Channel) channelOutput {
	endpoints := make([]ingestEndpointOutput, 0, len(ch.HlsIngest.IngestEndpoints))
	for _, ep := range ch.HlsIngest.IngestEndpoints {
		endpoints = append(endpoints, ingestEndpointOutput{
			ID:       ep.ID,
			URL:      ep.URL,
			Username: ep.Username,
			Password: ep.Password,
		})
	}

	tags := make(map[string]any, len(ch.Tags))
	for k, v := range ch.Tags {
		tags[k] = v
	}

	out := channelOutput{
		Arn:         ch.ARN,
		ID:          ch.ID,
		Description: ch.Description,
		CreatedAt:   ch.CreatedAt,
		HlsIngest:   hlsIngestOutput{IngestEndpoints: endpoints},
		Tags:        tags,
	}

	if ch.EgressLogGroupName != nil {
		out.EgressAccessLogs = &logGroupOutput{LogGroupName: *ch.EgressLogGroupName}
	}

	if ch.IngressLogGroupName != nil {
		out.IngressAccessLogs = &logGroupOutput{LogGroupName: *ch.IngressLogGroupName}
	}

	return out
}

// --- channel handlers ---

func (h *Handler) handleCreateChannel(c *echo.Context, body map[string]any) error {
	id, _ := body["id"].(string)
	description, _ := body["description"].(string)
	tags := extractTags(body)

	ch, err := h.Backend.CreateChannel(id, description, tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, toChannelOutput(ch))
}

func (h *Handler) handleDescribeChannel(c *echo.Context, id string) error {
	ch, err := h.Backend.DescribeChannel(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleUpdateChannel(c *echo.Context, id string, body map[string]any) error {
	description, _ := body["description"].(string)

	ch, err := h.Backend.UpdateChannel(id, description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleDeleteChannel(c *echo.Context, id string) error {
	_, err := h.Backend.DeleteChannel(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleListChannels(c *echo.Context) error {
	maxResults := parseMediaPkgMaxResults(c.QueryParam("maxResults"))
	channels, nextToken, err := h.Backend.ListChannels(maxResults, c.QueryParam("nextToken"))
	if err != nil {
		return h.mapError(c, err)
	}

	out := make([]channelOutput, 0, len(channels))
	for _, ch := range channels {
		out = append(out, toChannelOutput(ch))
	}

	resp := map[string]any{"channels": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleConfigureLogs(c *echo.Context, id string, body map[string]any) error {
	egressLogGroup := logGroupNameFromBody(body, "egressAccessLogs")
	ingressLogGroup := logGroupNameFromBody(body, "ingressAccessLogs")

	ch, err := h.Backend.ConfigureLogs(id, egressLogGroup, ingressLogGroup)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

// logGroupNameFromBody extracts {key}.logGroupName as a *string, returning
// nil when the request body did not include the key at all -- this
// distinguishes "leave the existing log configuration untouched" (key
// absent) from "configure with this log group name" (key present).
func logGroupNameFromBody(body map[string]any, key string) *string {
	raw, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}

	name, _ := raw["logGroupName"].(string)

	return &name
}

func (h *Handler) handleRotateChannelCredentials(c *echo.Context, id string) error {
	ch, err := h.Backend.RotateChannelCredentials(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handleRotateIngestEndpointCredentials(c *echo.Context, path string) error {
	// path: /channels/{channelId}/ingest_endpoints/{ingestEndpointId}/credentials
	rest := strings.TrimPrefix(path, pathChannels+"/")
	channelID, sub, _ := strings.Cut(rest, "/")
	// sub: ingest_endpoints/{ingestEndpointId}/credentials
	sub = strings.TrimPrefix(sub, "ingest_endpoints/")
	ingestEndpointID := strings.TrimSuffix(sub, "/credentials")

	ch, err := h.Backend.RotateIngestEndpointCredentials(channelID, ingestEndpointID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toChannelOutput(ch))
}

func (h *Handler) handlePutChannelLifecyclePolicy(c *echo.Context, channelID string, body map[string]any) error {
	policy, _ := body["policy"].(string)
	if err := h.Backend.PutChannelLifecyclePolicy(channelID, policy); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetChannelLifecyclePolicy(c *echo.Context, channelID string) error {
	policy, err := h.Backend.GetChannelLifecyclePolicy(channelID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"policy": policy})
}
