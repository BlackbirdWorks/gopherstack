package mediapackage

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- origin endpoint output helper ---

type originEndpointOutput struct {
	Authorization          map[string]any `json:"authorization,omitempty"`
	CmafPackage            map[string]any `json:"cmafPackage,omitempty"`
	DashPackage            map[string]any `json:"dashPackage,omitempty"`
	HlsPackage             map[string]any `json:"hlsPackage,omitempty"`
	MssPackage             map[string]any `json:"mssPackage,omitempty"`
	Tags                   map[string]any `json:"tags"`
	Arn                    string         `json:"arn"`
	ChannelID              string         `json:"channelId"`
	ID                     string         `json:"id"`
	Description            string         `json:"description"`
	ManifestName           string         `json:"manifestName"`
	URL                    string         `json:"url"`
	Origination            string         `json:"origination"`
	CreatedAt              string         `json:"createdAt"`
	Whitelist              []string       `json:"whitelist"`
	StartoverWindowSeconds int            `json:"startoverWindowSeconds"`
	TimeDelaySeconds       int            `json:"timeDelaySeconds"`
}

func toOriginEndpointOutput(ep *OriginEndpoint) originEndpointOutput {
	tags := make(map[string]any, len(ep.Tags))
	for k, v := range ep.Tags {
		tags[k] = v
	}

	whitelist := ep.Whitelist
	if whitelist == nil {
		whitelist = []string{}
	}

	return originEndpointOutput{
		Arn:                    ep.ARN,
		ChannelID:              ep.ChannelID,
		ID:                     ep.ID,
		Description:            ep.Description,
		ManifestName:           ep.ManifestName,
		URL:                    ep.URL,
		Origination:            ep.Origination,
		CreatedAt:              ep.CreatedAt,
		StartoverWindowSeconds: ep.StartoverWindowSeconds,
		TimeDelaySeconds:       ep.TimeDelaySeconds,
		Whitelist:              whitelist,
		Tags:                   tags,
		Authorization:          ep.Authorization,
		CmafPackage:            ep.CmafPackage,
		DashPackage:            ep.DashPackage,
		HlsPackage:             ep.HlsPackage,
		MssPackage:             ep.MssPackage,
	}
}

// --- origin endpoint handlers ---

func (h *Handler) handleCreateOriginEndpoint(c *echo.Context, body map[string]any) error {
	channelID, _ := body["channelId"].(string)
	id, _ := body["id"].(string)
	description, _ := body["description"].(string)
	manifestName, _ := body["manifestName"].(string)
	origination, _ := body["origination"].(string)
	startover := intFromBody(body, "startoverWindowSeconds")
	timeDelay := intFromBody(body, "timeDelaySeconds")
	whitelist := stringsFromBody(body, "whitelist")
	tags := extractTags(body)
	pkg := packagingConfigFromBody(body)

	ep, err := h.Backend.CreateOriginEndpoint(
		channelID,
		id,
		description,
		manifestName,
		startover,
		timeDelay,
		origination,
		whitelist,
		tags,
		pkg,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, toOriginEndpointOutput(ep))
}

// packagingConfigFromBody extracts the opaque CDN-authorization and
// per-protocol packaging blocks from a CreateOriginEndpoint/
// UpdateOriginEndpoint request body. Each block is passed through verbatim
// (see PackagingConfig).
func packagingConfigFromBody(body map[string]any) PackagingConfig {
	return PackagingConfig{
		Authorization: mapFromBody(body, "authorization"),
		CmafPackage:   mapFromBody(body, "cmafPackage"),
		DashPackage:   mapFromBody(body, "dashPackage"),
		HlsPackage:    mapFromBody(body, "hlsPackage"),
		MssPackage:    mapFromBody(body, "mssPackage"),
	}
}

func (h *Handler) handleDescribeOriginEndpoint(c *echo.Context, id string) error {
	ep, err := h.Backend.DescribeOriginEndpoint(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toOriginEndpointOutput(ep))
}

func (h *Handler) handleUpdateOriginEndpoint(c *echo.Context, id string, body map[string]any) error {
	description, _ := body["description"].(string)
	manifestName, _ := body["manifestName"].(string)
	origination, _ := body["origination"].(string)
	startover := intFromBody(body, "startoverWindowSeconds")
	timeDelay := intFromBody(body, "timeDelaySeconds")
	whitelist := stringsFromBody(body, "whitelist")
	pkg := packagingConfigFromBody(body)

	ep, err := h.Backend.UpdateOriginEndpoint(
		id,
		description,
		manifestName,
		startover,
		timeDelay,
		origination,
		whitelist,
		pkg,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, toOriginEndpointOutput(ep))
}

func (h *Handler) handleDeleteOriginEndpoint(c *echo.Context, id string) error {
	_, err := h.Backend.DeleteOriginEndpoint(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleListOriginEndpoints(c *echo.Context) error {
	channelID := c.QueryParam("channelId")

	maxResults := parseMediaPkgMaxResults(c.QueryParam("maxResults"))
	endpoints, nextToken, err := h.Backend.ListOriginEndpoints(channelID, maxResults, c.QueryParam("nextToken"))
	if err != nil {
		return h.mapError(c, err)
	}

	out := make([]originEndpointOutput, 0, len(endpoints))
	for _, ep := range endpoints {
		out = append(out, toOriginEndpointOutput(ep))
	}

	resp := map[string]any{"originEndpoints": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
