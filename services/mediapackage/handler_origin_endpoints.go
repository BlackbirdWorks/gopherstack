package mediapackage

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- origin endpoint output helper ---

type originEndpointOutput struct {
	Authorization          *Authorization `json:"authorization,omitempty"`
	MssPackage             *MssPackage    `json:"mssPackage,omitempty"`
	CmafPackage            map[string]any `json:"cmafPackage,omitempty"`
	DashPackage            map[string]any `json:"dashPackage,omitempty"`
	HlsPackage             map[string]any `json:"hlsPackage,omitempty"`
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

// packagingConfigFromBody extracts the CDN-authorization and per-protocol
// packaging blocks from a CreateOriginEndpoint/UpdateOriginEndpoint request
// body. Authorization and MssPackage are parsed to their full typed shape;
// CmafPackage/DashPackage/HlsPackage remain an opaque passthrough (see
// PackagingConfig for why).
func packagingConfigFromBody(body map[string]any) PackagingConfig {
	return PackagingConfig{
		Authorization: authorizationFromBody(body),
		CmafPackage:   mapFromBody(body, "cmafPackage"),
		DashPackage:   mapFromBody(body, "dashPackage"),
		HlsPackage:    mapFromBody(body, "hlsPackage"),
		MssPackage:    mssPackageFromBody(body),
	}
}

func authorizationFromBody(body map[string]any) *Authorization {
	raw := mapFromBody(body, "authorization")
	if raw == nil {
		return nil
	}

	cdn, _ := raw["cdnIdentifierSecret"].(string)
	role, _ := raw["secretsRoleArn"].(string)

	return &Authorization{CdnIdentifierSecret: cdn, SecretsRoleArn: role}
}

func mssPackageFromBody(body map[string]any) *MssPackage {
	raw := mapFromBody(body, "mssPackage")
	if raw == nil {
		return nil
	}

	return &MssPackage{
		Encryption:             mssEncryptionFromMap(raw),
		StreamSelection:        streamSelectionFromMap(raw),
		ManifestWindowSeconds:  intFromMap(raw, "manifestWindowSeconds"),
		SegmentDurationSeconds: intFromMap(raw, "segmentDurationSeconds"),
	}
}

func mssEncryptionFromMap(m map[string]any) *MssEncryption {
	raw, ok := m["encryption"].(map[string]any)
	if !ok {
		return nil
	}

	return &MssEncryption{SpekeKeyProvider: spekeKeyProviderFromMap(raw)}
}

func spekeKeyProviderFromMap(m map[string]any) *SpekeKeyProvider {
	raw, ok := m["spekeKeyProvider"].(map[string]any)
	if !ok {
		return nil
	}

	resourceID, _ := raw["resourceId"].(string)
	roleArn, _ := raw["roleArn"].(string)
	url, _ := raw["url"].(string)
	certArn, _ := raw["certificateArn"].(string)

	return &SpekeKeyProvider{
		ResourceID:                      resourceID,
		RoleArn:                         roleArn,
		URL:                             url,
		CertificateArn:                  certArn,
		SystemIDs:                       stringsFromBody(raw, "systemIds"),
		EncryptionContractConfiguration: encryptionContractConfigFromMap(raw),
	}
}

func encryptionContractConfigFromMap(m map[string]any) *EncryptionContractConfiguration {
	raw, ok := m["encryptionContractConfiguration"].(map[string]any)
	if !ok {
		return nil
	}

	audio, _ := raw["presetSpeke20Audio"].(string)
	video, _ := raw["presetSpeke20Video"].(string)

	return &EncryptionContractConfiguration{PresetSpeke20Audio: audio, PresetSpeke20Video: video}
}

func streamSelectionFromMap(m map[string]any) *StreamSelection {
	raw, ok := m["streamSelection"].(map[string]any)
	if !ok {
		return nil
	}

	order, _ := raw["streamOrder"].(string)

	return &StreamSelection{
		StreamOrder:           order,
		MaxVideoBitsPerSecond: intFromMap(raw, "maxVideoBitsPerSecond"),
		MinVideoBitsPerSecond: intFromMap(raw, "minVideoBitsPerSecond"),
	}
}

func intFromMap(m map[string]any, key string) int {
	switch n := m[key].(type) {
	case float64:
		return int(n)
	case int:
		return n
	}

	return 0
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
