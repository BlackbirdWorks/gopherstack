package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	opCreateVpcConfiguration = "CreateVpcConfiguration"
	opGetVpcConfiguration    = "GetVpcConfiguration"
	opDeleteVpcConfiguration = "DeleteVpcConfiguration"
	opListVpcConfigurations  = "ListVpcConfigurations"

	segVpcConfigurations = "vpcconfigurations"
	keyVpcConfiguration  = "vpcConfiguration"
	keyVpcConfigurations = "items"
)

// vpcConfigurationWire mirrors types.VpcConfiguration / VpcConfigurationSummary
// (bedrockagent@v1.68.2; timestamps are date-time).
type vpcConfigurationWire struct {
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
	VpcConfigurationID string    `json:"vpcConfigurationId"`
	Status             string    `json:"status"`
	VpcID              string    `json:"vpcId"`
	ResourceTarget     string    `json:"resourceTarget"`
	Protocol           string    `json:"protocol"`
	ResolutionMode     string    `json:"resolutionMode"`
	Name               string    `json:"name,omitempty"`
	Description        string    `json:"description,omitempty"`
	HostHeader         string    `json:"hostHeader,omitempty"`
	TLSServerName      string    `json:"tlsServerName,omitempty"`
	SubnetIDs          []string  `json:"subnetIds"`
	Port               int32     `json:"port"`
}

func toVpcConfigurationWire(v *VpcConfiguration) vpcConfigurationWire {
	return vpcConfigurationWire{
		VpcConfigurationID: v.VpcConfigurationID,
		Status:             v.Status,
		VpcID:              v.VpcID,
		SubnetIDs:          v.SubnetIDs,
		ResourceTarget:     v.ResourceTarget,
		Port:               v.Port,
		Protocol:           v.Protocol,
		ResolutionMode:     v.ResolutionMode,
		Name:               v.Name,
		Description:        v.Description,
		HostHeader:         v.HostHeader,
		TLSServerName:      v.TLSServerName,
		CreatedAt:          v.CreatedAt,
		UpdatedAt:          v.UpdatedAt,
	}
}

func (h *Handler) handleCreateVpcConfiguration(
	ctx context.Context, c *echo.Context, kbID string, body []byte,
) error {
	var req struct {
		Name           string   `json:"name"`
		Description    string   `json:"description"`
		VpcID          string   `json:"vpcId"`
		ResourceTarget string   `json:"resourceTarget"`
		Protocol       string   `json:"protocol"`
		ResolutionMode string   `json:"resolutionMode"`
		HostHeader     string   `json:"hostHeader"`
		TLSServerName  string   `json:"tlsServerName"`
		ClientToken    string   `json:"clientToken"`
		SubnetIDs      []string `json:"subnetIds"`
		Port           int32    `json:"port"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	if req.ResolutionMode == "" {
		req.ResolutionMode = vpcResolutionInVPC
	}

	v, err := h.Backend.CreateVpcConfiguration(ctx, kbID, VpcConfigurationConfig{
		Name:           req.Name,
		Description:    req.Description,
		VpcID:          req.VpcID,
		ResourceTarget: req.ResourceTarget,
		Protocol:       req.Protocol,
		ResolutionMode: req.ResolutionMode,
		HostHeader:     req.HostHeader,
		TLSServerName:  req.TLSServerName,
		ClientToken:    req.ClientToken,
		SubnetIDs:      req.SubnetIDs,
		Port:           req.Port,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{
		"vpcConfigurationId": v.VpcConfigurationID,
		keyStatus:            v.Status,
	})
}

func (h *Handler) handleGetVpcConfiguration(ctx context.Context, c *echo.Context, kbID, id string) error {
	v, err := h.Backend.GetVpcConfiguration(ctx, kbID, id)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVpcConfiguration: toVpcConfigurationWire(v)})
}

func (h *Handler) handleDeleteVpcConfiguration(ctx context.Context, c *echo.Context, kbID, id string) error {
	v, err := h.Backend.DeleteVpcConfiguration(ctx, kbID, id)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{
		"vpcConfigurationId": v.VpcConfigurationID,
		keyStatus:            v.Status,
	})
}

func (h *Handler) handleListVpcConfigurations(ctx context.Context, c *echo.Context, kbID string) error {
	q := c.Request().URL.Query()

	maxResults := maxPageDefault
	if n, err := strconv.Atoi(q.Get("maxResults")); err == nil && n > 0 {
		maxResults = n
	}

	vs, next, err := h.Backend.ListVpcConfigurations(ctx, kbID, q.Get("status"), maxResults, q.Get(keyNextToken))
	if err != nil {
		return handleErr(c, err)
	}

	items := make([]vpcConfigurationWire, 0, len(vs))
	for _, v := range vs {
		items = append(items, toVpcConfigurationWire(v))
	}

	resp := map[string]any{keyVpcConfigurations: items}
	if next != "" {
		resp[keyNextToken] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// dispatchVpcConfigurations routes /knowledgebases/{kb}/vpcconfigurations[/{id}].
func (h *Handler) dispatchVpcConfigurations(
	ctx context.Context, c *echo.Context, kbID, suffix, method string, body []byte,
) error {
	id := strings.Trim(strings.TrimPrefix(suffix, "/"+segVpcConfigurations), "/")

	switch {
	case id == "" && method == http.MethodPost:
		return h.handleCreateVpcConfiguration(ctx, c, kbID, body)
	case id == "" && method == http.MethodGet:
		return h.handleListVpcConfigurations(ctx, c, kbID)
	case id != "" && method == http.MethodGet:
		return h.handleGetVpcConfiguration(ctx, c, kbID, id)
	case id != "" && method == http.MethodDelete:
		return h.handleDeleteVpcConfiguration(ctx, c, kbID, id)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown vpc configuration op"))
}

func classifyVpcConfigurationPath(method string, segs []string) string {
	idx := indexOf(segs, segVpcConfigurations)
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	switch {
	case !hasID && method == http.MethodPost:
		return opCreateVpcConfiguration
	case !hasID && method == http.MethodGet:
		return opListVpcConfigurations
	case hasID && method == http.MethodGet:
		return opGetVpcConfiguration
	case hasID && method == http.MethodDelete:
		return opDeleteVpcConfiguration
	}

	return opUnknown
}
