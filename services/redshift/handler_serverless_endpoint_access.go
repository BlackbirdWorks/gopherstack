package redshift

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

// slVpcSecurityGroupMembership mirrors types.VpcSecurityGroupMembership --
// the same shape/semantics as classic Redshift's xmlVpcSecurityGroupMembership
// (endpoint_access.go), so it reuses that type's "active" status convention:
// this backend applies security group changes synchronously.
type slVpcSecurityGroupMembership struct {
	Status             string `json:"status"`
	VpcSecurityGroupID string `json:"vpcSecurityGroupId"`
}

// slEndpointAccessWire is the wire shape for ServerlessEndpointAccess
// responses, expanding VpcSecurityGroupIDs into the real
// vpcSecurityGroups>{status,vpcSecurityGroupId} list at the response
// boundary -- same at-boundary-conversion pattern as slScheduledActionWire.
type slEndpointAccessWire struct {
	EndpointCreateTime time.Time                      `json:"endpointCreateTime"`
	Address            string                         `json:"address,omitempty"`
	EndpointArn        string                         `json:"endpointArn"`
	EndpointName       string                         `json:"endpointName"`
	EndpointStatus     string                         `json:"endpointStatus"`
	WorkgroupName      string                         `json:"workgroupName"`
	SubnetIDs          []string                       `json:"subnetIds,omitempty"`
	VpcSecurityGroups  []slVpcSecurityGroupMembership `json:"vpcSecurityGroups,omitempty"`
	Port               int                            `json:"port,omitempty"`
}

func toEndpointAccessWire(ep *ServerlessEndpointAccess) *slEndpointAccessWire {
	groups := make([]slVpcSecurityGroupMembership, 0, len(ep.VpcSecurityGroupIDs))
	for _, id := range ep.VpcSecurityGroupIDs {
		groups = append(groups, slVpcSecurityGroupMembership{VpcSecurityGroupID: id, Status: endpointStatusActive})
	}

	return &slEndpointAccessWire{
		EndpointCreateTime: ep.EndpointCreateTime,
		Address:            ep.Address,
		EndpointArn:        ep.EndpointArn,
		EndpointName:       ep.EndpointName,
		EndpointStatus:     ep.EndpointStatus,
		WorkgroupName:      ep.WorkgroupName,
		SubnetIDs:          ep.SubnetIDs,
		VpcSecurityGroups:  groups,
		Port:               ep.Port,
	}
}

func (h *ServerlessHandler) handleCreateEndpointAccessSL(c *echo.Context, body []byte) error {
	var req struct {
		EndpointName        string   `json:"endpointName"`
		WorkgroupName       string   `json:"workgroupName"`
		OwnerAccount        string   `json:"ownerAccount"`
		SubnetIDs           []string `json:"subnetIds"`
		VpcSecurityGroupIDs []string `json:"vpcSecurityGroupIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	ep, err := h.Backend.CreateEndpointAccessSL(
		req.EndpointName, req.WorkgroupName, req.OwnerAccount, req.SubnetIDs, req.VpcSecurityGroupIDs,
	)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespEndpoint: toEndpointAccessWire(ep)})
}

func (h *ServerlessHandler) handleGetEndpointAccessSL(c *echo.Context, body []byte) error {
	var req struct {
		EndpointName string `json:"endpointName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.EndpointName == "" {
		return slBadRequest(c, "endpointName is required")
	}

	ep, err := h.Backend.GetEndpointAccessSL(req.EndpointName)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespEndpoint: toEndpointAccessWire(ep)})
}

// handleListEndpointAccessSL implements ListEndpointAccess. The real
// ListEndpointAccessRequest also accepts a vpcId filter, deliberately not
// parsed here -- see ListEndpointAccessSL's doc comment for why.
func (h *ServerlessHandler) handleListEndpointAccessSL(c *echo.Context, body []byte) error {
	var req struct {
		WorkgroupName string `json:"workgroupName"`
		OwnerAccount  string `json:"ownerAccount"`
		NextToken     string `json:"nextToken"`
		MaxResults    int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListEndpointAccessSL(req.WorkgroupName, req.OwnerAccount, req.MaxResults, req.NextToken)

	wire := make([]*slEndpointAccessWire, 0, len(list))
	for _, ep := range list {
		wire = append(wire, toEndpointAccessWire(ep))
	}

	resp := map[string]any{"endpoints": wire}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleUpdateEndpointAccessSL(c *echo.Context, body []byte) error {
	var req struct {
		EndpointName        string   `json:"endpointName"`
		VpcSecurityGroupIDs []string `json:"vpcSecurityGroupIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	ep, err := h.Backend.UpdateEndpointAccessSL(req.EndpointName, req.VpcSecurityGroupIDs)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespEndpoint: toEndpointAccessWire(ep)})
}

func (h *ServerlessHandler) handleDeleteEndpointAccessSL(c *echo.Context, body []byte) error {
	var req struct {
		EndpointName string `json:"endpointName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	ep, err := h.Backend.DeleteEndpointAccessSL(req.EndpointName)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespEndpoint: toEndpointAccessWire(ep)})
}

// handleListManagedWorkgroups implements ListManagedWorkgroups. See
// ManagedWorkgroupListItem's doc comment for why this backend always
// returns an empty, correctly-shaped list.
func (h *ServerlessHandler) handleListManagedWorkgroups(c *echo.Context, body []byte) error {
	var req struct {
		SourceArn  string `json:"sourceArn"`
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListManagedWorkgroupsSL(req.SourceArn, req.MaxResults, req.NextToken)
	resp := map[string]any{"managedWorkgroups": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}
