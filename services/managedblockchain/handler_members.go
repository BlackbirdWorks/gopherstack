package managedblockchain

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateMember(c *echo.Context, networkID string, body []byte) error {
	if networkID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkID.Error())
	}

	var req createMemberRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if req.MemberConfiguration.Name == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingMemberName.Error())
	}

	member, err := h.Backend.CreateMember(
		h.DefaultRegion,
		h.AccountID,
		networkID,
		req.MemberConfiguration.Name,
		req.MemberConfiguration.Description,
		req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createMemberResponse{MemberID: member.ID})
}

func (h *Handler) handleGetMember(c *echo.Context, resource string) error {
	networkID, memberID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	member, err := h.Backend.GetMember(networkID, memberID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getMemberResponse{
		Member: toMemberObject(member),
	})
}

func (h *Handler) handleListMembers(c *echo.Context, networkID string) error {
	if networkID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkID.Error())
	}

	q := c.Request().URL.Query()
	filter := ListMembersFilter{
		Name:   q.Get("name"),
		Status: q.Get("status"),
	}

	if isOwnedStr := q.Get("isOwned"); isOwnedStr != "" {
		isOwned := isOwnedStr == "true"
		filter.IsOwned = &isOwned
	}

	members, err := h.Backend.ListMembers(networkID, filter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]memberSummaryObject, 0, len(members))

	for _, m := range members {
		summaries = append(summaries, toMemberSummaryObject(m))
	}

	return c.JSON(http.StatusOK, listMembersResponse{Members: summaries})
}

func (h *Handler) handleDeleteMember(c *echo.Context, resource string) error {
	networkID, memberID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	if err := h.Backend.DeleteMember(networkID, memberID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateMember(c *echo.Context, resource string, body []byte) error {
	networkID, memberID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	var req updateMemberRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	_, err := h.Backend.UpdateMember(networkID, memberID, buildMemberLogConfig(req.LogPublishingConfiguration))
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func buildMemberLogConfig(req *memberLogPublishingConfigReq) *MemberLogPublishingConfigState {
	if req == nil {
		return nil
	}

	logConfig := &MemberLogPublishingConfigState{}

	if req.Fabric == nil {
		return logConfig
	}

	fabric := &MemberFabricLogState{}

	if req.Fabric.CaLogs != nil {
		caLogs := &LogConfigState{}
		if req.Fabric.CaLogs.CloudWatch != nil {
			caLogs.CloudWatch = &CloudWatchLogState{Enabled: req.Fabric.CaLogs.CloudWatch.Enabled}
		}
		fabric.CALogs = caLogs
	}

	logConfig.Fabric = fabric

	return logConfig
}

// toMemberObject converts a Member to its JSON representation.
func toMemberObject(m *Member) memberObject {
	obj := memberObject{
		ID:           m.ID,
		Arn:          m.Arn,
		Name:         m.Name,
		Description:  m.Description,
		NetworkID:    m.NetworkID,
		Status:       m.Status,
		CreationDate: m.CreationDate,
		Tags:         m.Tags,
		IsOwned:      m.IsOwned,
	}

	if m.LogPublishingConfiguration != nil {
		obj.LogPublishingConfiguration = toMemberLogConfigRespObj(m.LogPublishingConfiguration)
	}

	return obj
}

// toMemberLogConfigRespObj converts MemberLogPublishingConfigState to its response JSON.
func toMemberLogConfigRespObj(c *MemberLogPublishingConfigState) *memberLogPublishingConfigRespObj {
	if c == nil {
		return nil
	}

	obj := &memberLogPublishingConfigRespObj{}

	if c.Fabric != nil {
		fabric := &memberFabricLogRespObj{}

		if c.Fabric.CALogs != nil {
			fabric.CaLogs = toLogConfigRespObj(c.Fabric.CALogs)
		}

		obj.Fabric = fabric
	}

	return obj
}

// toLogConfigRespObj converts LogConfigState to its response JSON. Shared by
// both toMemberLogConfigRespObj above and toNodeLogConfigRespObj in
// handler_nodes.go.
func toLogConfigRespObj(c *LogConfigState) *logConfigRespObj {
	if c == nil {
		return nil
	}

	obj := &logConfigRespObj{}

	if c.CloudWatch != nil {
		obj.CloudWatch = &cloudWatchLogRespObj{Enabled: c.CloudWatch.Enabled}
	}

	return obj
}

// toMemberSummaryObject converts a Member to its summary JSON representation.
func toMemberSummaryObject(m *Member) memberSummaryObject {
	return memberSummaryObject{
		ID:           m.ID,
		Arn:          m.Arn,
		Name:         m.Name,
		Description:  m.Description,
		Status:       m.Status,
		CreationDate: m.CreationDate,
		IsOwned:      m.IsOwned,
	}
}
