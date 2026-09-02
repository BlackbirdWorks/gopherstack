package managedblockchain

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateNetwork(c *echo.Context, body []byte) error {
	var req createNetworkRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if req.ClientRequestToken == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingClientRequestToken.Error())
	}

	if req.Name == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkName.Error())
	}

	// Real AWS's CreateNetwork "Applies only to Hyperledger Fabric" -- new
	// networks can no longer be created on any other framework (including
	// ETHEREUM, still a valid Framework enum value elsewhere, e.g. accessors).
	if req.Framework != "" && req.Framework != defaultFramework {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrUnsupportedNetworkFramework.Error())
	}

	if errResp := validateMemberConfigurationRequest(req.MemberConfiguration); errResp != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", errResp.Error())
	}

	edition, err := extractNetworkFabricEdition(req.FrameworkConfiguration)
	if err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", err.Error())
	}

	var votingPolicy *VotingPolicy

	if req.VotingPolicy != nil {
		votingPolicy = &VotingPolicy{}

		if req.VotingPolicy.ApprovalThresholdPolicy != nil {
			votingPolicy.ApprovalThresholdPolicy = &ApprovalThresholdPolicy{
				ThresholdComparator:     req.VotingPolicy.ApprovalThresholdPolicy.ThresholdComparator,
				ProposalDurationInHours: req.VotingPolicy.ApprovalThresholdPolicy.ProposalDurationInHours,
				ThresholdPercentage:     req.VotingPolicy.ApprovalThresholdPolicy.ThresholdPercentage,
			}
		}
	}

	adminUsername := req.MemberConfiguration.FrameworkConfiguration.Fabric.AdminUsername

	network, member, err := h.Backend.CreateNetwork(
		h.DefaultRegion,
		h.AccountID,
		req.Name,
		req.Description,
		req.Framework,
		req.FrameworkVersion,
		req.MemberConfiguration.Name,
		req.MemberConfiguration.Description,
		req.Tags,
		req.MemberConfiguration.Tags,
		votingPolicy,
		edition,
		adminUsername,
		req.MemberConfiguration.KmsKeyArn,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createNetworkResponse{
		NetworkID: network.ID,
		MemberID:  member.ID,
	})
}

// validateMemberConfigurationRequest validates the shared MemberConfiguration
// shape used by both CreateNetwork's nested member config and CreateMember's
// top-level one. It mirrors the real aws-sdk-go-v2 client-side validators
// (validateMemberConfiguration, validateMemberFrameworkConfiguration,
// validateMemberFabricConfiguration) so a raw HTTP client that bypasses SDK
// validation still gets the same InvalidRequestException a real client would
// hit before ever reaching the wire. gopherstack requires Fabric specifically
// (stricter than the real API, which merely requires the outer
// FrameworkConfiguration to be non-nil) since gopherstack only emulates
// Hyperledger Fabric networks -- see ErrMissingNodeMemberID's doc comment for
// the same rationale applied to nodes.
func validateMemberConfigurationRequest(cfg memberConfiguration) error {
	if cfg.Name == "" {
		return ErrMissingMemberName
	}

	if cfg.FrameworkConfiguration == nil {
		return ErrMissingMemberFrameworkConfig
	}

	if cfg.FrameworkConfiguration.Fabric == nil {
		return ErrMissingMemberFabricConfig
	}

	fabric := cfg.FrameworkConfiguration.Fabric

	if fabric.AdminUsername == "" {
		return ErrMissingMemberAdminUsername
	}

	if fabric.AdminPassword == "" {
		return ErrMissingMemberAdminPassword
	}

	if len(fabric.AdminPassword) < 8 || len(fabric.AdminPassword) > 32 {
		return ErrInvalidMemberAdminPassword
	}

	return nil
}

// extractNetworkFabricEdition validates an optional
// NetworkFrameworkConfiguration and returns the requested Fabric edition
// (empty if the caller omitted FrameworkConfiguration or its Fabric object
// entirely, both of which the real API's client-side validator permits).
func extractNetworkFabricEdition(cfg *networkFrameworkConfigurationRequest) (string, error) {
	if cfg == nil || cfg.Fabric == nil {
		return "", nil
	}

	if cfg.Fabric.Edition == "" {
		return "", ErrMissingNetworkFabricEdition
	}

	return cfg.Fabric.Edition, nil
}

func (h *Handler) handleGetNetwork(c *echo.Context, networkID string) error {
	network, err := h.Backend.GetNetwork(networkID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getNetworkResponse{
		Network: toNetworkObject(network),
	})
}

func (h *Handler) handleListNetworks(c *echo.Context) error {
	q := c.Request().URL.Query()
	filter := ListNetworksFilter{
		Name:      q.Get("name"),
		Framework: q.Get("framework"),
		Status:    q.Get("status"),
	}

	networks, err := h.Backend.ListNetworks(filter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	pageItems, nextToken := paginate(networks, q)

	summaries := make([]networkSummaryObject, 0, len(pageItems))

	for _, n := range pageItems {
		summaries = append(summaries, toNetworkSummaryObject(n))
	}

	return c.JSON(http.StatusOK, listNetworksResponse{Networks: summaries, NextToken: nextToken})
}

// toNetworkObject converts a Network to its JSON representation.
func toNetworkObject(n *Network) networkObject {
	obj := networkObject{
		ID:                     n.ID,
		Arn:                    n.Arn,
		Name:                   n.Name,
		Description:            n.Description,
		Framework:              n.Framework,
		FrameworkVersion:       n.FrameworkVersion,
		Status:                 n.Status,
		CreationDate:           n.CreationDate,
		Tags:                   n.Tags,
		VpcEndpointServiceName: n.VpcEndpointServiceName,
	}

	if n.VotingPolicy != nil {
		vp := &votingPolicyObject{}

		if n.VotingPolicy.ApprovalThresholdPolicy != nil {
			vp.ApprovalThresholdPolicy = &approvalThresholdPolicyObject{
				ThresholdComparator:     n.VotingPolicy.ApprovalThresholdPolicy.ThresholdComparator,
				ProposalDurationInHours: n.VotingPolicy.ApprovalThresholdPolicy.ProposalDurationInHours,
				ThresholdPercentage:     n.VotingPolicy.ApprovalThresholdPolicy.ThresholdPercentage,
			}
		}

		obj.VotingPolicy = vp
	}

	if n.FrameworkAttributes != nil {
		obj.FrameworkAttributes = toNetworkFrameworkAttributesRespObj(n.FrameworkAttributes)
	}

	return obj
}

// toNetworkFrameworkAttributesRespObj converts a NetworkFrameworkAttributesState to its response JSON.
func toNetworkFrameworkAttributesRespObj(fa *NetworkFrameworkAttributesState) *networkFrameworkAttributesRespObj {
	if fa == nil {
		return nil
	}

	obj := &networkFrameworkAttributesRespObj{}

	if fa.Fabric != nil {
		obj.Fabric = &networkFabricAttributesRespObj{
			Edition:                 fa.Fabric.Edition,
			OrderingServiceEndpoint: fa.Fabric.OrderingServiceEndpoint,
		}
	}

	return obj
}

// toNetworkSummaryObject converts a Network to its summary JSON representation.
func toNetworkSummaryObject(n *Network) networkSummaryObject {
	return networkSummaryObject{
		ID:               n.ID,
		Arn:              n.Arn,
		Name:             n.Name,
		Description:      n.Description,
		Framework:        n.Framework,
		FrameworkVersion: n.FrameworkVersion,
		Status:           n.Status,
		CreationDate:     n.CreationDate,
	}
}
