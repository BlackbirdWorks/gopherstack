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

	if req.Name == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkName.Error())
	}

	if req.MemberConfiguration.Name == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingMemberName.Error())
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
		votingPolicy,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createNetworkResponse{
		NetworkID: network.ID,
		MemberID:  member.ID,
	})
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

	summaries := make([]networkSummaryObject, 0, len(networks))

	for _, n := range networks {
		summaries = append(summaries, toNetworkSummaryObject(n))
	}

	return c.JSON(http.StatusOK, listNetworksResponse{Networks: summaries})
}

// toNetworkObject converts a Network to its JSON representation.
func toNetworkObject(n *Network) networkObject {
	obj := networkObject{
		ID:               n.ID,
		Arn:              n.Arn,
		Name:             n.Name,
		Description:      n.Description,
		Framework:        n.Framework,
		FrameworkVersion: n.FrameworkVersion,
		Status:           n.Status,
		CreationDate:     n.CreationDate,
		Tags:             n.Tags,
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
