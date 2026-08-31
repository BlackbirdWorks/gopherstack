package managedblockchain

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateProposal(c *echo.Context, networkID string, body []byte) error {
	if networkID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkID.Error())
	}

	var req createProposalRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if req.ClientRequestToken == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingClientRequestToken.Error())
	}

	if req.MemberID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingMemberID.Error())
	}

	var actions *ProposalActions

	if req.Actions != nil {
		actions = &ProposalActions{}

		for _, inv := range req.Actions.Invitations {
			actions.Invitations = append(actions.Invitations, InviteAction(inv))
		}

		for _, rem := range req.Actions.Removals {
			actions.Removals = append(actions.Removals, RemoveAction(rem))
		}
	}

	proposal, err := h.Backend.CreateProposal(
		h.DefaultRegion,
		h.AccountID,
		networkID,
		req.MemberID,
		req.Description,
		actions,
		req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createProposalResponse{ProposalID: proposal.ProposalID})
}

func (h *Handler) handleGetProposal(c *echo.Context, resource string) error {
	networkID, proposalID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	proposal, err := h.Backend.GetProposal(networkID, proposalID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getProposalResponse{Proposal: toProposalObject(proposal)})
}

func (h *Handler) handleListProposals(c *echo.Context, networkID string) error {
	if networkID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkID.Error())
	}

	q := c.Request().URL.Query()
	statusFilter := q.Get("status")

	proposals, err := h.Backend.ListProposals(networkID, statusFilter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	pageItems, nextToken := paginate(proposals, q)

	summaries := make([]proposalSummaryObject, 0, len(pageItems))

	for _, p := range pageItems {
		summaries = append(summaries, toProposalSummaryObject(p))
	}

	return c.JSON(http.StatusOK, listProposalsResponse{Proposals: summaries, NextToken: nextToken})
}

func (h *Handler) handleListProposalVotes(c *echo.Context, resource string) error {
	networkID, proposalID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	votes, err := h.Backend.ListProposalVotes(networkID, proposalID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	pageItems, nextToken := paginate(votes, c.Request().URL.Query())

	summaries := make([]voteSummaryObject, 0, len(pageItems))

	for _, v := range pageItems {
		summaries = append(summaries, voteSummaryObject{
			MemberID:   v.MemberID,
			MemberName: v.MemberName,
			Vote:       v.Vote,
		})
	}

	return c.JSON(http.StatusOK, listProposalVotesResponse{ProposalVotes: summaries, NextToken: nextToken})
}

func (h *Handler) handleVoteOnProposal(c *echo.Context, resource string, body []byte) error {
	networkID, proposalID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	var req voteOnProposalRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if req.VoterMemberID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingVoterMemberID.Error())
	}

	if err := h.Backend.VoteOnProposal(networkID, proposalID, req.VoterMemberID, req.Vote); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// toProposalObject converts a Proposal to its JSON representation.
func toProposalObject(p *Proposal) proposalObject {
	obj := proposalObject{
		ProposalID:           p.ProposalID,
		Arn:                  p.Arn,
		NetworkID:            p.NetworkID,
		ProposedByMemberID:   p.ProposedByMemberID,
		ProposedByMemberName: p.ProposedByMemberName,
		Description:          p.Description,
		Status:               p.Status,
		CreationDate:         p.CreationDate,
		ExpirationDate:       p.ExpirationDate,
		YesVoteCount:         p.YesVoteCount,
		NoVoteCount:          p.NoVoteCount,
		OutstandingVoteCount: p.OutstandingVoteCount,
		Tags:                 p.Tags,
	}

	if p.Actions != nil {
		actObj := &proposalActionsObject{}

		for _, inv := range p.Actions.Invitations {
			actObj.Invitations = append(actObj.Invitations, inviteActionObject(inv))
		}

		for _, rem := range p.Actions.Removals {
			actObj.Removals = append(actObj.Removals, removeActionObject(rem))
		}

		obj.Actions = actObj
	}

	return obj
}

// toProposalSummaryObject converts a Proposal to its summary JSON representation.
func toProposalSummaryObject(p *Proposal) proposalSummaryObject {
	return proposalSummaryObject{
		ProposalID:           p.ProposalID,
		Arn:                  p.Arn,
		ProposedByMemberID:   p.ProposedByMemberID,
		ProposedByMemberName: p.ProposedByMemberName,
		Description:          p.Description,
		Status:               p.Status,
		CreationDate:         p.CreationDate,
		ExpirationDate:       p.ExpirationDate,
	}
}
