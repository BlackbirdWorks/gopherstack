package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildAccountLinksOps returns the map of account link operations.
func (h *Handler) buildAccountLinksOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateAccountLinkInvitation": service.WrapOp(h.handleCreateAccountLinkInvitation),
		"AcceptAccountLinkInvitation": service.WrapOp(h.handleAcceptAccountLinkInvitation),
		"RejectAccountLinkInvitation": service.WrapOp(h.handleRejectAccountLinkInvitation),
		"DeleteAccountLinkInvitation": service.WrapOp(h.handleDeleteAccountLinkInvitation),
		"GetAccountLink":              service.WrapOp(h.handleGetAccountLink),
		"ListAccountLinks":            service.WrapOp(h.handleListAccountLinks),
	}
}

type createAccountLinkInvitationInput struct {
	TargetAccountId string `json:"TargetAccountId"` //nolint:revive,staticcheck // existing issue.
	ClientToken     string `json:"ClientToken"`
}

// accountLinkResp mirrors the real AccountLink shape (field-diffed against
// deserializers.go's awsAwsjson11_deserializeDocumentAccountLink): the
// identifying/status keys are "AccountLinkId"/"AccountLinkStatus", not the
// request-side "LinkId"/"Status" names.
type accountLinkResp struct {
	AccountLinkId     string `json:"AccountLinkId"` //nolint:revive,staticcheck // existing issue.
	AccountLinkStatus string `json:"AccountLinkStatus"`
	SourceAccountId   string `json:"SourceAccountId"` //nolint:revive,staticcheck // existing issue.
	TargetAccountId   string `json:"TargetAccountId"` //nolint:revive,staticcheck // existing issue.
}

type createAccountLinkInvitationOutput struct {
	AccountLink accountLinkResp `json:"AccountLink"`
}

func (h *Handler) handleCreateAccountLinkInvitation(
	_ context.Context, req *createAccountLinkInvitationInput,
) (*createAccountLinkInvitationOutput, error) {
	link, err := h.Backend.CreateAccountLinkInvitation(req.TargetAccountId)
	if err != nil {
		return nil, err
	}

	return &createAccountLinkInvitationOutput{AccountLink: toAccountLinkResp(link)}, nil
}

func toAccountLinkResp(l *storedAccountLink) accountLinkResp {
	return accountLinkResp{
		AccountLinkId:     l.LinkID,
		AccountLinkStatus: l.Status,
		SourceAccountId:   l.SourceAccountID,
		TargetAccountId:   l.TargetAccountID,
	}
}

type acceptAccountLinkInvitationInput struct {
	LinkId string `json:"LinkId"` //nolint:revive,staticcheck // existing issue.
}

type acceptAccountLinkInvitationOutput struct {
	AccountLink accountLinkResp `json:"AccountLink"`
}

func (h *Handler) handleAcceptAccountLinkInvitation(
	_ context.Context, req *acceptAccountLinkInvitationInput,
) (*acceptAccountLinkInvitationOutput, error) {
	link, err := h.Backend.AcceptAccountLinkInvitation(req.LinkId)
	if err != nil {
		return nil, err
	}

	return &acceptAccountLinkInvitationOutput{AccountLink: toAccountLinkResp(link)}, nil
}

type rejectAccountLinkInvitationInput struct {
	LinkId string `json:"LinkId"` //nolint:revive,staticcheck // existing issue.
}

type rejectAccountLinkInvitationOutput struct {
	AccountLink accountLinkResp `json:"AccountLink"`
}

func (h *Handler) handleRejectAccountLinkInvitation(
	_ context.Context, req *rejectAccountLinkInvitationInput,
) (*rejectAccountLinkInvitationOutput, error) {
	link, err := h.Backend.RejectAccountLinkInvitation(req.LinkId)
	if err != nil {
		return nil, err
	}

	return &rejectAccountLinkInvitationOutput{AccountLink: toAccountLinkResp(link)}, nil
}

type deleteAccountLinkInvitationInput struct {
	LinkId string `json:"LinkId"` //nolint:revive,staticcheck // existing issue.
}

type deleteAccountLinkInvitationOutput struct {
	AccountLink accountLinkResp `json:"AccountLink"`
}

func (h *Handler) handleDeleteAccountLinkInvitation(
	_ context.Context, req *deleteAccountLinkInvitationInput,
) (*deleteAccountLinkInvitationOutput, error) {
	link, err := h.Backend.DeleteAccountLinkInvitation(req.LinkId)
	if err != nil {
		return nil, err
	}

	return &deleteAccountLinkInvitationOutput{AccountLink: toAccountLinkResp(link)}, nil
}

type getAccountLinkInput struct {
	LinkId string `json:"LinkId"` //nolint:revive,staticcheck // existing issue.
}

type getAccountLinkOutput struct {
	AccountLink accountLinkResp `json:"AccountLink"`
}

func (h *Handler) handleGetAccountLink(
	_ context.Context, req *getAccountLinkInput,
) (*getAccountLinkOutput, error) {
	link, err := h.Backend.GetAccountLink(req.LinkId)
	if err != nil {
		return nil, err
	}

	return &getAccountLinkOutput{AccountLink: toAccountLinkResp(link)}, nil
}

type listAccountLinksInput struct {
	LinkStatusFilter string `json:"LinkStatusFilter"`
	NextToken        string `json:"NextToken"`
	MaxResults       int32  `json:"MaxResults"`
}

type listAccountLinksOutput struct {
	NextToken    string            `json:"NextToken,omitempty"`
	AccountLinks []accountLinkResp `json:"AccountLinks"`
}

func (h *Handler) handleListAccountLinks(
	_ context.Context, req *listAccountLinksInput,
) (*listAccountLinksOutput, error) {
	links, nextToken, err := h.Backend.ListAccountLinks(
		req.LinkStatusFilter,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]accountLinkResp, 0, len(links))
	for _, l := range links {
		items = append(items, toAccountLinkResp(l))
	}

	return &listAccountLinksOutput{AccountLinks: items, NextToken: nextToken}, nil
}
