package iotwireless

import (
	"cmp"
	"encoding/json"
	"net/http"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type sidewalkAssociateAccountRequest struct {
	AmazonID string `json:"AmazonId"`
}

type associatePartnerAccountRequest struct {
	Sidewalk           *sidewalkAssociateAccountRequest `json:"Sidewalk"`
	ClientRequestToken string                           `json:"ClientRequestToken"`
	Tags               []tags.KV                        `json:"Tags"`
}

type sidewalkAssociateAccountResponse struct {
	AmazonID string `json:"AmazonId"`
}

type associatePartnerAccountResponse struct {
	Sidewalk *sidewalkAssociateAccountResponse `json:"Sidewalk,omitempty"`
	Arn      string                            `json:"Arn"`
}

type sidewalkAccountInfo struct {
	AmazonID string `json:"AmazonId,omitempty"`
	Arn      string `json:"Arn,omitempty"`
}

type getPartnerAccountResponse struct {
	Sidewalk      *sidewalkAccountInfo `json:"Sidewalk,omitempty"`
	AccountLinked bool                 `json:"AccountLinked"`
}

type listPartnerAccountsResponse struct {
	NextToken string                `json:"NextToken"`
	Sidewalk  []sidewalkAccountInfo `json:"Sidewalk"`
}

func (h *Handler) associateAwsAccountWithPartnerAccount(c *echo.Context, body []byte) error {
	var req associatePartnerAccountRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Sidewalk == nil || req.Sidewalk.AmazonID == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException: Sidewalk.AmazonId is required")
	}

	arn, err := h.Backend.AssociateAwsAccountWithPartnerAccount(
		h.AccountID,
		h.DefaultRegion,
		req.Sidewalk.AmazonID,
		tagKVsToMap(req.Tags),
	)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, err.Error())
	}

	return writeJSON(c, http.StatusOK, associatePartnerAccountResponse{
		Arn:      arn,
		Sidewalk: &sidewalkAssociateAccountResponse{AmazonID: req.Sidewalk.AmazonID},
	})
}

func (h *Handler) getPartnerAccount(c *echo.Context, partnerAccountID string) error {
	arn, err := h.Backend.GetPartnerAccount(partnerAccountID)
	if err != nil {
		// GetPartnerAccount reports link status rather than 404ing on an
		// unlinked account — matching AWS's use of it as a link-status check.
		return writeJSON(c, http.StatusOK, getPartnerAccountResponse{AccountLinked: false})
	}

	return writeJSON(c, http.StatusOK, getPartnerAccountResponse{
		AccountLinked: true,
		Sidewalk:      &sidewalkAccountInfo{AmazonID: partnerAccountID, Arn: arn},
	})
}

func (h *Handler) listPartnerAccounts(c *echo.Context) error {
	accounts := h.Backend.ListPartnerAccounts()

	all := make([]sidewalkAccountInfo, 0, len(accounts))
	for id, arn := range accounts {
		all = append(all, sidewalkAccountInfo{AmazonID: id, Arn: arn})
	}

	// Sort for deterministic, pagination-stable ordering -- ListPartnerAccounts
	// returns a map, whose iteration order Go randomizes on every call.
	slices.SortFunc(all, func(a, b sidewalkAccountInfo) int {
		return cmp.Compare(a.AmazonID, b.AmazonID)
	})

	pg, next := paginateQuery(c, all)

	return writeJSON(c, http.StatusOK, listPartnerAccountsResponse{Sidewalk: pg, NextToken: next})
}

func (h *Handler) disassociateAwsAccountFromPartnerAccount(
	c *echo.Context,
	partnerAccountID string,
) error {
	// Ignore not-found; return 204 for idempotency.
	_ = h.Backend.DisassociateAwsAccountFromPartnerAccount(partnerAccountID)

	return stubNoContent(c)
}

func (h *Handler) updatePartnerAccount(c *echo.Context, partnerAccountID string) error {
	if _, err := h.Backend.GetPartnerAccount(partnerAccountID); err != nil {
		return handleError(c, err)
	}

	return stubNoContent(c)
}
