package workmail

import (
	"context"
)

// ---- Personal Access Tokens ----

type deletePersonalAccessTokenReq struct {
	OrganizationID        string `json:"OrganizationId"`
	PersonalAccessTokenId string `json:"PersonalAccessTokenId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeletePersonalAccessToken(
	_ context.Context, req *deletePersonalAccessTokenReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeletePersonalAccessToken(req.OrganizationID, req.PersonalAccessTokenId)
}

type getPersonalAccessTokenMetadataReq struct {
	OrganizationID        string `json:"OrganizationId"`
	PersonalAccessTokenId string `json:"PersonalAccessTokenId"` //nolint:revive,staticcheck // existing issue.
}

type personalAccessTokenMetadataResp struct {
	PersonalAccessTokenId string   `json:"PersonalAccessTokenId,omitempty"` //nolint:revive,staticcheck // existing issue.
	UserId                string   `json:"UserId,omitempty"`                //nolint:revive // existing issue.
	Name                  string   `json:"Name,omitempty"`
	Scopes                []string `json:"Scopes,omitempty"`
	DateCreated           int64    `json:"DateCreated,omitempty"`
	DateLastUsed          int64    `json:"DateLastUsed,omitempty"`
	ExpiresTime           int64    `json:"ExpiresTime,omitempty"`
}

func (h *Handler) handleGetPersonalAccessTokenMetadata(
	_ context.Context, req *getPersonalAccessTokenMetadataReq,
) (*personalAccessTokenMetadataResp, error) {
	tok, err := h.Backend.GetPersonalAccessTokenMetadata(req.OrganizationID, req.PersonalAccessTokenId)
	if err != nil {
		return nil, err
	}

	return &personalAccessTokenMetadataResp{
		PersonalAccessTokenId: tok.TokenID,
		UserId:                tok.UserID,
		Name:                  tok.Name,
		DateCreated:           tok.DateCreated.Unix(),
		DateLastUsed:          tok.DateLastUsed.Unix(),
		ExpiresTime:           tok.ExpiresTime.Unix(),
		Scopes:                tok.Scopes,
	}, nil
}

type listPersonalAccessTokensReq struct {
	OrganizationID string `json:"OrganizationId"`
	UserId         string `json:"UserId"` //nolint:revive,staticcheck // existing issue.
	MaxResults     *int32 `json:"MaxResults"`
	NextToken      string `json:"NextToken"`
}

type personalAccessTokenSummaryJSON struct {
	PersonalAccessTokenId string   `json:"PersonalAccessTokenId"` //nolint:revive,staticcheck // existing issue.
	UserId                string   `json:"UserId,omitempty"`      //nolint:revive // existing issue.
	Name                  string   `json:"Name,omitempty"`
	Scopes                []string `json:"Scopes,omitempty"`
	DateCreated           int64    `json:"DateCreated,omitempty"`
	DateLastUsed          int64    `json:"DateLastUsed,omitempty"`
	ExpiresTime           int64    `json:"ExpiresTime,omitempty"`
}

type listPersonalAccessTokensResp struct {
	NextToken                    string                           `json:"NextToken,omitempty"`
	PersonalAccessTokenSummaries []personalAccessTokenSummaryJSON `json:"PersonalAccessTokenSummaries"`
}

func (h *Handler) handleListPersonalAccessTokens(
	_ context.Context, req *listPersonalAccessTokensReq,
) (*listPersonalAccessTokensResp, error) {
	maxResults := int32(0)
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}
	tokens, next, err := h.Backend.ListPersonalAccessTokens(req.OrganizationID, req.UserId, maxResults, req.NextToken)
	if err != nil {
		return nil, err
	}
	result := make([]personalAccessTokenSummaryJSON, 0, len(tokens))
	for _, tok := range tokens {
		result = append(result, personalAccessTokenSummaryJSON{
			PersonalAccessTokenId: tok.TokenID,
			UserId:                tok.UserID,
			Name:                  tok.Name,
			DateCreated:           tok.DateCreated.Unix(),
			DateLastUsed:          tok.DateLastUsed.Unix(),
			ExpiresTime:           tok.ExpiresTime.Unix(),
			Scopes:                tok.Scopes,
		})
	}

	return &listPersonalAccessTokensResp{PersonalAccessTokenSummaries: result, NextToken: next}, nil
}
