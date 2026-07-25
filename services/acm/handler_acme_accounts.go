package acm

import (
	"context"
	"encoding/json"
)

type acmeAccountWire struct {
	AccountURL                    string   `json:"AccountUrl"`
	AcmeExternalAccountBindingArn string   `json:"AcmeExternalAccountBindingArn,omitempty"`
	PublicKeyThumbprint           string   `json:"PublicKeyThumbprint,omitempty"`
	Status                        string   `json:"Status"`
	Contacts                      []string `json:"Contacts,omitempty"`
	CreatedAt                     int64    `json:"CreatedAt"`
}

func acmeAccountToWire(a *AcmeAccount) acmeAccountWire {
	return acmeAccountWire{
		AccountURL:                    a.AccountURL,
		AcmeExternalAccountBindingArn: a.AcmeExternalAccountBindingArn,
		PublicKeyThumbprint:           a.PublicKeyThumbprint,
		Status:                        a.Status,
		Contacts:                      a.Contacts,
		CreatedAt:                     a.CreatedAt.Unix(),
	}
}

type describeAcmeAccountInput struct {
	AccountURL      string `json:"AccountUrl"`
	AcmeEndpointArn string `json:"AcmeEndpointArn"`
}

type describeAcmeAccountOutput struct {
	AcmeAccount *acmeAccountWire `json:"AcmeAccount"`
}

func (h *Handler) jsonDescribeAcmeAccount(ctx context.Context, body []byte) (any, error) {
	var input describeAcmeAccountInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	acct, err := h.Backend.DescribeAcmeAccount(ctx, input.AcmeEndpointArn, input.AccountURL)
	if err != nil {
		return nil, err
	}

	wire := acmeAccountToWire(acct)

	return &describeAcmeAccountOutput{AcmeAccount: &wire}, nil
}

type listAcmeAccountsInput struct {
	AcmeEndpointArn string `json:"AcmeEndpointArn"`
	NextToken       string `json:"NextToken"`
	MaxResults      int32  `json:"MaxResults"`
}

type listAcmeAccountsOutput struct {
	NextToken    string            `json:"NextToken,omitempty"`
	AcmeAccounts []acmeAccountWire `json:"AcmeAccounts"`
}

func (h *Handler) jsonListAcmeAccounts(ctx context.Context, body []byte) (any, error) {
	var input listAcmeAccountsInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	pg, err := h.Backend.ListAcmeAccounts(ctx, input.AcmeEndpointArn, input.NextToken, int(input.MaxResults))
	if err != nil {
		return nil, err
	}

	out := make([]acmeAccountWire, 0, len(pg.Data))
	for i := range pg.Data {
		out = append(out, acmeAccountToWire(&pg.Data[i]))
	}

	return &listAcmeAccountsOutput{AcmeAccounts: out, NextToken: pg.Next}, nil
}

type revokeAcmeAccountInput struct {
	AccountURL      string `json:"AccountUrl"`
	AcmeEndpointArn string `json:"AcmeEndpointArn"`
}

type revokeAcmeAccountOutput struct{}

func (h *Handler) jsonRevokeAcmeAccount(ctx context.Context, body []byte) (any, error) {
	var input revokeAcmeAccountInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.RevokeAcmeAccount(ctx, input.AcmeEndpointArn, input.AccountURL); err != nil {
		return nil, err
	}

	return &revokeAcmeAccountOutput{}, nil
}
