package cognitoidentity

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type getCredentialsForIdentityInput struct {
	Logins        map[string]string `json:"Logins"`
	IdentityID    string            `json:"IdentityId"`
	CustomRoleArn string            `json:"CustomRoleArn"`
}

type credentialsOutput struct {
	AccessKeyID  string  `json:"AccessKeyId"`
	SecretKey    string  `json:"SecretKey"`
	SessionToken string  `json:"SessionToken"`
	Expiration   float64 `json:"Expiration"`
}

type getCredentialsForIdentityOutput struct {
	IdentityID  string            `json:"IdentityId"`
	Credentials credentialsOutput `json:"Credentials"`
}

func (h *Handler) handleGetCredentialsForIdentity(
	ctx context.Context,
	in *getCredentialsForIdentityInput,
) (*getCredentialsForIdentityOutput, error) {
	creds, err := h.Backend.GetCredentialsForIdentity(ctx, in.IdentityID, in.Logins)
	if err != nil {
		return nil, err
	}

	return &getCredentialsForIdentityOutput{
		IdentityID: creds.IdentityID,
		Credentials: credentialsOutput{
			AccessKeyID:  creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
			Expiration:   awstime.Epoch(creds.Expiration),
		},
	}, nil
}

type getOpenIDTokenInput struct {
	Logins     map[string]string `json:"Logins"`
	IdentityID string            `json:"IdentityId"`
}

type getOpenIDTokenOutput struct {
	IdentityID string `json:"IdentityId"`
	Token      string `json:"Token"`
}

func (h *Handler) handleGetOpenIDToken(
	ctx context.Context,
	in *getOpenIDTokenInput,
) (*getOpenIDTokenOutput, error) {
	token, err := h.Backend.GetOpenIDToken(ctx, in.IdentityID, in.Logins)
	if err != nil {
		return nil, err
	}

	return &getOpenIDTokenOutput{
		IdentityID: token.IdentityID,
		Token:      token.Token,
	}, nil
}

type getOpenIDTokenForDeveloperIdentityInput struct {
	Logins         map[string]string `json:"Logins"`
	IdentityPoolID string            `json:"IdentityPoolId"`
	IdentityID     string            `json:"IdentityId"`
	TokenDuration  int64             `json:"TokenDuration"`
}

type getOpenIDTokenForDeveloperIdentityOutput struct {
	IdentityID string `json:"IdentityId"`
	Token      string `json:"Token"`
}

func (h *Handler) handleGetOpenIDTokenForDeveloperIdentity(
	ctx context.Context,
	in *getOpenIDTokenForDeveloperIdentityInput,
) (*getOpenIDTokenForDeveloperIdentityOutput, error) {
	result, err := h.Backend.GetOpenIDTokenForDeveloperIdentity(
		ctx,
		in.IdentityPoolID,
		in.IdentityID,
		in.Logins,
		in.TokenDuration,
	)
	if err != nil {
		return nil, err
	}

	return &getOpenIDTokenForDeveloperIdentityOutput{
		IdentityID: result.IdentityID,
		Token:      result.Token,
	}, nil
}
