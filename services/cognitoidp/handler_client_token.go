package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) handleGetClientToken(_ context.Context, in *getClientTokenInput) (*getClientTokenOutput, error) {
	token, expires, err := h.Backend.GetClientToken(in.ClientID, in.Secret, in.Scopes, in.ClientMetadata)
	if err != nil {
		return nil, err
	}

	return &getClientTokenOutput{ClientAuthenticationResult: &clientAuthenticationResultType{
		AccessToken: token,
		ExpiresIn:   expires,
		TokenType:   authTypeBearer,
	}}, nil
}

func (h *Handler) handleAdminDeleteSoftwareToken(
	_ context.Context, in *adminDeleteSoftwareTokenInput,
) (*adminDeleteSoftwareTokenOutput, error) {
	if err := h.Backend.AdminDeleteSoftwareToken(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminDeleteSoftwareTokenOutput{}, nil
}

func (h *Handler) clientTokenOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetClientToken":           service.WrapOp(h.handleGetClientToken),
		"AdminDeleteSoftwareToken": service.WrapOp(h.handleAdminDeleteSoftwareToken),
	}
}
