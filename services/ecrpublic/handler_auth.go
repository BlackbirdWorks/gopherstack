package ecrpublic

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) buildAuthOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetAuthorizationToken": service.WrapOp(h.handleGetAuthorizationToken),
	}
}

type getAuthorizationTokenInput struct{}

type getAuthorizationTokenOutput struct {
	AuthorizationData *AuthorizationDataWire `json:"authorizationData,omitempty"`
}

func (h *Handler) handleGetAuthorizationToken(
	ctx context.Context, _ *getAuthorizationTokenInput,
) (*getAuthorizationTokenOutput, error) {
	token, expiresAt, err := h.Backend.GetAuthorizationToken(ctx)
	if err != nil {
		return nil, err
	}

	return &getAuthorizationTokenOutput{
		AuthorizationData: &AuthorizationDataWire{
			AuthorizationToken: token,
			ExpiresAt:          float64(expiresAt),
		},
	}, nil
}
