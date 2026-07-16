package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) handleRevokeToken(_ context.Context, in *revokeTokenInput) (*revokeTokenOutput, error) {
	if err := h.Backend.RevokeToken(in.Token, in.ClientID); err != nil {
		return nil, err
	}

	return &revokeTokenOutput{}, nil
}

func (h *Handler) handleAdminUserGlobalSignOut(
	_ context.Context,
	in *adminUserGlobalSignOutInput,
) (*adminUserGlobalSignOutOutput, error) {
	if err := h.Backend.AdminUserGlobalSignOut(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminUserGlobalSignOutOutput{}, nil
}

func (h *Handler) handleGlobalSignOut(_ context.Context, in *globalSignOutInput) (*globalSignOutOutput, error) {
	if err := h.Backend.GlobalSignOut(in.AccessToken); err != nil {
		return nil, err
	}

	return &globalSignOutOutput{}, nil
}

func (h *Handler) handleGetSigningCertificate(
	_ context.Context,
	in *getSigningCertificateInput,
) (*getSigningCertificateOutput, error) {
	cert, err := h.Backend.GetSigningCertificate(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	return &getSigningCertificateOutput{Certificate: cert}, nil
}

func (h *Handler) handleGetTokensFromRefreshToken(
	_ context.Context,
	in *getTokensFromRefreshTokenInput,
) (*getTokensFromRefreshTokenOutput, error) {
	tokens, err := h.Backend.InitiateAuthRefreshToken(in.ClientID, in.RefreshToken)
	if err != nil {
		return nil, err
	}

	return &getTokensFromRefreshTokenOutput{
		AuthenticationResult: &authResult{
			AccessToken:  tokens.AccessToken,
			IDToken:      tokens.IDToken,
			RefreshToken: tokens.RefreshToken,
			TokenType:    authTypeBearer,
			ExpiresIn:    tokens.ExpiresIn,
		},
	}, nil
}

func (h *Handler) authTokensOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"RevokeToken":            service.WrapOp(h.handleRevokeToken),
		"AdminUserGlobalSignOut": service.WrapOp(h.handleAdminUserGlobalSignOut),
		"GlobalSignOut":          service.WrapOp(h.handleGlobalSignOut),
		"GetSigningCertificate":  service.WrapOp(h.handleGetSigningCertificate),
	}
}

func (h *Handler) authTokensOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetTokensFromRefreshToken": service.WrapOp(h.handleGetTokensFromRefreshToken),
	}
}
