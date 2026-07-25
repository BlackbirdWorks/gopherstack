package cognitoidp

import (
	"context"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/labstack/echo/v5"
)

const (
	medEmail = "EMAIL"
)

func (h *Handler) handleJWKS(c *echo.Context) error {
	path := c.Request().URL.Path
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.Split(trimmed, "/")

	if len(parts) == 0 || parts[0] == "" {
		return c.JSON(http.StatusBadRequest, service.JSONErrorResponse{
			Type:    errInvalidParameterException,
			Message: "missing user pool ID in path",
		})
	}

	userPoolID := parts[0]

	jwks, err := h.Backend.GetUserPoolJWKS(userPoolID)
	if err != nil {
		return c.JSON(http.StatusNotFound, service.JSONErrorResponse{
			Type:    ErrUserPoolNotFound.Error(),
			Message: err.Error(),
		})
	}

	data, marshalErr := jwksResponseJSON(*jwks)
	if marshalErr != nil {
		return c.JSON(http.StatusInternalServerError, service.JSONErrorResponse{
			Type:    "InternalFailure",
			Message: marshalErr.Error(),
		})
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusOK, data)
}

// authResultFromTokenResult converts a TokenResult to an authResult.
func authResultFromTokenResult(tokens *TokenResult) *authResult {
	return &authResult{
		AccessToken:  tokens.AccessToken,
		IDToken:      tokens.IDToken,
		RefreshToken: tokens.RefreshToken,
		TokenType:    authTypeBearer,
		ExpiresIn:    tokens.ExpiresIn,
	}
}

// authOutputFromResult converts an AuthResult to an authOutput.
func authOutputFromResult(result *AuthResult) *authOutput {
	if result.MFASession != "" {
		name := result.ChallengeName

		params := result.ChallengeParameters
		if params == nil {
			params = map[string]string{}
		}

		return &authOutput{
			ChallengeName:       &name,
			Session:             &result.MFASession,
			ChallengeParameters: params,
		}
	}

	return &authOutput{
		AuthenticationResult: authResultFromTokenResult(result.Tokens),
	}
}

func (h *Handler) handleAdminConfirmSignUp(
	_ context.Context,
	in *adminConfirmSignUpInput,
) (*adminConfirmSignUpOutput, error) {
	if err := h.Backend.AdminConfirmSignUp(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminConfirmSignUpOutput{}, nil
}

func (h *Handler) handleChangePassword(
	_ context.Context,
	in *changePasswordInput,
) (*changePasswordOutput, error) {
	if err := h.Backend.ChangePassword(in.AccessToken, in.PreviousPassword, in.ProposedPassword); err != nil {
		return nil, err
	}

	return &changePasswordOutput{}, nil
}

func (h *Handler) handleAdminResetUserPassword(
	_ context.Context,
	in *adminResetUserPasswordInput,
) (*adminResetUserPasswordOutput, error) {
	if err := h.Backend.AdminResetUserPassword(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminResetUserPasswordOutput{}, nil
}

func (h *Handler) handleRespondToAuthChallengeAccurate(
	_ context.Context,
	in *respondToAuthChallengeAccurateInput,
) (*respondToAuthChallengeAccurateOutput, error) {
	switch in.ChallengeName {
	case challengeSoftwareTokenMFA:
		totpCode := in.ChallengeResponses["SOFTWARE_TOKEN_MFA_CODE"]

		tokens, err := h.Backend.RespondToMFAChallenge(in.ClientID, in.Session, totpCode)
		if err != nil {
			return nil, err
		}

		return &respondToAuthChallengeAccurateOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengeSMSMFA:
		// SMS MFA: accept any numeric code (simulation — no real SMS gateway).
		totpCode := in.ChallengeResponses["SMS_MFA_CODE"]

		tokens, err := h.Backend.RespondToMFAChallenge(in.ClientID, in.Session, totpCode)
		if err != nil {
			return nil, err
		}

		return &respondToAuthChallengeAccurateOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengeEmailOTP:
		// EMAIL_OTP: accept any numeric code (simulation).
		otpCode := in.ChallengeResponses["EMAIL_OTP_CODE"]

		tokens, err := h.Backend.RespondToMFAChallenge(in.ClientID, in.Session, otpCode)
		if err != nil {
			return nil, err
		}

		return &respondToAuthChallengeAccurateOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengeNewPasswordRequired:
		newPassword := in.ChallengeResponses["NEW_PASSWORD"]

		tokens, err := h.Backend.RespondToNewPasswordRequired(in.ClientID, in.Session, newPassword)
		if err != nil {
			return nil, err
		}

		return &respondToAuthChallengeAccurateOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengePasswordVerifier:
		// USER_SRP_AUTH second step: credentials were verified in InitiateAuth; just issue tokens.
		tokens, err := h.Backend.RespondToSRPChallenge(in.ClientID, in.Session)
		if err != nil {
			return nil, err
		}

		return &respondToAuthChallengeAccurateOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengeCustomChallenge:
		answer := in.ChallengeResponses["ANSWER"]

		result, err := h.Backend.RespondToCustomAuthChallenge(in.ClientID, in.Session, answer)
		if err != nil {
			return nil, err
		}

		return respondToAuthChallengeOutputFromResult(result), nil

	default:
		return &respondToAuthChallengeAccurateOutput{}, nil
	}
}

// respondToAuthChallengeOutputFromResult converts an AuthResult from a CUSTOM_AUTH
// round into a respondToAuthChallengeAccurateOutput: either completed tokens, or
// another pending challenge (same MFASession/ChallengeName/ChallengeParameters shape
// authOutputFromResult uses for InitiateAuth, since CUSTOM_AUTH is the only flow whose
// RespondToAuthChallenge can itself return a further challenge rather than always
// terminating in tokens or an error).
func respondToAuthChallengeOutputFromResult(result *AuthResult) *respondToAuthChallengeAccurateOutput {
	if result.MFASession != "" {
		params := result.ChallengeParameters
		if params == nil {
			params = map[string]string{}
		}

		return &respondToAuthChallengeAccurateOutput{
			ChallengeName:       result.ChallengeName,
			Session:             result.MFASession,
			ChallengeParameters: params,
		}
	}

	return &respondToAuthChallengeAccurateOutput{
		AuthenticationResult: authResultFromTokenResult(result.Tokens),
	}
}

func (h *Handler) handleAdminRespondToAuthChallengeAccurate(
	_ context.Context,
	in *adminRespondToAuthChallengeInput,
) (*adminRespondToAuthChallengeOutput, error) {
	switch in.ChallengeName {
	case challengeSoftwareTokenMFA, challengeSMSMFA, challengeEmailOTP:
		var code string

		switch in.ChallengeName {
		case challengeSoftwareTokenMFA:
			code = in.ChallengeResponses["SOFTWARE_TOKEN_MFA_CODE"]
		case challengeSMSMFA:
			code = in.ChallengeResponses["SMS_MFA_CODE"]
		case challengeEmailOTP:
			code = in.ChallengeResponses["EMAIL_OTP_CODE"]
		}

		tokens, err := h.Backend.RespondToMFAChallenge(in.ClientID, in.Session, code)
		if err != nil {
			return nil, err
		}

		return &adminRespondToAuthChallengeOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengeNewPasswordRequired:
		newPassword := in.ChallengeResponses["NEW_PASSWORD"]

		tokens, err := h.Backend.RespondToNewPasswordRequired(in.ClientID, in.Session, newPassword)
		if err != nil {
			return nil, err
		}

		return &adminRespondToAuthChallengeOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengePasswordVerifier:
		tokens, err := h.Backend.RespondToSRPChallenge(in.ClientID, in.Session)
		if err != nil {
			return nil, err
		}

		return &adminRespondToAuthChallengeOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengeCustomChallenge:
		answer := in.ChallengeResponses["ANSWER"]

		result, err := h.Backend.RespondToCustomAuthChallenge(in.ClientID, in.Session, answer)
		if err != nil {
			return nil, err
		}

		if result.MFASession != "" {
			params := result.ChallengeParameters
			if params == nil {
				params = map[string]string{}
			}

			return &adminRespondToAuthChallengeOutput{
				ChallengeName:       result.ChallengeName,
				Session:             result.MFASession,
				ChallengeParameters: params,
			}, nil
		}

		return &adminRespondToAuthChallengeOutput{
			AuthenticationResult: authResultFromTokenResult(result.Tokens),
		}, nil

	default:
		return &adminRespondToAuthChallengeOutput{}, nil
	}
}

func (h *Handler) handleSignUpAccurate(
	_ context.Context,
	in *signUpAccurateInput,
) (*signUpAccurateOutput, error) {
	if err := h.Backend.ValidateSecretHash(in.ClientID, in.Username, in.SecretHash); err != nil {
		return nil, err
	}

	attrs := attributeListToMap(in.UserAttributes)

	user, err := h.Backend.SignUpWithValidation(in.ClientID, in.Username, in.Password, attrs)
	if err != nil {
		return nil, err
	}

	out := &signUpAccurateOutput{
		UserSub:       user.Sub,
		UserConfirmed: user.Status == UserStatusConfirmed,
	}

	if user.ConfirmCode != "" {
		out.CodeDeliveryDetails = map[string]string{
			keyDeliveryMedium:   medEmail,
			keyDestination:      mockDestination,
			keyAttributeName:    attrEmail,
			keyConfirmationCode: user.ConfirmCode,
		}

		message, subject, cmErr := h.Backend.InvokeCustomMessageTrigger(
			in.ClientID, in.Username, user.ConfirmCode, triggerSourceCustomMessageSignUp,
		)
		if cmErr != nil {
			return nil, cmErr
		}

		if message != "" {
			out.CodeDeliveryDetails[keyCustomMessage] = message
		}

		if subject != "" {
			out.CodeDeliveryDetails[keyCustomMessageSubject] = subject
		}
	}

	return out, nil
}

func (h *Handler) handleInitiateAuthAccurate(
	_ context.Context,
	in *initiateAuthAccurateInput,
) (*authOutput, error) {
	username := in.AuthParameters["USERNAME"]

	if err := h.Backend.ValidateSecretHash(
		in.ClientID, username, in.AuthParameters["SECRET_HASH"],
	); err != nil {
		return nil, err
	}

	if in.AuthFlow == authFlowRefreshTokenAuth || in.AuthFlow == authFlowRefreshToken {
		refreshToken := in.AuthParameters[authFlowRefreshToken]

		tokens, err := h.Backend.InitiateAuthRefreshToken(in.ClientID, refreshToken)
		if err != nil {
			return nil, err
		}

		return &authOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil
	}

	password := in.AuthParameters["PASSWORD"]

	result, err := h.Backend.InitiateAuth(in.ClientID, in.AuthFlow, username, password)
	if err != nil {
		return nil, err
	}

	return authOutputFromResult(result), nil
}

func (h *Handler) handleAdminInitiateAuthAccurate(
	_ context.Context,
	in *adminInitiateAuthAccurateInput,
) (*authOutput, error) {
	username := in.AuthParameters["USERNAME"]

	if err := h.Backend.ValidateSecretHash(
		in.ClientID, username, in.AuthParameters["SECRET_HASH"],
	); err != nil {
		return nil, err
	}

	if in.AuthFlow == authFlowRefreshTokenAuth || in.AuthFlow == authFlowRefreshToken {
		refreshToken := in.AuthParameters[authFlowRefreshToken]

		tokens, err := h.Backend.InitiateAuthRefreshToken(in.ClientID, refreshToken)
		if err != nil {
			return nil, err
		}

		return &authOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil
	}

	password := in.AuthParameters["PASSWORD"]

	result, err := h.Backend.AdminInitiateAuth(in.UserPoolID, in.ClientID, in.AuthFlow, username, password)
	if err != nil {
		return nil, err
	}

	return authOutputFromResult(result), nil
}

func (h *Handler) handleConfirmSignUpAccurate(
	_ context.Context,
	in *confirmSignUpAccurateInput,
) (*confirmSignUpAccurateOutput, error) {
	if err := h.Backend.ValidateSecretHash(in.ClientID, in.Username, in.SecretHash); err != nil {
		return nil, err
	}

	if err := h.Backend.ConfirmSignUp(in.ClientID, in.Username, in.ConfirmationCode); err != nil {
		return nil, err
	}

	return &confirmSignUpAccurateOutput{}, nil
}

func (h *Handler) handleForgotPasswordAccurate(
	_ context.Context,
	in *forgotPasswordAccurateInput,
) (*forgotPasswordAccurateOutput, error) {
	if err := h.Backend.ValidateSecretHash(in.ClientID, in.Username, in.SecretHash); err != nil {
		return nil, err
	}

	code, err := h.Backend.ForgotPassword(in.ClientID, in.Username)
	if err != nil {
		return nil, err
	}

	details := map[string]string{
		keyDestination:      "mock@example.com",
		keyDeliveryMedium:   medEmail,
		keyAttributeName:    attrEmail,
		keyConfirmationCode: code,
	}

	message, subject, cmErr := h.Backend.InvokeCustomMessageTrigger(
		in.ClientID, in.Username, code, triggerSourceCustomMessageForgotPwd,
	)
	if cmErr != nil {
		return nil, cmErr
	}

	if message != "" {
		details[keyCustomMessage] = message
	}

	if subject != "" {
		details[keyCustomMessageSubject] = subject
	}

	return &forgotPasswordAccurateOutput{CodeDeliveryDetails: details}, nil
}

func (h *Handler) handleConfirmForgotPasswordAccurate(
	_ context.Context,
	in *confirmForgotPasswordAccurateInput,
) (*confirmForgotPasswordAccurateOutput, error) {
	if err := h.Backend.ValidateSecretHash(in.ClientID, in.Username, in.SecretHash); err != nil {
		return nil, err
	}

	if err := h.Backend.ConfirmForgotPassword(
		in.ClientID, in.Username, in.ConfirmationCode, in.Password,
	); err != nil {
		return nil, err
	}

	return &confirmForgotPasswordAccurateOutput{}, nil
}

func (h *Handler) handleResendConfirmationCodeAccurate(
	_ context.Context,
	in *resendConfirmationCodeAccurateInput,
) (*resendConfirmationCodeAccurateOutput, error) {
	if err := h.Backend.ValidateSecretHash(in.ClientID, in.Username, in.SecretHash); err != nil {
		return nil, err
	}

	code, err := h.Backend.ResendConfirmationCode(in.ClientID, in.Username)
	if err != nil {
		return nil, err
	}

	details := map[string]string{
		keyDeliveryMedium:   medEmail,
		keyDestination:      mockDestination,
		keyAttributeName:    attrEmail,
		keyConfirmationCode: code,
	}

	message, subject, cmErr := h.Backend.InvokeCustomMessageTrigger(
		in.ClientID, in.Username, code, triggerSourceCustomMessageResendCode,
	)
	if cmErr != nil {
		return nil, cmErr
	}

	if message != "" {
		details[keyCustomMessage] = message
	}

	if subject != "" {
		details[keyCustomMessageSubject] = subject
	}

	return &resendConfirmationCodeAccurateOutput{CodeDeliveryDetails: details}, nil
}

// authOpsA registers the ops in this file that have no SecretHash-validating
// "*Accurate" twin in authOpsC (SignUp, ConfirmSignUp, InitiateAuth,
// AdminInitiateAuth, ForgotPassword, ConfirmForgotPassword,
// ResendConfirmationCode, and RespondToAuthChallenge all moved to authOpsC
// only -- see de-stub-hygiene note in PARITY.md).
func (h *Handler) authOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AdminConfirmSignUp":     service.WrapOp(h.handleAdminConfirmSignUp),
		"AdminResetUserPassword": service.WrapOp(h.handleAdminResetUserPassword),
		"ChangePassword":         service.WrapOp(h.handleChangePassword),
	}
}

func (h *Handler) authOpsC() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opSignUp:                      wrapAccuracy(h.handleSignUpAccurate),
		opRespondToAuthChallenge:      wrapAccuracy(h.handleRespondToAuthChallengeAccurate),
		opAdminRespondToAuthChallenge: wrapAccuracy(h.handleAdminRespondToAuthChallengeAccurate),
		opInitiateAuth:                wrapAccuracy(h.handleInitiateAuthAccurate),
		opAdminInitiateAuth:           wrapAccuracy(h.handleAdminInitiateAuthAccurate),
		opConfirmSignUp:               wrapAccuracy(h.handleConfirmSignUpAccurate),
		opForgotPassword:              wrapAccuracy(h.handleForgotPasswordAccurate),
		opConfirmForgotPassword:       wrapAccuracy(h.handleConfirmForgotPasswordAccurate),
		opResendConfirmationCode:      wrapAccuracy(h.handleResendConfirmationCodeAccurate),
	}
}
