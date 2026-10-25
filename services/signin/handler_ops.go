package signin

import (
	"context"
	"net/http"
	"net/url"
	"strings"
)

func operationTable() map[string]dispatchFunc {
	return map[string]dispatchFunc{
		opCreateOAuth2Token:                       (*Handler).createOAuth2Token,
		opCreateOAuth2TokenWithIAM:                (*Handler).createOAuth2TokenWithIAM,
		opIntrospectOAuth2TokenWithIAM:            (*Handler).introspect,
		opRevokeOAuth2TokenWithIAM:                (*Handler).revoke,
		opPutConsoleAuthorizationConfiguration:    (*Handler).putConsoleConfig,
		opGetConsoleAuthorizationConfiguration:    (*Handler).getConsoleConfig,
		opDeleteConsoleAuthorizationConfiguration: (*Handler).deleteConsoleConfig,
		opGetResourcePolicy:                       (*Handler).getResourcePolicy,
		opPutResourcePermissionStatement:          (*Handler).putStatement,
		opDeleteResourcePermissionStatement:       (*Handler).deleteStatement,
		opListResourcePermissionStatements:        (*Handler).listStatements,
	}
}

func (h *Handler) createOAuth2Token(_ context.Context, body []byte, r *http.Request) (int, any, error) {
	var in tokenRequestWire

	if strings.HasPrefix(r.Header.Get("Content-Type"), formContent) {
		q, err := url.ParseQuery(string(body))
		if err != nil {
			return 0, nil, validation("malformed form body")
		}

		in = tokenRequestWire{
			ClientID: q.Get("client_id"), GrantType: q.Get("grant_type"), Code: q.Get("code"),
			CodeVerifier: q.Get("code_verifier"), RedirectURI: q.Get("redirect_uri"),
			RefreshToken: q.Get("refresh_token"),
		}
	} else if err := decode(body, &in); err != nil {
		return 0, nil, err
	}

	res, err := h.Backend.CreateOAuth2Token(in.request())
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, tokenResponseBodyWire{
		AccessToken: accessTokenWire{
			AccessKeyID: res.AccessKeyID, SecretAccessKey: res.SecretKey, SessionToken: res.SessionToken,
		},
		TokenType: tokenTypeSigV4, RefreshToken: res.RefreshToken, IDToken: res.IDToken, ExpiresIn: res.ExpiresIn,
	}, nil
}

func (h *Handler) createOAuth2TokenWithIAM(ctx context.Context, body []byte, _ *http.Request) (int, any, error) {
	var in createTokenIAMInputWire
	if err := decode(body, &in); err != nil {
		return 0, nil, err
	}

	res, err := h.Backend.CreateOAuth2TokenWithIAM(ctx, in.GrantType, in.Resource)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, createTokenIAMOutputWire{
		AccessToken: res.AccessToken, TokenType: tokenTypeBearer, ExpiresIn: res.ExpiresIn,
	}, nil
}

func ptrIf(v int64) *int64 {
	if v == 0 {
		return nil
	}

	return &v
}

func (h *Handler) introspect(ctx context.Context, body []byte, _ *http.Request) (int, any, error) {
	var in introspectInputWire
	if err := decode(body, &in); err != nil {
		return 0, nil, err
	}

	res, err := h.Backend.IntrospectOAuth2Token(ctx, in.Token, in.TokenTypeHint)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, introspectOutputWire{
		Active: res.Active, AccountID: res.Account, Aud: res.Aud, ClientID: res.ClientID, Exp: ptrIf(res.Exp),
		Iat: ptrIf(res.Iat), Iss: res.Iss, Jti: res.Jti, Nbf: ptrIf(res.Nbf), Resource: res.Resource,
		Session: res.Session, Sub: res.Sub, TokenType: res.TokenType, UserID: res.UserID,
	}, nil
}

func (h *Handler) revoke(ctx context.Context, body []byte, _ *http.Request) (int, any, error) {
	var in revokeInputWire
	if err := decode(body, &in); err != nil {
		return 0, nil, err
	}

	if err := h.Backend.RevokeOAuth2Token(ctx, in.Token); err != nil {
		return 0, nil, err
	}

	return http.StatusOK, nil, nil
}

func consoleOp(
	fn func(*InMemoryBackend, context.Context, string) (*ConsoleAuthorization, error),
) dispatchFunc {
	return func(h *Handler, ctx context.Context, body []byte, _ *http.Request) (int, any, error) {
		var in consoleConfigInputWire
		if err := decode(body, &in); err != nil {
			return 0, nil, err
		}

		res, err := fn(h.Backend, ctx, in.TargetID)
		if err != nil {
			return 0, nil, err
		}

		return http.StatusOK, consoleConfigOutputWire{
			Scope: res.Scope, TargetID: res.TargetID, ConsoleAuthorizationEnabled: res.Enabled,
		}, nil
	}
}

func (h *Handler) putConsoleConfig(ctx context.Context, body []byte, r *http.Request) (int, any, error) {
	return consoleOp((*InMemoryBackend).PutConsoleAuthorizationConfiguration)(h, ctx, body, r)
}

func (h *Handler) getConsoleConfig(ctx context.Context, body []byte, r *http.Request) (int, any, error) {
	return consoleOp((*InMemoryBackend).GetConsoleAuthorizationConfiguration)(h, ctx, body, r)
}

func (h *Handler) deleteConsoleConfig(ctx context.Context, body []byte, r *http.Request) (int, any, error) {
	return consoleOp((*InMemoryBackend).DeleteConsoleAuthorizationConfiguration)(h, ctx, body, r)
}

func (h *Handler) getResourcePolicy(ctx context.Context, _ []byte, _ *http.Request) (int, any, error) {
	sts, version, err := h.Backend.GetResourcePolicy(ctx)
	if err != nil {
		return 0, nil, err
	}

	out := make([]policyStatementWire, len(sts))
	for i, s := range sts {
		out[i] = policyStatementWire(s)
	}

	return http.StatusOK, getPolicyOutputWire{Policy: policyWire{Version: version, Statement: out}}, nil
}

func (h *Handler) putStatement(ctx context.Context, body []byte, _ *http.Request) (int, any, error) {
	var in putStatementInputWire
	if err := decode(body, &in); err != nil {
		return 0, nil, err
	}

	sid, err := h.Backend.PutResourcePermissionStatement(ctx, StatementInput(in))
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, statementIDWire{StatementID: sid}, nil
}

func (h *Handler) deleteStatement(ctx context.Context, body []byte, _ *http.Request) (int, any, error) {
	var in deleteStatementInputWire
	if err := decode(body, &in); err != nil {
		return 0, nil, err
	}

	if err := h.Backend.DeleteResourcePermissionStatement(ctx, in.StatementID); err != nil {
		return 0, nil, err
	}

	return http.StatusOK, struct{}{}, nil
}

func (h *Handler) listStatements(ctx context.Context, body []byte, _ *http.Request) (int, any, error) {
	var in listStatementsInputWire
	if err := decode(body, &in); err != nil {
		return 0, nil, err
	}

	limit := 0
	if in.MaxResults != nil {
		limit = int(*in.MaxResults)
		if limit < 1 {
			return 0, nil, validation("maxResults must be positive")
		}
	}

	pg, err := h.Backend.ListResourcePermissionStatements(ctx, in.NextToken, limit)
	if err != nil {
		return 0, nil, err
	}

	out := listStatementsOutputWire{
		NextToken:            pg.Next,
		PermissionStatements: make([]statementSummaryWire, len(pg.Data)),
	}
	for i, s := range pg.Data {
		out.PermissionStatements[i] = statementSummaryWire{SID: s.SID, Condition: s.Condition}
	}

	return http.StatusOK, out, nil
}
