package signin

// Wire shapes follow aws-sdk-go-v2/service/signin serializers.go and deserializers.go (awsRestjson1).
// CreateOAuth2Token is the one operation whose request and response bodies are the bare httpPayload document.

type tokenRequestWire struct {
	ClientID          string `json:"clientId"`
	ClientIDSnake     string `json:"client_id"`
	GrantType         string `json:"grantType"`
	GrantTypeSnake    string `json:"grant_type"`
	Code              string `json:"code"`
	CodeVerifier      string `json:"codeVerifier"`
	CodeVerifierSnake string `json:"code_verifier"`
	RedirectURI       string `json:"redirectUri"`
	RedirectURISnake  string `json:"redirect_uri"`
	RefreshToken      string `json:"refreshToken"`
	RefreshTokenSnake string `json:"refresh_token"`
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}

	return ""
}

func (w tokenRequestWire) request() TokenRequest {
	return TokenRequest{
		ClientID:     firstNonEmpty(w.ClientID, w.ClientIDSnake),
		GrantType:    firstNonEmpty(w.GrantType, w.GrantTypeSnake),
		Code:         w.Code,
		CodeVerifier: firstNonEmpty(w.CodeVerifier, w.CodeVerifierSnake),
		RedirectURI:  firstNonEmpty(w.RedirectURI, w.RedirectURISnake),
		RefreshToken: firstNonEmpty(w.RefreshToken, w.RefreshTokenSnake),
	}
}

type accessTokenWire struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken"`
}

type tokenResponseBodyWire struct {
	AccessToken  accessTokenWire `json:"accessToken"`
	TokenType    string          `json:"tokenType"`
	RefreshToken string          `json:"refreshToken"`
	IDToken      string          `json:"idToken,omitempty"`
	ExpiresIn    int32           `json:"expiresIn"`
}

type createTokenIAMInputWire struct {
	GrantType string `json:"grant_type"`
	Resource  string `json:"resource"`
}

type createTokenIAMOutputWire struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int32  `json:"expires_in"`
}

type introspectInputWire struct {
	Token         string `json:"token"`
	TokenTypeHint string `json:"token_type_hint"`
}

type introspectOutputWire struct {
	Exp       *int64 `json:"exp,omitempty"`
	Nbf       *int64 `json:"nbf,omitempty"`
	Iat       *int64 `json:"iat,omitempty"`
	Iss       string `json:"iss,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	Aud       string `json:"aud,omitempty"`
	Jti       string `json:"jti,omitempty"`
	AccountID string `json:"account_id,omitempty"`
	Resource  string `json:"resource,omitempty"`
	Session   string `json:"signin_session,omitempty"`
	Sub       string `json:"sub,omitempty"`
	TokenType string `json:"token_type,omitempty"`
	UserID    string `json:"user_id,omitempty"`
	Active    bool   `json:"active"`
}

type revokeInputWire struct {
	Token string `json:"token"`
}

type consoleConfigInputWire struct {
	TargetID string `json:"targetId"`
}

type consoleConfigOutputWire struct {
	Scope                       string `json:"scope"`
	TargetID                    string `json:"targetId"`
	ConsoleAuthorizationEnabled bool   `json:"consoleAuthorizationEnabled"`
}

type putStatementInputWire struct {
	ClientToken       string `json:"clientToken"`
	ConsoleSourceVpce string `json:"consoleSourceVpce"`
	ExcludedPrincipal string `json:"excludedPrincipal"`
	RequestedRegion   string `json:"requestedRegion"`
	SigninSourceVpce  string `json:"signinSourceVpce"`
	SourceIP          string `json:"sourceIp"`
	SourceVpc         string `json:"sourceVpc"`
	VpcSourceIP       string `json:"vpcSourceIp"`
}

type statementIDWire struct {
	StatementID string `json:"statementId"`
}

type deleteStatementInputWire struct {
	StatementID string `json:"statementId"`
	ClientToken string `json:"clientToken"`
}

type listStatementsInputWire struct {
	MaxResults *int32 `json:"maxResults"`
	NextToken  string `json:"nextToken"`
}

type statementSummaryWire struct {
	Condition map[string]map[string][]string `json:"condition,omitempty"`
	SID       string                         `json:"sid"`
}

type listStatementsOutputWire struct {
	NextToken            string                 `json:"nextToken,omitempty"`
	PermissionStatements []statementSummaryWire `json:"permissionStatements"`
}

type policyStatementWire struct {
	Condition map[string]map[string][]string `json:"Condition,omitempty"`
	Principal map[string]string              `json:"Principal"`
	Effect    string                         `json:"Effect"`
	Resource  string                         `json:"Resource"`
	Action    []string                       `json:"Action"`
}

type policyWire struct {
	Version   string                `json:"Version"`
	Statement []policyStatementWire `json:"Statement"`
}

type getPolicyOutputWire struct {
	Policy policyWire `json:"signinResourceBasedPolicy"`
}

type errorWire struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}
