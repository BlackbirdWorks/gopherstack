package ssooidc

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	signingName   = "sso-oauth"
	pathToken     = "/token"
	pathRegister  = "/client/register"
	pathDeviceAut = "/device_authorization"
	errorTypeHdr  = "X-Amzn-Errortype"

	opRegisterClient      = "RegisterClient"
	opStartDeviceAuth     = "StartDeviceAuthorization"
	opCreateToken         = "CreateToken"
	opCreateTokenWithIAM  = "CreateTokenWithIAM"
	iamTokenQueryKey      = "aws_iam"
	iamTokenQueryValue    = "t"
	accessTokenType       = "urn:ietf:params:oauth:token-type:access_token" //nolint:gosec // G101: token type URN
	maxRequestBodyBytes   = 1 << 20
	unmatchedErrorMessage = "The requested operation is not supported"
)

// SubjectResolver maps an Identity Center user name to the subject a sign-in approval acts for.
// An empty userName selects the default user.
type SubjectResolver interface {
	ResolveSubject(userName string) (Subject, error)
}

// Handler is the Echo handler for the IAM Identity Center OIDC API.
type Handler struct {
	Backend  *InMemoryBackend
	resolver atomic.Pointer[SubjectResolver]
}

// NewHandler creates a Handler over backend.
func NewHandler(backend *InMemoryBackend) *Handler { return &Handler{Backend: backend} }

// SetSubjectResolver wires the directory used to resolve users on sign-in approval.
func (h *Handler) SetSubjectResolver(r SubjectResolver) { h.resolver.Store(&r) }

// Name returns the service name.
func (h *Handler) Name() string { return "SSOOIDC" }

// GetSupportedOperations returns the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{opRegisterClient, opStartDeviceAuth, opCreateToken, opCreateTokenWithIAM}
}

// ChaosServiceName returns the lowercase service name for fault rules.
func (h *Handler) ChaosServiceName() string { return "ssooidc" }

// ChaosOperations returns the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions served.
func (h *Handler) ChaosRegions() []string { return []string{"us-east-1"} }

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

func operationFor(r *http.Request) string {
	if r.Method != http.MethodPost {
		return ""
	}

	switch r.URL.Path {
	case pathRegister:
		return opRegisterClient
	case pathDeviceAut:
		return opStartDeviceAuth
	case pathToken:
		if r.URL.RawQuery == iamTokenQueryKey+"="+iamTokenQueryValue {
			return opCreateTokenWithIAM
		}

		return opCreateToken
	}

	return ""
}

// RouteMatcher matches the OIDC endpoints. The SDK sends these calls unsigned, so an
// S3-signed request or one carrying query parameters (an S3 subresource) is not ours.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if operationFor(r) == "" {
			return false
		}

		if q := r.URL.RawQuery; q != "" && q != iamTokenQueryKey+"="+iamTokenQueryValue {
			return false
		}

		svc := httputils.ExtractServiceFromRequest(r)

		return svc == "" || svc == signingName
	}
}

// ExtractOperation returns the operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	if op := operationFor(c.Request()); op != "" {
		return op
	}

	return "Unknown"
}

// ExtractResource returns a fixed label to avoid high-cardinality metrics.
func (h *Handler) ExtractResource(_ *echo.Context) string { return "oidc" }

// Handler returns the Echo handler.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		switch operationFor(c.Request()) {
		case opRegisterClient:
			return h.handleRegisterClient(c)
		case opStartDeviceAuth:
			return h.handleStartDeviceAuthorization(c)
		case opCreateToken:
			return h.handleCreateToken(c, false)
		case opCreateTokenWithIAM:
			return h.handleCreateToken(c, true)
		}

		c.Response().Header().Set(errorTypeHdr, codeInvalidRequest)

		return c.JSON(http.StatusNotFound, map[string]string{"message": unmatchedErrorMessage})
	}
}

func decodeBody(c *echo.Context, v any) *APIError {
	body, err := io.ReadAll(io.LimitReader(c.Request().Body, maxRequestBodyBytes))
	if err != nil {
		return errInvalidRequest("failed to read the request body")
	}

	if len(body) == 0 {
		return nil
	}

	if json.Unmarshal(body, v) != nil {
		return errInvalidRequest("the request body is not valid JSON")
	}

	return nil
}

func writeError(c *echo.Context, e *APIError) error {
	c.Response().Header().Set(errorTypeHdr, e.Code)

	return c.JSON(e.Status, map[string]string{
		"error":             e.OAuth,
		"error_description": e.Message,
		"message":           e.Message,
	})
}

func baseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == schemeHTTPS {
		scheme = schemeHTTPS
	}

	return scheme + "://" + r.Host
}

type registerClientRequest struct {
	ClientName             string   `json:"clientName"`
	ClientType             string   `json:"clientType"`
	IssuerURL              string   `json:"issuerUrl"`
	EntitledApplicationArn string   `json:"entitledApplicationArn"`
	Scopes                 []string `json:"scopes"`
	RedirectURIs           []string `json:"redirectUris"`
	GrantTypes             []string `json:"grantTypes"`
}

type registerClientResponse struct {
	AuthorizationEndpoint string `json:"authorizationEndpoint,omitempty"`
	TokenEndpoint         string `json:"tokenEndpoint,omitempty"`
	ClientID              string `json:"clientId"`
	ClientSecret          string `json:"clientSecret"`
	ClientIDIssuedAt      int64  `json:"clientIdIssuedAt"`
	ClientSecretExpiresAt int64  `json:"clientSecretExpiresAt"`
}

func (h *Handler) handleRegisterClient(c *echo.Context) error {
	var req registerClientRequest
	if e := decodeBody(c, &req); e != nil {
		return writeError(c, e)
	}

	reg, e := h.Backend.RegisterClient(&RegisterClientInput{
		ClientName: req.ClientName, ClientType: req.ClientType, IssuerURL: req.IssuerURL,
		EntitledApp: req.EntitledApplicationArn, GrantTypes: req.GrantTypes,
		RedirectURIs: req.RedirectURIs, Scopes: req.Scopes,
	})
	if e != nil {
		return writeError(c, e)
	}

	resp := registerClientResponse{
		ClientID: reg.ClientID, ClientSecret: reg.ClientSecret,
		ClientIDIssuedAt: reg.ClientIDIssuedAt, ClientSecretExpiresAt: reg.ClientSecretExpiresAt,
	}

	for _, g := range req.GrantTypes {
		if g == grantAuthCode {
			base := baseURL(c.Request())
			resp.AuthorizationEndpoint = base + AuthorizePath
			resp.TokenEndpoint = base + pathToken
		}
	}

	return c.JSON(http.StatusOK, resp)
}

type startDeviceRequest struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	StartURL     string `json:"startUrl"`
}

type startDeviceResponse struct {
	DeviceCode              string `json:"deviceCode"`
	UserCode                string `json:"userCode"`
	VerificationURI         string `json:"verificationUri"`
	VerificationURIComplete string `json:"verificationUriComplete"`
	ExpiresIn               int32  `json:"expiresIn"`
	Interval                int32  `json:"interval"`
}

func (h *Handler) handleStartDeviceAuthorization(c *echo.Context) error {
	var req startDeviceRequest
	if e := decodeBody(c, &req); e != nil {
		return writeError(c, e)
	}

	out, e := h.Backend.StartDeviceAuthorization(req.ClientID, req.ClientSecret, req.StartURL)
	if e != nil {
		return writeError(c, e)
	}

	verification := baseURL(c.Request()) + DevicePath

	return c.JSON(http.StatusOK, startDeviceResponse{
		DeviceCode: out.DeviceCode, UserCode: out.UserCode, ExpiresIn: out.ExpiresIn, Interval: out.Interval,
		VerificationURI:         verification,
		VerificationURIComplete: verification + "?user_code=" + url.QueryEscape(out.UserCode),
	})
}

type createTokenRequest struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	GrantType    string `json:"grantType"`
	Code         string `json:"code"`
	CodeVerifier string `json:"codeVerifier"`
	DeviceCode   string `json:"deviceCode"`
	RedirectURI  string `json:"redirectUri"`
	RefreshToken string `json:"refreshToken"`
}

type createTokenResponse struct {
	AccessToken  string   `json:"accessToken"`
	RefreshToken string   `json:"refreshToken,omitempty"`
	TokenType    string   `json:"tokenType"`
	IssuedType   string   `json:"issuedTokenType,omitempty"`
	Scope        []string `json:"scope,omitempty"`
	ExpiresIn    int32    `json:"expiresIn"`
}

func (h *Handler) handleCreateToken(c *echo.Context, iamCaller bool) error {
	var req createTokenRequest
	if e := decodeBody(c, &req); e != nil {
		return writeError(c, e)
	}

	res, e := h.Backend.CreateToken(&CreateTokenInput{
		ClientID: req.ClientID, ClientSecret: req.ClientSecret, GrantType: req.GrantType, Code: req.Code,
		CodeVerifier: req.CodeVerifier, DeviceCode: req.DeviceCode, RedirectURI: req.RedirectURI,
		RefreshToken: req.RefreshToken,
	}, iamCaller)
	if e != nil {
		return writeError(c, e)
	}

	resp := createTokenResponse{
		AccessToken:  res.AccessToken,
		RefreshToken: res.RefreshToken,
		TokenType:    res.TokenType,
		ExpiresIn:    res.ExpiresIn,
	}
	if iamCaller {
		resp.Scope = res.Scopes
		resp.IssuedType = accessTokenType
	}

	return c.JSON(http.StatusOK, resp)
}
