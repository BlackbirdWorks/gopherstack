package cognitoidp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	cognitoTargetPrefix = "AWSCognitoIdentityProviderService."
	jwksPathSuffix      = "/.well-known/jwks.json"
	contentType         = "application/x-amz-json-1.1"
)

var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for Cognito IDP operations.
type Handler struct {
	backend *InMemoryBackend
	region  string
}

// NewHandler creates a new Cognito IDP handler.
func NewHandler(backend *InMemoryBackend, region string) *Handler {
	return &Handler{backend: backend, region: region}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CognitoIDP" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateUserPool",
		"DescribeUserPool",
		"ListUserPools",
		"CreateUserPoolClient",
		"DescribeUserPoolClient",
		"SignUp",
		"ConfirmSignUp",
		"InitiateAuth",
		"AdminInitiateAuth",
		"AdminCreateUser",
		"AdminSetUserPassword",
		"AdminGetUser",
	}
}

// RouteMatcher returns a function that matches Cognito IDP requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), cognitoTargetPrefix) {
			return true
		}

		return strings.HasSuffix(c.Request().URL.Path, jwksPathSuffix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Cognito action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, cognitoTargetPrefix)

	if action == "" || action == target {
		if strings.HasSuffix(c.Request().URL.Path, jwksPathSuffix) {
			return "GetJWKS"
		}

		return "Unknown"
	}

	return action
}

// ExtractResource extracts the user pool or user resource from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	// For JWKS endpoint, extract pool ID from the path.
	if strings.HasSuffix(c.Request().URL.Path, jwksPathSuffix) {
		trimmed := strings.TrimPrefix(c.Request().URL.Path, "/")
		poolID, _, _ := strings.Cut(trimmed, "/")

		return poolID
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		UserPoolID string `json:"UserPoolId"`
		ClientID   string `json:"ClientId"`
		Username   string `json:"Username"`
	}

	_ = json.Unmarshal(body, &req)

	if req.UserPoolID != "" {
		return req.UserPoolID
	}

	if req.ClientID != "" {
		return req.ClientID
	}

	return req.Username
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		if strings.HasSuffix(c.Request().URL.Path, jwksPathSuffix) {
			return h.handleJWKS(c)
		}

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"AWSCognitoIdentityProviderService", contentType,
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateUserPool":         service.WrapOp(h.handleCreateUserPool),
		"DescribeUserPool":       service.WrapOp(h.handleDescribeUserPool),
		"ListUserPools":          service.WrapOp(h.handleListUserPools),
		"CreateUserPoolClient":   service.WrapOp(h.handleCreateUserPoolClient),
		"DescribeUserPoolClient": service.WrapOp(h.handleDescribeUserPoolClient),
		"SignUp":                 service.WrapOp(h.handleSignUp),
		"ConfirmSignUp":          service.WrapOp(h.handleConfirmSignUp),
		"InitiateAuth":           service.WrapOp(h.handleInitiateAuth),
		"AdminInitiateAuth":      service.WrapOp(h.handleAdminInitiateAuth),
		"AdminCreateUser":        service.WrapOp(h.handleAdminCreateUser),
		"AdminSetUserPassword":   service.WrapOp(h.handleAdminSetUserPassword),
		"AdminGetUser":           service.WrapOp(h.handleAdminGetUser),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	errType, statusCode := resolveErrorType(err)

	return c.JSON(statusCode, service.JSONErrorResponse{
		Type:    errType,
		Message: err.Error(),
	})
}

// cognitoSentinelErrors maps sentinel errors to their Cognito exception type names.
// All Cognito errors return 400 Bad Request.
var cognitoSentinelErrors = []struct { //nolint:gochecknoglobals // package-level lookup table
	sentinel error
	typeName string
}{
	{ErrUserNotFound, ErrUserNotFound.Error()},
	{ErrUserPoolNotFound, ErrUserPoolNotFound.Error()},
	{ErrClientNotFound, ErrClientNotFound.Error()},
	{ErrExpiredCode, ErrExpiredCode.Error()},
	{ErrUsernameExists, ErrUsernameExists.Error()},
	{ErrUserAlreadyExists, ErrUserAlreadyExists.Error()},
	{ErrUserPoolAlreadyExists, ErrUserPoolAlreadyExists.Error()},
	{ErrNotAuthorized, ErrNotAuthorized.Error()},
	{ErrInvalidPassword, ErrInvalidPassword.Error()},
	{ErrUserNotConfirmed, ErrUserNotConfirmed.Error()},
	{ErrPasswordResetRequired, ErrPasswordResetRequired.Error()},
	{ErrCodeMismatch, ErrCodeMismatch.Error()},
	{ErrInvalidUserPoolConfig, ErrInvalidUserPoolConfig.Error()},
	{errUnknownAction, "UnknownOperationException"},
}

func resolveErrorType(err error) (string, int) {
	for _, entry := range cognitoSentinelErrors {
		if errors.Is(err, entry.sentinel) {
			return entry.typeName, http.StatusBadRequest
		}
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return "InvalidParameterException", http.StatusBadRequest
	}

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return "InvalidParameterException", http.StatusBadRequest
	}

	return "InternalFailure", http.StatusInternalServerError
}

func (h *Handler) handleJWKS(c *echo.Context) error {
	path := c.Request().URL.Path
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.Split(trimmed, "/")

	if len(parts) == 0 || parts[0] == "" {
		return c.JSON(http.StatusBadRequest, service.JSONErrorResponse{
			Type:    "InvalidParameterException",
			Message: "missing user pool ID in path",
		})
	}

	userPoolID := parts[0]

	jwks, err := h.backend.GetUserPoolJWKS(userPoolID)
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

// --- Request/Response types ---

type createUserPoolInput struct {
	PoolName string `json:"PoolName"`
}

type userPoolData struct {
	ID           string  `json:"Id"`
	Name         string  `json:"Name"`
	ARN          string  `json:"Arn"`
	CreationDate float64 `json:"CreationDate"`
}

type createUserPoolOutput struct {
	UserPool userPoolData `json:"UserPool"`
}

func (h *Handler) handleCreateUserPool(_ context.Context, in *createUserPoolInput) (*createUserPoolOutput, error) {
	pool, err := h.backend.CreateUserPool(in.PoolName)
	if err != nil {
		return nil, err
	}

	return &createUserPoolOutput{
		UserPool: userPoolData{
			ID:           pool.ID,
			Name:         pool.Name,
			ARN:          pool.ARN,
			CreationDate: float64(pool.CreatedAt.Unix()),
		},
	}, nil
}

type describeUserPoolInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type describeUserPoolOutput struct {
	UserPool userPoolData `json:"UserPool"`
}

func (h *Handler) handleDescribeUserPool(
	_ context.Context,
	in *describeUserPoolInput,
) (*describeUserPoolOutput, error) {
	pool, err := h.backend.DescribeUserPool(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	return &describeUserPoolOutput{
		UserPool: userPoolData{
			ID:           pool.ID,
			Name:         pool.Name,
			ARN:          pool.ARN,
			CreationDate: float64(pool.CreatedAt.Unix()),
		},
	}, nil
}

type listUserPoolsInput struct {
	MaxResults int `json:"MaxResults"`
}

type listUserPoolsOutput struct {
	UserPools []userPoolData `json:"UserPools"`
}

func (h *Handler) handleListUserPools(_ context.Context, _ *listUserPoolsInput) (*listUserPoolsOutput, error) {
	pools := h.backend.ListUserPools()

	items := make([]userPoolData, 0, len(pools))
	for _, p := range pools {
		items = append(items, userPoolData{
			ID:           p.ID,
			Name:         p.Name,
			ARN:          p.ARN,
			CreationDate: float64(p.CreatedAt.Unix()),
		})
	}

	return &listUserPoolsOutput{UserPools: items}, nil
}

type createUserPoolClientInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientName string `json:"ClientName"`
}

type userPoolClientData struct {
	ClientID     string  `json:"ClientId"`
	ClientName   string  `json:"ClientName"`
	UserPoolID   string  `json:"UserPoolId"`
	CreationDate float64 `json:"CreationDate"`
}

type createUserPoolClientOutput struct {
	UserPoolClient userPoolClientData `json:"UserPoolClient"`
}

func (h *Handler) handleCreateUserPoolClient(
	_ context.Context,
	in *createUserPoolClientInput,
) (*createUserPoolClientOutput, error) {
	client, err := h.backend.CreateUserPoolClient(in.UserPoolID, in.ClientName)
	if err != nil {
		return nil, err
	}

	return &createUserPoolClientOutput{
		UserPoolClient: userPoolClientData{
			ClientID:     client.ClientID,
			ClientName:   client.ClientName,
			UserPoolID:   client.UserPoolID,
			CreationDate: float64(client.CreatedAt.Unix()),
		},
	}, nil
}

type describeUserPoolClientInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId"`
}

type describeUserPoolClientOutput struct {
	UserPoolClient userPoolClientData `json:"UserPoolClient"`
}

func (h *Handler) handleDescribeUserPoolClient(
	_ context.Context,
	in *describeUserPoolClientInput,
) (*describeUserPoolClientOutput, error) {
	client, err := h.backend.DescribeUserPoolClient(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &describeUserPoolClientOutput{
		UserPoolClient: userPoolClientData{
			ClientID:     client.ClientID,
			ClientName:   client.ClientName,
			UserPoolID:   client.UserPoolID,
			CreationDate: float64(client.CreatedAt.Unix()),
		},
	}, nil
}

type attributeType struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

type signUpInput struct {
	Username       string          `json:"Username"`
	Password       string          `json:"Password"`
	ClientID       string          `json:"ClientId"`
	UserAttributes []attributeType `json:"UserAttributes"`
}

type signUpOutput struct {
	UserSub       string `json:"UserSub"`
	UserConfirmed bool   `json:"UserConfirmed"`
}

func (h *Handler) handleSignUp(_ context.Context, in *signUpInput) (*signUpOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)

	user, err := h.backend.SignUp(in.ClientID, in.Username, in.Password, attrs)
	if err != nil {
		return nil, err
	}

	return &signUpOutput{
		UserSub:       user.Sub,
		UserConfirmed: user.Status == UserStatusConfirmed,
	}, nil
}

type confirmSignUpInput struct {
	Username         string `json:"Username"`
	ConfirmationCode string `json:"ConfirmationCode"`
	ClientID         string `json:"ClientId"`
}

type confirmSignUpOutput struct{}

func (h *Handler) handleConfirmSignUp(_ context.Context, in *confirmSignUpInput) (*confirmSignUpOutput, error) {
	if err := h.backend.ConfirmSignUp(in.ClientID, in.Username, in.ConfirmationCode); err != nil {
		return nil, err
	}

	return &confirmSignUpOutput{}, nil
}

type authInput struct {
	AuthParameters map[string]string `json:"AuthParameters"`
	AuthFlow       string            `json:"AuthFlow"`
	ClientID       string            `json:"ClientId"`
	UserPoolID     string            `json:"UserPoolId"`
}

type authResult struct {
	AccessToken  string `json:"AccessToken"`
	IDToken      string `json:"IdToken"`
	RefreshToken string `json:"RefreshToken"`
	TokenType    string `json:"TokenType"`
	ExpiresIn    int32  `json:"ExpiresIn"`
}

type authOutput struct {
	AuthenticationResult *authResult `json:"AuthenticationResult,omitempty"`
	ChallengeName        *string     `json:"ChallengeName,omitempty"`
}

func (h *Handler) handleInitiateAuth(_ context.Context, in *authInput) (*authOutput, error) {
	username := in.AuthParameters["USERNAME"]
	password := in.AuthParameters["PASSWORD"]

	tokens, err := h.backend.InitiateAuth(in.ClientID, in.AuthFlow, username, password)
	if err != nil {
		return nil, err
	}

	return &authOutput{
		AuthenticationResult: &authResult{
			AccessToken:  tokens.AccessToken,
			IDToken:      tokens.IDToken,
			RefreshToken: tokens.RefreshToken,
			TokenType:    "Bearer",
			ExpiresIn:    tokens.ExpiresIn,
		},
	}, nil
}

func (h *Handler) handleAdminInitiateAuth(_ context.Context, in *authInput) (*authOutput, error) {
	username := in.AuthParameters["USERNAME"]
	password := in.AuthParameters["PASSWORD"]

	tokens, err := h.backend.AdminInitiateAuth(in.UserPoolID, in.ClientID, in.AuthFlow, username, password)
	if err != nil {
		return nil, err
	}

	return &authOutput{
		AuthenticationResult: &authResult{
			AccessToken:  tokens.AccessToken,
			IDToken:      tokens.IDToken,
			RefreshToken: tokens.RefreshToken,
			TokenType:    "Bearer",
			ExpiresIn:    tokens.ExpiresIn,
		},
	}, nil
}

type adminCreateUserInput struct {
	UserPoolID        string          `json:"UserPoolId"`
	Username          string          `json:"Username"`
	TemporaryPassword string          `json:"TemporaryPassword"`
	UserAttributes    []attributeType `json:"UserAttributes"`
}

type adminUserType struct {
	Username       string          `json:"Username"`
	UserStatus     string          `json:"UserStatus"`
	Attributes     []attributeType `json:"Attributes"`
	UserCreateDate float64         `json:"UserCreateDate"`
}

type adminCreateUserOutput struct {
	User adminUserType `json:"User"`
}

func (h *Handler) handleAdminCreateUser(_ context.Context, in *adminCreateUserInput) (*adminCreateUserOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)

	user, err := h.backend.AdminCreateUser(in.UserPoolID, in.Username, in.TemporaryPassword, attrs)
	if err != nil {
		return nil, err
	}

	return &adminCreateUserOutput{
		User: adminUserType{
			Username:       user.Username,
			UserStatus:     user.Status,
			UserCreateDate: float64(user.CreatedAt.Unix()),
			Attributes:     mapToAttributeList(user.Attributes),
		},
	}, nil
}

type adminSetUserPasswordInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
	Password   string `json:"Password"`
	Permanent  bool   `json:"Permanent"`
}

type adminSetUserPasswordOutput struct{}

func (h *Handler) handleAdminSetUserPassword(
	_ context.Context,
	in *adminSetUserPasswordInput,
) (*adminSetUserPasswordOutput, error) {
	if err := h.backend.AdminSetUserPassword(in.UserPoolID, in.Username, in.Password, in.Permanent); err != nil {
		return nil, err
	}

	return &adminSetUserPasswordOutput{}, nil
}

type adminGetUserInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminGetUserOutput struct {
	Username       string          `json:"Username"`
	UserStatus     string          `json:"UserStatus"`
	UserAttributes []attributeType `json:"UserAttributes"`
	UserCreateDate float64         `json:"UserCreateDate"`
}

func (h *Handler) handleAdminGetUser(_ context.Context, in *adminGetUserInput) (*adminGetUserOutput, error) {
	user, err := h.backend.AdminGetUser(in.UserPoolID, in.Username)
	if err != nil {
		return nil, err
	}

	return &adminGetUserOutput{
		Username:       user.Username,
		UserStatus:     user.Status,
		UserCreateDate: float64(user.CreatedAt.Unix()),
		UserAttributes: mapToAttributeList(user.Attributes),
	}, nil
}

// attributeListToMap converts a slice of Cognito attribute types to a map.
func attributeListToMap(attrs []attributeType) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		m[a.Name] = a.Value
	}

	return m
}

// mapToAttributeList converts a map to a slice of Cognito attribute types.
func mapToAttributeList(m map[string]string) []attributeType {
	out := make([]attributeType, 0, len(m))
	for k, v := range m {
		out = append(out, attributeType{Name: k, Value: v})
	}

	return out
}
