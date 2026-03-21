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
	Backend *InMemoryBackend
	region  string
}

// NewHandler creates a new Cognito IDP handler.
func NewHandler(backend *InMemoryBackend, region string) *Handler {
	return &Handler{Backend: backend, region: region}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CognitoIDP" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateUserPool",
		"DescribeUserPool",
		"ListUserPools",
		"DeleteUserPool",
		"GetUserPoolMfaConfig",
		"CreateUserPoolClient",
		"DescribeUserPoolClient",
		"ListUserPoolClients",
		"DeleteUserPoolClient",
		"SignUp",
		"ConfirmSignUp",
		"InitiateAuth",
		"AdminInitiateAuth",
		"AdminCreateUser",
		"AdminSetUserPassword",
		"AdminGetUser",
		"AdminConfirmSignUp",
		"AdminDeleteUser",
		"ListUsers",
		"ForgotPassword",
		"ConfirmForgotPassword",
		"GetUser",
		"ChangePassword",
		"CreateGroup",
		"DeleteGroup",
		"ListGroups",
		"AdminAddUserToGroup",
		"AdminRemoveUserFromGroup",
		"AdminListGroupsForUser",
		"UpdateUserAttributes",
		"AdminUpdateUserAttributes",
		"RevokeToken",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "cognito-idp" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Cognito IDP instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.region} }

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
		"CreateUserPool":            service.WrapOp(h.handleCreateUserPool),
		"DescribeUserPool":          service.WrapOp(h.handleDescribeUserPool),
		"ListUserPools":             service.WrapOp(h.handleListUserPools),
		"DeleteUserPool":            service.WrapOp(h.handleDeleteUserPool),
		"GetUserPoolMfaConfig":      service.WrapOp(h.handleGetUserPoolMfaConfig),
		"CreateUserPoolClient":      service.WrapOp(h.handleCreateUserPoolClient),
		"DescribeUserPoolClient":    service.WrapOp(h.handleDescribeUserPoolClient),
		"ListUserPoolClients":       service.WrapOp(h.handleListUserPoolClients),
		"DeleteUserPoolClient":      service.WrapOp(h.handleDeleteUserPoolClient),
		"SignUp":                    service.WrapOp(h.handleSignUp),
		"ConfirmSignUp":             service.WrapOp(h.handleConfirmSignUp),
		"InitiateAuth":              service.WrapOp(h.handleInitiateAuth),
		"AdminInitiateAuth":         service.WrapOp(h.handleAdminInitiateAuth),
		"AdminCreateUser":           service.WrapOp(h.handleAdminCreateUser),
		"AdminSetUserPassword":      service.WrapOp(h.handleAdminSetUserPassword),
		"AdminGetUser":              service.WrapOp(h.handleAdminGetUser),
		"AdminConfirmSignUp":        service.WrapOp(h.handleAdminConfirmSignUp),
		"AdminDeleteUser":           service.WrapOp(h.handleAdminDeleteUser),
		"ListUsers":                 service.WrapOp(h.handleListUsers),
		"ForgotPassword":            service.WrapOp(h.handleForgotPassword),
		"ConfirmForgotPassword":     service.WrapOp(h.handleConfirmForgotPassword),
		"GetUser":                   service.WrapOp(h.handleGetUser),
		"ChangePassword":            service.WrapOp(h.handleChangePassword),
		"CreateGroup":               service.WrapOp(h.handleCreateGroup),
		"DeleteGroup":               service.WrapOp(h.handleDeleteGroup),
		"ListGroups":                service.WrapOp(h.handleListGroups),
		"AdminAddUserToGroup":       service.WrapOp(h.handleAdminAddUserToGroup),
		"AdminRemoveUserFromGroup":  service.WrapOp(h.handleAdminRemoveUserFromGroup),
		"AdminListGroupsForUser":    service.WrapOp(h.handleAdminListGroupsForUser),
		"UpdateUserAttributes":      service.WrapOp(h.handleUpdateUserAttributes),
		"AdminUpdateUserAttributes": service.WrapOp(h.handleAdminUpdateUserAttributes),
		"RevokeToken":               service.WrapOp(h.handleRevokeToken),
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

// --- Request/Response types ---

type createUserPoolInput struct {
	PoolName string `json:"PoolName"`
}

// userPoolPoliciesData holds the Policies block returned by the Cognito IDP API.
// Returning a non-nil Policies object prevents nil-pointer panics in the Terraform
// AWS provider, which accesses Policies.PasswordPolicy and Policies.SignInPolicy
// unconditionally.
type userPoolPoliciesData struct {
	PasswordPolicy *userPoolPasswordPolicyData `json:"PasswordPolicy"`
	SignInPolicy   *userPoolSignInPolicyData   `json:"SignInPolicy"`
}

// userPoolPasswordPolicyData is a minimal representation of PasswordPolicyType.
type userPoolPasswordPolicyData struct{}

// userPoolSignInPolicyData is a minimal representation of SignInPolicyType.
type userPoolSignInPolicyData struct{}

type userPoolData struct {
	Policies           userPoolPoliciesData `json:"Policies"`
	ID                 string               `json:"Id"`
	Name               string               `json:"Name"`
	ARN                string               `json:"Arn"`
	DeletionProtection string               `json:"DeletionProtection"`
	MfaConfiguration   string               `json:"MfaConfiguration"`
	CreationDate       float64              `json:"CreationDate"`
	LastModifiedDate   float64              `json:"LastModifiedDate"`
}

type createUserPoolOutput struct {
	UserPool userPoolData `json:"UserPool"`
}

func (h *Handler) handleCreateUserPool(_ context.Context, in *createUserPoolInput) (*createUserPoolOutput, error) {
	pool, err := h.Backend.CreateUserPool(in.PoolName)
	if err != nil {
		return nil, err
	}

	return &createUserPoolOutput{
		UserPool: userPoolData{
			ID:                 pool.ID,
			Name:               pool.Name,
			ARN:                pool.ARN,
			CreationDate:       float64(pool.CreatedAt.Unix()),
			LastModifiedDate:   float64(pool.CreatedAt.Unix()),
			DeletionProtection: "INACTIVE",
			MfaConfiguration:   "OFF",
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
	pool, err := h.Backend.DescribeUserPool(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	return &describeUserPoolOutput{
		UserPool: userPoolData{
			ID:                 pool.ID,
			Name:               pool.Name,
			ARN:                pool.ARN,
			CreationDate:       float64(pool.CreatedAt.Unix()),
			LastModifiedDate:   float64(pool.CreatedAt.Unix()),
			DeletionProtection: "INACTIVE",
			MfaConfiguration:   "OFF",
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
	pools := h.Backend.ListUserPools()

	items := make([]userPoolData, 0, len(pools))
	for _, p := range pools {
		items = append(items, userPoolData{
			ID:                 p.ID,
			Name:               p.Name,
			ARN:                p.ARN,
			CreationDate:       float64(p.CreatedAt.Unix()),
			LastModifiedDate:   float64(p.CreatedAt.Unix()),
			DeletionProtection: "INACTIVE",
			MfaConfiguration:   "OFF",
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
	client, err := h.Backend.CreateUserPoolClient(in.UserPoolID, in.ClientName)
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
	client, err := h.Backend.DescribeUserPoolClient(in.UserPoolID, in.ClientID)
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

type deleteUserPoolInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type deleteUserPoolOutput struct{}

func (h *Handler) handleDeleteUserPool(_ context.Context, in *deleteUserPoolInput) (*deleteUserPoolOutput, error) {
	if err := h.Backend.DeleteUserPool(in.UserPoolID); err != nil {
		return nil, err
	}

	return &deleteUserPoolOutput{}, nil
}

type deleteUserPoolClientInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId"`
}

type deleteUserPoolClientOutput struct{}

func (h *Handler) handleDeleteUserPoolClient(
	_ context.Context,
	in *deleteUserPoolClientInput,
) (*deleteUserPoolClientOutput, error) {
	if err := h.Backend.DeleteUserPoolClient(in.UserPoolID, in.ClientID); err != nil {
		return nil, err
	}

	return &deleteUserPoolClientOutput{}, nil
}

type getUserPoolMfaConfigInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type getUserPoolMfaConfigOutput struct {
	MfaConfiguration string `json:"MfaConfiguration"`
}

func (h *Handler) handleGetUserPoolMfaConfig(
	_ context.Context,
	in *getUserPoolMfaConfigInput,
) (*getUserPoolMfaConfigOutput, error) {
	if _, err := h.Backend.DescribeUserPool(in.UserPoolID); err != nil {
		return nil, err
	}

	return &getUserPoolMfaConfigOutput{MfaConfiguration: "OFF"}, nil
}

type listUserPoolClientsInput struct {
	UserPoolID string `json:"UserPoolId"`
	MaxResults int    `json:"MaxResults"`
}

type listUserPoolClientsOutput struct {
	UserPoolClients []userPoolClientData `json:"UserPoolClients"`
}

func (h *Handler) handleListUserPoolClients(
	_ context.Context,
	in *listUserPoolClientsInput,
) (*listUserPoolClientsOutput, error) {
	clients, err := h.Backend.ListUserPoolClients(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	items := make([]userPoolClientData, 0, len(clients))
	for _, c := range clients {
		items = append(items, userPoolClientData{
			ClientID:     c.ClientID,
			ClientName:   c.ClientName,
			UserPoolID:   c.UserPoolID,
			CreationDate: float64(c.CreatedAt.Unix()),
		})
	}

	return &listUserPoolClientsOutput{UserPoolClients: items}, nil
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
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
	UserSub             string            `json:"UserSub"`
	UserConfirmed       bool              `json:"UserConfirmed"`
}

func (h *Handler) handleSignUp(_ context.Context, in *signUpInput) (*signUpOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)

	user, err := h.Backend.SignUp(in.ClientID, in.Username, in.Password, attrs)
	if err != nil {
		return nil, err
	}

	out := &signUpOutput{
		UserSub:       user.Sub,
		UserConfirmed: user.Status == UserStatusConfirmed,
	}

	// Include the confirmation code in the response to facilitate integration testing.
	// In production Cognito the code is delivered via email/SMS; the mock returns it
	// directly so test harnesses don't need an out-of-band code delivery mechanism.
	if user.ConfirmCode != "" {
		out.CodeDeliveryDetails = map[string]string{
			"DeliveryMedium":   "EMAIL",
			"Destination":      "mock",
			"AttributeName":    "email",
			"ConfirmationCode": user.ConfirmCode,
		}
	}

	return out, nil
}

type confirmSignUpInput struct {
	Username         string `json:"Username"`
	ConfirmationCode string `json:"ConfirmationCode"`
	ClientID         string `json:"ClientId"`
}

type confirmSignUpOutput struct{}

func (h *Handler) handleConfirmSignUp(_ context.Context, in *confirmSignUpInput) (*confirmSignUpOutput, error) {
	if err := h.Backend.ConfirmSignUp(in.ClientID, in.Username, in.ConfirmationCode); err != nil {
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
	if in.AuthFlow == "REFRESH_TOKEN_AUTH" || in.AuthFlow == "REFRESH_TOKEN" {
		refreshToken := in.AuthParameters["REFRESH_TOKEN"]
		tokens, err := h.Backend.InitiateAuthRefreshToken(in.ClientID, refreshToken)
		if err != nil {
			return nil, err
		}

		return &authOutput{
			AuthenticationResult: &authResult{
				AccessToken: tokens.AccessToken,
				IDToken:     tokens.IDToken,
				// AWS does not rotate the refresh token on every refresh by default;
				// we return the new token to keep the mock consistent with rotation.
				RefreshToken: tokens.RefreshToken,
				TokenType:    "Bearer",
				ExpiresIn:    tokens.ExpiresIn,
			},
		}, nil
	}

	username := in.AuthParameters["USERNAME"]
	password := in.AuthParameters["PASSWORD"]

	tokens, err := h.Backend.InitiateAuth(in.ClientID, in.AuthFlow, username, password)
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

	tokens, err := h.Backend.AdminInitiateAuth(in.UserPoolID, in.ClientID, in.AuthFlow, username, password)
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

	user, err := h.Backend.AdminCreateUser(in.UserPoolID, in.Username, in.TemporaryPassword, attrs)
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
	if err := h.Backend.AdminSetUserPassword(in.UserPoolID, in.Username, in.Password, in.Permanent); err != nil {
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
	user, err := h.Backend.AdminGetUser(in.UserPoolID, in.Username)
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

type adminConfirmSignUpInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminConfirmSignUpOutput struct{}

func (h *Handler) handleAdminConfirmSignUp(
	_ context.Context,
	in *adminConfirmSignUpInput,
) (*adminConfirmSignUpOutput, error) {
	if err := h.Backend.AdminConfirmSignUp(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminConfirmSignUpOutput{}, nil
}

type adminDeleteUserInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminDeleteUserOutput struct{}

func (h *Handler) handleAdminDeleteUser(
	_ context.Context,
	in *adminDeleteUserInput,
) (*adminDeleteUserOutput, error) {
	if err := h.Backend.AdminDeleteUser(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminDeleteUserOutput{}, nil
}

type listUsersInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type listUsersOutput struct {
	Users []*userSummary `json:"Users"`
}

type userSummary struct {
	Username         string          `json:"Username"`
	UserStatus       string          `json:"UserStatus"`
	Attributes       []attributeType `json:"Attributes"`
	UserCreateDate   float64         `json:"UserCreateDate"`
	UserLastModified float64         `json:"UserLastModifiedDate"`
	Enabled          bool            `json:"Enabled"`
}

func (h *Handler) handleListUsers(
	_ context.Context,
	in *listUsersInput,
) (*listUsersOutput, error) {
	users, err := h.Backend.ListUsers(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	summaries := make([]*userSummary, 0, len(users))
	for _, u := range users {
		summaries = append(summaries, &userSummary{
			Username:       u.Username,
			UserStatus:     u.Status,
			UserCreateDate: float64(u.CreatedAt.Unix()),
			Attributes:     mapToAttributeList(u.Attributes),
			Enabled:        true,
		})
	}

	return &listUsersOutput{Users: summaries}, nil
}

type forgotPasswordInput struct {
	ClientID string `json:"ClientId"`
	Username string `json:"Username"`
}

type forgotPasswordOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}

func (h *Handler) handleForgotPassword(
	_ context.Context,
	in *forgotPasswordInput,
) (*forgotPasswordOutput, error) {
	code, err := h.Backend.ForgotPassword(in.ClientID, in.Username)
	if err != nil {
		return nil, err
	}

	return &forgotPasswordOutput{
		CodeDeliveryDetails: map[string]string{
			"Destination":      "mock@example.com",
			"DeliveryMedium":   "EMAIL",
			"AttributeName":    "email",
			"ConfirmationCode": code,
		},
	}, nil
}

type confirmForgotPasswordInput struct {
	ClientID         string `json:"ClientId"`
	Username         string `json:"Username"`
	ConfirmationCode string `json:"ConfirmationCode"`
	Password         string `json:"Password"`
}

type confirmForgotPasswordOutput struct{}

func (h *Handler) handleConfirmForgotPassword(
	_ context.Context,
	in *confirmForgotPasswordInput,
) (*confirmForgotPasswordOutput, error) {
	if err := h.Backend.ConfirmForgotPassword(in.ClientID, in.Username, in.ConfirmationCode, in.Password); err != nil {
		return nil, err
	}

	return &confirmForgotPasswordOutput{}, nil
}

type getUserInput struct {
	AccessToken string `json:"AccessToken"`
}

type getUserOutput struct {
	Username       string          `json:"Username"`
	UserAttributes []attributeType `json:"UserAttributes"`
}

func (h *Handler) handleGetUser(
	_ context.Context,
	in *getUserInput,
) (*getUserOutput, error) {
	user, err := h.Backend.GetUser(in.AccessToken)
	if err != nil {
		return nil, err
	}

	return &getUserOutput{
		Username:       user.Username,
		UserAttributes: mapToAttributeList(user.Attributes),
	}, nil
}

type changePasswordInput struct {
	AccessToken      string `json:"AccessToken"`
	PreviousPassword string `json:"PreviousPassword"`
	ProposedPassword string `json:"ProposedPassword"`
}

type changePasswordOutput struct{}

func (h *Handler) handleChangePassword(
	_ context.Context,
	in *changePasswordInput,
) (*changePasswordOutput, error) {
	if err := h.Backend.ChangePassword(in.AccessToken, in.PreviousPassword, in.ProposedPassword); err != nil {
		return nil, err
	}

	return &changePasswordOutput{}, nil
}

type createGroupInput struct {
	UserPoolID  string `json:"UserPoolId"`
	GroupName   string `json:"GroupName"`
	Description string `json:"Description"`
	Precedence  int32  `json:"Precedence"`
}

type createGroupOutput struct {
	Group *groupSummary `json:"Group"`
}

type groupSummary struct {
	GroupName    string  `json:"GroupName"`
	UserPoolID   string  `json:"UserPoolId"`
	Description  string  `json:"Description,omitempty"`
	Precedence   int32   `json:"Precedence"`
	CreationDate float64 `json:"CreationDate"`
}

func toGroupSummary(g *Group) *groupSummary {
	return &groupSummary{
		GroupName:    g.GroupName,
		UserPoolID:   g.UserPoolID,
		Description:  g.Description,
		Precedence:   g.Precedence,
		CreationDate: float64(g.CreatedAt.Unix()),
	}
}

func (h *Handler) handleCreateGroup(
	_ context.Context,
	in *createGroupInput,
) (*createGroupOutput, error) {
	g, err := h.Backend.CreateGroup(in.UserPoolID, in.GroupName, in.Description, in.Precedence)
	if err != nil {
		return nil, err
	}

	return &createGroupOutput{Group: toGroupSummary(g)}, nil
}

type deleteGroupInput struct {
	UserPoolID string `json:"UserPoolId"`
	GroupName  string `json:"GroupName"`
}

type deleteGroupOutput struct{}

func (h *Handler) handleDeleteGroup(_ context.Context, in *deleteGroupInput) (*deleteGroupOutput, error) {
	if err := h.Backend.DeleteGroup(in.UserPoolID, in.GroupName); err != nil {
		return nil, err
	}

	return &deleteGroupOutput{}, nil
}

type listGroupsInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type listGroupsOutput struct {
	Groups []*groupSummary `json:"Groups"`
}

func (h *Handler) handleListGroups(_ context.Context, in *listGroupsInput) (*listGroupsOutput, error) {
	groups, err := h.Backend.ListGroups(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := make([]*groupSummary, 0, len(groups))
	for _, g := range groups {
		out = append(out, toGroupSummary(g))
	}

	return &listGroupsOutput{Groups: out}, nil
}

type adminAddUserToGroupInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
	GroupName  string `json:"GroupName"`
}

type adminAddUserToGroupOutput struct{}

func (h *Handler) handleAdminAddUserToGroup(
	_ context.Context,
	in *adminAddUserToGroupInput,
) (*adminAddUserToGroupOutput, error) {
	if err := h.Backend.AdminAddUserToGroup(in.UserPoolID, in.Username, in.GroupName); err != nil {
		return nil, err
	}

	return &adminAddUserToGroupOutput{}, nil
}

type adminRemoveUserFromGroupInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
	GroupName  string `json:"GroupName"`
}

type adminRemoveUserFromGroupOutput struct{}

func (h *Handler) handleAdminRemoveUserFromGroup(
	_ context.Context,
	in *adminRemoveUserFromGroupInput,
) (*adminRemoveUserFromGroupOutput, error) {
	if err := h.Backend.AdminRemoveUserFromGroup(in.UserPoolID, in.Username, in.GroupName); err != nil {
		return nil, err
	}

	return &adminRemoveUserFromGroupOutput{}, nil
}

type adminListGroupsForUserInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminListGroupsForUserOutput struct {
	Groups []*groupSummary `json:"Groups"`
}

func (h *Handler) handleAdminListGroupsForUser(
	_ context.Context,
	in *adminListGroupsForUserInput,
) (*adminListGroupsForUserOutput, error) {
	groups, err := h.Backend.AdminListGroupsForUser(in.UserPoolID, in.Username)
	if err != nil {
		return nil, err
	}

	out := make([]*groupSummary, 0, len(groups))
	for _, g := range groups {
		out = append(out, toGroupSummary(g))
	}

	return &adminListGroupsForUserOutput{Groups: out}, nil
}

type updateUserAttributesInput struct {
	AccessToken    string          `json:"AccessToken"`
	UserAttributes []attributeType `json:"UserAttributes"`
}

type updateUserAttributesOutput struct{}

func (h *Handler) handleUpdateUserAttributes(
	_ context.Context,
	in *updateUserAttributesInput,
) (*updateUserAttributesOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)
	if err := h.Backend.UpdateUserAttributes(in.AccessToken, attrs); err != nil {
		return nil, err
	}

	return &updateUserAttributesOutput{}, nil
}

type adminUpdateUserAttributesInput struct {
	UserPoolID     string          `json:"UserPoolId"`
	Username       string          `json:"Username"`
	UserAttributes []attributeType `json:"UserAttributes"`
}

type adminUpdateUserAttributesOutput struct{}

func (h *Handler) handleAdminUpdateUserAttributes(
	_ context.Context,
	in *adminUpdateUserAttributesInput,
) (*adminUpdateUserAttributesOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)
	if err := h.Backend.AdminUpdateUserAttributes(in.UserPoolID, in.Username, attrs); err != nil {
		return nil, err
	}

	return &adminUpdateUserAttributesOutput{}, nil
}

type revokeTokenInput struct {
	Token    string `json:"Token"`
	ClientID string `json:"ClientId"`
}

type revokeTokenOutput struct{}

func (h *Handler) handleRevokeToken(_ context.Context, in *revokeTokenInput) (*revokeTokenOutput, error) {
	if err := h.Backend.RevokeToken(in.Token, in.ClientID); err != nil {
		return nil, err
	}

	return &revokeTokenOutput{}, nil
}
