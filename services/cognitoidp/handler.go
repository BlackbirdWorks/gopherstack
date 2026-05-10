package cognitoidp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	medEmail = "EMAIL"
)

const (
	errInvalidParameterException = "InvalidParameterException"
	keyDeliveryMedium            = "DeliveryMedium"
	keyDestination               = "Destination"
	keyAttributeName             = "AttributeName"
	keyConfirmationCode          = "ConfirmationCode"
	authTypeBearer               = "Bearer"
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
	janitor *Janitor
	ops     map[string]service.JSONOpFunc
	region  string
}

// NewHandler creates a new Cognito IDP handler.
func NewHandler(backend *InMemoryBackend, region string) *Handler {
	h := &Handler{Backend: backend, region: region}
	h.ops = h.dispatchTable()

	return h
}

// WithJanitor attaches a background janitor to the handler.
// The janitor periodically evicts expired refresh tokens. interval=0 uses the default.
// The optional taskTimeout bounds each sweep; 0 means no per-task timeout.
func (h *Handler) WithJanitor(interval time.Duration, taskTimeout ...time.Duration) *Handler {
	j := NewJanitor(h.Backend, interval)
	if len(taskTimeout) > 0 {
		j.TaskTimeout = taskTimeout[0]
	}

	h.janitor = j

	return h
}

// StartWorker starts the background janitor if it is configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Reset clears all backend state. Useful for test isolation.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "CognitoIDP" }

// GetSupportedOperations returns the list of supported operations.
//
//nolint:funlen // large service with many operations
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateUserPool",
		"DescribeUserPool",
		"ListUserPools",
		"DeleteUserPool",
		"UpdateUserPool",
		"GetUserPoolMfaConfig",
		"CreateUserPoolClient",
		"DescribeUserPoolClient",
		"ListUserPoolClients",
		"DeleteUserPoolClient",
		"UpdateUserPoolClient",
		"SignUp",
		"ConfirmSignUp",
		"InitiateAuth",
		"AdminInitiateAuth",
		"AdminCreateUser",
		"AdminSetUserPassword",
		"AdminGetUser",
		"AdminConfirmSignUp",
		"AdminDeleteUser",
		"AdminResetUserPassword",
		"ListUsers",
		"ForgotPassword",
		"ConfirmForgotPassword",
		"GetUser",
		"ChangePassword",
		"DeleteUser",
		"DeleteUserAttributes",
		"VerifyUserAttribute",
		"CreateGroup",
		"DeleteGroup",
		"GetGroup",
		"ListGroups",
		"AdminAddUserToGroup",
		"AdminRemoveUserFromGroup",
		"AdminListGroupsForUser",
		"UpdateUserAttributes",
		"AdminUpdateUserAttributes",
		"RevokeToken",
		"AddCustomAttributes",
		"AddUserPoolClientSecret",
		"AdminDeleteUserAttributes",
		"AdminDisableProviderForUser",
		"AdminDisableUser",
		"AdminEnableUser",
		"AdminForgetDevice",
		"ListUsersInGroup",
		"AdminUserGlobalSignOut",
		"GlobalSignOut",
		"ResendConfirmationCode",
		"SetUserPoolMfaConfig",
		"UpdateGroup",
		"GetSigningCertificate",
		// Completeness pass — previously notImplemented
		"AdminGetDevice",
		"AdminLinkProviderForUser",
		"AdminListDevices",
		"AdminListUserAuthEvents",
		"AdminRespondToAuthChallenge",
		"AdminSetUserMFAPreference",
		"AdminSetUserSettings",
		"AdminUpdateAuthEventFeedback",
		"AdminUpdateDeviceStatus",
		"AssociateSoftwareToken",
		"CompleteWebAuthnRegistration",
		"ConfirmDevice",
		"CreateIdentityProvider",
		"CreateManagedLoginBranding",
		"CreateResourceServer",
		"CreateTerms",
		"CreateUserImportJob",
		"CreateUserPoolDomain",
		"DeleteIdentityProvider",
		"DeleteManagedLoginBranding",
		"DeleteResourceServer",
		"DeleteTerms",
		"DeleteUserPoolClientSecret",
		"DeleteUserPoolDomain",
		"DeleteWebAuthnCredential",
		"DescribeIdentityProvider",
		"DescribeManagedLoginBranding",
		"DescribeManagedLoginBrandingByClient",
		"DescribeResourceServer",
		"DescribeRiskConfiguration",
		"DescribeTerms",
		"DescribeUserImportJob",
		"DescribeUserPoolDomain",
		"ForgetDevice",
		"GetCSVHeader",
		"GetDevice",
		"GetIdentityProviderByIdentifier",
		"GetLogDeliveryConfiguration",
		"GetTokensFromRefreshToken",
		"GetUICustomization",
		"GetUserAttributeVerificationCode",
		"GetUserAuthFactors",
		"ListDevices",
		"ListIdentityProviders",
		"ListResourceServers",
		"ListTagsForResource",
		"ListTerms",
		"ListUserImportJobs",
		"ListUserPoolClientSecrets",
		"ListWebAuthnCredentials",
		"RespondToAuthChallenge",
		"SetLogDeliveryConfiguration",
		"SetRiskConfiguration",
		"SetUICustomization",
		"SetUserMFAPreference",
		"SetUserSettings",
		"StartUserImportJob",
		"StartWebAuthnRegistration",
		"StopUserImportJob",
		"TagResource",
		"UntagResource",
		"UpdateAuthEventFeedback",
		"UpdateDeviceStatus",
		"UpdateIdentityProvider",
		"UpdateManagedLoginBranding",
		"UpdateResourceServer",
		"UpdateTerms",
		"UpdateUserPoolDomain",
		"VerifySoftwareToken",
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
	table := map[string]service.JSONOpFunc{
		"CreateUserPool":              service.WrapOp(h.handleCreateUserPool),
		"DescribeUserPool":            service.WrapOp(h.handleDescribeUserPool),
		"ListUserPools":               service.WrapOp(h.handleListUserPools),
		"DeleteUserPool":              service.WrapOp(h.handleDeleteUserPool),
		"UpdateUserPool":              service.WrapOp(h.handleUpdateUserPool),
		"GetUserPoolMfaConfig":        service.WrapOp(h.handleGetUserPoolMfaConfig),
		"CreateUserPoolClient":        service.WrapOp(h.handleCreateUserPoolClient),
		"DescribeUserPoolClient":      service.WrapOp(h.handleDescribeUserPoolClient),
		"ListUserPoolClients":         service.WrapOp(h.handleListUserPoolClients),
		"DeleteUserPoolClient":        service.WrapOp(h.handleDeleteUserPoolClient),
		"UpdateUserPoolClient":        service.WrapOp(h.handleUpdateUserPoolClient),
		"SignUp":                      service.WrapOp(h.handleSignUp),
		"ConfirmSignUp":               service.WrapOp(h.handleConfirmSignUp),
		"InitiateAuth":                service.WrapOp(h.handleInitiateAuth),
		"AdminInitiateAuth":           service.WrapOp(h.handleAdminInitiateAuth),
		"AdminCreateUser":             service.WrapOp(h.handleAdminCreateUser),
		"AdminSetUserPassword":        service.WrapOp(h.handleAdminSetUserPassword),
		"AdminGetUser":                service.WrapOp(h.handleAdminGetUser),
		"AdminConfirmSignUp":          service.WrapOp(h.handleAdminConfirmSignUp),
		"AdminDeleteUser":             service.WrapOp(h.handleAdminDeleteUser),
		"AdminResetUserPassword":      service.WrapOp(h.handleAdminResetUserPassword),
		"ListUsers":                   service.WrapOp(h.handleListUsers),
		"ForgotPassword":              service.WrapOp(h.handleForgotPassword),
		"ConfirmForgotPassword":       service.WrapOp(h.handleConfirmForgotPassword),
		"GetUser":                     service.WrapOp(h.handleGetUser),
		"ChangePassword":              service.WrapOp(h.handleChangePassword),
		"DeleteUser":                  service.WrapOp(h.handleDeleteUser),
		"DeleteUserAttributes":        service.WrapOp(h.handleDeleteUserAttributes),
		"VerifyUserAttribute":         service.WrapOp(h.handleVerifyUserAttribute),
		"CreateGroup":                 service.WrapOp(h.handleCreateGroup),
		"DeleteGroup":                 service.WrapOp(h.handleDeleteGroup),
		"GetGroup":                    service.WrapOp(h.handleGetGroup),
		"ListGroups":                  service.WrapOp(h.handleListGroups),
		"AdminAddUserToGroup":         service.WrapOp(h.handleAdminAddUserToGroup),
		"AdminRemoveUserFromGroup":    service.WrapOp(h.handleAdminRemoveUserFromGroup),
		"AdminListGroupsForUser":      service.WrapOp(h.handleAdminListGroupsForUser),
		"UpdateUserAttributes":        service.WrapOp(h.handleUpdateUserAttributes),
		"AdminUpdateUserAttributes":   service.WrapOp(h.handleAdminUpdateUserAttributes),
		"RevokeToken":                 service.WrapOp(h.handleRevokeToken),
		"AddCustomAttributes":         service.WrapOp(h.handleAddCustomAttributes),
		"AddUserPoolClientSecret":     service.WrapOp(h.handleAddUserPoolClientSecret),
		"AdminDeleteUserAttributes":   service.WrapOp(h.handleAdminDeleteUserAttributes),
		"AdminDisableProviderForUser": service.WrapOp(h.handleAdminDisableProviderForUser),
		"AdminDisableUser":            service.WrapOp(h.handleAdminDisableUser),
		"AdminEnableUser":             service.WrapOp(h.handleAdminEnableUser),
		"AdminForgetDevice":           service.WrapOp(h.handleAdminForgetDevice),
		"ListUsersInGroup":            service.WrapOp(h.handleListUsersInGroup),
		"AdminUserGlobalSignOut":      service.WrapOp(h.handleAdminUserGlobalSignOut),
		"GlobalSignOut":               service.WrapOp(h.handleGlobalSignOut),
		"ResendConfirmationCode":      service.WrapOp(h.handleResendConfirmationCode),
		"SetUserPoolMfaConfig":        service.WrapOp(h.handleSetUserPoolMfaConfig),
		"UpdateGroup":                 service.WrapOp(h.handleUpdateGroup),
		"GetSigningCertificate":       service.WrapOp(h.handleGetSigningCertificate),
	}
	maps.Copy(table, h.completenessDispatchTable())
	maps.Copy(table, h.accuracyDispatchTable())

	return table
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
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
	{ErrGroupNotFound, ErrGroupNotFound.Error()},
	{ErrAlreadyExists, ErrAlreadyExists.Error()},
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
		return errInvalidParameterException, http.StatusBadRequest
	}

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return errInvalidParameterException, http.StatusBadRequest
	}

	return "InternalFailure", http.StatusInternalServerError
}

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
	SchemaAttributes   []SchemaAttribute    `json:"SchemaAttributes,omitempty"`
	CreationDate       float64              `json:"CreationDate"`
	LastModifiedDate   float64              `json:"LastModifiedDate"`
}

func mfaConfigOrDefault(s string) string {
	if s == "" {
		return "OFF"
	}

	return s
}

// poolToData converts a UserPool to the userPoolData wire format.
func poolToData(pool *UserPool) userPoolData {
	return userPoolData{
		ID:                 pool.ID,
		Name:               pool.Name,
		ARN:                pool.ARN,
		CreationDate:       float64(pool.CreatedAt.Unix()),
		LastModifiedDate:   float64(pool.CreatedAt.Unix()),
		DeletionProtection: "INACTIVE",
		MfaConfiguration:   mfaConfigOrDefault(pool.MfaConfiguration),
		SchemaAttributes:   sortedCustomAttributes(pool.CustomAttributes),
	}
}

type createUserPoolOutput struct {
	UserPool userPoolData `json:"UserPool"`
}

func (h *Handler) handleCreateUserPool(_ context.Context, in *createUserPoolInput) (*createUserPoolOutput, error) {
	pool, err := h.Backend.CreateUserPool(in.PoolName)
	if err != nil {
		return nil, err
	}

	return &createUserPoolOutput{UserPool: poolToData(pool)}, nil
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

	return &describeUserPoolOutput{UserPool: poolToData(pool)}, nil
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
		items = append(items, poolToData(p))
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
	ClientSecret string  `json:"ClientSecret,omitempty"`
	CreationDate float64 `json:"CreationDate"`
}

// clientToData converts a UserPoolClient to the wire format.
func clientToData(c *UserPoolClient) userPoolClientData {
	return userPoolClientData{
		ClientID:     c.ClientID,
		ClientName:   c.ClientName,
		UserPoolID:   c.UserPoolID,
		ClientSecret: c.ClientSecret,
		CreationDate: float64(c.CreatedAt.Unix()),
	}
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

	return &createUserPoolClientOutput{UserPoolClient: clientToData(client)}, nil
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

	return &describeUserPoolClientOutput{UserPoolClient: clientToData(client)}, nil
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
	pool, err := h.Backend.DescribeUserPool(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	mfa := pool.MfaConfiguration
	if mfa == "" {
		mfa = "OFF"
	}

	return &getUserPoolMfaConfigOutput{MfaConfiguration: mfa}, nil
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
		items = append(items, clientToData(c))
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
			keyDeliveryMedium:   medEmail,
			keyDestination:      "mock",
			keyAttributeName:    attrEmail,
			keyConfirmationCode: user.ConfirmCode,
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
	AuthenticationResult *authResult       `json:"AuthenticationResult,omitempty"`
	ChallengeName        *string           `json:"ChallengeName,omitempty"`
	Session              *string           `json:"Session,omitempty"`
	ChallengeParameters  map[string]string `json:"ChallengeParameters,omitempty"`
}

// challengeName is the Cognito challenge name for software token MFA.
const challengeName = "SOFTWARE_TOKEN_MFA"

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

		return &authOutput{
			ChallengeName:       &name,
			Session:             &result.MFASession,
			ChallengeParameters: map[string]string{},
		}
	}

	return &authOutput{
		AuthenticationResult: authResultFromTokenResult(result.Tokens),
	}
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
				// AWS does not rotate the refresh token on every refresh by default;
				// we return the new token to keep the mock consistent with rotation.
				AccessToken:  tokens.AccessToken,
				IDToken:      tokens.IDToken,
				RefreshToken: tokens.RefreshToken,
				TokenType:    authTypeBearer,
				ExpiresIn:    tokens.ExpiresIn,
			},
		}, nil
	}

	username := in.AuthParameters["USERNAME"]
	password := in.AuthParameters["PASSWORD"]

	result, err := h.Backend.InitiateAuth(in.ClientID, in.AuthFlow, username, password)
	if err != nil {
		return nil, err
	}

	return authOutputFromResult(result), nil
}

func (h *Handler) handleAdminInitiateAuth(_ context.Context, in *authInput) (*authOutput, error) {
	if in.AuthFlow == "REFRESH_TOKEN_AUTH" || in.AuthFlow == "REFRESH_TOKEN" {
		refreshToken := in.AuthParameters["REFRESH_TOKEN"]
		tokens, err := h.Backend.InitiateAuthRefreshToken(in.ClientID, refreshToken)
		if err != nil {
			return nil, err
		}

		return &authOutput{
			AuthenticationResult: &authResult{
				AccessToken:  tokens.AccessToken,
				IDToken:      tokens.IDToken,
				RefreshToken: tokens.RefreshToken,
				TokenType:    authTypeBearer,
				ExpiresIn:    tokens.ExpiresIn,
			},
		}, nil
	}

	username := in.AuthParameters["USERNAME"]
	password := in.AuthParameters["PASSWORD"]

	result, err := h.Backend.AdminInitiateAuth(in.UserPoolID, in.ClientID, in.AuthFlow, username, password)
	if err != nil {
		return nil, err
	}

	return authOutputFromResult(result), nil
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
	Enabled        bool            `json:"Enabled"`
}

type adminCreateUserOutput struct {
	User adminUserType `json:"User"`
}

func (h *Handler) handleAdminCreateUser(_ context.Context, in *adminCreateUserInput) (*adminCreateUserOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)

	user, err := h.Backend.AdminCreateUserWithPolicy(in.UserPoolID, in.Username, in.TemporaryPassword, attrs)
	if err != nil {
		return nil, err
	}

	return &adminCreateUserOutput{
		User: adminUserType{
			Username:       user.Username,
			UserStatus:     user.Status,
			UserCreateDate: float64(user.CreatedAt.Unix()),
			Attributes:     sortedAttributeList(user.Attributes),
			Enabled:        user.Enabled,
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
	Username             string          `json:"Username"`
	UserStatus           string          `json:"UserStatus"`
	UserAttributes       []attributeType `json:"UserAttributes"`
	UserCreateDate       float64         `json:"UserCreateDate"`
	UserLastModifiedDate float64         `json:"UserLastModifiedDate"`
	Enabled              bool            `json:"Enabled"`
}

func (h *Handler) handleAdminGetUser(_ context.Context, in *adminGetUserInput) (*adminGetUserOutput, error) {
	user, err := h.Backend.AdminGetUser(in.UserPoolID, in.Username)
	if err != nil {
		return nil, err
	}

	attrs := userAttrsWithSub(user)

	updatedAt := user.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = user.CreatedAt
	}

	return &adminGetUserOutput{
		Username:             user.Username,
		UserStatus:           user.Status,
		UserCreateDate:       float64(user.CreatedAt.Unix()),
		UserLastModifiedDate: float64(updatedAt.Unix()),
		UserAttributes:       sortedAttributeList(attrs),
		Enabled:              user.Enabled,
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

// userAttrsWithSub returns a copy of the user's attribute map with the 'sub' attribute injected.
func userAttrsWithSub(u *User) map[string]string {
	attrs := maps.Clone(u.Attributes)
	if attrs == nil {
		attrs = make(map[string]string)
	}

	attrs["sub"] = u.Sub

	return attrs
}

// sortedAttributeList converts a map to a sorted slice of Cognito attribute types.
// Sorting by name ensures deterministic output, matching AWS behaviour.
func sortedAttributeList(m map[string]string) []attributeType {
	if len(m) == 0 {
		return []attributeType{}
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]attributeType, 0, len(m))
	for _, k := range keys {
		out = append(out, attributeType{Name: k, Value: m[k]})
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

// toUserSummary converts a backend User to a userSummary for API responses.
func toUserSummary(u *User) *userSummary {
	updatedAt := u.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = u.CreatedAt
	}

	return &userSummary{
		Username:         u.Username,
		UserStatus:       u.Status,
		UserCreateDate:   float64(u.CreatedAt.Unix()),
		UserLastModified: float64(updatedAt.Unix()),
		Attributes:       sortedAttributeList(userAttrsWithSub(u)),
		Enabled:          u.Enabled,
	}
}

type listUsersInput struct {
	UserPoolID string `json:"UserPoolId"`
	Filter     string `json:"Filter"`
	Limit      int    `json:"Limit"`
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
	users, err := h.Backend.ListUsersFiltered(in.UserPoolID, in.Filter)
	if err != nil {
		return nil, err
	}

	summaries := make([]*userSummary, 0, len(users))
	for _, u := range users {
		summaries = append(summaries, toUserSummary(u))
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
			keyDestination:      "mock@example.com",
			keyDeliveryMedium:   medEmail,
			keyAttributeName:    attrEmail,
			keyConfirmationCode: code,
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
		UserAttributes: sortedAttributeList(userAttrsWithSub(user)),
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

type addCustomAttributesInput struct {
	UserPoolID       string            `json:"UserPoolId"`
	CustomAttributes []SchemaAttribute `json:"CustomAttributes"`
}

type addCustomAttributesOutput struct{}

func (h *Handler) handleAddCustomAttributes(
	_ context.Context,
	in *addCustomAttributesInput,
) (*addCustomAttributesOutput, error) {
	if err := h.Backend.AddCustomAttributes(in.UserPoolID, in.CustomAttributes); err != nil {
		return nil, err
	}

	return &addCustomAttributesOutput{}, nil
}

type addUserPoolClientSecretInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId"`
}

type addUserPoolClientSecretOutput struct {
	ClientSecret string `json:"ClientSecret"`
}

func (h *Handler) handleAddUserPoolClientSecret(
	_ context.Context,
	in *addUserPoolClientSecretInput,
) (*addUserPoolClientSecretOutput, error) {
	secret, err := h.Backend.AddUserPoolClientSecret(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &addUserPoolClientSecretOutput{ClientSecret: secret}, nil
}

type adminDeleteUserAttributesInput struct {
	UserPoolID         string   `json:"UserPoolId"`
	Username           string   `json:"Username"`
	UserAttributeNames []string `json:"UserAttributeNames"`
}

type adminDeleteUserAttributesOutput struct{}

func (h *Handler) handleAdminDeleteUserAttributes(
	_ context.Context,
	in *adminDeleteUserAttributesInput,
) (*adminDeleteUserAttributesOutput, error) {
	if err := h.Backend.AdminDeleteUserAttributes(in.UserPoolID, in.Username, in.UserAttributeNames); err != nil {
		return nil, err
	}

	return &adminDeleteUserAttributesOutput{}, nil
}

type providerUserIdentifierType struct {
	ProviderAttributeName  string `json:"ProviderAttributeName"`
	ProviderAttributeValue string `json:"ProviderAttributeValue"`
	ProviderName           string `json:"ProviderName"`
}

type adminDisableProviderForUserInput struct {
	UserPoolID string                     `json:"UserPoolId"`
	User       providerUserIdentifierType `json:"User"`
}

type adminDisableProviderForUserOutput struct{}

func (h *Handler) handleAdminDisableProviderForUser(
	_ context.Context,
	in *adminDisableProviderForUserInput,
) (*adminDisableProviderForUserOutput, error) {
	if err := h.Backend.AdminDisableProviderForUser(in.UserPoolID); err != nil {
		return nil, err
	}

	return &adminDisableProviderForUserOutput{}, nil
}

type adminDisableUserInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminDisableUserOutput struct{}

func (h *Handler) handleAdminDisableUser(
	_ context.Context,
	in *adminDisableUserInput,
) (*adminDisableUserOutput, error) {
	if err := h.Backend.AdminDisableUser(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminDisableUserOutput{}, nil
}

type adminEnableUserInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminEnableUserOutput struct{}

func (h *Handler) handleAdminEnableUser(
	_ context.Context,
	in *adminEnableUserInput,
) (*adminEnableUserOutput, error) {
	if err := h.Backend.AdminEnableUser(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminEnableUserOutput{}, nil
}

type adminForgetDeviceInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
	DeviceKey  string `json:"DeviceKey"`
}

type adminForgetDeviceOutput struct{}

func (h *Handler) handleAdminForgetDevice(
	_ context.Context,
	in *adminForgetDeviceInput,
) (*adminForgetDeviceOutput, error) {
	if err := h.Backend.AdminForgetDevice(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminForgetDeviceOutput{}, nil
}

type listUsersInGroupInput struct {
	UserPoolID string `json:"UserPoolId"`
	GroupName  string `json:"GroupName"`
}

type listUsersInGroupOutput struct {
	Users []*userSummary `json:"Users"`
}

func (h *Handler) handleListUsersInGroup(
	_ context.Context,
	in *listUsersInGroupInput,
) (*listUsersInGroupOutput, error) {
	users, err := h.Backend.ListUsersInGroup(in.UserPoolID, in.GroupName)
	if err != nil {
		return nil, err
	}

	summaries := make([]*userSummary, 0, len(users))
	for _, u := range users {
		summaries = append(summaries, toUserSummary(u))
	}

	return &listUsersInGroupOutput{Users: summaries}, nil
}

type adminUserGlobalSignOutInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminUserGlobalSignOutOutput struct{}

func (h *Handler) handleAdminUserGlobalSignOut(
	_ context.Context,
	in *adminUserGlobalSignOutInput,
) (*adminUserGlobalSignOutOutput, error) {
	if err := h.Backend.AdminUserGlobalSignOut(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminUserGlobalSignOutOutput{}, nil
}

type globalSignOutInput struct {
	AccessToken string `json:"AccessToken"`
}

type globalSignOutOutput struct{}

func (h *Handler) handleGlobalSignOut(_ context.Context, in *globalSignOutInput) (*globalSignOutOutput, error) {
	if err := h.Backend.GlobalSignOut(in.AccessToken); err != nil {
		return nil, err
	}

	return &globalSignOutOutput{}, nil
}

type resendConfirmationCodeInput struct {
	ClientID string `json:"ClientId"`
	Username string `json:"Username"`
}

type resendConfirmationCodeOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}

func (h *Handler) handleResendConfirmationCode(
	_ context.Context,
	in *resendConfirmationCodeInput,
) (*resendConfirmationCodeOutput, error) {
	code, err := h.Backend.ResendConfirmationCode(in.ClientID, in.Username)
	if err != nil {
		return nil, err
	}

	return &resendConfirmationCodeOutput{
		CodeDeliveryDetails: map[string]string{
			keyDeliveryMedium:   medEmail,
			keyDestination:      "mock",
			keyAttributeName:    attrEmail,
			keyConfirmationCode: code,
		},
	}, nil
}

type setUserPoolMfaConfigInput struct {
	UserPoolID       string `json:"UserPoolId"`
	MfaConfiguration string `json:"MfaConfiguration"`
}

type setUserPoolMfaConfigOutput struct {
	MfaConfiguration string `json:"MfaConfiguration"`
}

func (h *Handler) handleSetUserPoolMfaConfig(
	_ context.Context,
	in *setUserPoolMfaConfigInput,
) (*setUserPoolMfaConfigOutput, error) {
	if err := h.Backend.SetUserPoolMfaConfig(in.UserPoolID, in.MfaConfiguration); err != nil {
		return nil, err
	}

	return &setUserPoolMfaConfigOutput{MfaConfiguration: in.MfaConfiguration}, nil
}

type updateGroupInput struct {
	UserPoolID  string `json:"UserPoolId"`
	GroupName   string `json:"GroupName"`
	Description string `json:"Description"`
	Precedence  int32  `json:"Precedence"`
}

type updateGroupOutput struct {
	Group *groupSummary `json:"Group"`
}

func (h *Handler) handleUpdateGroup(_ context.Context, in *updateGroupInput) (*updateGroupOutput, error) {
	g, err := h.Backend.UpdateGroup(in.UserPoolID, in.GroupName, in.Description, in.Precedence)
	if err != nil {
		return nil, err
	}

	return &updateGroupOutput{Group: toGroupSummary(g)}, nil
}

type getSigningCertificateInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type getSigningCertificateOutput struct {
	Certificate string `json:"Certificate"`
}

func (h *Handler) handleGetSigningCertificate(
	_ context.Context,
	in *getSigningCertificateInput,
) (*getSigningCertificateOutput, error) {
	if _, err := h.Backend.DescribeUserPool(in.UserPoolID); err != nil {
		return nil, err
	}

	return &getSigningCertificateOutput{}, nil
}

// --- UpdateUserPool ---

type updateUserPoolInput struct {
	UserPoolID       string `json:"UserPoolId"`
	MfaConfiguration string `json:"MfaConfiguration"`
}

type updateUserPoolOutput struct{}

func (h *Handler) handleUpdateUserPool(_ context.Context, in *updateUserPoolInput) (*updateUserPoolOutput, error) {
	if err := h.Backend.UpdateUserPool(in.UserPoolID, in.MfaConfiguration); err != nil {
		return nil, err
	}

	return &updateUserPoolOutput{}, nil
}

// --- UpdateUserPoolClient ---

type updateUserPoolClientInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId"`
	ClientName string `json:"ClientName"`
}

type updateUserPoolClientOutput struct {
	UserPoolClient userPoolClientData `json:"UserPoolClient"`
}

func (h *Handler) handleUpdateUserPoolClient(
	_ context.Context,
	in *updateUserPoolClientInput,
) (*updateUserPoolClientOutput, error) {
	client, err := h.Backend.UpdateUserPoolClient(in.UserPoolID, in.ClientID, in.ClientName)
	if err != nil {
		return nil, err
	}

	return &updateUserPoolClientOutput{UserPoolClient: clientToData(client)}, nil
}

// --- AdminResetUserPassword ---

type adminResetUserPasswordInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminResetUserPasswordOutput struct{}

func (h *Handler) handleAdminResetUserPassword(
	_ context.Context,
	in *adminResetUserPasswordInput,
) (*adminResetUserPasswordOutput, error) {
	if err := h.Backend.AdminResetUserPassword(in.UserPoolID, in.Username); err != nil {
		return nil, err
	}

	return &adminResetUserPasswordOutput{}, nil
}

// --- GetGroup ---

type getGroupInput struct {
	UserPoolID string `json:"UserPoolId"`
	GroupName  string `json:"GroupName"`
}

type getGroupOutput struct {
	Group *groupSummary `json:"Group"`
}

func (h *Handler) handleGetGroup(_ context.Context, in *getGroupInput) (*getGroupOutput, error) {
	g, err := h.Backend.GetGroup(in.UserPoolID, in.GroupName)
	if err != nil {
		return nil, err
	}

	return &getGroupOutput{Group: toGroupSummary(g)}, nil
}

// --- DeleteUser (self-service) ---

type deleteUserInput struct {
	AccessToken string `json:"AccessToken"`
}

type deleteUserOutput struct{}

func (h *Handler) handleDeleteUser(_ context.Context, in *deleteUserInput) (*deleteUserOutput, error) {
	if err := h.Backend.DeleteUser(in.AccessToken); err != nil {
		return nil, err
	}

	return &deleteUserOutput{}, nil
}

// --- DeleteUserAttributes (self-service) ---

type deleteUserAttributesInput struct {
	AccessToken        string   `json:"AccessToken"`
	UserAttributeNames []string `json:"UserAttributeNames"`
}

type deleteUserAttributesOutput struct{}

func (h *Handler) handleDeleteUserAttributes(
	_ context.Context,
	in *deleteUserAttributesInput,
) (*deleteUserAttributesOutput, error) {
	if err := h.Backend.DeleteUserAttributes(in.AccessToken, in.UserAttributeNames); err != nil {
		return nil, err
	}

	return &deleteUserAttributesOutput{}, nil
}

// --- VerifyUserAttribute ---

type verifyUserAttributeInput struct {
	AccessToken   string `json:"AccessToken"`
	AttributeName string `json:"AttributeName"`
	Code          string `json:"Code"`
}

type verifyUserAttributeOutput struct{}

func (h *Handler) handleVerifyUserAttribute(
	_ context.Context,
	in *verifyUserAttributeInput,
) (*verifyUserAttributeOutput, error) {
	if err := h.Backend.VerifyUserAttribute(in.AccessToken, in.AttributeName, in.Code); err != nil {
		return nil, err
	}

	return &verifyUserAttributeOutput{}, nil
}
