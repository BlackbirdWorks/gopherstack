package cognitoidentity

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
	cognitoIdentityTargetPrefix = "AWSCognitoIdentityService."
	contentType                 = "application/x-amz-json-1.1"

	// millisPerSecond is the divisor to convert Unix milliseconds to seconds
	// (as a float64) for AWS timestamp fields.
	millisPerSecond = 1000.0
)

var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for Cognito Identity Pool operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]service.JSONOpFunc
	region  string
}

// NewHandler creates a new Cognito Identity handler.
func NewHandler(backend *InMemoryBackend, region string) *Handler {
	h := &Handler{Backend: backend, region: region}
	h.ops = h.buildOps()

	return h
}

// Reset clears all backend state and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "CognitoIdentity" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateIdentityPool",
		"DeleteIdentityPool",
		"DescribeIdentityPool",
		"ListIdentityPools",
		"UpdateIdentityPool",
		"GetId",
		"GetCredentialsForIdentity",
		"GetOpenIdToken",
		"SetIdentityPoolRoles",
		"GetIdentityPoolRoles",
		"DeleteIdentities",
		"DescribeIdentity",
		"GetOpenIdTokenForDeveloperIdentity",
		"GetPrincipalTagAttributeMap",
		"ListIdentities",
		"ListTagsForResource",
		"LookupDeveloperIdentity",
		"MergeDeveloperIdentities",
		"SetPrincipalTagAttributeMap",
		"TagResource",
		"UnlinkDeveloperIdentity",
		"UnlinkIdentity",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "cognito-identity" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Cognito Identity instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.region} }

// RouteMatcher returns a function that matches Cognito Identity requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), cognitoIdentityTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Cognito Identity action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, cognitoIdentityTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the identity pool or identity resource from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		IdentityPoolID string `json:"IdentityPoolId"`
		IdentityID     string `json:"IdentityId"`
	}

	_ = json.Unmarshal(body, &req)

	if req.IdentityPoolID != "" {
		return req.IdentityPoolID
	}

	return req.IdentityID
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"AWSCognitoIdentityService", contentType,
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateIdentityPool":                 service.WrapOp(h.handleCreateIdentityPool),
		"DeleteIdentityPool":                 service.WrapOp(h.handleDeleteIdentityPool),
		"DescribeIdentityPool":               service.WrapOp(h.handleDescribeIdentityPool),
		"ListIdentityPools":                  service.WrapOp(h.handleListIdentityPools),
		"UpdateIdentityPool":                 service.WrapOp(h.handleUpdateIdentityPool),
		"GetId":                              service.WrapOp(h.handleGetID),
		"GetCredentialsForIdentity":          service.WrapOp(h.handleGetCredentialsForIdentity),
		"GetOpenIdToken":                     service.WrapOp(h.handleGetOpenIDToken),
		"SetIdentityPoolRoles":               service.WrapOp(h.handleSetIdentityPoolRoles),
		"GetIdentityPoolRoles":               service.WrapOp(h.handleGetIdentityPoolRoles),
		"DeleteIdentities":                   service.WrapOp(h.handleDeleteIdentities),
		"DescribeIdentity":                   service.WrapOp(h.handleDescribeIdentity),
		"GetOpenIdTokenForDeveloperIdentity": service.WrapOp(h.handleGetOpenIDTokenForDeveloperIdentity),
		"GetPrincipalTagAttributeMap":        service.WrapOp(h.handleGetPrincipalTagAttributeMap),
		"ListIdentities":                     service.WrapOp(h.handleListIdentities),
		"ListTagsForResource":                service.WrapOp(h.handleListTagsForResource),
		"LookupDeveloperIdentity":            service.WrapOp(h.handleLookupDeveloperIdentity),
		"MergeDeveloperIdentities":           service.WrapOp(h.handleMergeDeveloperIdentities),
		"SetPrincipalTagAttributeMap":        service.WrapOp(h.handleSetPrincipalTagAttributeMap),
		"TagResource":                        service.WrapOp(h.handleTagResource),
		"UnlinkDeveloperIdentity":            service.WrapOp(h.handleUnlinkDeveloperIdentity),
		"UnlinkIdentity":                     service.WrapOp(h.handleUnlinkIdentity),
		"UntagResource":                      service.WrapOp(h.handleUntagResource),
	}
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

// cognitoIdentitySentinelErrors maps sentinel errors to their AWS exception type names.
var cognitoIdentitySentinelErrors = []struct { //nolint:gochecknoglobals // package-level lookup table
	sentinel error
	typeName string
}{
	{ErrIdentityPoolNotFound, ErrIdentityPoolNotFound.Error()},
	{ErrIdentityPoolAlreadyExists, ErrIdentityPoolAlreadyExists.Error()},
	{ErrInvalidParameter, ErrInvalidParameter.Error()},
	{ErrNotAuthorized, ErrNotAuthorized.Error()},
	{errUnknownAction, "UnknownOperationException"},
}

func resolveErrorType(err error) (string, int) {
	for _, entry := range cognitoIdentitySentinelErrors {
		if errors.Is(err, entry.sentinel) {
			statusCode := http.StatusBadRequest
			if errors.Is(err, ErrNotAuthorized) {
				statusCode = http.StatusForbidden
			}

			return entry.typeName, statusCode
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

// --- Request/Response types ---

type cognitoIdentityProviderInput struct {
	ProviderName         string `json:"ProviderName"`
	ClientID             string `json:"ClientId"`
	ServerSideTokenCheck bool   `json:"ServerSideTokenCheck"`
}

type identityPoolOutput struct {
	SupportedLoginProviders        map[string]string              `json:"SupportedLoginProviders,omitempty"`
	IdentityPoolTags               map[string]string              `json:"IdentityPoolTags,omitempty"`
	OpenIDConnectProviderARNs      []string                       `json:"OpenIDConnectProviderARNs,omitempty"`
	SamlProviderARNs               []string                       `json:"SamlProviderARNs,omitempty"`
	IdentityPoolID                 string                         `json:"IdentityPoolId"`
	IdentityPoolName               string                         `json:"IdentityPoolName"`
	DeveloperProviderName          string                         `json:"DeveloperProviderName,omitempty"`
	IdentityProviders              []cognitoIdentityProviderInput `json:"CognitoIdentityProviders,omitempty"`
	AllowUnauthenticatedIdentities bool                           `json:"AllowUnauthenticatedIdentities"`
	AllowClassicFlow               bool                           `json:"AllowClassicFlow,omitempty"`
}

type createIdentityPoolInput struct {
	SupportedLoginProviders        map[string]string              `json:"SupportedLoginProviders"`
	Tags                           map[string]string              `json:"IdentityPoolTags"`
	OpenIDConnectProviderARNs      []string                       `json:"OpenIDConnectProviderARNs"`
	SamlProviderARNs               []string                       `json:"SamlProviderARNs"`
	IdentityPoolName               string                         `json:"IdentityPoolName"`
	DeveloperProviderName          string                         `json:"DeveloperProviderName"`
	IdentityProviders              []cognitoIdentityProviderInput `json:"CognitoIdentityProviders"`
	AllowUnauthenticatedIdentities bool                           `json:"AllowUnauthenticatedIdentities"`
	AllowClassicFlow               bool                           `json:"AllowClassicFlow"`
}

func (h *Handler) handleCreateIdentityPool(
	ctx context.Context,
	in *createIdentityPoolInput,
) (*identityPoolOutput, error) {
	providers := toBackendProviders(in.IdentityProviders)

	pool, err := h.Backend.CreateIdentityPool(
		ctx,
		in.IdentityPoolName,
		in.AllowUnauthenticatedIdentities,
		in.AllowClassicFlow,
		in.DeveloperProviderName,
		providers,
		in.SupportedLoginProviders,
		in.Tags,
		&PoolExtendedConfig{
			OpenIDConnectProviderARNs: in.OpenIDConnectProviderARNs,
			SamlProviderARNs:          in.SamlProviderARNs,
		},
	)
	if err != nil {
		return nil, err
	}

	return toIdentityPoolOutput(pool), nil
}

type deleteIdentityPoolInput struct {
	IdentityPoolID string `json:"IdentityPoolId"`
}

type deleteIdentityPoolOutput struct{}

func (h *Handler) handleDeleteIdentityPool(
	ctx context.Context,
	in *deleteIdentityPoolInput,
) (*deleteIdentityPoolOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteIdentityPool(ctx, in.IdentityPoolID); err != nil {
		return nil, err
	}

	return &deleteIdentityPoolOutput{}, nil
}

type describeIdentityPoolInput struct {
	IdentityPoolID string `json:"IdentityPoolId"`
}

func (h *Handler) handleDescribeIdentityPool(
	ctx context.Context,
	in *describeIdentityPoolInput,
) (*identityPoolOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	pool, err := h.Backend.DescribeIdentityPool(ctx, in.IdentityPoolID)
	if err != nil {
		return nil, err
	}

	return toIdentityPoolOutput(pool), nil
}

type listIdentityPoolsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type identityPoolShortDescription struct {
	IdentityPoolID   string `json:"IdentityPoolId"`
	IdentityPoolName string `json:"IdentityPoolName"`
}

type listIdentityPoolsOutput struct {
	NextToken     string                         `json:"NextToken,omitempty"`
	IdentityPools []identityPoolShortDescription `json:"IdentityPools"`
}

func (h *Handler) handleListIdentityPools(
	ctx context.Context,
	in *listIdentityPoolsInput,
) (*listIdentityPoolsOutput, error) {
	pools, nextToken := h.Backend.ListIdentityPools(ctx, in.MaxResults, in.NextToken)

	items := make([]identityPoolShortDescription, 0, len(pools))
	for _, p := range pools {
		items = append(items, identityPoolShortDescription{
			IdentityPoolID:   p.IdentityPoolID,
			IdentityPoolName: p.IdentityPoolName,
		})
	}

	return &listIdentityPoolsOutput{IdentityPools: items, NextToken: nextToken}, nil
}

type updateIdentityPoolInput struct {
	SupportedLoginProviders        map[string]string              `json:"SupportedLoginProviders"`
	IdentityPoolTags               map[string]string              `json:"IdentityPoolTags"`
	OpenIDConnectProviderARNs      []string                       `json:"OpenIDConnectProviderARNs"`
	SamlProviderARNs               []string                       `json:"SamlProviderARNs"`
	IdentityPoolID                 string                         `json:"IdentityPoolId"`
	IdentityPoolName               string                         `json:"IdentityPoolName"`
	DeveloperProviderName          string                         `json:"DeveloperProviderName"`
	IdentityProviders              []cognitoIdentityProviderInput `json:"CognitoIdentityProviders"`
	AllowUnauthenticatedIdentities bool                           `json:"AllowUnauthenticatedIdentities"`
	AllowClassicFlow               bool                           `json:"AllowClassicFlow"`
}

func (h *Handler) handleUpdateIdentityPool(
	ctx context.Context,
	in *updateIdentityPoolInput,
) (*identityPoolOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	providers := toBackendProviders(in.IdentityProviders)

	pool, err := h.Backend.UpdateIdentityPool(
		ctx,
		in.IdentityPoolID,
		in.IdentityPoolName,
		in.AllowUnauthenticatedIdentities,
		in.AllowClassicFlow,
		in.DeveloperProviderName,
		providers,
		in.SupportedLoginProviders,
		in.IdentityPoolTags,
		&PoolExtendedConfig{
			OpenIDConnectProviderARNs: in.OpenIDConnectProviderARNs,
			SamlProviderARNs:          in.SamlProviderARNs,
		},
	)
	if err != nil {
		return nil, err
	}

	return toIdentityPoolOutput(pool), nil
}

type getIDInput struct {
	Logins         map[string]string `json:"Logins"`
	IdentityPoolID string            `json:"IdentityPoolId"`
	AccountID      string            `json:"AccountId"`
}

type getIDOutput struct {
	IdentityID string `json:"IdentityId"`
}

func (h *Handler) handleGetID(ctx context.Context, in *getIDInput) (*getIDOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	identity, err := h.Backend.GetID(ctx, in.IdentityPoolID, in.AccountID, in.Logins)
	if err != nil {
		return nil, err
	}

	return &getIDOutput{IdentityID: identity.IdentityID}, nil
}

type getCredentialsForIdentityInput struct {
	Logins        map[string]string `json:"Logins"`
	IdentityID    string            `json:"IdentityId"`
	CustomRoleArn string            `json:"CustomRoleArn"`
}

type credentialsOutput struct {
	AccessKeyID  string `json:"AccessKeyId"`
	SecretKey    string `json:"SecretKey"`
	SessionToken string `json:"SessionToken"`
	Expiration   int64  `json:"Expiration"`
}

type getCredentialsForIdentityOutput struct {
	IdentityID  string            `json:"IdentityId"`
	Credentials credentialsOutput `json:"Credentials"`
}

func (h *Handler) handleGetCredentialsForIdentity(
	ctx context.Context,
	in *getCredentialsForIdentityInput,
) (*getCredentialsForIdentityOutput, error) {
	creds, err := h.Backend.GetCredentialsForIdentity(ctx, in.IdentityID, in.Logins)
	if err != nil {
		return nil, err
	}

	return &getCredentialsForIdentityOutput{
		IdentityID: creds.IdentityID,
		Credentials: credentialsOutput{
			AccessKeyID:  creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
			Expiration:   creds.Expiration.UnixMilli(),
		},
	}, nil
}

type getOpenIDTokenInput struct {
	Logins     map[string]string `json:"Logins"`
	IdentityID string            `json:"IdentityId"`
}

type getOpenIDTokenOutput struct {
	IdentityID string `json:"IdentityId"`
	Token      string `json:"Token"`
}

func (h *Handler) handleGetOpenIDToken(
	ctx context.Context,
	in *getOpenIDTokenInput,
) (*getOpenIDTokenOutput, error) {
	token, err := h.Backend.GetOpenIDToken(ctx, in.IdentityID, in.Logins)
	if err != nil {
		return nil, err
	}

	return &getOpenIDTokenOutput{
		IdentityID: token.IdentityID,
		Token:      token.Token,
	}, nil
}

type roleMappingRuleInput struct {
	Claim     string `json:"Claim"`
	MatchType string `json:"MatchType"`
	Value     string `json:"Value"`
	RoleARN   string `json:"RoleARN"`
}

type rulesConfigurationInput struct {
	Rules []roleMappingRuleInput `json:"Rules"`
}

type roleMappingInput struct {
	RulesConfiguration      *rulesConfigurationInput `json:"RulesConfiguration,omitempty"`
	Type                    string                   `json:"Type"`
	AmbiguousRoleResolution string                   `json:"AmbiguousRoleResolution,omitempty"`
}

type setIdentityPoolRolesInput struct {
	Roles          map[string]string           `json:"Roles"`
	RoleMappings   map[string]roleMappingInput `json:"RoleMappings"`
	IdentityPoolID string                      `json:"IdentityPoolId"`
}

type setIdentityPoolRolesOutput struct{}

func (h *Handler) handleSetIdentityPoolRoles(
	ctx context.Context,
	in *setIdentityPoolRolesInput,
) (*setIdentityPoolRolesOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if len(in.Roles) == 0 {
		return nil, fmt.Errorf(
			"%w: Roles must contain at least one of authenticated or unauthenticated",
			ErrInvalidParameter,
		)
	}

	var roleMappings map[string]RoleMapping
	if len(in.RoleMappings) > 0 {
		roleMappings = make(map[string]RoleMapping, len(in.RoleMappings))

		for provider, rm := range in.RoleMappings {
			backendRM := RoleMapping{
				Type:                    rm.Type,
				AmbiguousRoleResolution: rm.AmbiguousRoleResolution,
			}

			if rm.RulesConfiguration != nil {
				rules := make([]MappingRule, len(rm.RulesConfiguration.Rules))

				for i, r := range rm.RulesConfiguration.Rules {
					rules[i] = MappingRule(r)
				}

				backendRM.RulesConfiguration = &RulesConfiguration{Rules: rules}
			}

			roleMappings[provider] = backendRM
		}
	}

	if err := h.Backend.SetIdentityPoolRoles(
		ctx,
		in.IdentityPoolID,
		in.Roles["authenticated"],
		in.Roles["unauthenticated"],
		roleMappings,
	); err != nil {
		return nil, err
	}

	return &setIdentityPoolRolesOutput{}, nil
}

type getIdentityPoolRolesInput struct {
	IdentityPoolID string `json:"IdentityPoolId"`
}

type roleMappingRuleOutput struct {
	Claim     string `json:"Claim"`
	MatchType string `json:"MatchType"`
	Value     string `json:"Value"`
	RoleARN   string `json:"RoleARN"`
}

type rulesConfigurationOutput struct {
	Rules []roleMappingRuleOutput `json:"Rules"`
}

type roleMappingOutput struct {
	RulesConfiguration      *rulesConfigurationOutput `json:"RulesConfiguration,omitempty"`
	Type                    string                    `json:"Type"`
	AmbiguousRoleResolution string                    `json:"AmbiguousRoleResolution,omitempty"`
}

type getIdentityPoolRolesOutput struct {
	Roles          map[string]string            `json:"Roles"`
	RoleMappings   map[string]roleMappingOutput `json:"RoleMappings,omitempty"`
	IdentityPoolID string                       `json:"IdentityPoolId"`
}

func (h *Handler) handleGetIdentityPoolRoles(
	ctx context.Context,
	in *getIdentityPoolRolesInput,
) (*getIdentityPoolRolesOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	roles, err := h.Backend.GetIdentityPoolRoles(ctx, in.IdentityPoolID)
	if err != nil {
		return nil, err
	}

	// Only include roles that have been set (omit empty strings like AWS does).
	const maxRoleTypes = 2
	rolesMap := make(map[string]string, maxRoleTypes)
	if roles.AuthenticatedRoleARN != "" {
		rolesMap["authenticated"] = roles.AuthenticatedRoleARN
	}
	if roles.UnauthenticatedRoleARN != "" {
		rolesMap["unauthenticated"] = roles.UnauthenticatedRoleARN
	}

	var outMappings map[string]roleMappingOutput
	if len(roles.RoleMappings) > 0 {
		outMappings = make(map[string]roleMappingOutput, len(roles.RoleMappings))

		for provider, rm := range roles.RoleMappings {
			outRM := roleMappingOutput{
				Type:                    rm.Type,
				AmbiguousRoleResolution: rm.AmbiguousRoleResolution,
			}

			if rm.RulesConfiguration != nil {
				rules := make([]roleMappingRuleOutput, len(rm.RulesConfiguration.Rules))

				for i, r := range rm.RulesConfiguration.Rules {
					rules[i] = roleMappingRuleOutput(r)
				}

				outRM.RulesConfiguration = &rulesConfigurationOutput{Rules: rules}
			}

			outMappings[provider] = outRM
		}
	}

	return &getIdentityPoolRolesOutput{
		IdentityPoolID: in.IdentityPoolID,
		Roles:          rolesMap,
		RoleMappings:   outMappings,
	}, nil
}

// toBackendProviders converts handler-level provider inputs to backend provider structs.
func toBackendProviders(in []cognitoIdentityProviderInput) []IdentityProvider {
	if in == nil {
		return nil
	}

	out := make([]IdentityProvider, len(in))
	for i, p := range in {
		out[i] = IdentityProvider(p)
	}

	return out
}

// toIdentityPoolOutput converts a backend IdentityPool to the handler output struct.
func toIdentityPoolOutput(pool *IdentityPool) *identityPoolOutput {
	providers := make([]cognitoIdentityProviderInput, len(pool.IdentityProviders))
	for i, p := range pool.IdentityProviders {
		providers[i] = cognitoIdentityProviderInput(p)
	}

	return &identityPoolOutput{
		IdentityPoolID:                 pool.IdentityPoolID,
		IdentityPoolName:               pool.IdentityPoolName,
		DeveloperProviderName:          pool.DeveloperProviderName,
		AllowUnauthenticatedIdentities: pool.AllowUnauthenticatedIdentities,
		AllowClassicFlow:               pool.AllowClassicFlow,
		IdentityProviders:              providers,
		SupportedLoginProviders:        pool.SupportedLoginProviders,
		IdentityPoolTags:               pool.Tags,
		OpenIDConnectProviderARNs:      pool.OpenIDConnectProviderARNs,
		SamlProviderARNs:               pool.SamlProviderARNs,
	}
}

// --- New operations ---

type deleteIdentitiesInput struct {
	IdentityIDsToDelete []string `json:"IdentityIdsToDelete"`
}

type unprocessedIdentityIDOutput struct {
	ErrorCode  string `json:"ErrorCode"`
	IdentityID string `json:"IdentityId"`
}

type deleteIdentitiesOutput struct {
	UnprocessedIdentityIDs []unprocessedIdentityIDOutput `json:"UnprocessedIdentityIds"`
}

func (h *Handler) handleDeleteIdentities(
	ctx context.Context,
	in *deleteIdentitiesInput,
) (*deleteIdentitiesOutput, error) {
	if len(in.IdentityIDsToDelete) == 0 {
		return nil, fmt.Errorf("%w: IdentityIdsToDelete must not be empty", ErrInvalidParameter)
	}

	unprocessed, err := h.Backend.DeleteIdentities(ctx, in.IdentityIDsToDelete)
	if err != nil {
		return nil, err
	}

	out := make([]unprocessedIdentityIDOutput, 0, len(unprocessed))
	for _, u := range unprocessed {
		out = append(out, unprocessedIdentityIDOutput(u))
	}

	return &deleteIdentitiesOutput{UnprocessedIdentityIDs: out}, nil
}

type describeIdentityInput struct {
	IdentityID string `json:"IdentityId"`
}

type describeIdentityOutput struct {
	IdentityID       string   `json:"IdentityId"`
	Logins           []string `json:"Logins"`
	CreationDate     float64  `json:"CreationDate"`
	LastModifiedDate float64  `json:"LastModifiedDate"`
}

func (h *Handler) handleDescribeIdentity(
	ctx context.Context,
	in *describeIdentityInput,
) (*describeIdentityOutput, error) {
	if in.IdentityID == "" {
		return nil, fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	desc, err := h.Backend.DescribeIdentity(ctx, in.IdentityID)
	if err != nil {
		return nil, err
	}

	logins := desc.Logins
	if logins == nil {
		logins = []string{}
	}

	return &describeIdentityOutput{
		IdentityID:       desc.IdentityID,
		Logins:           logins,
		CreationDate:     float64(desc.CreationDate.UnixMilli()) / millisPerSecond,
		LastModifiedDate: float64(desc.LastModifiedDate.UnixMilli()) / millisPerSecond,
	}, nil
}

type getOpenIDTokenForDeveloperIdentityInput struct {
	Logins         map[string]string `json:"Logins"`
	IdentityPoolID string            `json:"IdentityPoolId"`
	IdentityID     string            `json:"IdentityId"`
	TokenDuration  int64             `json:"TokenDuration"`
}

type getOpenIDTokenForDeveloperIdentityOutput struct {
	IdentityID string `json:"IdentityId"`
	Token      string `json:"Token"`
}

func (h *Handler) handleGetOpenIDTokenForDeveloperIdentity(
	ctx context.Context,
	in *getOpenIDTokenForDeveloperIdentityInput,
) (*getOpenIDTokenForDeveloperIdentityOutput, error) {
	result, err := h.Backend.GetOpenIDTokenForDeveloperIdentity(
		ctx,
		in.IdentityPoolID,
		in.IdentityID,
		in.Logins,
		in.TokenDuration,
	)
	if err != nil {
		return nil, err
	}

	return &getOpenIDTokenForDeveloperIdentityOutput{
		IdentityID: result.IdentityID,
		Token:      result.Token,
	}, nil
}

type getPrincipalTagAttributeMapInput struct {
	IdentityPoolID       string `json:"IdentityPoolId"`
	IdentityProviderName string `json:"IdentityProviderName"`
}

type getPrincipalTagAttributeMapOutput struct {
	PrincipalTags        map[string]string `json:"PrincipalTags,omitempty"`
	IdentityPoolID       string            `json:"IdentityPoolId"`
	IdentityProviderName string            `json:"IdentityProviderName"`
	UseDefaults          bool              `json:"UseDefaults"`
}

func (h *Handler) handleGetPrincipalTagAttributeMap(
	ctx context.Context,
	in *getPrincipalTagAttributeMapInput,
) (*getPrincipalTagAttributeMapOutput, error) {
	mapping, err := h.Backend.GetPrincipalTagAttributeMap(ctx, in.IdentityPoolID, in.IdentityProviderName)
	if err != nil {
		return nil, err
	}

	return &getPrincipalTagAttributeMapOutput{
		IdentityPoolID:       in.IdentityPoolID,
		IdentityProviderName: in.IdentityProviderName,
		UseDefaults:          mapping.UseDefaults,
		PrincipalTags:        mapping.PrincipalTags,
	}, nil
}

type listIdentitiesInput struct {
	NextToken      string `json:"NextToken"`
	IdentityPoolID string `json:"IdentityPoolId"`
	MaxResults     int    `json:"MaxResults"`
	HideDisabled   bool   `json:"HideDisabled"`
}

type identityDescriptionOutput struct {
	IdentityID       string   `json:"IdentityId"`
	Logins           []string `json:"Logins"`
	CreationDate     float64  `json:"CreationDate"`
	LastModifiedDate float64  `json:"LastModifiedDate"`
}

type listIdentitiesOutput struct {
	IdentityPoolID string                      `json:"IdentityPoolId"`
	NextToken      string                      `json:"NextToken,omitempty"`
	Identities     []identityDescriptionOutput `json:"Identities"`
}

func (h *Handler) handleListIdentities(
	ctx context.Context,
	in *listIdentitiesInput,
) (*listIdentitiesOutput, error) {
	result, err := h.Backend.ListIdentities(ctx, in.IdentityPoolID, in.MaxResults, in.HideDisabled, in.NextToken)
	if err != nil {
		return nil, err
	}

	items := make([]identityDescriptionOutput, 0, len(result.Identities))
	for _, id := range result.Identities {
		logins := id.Logins
		if logins == nil {
			logins = []string{}
		}

		items = append(items, identityDescriptionOutput{
			IdentityID:       id.IdentityID,
			Logins:           logins,
			CreationDate:     float64(id.CreationDate.UnixMilli()) / millisPerSecond,
			LastModifiedDate: float64(id.LastModifiedDate.UnixMilli()) / millisPerSecond,
		})
	}

	return &listIdentitiesOutput{
		IdentityPoolID: result.IdentityPoolID,
		NextToken:      result.NextToken,
		Identities:     items,
	}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	if tags == nil {
		tags = make(map[string]string)
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

type lookupDeveloperIdentityInput struct {
	NextToken               string `json:"NextToken"`
	IdentityPoolID          string `json:"IdentityPoolId"`
	IdentityID              string `json:"IdentityId"`
	DeveloperUserIdentifier string `json:"DeveloperUserIdentifier"`
	DeveloperProviderName   string `json:"DeveloperProviderName"`
	MaxResults              int    `json:"MaxResults"`
}

type lookupDeveloperIdentityOutput struct {
	IdentityID                  string   `json:"IdentityId"`
	NextToken                   string   `json:"NextToken,omitempty"`
	DeveloperUserIdentifierList []string `json:"DeveloperUserIdentifierList"`
}

func (h *Handler) handleLookupDeveloperIdentity(
	ctx context.Context,
	in *lookupDeveloperIdentityInput,
) (*lookupDeveloperIdentityOutput, error) {
	result, err := h.Backend.LookupDeveloperIdentity(
		ctx,
		in.IdentityPoolID,
		in.IdentityID,
		in.DeveloperUserIdentifier,
		in.DeveloperProviderName,
	)
	if err != nil {
		return nil, err
	}

	ids := result.DeveloperUserIdentifierList
	if ids == nil {
		ids = []string{}
	}

	return &lookupDeveloperIdentityOutput{
		IdentityID:                  result.IdentityID,
		DeveloperUserIdentifierList: ids,
	}, nil
}

type mergeDeveloperIdentitiesInput struct {
	SourceUserIdentifier      string `json:"SourceUserIdentifier"`
	DestinationUserIdentifier string `json:"DestinationUserIdentifier"`
	DeveloperProviderName     string `json:"DeveloperProviderName"`
	IdentityPoolID            string `json:"IdentityPoolId"`
}

type mergeDeveloperIdentitiesOutput struct {
	IdentityID string `json:"IdentityId"`
}

func (h *Handler) handleMergeDeveloperIdentities(
	ctx context.Context,
	in *mergeDeveloperIdentitiesInput,
) (*mergeDeveloperIdentitiesOutput, error) {
	identity, err := h.Backend.MergeDeveloperIdentities(
		ctx,
		in.SourceUserIdentifier,
		in.DestinationUserIdentifier,
		in.DeveloperProviderName,
		in.IdentityPoolID,
	)
	if err != nil {
		return nil, err
	}

	return &mergeDeveloperIdentitiesOutput{IdentityID: identity.IdentityID}, nil
}

type setPrincipalTagAttributeMapInput struct {
	PrincipalTags        map[string]string `json:"PrincipalTags"`
	IdentityPoolID       string            `json:"IdentityPoolId"`
	IdentityProviderName string            `json:"IdentityProviderName"`
	UseDefaults          bool              `json:"UseDefaults"`
}

type setPrincipalTagAttributeMapOutput struct {
	PrincipalTags        map[string]string `json:"PrincipalTags,omitempty"`
	IdentityPoolID       string            `json:"IdentityPoolId"`
	IdentityProviderName string            `json:"IdentityProviderName"`
	UseDefaults          bool              `json:"UseDefaults"`
}

func (h *Handler) handleSetPrincipalTagAttributeMap(
	ctx context.Context,
	in *setPrincipalTagAttributeMapInput,
) (*setPrincipalTagAttributeMapOutput, error) {
	mapping, err := h.Backend.SetPrincipalTagAttributeMap(
		ctx,
		in.IdentityPoolID,
		in.IdentityProviderName,
		in.UseDefaults,
		in.PrincipalTags,
	)
	if err != nil {
		return nil, err
	}

	return &setPrincipalTagAttributeMapOutput{
		IdentityPoolID:       in.IdentityPoolID,
		IdentityProviderName: in.IdentityProviderName,
		UseDefaults:          mapping.UseDefaults,
		PrincipalTags:        mapping.PrincipalTags,
	}, nil
}

type tagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceArn string            `json:"ResourceArn"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(ctx, in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type unlinkDeveloperIdentityInput struct {
	IdentityID              string `json:"IdentityId"`
	IdentityPoolID          string `json:"IdentityPoolId"`
	DeveloperProviderName   string `json:"DeveloperProviderName"`
	DeveloperUserIdentifier string `json:"DeveloperUserIdentifier"`
}

type unlinkDeveloperIdentityOutput struct{}

func (h *Handler) handleUnlinkDeveloperIdentity(
	ctx context.Context,
	in *unlinkDeveloperIdentityInput,
) (*unlinkDeveloperIdentityOutput, error) {
	if err := h.Backend.UnlinkDeveloperIdentity(
		ctx,
		in.IdentityID,
		in.IdentityPoolID,
		in.DeveloperProviderName,
		in.DeveloperUserIdentifier,
	); err != nil {
		return nil, err
	}

	return &unlinkDeveloperIdentityOutput{}, nil
}

type unlinkIdentityInput struct {
	IdentityID     string            `json:"IdentityId"`
	Logins         map[string]string `json:"Logins"`
	LoginsToRemove []string          `json:"LoginsToRemove"`
}

type unlinkIdentityOutput struct{}

func (h *Handler) handleUnlinkIdentity(
	ctx context.Context,
	in *unlinkIdentityInput,
) (*unlinkIdentityOutput, error) {
	if err := h.Backend.UnlinkIdentity(ctx, in.IdentityID, in.Logins, in.LoginsToRemove); err != nil {
		return nil, err
	}

	return &unlinkIdentityOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}
