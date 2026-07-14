package apigatewayv2

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// defaultRegion is used for ARNs and execute-api endpoints when the request
// context carries no region (e.g. an unsigned request).
const defaultRegion = "us-east-1"

// regionFromCtx returns the request-scoped region from the ctxbag, falling back
// to the service default so endpoints/ARNs are always well-formed.
func regionFromCtx(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return defaultRegion
}

func applyDomainNameDefaults(
	in []DomainNameConfiguration,
	domain, region string,
) []DomainNameConfiguration {
	configs := make([]DomainNameConfiguration, len(in))
	copy(configs, in)

	for i := range configs {
		if configs[i].DomainNameStatus == "" {
			configs[i].DomainNameStatus = "AVAILABLE"
		}

		if configs[i].SecurityPolicy == "" {
			configs[i].SecurityPolicy = "TLS_1_2"
		}

		if configs[i].EndpointType == "" {
			configs[i].EndpointType = "REGIONAL"
		}

		if configs[i].APIGatewayDomainName == "" {
			configs[i].APIGatewayDomainName = domain + ".execute-api." + region + ".amazonaws.com"
		}

		if configs[i].HostedZoneID == "" {
			configs[i].HostedZoneID = "Z2FDTNDATAQYW2"
		}
	}

	return configs
}

const (
	apiIDChars  = "abcdefghijklmnopqrstuvwxyz0123456789"
	apiIDLength = 10

	authorizerTypeJWT     = "JWT"
	authorizerTypeRequest = "REQUEST"
	authorizationTypeNone = "NONE"
	// authorizationTypeCustom is the route AuthorizationType for a Lambda
	// (REQUEST-type) authorizer.
	authorizationTypeCustom = "CUSTOM"
	// authorizationTypeAWSIAM is the route AuthorizationType requiring SigV4.
	authorizationTypeAWSIAM = "AWS_IAM"
	protocolTypeHTTP        = "HTTP"
	integrationTypeHTTP     = "HTTP"
	// httpMethodAny is the HTTP route-key method wildcard ("ANY /path")
	// matching every HTTP method, also used as the default IntegrationMethod
	// for the integration CreateApi's quick-create shortcut auto-provisions.
	httpMethodAny = "ANY"

	integrationTimeoutMin = int32(50)
	// integrationTimeoutMaxWebSocket is the maximum (and default) integration
	// timeout for WebSocket APIs: 29 seconds.
	integrationTimeoutMaxWebSocket = int32(29000)
	// integrationTimeoutMaxHTTP is the maximum (and default) integration timeout
	// for HTTP APIs: 30 seconds. AWS allows a longer ceiling for HTTP APIs than
	// for WebSocket APIs; see CreateIntegration/UpdateIntegration docs.
	integrationTimeoutMaxHTTP = int32(30000)

	// connectionTypeInternet is the default Integration ConnectionType when the
	// caller does not specify one.
	connectionTypeInternet = "INTERNET"
	connectionTypeVpcLink  = "VPC_LINK"
)

// validRouteAuthorizationType reports whether t is a valid route AuthorizationType
// for API Gateway v2 (the AWS-modeled enum: NONE, AWS_IAM, JWT, CUSTOM).
func validRouteAuthorizationType(t string) bool {
	switch t {
	case authorizationTypeNone, authorizationTypeAWSIAM, authorizerTypeJWT, authorizationTypeCustom:
		return true
	default:
		return false
	}
}

// isValidHTTPRouteKeyMethod reports whether method is accepted in an HTTP API route key.
func isValidHTTPRouteKeyMethod(method string) bool {
	switch method {
	case "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", httpMethodAny:
		return true
	default:
		return false
	}
}

// validateHTTPRouteKey returns ErrBadRequest if key is invalid for an HTTP API.
// Valid forms: "$default" or "METHOD /path" (e.g. "GET /items").
func validateHTTPRouteKey(key string) error {
	if key == "$default" {
		return nil
	}

	const maxParts = 2
	parts := strings.SplitN(key, " ", maxParts)
	if len(parts) != maxParts || !isValidHTTPRouteKeyMethod(parts[0]) || !strings.HasPrefix(parts[1], "/") {
		return fmt.Errorf(
			"%w: routeKey must be $default or start with a valid HTTP method and a forward slash, e.g. GET /items",
			ErrBadRequest,
		)
	}

	return nil
}

// integrationTimeoutMaxFor returns the maximum (and default) integration
// timeout in milliseconds for the given API protocol type: 30,000ms for HTTP
// APIs and 29,000ms for WebSocket APIs, matching real API Gateway v2 limits.
func integrationTimeoutMaxFor(protocolType string) int32 {
	if protocolType == protocolTypeHTTP {
		return integrationTimeoutMaxHTTP
	}

	return integrationTimeoutMaxWebSocket
}

// validateTimeoutInMillis returns ErrBadRequest if ms is outside
// [50, integrationTimeoutMaxFor(protocolType)].
func validateTimeoutInMillis(ms int32, protocolType string) error {
	maxMs := integrationTimeoutMaxFor(protocolType)
	if ms < integrationTimeoutMin || ms > maxMs {
		return fmt.Errorf(
			"%w: timeoutInMillis must be between %d and %d",
			ErrBadRequest, integrationTimeoutMin, maxMs,
		)
	}

	return nil
}

// validateConnectionType returns ErrBadRequest if connectionType is not one of
// the modeled enum values (INTERNET, VPC_LINK), or if VPC_LINK is specified
// without a connectionId (the VPC link to route the private integration
// through), matching real API Gateway v2 validation.
func validateConnectionType(connectionType, connectionID string) error {
	switch connectionType {
	case connectionTypeInternet:
		return nil
	case connectionTypeVpcLink:
		if connectionID == "" {
			return fmt.Errorf("%w: connectionId is required when connectionType is VPC_LINK", ErrBadRequest)
		}

		return nil
	default:
		return fmt.Errorf("%w: connectionType must be one of INTERNET, VPC_LINK", ErrBadRequest)
	}
}

// cloneIntegrationTLSConfig returns a deep copy of cfg, or nil if cfg is nil.
func cloneIntegrationTLSConfig(cfg *IntegrationTLSConfig) *IntegrationTLSConfig {
	if cfg == nil {
		return nil
	}

	cp := *cfg

	return &cp
}

// cloneMutualTLSAuthentication returns a deep copy of cfg, or nil if cfg is
// nil. TruststoreWarnings is never populated by the caller (it is an
// output-only field computed by API Gateway while validating the truststore);
// the emulator has no truststore to validate against S3, so it is left empty,
// matching the "no warnings" case real AWS returns for a well-formed request.
func cloneMutualTLSAuthentication(cfg *MutualTLSAuthentication) *MutualTLSAuthentication {
	if cfg == nil {
		return nil
	}

	cp := *cfg
	cp.TruststoreWarnings = nil

	return &cp
}

var (
	// ErrAPINotFound is returned when a requested API does not exist.
	ErrAPINotFound = errors.New("NotFoundException")
	// ErrStageNotFound is returned when a requested stage does not exist.
	ErrStageNotFound = errors.New("NotFoundException")
	// ErrRouteNotFound is returned when a requested route does not exist.
	ErrRouteNotFound = errors.New("NotFoundException")
	// ErrIntegrationNotFound is returned when a requested integration does not exist.
	ErrIntegrationNotFound = errors.New("NotFoundException")
	// ErrDeploymentNotFound is returned when a requested deployment does not exist.
	ErrDeploymentNotFound = errors.New("NotFoundException")
	// ErrAuthorizerNotFound is returned when a requested authorizer does not exist.
	ErrAuthorizerNotFound = errors.New("NotFoundException")
	// ErrDomainNameNotFound is returned when a requested domain name does not exist.
	ErrDomainNameNotFound = errors.New("NotFoundException")
	// ErrAPIMappingNotFound is returned when a requested API mapping does not exist.
	ErrAPIMappingNotFound = errors.New("NotFoundException")
	// ErrIntegrationResponseNotFound is returned when a requested integration response does not exist.
	ErrIntegrationResponseNotFound = errors.New("NotFoundException")
	// ErrModelNotFound is returned when a requested model does not exist.
	ErrModelNotFound = errors.New("NotFoundException")
	// ErrRouteResponseNotFound is returned when a requested route response does not exist.
	ErrRouteResponseNotFound = errors.New("NotFoundException")
	// ErrPortalNotFound is returned when a requested portal does not exist.
	ErrPortalNotFound = errors.New("NotFoundException")
	// ErrPortalProductNotFound is returned when a requested portal product does not exist.
	ErrPortalProductNotFound = errors.New("NotFoundException")
	// ErrBadRequest is returned when required fields are missing or invalid.
	ErrBadRequest = errors.New("BadRequestException")
	// ErrAlreadyExists is returned when a resource with the same identifier already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrProductPageNotFound is returned when a requested product page does not exist.
	ErrProductPageNotFound = errors.New("NotFoundException")
	// ErrProductREPageNotFound is returned when a requested product REST endpoint page does not exist.
	ErrProductREPageNotFound = errors.New("NotFoundException")
	// ErrVpcLinkNotFound is returned when a requested VPC link does not exist.
	ErrVpcLinkNotFound = errors.New("NotFoundException")
	// ErrRoutingRuleNotFound is returned when a requested routing rule does not exist.
	ErrRoutingRuleNotFound = errors.New("NotFoundException")
)

const (
	// IntegrationTypeAWSProxy is the AWS_PROXY integration type.
	IntegrationTypeAWSProxy = "AWS_PROXY"
	// integrationTypeHTTPProxy ("HTTP_PROXY") is declared in http_proxy.go.
)

// StorageBackend is the interface for the API Gateway v2 in-memory store.
type StorageBackend interface {
	// APIs
	CreateAPI(ctx context.Context, input CreateAPIInput) (*API, error)
	GetAPI(apiID string) (*API, error)
	GetAPIs() ([]API, error)
	DeleteAPI(apiID string) error
	UpdateAPI(apiID string, input UpdateAPIInput) (*API, error)

	// Stages
	CreateStage(apiID string, input CreateStageInput) (*Stage, error)
	GetStage(apiID, stageName string) (*Stage, error)
	GetStages(apiID string) ([]Stage, error)
	DeleteStage(apiID, stageName string) error
	UpdateStage(apiID, stageName string, input UpdateStageInput) (*Stage, error)

	// Routes
	CreateRoute(apiID string, input CreateRouteInput) (*Route, error)
	GetRoute(apiID, routeID string) (*Route, error)
	GetRoutes(apiID string) ([]Route, error)
	DeleteRoute(apiID, routeID string) error
	UpdateRoute(apiID, routeID string, input UpdateRouteInput) (*Route, error)

	// Integrations
	CreateIntegration(apiID string, input CreateIntegrationInput) (*Integration, error)
	GetIntegration(apiID, integrationID string) (*Integration, error)
	GetIntegrations(apiID string) ([]Integration, error)
	DeleteIntegration(apiID, integrationID string) error
	UpdateIntegration(apiID, integrationID string, input UpdateIntegrationInput) (*Integration, error)

	// Deployments
	CreateDeployment(apiID string, input CreateDeploymentInput) (*Deployment, error)
	GetDeployment(apiID, deploymentID string) (*Deployment, error)
	GetDeployments(apiID string) ([]Deployment, error)
	DeleteDeployment(apiID, deploymentID string) error

	// Authorizers
	CreateAuthorizer(apiID string, input CreateAuthorizerInput) (*Authorizer, error)
	GetAuthorizer(apiID, authorizerID string) (*Authorizer, error)
	GetAuthorizers(apiID string) ([]Authorizer, error)
	DeleteAuthorizer(apiID, authorizerID string) error
	UpdateAuthorizer(apiID, authorizerID string, input UpdateAuthorizerInput) (*Authorizer, error)

	// Domain Names
	CreateDomainName(ctx context.Context, input CreateDomainNameInput) (*DomainName, error)

	// API Mappings
	CreateAPIMapping(domainName string, input CreateAPIMappingInput) (*APIMapping, error)

	// Integration Responses
	CreateIntegrationResponse(
		apiID, integrationID string,
		input CreateIntegrationResponseInput,
	) (*IntegrationResponse, error)

	// Models
	CreateModel(apiID string, input CreateModelInput) (*Model, error)

	// Route Responses
	CreateRouteResponse(apiID, routeID string, input CreateRouteResponseInput) (*RouteResponse, error)

	// Portals
	CreatePortal(input CreatePortalInput) (*Portal, error)

	// Portal Products
	CreatePortalProduct(input CreatePortalProductInput) (*PortalProduct, error)

	// Product Pages
	CreateProductPage(portalProductID string, input CreateProductPageInput) (*ProductPage, error)

	// Product REST Endpoint Pages
	CreateProductRestEndpointPage(
		portalProductID string,
		input CreateProductRestEndpointPageInput,
	) (*ProductRestEndpointPage, error)

	// VPC links
	CreateVpcLink(input CreateVpcLinkInput) (*VpcLink, error)
	GetVpcLink(vpcLinkID string) (*VpcLink, error)
	GetVpcLinks() ([]VpcLink, error)
	UpdateVpcLink(vpcLinkID string, input UpdateVpcLinkInput) (*VpcLink, error)
	DeleteVpcLink(vpcLinkID string) error

	// Routing rules
	CreateRoutingRule(
		ctx context.Context,
		domainName string,
		input CreateRoutingRuleInput,
	) (*RoutingRule, error)
	GetRoutingRule(domainName, routingRuleID string) (*RoutingRule, error)
	ListRoutingRules(domainName string) ([]RoutingRule, error)
	PutRoutingRule(domainName, routingRuleID string, input PutRoutingRuleInput) (*RoutingRule, error)
	DeleteRoutingRule(domainName, routingRuleID string) error

	// Portal sharing policy
	GetPortalProductSharingPolicy(portalProductID string) (*PortalProductSharingPolicy, error)
	PutPortalProductSharingPolicy(portalProductID, policyDocument string) (*PortalProductSharingPolicy, error)
	DeletePortalProductSharingPolicy(portalProductID string) error

	// Domain Names - Get/Delete
	GetDomainName(domainName string) (*DomainName, error)
	GetDomainNames() ([]DomainName, error)
	DeleteDomainName(domainName string) error

	// API Mappings - Get/Delete
	GetAPIMapping(domainName, mappingID string) (*APIMapping, error)
	GetAPIMappings(domainName string) ([]APIMapping, error)
	DeleteAPIMapping(domainName, mappingID string) error

	// Integration Responses - Get/Delete
	GetIntegrationResponse(apiID, integrationID, responseID string) (*IntegrationResponse, error)
	GetIntegrationResponses(apiID, integrationID string) ([]IntegrationResponse, error)
	DeleteIntegrationResponse(apiID, integrationID, responseID string) error

	// Models - Get/Delete
	GetModel(apiID, modelID string) (*Model, error)
	GetModels(apiID string) ([]Model, error)
	DeleteModel(apiID, modelID string) error

	// Route Responses - Get/Delete
	GetRouteResponse(apiID, routeID, responseID string) (*RouteResponse, error)
	GetRouteResponses(apiID, routeID string) ([]RouteResponse, error)
	DeleteRouteResponse(apiID, routeID, responseID string) error

	// Portals - Get/List
	GetPortal(portalID string) (*Portal, error)
	ListPortals() ([]Portal, error)

	// Portal Products - Get/List
	GetPortalProduct(portalProductID string) (*PortalProduct, error)
	ListPortalProducts() ([]PortalProduct, error)

	// Product Pages - List
	ListProductPages(portalProductID string) ([]ProductPage, error)

	// Product REST Endpoint Pages - List
	ListProductRestEndpointPages(portalProductID string) ([]ProductRestEndpointPage, error)

	// Tags
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	GetTags(resourceARN string) (map[string]string, error)

	// ExportAPI generates an OpenAPI specification for the API's routes.
	ExportAPI(apiID string) (map[string]any, error)

	// UpdateAPIMapping
	UpdateAPIMapping(domainName, mappingID string, input UpdateAPIMappingInput) (*APIMapping, error)

	// UpdateDeployment
	UpdateDeployment(apiID, deploymentID string, input UpdateDeploymentInput) (*Deployment, error)

	// UpdateDomainName
	UpdateDomainName(domainName string, input UpdateDomainNameInput) (*DomainName, error)

	// UpdateIntegrationResponse
	UpdateIntegrationResponse(
		apiID, integrationID, responseID string,
		input UpdateIntegrationResponseInput,
	) (*IntegrationResponse, error)

	// UpdateModel
	UpdateModel(apiID, modelID string, input UpdateModelInput) (*Model, error)

	// UpdateRouteResponse
	UpdateRouteResponse(apiID, routeID, responseID string, input UpdateRouteResponseInput) (*RouteResponse, error)

	// UpdatePortal
	UpdatePortal(portalID string, input UpdatePortalInput) (*Portal, error)

	// UpdatePortalProduct
	UpdatePortalProduct(portalProductID string, input UpdatePortalProductInput) (*PortalProduct, error)

	// UpdateProductPage
	UpdateProductPage(portalProductID, pageID string, input UpdateProductPageInput) (*ProductPage, error)

	// UpdateProductRestEndpointPage
	UpdateProductRestEndpointPage(
		portalProductID, pageID string,
		input UpdateProductRestEndpointPageInput,
	) (*ProductRestEndpointPage, error)

	// DeletePortal
	DeletePortal(portalID string) error

	// DeletePortalProduct
	DeletePortalProduct(portalProductID string) error

	// GetProductPage
	GetProductPage(portalProductID, pageID string) (*ProductPage, error)

	// GetProductRestEndpointPage
	GetProductRestEndpointPage(portalProductID, pageID string) (*ProductRestEndpointPage, error)

	// DeleteProductPage
	DeleteProductPage(portalProductID, pageID string) error

	// DeleteProductRestEndpointPage
	DeleteProductRestEndpointPage(portalProductID, pageID string) error

	// ResetAuthorizersCache
	ResetAuthorizersCache(apiID, stageName string) error

	// DeleteCorsConfiguration clears the CORS configuration on an API.
	DeleteCorsConfiguration(apiID string) error

	// DeleteAccessLogSettings clears the access log settings on a stage.
	DeleteAccessLogSettings(apiID, stageName string) error

	// DeleteRouteSettings removes per-route settings for a specific routeKey from a stage.
	DeleteRouteSettings(apiID, stageName, routeKey string) error

	// DeleteRouteRequestParameter removes a specific request parameter from a route.
	DeleteRouteRequestParameter(apiID, routeID, requestParameterKey string) error
}

// InMemoryBackend implements StorageBackend using pkgs/store tables. Every
// resource family nested under an API/domain name/portal product (formerly a
// per-parent nested map -- see apiData in the pre-Phase-3.3 history) is now a
// single flat table keyed by a composite "<parentID>#<childID>" string, with
// a secondary [store.Index] grouping by parent ID. See store_setup.go's doc
// comment for the full clean/dirty table split.
type InMemoryBackend struct {
	apis                              *store.Table[API]
	stages                            *store.Table[Stage]
	stagesByAPI                       *store.Index[Stage]
	routes                            *store.Table[Route]
	routesByAPI                       *store.Index[Route]
	integrations                      *store.Table[Integration]
	integrationsByAPI                 *store.Index[Integration]
	deployments                       *store.Table[Deployment]
	deploymentsByAPI                  *store.Index[Deployment]
	authorizers                       *store.Table[Authorizer]
	authorizersByAPI                  *store.Index[Authorizer]
	models                            *store.Table[Model]
	modelsByAPI                       *store.Index[Model]
	integrationResponses              *store.Table[IntegrationResponse]
	integrationResponsesByIntegration *store.Index[IntegrationResponse]
	routeResponses                    *store.Table[RouteResponse]
	routeResponsesByRoute             *store.Index[RouteResponse]
	domainNames                       *store.Table[DomainName]
	apiMappings                       *store.Table[APIMapping]
	apiMappingsByDomain               *store.Index[APIMapping]
	portals                           *store.Table[Portal]
	portalProducts                    *store.Table[PortalProduct]
	// portalProductSharingPolicies (portalProductID -> policy document) is a
	// plain map, not a store.Table -- see store_setup.go's doc comment for why.
	portalProductSharingPolicies  map[string]string
	productPages                  *store.Table[ProductPage]
	productPagesByPortalProduct   *store.Index[ProductPage]
	productREPages                *store.Table[ProductRestEndpointPage]
	productREPagesByPortalProduct *store.Index[ProductRestEndpointPage]
	vpcLinks                      *store.Table[VpcLink]
	routingRules                  *store.Table[RoutingRule]
	routingRulesByDomain          *store.Index[RoutingRule]
	registry                      *store.Registry
	mu                            *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		portalProductSharingPolicies: make(map[string]string),
		registry:                     store.NewRegistry(),
		mu:                           lockmetrics.New("apigatewayv2"),
	}
	registerAllTables(b)

	return b
}

// copyTags returns a deep copy of a tags map, guarding against nil.
func copyTags(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}

	return maps.Clone(src)
}

// Reset clears all backend state, reinitialising all tables.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	// The "dirty" tables (see store_setup.go's registerAllTables doc) are
	// deliberately NOT on b.registry, so each needs its own Reset() call here.
	b.stages.Reset()
	b.routes.Reset()
	b.integrations.Reset()
	b.deployments.Reset()
	b.authorizers.Reset()
	b.models.Reset()
	b.integrationResponses.Reset()
	b.routeResponses.Reset()
	b.apiMappings.Reset()
	b.productPages.Reset()
	b.productREPages.Reset()
	b.routingRules.Reset()

	b.portalProductSharingPolicies = make(map[string]string)
}

// randomID generates a cryptographically random 10-character alphanumeric ID.
func randomID() string {
	b := make([]byte, apiIDLength)
	charCount := uint64(len(apiIDChars))

	for i := range b {
		var v [8]byte
		// crypto/rand.Read always fills the buffer and never returns a non-nil error.
		_, _ = rand.Read(v[:])
		b[i] = apiIDChars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return string(b)
}

// --- APIs ---

// CreateAPI creates a new HTTP API.
func (b *InMemoryBackend) CreateAPI(ctx context.Context, input CreateAPIInput) (*API, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrBadRequest)
	}

	b.mu.Lock("CreateAPI")
	defer b.mu.Unlock()

	validProtocols := map[string]bool{protocolTypeHTTP: true, protocolTypeWebSocket: true}
	if !validProtocols[input.ProtocolType] {
		return nil, fmt.Errorf("%w: protocolType must be HTTP or WEBSOCKET", ErrBadRequest)
	}

	if err := validateQuickCreateInput(input); err != nil {
		return nil, err
	}

	// Apply AWS-realistic default RouteSelectionExpression when not provided.
	rse := input.RouteSelectionExpression
	if rse == "" {
		if input.ProtocolType == protocolTypeWebSocket {
			rse = "$request.body.action"
		} else {
			rse = "${request.method} ${request.path}"
		}
	}

	// Apply AWS-realistic default APIKeySelectionExpression when not provided.
	keySelExpr := input.APIKeySelectionExpression
	if keySelExpr == "" {
		if input.ProtocolType == protocolTypeWebSocket {
			keySelExpr = "$context.authorizer.usageIdentifierKey"
		} else {
			keySelExpr = "$request.header.x-api-key"
		}
	}

	id := randomID()
	api := API{
		APIID:                     id,
		Name:                      input.Name,
		Description:               input.Description,
		ProtocolType:              input.ProtocolType,
		RouteSelectionExpression:  rse,
		Version:                   input.Version,
		Tags:                      copyTags(input.Tags),
		APIEndpoint:               "https://" + id + ".execute-api." + regionFromCtx(ctx) + ".amazonaws.com",
		CreatedDate:               isoTime{time.Now()},
		APIKeySelectionExpression: keySelExpr,
		DisableSchemaValidation:   input.DisableSchemaValidation,
		DisableExecuteAPIEndpoint: input.DisableExecuteAPIEndpoint,
	}

	if input.CorsConfiguration != nil {
		clone := *input.CorsConfiguration
		api.CorsConfiguration = &clone
	}

	b.apis.Put(&api)

	if input.RouteKey != "" && input.Target != "" {
		if err := b.quickCreateLocked(id, input.RouteKey, input.Target); err != nil {
			return nil, err
		}
	}

	cp := api

	return &cp, nil
}

// validateQuickCreateInput enforces CreateApi's routeKey/target ("quick
// create") rules: both fields are supported only for HTTP APIs, AWS requires
// both or neither, and a provided routeKey must be a valid HTTP route key.
func validateQuickCreateInput(input CreateAPIInput) error {
	if input.RouteKey == "" && input.Target == "" {
		return nil
	}

	if input.ProtocolType != protocolTypeHTTP {
		return fmt.Errorf("%w: routeKey and target are supported only for HTTP APIs", ErrBadRequest)
	}

	if input.RouteKey == "" || input.Target == "" {
		return fmt.Errorf("%w: routeKey and target must both be specified", ErrBadRequest)
	}

	return validateHTTPRouteKey(input.RouteKey)
}

// quickCreateLocked implements CreateApi's routeKey+target shortcut: it
// auto-provisions the integration, $default route, and auto-deployed
// $default stage that real AWS creates for a "quick create" HTTP API,
// mirroring the resources a caller would otherwise create by hand via
// CreateIntegration/CreateRoute/CreateStage. All three are marked
// apiGatewayManaged, matching AWS (the $default route key and $default stage
// become immutable; the integration stays updatable but not deletable while
// the API exists). Callers must already hold b.mu and have inserted apiID
// into b.apis.
func (b *InMemoryBackend) quickCreateLocked(apiID, routeKey, target string) error {
	integrationType := integrationTypeHTTPProxy
	if isLambdaFunctionARN(target) {
		integrationType = IntegrationTypeAWSProxy
	}

	integration, err := buildIntegration(apiID, protocolTypeHTTP, CreateIntegrationInput{
		IntegrationType:   integrationType,
		IntegrationURI:    target,
		IntegrationMethod: httpMethodAny,
	})
	if err != nil {
		return err
	}

	integration.APIGatewayManaged = true
	b.integrations.Put(integration)

	route := &Route{
		RouteID:             randomID(),
		APIID:               apiID,
		RouteKey:            routeKey,
		Target:              "integrations/" + integration.IntegrationID,
		AuthorizationType:   authorizationTypeNone,
		AuthorizationScopes: []string{},
		APIGatewayManaged:   true,
	}
	b.routes.Put(route)

	now := isoTime{time.Now()}
	b.stages.Put(&Stage{
		StageName:         routeKeyDefault,
		APIID:             apiID,
		AutoDeploy:        true,
		CreatedDate:       now,
		LastUpdatedDate:   now,
		APIGatewayManaged: true,
	})

	b.autoDeployLocked(apiID)

	return nil
}

// isLambdaFunctionARN reports whether target looks like a Lambda function
// ARN (arn:{partition}:lambda:...:function:...). CreateApi's quick-create
// shortcut uses this to pick AWS_PROXY vs HTTP_PROXY for the auto-created
// integration: per AWS, a Lambda ARN target yields AWS_PROXY, any other
// (URL) target yields HTTP_PROXY.
func isLambdaFunctionARN(target string) bool {
	return strings.HasPrefix(target, "arn:") &&
		strings.Contains(target, ":lambda:") &&
		strings.Contains(target, ":function:")
}

// GetAPI retrieves an API by ID.
func (b *InMemoryBackend) GetAPI(apiID string) (*API, error) {
	b.mu.RLock("GetAPI")
	defer b.mu.RUnlock()

	api, ok := b.apis.Get(apiID)
	if !ok {
		return nil, ErrAPINotFound
	}

	cp := *api

	return &cp, nil
}

// GetAPIs retrieves all APIs.
func (b *InMemoryBackend) GetAPIs() ([]API, error) {
	b.mu.RLock("GetAPIs")
	defer b.mu.RUnlock()

	all := b.apis.All()
	result := make([]API, 0, len(all))

	for _, api := range all {
		result = append(result, *api)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].APIID < result[j].APIID
	})

	return result, nil
}

// deleteAPIChildrenLocked removes every child resource (stages, routes,
// integrations, deployments, authorizers, models, and their nested
// responses) belonging to apiID. It mirrors the implicit cascade that
// deleting the pre-Phase-3.3 nested apiData map provided. Callers must
// already hold b.mu.Lock.
func (b *InMemoryBackend) deleteAPIChildrenLocked(apiID string) {
	for _, s := range slices.Clone(b.stagesByAPI.Get(apiID)) {
		b.stages.Delete(stageKey(apiID, s.StageName))
	}

	for _, r := range slices.Clone(b.routesByAPI.Get(apiID)) {
		for _, rr := range slices.Clone(b.routeResponsesByRoute.Get(routeKey(apiID, r.RouteID))) {
			b.routeResponses.Delete(routeResponseKey(apiID, r.RouteID, rr.RouteResponseID))
		}

		b.routes.Delete(routeKey(apiID, r.RouteID))
	}

	for _, i := range slices.Clone(b.integrationsByAPI.Get(apiID)) {
		for _, ir := range slices.Clone(b.integrationResponsesByIntegration.Get(integrationKey(apiID, i.IntegrationID))) {
			b.integrationResponses.Delete(integrationResponseKey(apiID, i.IntegrationID, ir.IntegrationResponseID))
		}

		b.integrations.Delete(integrationKey(apiID, i.IntegrationID))
	}

	for _, dep := range slices.Clone(b.deploymentsByAPI.Get(apiID)) {
		b.deployments.Delete(deploymentKey(apiID, dep.DeploymentID))
	}

	for _, a := range slices.Clone(b.authorizersByAPI.Get(apiID)) {
		b.authorizers.Delete(authorizerKey(apiID, a.AuthorizerID))
	}

	for _, m := range slices.Clone(b.modelsByAPI.Get(apiID)) {
		b.models.Delete(modelKey(apiID, m.ModelID))
	}
}

// DeleteAPI removes an API by ID.
func (b *InMemoryBackend) DeleteAPI(apiID string) error {
	b.mu.Lock("DeleteAPI")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	b.deleteAPIChildrenLocked(apiID)
	b.apis.Delete(apiID)

	// Clean up stale API mappings pointing to this API.
	for _, m := range b.apiMappings.All() {
		if m.APIID == apiID {
			b.apiMappings.Delete(apiMappingKey(m.DomainName, m.APIMappingID))
		}
	}

	return nil
}

// UpdateAPI updates fields on an existing API.
func (b *InMemoryBackend) UpdateAPI(apiID string, input UpdateAPIInput) (*API, error) {
	b.mu.Lock("UpdateAPI")
	defer b.mu.Unlock()

	api, ok := b.apis.Get(apiID)
	if !ok {
		return nil, ErrAPINotFound
	}

	if input.Name != "" {
		api.Name = input.Name
	}

	if input.Description != "" {
		api.Description = input.Description
	}

	if input.RouteSelectionExpression != "" {
		api.RouteSelectionExpression = input.RouteSelectionExpression
	}

	if input.Version != "" {
		api.Version = input.Version
	}

	if input.Tags != nil {
		api.Tags = copyTags(input.Tags)
	}

	if input.APIKeySelectionExpression != "" {
		api.APIKeySelectionExpression = input.APIKeySelectionExpression
	}

	if input.CorsConfiguration != nil {
		clone := *input.CorsConfiguration
		api.CorsConfiguration = &clone
	}

	if input.DisableSchemaValidation != nil {
		api.DisableSchemaValidation = *input.DisableSchemaValidation
	}

	if input.DisableExecuteAPIEndpoint != nil {
		api.DisableExecuteAPIEndpoint = *input.DisableExecuteAPIEndpoint
	}

	if err := b.applyQuickCreateUpdateLocked(apiID, input); err != nil {
		return nil, err
	}

	cp := *api

	return &cp, nil
}

// applyQuickCreateUpdateLocked applies UpdateApiInput's routeKey/target
// fields, which are "part of quick create": each independently replaces the
// route key / integration target+type of the API's existing quick-create
// route/integration (found via APIGatewayManaged), matching AWS ("you can
// update a quick-created target, but you can't remove it from an API").
// Callers must already hold b.mu.
func (b *InMemoryBackend) applyQuickCreateUpdateLocked(apiID string, input UpdateAPIInput) error {
	if input.RouteKey != "" {
		route := findManagedRoute(b.routesByAPI.Get(apiID))
		if route == nil {
			return fmt.Errorf("%w: API has no quick-create route to update", ErrBadRequest)
		}

		if err := validateHTTPRouteKey(input.RouteKey); err != nil {
			return err
		}

		for _, existing := range b.routesByAPI.Get(apiID) {
			if existing.RouteID != route.RouteID && existing.RouteKey == input.RouteKey {
				return fmt.Errorf("%w: route key %q already exists", ErrAlreadyExists, input.RouteKey)
			}
		}

		route.RouteKey = input.RouteKey
	}

	if input.Target != "" {
		integration := findManagedIntegration(b.integrationsByAPI.Get(apiID))
		if integration == nil {
			return fmt.Errorf("%w: API has no quick-create target to update", ErrBadRequest)
		}

		integration.IntegrationURI = input.Target
		integration.IntegrationType = integrationTypeHTTPProxy

		if isLambdaFunctionARN(input.Target) {
			integration.IntegrationType = IntegrationTypeAWSProxy
		}
	}

	return nil
}

// findManagedRoute returns the quick-create-provisioned route among routes,
// or nil if none is managed (the API wasn't quick-created).
func findManagedRoute(routes []*Route) *Route {
	for _, r := range routes {
		if r.APIGatewayManaged {
			return r
		}
	}

	return nil
}

// findManagedIntegration returns the quick-create-provisioned integration
// among integrations, or nil if none is managed (the API wasn't
// quick-created).
func findManagedIntegration(integrations []*Integration) *Integration {
	for _, i := range integrations {
		if i.APIGatewayManaged {
			return i
		}
	}

	return nil
}

// --- Stages ---

// CreateStage creates a new stage for an API.
func (b *InMemoryBackend) CreateStage(apiID string, input CreateStageInput) (*Stage, error) {
	b.mu.Lock("CreateStage")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if input.StageName == "" {
		return nil, fmt.Errorf("%w: stageName is required", ErrBadRequest)
	}

	if b.stages.Has(stageKey(apiID, input.StageName)) {
		return nil, fmt.Errorf("%w: stage %q already exists", ErrAlreadyExists, input.StageName)
	}

	now := isoTime{time.Now()}
	stage := &Stage{
		StageName:            input.StageName,
		APIID:                apiID,
		DeploymentID:         input.DeploymentID,
		Description:          input.Description,
		ClientCertificateID:  input.ClientCertificateID,
		AutoDeploy:           input.AutoDeploy,
		StageVariables:       input.StageVariables,
		CreatedDate:          now,
		LastUpdatedDate:      now,
		AccessLogSettings:    input.AccessLogSettings,
		DefaultRouteSettings: input.DefaultRouteSettings,
		RouteSettings:        input.RouteSettings,
	}

	b.stages.Put(stage)

	cp := *stage

	return &cp, nil
}

// GetStage retrieves a stage by name.
func (b *InMemoryBackend) GetStage(apiID, stageName string) (*Stage, error) {
	b.mu.RLock("GetStage")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	s, ok := b.stages.Get(stageKey(apiID, stageName))
	if !ok {
		return nil, ErrStageNotFound
	}

	cp := *s

	return &cp, nil
}

// GetStages retrieves all stages for an API.
func (b *InMemoryBackend) GetStages(apiID string) ([]Stage, error) {
	b.mu.RLock("GetStages")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	stages := b.stagesByAPI.Get(apiID)
	result := make([]Stage, 0, len(stages))

	for _, s := range stages {
		result = append(result, *s)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].StageName < result[j].StageName
	})

	return result, nil
}

// DeleteStage removes a stage from an API.
func (b *InMemoryBackend) DeleteStage(apiID, stageName string) error {
	b.mu.Lock("DeleteStage")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.stages.Delete(stageKey(apiID, stageName)) {
		return ErrStageNotFound
	}

	return nil
}

// UpdateStage updates fields on an existing stage.
func (b *InMemoryBackend) UpdateStage(apiID, stageName string, input UpdateStageInput) (*Stage, error) {
	b.mu.Lock("UpdateStage")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	s, ok := b.stages.Get(stageKey(apiID, stageName))
	if !ok {
		return nil, ErrStageNotFound
	}

	if input.DeploymentID != "" {
		s.DeploymentID = input.DeploymentID
	}

	if input.Description != "" {
		s.Description = input.Description
	}

	if input.ClientCertificateID != "" {
		s.ClientCertificateID = input.ClientCertificateID
	}

	if input.AutoDeploy != nil {
		s.AutoDeploy = *input.AutoDeploy
	}

	if input.StageVariables != nil {
		s.StageVariables = input.StageVariables
	}

	if input.AccessLogSettings != nil {
		clone := *input.AccessLogSettings
		s.AccessLogSettings = &clone
	}

	if input.DefaultRouteSettings != nil {
		clone := *input.DefaultRouteSettings
		s.DefaultRouteSettings = &clone
	}

	if input.RouteSettings != nil {
		s.RouteSettings = input.RouteSettings
	}

	s.LastUpdatedDate = isoTime{time.Now()}

	cp := *s

	return &cp, nil
}

// --- Routes ---

// CreateRoute creates a new route for an API.
func (b *InMemoryBackend) CreateRoute(apiID string, input CreateRouteInput) (*Route, error) {
	b.mu.Lock("CreateRoute")
	defer b.mu.Unlock()

	api, ok := b.apis.Get(apiID)
	if !ok {
		return nil, ErrAPINotFound
	}

	if input.RouteKey == "" {
		return nil, fmt.Errorf("%w: routeKey is required", ErrBadRequest)
	}

	if api.ProtocolType == protocolTypeHTTP {
		if err := validateHTTPRouteKey(input.RouteKey); err != nil {
			return nil, err
		}
	}

	for _, existing := range b.routesByAPI.Get(apiID) {
		if existing.RouteKey == input.RouteKey {
			return nil, fmt.Errorf("%w: route key %q already exists", ErrAlreadyExists, input.RouteKey)
		}
	}

	authType := input.AuthorizationType
	if authType == "" {
		authType = authorizationTypeNone
	}

	if !validRouteAuthorizationType(authType) {
		return nil, fmt.Errorf(
			"%w: authorizationType must be one of NONE, AWS_IAM, JWT, CUSTOM", ErrBadRequest,
		)
	}

	if (authType == authorizerTypeJWT || authType == authorizationTypeCustom) && input.AuthorizerID == "" {
		return nil, fmt.Errorf("%w: authorizerId is required for %s authorization", ErrBadRequest, authType)
	}

	authScopes := input.AuthorizationScopes
	if authScopes == nil {
		authScopes = []string{}
	}

	id := randomID()
	route := &Route{
		RouteID:                  id,
		APIID:                    apiID,
		RouteKey:                 input.RouteKey,
		Target:                   input.Target,
		AuthorizationType:        authType,
		AuthorizerID:             input.AuthorizerID,
		OperationName:            input.OperationName,
		ModelSelectionExpression: input.ModelSelectionExpression,
		RequestModels:            input.RequestModels,
		RequestParameters:        input.RequestParameters,
		AuthorizationScopes:      authScopes,
		APIKeyRequired:           input.APIKeyRequired,
	}

	b.routes.Put(route)
	b.autoDeployLocked(apiID)

	cp := *route

	return &cp, nil
}

// GetRoute retrieves a route by ID.
func (b *InMemoryBackend) GetRoute(apiID, routeID string) (*Route, error) {
	b.mu.RLock("GetRoute")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	r, ok := b.routes.Get(routeKey(apiID, routeID))
	if !ok {
		return nil, ErrRouteNotFound
	}

	cp := *r

	return &cp, nil
}

// GetRoutes retrieves all routes for an API.
func (b *InMemoryBackend) GetRoutes(apiID string) ([]Route, error) {
	b.mu.RLock("GetRoutes")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	routes := b.routesByAPI.Get(apiID)
	result := make([]Route, 0, len(routes))

	for _, r := range routes {
		result = append(result, *r)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RouteID < result[j].RouteID
	})

	return result, nil
}

// DeleteRoute removes a route from an API.
func (b *InMemoryBackend) DeleteRoute(apiID, routeID string) error {
	b.mu.Lock("DeleteRoute")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.routes.Delete(routeKey(apiID, routeID)) {
		return ErrRouteNotFound
	}

	for _, rr := range slices.Clone(b.routeResponsesByRoute.Get(routeKey(apiID, routeID))) {
		b.routeResponses.Delete(routeResponseKey(apiID, routeID, rr.RouteResponseID))
	}

	b.autoDeployLocked(apiID)

	return nil
}

// setRouteKey validates newKey for protocolType and ensures it is not a duplicate
// among routes (excluding the route being updated), then sets r.RouteKey.
func setRouteKey(r *Route, routes []*Route, routeID, newKey, protocolType string) error {
	if protocolType == protocolTypeHTTP {
		if err := validateHTTPRouteKey(newKey); err != nil {
			return err
		}
	}

	for _, existing := range routes {
		if existing.RouteID != routeID && existing.RouteKey == newKey {
			return fmt.Errorf("%w: route key %q already exists", ErrAlreadyExists, newKey)
		}
	}

	r.RouteKey = newKey

	return nil
}

// applyRouteAuthUpdate applies AuthorizationType/AuthorizerID changes from a
// route update, validating the authorization type against the AWS enum.
func applyRouteAuthUpdate(r *Route, input UpdateRouteInput) error {
	if input.AuthorizationType != "" {
		if !validRouteAuthorizationType(input.AuthorizationType) {
			return fmt.Errorf(
				"%w: authorizationType must be one of NONE, AWS_IAM, JWT, CUSTOM", ErrBadRequest,
			)
		}

		r.AuthorizationType = input.AuthorizationType
		if input.AuthorizationType == authorizationTypeNone {
			r.AuthorizerID = ""
		}
	}

	if input.AuthorizerID != "" {
		r.AuthorizerID = input.AuthorizerID
	}

	return nil
}

// UpdateRoute updates fields on an existing route.
func (b *InMemoryBackend) UpdateRoute(apiID, routeID string, input UpdateRouteInput) (*Route, error) {
	b.mu.Lock("UpdateRoute")
	defer b.mu.Unlock()

	api, ok := b.apis.Get(apiID)
	if !ok {
		return nil, ErrAPINotFound
	}

	r, ok := b.routes.Get(routeKey(apiID, routeID))
	if !ok {
		return nil, ErrRouteNotFound
	}

	if input.RouteKey != "" {
		if err := setRouteKey(r, b.routesByAPI.Get(apiID), routeID, input.RouteKey, api.ProtocolType); err != nil {
			return nil, err
		}
	}

	if input.Target != "" {
		r.Target = input.Target
	}

	if err := applyRouteAuthUpdate(r, input); err != nil {
		return nil, err
	}

	if input.OperationName != "" {
		r.OperationName = input.OperationName
	}

	if input.ModelSelectionExpression != "" {
		r.ModelSelectionExpression = input.ModelSelectionExpression
	}

	if input.RequestModels != nil {
		r.RequestModels = input.RequestModels
	}

	if input.RequestParameters != nil {
		r.RequestParameters = input.RequestParameters
	}

	if input.AuthorizationScopes != nil {
		r.AuthorizationScopes = input.AuthorizationScopes
	}

	if input.APIKeyRequired != nil {
		r.APIKeyRequired = *input.APIKeyRequired
	}

	b.autoDeployLocked(apiID)

	cp := *r

	return &cp, nil
}

// --- Integrations ---

// CreateIntegration creates a new integration for an API.
func (b *InMemoryBackend) CreateIntegration(apiID string, input CreateIntegrationInput) (*Integration, error) {
	b.mu.Lock("CreateIntegration")
	defer b.mu.Unlock()

	api, ok := b.apis.Get(apiID)
	if !ok {
		return nil, ErrAPINotFound
	}

	integration, err := buildIntegration(apiID, api.ProtocolType, input)
	if err != nil {
		return nil, err
	}

	b.integrations.Put(integration)
	b.autoDeployLocked(apiID)

	cp := *integration

	return &cp, nil
}

// buildIntegration validates input and constructs a new Integration for apiID
// (of the given protocolType), applying the same AWS-realistic defaults
// CreateIntegration always has. It does not touch backend state -- callers
// (CreateIntegration, and CreateAPI's quick-create shortcut) store it and
// hold b.mu themselves.
func buildIntegration(apiID, protocolType string, input CreateIntegrationInput) (*Integration, error) {
	validTypes := map[string]bool{
		"AWS":                    true,
		integrationTypeHTTP:      true,
		"MOCK":                   true,
		IntegrationTypeAWSProxy:  true,
		integrationTypeHTTPProxy: true,
	}
	if !validTypes[input.IntegrationType] {
		return nil, fmt.Errorf(
			"%w: integrationType must be one of AWS, HTTP, MOCK, AWS_PROXY, HTTP_PROXY",
			ErrBadRequest,
		)
	}

	// Apply AWS-realistic defaults.
	payloadFmtVer := input.PayloadFormatVersion
	if payloadFmtVer == "" && input.IntegrationType == IntegrationTypeAWSProxy {
		payloadFmtVer = "1.0"
	}

	passthroughBehavior := input.PassthroughBehavior
	if passthroughBehavior == "" && input.IntegrationType == integrationTypeHTTPProxy {
		passthroughBehavior = "WHEN_NO_MATCH"
	}

	connectionType := input.ConnectionType
	if connectionType == "" {
		connectionType = connectionTypeInternet
	}

	if err := validateConnectionType(connectionType, input.ConnectionID); err != nil {
		return nil, err
	}

	timeoutMs := input.TimeoutInMillis
	if timeoutMs == 0 {
		timeoutMs = integrationTimeoutMaxFor(protocolType)
	} else if err := validateTimeoutInMillis(timeoutMs, protocolType); err != nil {
		return nil, err
	}

	return &Integration{
		IntegrationID:               randomID(),
		APIID:                       apiID,
		IntegrationType:             input.IntegrationType,
		IntegrationSubtype:          input.IntegrationSubtype,
		IntegrationMethod:           input.IntegrationMethod,
		IntegrationURI:              input.IntegrationURI,
		Description:                 input.Description,
		PayloadFormatVersion:        payloadFmtVer,
		ConnectionType:              connectionType,
		ConnectionID:                input.ConnectionID,
		TimeoutInMillis:             timeoutMs,
		RequestParameters:           input.RequestParameters,
		RequestTemplates:            input.RequestTemplates,
		TemplateSelectionExpression: input.TemplateSelectionExpression,
		PassthroughBehavior:         passthroughBehavior,
		TLSConfig:                   cloneIntegrationTLSConfig(input.TLSConfig),
	}, nil
}

// GetIntegration retrieves an integration by ID.
func (b *InMemoryBackend) GetIntegration(apiID, integrationID string) (*Integration, error) {
	b.mu.RLock("GetIntegration")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	i, ok := b.integrations.Get(integrationKey(apiID, integrationID))
	if !ok {
		return nil, ErrIntegrationNotFound
	}

	cp := *i

	return &cp, nil
}

// GetIntegrations retrieves all integrations for an API.
func (b *InMemoryBackend) GetIntegrations(apiID string) ([]Integration, error) {
	b.mu.RLock("GetIntegrations")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	integrations := b.integrationsByAPI.Get(apiID)
	result := make([]Integration, 0, len(integrations))

	for _, i := range integrations {
		result = append(result, *i)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].IntegrationID < result[j].IntegrationID
	})

	return result, nil
}

// DeleteIntegration removes an integration from an API.
func (b *InMemoryBackend) DeleteIntegration(apiID, integrationID string) error {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.integrations.Delete(integrationKey(apiID, integrationID)) {
		return ErrIntegrationNotFound
	}

	for _, ir := range slices.Clone(b.integrationResponsesByIntegration.Get(integrationKey(apiID, integrationID))) {
		b.integrationResponses.Delete(integrationResponseKey(apiID, integrationID, ir.IntegrationResponseID))
	}

	b.autoDeployLocked(apiID)

	return nil
}

// UpdateIntegration updates fields on an existing integration.
// applyIntegrationUpdate copies non-zero fields from input onto the integration.
func applyIntegrationUpdate(i *Integration, input UpdateIntegrationInput) {
	if input.IntegrationType != "" {
		i.IntegrationType = input.IntegrationType
	}

	if input.IntegrationSubtype != "" {
		i.IntegrationSubtype = input.IntegrationSubtype
	}

	if input.IntegrationMethod != "" {
		i.IntegrationMethod = input.IntegrationMethod
	}

	if input.IntegrationURI != "" {
		i.IntegrationURI = input.IntegrationURI
	}

	if input.Description != "" {
		i.Description = input.Description
	}

	if input.PayloadFormatVersion != "" {
		i.PayloadFormatVersion = input.PayloadFormatVersion
	}

	if input.ConnectionType != "" {
		i.ConnectionType = input.ConnectionType
	}

	if input.ConnectionID != "" {
		i.ConnectionID = input.ConnectionID
	}

	if input.TimeoutInMillis != 0 {
		i.TimeoutInMillis = input.TimeoutInMillis
	}

	if input.RequestParameters != nil {
		i.RequestParameters = input.RequestParameters
	}

	if input.RequestTemplates != nil {
		i.RequestTemplates = input.RequestTemplates
	}

	if input.TemplateSelectionExpression != "" {
		i.TemplateSelectionExpression = input.TemplateSelectionExpression
	}

	if input.PassthroughBehavior != "" {
		i.PassthroughBehavior = input.PassthroughBehavior
	}

	if input.TLSConfig != nil {
		i.TLSConfig = cloneIntegrationTLSConfig(input.TLSConfig)
	}
}

// UpdateIntegration updates fields on an existing integration.
func (b *InMemoryBackend) UpdateIntegration(
	apiID, integrationID string,
	input UpdateIntegrationInput,
) (*Integration, error) {
	b.mu.Lock("UpdateIntegration")
	defer b.mu.Unlock()

	api, ok := b.apis.Get(apiID)
	if !ok {
		return nil, ErrAPINotFound
	}

	i, ok := b.integrations.Get(integrationKey(apiID, integrationID))
	if !ok {
		return nil, ErrIntegrationNotFound
	}

	if input.TimeoutInMillis != 0 {
		if err := validateTimeoutInMillis(input.TimeoutInMillis, api.ProtocolType); err != nil {
			return nil, err
		}
	}

	effectiveConnectionType := i.ConnectionType
	if input.ConnectionType != "" {
		effectiveConnectionType = input.ConnectionType
	}

	effectiveConnectionID := i.ConnectionID
	if input.ConnectionID != "" {
		effectiveConnectionID = input.ConnectionID
	}

	if input.ConnectionType != "" || input.ConnectionID != "" {
		if err := validateConnectionType(effectiveConnectionType, effectiveConnectionID); err != nil {
			return nil, err
		}
	}

	applyIntegrationUpdate(i, input)
	b.autoDeployLocked(apiID)

	cp := *i

	return &cp, nil
}

// --- Deployments ---

// CreateDeployment creates a new deployment for an API.
func (b *InMemoryBackend) CreateDeployment(apiID string, input CreateDeploymentInput) (*Deployment, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	id := randomID()
	deployment := &Deployment{
		DeploymentID:     id,
		APIID:            apiID,
		Description:      input.Description,
		DeploymentStatus: "DEPLOYED",
		CreatedDate:      isoTime{time.Now()},
	}

	b.deployments.Put(deployment)

	// When a stage name is provided, link the deployment to that stage (AWS behaviour).
	if input.StageName != "" {
		s, stageExists := b.stages.Get(stageKey(apiID, input.StageName))
		if !stageExists {
			return nil, ErrStageNotFound
		}

		s.DeploymentID = id
	}

	cp := *deployment

	return &cp, nil
}

// autoDeployLocked triggers an automatic deployment for every stage of the API
// that has AutoDeploy enabled. Real API Gateway v2 creates a fresh deployment
// and repoints the stage at it whenever a route, integration, or other routing
// configuration changes on an auto-deploy-enabled stage. The caller must hold
// b.mu.Lock.
func (b *InMemoryBackend) autoDeployLocked(apiID string) {
	now := isoTime{time.Now()}

	for _, s := range b.stagesByAPI.Get(apiID) {
		if !s.AutoDeploy {
			continue
		}

		id := randomID()
		b.deployments.Put(&Deployment{
			DeploymentID:     id,
			APIID:            apiID,
			Description:      "Automatic deployment triggered by changes to the Api configuration",
			DeploymentStatus: "DEPLOYED",
			AutoDeployed:     true,
			CreatedDate:      now,
		})
		s.DeploymentID = id
		s.LastUpdatedDate = now
	}
}

// GetDeployment retrieves a deployment by ID.
func (b *InMemoryBackend) GetDeployment(apiID, deploymentID string) (*Deployment, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	dep, ok := b.deployments.Get(deploymentKey(apiID, deploymentID))
	if !ok {
		return nil, ErrDeploymentNotFound
	}

	cp := *dep

	return &cp, nil
}

// GetDeployments retrieves all deployments for an API.
func (b *InMemoryBackend) GetDeployments(apiID string) ([]Deployment, error) {
	b.mu.RLock("GetDeployments")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	deployments := b.deploymentsByAPI.Get(apiID)
	result := make([]Deployment, 0, len(deployments))

	for _, dep := range deployments {
		result = append(result, *dep)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].DeploymentID < result[j].DeploymentID
	})

	return result, nil
}

// DeleteDeployment removes a deployment from an API.
func (b *InMemoryBackend) DeleteDeployment(apiID, deploymentID string) error {
	b.mu.Lock("DeleteDeployment")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.deployments.Delete(deploymentKey(apiID, deploymentID)) {
		return ErrDeploymentNotFound
	}

	return nil
}

// --- Authorizers ---

// CreateAuthorizer creates a new authorizer for an API.
func (b *InMemoryBackend) CreateAuthorizer(apiID string, input CreateAuthorizerInput) (*Authorizer, error) {
	b.mu.Lock("CreateAuthorizer")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrBadRequest)
	}

	validTypes := map[string]bool{authorizerTypeJWT: true, "REQUEST": true, "CUSTOM": true}
	if !validTypes[input.AuthorizerType] {
		return nil, fmt.Errorf("%w: authorizerType must be JWT, REQUEST, or CUSTOM", ErrBadRequest)
	}

	// Validate JWT authorizer requires JwtConfiguration.
	if input.AuthorizerType == authorizerTypeJWT && input.JwtConfiguration == nil {
		return nil, fmt.Errorf("%w: jwtConfiguration is required for JWT authorizers", ErrBadRequest)
	}

	// Apply AWS-realistic defaults for IdentitySource.
	identitySource := input.IdentitySource
	if len(identitySource) == 0 && input.AuthorizerType == authorizerTypeJWT {
		identitySource = []string{"$request.header.Authorization"}
	}

	id := randomID()
	authorizer := &Authorizer{
		AuthorizerID:                   id,
		APIID:                          apiID,
		Name:                           input.Name,
		AuthorizerType:                 input.AuthorizerType,
		AuthorizerURI:                  input.AuthorizerURI,
		IdentitySource:                 identitySource,
		AuthorizerCredentialsArn:       input.AuthorizerCredentialsArn,
		AuthorizerResultTTLInSeconds:   input.AuthorizerResultTTLInSeconds,
		AuthorizerPayloadFormatVersion: input.AuthorizerPayloadFormatVersion,
		EnableSimpleResponses:          input.EnableSimpleResponses,
	}

	if input.JwtConfiguration != nil {
		clone := *input.JwtConfiguration
		authorizer.JwtConfiguration = &clone
	}

	b.authorizers.Put(authorizer)

	cp := *authorizer

	return &cp, nil
}

// GetAuthorizer retrieves an authorizer by ID.
func (b *InMemoryBackend) GetAuthorizer(apiID, authorizerID string) (*Authorizer, error) {
	b.mu.RLock("GetAuthorizer")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	a, ok := b.authorizers.Get(authorizerKey(apiID, authorizerID))
	if !ok {
		return nil, ErrAuthorizerNotFound
	}

	cp := *a

	return &cp, nil
}

// GetAuthorizers retrieves all authorizers for an API.
func (b *InMemoryBackend) GetAuthorizers(apiID string) ([]Authorizer, error) {
	b.mu.RLock("GetAuthorizers")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	authorizers := b.authorizersByAPI.Get(apiID)
	result := make([]Authorizer, 0, len(authorizers))

	for _, a := range authorizers {
		result = append(result, *a)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AuthorizerID < result[j].AuthorizerID
	})

	return result, nil
}

// DeleteAuthorizer removes an authorizer from an API.
func (b *InMemoryBackend) DeleteAuthorizer(apiID, authorizerID string) error {
	b.mu.Lock("DeleteAuthorizer")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.authorizers.Delete(authorizerKey(apiID, authorizerID)) {
		return ErrAuthorizerNotFound
	}

	return nil
}

// UpdateAuthorizer updates fields on an existing authorizer.
func (b *InMemoryBackend) UpdateAuthorizer(
	apiID, authorizerID string,
	input UpdateAuthorizerInput,
) (*Authorizer, error) {
	b.mu.Lock("UpdateAuthorizer")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	a, ok := b.authorizers.Get(authorizerKey(apiID, authorizerID))
	if !ok {
		return nil, ErrAuthorizerNotFound
	}

	if input.Name != "" {
		a.Name = input.Name
	}

	if input.AuthorizerType != "" {
		a.AuthorizerType = input.AuthorizerType
	}

	if input.AuthorizerURI != "" {
		a.AuthorizerURI = input.AuthorizerURI
	}

	if len(input.IdentitySource) > 0 {
		a.IdentitySource = input.IdentitySource
	}

	if input.AuthorizerCredentialsArn != "" {
		a.AuthorizerCredentialsArn = input.AuthorizerCredentialsArn
	}

	if input.AuthorizerResultTTLInSeconds != 0 {
		a.AuthorizerResultTTLInSeconds = input.AuthorizerResultTTLInSeconds
	}

	if input.AuthorizerPayloadFormatVersion != "" {
		a.AuthorizerPayloadFormatVersion = input.AuthorizerPayloadFormatVersion
	}

	if input.EnableSimpleResponses {
		a.EnableSimpleResponses = input.EnableSimpleResponses
	}

	if input.JwtConfiguration != nil {
		clone := *input.JwtConfiguration
		a.JwtConfiguration = &clone
	}

	cp := *a

	return &cp, nil
}

// --- Domain Names ---

// CreateDomainName creates a new custom domain name.
func (b *InMemoryBackend) CreateDomainName(
	ctx context.Context,
	input CreateDomainNameInput,
) (*DomainName, error) {
	if input.DomainNameValue == "" {
		return nil, fmt.Errorf("%w: domainName is required", ErrBadRequest)
	}

	b.mu.Lock("CreateDomainName")
	defer b.mu.Unlock()

	if b.domainNames.Has(input.DomainNameValue) {
		return nil, fmt.Errorf("%w: domain name %q already exists", ErrAlreadyExists, input.DomainNameValue)
	}

	domainNameConfigs := []DomainNameConfiguration{}
	if len(input.DomainNameConfigurations) > 0 {
		domainNameConfigs = applyDomainNameDefaults(
			input.DomainNameConfigurations, input.DomainNameValue, regionFromCtx(ctx))
	}

	domainNameArn := "arn:aws:apigateway:" + regionFromCtx(ctx) + "::/domainnames/" + input.DomainNameValue

	dn := &DomainName{
		DomainNameValue:          input.DomainNameValue,
		DomainNameArn:            domainNameArn,
		Tags:                     copyTags(input.Tags),
		DomainNameConfigurations: domainNameConfigs,
		MutualTLSAuthentication:  cloneMutualTLSAuthentication(input.MutualTLSAuthentication),
	}

	b.domainNames.Put(dn)

	cp := *dn

	return &cp, nil
}

// --- API Mappings ---

// CreateAPIMapping creates a new API mapping for a custom domain name.
func (b *InMemoryBackend) CreateAPIMapping(domainName string, input CreateAPIMappingInput) (*APIMapping, error) {
	if input.APIID == "" {
		return nil, fmt.Errorf("%w: apiId is required", ErrBadRequest)
	}

	if input.Stage == "" {
		return nil, fmt.Errorf("%w: stage is required", ErrBadRequest)
	}

	b.mu.Lock("CreateAPIMapping")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	if !b.apis.Has(input.APIID) {
		return nil, ErrAPINotFound
	}

	if !b.stages.Has(stageKey(input.APIID, input.Stage)) {
		return nil, ErrStageNotFound
	}

	// AWS allows only one mapping per (domain, apiMappingKey). The empty key is
	// the domain's default (base-path) mapping and is itself unique. Reject a
	// duplicate key with a ConflictException, matching real API Gateway v2.
	for _, existing := range b.apiMappingsByDomain.Get(domainName) {
		if existing.APIMappingKey == input.APIMappingKey {
			return nil, fmt.Errorf(
				"%w: an api mapping already exists for the mapping key %q on domain %q",
				ErrAlreadyExists, input.APIMappingKey, domainName,
			)
		}
	}

	id := randomID()
	mapping := &APIMapping{
		APIMappingID:  id,
		DomainName:    domainName,
		APIID:         input.APIID,
		Stage:         input.Stage,
		APIMappingKey: input.APIMappingKey,
	}

	b.apiMappings.Put(mapping)

	cp := *mapping

	return &cp, nil
}

// --- Integration Responses ---

// CreateIntegrationResponse creates a new integration response.
func (b *InMemoryBackend) CreateIntegrationResponse(
	apiID, integrationID string,
	input CreateIntegrationResponseInput,
) (*IntegrationResponse, error) {
	if input.IntegrationResponseKey == "" {
		return nil, fmt.Errorf("%w: integrationResponseKey is required", ErrBadRequest)
	}

	b.mu.Lock("CreateIntegrationResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.integrations.Has(integrationKey(apiID, integrationID)) {
		return nil, ErrIntegrationNotFound
	}

	for _, existing := range b.integrationResponsesByIntegration.Get(integrationKey(apiID, integrationID)) {
		if existing.IntegrationResponseKey == input.IntegrationResponseKey {
			return nil, fmt.Errorf(
				"%w: integration response key %q already exists",
				ErrAlreadyExists,
				input.IntegrationResponseKey,
			)
		}
	}

	id := randomID()
	ir := &IntegrationResponse{
		IntegrationResponseID:       id,
		IntegrationResponseKey:      input.IntegrationResponseKey,
		APIID:                       apiID,
		IntegrationID:               integrationID,
		ContentHandlingStrategy:     input.ContentHandlingStrategy,
		TemplateSelectionExpression: input.TemplateSelectionExpression,
		ResponseParameters:          input.ResponseParameters,
		ResponseTemplates:           input.ResponseTemplates,
	}

	b.integrationResponses.Put(ir)

	cp := *ir

	return &cp, nil
}

// --- Models ---

// CreateModel creates a new model for an API.
func (b *InMemoryBackend) CreateModel(apiID string, input CreateModelInput) (*Model, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrBadRequest)
	}

	b.mu.Lock("CreateModel")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	for _, existing := range b.modelsByAPI.Get(apiID) {
		if existing.Name == input.Name {
			return nil, fmt.Errorf("%w: model name %q already exists", ErrAlreadyExists, input.Name)
		}
	}

	id := randomID()
	model := &Model{
		ModelID:     id,
		APIID:       apiID,
		Name:        input.Name,
		Schema:      input.Schema,
		ContentType: input.ContentType,
		Description: input.Description,
	}

	b.models.Put(model)

	cp := *model

	return &cp, nil
}

// --- Route Responses ---

// CreateRouteResponse creates a new route response.
func (b *InMemoryBackend) CreateRouteResponse(
	apiID, routeID string,
	input CreateRouteResponseInput,
) (*RouteResponse, error) {
	if input.RouteResponseKey == "" {
		return nil, fmt.Errorf("%w: routeResponseKey is required", ErrBadRequest)
	}

	b.mu.Lock("CreateRouteResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.routes.Has(routeKey(apiID, routeID)) {
		return nil, ErrRouteNotFound
	}

	for _, existing := range b.routeResponsesByRoute.Get(routeKey(apiID, routeID)) {
		if existing.RouteResponseKey == input.RouteResponseKey {
			return nil, fmt.Errorf("%w: route response key %q already exists", ErrAlreadyExists, input.RouteResponseKey)
		}
	}

	id := randomID()
	rr := &RouteResponse{
		RouteResponseID:          id,
		RouteResponseKey:         input.RouteResponseKey,
		APIID:                    apiID,
		RouteID:                  routeID,
		ModelSelectionExpression: input.ModelSelectionExpression,
		ResponseModels:           input.ResponseModels,
	}

	b.routeResponses.Put(rr)

	cp := *rr

	return &cp, nil
}

// --- Portals ---

// CreatePortal creates a new portal.
func (b *InMemoryBackend) CreatePortal(input CreatePortalInput) (*Portal, error) {
	b.mu.Lock("CreatePortal")
	defer b.mu.Unlock()

	id := randomID()
	portal := &Portal{
		PortalID: id,
		LogoURI:  input.LogoURI,
		Tags:     copyTags(input.Tags),
		Status:   "ACTIVE",
	}

	b.portals.Put(portal)

	cp := *portal

	return &cp, nil
}

// --- Portal Products ---

// CreatePortalProduct creates a new portal product.
func (b *InMemoryBackend) CreatePortalProduct(input CreatePortalProductInput) (*PortalProduct, error) {
	if input.DisplayName == "" {
		return nil, fmt.Errorf("%w: displayName is required", ErrBadRequest)
	}

	b.mu.Lock("CreatePortalProduct")
	defer b.mu.Unlock()

	id := randomID()
	product := &PortalProduct{
		PortalProductID: id,
		DisplayName:     input.DisplayName,
		Description:     input.Description,
		Tags:            copyTags(input.Tags),
	}

	b.portalProducts.Put(product)

	cp := *product

	return &cp, nil
}

// --- Product Pages ---

// CreateProductPage creates a new product page for a portal product.
func (b *InMemoryBackend) CreateProductPage(
	portalProductID string,
	_ CreateProductPageInput,
) (*ProductPage, error) {
	b.mu.Lock("CreateProductPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	now := isoTime{time.Now()}
	id := randomID()
	page := &ProductPage{
		ProductPageID:   id,
		PortalProductID: portalProductID,
		LastModified:    &now,
	}

	b.productPages.Put(page)

	cp := *page

	return &cp, nil
}

// --- Product REST Endpoint Pages ---

// CreateProductRestEndpointPage creates a new product REST endpoint page for a portal product.
func (b *InMemoryBackend) CreateProductRestEndpointPage(
	portalProductID string,
	_ CreateProductRestEndpointPageInput,
) (*ProductRestEndpointPage, error) {
	b.mu.Lock("CreateProductRestEndpointPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	now := isoTime{time.Now()}
	id := randomID()
	page := &ProductRestEndpointPage{
		ProductRestEndpointPageID: id,
		PortalProductID:           portalProductID,
		LastModified:              &now,
	}

	b.productREPages.Put(page)

	cp := *page

	return &cp, nil
}

// --- VPC Links ---

// CreateVpcLink creates a new VPC link.
func (b *InMemoryBackend) CreateVpcLink(input CreateVpcLinkInput) (*VpcLink, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrBadRequest)
	}

	if len(input.SubnetIDs) == 0 {
		return nil, fmt.Errorf("%w: subnetIds is required", ErrBadRequest)
	}

	b.mu.Lock("CreateVpcLink")
	defer b.mu.Unlock()

	securityGroupIDs := input.SecurityGroupIDs
	if securityGroupIDs == nil {
		securityGroupIDs = []string{}
	}

	now := isoTime{time.Now()}
	id := randomID()
	vpcLink := &VpcLink{
		CreatedDate:      now,
		VpcLinkID:        id,
		Name:             input.Name,
		SecurityGroupIDs: securityGroupIDs,
		SubnetIDs:        input.SubnetIDs,
		Tags:             copyTags(input.Tags),
		VpcLinkStatus:    "AVAILABLE",
	}
	b.vpcLinks.Put(vpcLink)

	cp := *vpcLink

	return &cp, nil
}

// GetVpcLink retrieves a VPC link by ID.
func (b *InMemoryBackend) GetVpcLink(vpcLinkID string) (*VpcLink, error) {
	b.mu.RLock("GetVpcLink")
	defer b.mu.RUnlock()

	vpcLink, ok := b.vpcLinks.Get(vpcLinkID)
	if !ok {
		return nil, ErrVpcLinkNotFound
	}

	cp := *vpcLink

	return &cp, nil
}

// GetVpcLinks retrieves all VPC links.
func (b *InMemoryBackend) GetVpcLinks() ([]VpcLink, error) {
	b.mu.RLock("GetVpcLinks")
	defer b.mu.RUnlock()

	all := b.vpcLinks.All()
	out := make([]VpcLink, 0, len(all))

	for _, item := range all {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VpcLinkID < out[j].VpcLinkID })

	return out, nil
}

// UpdateVpcLink updates a VPC link.
func (b *InMemoryBackend) UpdateVpcLink(vpcLinkID string, input UpdateVpcLinkInput) (*VpcLink, error) {
	b.mu.Lock("UpdateVpcLink")
	defer b.mu.Unlock()

	vpcLink, ok := b.vpcLinks.Get(vpcLinkID)
	if !ok {
		return nil, ErrVpcLinkNotFound
	}
	if input.Name != "" {
		vpcLink.Name = input.Name
	}

	cp := *vpcLink

	return &cp, nil
}

// DeleteVpcLink removes a VPC link.
func (b *InMemoryBackend) DeleteVpcLink(vpcLinkID string) error {
	b.mu.Lock("DeleteVpcLink")
	defer b.mu.Unlock()

	if !b.vpcLinks.Delete(vpcLinkID) {
		return ErrVpcLinkNotFound
	}

	return nil
}

// --- Routing Rules ---

// CreateRoutingRule creates a routing rule under a domain name.
func (b *InMemoryBackend) CreateRoutingRule(
	ctx context.Context,
	domainName string,
	input CreateRoutingRuleInput,
) (*RoutingRule, error) {
	b.mu.Lock("CreateRoutingRule")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	id := randomID()
	rule := &RoutingRule{
		RoutingRuleID: id,
		RoutingRuleARN: "arn:aws:apigateway:" + regionFromCtx(ctx) +
			"::/domainnames/" + domainName + "/routingrules/" + id,
		DomainName: domainName,
		Priority:   input.Priority,
		Actions:    input.Actions,
		Conditions: input.Conditions,
	}
	b.routingRules.Put(rule)

	cp := *rule

	return &cp, nil
}

// GetRoutingRule retrieves a routing rule.
func (b *InMemoryBackend) GetRoutingRule(domainName, routingRuleID string) (*RoutingRule, error) {
	b.mu.RLock("GetRoutingRule")
	defer b.mu.RUnlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	rule, ok := b.routingRules.Get(routingRuleKey(domainName, routingRuleID))
	if !ok {
		return nil, ErrRoutingRuleNotFound
	}

	cp := *rule

	return &cp, nil
}

// ListRoutingRules lists routing rules for a domain.
func (b *InMemoryBackend) ListRoutingRules(domainName string) ([]RoutingRule, error) {
	b.mu.RLock("ListRoutingRules")
	defer b.mu.RUnlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	rules := b.routingRulesByDomain.Get(domainName)
	out := make([]RoutingRule, 0, len(rules))

	for _, rule := range rules {
		out = append(out, *rule)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RoutingRuleID < out[j].RoutingRuleID })

	return out, nil
}

// PutRoutingRule updates an existing routing rule.
func (b *InMemoryBackend) PutRoutingRule(
	domainName, routingRuleID string,
	input PutRoutingRuleInput,
) (*RoutingRule, error) {
	b.mu.Lock("PutRoutingRule")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	rule, ok := b.routingRules.Get(routingRuleKey(domainName, routingRuleID))
	if !ok {
		return nil, ErrRoutingRuleNotFound
	}
	rule.Priority = input.Priority
	rule.Actions = input.Actions
	rule.Conditions = input.Conditions

	cp := *rule

	return &cp, nil
}

// DeleteRoutingRule deletes a routing rule.
func (b *InMemoryBackend) DeleteRoutingRule(domainName, routingRuleID string) error {
	b.mu.Lock("DeleteRoutingRule")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return ErrDomainNameNotFound
	}

	if !b.routingRules.Delete(routingRuleKey(domainName, routingRuleID)) {
		return ErrRoutingRuleNotFound
	}

	return nil
}

// --- Portal Product Sharing Policy ---

// GetPortalProductSharingPolicy gets sharing policy for a portal product.
func (b *InMemoryBackend) GetPortalProductSharingPolicy(portalProductID string) (*PortalProductSharingPolicy, error) {
	b.mu.RLock("GetPortalProductSharingPolicy")
	defer b.mu.RUnlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	return &PortalProductSharingPolicy{PolicyDocument: b.portalProductSharingPolicies[portalProductID]}, nil
}

// PutPortalProductSharingPolicy stores sharing policy for a portal product.
func (b *InMemoryBackend) PutPortalProductSharingPolicy(
	portalProductID, policyDocument string,
) (*PortalProductSharingPolicy, error) {
	b.mu.Lock("PutPortalProductSharingPolicy")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}
	b.portalProductSharingPolicies[portalProductID] = policyDocument

	return &PortalProductSharingPolicy{PolicyDocument: policyDocument}, nil
}

// DeletePortalProductSharingPolicy deletes sharing policy for a portal product.
func (b *InMemoryBackend) DeletePortalProductSharingPolicy(portalProductID string) error {
	b.mu.Lock("DeletePortalProductSharingPolicy")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return ErrPortalProductNotFound
	}
	delete(b.portalProductSharingPolicies, portalProductID)

	return nil
}

// --- Get/Delete Domain Names ---

// GetDomainName retrieves a domain name by name.
func (b *InMemoryBackend) GetDomainName(domainName string) (*DomainName, error) {
	b.mu.RLock("GetDomainName")
	defer b.mu.RUnlock()

	dn, ok := b.domainNames.Get(domainName)
	if !ok {
		return nil, ErrDomainNameNotFound
	}

	cp := *dn

	return &cp, nil
}

// GetDomainNames retrieves all custom domain names.
func (b *InMemoryBackend) GetDomainNames() ([]DomainName, error) {
	b.mu.RLock("GetDomainNames")
	defer b.mu.RUnlock()

	all := b.domainNames.All()
	result := make([]DomainName, 0, len(all))

	for _, dn := range all {
		result = append(result, *dn)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].DomainNameValue < result[j].DomainNameValue
	})

	return result, nil
}

// DeleteDomainName removes a custom domain name and all its API mappings.
func (b *InMemoryBackend) DeleteDomainName(domainName string) error {
	b.mu.Lock("DeleteDomainName")
	defer b.mu.Unlock()

	if !b.domainNames.Delete(domainName) {
		return ErrDomainNameNotFound
	}

	for _, m := range slices.Clone(b.apiMappingsByDomain.Get(domainName)) {
		b.apiMappings.Delete(apiMappingKey(domainName, m.APIMappingID))
	}

	for _, r := range slices.Clone(b.routingRulesByDomain.Get(domainName)) {
		b.routingRules.Delete(routingRuleKey(domainName, r.RoutingRuleID))
	}

	return nil
}

// --- Get/Delete API Mappings ---

// GetAPIMapping retrieves a specific API mapping.
func (b *InMemoryBackend) GetAPIMapping(domainName, mappingID string) (*APIMapping, error) {
	b.mu.RLock("GetAPIMapping")
	defer b.mu.RUnlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	m, ok := b.apiMappings.Get(apiMappingKey(domainName, mappingID))
	if !ok {
		return nil, ErrAPIMappingNotFound
	}

	cp := *m

	return &cp, nil
}

// GetAPIMappings retrieves all API mappings for a domain name.
func (b *InMemoryBackend) GetAPIMappings(domainName string) ([]APIMapping, error) {
	b.mu.RLock("GetAPIMappings")
	defer b.mu.RUnlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	mappings := b.apiMappingsByDomain.Get(domainName)
	result := make([]APIMapping, 0, len(mappings))

	for _, m := range mappings {
		result = append(result, *m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].APIMappingID < result[j].APIMappingID
	})

	return result, nil
}

// DeleteAPIMapping removes an API mapping from a domain name.
func (b *InMemoryBackend) DeleteAPIMapping(domainName, mappingID string) error {
	b.mu.Lock("DeleteAPIMapping")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return ErrDomainNameNotFound
	}

	if !b.apiMappings.Delete(apiMappingKey(domainName, mappingID)) {
		return ErrAPIMappingNotFound
	}

	return nil
}

// --- Get/Delete Integration Responses ---

// GetIntegrationResponse retrieves a specific integration response.
func (b *InMemoryBackend) GetIntegrationResponse(
	apiID, integrationID, responseID string,
) (*IntegrationResponse, error) {
	b.mu.RLock("GetIntegrationResponse")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.integrations.Has(integrationKey(apiID, integrationID)) {
		return nil, ErrIntegrationNotFound
	}

	ir, ok := b.integrationResponses.Get(integrationResponseKey(apiID, integrationID, responseID))
	if !ok {
		return nil, ErrIntegrationResponseNotFound
	}

	cp := *ir

	return &cp, nil
}

// GetIntegrationResponses retrieves all integration responses for an integration.
func (b *InMemoryBackend) GetIntegrationResponses(apiID, integrationID string) ([]IntegrationResponse, error) {
	b.mu.RLock("GetIntegrationResponses")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.integrations.Has(integrationKey(apiID, integrationID)) {
		return nil, ErrIntegrationNotFound
	}

	responses := b.integrationResponsesByIntegration.Get(integrationKey(apiID, integrationID))
	result := make([]IntegrationResponse, 0, len(responses))

	for _, ir := range responses {
		result = append(result, *ir)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].IntegrationResponseID < result[j].IntegrationResponseID
	})

	return result, nil
}

// DeleteIntegrationResponse removes an integration response.
func (b *InMemoryBackend) DeleteIntegrationResponse(apiID, integrationID, responseID string) error {
	b.mu.Lock("DeleteIntegrationResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.integrations.Has(integrationKey(apiID, integrationID)) {
		return ErrIntegrationNotFound
	}

	if !b.integrationResponses.Delete(integrationResponseKey(apiID, integrationID, responseID)) {
		return ErrIntegrationResponseNotFound
	}

	return nil
}

// --- Get/Delete Models ---

// GetModel retrieves a model by ID.
func (b *InMemoryBackend) GetModel(apiID, modelID string) (*Model, error) {
	b.mu.RLock("GetModel")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	m, ok := b.models.Get(modelKey(apiID, modelID))
	if !ok {
		return nil, ErrModelNotFound
	}

	cp := *m

	return &cp, nil
}

// GetModels retrieves all models for an API.
func (b *InMemoryBackend) GetModels(apiID string) ([]Model, error) {
	b.mu.RLock("GetModels")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	models := b.modelsByAPI.Get(apiID)
	result := make([]Model, 0, len(models))

	for _, m := range models {
		result = append(result, *m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ModelID < result[j].ModelID
	})

	return result, nil
}

// DeleteModel removes a model from an API.
func (b *InMemoryBackend) DeleteModel(apiID, modelID string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.models.Delete(modelKey(apiID, modelID)) {
		return ErrModelNotFound
	}

	return nil
}

// --- Get/Delete Route Responses ---

// GetRouteResponse retrieves a specific route response.
func (b *InMemoryBackend) GetRouteResponse(apiID, routeID, responseID string) (*RouteResponse, error) {
	b.mu.RLock("GetRouteResponse")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.routes.Has(routeKey(apiID, routeID)) {
		return nil, ErrRouteNotFound
	}

	rr, ok := b.routeResponses.Get(routeResponseKey(apiID, routeID, responseID))
	if !ok {
		return nil, ErrRouteResponseNotFound
	}

	cp := *rr

	return &cp, nil
}

// GetRouteResponses retrieves all route responses for a route.
func (b *InMemoryBackend) GetRouteResponses(apiID, routeID string) ([]RouteResponse, error) {
	b.mu.RLock("GetRouteResponses")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.routes.Has(routeKey(apiID, routeID)) {
		return nil, ErrRouteNotFound
	}

	responses := b.routeResponsesByRoute.Get(routeKey(apiID, routeID))
	result := make([]RouteResponse, 0, len(responses))

	for _, rr := range responses {
		result = append(result, *rr)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RouteResponseID < result[j].RouteResponseID
	})

	return result, nil
}

// DeleteRouteResponse removes a route response.
func (b *InMemoryBackend) DeleteRouteResponse(apiID, routeID, responseID string) error {
	b.mu.Lock("DeleteRouteResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.routes.Has(routeKey(apiID, routeID)) {
		return ErrRouteNotFound
	}

	if !b.routeResponses.Delete(routeResponseKey(apiID, routeID, responseID)) {
		return ErrRouteResponseNotFound
	}

	return nil
}

// --- Get/List Portals ---

// GetPortal retrieves a portal by ID.
func (b *InMemoryBackend) GetPortal(portalID string) (*Portal, error) {
	b.mu.RLock("GetPortal")
	defer b.mu.RUnlock()

	p, ok := b.portals.Get(portalID)
	if !ok {
		return nil, ErrPortalNotFound
	}

	cp := *p

	return &cp, nil
}

// ListPortals retrieves all portals.
func (b *InMemoryBackend) ListPortals() ([]Portal, error) {
	b.mu.RLock("ListPortals")
	defer b.mu.RUnlock()

	all := b.portals.All()
	result := make([]Portal, 0, len(all))

	for _, p := range all {
		result = append(result, *p)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PortalID < result[j].PortalID
	})

	return result, nil
}

// --- Get/List Portal Products ---

// GetPortalProduct retrieves a portal product by ID.
func (b *InMemoryBackend) GetPortalProduct(portalProductID string) (*PortalProduct, error) {
	b.mu.RLock("GetPortalProduct")
	defer b.mu.RUnlock()

	pp, ok := b.portalProducts.Get(portalProductID)
	if !ok {
		return nil, ErrPortalProductNotFound
	}

	cp := *pp

	return &cp, nil
}

// ListPortalProducts retrieves all portal products.
func (b *InMemoryBackend) ListPortalProducts() ([]PortalProduct, error) {
	b.mu.RLock("ListPortalProducts")
	defer b.mu.RUnlock()

	all := b.portalProducts.All()
	result := make([]PortalProduct, 0, len(all))

	for _, pp := range all {
		result = append(result, *pp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PortalProductID < result[j].PortalProductID
	})

	return result, nil
}

// --- List Product Pages ---

// ListProductPages retrieves all product pages for a portal product.
func (b *InMemoryBackend) ListProductPages(portalProductID string) ([]ProductPage, error) {
	b.mu.RLock("ListProductPages")
	defer b.mu.RUnlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	pages := b.productPagesByPortalProduct.Get(portalProductID)
	result := make([]ProductPage, 0, len(pages))

	for _, p := range pages {
		result = append(result, *p)
	}

	return result, nil
}

// --- List Product REST Endpoint Pages ---

// ListProductRestEndpointPages retrieves all product REST endpoint pages for a portal product.
func (b *InMemoryBackend) ListProductRestEndpointPages(portalProductID string) ([]ProductRestEndpointPage, error) {
	b.mu.RLock("ListProductRestEndpointPages")
	defer b.mu.RUnlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	pages := b.productREPagesByPortalProduct.Get(portalProductID)
	result := make([]ProductRestEndpointPage, 0, len(pages))

	for _, p := range pages {
		result = append(result, *p)
	}

	return result, nil
}

// --- Tags ---

const (
	arnResourceTypeAPIs        = "apis"
	arnResourceTypeVpcLinks    = "vpclinks"
	arnResourceTypeDomainNames = "domainnames"
	arnResourceTypeStages      = "stages"

	// arnMinPartsWithResourceType is the minimum number of slash-separated
	// parts in an ARN that carries an explicit resource type segment.
	arnMinPartsWithResourceType = 2

	// arnStageParts is the number of trailing slash-separated segments in a
	// Stage resource ARN: "apis", "{apiId}", "stages", "{stageName}".
	arnStageParts = 4
)

// parseStageARN extracts the API ID and stage name from a Stage resource ARN
// of the form ".../apis/{apiId}/stages/{stageName}", the AWS-modeled ARN for
// a Stage (Stages, unlike APIs/VPC links/domain names, are nested one level
// under their owning API so a single trailing "type/id" pair is not enough to
// resolve them). Returns ok=false for any ARN that does not match this shape.
func parseStageARN(arn string) (string, string, bool) {
	parts := strings.Split(arn, "/")
	if len(parts) < arnStageParts {
		return "", "", false
	}

	tail := parts[len(parts)-arnStageParts:]
	if tail[0] != arnResourceTypeAPIs || tail[2] != arnResourceTypeStages {
		return "", "", false
	}

	return tail[1], tail[3], true
}

// lookupStageLocked resolves a Stage by API ID and stage name. Callers must
// already hold b.mu (read or write).
func (b *InMemoryBackend) lookupStageLocked(apiID, stageName string) (*Stage, error) {
	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	s, ok := b.stages.Get(stageKey(apiID, stageName))
	if !ok {
		return nil, ErrStageNotFound
	}

	return s, nil
}

// arnResourceType returns the resource type and ID extracted from an ARN.
// For ARNs like "arn:aws:apigateway:us-east-1::/apis/abc123" the resource
// type would be "apis" and the ID "abc123".
// For a bare resource ID (no slashes besides the leading one) the type
// defaults to "apis" to preserve backwards-compatible behaviour.
func arnResourceType(arn string) (string, string) {
	parts := strings.Split(arn, "/")
	if len(parts) >= arnMinPartsWithResourceType {
		return parts[len(parts)-2], parts[len(parts)-1]
	}

	return arnResourceTypeAPIs, arn
}

// TagResource adds tags to a resource identified by ARN.
// Supports APIs, stages, VPC links, and domain names.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if apiID, stageName, ok := parseStageARN(resourceARN); ok {
		s, err := b.lookupStageLocked(apiID, stageName)
		if err != nil {
			return err
		}

		if s.Tags == nil {
			s.Tags = make(map[string]string)
		}

		maps.Copy(s.Tags, tags)

		return nil
	}

	resourceType, resourceID := arnResourceType(resourceARN)

	switch resourceType {
	case arnResourceTypeAPIs:
		api, ok := b.apis.Get(resourceID)
		if !ok {
			return ErrAPINotFound
		}

		if api.Tags == nil {
			api.Tags = make(map[string]string)
		}

		maps.Copy(api.Tags, tags)
	case arnResourceTypeVpcLinks:
		v, ok := b.vpcLinks.Get(resourceID)
		if !ok {
			return ErrVpcLinkNotFound
		}

		if v.Tags == nil {
			v.Tags = make(map[string]string)
		}

		maps.Copy(v.Tags, tags)
	case arnResourceTypeDomainNames:
		dn, ok := b.domainNames.Get(resourceID)
		if !ok {
			return ErrDomainNameNotFound
		}

		if dn.Tags == nil {
			dn.Tags = make(map[string]string)
		}

		maps.Copy(dn.Tags, tags)
	default:
		return ErrAPINotFound
	}

	return nil
}

// UntagResource removes tag keys from a resource identified by ARN.
// Supports APIs, stages, VPC links, and domain names.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if apiID, stageName, ok := parseStageARN(resourceARN); ok {
		s, err := b.lookupStageLocked(apiID, stageName)
		if err != nil {
			return err
		}

		for _, k := range tagKeys {
			delete(s.Tags, k)
		}

		return nil
	}

	resourceType, resourceID := arnResourceType(resourceARN)

	switch resourceType {
	case arnResourceTypeAPIs:
		api, ok := b.apis.Get(resourceID)
		if !ok {
			return ErrAPINotFound
		}

		for _, k := range tagKeys {
			delete(api.Tags, k)
		}
	case arnResourceTypeVpcLinks:
		v, ok := b.vpcLinks.Get(resourceID)
		if !ok {
			return ErrVpcLinkNotFound
		}

		for _, k := range tagKeys {
			delete(v.Tags, k)
		}
	case arnResourceTypeDomainNames:
		dn, ok := b.domainNames.Get(resourceID)
		if !ok {
			return ErrDomainNameNotFound
		}

		for _, k := range tagKeys {
			delete(dn.Tags, k)
		}
	default:
		return ErrAPINotFound
	}

	return nil
}

// GetTags retrieves all tags for a resource identified by ARN.
// Supports APIs, stages, VPC links, and domain names.
func (b *InMemoryBackend) GetTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	if apiID, stageName, ok := parseStageARN(resourceARN); ok {
		s, err := b.lookupStageLocked(apiID, stageName)
		if err != nil {
			return nil, err
		}

		return copyTags(s.Tags), nil
	}

	resourceType, resourceID := arnResourceType(resourceARN)

	switch resourceType {
	case arnResourceTypeAPIs:
		api, ok := b.apis.Get(resourceID)
		if !ok {
			return nil, ErrAPINotFound
		}

		return copyTags(api.Tags), nil
	case arnResourceTypeVpcLinks:
		v, ok := b.vpcLinks.Get(resourceID)
		if !ok {
			return nil, ErrVpcLinkNotFound
		}

		return copyTags(v.Tags), nil
	case arnResourceTypeDomainNames:
		dn, ok := b.domainNames.Get(resourceID)
		if !ok {
			return nil, ErrDomainNameNotFound
		}

		return copyTags(dn.Tags), nil
	default:
		return nil, ErrAPINotFound
	}
}

// UpdateAPIMapping updates fields on an existing API mapping.
func (b *InMemoryBackend) UpdateAPIMapping(
	domainName, mappingID string,
	input UpdateAPIMappingInput,
) (*APIMapping, error) {
	b.mu.Lock("UpdateAPIMapping")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	m, ok := b.apiMappings.Get(apiMappingKey(domainName, mappingID))
	if !ok {
		return nil, ErrAPIMappingNotFound
	}

	if input.APIID != "" {
		if !b.apis.Has(input.APIID) {
			return nil, ErrAPINotFound
		}
		stageToCheck := m.Stage
		if input.Stage != "" {
			stageToCheck = input.Stage
		}
		if !b.stages.Has(stageKey(input.APIID, stageToCheck)) {
			return nil, ErrStageNotFound
		}
		m.APIID = input.APIID
	}

	if input.Stage != "" {
		m.Stage = input.Stage
	}

	if input.APIMappingKey != "" {
		m.APIMappingKey = input.APIMappingKey
	}

	cp := *m

	return &cp, nil
}

// UpdateDeployment updates fields on an existing deployment.
func (b *InMemoryBackend) UpdateDeployment(
	apiID, deploymentID string,
	input UpdateDeploymentInput,
) (*Deployment, error) {
	b.mu.Lock("UpdateDeployment")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	dep, ok := b.deployments.Get(deploymentKey(apiID, deploymentID))
	if !ok {
		return nil, ErrDeploymentNotFound
	}

	if input.Description != "" {
		dep.Description = input.Description
	}

	cp := *dep

	return &cp, nil
}

// UpdateDomainName updates fields on an existing domain name.
func (b *InMemoryBackend) UpdateDomainName(domainName string, input UpdateDomainNameInput) (*DomainName, error) {
	b.mu.Lock("UpdateDomainName")
	defer b.mu.Unlock()

	dn, ok := b.domainNames.Get(domainName)
	if !ok {
		return nil, ErrDomainNameNotFound
	}

	if input.Tags != nil {
		if dn.Tags == nil {
			dn.Tags = make(map[string]string)
		}
		maps.Copy(dn.Tags, input.Tags)
	}

	if len(input.DomainNameConfigurations) > 0 {
		configs := make([]DomainNameConfiguration, len(input.DomainNameConfigurations))
		copy(configs, input.DomainNameConfigurations)
		dn.DomainNameConfigurations = configs
	}

	if input.MutualTLSAuthentication != nil {
		dn.MutualTLSAuthentication = cloneMutualTLSAuthentication(input.MutualTLSAuthentication)
	}

	cp := *dn

	return &cp, nil
}

// UpdateIntegrationResponse updates fields on an existing integration response.
func (b *InMemoryBackend) UpdateIntegrationResponse(
	apiID, integrationID, responseID string,
	input UpdateIntegrationResponseInput,
) (*IntegrationResponse, error) {
	b.mu.Lock("UpdateIntegrationResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.integrations.Has(integrationKey(apiID, integrationID)) {
		return nil, ErrIntegrationNotFound
	}

	ir, ok := b.integrationResponses.Get(integrationResponseKey(apiID, integrationID, responseID))
	if !ok {
		return nil, ErrIntegrationResponseNotFound
	}

	if input.IntegrationResponseKey != "" {
		ir.IntegrationResponseKey = input.IntegrationResponseKey
	}

	if input.ContentHandlingStrategy != "" {
		ir.ContentHandlingStrategy = input.ContentHandlingStrategy
	}

	if input.TemplateSelectionExpression != "" {
		ir.TemplateSelectionExpression = input.TemplateSelectionExpression
	}

	if input.ResponseParameters != nil {
		ir.ResponseParameters = input.ResponseParameters
	}

	if input.ResponseTemplates != nil {
		ir.ResponseTemplates = input.ResponseTemplates
	}

	cp := *ir

	return &cp, nil
}

// UpdateModel updates fields on an existing model.
func (b *InMemoryBackend) UpdateModel(apiID, modelID string, input UpdateModelInput) (*Model, error) {
	b.mu.Lock("UpdateModel")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	m, ok := b.models.Get(modelKey(apiID, modelID))
	if !ok {
		return nil, ErrModelNotFound
	}

	if input.Name != "" {
		m.Name = input.Name
	}

	if input.Schema != "" {
		m.Schema = input.Schema
	}

	if input.ContentType != "" {
		m.ContentType = input.ContentType
	}

	if input.Description != "" {
		m.Description = input.Description
	}

	cp := *m

	return &cp, nil
}

// UpdateRouteResponse updates fields on an existing route response.
func (b *InMemoryBackend) UpdateRouteResponse(
	apiID, routeID, responseID string,
	input UpdateRouteResponseInput,
) (*RouteResponse, error) {
	b.mu.Lock("UpdateRouteResponse")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	if !b.routes.Has(routeKey(apiID, routeID)) {
		return nil, ErrRouteNotFound
	}

	rr, ok := b.routeResponses.Get(routeResponseKey(apiID, routeID, responseID))
	if !ok {
		return nil, ErrRouteResponseNotFound
	}

	if input.RouteResponseKey != "" {
		rr.RouteResponseKey = input.RouteResponseKey
	}

	if input.ModelSelectionExpression != "" {
		rr.ModelSelectionExpression = input.ModelSelectionExpression
	}

	if input.ResponseModels != nil {
		rr.ResponseModels = input.ResponseModels
	}

	cp := *rr

	return &cp, nil
}

// UpdatePortal updates fields on an existing portal.
func (b *InMemoryBackend) UpdatePortal(portalID string, input UpdatePortalInput) (*Portal, error) {
	b.mu.Lock("UpdatePortal")
	defer b.mu.Unlock()

	p, ok := b.portals.Get(portalID)
	if !ok {
		return nil, ErrPortalNotFound
	}

	if input.Tags != nil {
		if p.Tags == nil {
			p.Tags = make(map[string]string)
		}
		maps.Copy(p.Tags, input.Tags)
	}

	if input.LogoURI != "" {
		p.LogoURI = input.LogoURI
	}
	if input.Status != "" {
		p.Status = input.Status
	}

	cp := *p

	return &cp, nil
}

// UpdatePortalProduct updates fields on an existing portal product.
func (b *InMemoryBackend) UpdatePortalProduct(
	portalProductID string,
	input UpdatePortalProductInput,
) (*PortalProduct, error) {
	b.mu.Lock("UpdatePortalProduct")
	defer b.mu.Unlock()

	pp, ok := b.portalProducts.Get(portalProductID)
	if !ok {
		return nil, ErrPortalProductNotFound
	}

	if input.Tags != nil {
		if pp.Tags == nil {
			pp.Tags = make(map[string]string)
		}
		maps.Copy(pp.Tags, input.Tags)
	}

	if input.DisplayName != "" {
		pp.DisplayName = input.DisplayName
	}

	if input.Description != "" {
		pp.Description = input.Description
	}

	cp := *pp

	return &cp, nil
}

// UpdateProductPage updates a product page.
func (b *InMemoryBackend) UpdateProductPage(
	portalProductID, pageID string,
	input UpdateProductPageInput,
) (*ProductPage, error) {
	b.mu.Lock("UpdateProductPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	page, ok := b.productPages.Get(productPageKey(portalProductID, pageID))
	if !ok {
		return nil, ErrProductPageNotFound
	}

	now := isoTime{time.Now()}
	if input.DisplayContent != nil {
		page.DisplayContent = input.DisplayContent
	}
	page.LastModified = &now

	cp := *page

	return &cp, nil
}

// UpdateProductRestEndpointPage updates a product REST endpoint page.
func (b *InMemoryBackend) UpdateProductRestEndpointPage(
	portalProductID, pageID string,
	input UpdateProductRestEndpointPageInput,
) (*ProductRestEndpointPage, error) {
	b.mu.Lock("UpdateProductRestEndpointPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	page, ok := b.productREPages.Get(productREPageKey(portalProductID, pageID))
	if !ok {
		return nil, ErrProductREPageNotFound
	}

	now := isoTime{time.Now()}
	if input.DisplayContent != nil {
		page.DisplayContent = input.DisplayContent
	}
	page.LastModified = &now

	cp := *page

	return &cp, nil
}

// DeletePortal removes a portal by ID.
func (b *InMemoryBackend) DeletePortal(portalID string) error {
	b.mu.Lock("DeletePortal")
	defer b.mu.Unlock()

	if !b.portals.Delete(portalID) {
		return ErrPortalNotFound
	}

	return nil
}

// DeletePortalProduct removes a portal product and all its associated pages.
func (b *InMemoryBackend) DeletePortalProduct(portalProductID string) error {
	b.mu.Lock("DeletePortalProduct")
	defer b.mu.Unlock()

	if !b.portalProducts.Delete(portalProductID) {
		return ErrPortalProductNotFound
	}

	for _, p := range slices.Clone(b.productPagesByPortalProduct.Get(portalProductID)) {
		b.productPages.Delete(productPageKey(portalProductID, p.ProductPageID))
	}

	for _, p := range slices.Clone(b.productREPagesByPortalProduct.Get(portalProductID)) {
		b.productREPages.Delete(productREPageKey(portalProductID, p.ProductRestEndpointPageID))
	}

	// Clean up the sharing policy entry so deleting a product does not leak its
	// policy document (AWS removes the associated sharing policy on delete).
	delete(b.portalProductSharingPolicies, portalProductID)

	return nil
}

// GetProductPage retrieves a specific product page.
func (b *InMemoryBackend) GetProductPage(portalProductID, pageID string) (*ProductPage, error) {
	b.mu.RLock("GetProductPage")
	defer b.mu.RUnlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	p, ok := b.productPages.Get(productPageKey(portalProductID, pageID))
	if !ok {
		return nil, ErrProductPageNotFound
	}

	cp := *p

	return &cp, nil
}

// GetProductRestEndpointPage retrieves a specific product REST endpoint page.
func (b *InMemoryBackend) GetProductRestEndpointPage(portalProductID, pageID string) (*ProductRestEndpointPage, error) {
	b.mu.RLock("GetProductRestEndpointPage")
	defer b.mu.RUnlock()

	if !b.portalProducts.Has(portalProductID) {
		return nil, ErrPortalProductNotFound
	}

	p, ok := b.productREPages.Get(productREPageKey(portalProductID, pageID))
	if !ok {
		return nil, ErrProductREPageNotFound
	}

	cp := *p

	return &cp, nil
}

// DeleteProductPage removes a product page from a portal product.
func (b *InMemoryBackend) DeleteProductPage(portalProductID, pageID string) error {
	b.mu.Lock("DeleteProductPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return ErrPortalProductNotFound
	}

	if !b.productPages.Delete(productPageKey(portalProductID, pageID)) {
		return ErrProductPageNotFound
	}

	return nil
}

// DeleteProductRestEndpointPage removes a product REST endpoint page from a portal product.
func (b *InMemoryBackend) DeleteProductRestEndpointPage(portalProductID, pageID string) error {
	b.mu.Lock("DeleteProductRestEndpointPage")
	defer b.mu.Unlock()

	if !b.portalProducts.Has(portalProductID) {
		return ErrPortalProductNotFound
	}

	if !b.productREPages.Delete(productREPageKey(portalProductID, pageID)) {
		return ErrProductREPageNotFound
	}

	return nil
}

// ResetAuthorizersCache is a no-op for the in-memory backend (caching is not simulated).
func (b *InMemoryBackend) ResetAuthorizersCache(apiID, stageName string) error {
	b.mu.RLock("ResetAuthorizersCache")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.stages.Has(stageKey(apiID, stageName)) {
		return ErrStageNotFound
	}

	return nil
}

// DeleteCorsConfiguration clears the CORS configuration on an API.
func (b *InMemoryBackend) DeleteCorsConfiguration(apiID string) error {
	b.mu.Lock("DeleteCorsConfiguration")
	defer b.mu.Unlock()

	api, ok := b.apis.Get(apiID)
	if !ok {
		return ErrAPINotFound
	}

	api.CorsConfiguration = nil

	return nil
}

// DeleteAccessLogSettings clears the access log settings on a stage.
func (b *InMemoryBackend) DeleteAccessLogSettings(apiID, stageName string) error {
	b.mu.Lock("DeleteAccessLogSettings")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	s, ok := b.stages.Get(stageKey(apiID, stageName))
	if !ok {
		return ErrStageNotFound
	}

	s.AccessLogSettings = nil
	s.LastUpdatedDate = isoTime{time.Now()}

	return nil
}

// DeleteRouteSettings removes per-route settings for a specific routeKey from a stage.
func (b *InMemoryBackend) DeleteRouteSettings(apiID, stageName, routeKey string) error {
	b.mu.Lock("DeleteRouteSettings")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	s, ok := b.stages.Get(stageKey(apiID, stageName))
	if !ok {
		return ErrStageNotFound
	}

	if s.RouteSettings != nil {
		delete(s.RouteSettings, routeKey)
	}

	s.LastUpdatedDate = isoTime{time.Now()}

	return nil
}

// ExportAPI generates a basic OpenAPI 3.0 specification for the API's routes.
func (b *InMemoryBackend) ExportAPI(apiID string) (map[string]any, error) {
	b.mu.RLock("ExportAPI")
	defer b.mu.RUnlock()

	api, ok := b.apis.Get(apiID)
	if !ok {
		return nil, ErrAPINotFound
	}

	const routeKeyParts = 2

	paths := map[string]any{}

	for _, route := range b.routesByAPI.Get(apiID) {
		// Parse route key: e.g. "GET /items" or "$connect" (WebSocket)
		parts := strings.SplitN(route.RouteKey, " ", routeKeyParts)

		var method, routePath string

		if len(parts) == routeKeyParts {
			method = strings.ToLower(parts[0])
			routePath = parts[1]
		} else {
			// WebSocket route like $connect, $disconnect, $default
			method = "get"
			routePath = "/" + strings.TrimPrefix(route.RouteKey, "$")
		}

		if _, exists := paths[routePath]; !exists {
			paths[routePath] = map[string]any{}
		}

		pathItem, _ := paths[routePath].(map[string]any)

		op := map[string]any{
			"operationId": route.RouteID,
			"responses":   map[string]any{"200": map[string]any{"description": "Success"}},
		}

		if route.OperationName != "" {
			op["summary"] = route.OperationName
		}

		if secName := b.exportRouteSecurityName(apiID, route); secName != "" {
			scopes := route.AuthorizationScopes
			if scopes == nil {
				scopes = []string{}
			}

			op["security"] = []any{map[string]any{secName: scopes}}
		}

		pathItem[method] = op
	}

	info := map[string]any{
		"title":   api.Name,
		"version": api.Version,
	}

	if api.Description != "" {
		info["description"] = api.Description
	}

	spec := map[string]any{
		"openapi": "3.0.1",
		"info":    info,
		"paths":   paths,
	}

	if schemes := b.exportSecuritySchemes(apiID); len(schemes) > 0 {
		spec["components"] = map[string]any{"securitySchemes": schemes}
	}

	return spec, nil
}

// exportRouteSecurityName returns the OpenAPI security-scheme name that a route
// references, or "" when the route requires no authorization. JWT/CUSTOM routes
// reference their authorizer by name; AWS_IAM references the "sigv4" scheme.
func (b *InMemoryBackend) exportRouteSecurityName(apiID string, route *Route) string {
	switch route.AuthorizationType {
	case authorizationTypeAWSIAM:
		return "sigv4"
	case authorizerTypeJWT, authorizationTypeCustom:
		if a, ok := b.authorizers.Get(authorizerKey(apiID, route.AuthorizerID)); ok {
			return a.Name
		}

		return route.AuthorizationType
	default:
		return ""
	}
}

// exportSecuritySchemes builds the components.securitySchemes block from the
// API's authorizers and any AWS_IAM routes, mirroring the AWS OpenAPI export
// (JWT authorizers carry the x-amazon-apigateway-authorizer extension).
// openAPIKeyType is the OpenAPI/JSON "type" discriminator key used in exported
// security schemes.
const openAPIKeyType = "type"

func (b *InMemoryBackend) exportSecuritySchemes(apiID string) map[string]any {
	schemes := map[string]any{}

	for _, a := range b.authorizersByAPI.Get(apiID) {
		if a.AuthorizerType != authorizerTypeJWT {
			continue
		}

		schemes[a.Name] = jwtSecurityScheme(a)
	}

	// Emit a sigv4 scheme when any route uses AWS_IAM authorization.
	for _, route := range b.routesByAPI.Get(apiID) {
		if route.AuthorizationType == authorizationTypeAWSIAM {
			schemes["sigv4"] = map[string]any{
				openAPIKeyType:                 "apiKey",
				"name":                         "Authorization",
				"in":                           "header",
				"x-amazon-apigateway-authtype": "awsSigv4",
			}

			break
		}
	}

	return schemes
}

// jwtSecurityScheme builds the OpenAPI securityScheme entry for a JWT authorizer,
// including the AWS x-amazon-apigateway-authorizer extension.
func jwtSecurityScheme(a *Authorizer) map[string]any {
	ext := map[string]any{
		openAPIKeyType:   "jwt",
		"identitySource": strings.Join(a.IdentitySource, ","),
	}

	if a.JwtConfiguration != nil {
		jwtCfg := map[string]any{}
		if a.JwtConfiguration.Issuer != "" {
			jwtCfg["issuer"] = a.JwtConfiguration.Issuer
		}

		if len(a.JwtConfiguration.Audience) > 0 {
			jwtCfg["audience"] = a.JwtConfiguration.Audience
		}

		ext["jwtConfiguration"] = jwtCfg
	}

	return map[string]any{
		openAPIKeyType:                   "oauth2",
		"flows":                          map[string]any{},
		"x-amazon-apigateway-authorizer": ext,
	}
}

// DeleteRouteRequestParameter removes a specific request parameter from a route.
func (b *InMemoryBackend) DeleteRouteRequestParameter(apiID, routeID, requestParameterKey string) error {
	b.mu.Lock("DeleteRouteRequestParameter")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	r, ok := b.routes.Get(routeKey(apiID, routeID))
	if !ok {
		return ErrRouteNotFound
	}

	if r.RequestParameters != nil {
		delete(r.RequestParameters, requestParameterKey)
	}

	return nil
}
