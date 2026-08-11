package vpclattice

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	matchPriority = service.PriorityPathVersioned

	pathServices                              = "/services"
	pathServiceNetworks                       = "/servicenetworks"
	pathServiceNetworkServiceAssociations     = "/servicenetworkserviceassociations"
	pathServiceNetworkVpcAssociations         = "/servicenetworkvpcassociations"
	pathTargetGroups                          = "/targetgroups"
	pathAccessLogSubscriptions                = "/accesslogsubscriptions"
	pathAuthPolicy                            = "/authpolicy"
	pathResourcePolicy                        = "/resourcepolicy"
	pathTags                                  = "/tags"
	pathResourceGateways                      = "/resourcegateways"
	pathResourceConfigurations                = "/resourceconfigurations"
	pathServiceNetworkResourceAssociations    = "/servicenetworkresourceassociations"
	pathDomainVerifications                   = "/domainverifications"
	pathResourceEndpointAssociations          = "/resourceendpointassociations"
	pathServiceNetworkVpcEndpointAssociations = "/servicenetworkvpcendpointassociations"

	opUnknown = "Unknown"

	keyMessage = "message"
	keyStatus  = "status"

	keyARN                = "arn"
	keyName               = "name"
	keyItems              = "items"
	keyCreatedAt          = "createdAt"
	keyLastUpdatedAt      = "lastUpdatedAt"
	keyServiceARN         = "serviceArn"
	keyServiceID          = "serviceId"
	keyServiceNetworkARN  = "serviceNetworkArn"
	keyServiceNetworkID   = "serviceNetworkId"
	keyServiceNetworkName = "serviceNetworkName"
	keyVPCID              = "vpcId"
	keyProtocol           = "protocol"
	keyPort               = "port"
	keyPriority           = "priority"
	keyIsDefault          = "isDefault"
	keyPolicy             = "policy"
	keyUnsuccessful       = "unsuccessful"
	keySuccessful         = "successful"
	keyDomainName         = "domainName"
	keyHostedZoneID       = "hostedZoneId"
	keyPrivateDNSEnabled  = "privateDnsEnabled"
	keyNameRequired       = "name is required"
	keyDestinationARN     = "destinationArn"

	keyType                        = "type"
	keyVpcIdentifier               = "vpcIdentifier"
	keyIPAddressType               = "ipAddressType"
	keyResourceConfigDNSResolution = "resourceConfigDnsResolution"
	keyIpv4AddressesPerENI         = "ipv4AddressesPerEni"
	keySecurityGroupIDs            = "securityGroupIds"
	keySubnetIDs                   = "subnetIds"
	keyCreatedBy                   = "createdBy"

	defKindArnResource = "arnResource"
	defKindDNSResource = "dnsResource"
	defKindIPResource  = "ipResource"

	opBatchUpdateRule      = "BatchUpdateRule"
	opCreateALS            = "CreateAccessLogSubscription"
	opCreateListener       = "CreateListener"
	opCreateRule           = "CreateRule"
	opCreateService        = "CreateService"
	opCreateSN             = "CreateServiceNetwork"
	opCreateSNSA           = "CreateServiceNetworkServiceAssociation"
	opCreateSNVA           = "CreateServiceNetworkVpcAssociation"
	opCreateTG             = "CreateTargetGroup"
	opDeleteALS            = "DeleteAccessLogSubscription"
	opDeleteAuthPolicy     = "DeleteAuthPolicy"
	opDeleteListener       = "DeleteListener"
	opDeleteResourcePolicy = "DeleteResourcePolicy"
	opDeleteRule           = "DeleteRule"
	opDeleteService        = "DeleteService"
	opDeleteSN             = "DeleteServiceNetwork"
	opDeleteSNSA           = "DeleteServiceNetworkServiceAssociation"
	opDeleteSNVA           = "DeleteServiceNetworkVpcAssociation"
	opDeleteTG             = "DeleteTargetGroup"
	opDeregisterTargets    = "DeregisterTargets"
	opGetALS               = "GetAccessLogSubscription"
	opGetAuthPolicy        = "GetAuthPolicy"
	opGetListener          = "GetListener"
	opGetResourcePolicy    = "GetResourcePolicy"
	opGetRule              = "GetRule"
	opGetService           = "GetService"
	opGetSN                = "GetServiceNetwork"
	opGetSNSA              = "GetServiceNetworkServiceAssociation"
	opGetSNVA              = "GetServiceNetworkVpcAssociation"
	opGetTG                = "GetTargetGroup"
	opListALSs             = "ListAccessLogSubscriptions"
	opListListeners        = "ListListeners"
	opListRules            = "ListRules"
	opListSNSAs            = "ListServiceNetworkServiceAssociations"
	opListSNVAs            = "ListServiceNetworkVpcAssociations"
	opListSNs              = "ListServiceNetworks"
	opListServices         = "ListServices"
	opListTagsForResource  = "ListTagsForResource"
	opListTGs              = "ListTargetGroups"
	opListTargets          = "ListTargets"
	opPutAuthPolicy        = "PutAuthPolicy"
	opPutResourcePolicy    = "PutResourcePolicy"
	opRegisterTargets      = "RegisterTargets"
	opTagResource          = "TagResource"
	opUntagResource        = "UntagResource"
	opUpdateALS            = "UpdateAccessLogSubscription"
	opUpdateListener       = "UpdateListener"
	opUpdateRule           = "UpdateRule"
	opUpdateService        = "UpdateService"
	opUpdateSN             = "UpdateServiceNetwork"
	opUpdateSNVA           = "UpdateServiceNetworkVpcAssociation"
	opUpdateTG             = "UpdateTargetGroup"

	opCreateResourceGateway = "CreateResourceGateway"
	opGetResourceGateway    = "GetResourceGateway"
	opUpdateResourceGateway = "UpdateResourceGateway"
	opDeleteResourceGateway = "DeleteResourceGateway"
	opListResourceGateways  = "ListResourceGateways"

	opCreateResourceConfiguration = "CreateResourceConfiguration"
	opGetResourceConfiguration    = "GetResourceConfiguration"
	opUpdateResourceConfiguration = "UpdateResourceConfiguration"
	opDeleteResourceConfiguration = "DeleteResourceConfiguration"
	opListResourceConfigurations  = "ListResourceConfigurations"

	opCreateSNRA = "CreateServiceNetworkResourceAssociation"
	opGetSNRA    = "GetServiceNetworkResourceAssociation"
	opDeleteSNRA = "DeleteServiceNetworkResourceAssociation"
	opListSNRAs  = "ListServiceNetworkResourceAssociations"

	opStartDomainVerification  = "StartDomainVerification"
	opGetDomainVerification    = "GetDomainVerification"
	opDeleteDomainVerification = "DeleteDomainVerification"
	opListDomainVerifications  = "ListDomainVerifications"

	opListResourceEndpointAssociations          = "ListResourceEndpointAssociations"
	opDeleteResourceEndpointAssociation         = "DeleteResourceEndpointAssociation"
	opListServiceNetworkVpcEndpointAssociations = "ListServiceNetworkVpcEndpointAssociations"
)

// Handler handles VPC Lattice HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "VPCLattice" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns all supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opBatchUpdateRule,
		opCreateALS,
		opCreateListener,
		opCreateRule,
		opCreateService,
		opCreateSN,
		opCreateSNSA,
		opCreateSNVA,
		opCreateTG,
		opDeleteALS,
		opDeleteAuthPolicy,
		opDeleteListener,
		opDeleteResourcePolicy,
		opDeleteRule,
		opDeleteService,
		opDeleteSN,
		opDeleteSNSA,
		opDeleteSNVA,
		opDeleteTG,
		opDeregisterTargets,
		opGetALS,
		opGetAuthPolicy,
		opGetListener,
		opGetResourcePolicy,
		opGetRule,
		opGetService,
		opGetSN,
		opGetSNSA,
		opGetSNVA,
		opGetTG,
		opListALSs,
		opListListeners,
		opListRules,
		opListSNSAs,
		opListSNVAs,
		opListSNs,
		opListServices,
		opListTagsForResource,
		opListTGs,
		opListTargets,
		opPutAuthPolicy,
		opPutResourcePolicy,
		opRegisterTargets,
		opTagResource,
		opUntagResource,
		opUpdateALS,
		opUpdateListener,
		opUpdateRule,
		opUpdateService,
		opUpdateSN,
		opUpdateSNVA,
		opUpdateTG,
		opCreateResourceGateway,
		opGetResourceGateway,
		opUpdateResourceGateway,
		opDeleteResourceGateway,
		opListResourceGateways,
		opCreateResourceConfiguration,
		opGetResourceConfiguration,
		opUpdateResourceConfiguration,
		opDeleteResourceConfiguration,
		opListResourceConfigurations,
		opCreateSNRA,
		opGetSNRA,
		opDeleteSNRA,
		opListSNRAs,
		opStartDomainVerification,
		opGetDomainVerification,
		opDeleteDomainVerification,
		opListDomainVerifications,
		opListResourceEndpointAssociations,
		opDeleteResourceEndpointAssociation,
		opListServiceNetworkVpcEndpointAssociations,
	}
}

// onceRoutedPathPrefixes lazily builds the list of top-level path prefixes
// RouteMatcher owns, exactly once. Every operation family's collection path
// lives here so RouteMatcher stays a simple data-driven scan instead of a
// long OR chain (which cyclop flags past ~10 families).
//
//nolint:gochecknoglobals // read-only package-level lookup table, built once via sync.OnceValue
var onceRoutedPathPrefixes = sync.OnceValue(func() []string {
	return []string{
		pathServices,
		pathServiceNetworks,
		pathServiceNetworkServiceAssociations,
		pathServiceNetworkVpcAssociations,
		pathTargetGroups,
		pathAccessLogSubscriptions,
		pathResourceGateways,
		pathResourceConfigurations,
		pathServiceNetworkResourceAssociations,
		pathDomainVerifications,
		pathResourceEndpointAssociations,
	}
})

// RouteMatcher returns a function that matches VPC Lattice API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		for _, p := range onceRoutedPathPrefixes() {
			if path == p || strings.HasPrefix(path, p+"/") {
				return true
			}
		}

		return path == pathServiceNetworkVpcEndpointAssociations ||
			strings.HasPrefix(path, pathAuthPolicy+"/") ||
			strings.HasPrefix(path, pathResourcePolicy+"/") ||
			isVPCLatticeTagPath(path)
	}
}

func isVPCLatticeTagPath(path string) bool {
	rest, ok := strings.CutPrefix(path, pathTags+"/")

	return ok && strings.Contains(rest, ":vpc-lattice:")
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation classifies the request into an operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _, _, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource returns the primary resource identifier.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, id, _, _ := classifyPath(c.Request().Method, c.Request().URL.Path)

	return id
}

// Handler returns the Echo handler function for VPC Lattice requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

// opHandlerFunc is the uniform shape every VPC Lattice operation handler is
// adapted to below, so the op -> handler mapping can live in a single lookup
// table (onceOpHandlers) instead of a large switch. id1/id2/id3 are the path
// segments classifyPath extracted (service/listener/rule etc, in order);
// most operations only need a subset of them.
type opHandlerFunc func(h *Handler, c *echo.Context, id1, id2, id3 string, body map[string]any) error

// onceOpHandlers lazily builds the operation-name -> handler lookup table
// used by handleREST, exactly once. Each entry is a thin adapter from the
// uniform opHandlerFunc shape to the specific handleXxx method's actual
// signature. This replaces what was previously a single ~50-case switch
// (handleREST's branching is mechanical -- one case per op, each a one-line
// delegation -- so moving it into data removes the cyclomatic/line-count
// complexity without changing behavior).
//
//nolint:gochecknoglobals // read-only package-level lookup table, built once via sync.OnceValue
var onceOpHandlers = sync.OnceValue(func() map[string]opHandlerFunc {
	return map[string]opHandlerFunc{
		opCreateService: func(h *Handler, c *echo.Context, _, _, _ string, body map[string]any) error {
			return h.handleCreateService(c, body)
		},
		opGetService: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetService(c, id1)
		},
		opUpdateService: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleUpdateService(c, id1, body)
		},
		opDeleteService: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteService(c, id1)
		},
		opListServices: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListServices(c)
		},
		opCreateSN: func(h *Handler, c *echo.Context, _, _, _ string, body map[string]any) error {
			return h.handleCreateServiceNetwork(c, body)
		},
		opGetSN: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetServiceNetwork(c, id1)
		},
		opUpdateSN: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleUpdateServiceNetwork(c, id1, body)
		},
		opDeleteSN: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteServiceNetwork(c, id1)
		},
		opListSNs: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListServiceNetworks(c)
		},
		opCreateSNSA: func(h *Handler, c *echo.Context, _, _, _ string, body map[string]any) error {
			return h.handleCreateSNSA(c, body)
		},
		opGetSNSA: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetSNSA(c, id1)
		},
		opDeleteSNSA: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteSNSA(c, id1)
		},
		opListSNSAs: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListSNSAs(c)
		},
		opCreateSNVA: func(h *Handler, c *echo.Context, _, _, _ string, body map[string]any) error {
			return h.handleCreateSNVA(c, body)
		},
		opGetSNVA: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetSNVA(c, id1)
		},
		opUpdateSNVA: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleUpdateSNVA(c, id1, body)
		},
		opDeleteSNVA: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteSNVA(c, id1)
		},
		opListSNVAs: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListSNVAs(c)
		},
		opCreateListener: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleCreateListener(c, id1, body)
		},
		opGetListener: func(h *Handler, c *echo.Context, id1, id2, _ string, _ map[string]any) error {
			return h.handleGetListener(c, id1, id2)
		},
		opUpdateListener: func(h *Handler, c *echo.Context, id1, id2, _ string, body map[string]any) error {
			return h.handleUpdateListener(c, id1, id2, body)
		},
		opDeleteListener: func(h *Handler, c *echo.Context, id1, id2, _ string, _ map[string]any) error {
			return h.handleDeleteListener(c, id1, id2)
		},
		opListListeners: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleListListeners(c, id1)
		},
		opCreateRule: func(h *Handler, c *echo.Context, id1, id2, _ string, body map[string]any) error {
			return h.handleCreateRule(c, id1, id2, body)
		},
		opGetRule: func(h *Handler, c *echo.Context, id1, id2, id3 string, _ map[string]any) error {
			return h.handleGetRule(c, id1, id2, id3)
		},
		opUpdateRule: func(h *Handler, c *echo.Context, id1, id2, id3 string, body map[string]any) error {
			return h.handleUpdateRule(c, id1, id2, id3, body)
		},
		opDeleteRule: func(h *Handler, c *echo.Context, id1, id2, id3 string, _ map[string]any) error {
			return h.handleDeleteRule(c, id1, id2, id3)
		},
		opListRules: func(h *Handler, c *echo.Context, id1, id2, _ string, _ map[string]any) error {
			return h.handleListRules(c, id1, id2)
		},
		opBatchUpdateRule: func(h *Handler, c *echo.Context, id1, id2, _ string, body map[string]any) error {
			return h.handleBatchUpdateRule(c, id1, id2, body)
		},
		opCreateTG: func(h *Handler, c *echo.Context, _, _, _ string, body map[string]any) error {
			return h.handleCreateTargetGroup(c, body)
		},
		opGetTG: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetTargetGroup(c, id1)
		},
		opUpdateTG: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleUpdateTargetGroup(c, id1, body)
		},
		opDeleteTG: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteTargetGroup(c, id1)
		},
		opListTGs: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListTargetGroups(c)
		},
		opRegisterTargets: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleRegisterTargets(c, id1, body)
		},
		opDeregisterTargets: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleDeregisterTargets(c, id1, body)
		},
		opListTargets: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleListTargets(c, id1, body)
		},
		opCreateALS: func(h *Handler, c *echo.Context, _, _, _ string, body map[string]any) error {
			return h.handleCreateALS(c, body)
		},
		opGetALS: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetALS(c, id1)
		},
		opUpdateALS: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleUpdateALS(c, id1, body)
		},
		opDeleteALS: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteALS(c, id1)
		},
		opListALSs: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListALSs(c)
		},
		opPutAuthPolicy: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handlePutAuthPolicy(c, id1, body)
		},
		opGetAuthPolicy: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetAuthPolicy(c, id1)
		},
		opDeleteAuthPolicy: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteAuthPolicy(c, id1)
		},
		opPutResourcePolicy: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handlePutResourcePolicy(c, id1, body)
		},
		opGetResourcePolicy: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetResourcePolicy(c, id1)
		},
		opDeleteResourcePolicy: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteResourcePolicy(c, id1)
		},
		opTagResource: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleTagResource(c, id1, body)
		},
		opUntagResource: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleUntagResource(c, id1)
		},
		opListTagsForResource: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleListTagsForResource(c, id1)
		},
		opCreateResourceGateway: func(h *Handler, c *echo.Context, _, _, _ string, body map[string]any) error {
			return h.handleCreateResourceGateway(c, body)
		},
		opGetResourceGateway: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetResourceGateway(c, id1)
		},
		opUpdateResourceGateway: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleUpdateResourceGateway(c, id1, body)
		},
		opDeleteResourceGateway: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteResourceGateway(c, id1)
		},
		opListResourceGateways: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListResourceGateways(c)
		},
		opCreateResourceConfiguration: func(h *Handler, c *echo.Context, _, _, _ string, body map[string]any) error {
			return h.handleCreateResourceConfiguration(c, body)
		},
		opGetResourceConfiguration: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetResourceConfiguration(c, id1)
		},
		opUpdateResourceConfiguration: func(h *Handler, c *echo.Context, id1, _, _ string, body map[string]any) error {
			return h.handleUpdateResourceConfiguration(c, id1, body)
		},
		opDeleteResourceConfiguration: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteResourceConfiguration(c, id1)
		},
		opListResourceConfigurations: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListResourceConfigurations(c)
		},
		opCreateSNRA: func(h *Handler, c *echo.Context, _, _, _ string, body map[string]any) error {
			return h.handleCreateSNRA(c, body)
		},
		opGetSNRA: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetSNRA(c, id1)
		},
		opDeleteSNRA: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteSNRA(c, id1)
		},
		opListSNRAs: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListSNRAs(c)
		},
		opStartDomainVerification: func(h *Handler, c *echo.Context, _, _, _ string, body map[string]any) error {
			return h.handleStartDomainVerification(c, body)
		},
		opGetDomainVerification: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleGetDomainVerification(c, id1)
		},
		opDeleteDomainVerification: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteDomainVerification(c, id1)
		},
		opListDomainVerifications: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListDomainVerifications(c)
		},
		opListResourceEndpointAssociations: func(h *Handler, c *echo.Context, _, _, _ string, _ map[string]any) error {
			return h.handleListResourceEndpointAssociations(c)
		},
		opDeleteResourceEndpointAssociation: func(h *Handler, c *echo.Context, id1, _, _ string, _ map[string]any) error {
			return h.handleDeleteResourceEndpointAssociation(c, id1)
		},
		opListServiceNetworkVpcEndpointAssociations: func(
			h *Handler, c *echo.Context, _, _, _ string, _ map[string]any,
		) error {
			return h.handleListServiceNetworkVpcEndpointAssociations(c)
		},
	}
})

// handleREST decodes the request body and dispatches to the operation
// resolved by classifyPath via onceOpHandlers.
func (h *Handler) handleREST(c *echo.Context) error {
	op, id1, id2, id3 := classifyPath(c.Request().Method, c.Request().URL.Path)

	var body map[string]any
	if c.Request().ContentLength != 0 {
		if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
			err.Error() != "EOF" {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "invalid JSON body"})
		}
	}

	if body == nil {
		body = map[string]any{}
	}

	fn, ok := onceOpHandlers()[op]
	if !ok {
		return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "unknown operation"})
	}

	return fn(h, c, id1, id2, id3, body)
}

// amznErrorTypeHeader carries the modeled exception type for the restjson1
// protocol. aws-sdk-go-v2's restjson.GetErrorInfo (aws/protocol/restjson/decoder_util.go)
// reads this header before falling back to a body "code"/"__type" field; without it every
// error here deserialized client-side as a generic UnknownError.
const amznErrorTypeHeader = "X-Amzn-Errortype"

// handleError converts backend errors to HTTP responses. Wire types are
// verified against this service's own deserializer error lists
// (vpclattice@v1.25.5 deserializers.go: CreateService/GetService/DeleteService/
// UpdateService/CreateServiceNetwork/CreateTargetGroup model exactly
// AccessDeniedException, ConflictException, InternalServerException,
// ResourceNotFoundException, ServiceQuotaExceededException,
// ThrottlingException, ValidationException).
func (h *Handler) handleError(c *echo.Context, err error) error {
	status := http.StatusInternalServerError
	wireType := "InternalServerException"

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		status, wireType = http.StatusNotFound, "ResourceNotFoundException"
	case errors.Is(err, awserr.ErrAlreadyExists), errors.Is(err, awserr.ErrConflict):
		status, wireType = http.StatusConflict, "ConflictException"
	case errors.Is(err, awserr.ErrInvalidParameter):
		status, wireType = http.StatusBadRequest, "ValidationException"
	}

	c.Response().Header().Set(amznErrorTypeHeader, wireType)

	return c.JSON(status, map[string]any{keyMessage: err.Error()})
}

// ------- Path classification -------

// collectionRoute pairs a top-level resource collection's exact path (e.g.
// "/services") with its create/list operations and the delegate that
// classifies paths beneath it (e.g. "/services/{id}/listeners/...").
type collectionRoute struct {
	subHandler func(method, path string) (string, string, string, string)
	path       string
	subPrefix  string
	createOp   string
	listOp     string
}

// onceCollectionRoutes lazily builds the ordered list of top-level resource
// collections handled by classifyPath, exactly once. Order doesn't affect
// matching: every collection's path/subPrefix pair is mutually exclusive
// with every other's.
//
//nolint:gochecknoglobals // read-only package-level lookup table, built once via sync.OnceValue
var onceCollectionRoutes = sync.OnceValue(func() []collectionRoute {
	return []collectionRoute{
		{
			path: pathServices, subPrefix: pathServices + "/",
			createOp: opCreateService, listOp: opListServices,
			subHandler: classifyServicePath,
		},
		{
			path: pathServiceNetworks, subPrefix: pathServiceNetworks + "/",
			createOp: opCreateSN, listOp: opListSNs,
			subHandler: classifyServiceNetworkPath,
		},
		{
			path: pathServiceNetworkServiceAssociations, subPrefix: pathServiceNetworkServiceAssociations + "/",
			createOp: opCreateSNSA, listOp: opListSNSAs,
			subHandler: classifySNSAPath,
		},
		{
			path: pathServiceNetworkVpcAssociations, subPrefix: pathServiceNetworkVpcAssociations + "/",
			createOp: opCreateSNVA, listOp: opListSNVAs,
			subHandler: classifySNVAPath,
		},
		{
			path: pathTargetGroups, subPrefix: pathTargetGroups + "/",
			createOp: opCreateTG, listOp: opListTGs,
			subHandler: classifyTargetGroupPath,
		},
		{
			path: pathAccessLogSubscriptions, subPrefix: pathAccessLogSubscriptions + "/",
			createOp: opCreateALS, listOp: opListALSs,
			subHandler: classifyALSPath,
		},
		{
			path: pathResourceGateways, subPrefix: pathResourceGateways + "/",
			createOp: opCreateResourceGateway, listOp: opListResourceGateways,
			subHandler: classifyResourceGatewayPath,
		},
		{
			path: pathResourceConfigurations, subPrefix: pathResourceConfigurations + "/",
			createOp: opCreateResourceConfiguration, listOp: opListResourceConfigurations,
			subHandler: classifyResourceConfigurationPath,
		},
		{
			path: pathServiceNetworkResourceAssociations, subPrefix: pathServiceNetworkResourceAssociations + "/",
			createOp: opCreateSNRA, listOp: opListSNRAs,
			subHandler: classifySNRAPath,
		},
		{
			path: pathDomainVerifications, subPrefix: pathDomainVerifications + "/",
			createOp: opStartDomainVerification, listOp: opListDomainVerifications,
			subHandler: classifyDomainVerificationPath,
		},
	}
})

// onceSingletonRoutes lazily builds the method -> operation lookup tables
// for the /authpolicy/, /resourcepolicy/, and /tags/ families, which key off
// a single resourceID/resourceArn path segment rather than a numbered
// collection, exactly once.
//
//nolint:gochecknoglobals // read-only package-level lookup tables, built once via sync.OnceValue
var (
	onceAuthPolicyOps = sync.OnceValue(func() map[string]string {
		return map[string]string{
			http.MethodPut:    opPutAuthPolicy,
			http.MethodGet:    opGetAuthPolicy,
			http.MethodDelete: opDeleteAuthPolicy,
		}
	})
	onceResourcePolicyOps = sync.OnceValue(func() map[string]string {
		return map[string]string{
			http.MethodPut:    opPutResourcePolicy,
			http.MethodGet:    opGetResourcePolicy,
			http.MethodDelete: opDeleteResourcePolicy,
		}
	})
	onceTagOps = sync.OnceValue(func() map[string]string {
		return map[string]string{
			http.MethodPost:   opTagResource,
			http.MethodDelete: opUntagResource,
			http.MethodGet:    opListTagsForResource,
		}
	})
)

// classifyPath maps (method, path) → (op, id1, id2, id3).
// id1..id3 are path segments in order (service, listener, rule etc.).
//
// Top-level dispatch is a data-driven walk over onceCollectionRoutes (for
// the numbered resource collections) falling back to classifySingletonPath
// (for the auth-policy/resource-policy/tags families), rather than a flat
// switch, so adding a resource family only means appending a table entry.
func classifyPath(method, path string) (string, string, string, string) {
	for _, r := range onceCollectionRoutes() {
		if path == r.path {
			if method == http.MethodPost {
				return r.createOp, "", "", ""
			}

			return r.listOp, "", "", ""
		}

		if strings.HasPrefix(path, r.subPrefix) {
			return r.subHandler(method, path)
		}
	}

	// ResourceEndpointAssociation/ServiceNetworkVpcEndpointAssociation are
	// list-only families in the real API (real AWS populates them solely via
	// EC2 CreateVpcEndpoint, not a vpc-lattice Create op -- see
	// service_network_resource_associations.go), so they aren't
	// onceCollectionRoutes entries (which always assume POST=create at the
	// collection path).
	if path == pathResourceEndpointAssociations && method == http.MethodGet {
		return opListResourceEndpointAssociations, "", "", ""
	}

	if id, ok := strings.CutPrefix(path, pathResourceEndpointAssociations+"/"); ok && method == http.MethodDelete {
		return opDeleteResourceEndpointAssociation, id, "", ""
	}

	if path == pathServiceNetworkVpcEndpointAssociations && method == http.MethodGet {
		return opListServiceNetworkVpcEndpointAssociations, "", "", ""
	}

	if op, id, ok := classifySingletonPath(path, pathAuthPolicy, method, onceAuthPolicyOps()); ok {
		return op, id, "", ""
	}

	if op, id, ok := classifySingletonPath(path, pathResourcePolicy, method, onceResourcePolicyOps()); ok {
		return op, id, "", ""
	}

	if op, id, ok := classifySingletonPath(path, pathTags, method, onceTagOps()); ok {
		return op, id, "", ""
	}

	return opUnknown, "", "", ""
}

// classifySingletonPath handles the /{prefix}/{resourceID} routes (auth
// policy, resource policy, tags) whose operation is selected purely by HTTP
// method. ok is false when path isn't under prefix at all, signaling the
// caller to try the next family; when path is under prefix but method has no
// mapped op, it returns (opUnknown, "", true) -- discarding the resource ID,
// matching classifyPath's pre-refactor fallthrough behavior for an
// unrecognized method on these routes.
func classifySingletonPath(
	path, prefix, method string,
	ops map[string]string,
) (string, string, bool) {
	id, ok := strings.CutPrefix(path, prefix+"/")
	if !ok {
		return "", "", false
	}

	if op, found := ops[method]; found {
		return op, id, true
	}

	return opUnknown, "", true
}

// classifyServicePath handles /services/{serviceID}[/listeners[/...]].
func classifyServicePath(method, path string) (string, string, string, string) {
	rest := strings.TrimPrefix(path, pathServices+"/")
	serviceID, sub, hasSub := strings.Cut(rest, "/")

	if !hasSub {
		return classifyServiceRootPath(method, serviceID)
	}

	// sub = listeners[/{listenerID}[/rules[/{ruleID}]]]
	if sub == "listeners" {
		if method == http.MethodPost {
			return opCreateListener, serviceID, "", ""
		}

		return opListListeners, serviceID, "", ""
	}

	if listenerRest, ok := strings.CutPrefix(sub, "listeners/"); ok {
		return classifyListenerPath(method, serviceID, listenerRest)
	}

	return opUnknown, serviceID, "", ""
}

// classifyServiceRootPath handles /services/{serviceID} with no sub-path.
func classifyServiceRootPath(method, serviceID string) (string, string, string, string) {
	switch method {
	case http.MethodGet:
		return opGetService, serviceID, "", ""
	case http.MethodPatch:
		return opUpdateService, serviceID, "", ""
	case http.MethodDelete:
		return opDeleteService, serviceID, "", ""
	}

	return opUnknown, serviceID, "", ""
}

// classifyListenerPath handles /services/{serviceID}/listeners/{listenerRest}.
func classifyListenerPath(method, serviceID, listenerRest string) (string, string, string, string) {
	listenerID, listenerSub, hasListenerSub := strings.Cut(listenerRest, "/")

	if !hasListenerSub {
		return classifyListenerRootPath(method, serviceID, listenerID)
	}

	if listenerSub == "rules" {
		return classifyRulesCollectionPath(method, serviceID, listenerID)
	}

	if ruleID, ok := strings.CutPrefix(listenerSub, "rules/"); ok {
		if op := classifyRuleOp(method); op != opUnknown {
			return op, serviceID, listenerID, ruleID
		}
	}

	return opUnknown, serviceID, "", ""
}

// classifyListenerRootPath handles /services/{serviceID}/listeners/{listenerID}
// with no further sub-path.
func classifyListenerRootPath(method, serviceID, listenerID string) (string, string, string, string) {
	switch method {
	case http.MethodGet:
		return opGetListener, serviceID, listenerID, ""
	case http.MethodPatch:
		return opUpdateListener, serviceID, listenerID, ""
	case http.MethodDelete:
		return opDeleteListener, serviceID, listenerID, ""
	}

	return opUnknown, serviceID, listenerID, ""
}

// classifyRulesCollectionPath handles the /rules collection under a listener.
func classifyRulesCollectionPath(method, serviceID, listenerID string) (string, string, string, string) {
	if method == http.MethodPost {
		return opCreateRule, serviceID, listenerID, ""
	}

	if method == http.MethodPatch {
		return opBatchUpdateRule, serviceID, listenerID, ""
	}

	return opListRules, serviceID, listenerID, ""
}

// classifyRuleOp maps a method to its single-rule operation name, or
// opUnknown if the method has no corresponding operation.
func classifyRuleOp(method string) string {
	switch method {
	case http.MethodGet:
		return opGetRule
	case http.MethodPatch:
		return opUpdateRule
	case http.MethodDelete:
		return opDeleteRule
	}

	return opUnknown
}

// classifyServiceNetworkPath handles /servicenetworks/{id}.
func classifyServiceNetworkPath(
	method, path string,
) (string, string, string, string) {
	id := strings.TrimPrefix(path, pathServiceNetworks+"/")
	switch method {
	case http.MethodGet:
		return opGetSN, id, "", ""
	case http.MethodPatch:
		return opUpdateSN, id, "", ""
	case http.MethodDelete:
		return opDeleteSN, id, "", ""
	}

	return opUnknown, id, "", ""
}

// classifySNSAPath handles /servicenetworkserviceassociations/{id}.
func classifySNSAPath(
	method, path string,
) (string, string, string, string) {
	id := strings.TrimPrefix(path, pathServiceNetworkServiceAssociations+"/")
	switch method {
	case http.MethodGet:
		return opGetSNSA, id, "", ""
	case http.MethodDelete:
		return opDeleteSNSA, id, "", ""
	}

	return opUnknown, id, "", ""
}

// classifySNVAPath handles /servicenetworkvpcassociations/{id}.
func classifySNVAPath(
	method, path string,
) (string, string, string, string) {
	id := strings.TrimPrefix(path, pathServiceNetworkVpcAssociations+"/")
	switch method {
	case http.MethodGet:
		return opGetSNVA, id, "", ""
	case http.MethodPatch:
		return opUpdateSNVA, id, "", ""
	case http.MethodDelete:
		return opDeleteSNVA, id, "", ""
	}

	return opUnknown, id, "", ""
}

// classifyTargetGroupPath handles /targetgroups/{id}[/registertargets|deregistertargets|listtargets].
func classifyTargetGroupPath(
	method, path string,
) (string, string, string, string) {
	rest := strings.TrimPrefix(path, pathTargetGroups+"/")
	tgID, sub, hasSub := strings.Cut(rest, "/")

	if !hasSub {
		switch method {
		case http.MethodGet:
			return opGetTG, tgID, "", ""
		case http.MethodPatch:
			return opUpdateTG, tgID, "", ""
		case http.MethodDelete:
			return opDeleteTG, tgID, "", ""
		}

		return opUnknown, tgID, "", ""
	}

	switch sub {
	case "registertargets":
		return opRegisterTargets, tgID, "", ""
	case "deregistertargets":
		return opDeregisterTargets, tgID, "", ""
	case "listtargets":
		return opListTargets, tgID, "", ""
	}

	return opUnknown, tgID, "", ""
}

// classifyResourceGatewayPath handles /resourcegateways/{id}.
func classifyResourceGatewayPath(method, path string) (string, string, string, string) {
	id := strings.TrimPrefix(path, pathResourceGateways+"/")
	switch method {
	case http.MethodGet:
		return opGetResourceGateway, id, "", ""
	case http.MethodPatch:
		return opUpdateResourceGateway, id, "", ""
	case http.MethodDelete:
		return opDeleteResourceGateway, id, "", ""
	}

	return opUnknown, id, "", ""
}

// classifyResourceConfigurationPath handles /resourceconfigurations/{id}.
func classifyResourceConfigurationPath(method, path string) (string, string, string, string) {
	id := strings.TrimPrefix(path, pathResourceConfigurations+"/")
	switch method {
	case http.MethodGet:
		return opGetResourceConfiguration, id, "", ""
	case http.MethodPatch:
		return opUpdateResourceConfiguration, id, "", ""
	case http.MethodDelete:
		return opDeleteResourceConfiguration, id, "", ""
	}

	return opUnknown, id, "", ""
}

// classifySNRAPath handles /servicenetworkresourceassociations/{id}.
func classifySNRAPath(method, path string) (string, string, string, string) {
	id := strings.TrimPrefix(path, pathServiceNetworkResourceAssociations+"/")
	switch method {
	case http.MethodGet:
		return opGetSNRA, id, "", ""
	case http.MethodDelete:
		return opDeleteSNRA, id, "", ""
	}

	return opUnknown, id, "", ""
}

// classifyDomainVerificationPath handles /domainverifications/{id}.
func classifyDomainVerificationPath(method, path string) (string, string, string, string) {
	id := strings.TrimPrefix(path, pathDomainVerifications+"/")
	switch method {
	case http.MethodGet:
		return opGetDomainVerification, id, "", ""
	case http.MethodDelete:
		return opDeleteDomainVerification, id, "", ""
	}

	return opUnknown, id, "", ""
}

// classifyALSPath handles /accesslogsubscriptions/{id}.
func classifyALSPath(
	method, path string,
) (string, string, string, string) {
	id := strings.TrimPrefix(path, pathAccessLogSubscriptions+"/")
	switch method {
	case http.MethodGet:
		return opGetALS, id, "", ""
	case http.MethodPatch:
		return opUpdateALS, id, "", ""
	case http.MethodDelete:
		return opDeleteALS, id, "", ""
	}

	return opUnknown, id, "", ""
}

// ------- Shared body/query extraction helpers -------

func extractTags(body map[string]any) map[string]string {
	tags := make(map[string]string)

	if t, ok := body["tags"].(map[string]any); ok {
		for k, v := range t {
			if s, ok2 := v.(string); ok2 {
				tags[k] = s
			}
		}
	}

	return tags
}

func bodyInt32(body map[string]any, key string) int32 {
	switch v := body[key].(type) {
	case float64:
		return int32(v)
	case int:
		return int32(v) //nolint:gosec // value bounded by JSON number range
	case int32:
		return v
	case int64:
		return int32(v) //nolint:gosec // value bounded by JSON number range
	}

	return 0
}

func queryInt32(c *echo.Context) int32 {
	v := c.QueryParam("maxResults")
	if v == "" {
		return 0
	}

	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0
	}

	return int32(n)
}
