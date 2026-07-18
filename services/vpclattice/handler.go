package vpclattice

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	matchPriority = service.PriorityPathVersioned

	pathServices                          = "/services"
	pathServiceNetworks                   = "/servicenetworks"
	pathServiceNetworkServiceAssociations = "/servicenetworkserviceassociations"
	pathServiceNetworkVpcAssociations     = "/servicenetworkvpcassociations"
	pathTargetGroups                      = "/targetgroups"
	pathAccessLogSubscriptions            = "/accesslogsubscriptions"
	pathAuthPolicy                        = "/authpolicy"
	pathResourcePolicy                    = "/resourcepolicy"
	pathTags                              = "/tags"

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
	keyNameRequired       = "name is required"

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
	}
}

// RouteMatcher returns a function that matches VPC Lattice API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return path == pathServices || strings.HasPrefix(path, pathServices+"/") ||
			path == pathServiceNetworks || strings.HasPrefix(path, pathServiceNetworks+"/") ||
			path == pathServiceNetworkServiceAssociations ||
			strings.HasPrefix(path, pathServiceNetworkServiceAssociations+"/") ||
			path == pathServiceNetworkVpcAssociations ||
			strings.HasPrefix(path, pathServiceNetworkVpcAssociations+"/") ||
			path == pathTargetGroups || strings.HasPrefix(path, pathTargetGroups+"/") ||
			path == pathAccessLogSubscriptions || strings.HasPrefix(path, pathAccessLogSubscriptions+"/") ||
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

// handleREST is a flat routing dispatch over every VPC Lattice operation;
// its size and branching are mechanical (one case per op, each a one-line
// delegation) rather than incidental complexity, so it is kept as a single
// function instead of being decomposed further.
func (h *Handler) handleREST( //nolint:gocyclo,cyclop,funlen // flat routing dispatch, see doc comment
	c *echo.Context,
) error {
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

	switch op {
	case opCreateService:
		return h.handleCreateService(c, body)
	case opGetService:
		return h.handleGetService(c, id1)
	case opUpdateService:
		return h.handleUpdateService(c, id1, body)
	case opDeleteService:
		return h.handleDeleteService(c, id1)
	case opListServices:
		return h.handleListServices(c)
	case opCreateSN:
		return h.handleCreateServiceNetwork(c, body)
	case opGetSN:
		return h.handleGetServiceNetwork(c, id1)
	case opUpdateSN:
		return h.handleUpdateServiceNetwork(c, id1, body)
	case opDeleteSN:
		return h.handleDeleteServiceNetwork(c, id1)
	case opListSNs:
		return h.handleListServiceNetworks(c)
	case opCreateSNSA:
		return h.handleCreateSNSA(c, body)
	case opGetSNSA:
		return h.handleGetSNSA(c, id1)
	case opDeleteSNSA:
		return h.handleDeleteSNSA(c, id1)
	case opListSNSAs:
		return h.handleListSNSAs(c)
	case opCreateSNVA:
		return h.handleCreateSNVA(c, body)
	case opGetSNVA:
		return h.handleGetSNVA(c, id1)
	case opUpdateSNVA:
		return h.handleUpdateSNVA(c, id1, body)
	case opDeleteSNVA:
		return h.handleDeleteSNVA(c, id1)
	case opListSNVAs:
		return h.handleListSNVAs(c)
	case opCreateListener:
		return h.handleCreateListener(c, id1, body)
	case opGetListener:
		return h.handleGetListener(c, id1, id2)
	case opUpdateListener:
		return h.handleUpdateListener(c, id1, id2, body)
	case opDeleteListener:
		return h.handleDeleteListener(c, id1, id2)
	case opListListeners:
		return h.handleListListeners(c, id1)
	case opCreateRule:
		return h.handleCreateRule(c, id1, id2, body)
	case opGetRule:
		return h.handleGetRule(c, id1, id2, id3)
	case opUpdateRule:
		return h.handleUpdateRule(c, id1, id2, id3, body)
	case opDeleteRule:
		return h.handleDeleteRule(c, id1, id2, id3)
	case opListRules:
		return h.handleListRules(c, id1, id2)
	case opBatchUpdateRule:
		return h.handleBatchUpdateRule(c, id1, id2, body)
	case opCreateTG:
		return h.handleCreateTargetGroup(c, body)
	case opGetTG:
		return h.handleGetTargetGroup(c, id1)
	case opUpdateTG:
		return h.handleUpdateTargetGroup(c, id1, body)
	case opDeleteTG:
		return h.handleDeleteTargetGroup(c, id1)
	case opListTGs:
		return h.handleListTargetGroups(c)
	case opRegisterTargets:
		return h.handleRegisterTargets(c, id1, body)
	case opDeregisterTargets:
		return h.handleDeregisterTargets(c, id1, body)
	case opListTargets:
		return h.handleListTargets(c, id1, body)
	case opCreateALS:
		return h.handleCreateALS(c, body)
	case opGetALS:
		return h.handleGetALS(c, id1)
	case opUpdateALS:
		return h.handleUpdateALS(c, id1, body)
	case opDeleteALS:
		return h.handleDeleteALS(c, id1)
	case opListALSs:
		return h.handleListALSs(c)
	case opPutAuthPolicy:
		return h.handlePutAuthPolicy(c, id1, body)
	case opGetAuthPolicy:
		return h.handleGetAuthPolicy(c, id1)
	case opDeleteAuthPolicy:
		return h.handleDeleteAuthPolicy(c, id1)
	case opPutResourcePolicy:
		return h.handlePutResourcePolicy(c, id1, body)
	case opGetResourcePolicy:
		return h.handleGetResourcePolicy(c, id1)
	case opDeleteResourcePolicy:
		return h.handleDeleteResourcePolicy(c, id1)
	case opTagResource:
		return h.handleTagResource(c, id1, body)
	case opUntagResource:
		return h.handleUntagResource(c, id1)
	case opListTagsForResource:
		return h.handleListTagsForResource(c, id1)
	default:
		return c.JSON(http.StatusNotFound, map[string]any{keyMessage: "unknown operation"})
	}
}

// handleError converts backend errors to HTTP responses.
func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound, map[string]any{keyMessage: err.Error()})
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusConflict, map[string]any{keyMessage: err.Error()})
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
}

// ------- Path classification -------

// classifyPath maps (method, path) → (op, id1, id2, id3).
// id1..id3 are path segments in order (service, listener, rule etc.).
//
// Like handleREST, this is a flat dispatch over path prefixes -- one case
// per resource collection -- so it is kept as a single function rather than
// decomposed further.
func classifyPath( //nolint:gocyclo,cyclop,funlen // flat routing dispatch, see doc comment
	method, path string,
) (string, string, string, string) {
	switch {
	case path == pathServices:
		if method == http.MethodPost {
			return opCreateService, "", "", ""
		}

		return opListServices, "", "", ""
	case strings.HasPrefix(path, pathServices+"/"):
		return classifyServicePath(method, path)

	case path == pathServiceNetworks:
		if method == http.MethodPost {
			return opCreateSN, "", "", ""
		}

		return opListSNs, "", "", ""
	case strings.HasPrefix(path, pathServiceNetworks+"/"):
		return classifyServiceNetworkPath(method, path)

	case path == pathServiceNetworkServiceAssociations:
		if method == http.MethodPost {
			return opCreateSNSA, "", "", ""
		}

		return opListSNSAs, "", "", ""
	case strings.HasPrefix(path, pathServiceNetworkServiceAssociations+"/"):
		return classifySNSAPath(method, path)

	case path == pathServiceNetworkVpcAssociations:
		if method == http.MethodPost {
			return opCreateSNVA, "", "", ""
		}

		return opListSNVAs, "", "", ""
	case strings.HasPrefix(path, pathServiceNetworkVpcAssociations+"/"):
		return classifySNVAPath(method, path)

	case path == pathTargetGroups:
		if method == http.MethodPost {
			return opCreateTG, "", "", ""
		}

		return opListTGs, "", "", ""
	case strings.HasPrefix(path, pathTargetGroups+"/"):
		return classifyTargetGroupPath(method, path)

	case path == pathAccessLogSubscriptions:
		if method == http.MethodPost {
			return opCreateALS, "", "", ""
		}

		return opListALSs, "", "", ""
	case strings.HasPrefix(path, pathAccessLogSubscriptions+"/"):
		return classifyALSPath(method, path)

	case strings.HasPrefix(path, pathAuthPolicy+"/"):
		resourceID := strings.TrimPrefix(path, pathAuthPolicy+"/")
		switch method {
		case http.MethodPut:
			return opPutAuthPolicy, resourceID, "", ""
		case http.MethodGet:
			return opGetAuthPolicy, resourceID, "", ""
		case http.MethodDelete:
			return opDeleteAuthPolicy, resourceID, "", ""
		}

	case strings.HasPrefix(path, pathResourcePolicy+"/"):
		resourceArn := strings.TrimPrefix(path, pathResourcePolicy+"/")
		switch method {
		case http.MethodPut:
			return opPutResourcePolicy, resourceArn, "", ""
		case http.MethodGet:
			return opGetResourcePolicy, resourceArn, "", ""
		case http.MethodDelete:
			return opDeleteResourcePolicy, resourceArn, "", ""
		}

	case strings.HasPrefix(path, pathTags+"/"):
		resourceArn := strings.TrimPrefix(path, pathTags+"/")
		switch method {
		case http.MethodPost:
			return opTagResource, resourceArn, "", ""
		case http.MethodDelete:
			return opUntagResource, resourceArn, "", ""
		case http.MethodGet:
			return opListTagsForResource, resourceArn, "", ""
		}
	}

	return opUnknown, "", "", ""
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
