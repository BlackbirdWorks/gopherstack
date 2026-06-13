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

	opBatchUpdateRule = "BatchUpdateRule"
	opCreateALS       = "CreateAccessLogSubscription"
	opCreateListener  = "CreateListener"
	opCreateRule      = "CreateRule"
	opCreateService   = "CreateService"
	opCreateSN        = "CreateServiceNetwork"
	opCreateSNSA      = "CreateServiceNetworkServiceAssociation"
	opCreateSNVA      = "CreateServiceNetworkVpcAssociation"
	opCreateTG        = "CreateTargetGroup"
	opDeleteALS       = "DeleteAccessLogSubscription"
	opDeleteAuthPolicy      = "DeleteAuthPolicy"
	opDeleteListener        = "DeleteListener"
	opDeleteResourcePolicy  = "DeleteResourcePolicy"
	opDeleteRule            = "DeleteRule"
	opDeleteService         = "DeleteService"
	opDeleteSN              = "DeleteServiceNetwork"
	opDeleteSNSA            = "DeleteServiceNetworkServiceAssociation"
	opDeleteSNVA            = "DeleteServiceNetworkVpcAssociation"
	opDeleteTG              = "DeleteTargetGroup"
	opDeregisterTargets     = "DeregisterTargets"
	opGetALS                = "GetAccessLogSubscription"
	opGetAuthPolicy         = "GetAuthPolicy"
	opGetListener           = "GetListener"
	opGetResourcePolicy     = "GetResourcePolicy"
	opGetRule               = "GetRule"
	opGetService            = "GetService"
	opGetSN                 = "GetServiceNetwork"
	opGetSNSA               = "GetServiceNetworkServiceAssociation"
	opGetSNVA               = "GetServiceNetworkVpcAssociation"
	opGetTG                 = "GetTargetGroup"
	opListALSs              = "ListAccessLogSubscriptions"
	opListListeners         = "ListListeners"
	opListRules             = "ListRules"
	opListSNSAs             = "ListServiceNetworkServiceAssociations"
	opListSNVAs             = "ListServiceNetworkVpcAssociations"
	opListSNs               = "ListServiceNetworks"
	opListServices          = "ListServices"
	opListTagsForResource   = "ListTagsForResource"
	opListTGs               = "ListTargetGroups"
	opListTargets           = "ListTargets"
	opPutAuthPolicy         = "PutAuthPolicy"
	opPutResourcePolicy     = "PutResourcePolicy"
	opRegisterTargets       = "RegisterTargets"
	opTagResource           = "TagResource"
	opUntagResource         = "UntagResource"
	opUpdateALS             = "UpdateAccessLogSubscription"
	opUpdateListener        = "UpdateListener"
	opUpdateRule            = "UpdateRule"
	opUpdateService         = "UpdateService"
	opUpdateSN              = "UpdateServiceNetwork"
	opUpdateSNVA            = "UpdateServiceNetworkVpcAssociation"
	opUpdateTG              = "UpdateTargetGroup"
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

func (h *Handler) handleREST(c *echo.Context) error { //nolint:gocognit,gocyclo // large routing dispatch is expected
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

// ------- Service handlers -------

func (h *Handler) handleCreateService(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	if name == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "name is required"})
	}

	authType, _ := body["authType"].(string)
	certArn, _ := body["certificateArn"].(string)
	customDomain, _ := body["customDomainName"].(string)
	tags := extractTags(body)

	svc, err := h.Backend.CreateService(name, authType, certArn, customDomain, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, serviceToJSON(svc))
}

func (h *Handler) handleGetService(c *echo.Context, id string) error {
	svc, err := h.Backend.GetService(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, serviceToJSON(svc))
}

func (h *Handler) handleUpdateService(c *echo.Context, id string, body map[string]any) error {
	authType, _ := body["authType"].(string)
	certArn, _ := body["certificateArn"].(string)

	svc, err := h.Backend.UpdateService(id, authType, certArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, serviceToJSON(svc))
}

func (h *Handler) handleDeleteService(c *echo.Context, id string) error {
	svc, err := h.Backend.DeleteService(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, serviceToJSON(svc))
}

func (h *Handler) handleListServices(c *echo.Context) error {
	maxResults := queryInt32(c, 0)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListServices(maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, serviceSummaryToJSON(s))
	}

	resp := map[string]any{"items": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- ServiceNetwork handlers -------

func (h *Handler) handleCreateServiceNetwork(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	if name == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "name is required"})
	}

	authType, _ := body["authType"].(string)
	tags := extractTags(body)

	sn, err := h.Backend.CreateServiceNetwork(name, authType, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, serviceNetworkToJSON(sn))
}

func (h *Handler) handleGetServiceNetwork(c *echo.Context, id string) error {
	sn, err := h.Backend.GetServiceNetwork(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, serviceNetworkToJSON(sn))
}

func (h *Handler) handleUpdateServiceNetwork(
	c *echo.Context,
	id string,
	body map[string]any,
) error {
	authType, _ := body["authType"].(string)

	sn, err := h.Backend.UpdateServiceNetwork(id, authType)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, serviceNetworkToJSON(sn))
}

func (h *Handler) handleDeleteServiceNetwork(c *echo.Context, id string) error {
	if err := h.Backend.DeleteServiceNetwork(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListServiceNetworks(c *echo.Context) error {
	maxResults := queryInt32(c, 0)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListServiceNetworks(maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, serviceNetworkSummaryToJSON(s))
	}

	resp := map[string]any{"items": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- SNSA handlers -------

func (h *Handler) handleCreateSNSA(c *echo.Context, body map[string]any) error {
	snID, _ := body["serviceNetworkIdentifier"].(string)
	svcID, _ := body["serviceIdentifier"].(string)

	if snID == "" || svcID == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]any{
				keyMessage: "serviceNetworkIdentifier and serviceIdentifier are required",
			},
		)
	}

	tags := extractTags(body)

	assoc, err := h.Backend.CreateServiceNetworkServiceAssociation(snID, svcID, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, snsaToJSON(assoc))
}

func (h *Handler) handleGetSNSA(c *echo.Context, id string) error {
	assoc, err := h.Backend.GetServiceNetworkServiceAssociation(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, snsaToJSON(assoc))
}

func (h *Handler) handleDeleteSNSA(c *echo.Context, id string) error {
	if err := h.Backend.DeleteServiceNetworkServiceAssociation(id); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"status": "DELETE_IN_PROGRESS"})
}

func (h *Handler) handleListSNSAs(c *echo.Context) error {
	maxResults := queryInt32(c, 0)
	nextToken := c.QueryParam("nextToken")
	snID := c.QueryParam("serviceNetworkIdentifier")
	svcID := c.QueryParam("serviceIdentifier")

	items, next, err := h.Backend.ListServiceNetworkServiceAssociations(
		snID,
		svcID,
		maxResults,
		nextToken,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, snsaSummaryToJSON(s))
	}

	resp := map[string]any{"items": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- SNVA handlers -------

func (h *Handler) handleCreateSNVA(c *echo.Context, body map[string]any) error {
	snID, _ := body["serviceNetworkIdentifier"].(string)
	vpcID, _ := body["vpcIdentifier"].(string)

	if snID == "" || vpcID == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]any{keyMessage: "serviceNetworkIdentifier and vpcIdentifier are required"},
		)
	}

	var sgs []string
	if sgRaw, ok := body["securityGroupIds"].([]any); ok {
		for _, v := range sgRaw {
			if s, ok2 := v.(string); ok2 {
				sgs = append(sgs, s)
			}
		}
	}

	tags := extractTags(body)

	assoc, err := h.Backend.CreateServiceNetworkVpcAssociation(snID, vpcID, sgs, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, snvaToJSON(assoc))
}

func (h *Handler) handleGetSNVA(c *echo.Context, id string) error {
	assoc, err := h.Backend.GetServiceNetworkVpcAssociation(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, snvaToJSON(assoc))
}

func (h *Handler) handleUpdateSNVA(c *echo.Context, id string, body map[string]any) error {
	var sgs []string
	if sgRaw, ok := body["securityGroupIds"].([]any); ok {
		for _, v := range sgRaw {
			if s, ok2 := v.(string); ok2 {
				sgs = append(sgs, s)
			}
		}
	}

	assoc, err := h.Backend.UpdateServiceNetworkVpcAssociation(id, sgs)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, snvaToJSON(assoc))
}

func (h *Handler) handleDeleteSNVA(c *echo.Context, id string) error {
	if err := h.Backend.DeleteServiceNetworkVpcAssociation(id); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"status": "DELETE_IN_PROGRESS"})
}

func (h *Handler) handleListSNVAs(c *echo.Context) error {
	maxResults := queryInt32(c, 0)
	nextToken := c.QueryParam("nextToken")
	snID := c.QueryParam("serviceNetworkIdentifier")
	vpcID := c.QueryParam("vpcIdentifier")

	items, next, err := h.Backend.ListServiceNetworkVpcAssociations(
		snID,
		vpcID,
		maxResults,
		nextToken,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, snvaSummaryToJSON(s))
	}

	resp := map[string]any{"items": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- Listener handlers -------

func (h *Handler) handleCreateListener(
	c *echo.Context,
	serviceID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	protocol, _ := body["protocol"].(string)

	if name == "" || protocol == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]any{keyMessage: "name and protocol are required"},
		)
	}

	port := bodyInt32(body, "port")
	defaultAction := extractRuleAction(body, "defaultAction")
	tags := extractTags(body)

	l, err := h.Backend.CreateListener(serviceID, name, protocol, port, defaultAction, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, listenerToJSON(l))
}

func (h *Handler) handleGetListener(c *echo.Context, serviceID, listenerID string) error {
	l, err := h.Backend.GetListener(serviceID, listenerID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, listenerToJSON(l))
}

func (h *Handler) handleUpdateListener(
	c *echo.Context,
	serviceID, listenerID string,
	body map[string]any,
) error {
	defaultAction := extractRuleAction(body, "defaultAction")

	l, err := h.Backend.UpdateListener(serviceID, listenerID, defaultAction)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, listenerToJSON(l))
}

func (h *Handler) handleDeleteListener(c *echo.Context, serviceID, listenerID string) error {
	if err := h.Backend.DeleteListener(serviceID, listenerID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListListeners(c *echo.Context, serviceID string) error {
	maxResults := queryInt32(c, 0)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListListeners(serviceID, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, l := range items {
		summaries = append(summaries, listenerSummaryToJSON(l))
	}

	resp := map[string]any{"items": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- Rule handlers -------

func (h *Handler) handleCreateRule(
	c *echo.Context,
	serviceID, listenerID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	if name == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "name is required"})
	}

	priority := bodyInt32(body, "priority")
	action := extractRuleAction(body, "action")
	match := extractRuleMatch(body, "match")
	tags := extractTags(body)

	r, err := h.Backend.CreateRule(serviceID, listenerID, name, priority, action, match, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, ruleToJSON(r))
}

func (h *Handler) handleGetRule(c *echo.Context, serviceID, listenerID, ruleID string) error {
	r, err := h.Backend.GetRule(serviceID, listenerID, ruleID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, ruleToJSON(r))
}

func (h *Handler) handleUpdateRule(
	c *echo.Context,
	serviceID, listenerID, ruleID string,
	body map[string]any,
) error {
	priority := bodyInt32(body, "priority")
	action := extractRuleAction(body, "action")
	match := extractRuleMatch(body, "match")

	r, err := h.Backend.UpdateRule(serviceID, listenerID, ruleID, priority, action, match)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, ruleToJSON(r))
}

func (h *Handler) handleDeleteRule(c *echo.Context, serviceID, listenerID, ruleID string) error {
	if err := h.Backend.DeleteRule(serviceID, listenerID, ruleID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListRules(c *echo.Context, serviceID, listenerID string) error {
	maxResults := queryInt32(c, 0)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListRules(serviceID, listenerID, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, r := range items {
		summaries = append(summaries, ruleSummaryToJSON(r))
	}

	resp := map[string]any{"items": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleBatchUpdateRule(
	c *echo.Context,
	serviceID, listenerID string,
	body map[string]any,
) error {
	var updates []*RuleUpdate

	if rawUpdates, ok := body["rules"].([]any); ok {
		for _, raw := range rawUpdates {
			if m, ok2 := raw.(map[string]any); ok2 {
				u := &RuleUpdate{}
				u.RuleIdentifier, _ = m["ruleIdentifier"].(string)
				u.Priority = bodyInt32(m, "priority")
				u.Action = extractRuleAction(m, "action")
				u.Match = extractRuleMatch(m, "match")
				updates = append(updates, u)
			}
		}
	}

	successes, failures, err := h.Backend.BatchUpdateRule(serviceID, listenerID, updates)
	if err != nil {
		return h.handleError(c, err)
	}

	successList := make([]any, 0, len(successes))
	for _, s := range successes {
		successList = append(successList, ruleUpdateSuccessToJSON(s))
	}

	failureList := make([]any, 0, len(failures))
	for _, f := range failures {
		failureList = append(failureList, map[string]any{
			"ruleIdentifier": f.RuleIdentifier,
			"message":        f.Message,
			"code":           f.Code,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"successful":   successList,
		"unsuccessful": failureList,
	})
}

// ------- TargetGroup handlers -------

func (h *Handler) handleCreateTargetGroup(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	tgType, _ := body["type"].(string)

	if name == "" || tgType == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]any{keyMessage: "name and type are required"},
		)
	}

	config := extractTargetGroupConfig(body)
	tags := extractTags(body)

	tg, err := h.Backend.CreateTargetGroup(name, tgType, config, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, targetGroupToJSON(tg))
}

func (h *Handler) handleGetTargetGroup(c *echo.Context, id string) error {
	tg, err := h.Backend.GetTargetGroup(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, targetGroupToJSON(tg))
}

func (h *Handler) handleUpdateTargetGroup(c *echo.Context, id string, body map[string]any) error {
	var healthCheck *HealthCheckConfig
	if hcRaw, ok := body["healthCheck"].(map[string]any); ok {
		healthCheck = extractHealthCheckConfig(hcRaw)
	}

	tg, err := h.Backend.UpdateTargetGroup(id, healthCheck)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, targetGroupToJSON(tg))
}

func (h *Handler) handleDeleteTargetGroup(c *echo.Context, id string) error {
	if err := h.Backend.DeleteTargetGroup(id); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": id, "status": "DELETE_IN_PROGRESS"})
}

func (h *Handler) handleListTargetGroups(c *echo.Context) error {
	maxResults := queryInt32(c, 0)
	nextToken := c.QueryParam("nextToken")
	tgType := c.QueryParam("targetGroupType")
	svcArn := c.QueryParam("serviceArn")

	items, next, err := h.Backend.ListTargetGroups(tgType, svcArn, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, tg := range items {
		summaries = append(summaries, targetGroupSummaryToJSON(tg))
	}

	resp := map[string]any{"items": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleRegisterTargets(c *echo.Context, tgID string, body map[string]any) error {
	targets := extractTargets(body)

	failures, err := h.Backend.RegisterTargets(tgID, targets)
	if err != nil {
		return h.handleError(c, err)
	}

	failureList := make([]any, 0, len(failures))
	for _, f := range failures {
		failureList = append(failureList, targetFailureToJSON(f))
	}

	return c.JSON(http.StatusOK, map[string]any{"unsuccessful": failureList})
}

func (h *Handler) handleDeregisterTargets(c *echo.Context, tgID string, body map[string]any) error {
	targets := extractTargets(body)

	failures, err := h.Backend.DeregisterTargets(tgID, targets)
	if err != nil {
		return h.handleError(c, err)
	}

	failureList := make([]any, 0, len(failures))
	for _, f := range failures {
		failureList = append(failureList, targetFailureToJSON(f))
	}

	return c.JSON(http.StatusOK, map[string]any{"unsuccessful": failureList})
}

func (h *Handler) handleListTargets(c *echo.Context, tgID string, _ map[string]any) error {
	maxResults := queryInt32(c, 0)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListTargets(tgID, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, t := range items {
		summaries = append(summaries, targetSummaryToJSON(t))
	}

	resp := map[string]any{"items": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- AccessLogSubscription handlers -------

func (h *Handler) handleCreateALS(c *echo.Context, body map[string]any) error {
	resourceID, _ := body["resourceIdentifier"].(string)
	destArn, _ := body["destinationArn"].(string)
	logType, _ := body["serviceNetworkLogType"].(string)

	if resourceID == "" || destArn == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]any{keyMessage: "resourceIdentifier and destinationArn are required"},
		)
	}

	tags := extractTags(body)

	als, err := h.Backend.CreateAccessLogSubscription(resourceID, destArn, logType, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, alsToJSON(als))
}

func (h *Handler) handleGetALS(c *echo.Context, id string) error {
	als, err := h.Backend.GetAccessLogSubscription(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, alsToJSON(als))
}

func (h *Handler) handleUpdateALS(c *echo.Context, id string, body map[string]any) error {
	destArn, _ := body["destinationArn"].(string)

	als, err := h.Backend.UpdateAccessLogSubscription(id, destArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, alsToJSON(als))
}

func (h *Handler) handleDeleteALS(c *echo.Context, id string) error {
	if err := h.Backend.DeleteAccessLogSubscription(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListALSs(c *echo.Context) error {
	maxResults := queryInt32(c, 0)
	nextToken := c.QueryParam("nextToken")
	resourceID := c.QueryParam("resourceIdentifier")

	items, next, err := h.Backend.ListAccessLogSubscriptions(resourceID, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, a := range items {
		summaries = append(summaries, alsSummaryToJSON(a))
	}

	resp := map[string]any{"items": summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- Auth/Resource Policy handlers -------

func (h *Handler) handlePutAuthPolicy(
	c *echo.Context,
	resourceID string,
	body map[string]any,
) error {
	policy, _ := body["policy"].(string)

	ap, err := h.Backend.PutAuthPolicy(resourceID, policy)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"policy": ap.Policy,
		"state":  ap.State,
	})
}

func (h *Handler) handleGetAuthPolicy(c *echo.Context, resourceID string) error {
	ap, err := h.Backend.GetAuthPolicy(resourceID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"policy": ap.Policy,
		"state":  ap.State,
	})
}

func (h *Handler) handleDeleteAuthPolicy(c *echo.Context, resourceID string) error {
	if err := h.Backend.DeleteAuthPolicy(resourceID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handlePutResourcePolicy(
	c *echo.Context,
	resourceArn string,
	body map[string]any,
) error {
	policy, _ := body["policy"].(string)

	if err := h.Backend.PutResourcePolicy(resourceArn, policy); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetResourcePolicy(c *echo.Context, resourceArn string) error {
	policy, err := h.Backend.GetResourcePolicy(resourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"policy": policy})
}

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context, resourceArn string) error {
	if err := h.Backend.DeleteResourcePolicy(resourceArn); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ------- Tagging handlers -------

func (h *Handler) handleTagResource(
	c *echo.Context,
	resourceArn string,
	body map[string]any,
) error {
	tags := make(map[string]string)
	if t, ok := body["tags"].(map[string]any); ok {
		for k, v := range t {
			if s, ok2 := v.(string); ok2 {
				tags[k] = s
			}
		}
	}

	if err := h.Backend.TagResource(resourceArn, tags); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string) error {
	keys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, keys); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceArn string) error {
	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
}

// ------- Path classification -------

// classifyPath maps (method, path) → (op, id1, id2, id3).
// id1..id3 are path segments in order (service, listener, rule etc.).
func classifyPath( //nolint:gocognit,gocyclo // large routing dispatch is expected
	method, path string,
) (op, id1, id2, id3 string) { //nolint:nonamedreturns // path dispatch needs named returns for clarity
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
func classifyServicePath( //nolint:gocognit,gocyclo // large routing dispatch is expected
	method, path string,
) (op, id1, id2, id3 string) { //nolint:nonamedreturns // path dispatch needs named returns for clarity
	rest := strings.TrimPrefix(path, pathServices+"/")
	serviceID, sub, hasSub := strings.Cut(rest, "/")

	if !hasSub {
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

	// sub = listeners[/{listenerID}[/rules[/{ruleID}]]]
	if sub == "listeners" {
		if method == http.MethodPost {
			return opCreateListener, serviceID, "", ""
		}

		return opListListeners, serviceID, "", ""
	}

	if listenerRest, ok := strings.CutPrefix(sub, "listeners/"); ok {
		listenerID, listenerSub, hasListenerSub := strings.Cut(listenerRest, "/")

		if !hasListenerSub {
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

		if listenerSub == "rules" {
			if method == http.MethodPost {
				return opCreateRule, serviceID, listenerID, ""
			}

			if method == http.MethodPatch {
				return opBatchUpdateRule, serviceID, listenerID, ""
			}

			return opListRules, serviceID, listenerID, ""
		}

		if ruleID, ok2 := strings.CutPrefix(listenerSub, "rules/"); ok2 {
			switch method {
			case http.MethodGet:
				return opGetRule, serviceID, listenerID, ruleID
			case http.MethodPatch:
				return opUpdateRule, serviceID, listenerID, ruleID
			case http.MethodDelete:
				return opDeleteRule, serviceID, listenerID, ruleID
			}
		}
	}

	return opUnknown, serviceID, "", ""
}

// classifyServiceNetworkPath handles /servicenetworks/{id}.
func classifyServiceNetworkPath(
	method, path string,
) (op, id1, id2, id3 string) { //nolint:nonamedreturns // path dispatch needs named returns for clarity
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
) (op, id1, id2, id3 string) { //nolint:nonamedreturns // path dispatch needs named returns for clarity
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
) (op, id1, id2, id3 string) { //nolint:nonamedreturns // path dispatch needs named returns for clarity
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
) (op, id1, id2, id3 string) { //nolint:nonamedreturns // path dispatch needs named returns for clarity
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
) (op, id1, id2, id3 string) { //nolint:nonamedreturns // path dispatch needs named returns for clarity
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

// ------- JSON serialization helpers -------

func serviceToJSON(s *Service) map[string]any {
	m := map[string]any{
		"arn":           s.ARN,
		"id":            s.ID,
		"name":          s.Name,
		"authType":      s.AuthType,
		"status":        s.Status,
		"createdAt":     s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		"lastUpdatedAt": s.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
		"dnsEntry":      map[string]any{"domainName": s.DNSName},
	}

	if s.CertificateArn != "" {
		m["certificateArn"] = s.CertificateArn
	}

	if s.CustomDomainName != "" {
		m["customDomainName"] = s.CustomDomainName
	}

	return m
}

func serviceSummaryToJSON(s *ServiceSummary) map[string]any {
	m := map[string]any{
		"arn":       s.ARN,
		"id":        s.ID,
		"name":      s.Name,
		"status":    s.Status,
		"createdAt": s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if s.DNSName != "" {
		m["dnsEntry"] = map[string]any{"domainName": s.DNSName}
	}

	if s.CustomDomainName != "" {
		m["customDomainName"] = s.CustomDomainName
	}

	return m
}

func serviceNetworkToJSON(s *ServiceNetwork) map[string]any {
	return map[string]any{
		"arn":                        s.ARN,
		"id":                         s.ID,
		"name":                       s.Name,
		"authType":                   s.AuthType,
		"numberOfAssociatedServices": s.NumberOfAssociatedServices,
		"numberOfAssociatedVPCs":     s.NumberOfAssociatedVPCs,
		"createdAt":                  s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		"lastUpdatedAt":              s.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func serviceNetworkSummaryToJSON(s *ServiceNetworkSummary) map[string]any {
	return map[string]any{
		"arn":                        s.ARN,
		"id":                         s.ID,
		"name":                       s.Name,
		"numberOfAssociatedServices": s.NumberOfAssociatedServices,
		"numberOfAssociatedVPCs":     s.NumberOfAssociatedVPCs,
		"createdAt":                  s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func snsaToJSON(s *ServiceNetworkServiceAssociation) map[string]any {
	return map[string]any{
		"arn":                s.ARN,
		"id":                 s.ID,
		"serviceArn":         s.ServiceARN,
		"serviceId":          s.ServiceID,
		"serviceName":        s.ServiceName,
		"serviceNetworkArn":  s.ServiceNetworkARN,
		"serviceNetworkId":   s.ServiceNetworkID,
		"serviceNetworkName": s.ServiceNetworkName,
		"status":             s.Status,
		"createdBy":          s.CreatedBy,
		"createdAt":          s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func snsaSummaryToJSON(s *ServiceNetworkServiceAssociationSummary) map[string]any {
	return map[string]any{
		"arn":                s.ARN,
		"id":                 s.ID,
		"serviceArn":         s.ServiceARN,
		"serviceId":          s.ServiceID,
		"serviceName":        s.ServiceName,
		"serviceNetworkArn":  s.ServiceNetworkARN,
		"serviceNetworkId":   s.ServiceNetworkID,
		"serviceNetworkName": s.ServiceNetworkName,
		"status":             s.Status,
		"createdAt":          s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func snvaToJSON(s *ServiceNetworkVpcAssociation) map[string]any {
	sgs := make([]string, len(s.SecurityGroupIDs))
	copy(sgs, s.SecurityGroupIDs)

	return map[string]any{
		"arn":                s.ARN,
		"id":                 s.ID,
		"vpcId":              s.VpcID,
		"serviceNetworkArn":  s.ServiceNetworkARN,
		"serviceNetworkId":   s.ServiceNetworkID,
		"serviceNetworkName": s.ServiceNetworkName,
		"securityGroupIds":   sgs,
		"status":             s.Status,
		"createdBy":          s.CreatedBy,
		"createdAt":          s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		"lastUpdatedAt":      s.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func snvaSummaryToJSON(s *ServiceNetworkVpcAssociationSummary) map[string]any {
	return map[string]any{
		"arn":                s.ARN,
		"id":                 s.ID,
		"vpcId":              s.VpcID,
		"serviceNetworkArn":  s.ServiceNetworkARN,
		"serviceNetworkId":   s.ServiceNetworkID,
		"serviceNetworkName": s.ServiceNetworkName,
		"status":             s.Status,
		"createdAt":          s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func listenerToJSON(l *Listener) map[string]any {
	m := map[string]any{
		"arn":           l.ARN,
		"id":            l.ID,
		"serviceArn":    l.ServiceARN,
		"serviceId":     l.ServiceID,
		"name":          l.Name,
		"protocol":      l.Protocol,
		"port":          l.Port,
		"createdAt":     l.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		"lastUpdatedAt": l.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if l.DefaultAction != nil {
		m["defaultAction"] = ruleActionToJSON(l.DefaultAction)
	}

	return m
}

func listenerSummaryToJSON(l *ListenerSummary) map[string]any {
	return map[string]any{
		"arn":           l.ARN,
		"id":            l.ID,
		"name":          l.Name,
		"protocol":      l.Protocol,
		"port":          l.Port,
		"createdAt":     l.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		"lastUpdatedAt": l.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func ruleToJSON(r *Rule) map[string]any {
	m := map[string]any{
		"arn":           r.ARN,
		"id":            r.ID,
		"name":          r.Name,
		"priority":      r.Priority,
		"isDefault":     r.IsDefault,
		"createdAt":     r.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		"lastUpdatedAt": r.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if r.Action != nil {
		m["action"] = ruleActionToJSON(r.Action)
	}

	if r.Match != nil {
		m["match"] = ruleMatchToJSON(r.Match)
	}

	return m
}

func ruleSummaryToJSON(r *RuleSummary) map[string]any {
	return map[string]any{
		"arn":       r.ARN,
		"id":        r.ID,
		"name":      r.Name,
		"priority":  r.Priority,
		"isDefault": r.IsDefault,
	}
}

func ruleUpdateSuccessToJSON(r *RuleUpdateSuccess) map[string]any {
	m := map[string]any{
		"arn":       r.ARN,
		"id":        r.ID,
		"name":      r.Name,
		"priority":  r.Priority,
		"isDefault": r.IsDefault,
	}

	if r.Action != nil {
		m["action"] = ruleActionToJSON(r.Action)
	}

	if r.Match != nil {
		m["match"] = ruleMatchToJSON(r.Match)
	}

	return m
}

func ruleActionToJSON(a *RuleAction) map[string]any {
	if a.IsFixedResponse {
		return map[string]any{
			"fixedResponse": map[string]any{
				"statusCode": a.FixedResponseStatusCode,
			},
		}
	}

	wts := make([]any, 0, len(a.ForwardTargetGroups))
	for _, w := range a.ForwardTargetGroups {
		wts = append(wts, map[string]any{
			"targetGroupIdentifier": w.TargetGroupID,
			"weight":                w.Weight,
		})
	}

	return map[string]any{
		"forward": map[string]any{
			"targetGroups": wts,
		},
	}
}

func ruleMatchToJSON(m *RuleMatch) map[string]any {
	if m == nil {
		return nil
	}

	httpMatch := map[string]any{}
	if m.HTTPMethod != "" {
		httpMatch["method"] = m.HTTPMethod
	}

	if m.PathMatchType != "" {
		pathMatch := map[string]any{
			"match": map[string]any{
				m.PathMatchType: m.PathMatchValue,
			},
		}
		httpMatch["path"] = pathMatch
	}

	if len(m.HeaderMatches) > 0 {
		headers := make([]any, 0, len(m.HeaderMatches))
		for _, h := range m.HeaderMatches {
			headers = append(headers, map[string]any{
				"name":  h.Name,
				"match": map[string]any{h.MatchType: h.MatchValue},
			})
		}

		httpMatch["headerMatches"] = headers
	}

	return map[string]any{"httpMatch": httpMatch}
}

func targetGroupToJSON(tg *TargetGroup) map[string]any {
	m := map[string]any{
		"arn":           tg.ARN,
		"id":            tg.ID,
		"name":          tg.Name,
		"type":          tg.Type,
		"status":        tg.Status,
		"serviceArns":   tg.ServiceARNs,
		"createdAt":     tg.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		"lastUpdatedAt": tg.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if tg.Config != nil {
		m["config"] = targetGroupConfigToJSON(tg.Config)
	}

	return m
}

func targetGroupSummaryToJSON(tg *TargetGroupSummary) map[string]any {
	return map[string]any{
		"arn":         tg.ARN,
		"id":          tg.ID,
		"name":        tg.Name,
		"type":        tg.Type,
		"status":      tg.Status,
		"port":        tg.Port,
		"protocol":    tg.Protocol,
		"vpcId":       tg.VpcID,
		"serviceArns": tg.ServiceARNs,
		"createdAt":   tg.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func targetGroupConfigToJSON(c *TargetGroupConfig) map[string]any {
	m := map[string]any{
		"port":            c.Port,
		"protocol":        c.Protocol,
		"protocolVersion": c.ProtocolVersion,
		"vpcIdentifier":   c.VpcID,
	}

	if c.HealthCheck != nil {
		m["healthCheck"] = healthCheckToJSON(c.HealthCheck)
	}

	return m
}

func healthCheckToJSON(hc *HealthCheckConfig) map[string]any {
	return map[string]any{
		"enabled":                    hc.Enabled,
		"protocol":                   hc.Protocol,
		"path":                       hc.Path,
		"port":                       hc.Port,
		"healthyThresholdCount":      hc.HealthyThresholdCount,
		"unhealthyThresholdCount":    hc.UnhealthyThresholdCount,
		"healthCheckIntervalSeconds": hc.HealthCheckIntervalSeconds,
		"healthCheckTimeoutSeconds":  hc.HealthCheckTimeoutSeconds,
	}
}

func targetSummaryToJSON(t *TargetSummary) map[string]any {
	return map[string]any{
		"id":         t.ID,
		"port":       t.Port,
		"status":     t.Status,
		"reasonCode": t.ReasonCode,
	}
}

func targetFailureToJSON(f *TargetFailure) map[string]any {
	return map[string]any{
		"id":      f.ID,
		"port":    f.Port,
		"code":    f.Code,
		"message": f.Message,
	}
}

func alsToJSON(a *AccessLogSubscription) map[string]any {
	return map[string]any{
		"arn":            a.ARN,
		"id":             a.ID,
		"resourceArn":    a.ResourceARN,
		"resourceId":     a.ResourceID,
		"destinationArn": a.DestinationARN,
		"createdAt":      a.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		"lastUpdatedAt":  a.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func alsSummaryToJSON(a *AccessLogSubscriptionSummary) map[string]any {
	return map[string]any{
		"arn":            a.ARN,
		"id":             a.ID,
		"resourceArn":    a.ResourceARN,
		"resourceId":     a.ResourceID,
		"destinationArn": a.DestinationARN,
		"createdAt":      a.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

// ------- Body extraction helpers -------

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
		return int32(v) //nolint:gosec // value is bounded by JSON number range
	case int:
		return int32(v) //nolint:gosec // value is bounded, overflow not possible
	case int32:
		return v
	case int64:
		return int32(v) //nolint:gosec // value is bounded, overflow not possible
	}

	return 0
}

func extractRuleAction(body map[string]any, key string) *RuleAction {
	raw, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}

	action := &RuleAction{}

	if fr, ok2 := raw["fixedResponse"].(map[string]any); ok2 {
		action.IsFixedResponse = true
		action.FixedResponseStatusCode = bodyInt32(fr, "statusCode")

		return action
	}

	if fwd, ok2 := raw["forward"].(map[string]any); ok2 {
		if tgs, ok3 := fwd["targetGroups"].([]any); ok3 {
			for _, tgRaw := range tgs {
				if tgMap, ok4 := tgRaw.(map[string]any); ok4 {
					wt := &WeightedTargetGroup{}
					wt.TargetGroupID, _ = tgMap["targetGroupIdentifier"].(string)
					wt.Weight = bodyInt32(tgMap, "weight")
					action.ForwardTargetGroups = append(action.ForwardTargetGroups, wt)
				}
			}
		}
	}

	return action
}

func extractRuleMatch(body map[string]any, key string) *RuleMatch { //nolint:gocognit // nested JSON extraction
	raw, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}

	match := &RuleMatch{}

	if httpMatch, ok2 := raw["httpMatch"].(map[string]any); ok2 {
		match.HTTPMethod, _ = httpMatch["method"].(string)

		if pathRaw, ok3 := httpMatch["path"].(map[string]any); ok3 {
			if matchRaw, ok4 := pathRaw["match"].(map[string]any); ok4 {
				for k, v := range matchRaw {
					if s, ok5 := v.(string); ok5 {
						match.PathMatchType = k
						match.PathMatchValue = s
					}
				}
			}
		}

		if headersRaw, ok3 := httpMatch["headerMatches"].([]any); ok3 {
			for _, hRaw := range headersRaw {
				if hMap, ok4 := hRaw.(map[string]any); ok4 {
					hm := &HeaderMatch{}
					hm.Name, _ = hMap["name"].(string)
					if matchRaw, ok5 := hMap["match"].(map[string]any); ok5 {
						for k, v := range matchRaw {
							if s, ok6 := v.(string); ok6 {
								hm.MatchType = k
								hm.MatchValue = s
							}
						}
					}

					match.HeaderMatches = append(match.HeaderMatches, hm)
				}
			}
		}
	}

	return match
}

func extractTargetGroupConfig(body map[string]any) *TargetGroupConfig {
	raw, ok := body["config"].(map[string]any)
	if !ok {
		return nil
	}

	cfg := &TargetGroupConfig{}
	cfg.Port = bodyInt32(raw, "port")
	cfg.Protocol, _ = raw["protocol"].(string)
	cfg.ProtocolVersion, _ = raw["protocolVersion"].(string)
	cfg.VpcID, _ = raw["vpcIdentifier"].(string)
	cfg.IPAddressType, _ = raw["ipAddressType"].(string)
	cfg.LambdaEventStructureVersion, _ = raw["lambdaEventStructureVersion"].(string)

	if hcRaw, ok2 := raw["healthCheck"].(map[string]any); ok2 {
		cfg.HealthCheck = extractHealthCheckConfig(hcRaw)
	}

	return cfg
}

func extractHealthCheckConfig(raw map[string]any) *HealthCheckConfig {
	hc := &HealthCheckConfig{}
	if v, ok := raw["enabled"].(bool); ok {
		hc.Enabled = v
	}

	hc.Protocol, _ = raw["protocol"].(string)
	hc.ProtocolVersion, _ = raw["protocolVersion"].(string)
	hc.Path, _ = raw["path"].(string)
	hc.Port = bodyInt32(raw, "port")
	hc.HealthyThresholdCount = bodyInt32(raw, "healthyThresholdCount")
	hc.UnhealthyThresholdCount = bodyInt32(raw, "unhealthyThresholdCount")
	hc.HealthCheckIntervalSeconds = bodyInt32(raw, "healthCheckIntervalSeconds")
	hc.HealthCheckTimeoutSeconds = bodyInt32(raw, "healthCheckTimeoutSeconds")

	return hc
}

func extractTargets(body map[string]any) []*Target {
	var targets []*Target

	if raw, ok := body["targets"].([]any); ok {
		for _, tRaw := range raw {
			if tMap, ok2 := tRaw.(map[string]any); ok2 {
				t := &Target{}
				t.ID, _ = tMap["id"].(string)
				t.Port = bodyInt32(tMap, "port")
				targets = append(targets, t)
			}
		}
	}

	return targets
}

func queryInt32(c *echo.Context, fallback int32) int32 {
	v := c.QueryParam("maxResults")
	if v == "" {
		return fallback
	}

	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return fallback
	}

	return int32(n) //nolint:gosec // value is bounded by ParseInt with bitSize=32
}
