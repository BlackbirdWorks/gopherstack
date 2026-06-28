package appmesh

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	appmeshPathPrefix = "/v20190125/"

	pathSegMeshes         = "meshes"
	pathSegVirtualNodes   = "virtualNodes"
	pathSegVirtualRouters = "virtualRouters"
	// Route paths use singular "virtualRouter" (AWS API quirk).
	pathSegVirtualRouter = "virtualRouter"
	pathSegRoutes        = "routes"
	pathSegVirtualSvcs   = "virtualServices"
	pathSegVirtualGWs    = "virtualGateways"
	// Gateway route paths use singular "virtualGateway" (AWS API quirk).
	pathSegVirtualGW     = "virtualGateway"
	pathSegGatewayRoutes = "gatewayRoutes"
	pathSegTags          = "tags"
	pathSegTag           = "tag"
	pathSegUntag         = "untag"

	keyCode    = "code"
	keyMessage = "message"

	defaultMaxResults = 100

	keyMesh               = "mesh"
	keyVirtualNode        = "virtualNode"
	keyVirtualRouter      = "virtualRouter"
	keyVirtualGateway     = "virtualGateway"
	keyRoute              = "route"
	keyVirtualService     = "virtualService"
	keyGatewayRoute       = "gatewayRoute"
	keyArn                = "arn"
	keyCreatedAt          = "createdAt"
	keyLastUpdatedAt      = "lastUpdatedAt"
	keyMeshOwner          = "meshOwner"
	keyResourceOwner      = "resourceOwner"
	keyVersion            = "version"
	keyMeshName           = "meshName"
	keyMetadata           = "metadata"
	keySpec               = "spec"
	keyStatus             = "status"
	keyVirtualRouterName  = "virtualRouterName"
	keyVirtualGatewayName = "virtualGatewayName"
	keyVirtualNodeName    = "virtualNodeName"
	keyVirtualServiceName = "virtualServiceName"
	keyRouteName          = "routeName"
	keyGatewayRouteName   = "gatewayRouteName"
	opUnknown             = "Unknown"

	// Path segment counts for URL depth matching.
	segsCollection       = 1 // /meshes
	segsSingle           = 2 // /meshes/{name}
	segsSubCollection    = 3 // /meshes/{name}/virtualNodes
	segsSubSingle        = 4 // /meshes/{name}/virtualNodes/{id} or min for nested
	segsNestedCollection = 5 // /meshes/{name}/virtualRouter/{id}/routes
	segsNestedSingle     = 6 // /meshes/{name}/virtualRouter/{id}/routes/{id}
)

// Handler handles App Mesh HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "AppMesh" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateMesh",
		"DescribeMesh",
		"UpdateMesh",
		"DeleteMesh",
		"ListMeshes",
		"CreateVirtualNode",
		"DescribeVirtualNode",
		"UpdateVirtualNode",
		"DeleteVirtualNode",
		"ListVirtualNodes",
		"CreateVirtualRouter",
		"DescribeVirtualRouter",
		"UpdateVirtualRouter",
		"DeleteVirtualRouter",
		"ListVirtualRouters",
		"CreateRoute",
		"DescribeRoute",
		"UpdateRoute",
		"DeleteRoute",
		"ListRoutes",
		"CreateVirtualService",
		"DescribeVirtualService",
		"UpdateVirtualService",
		"DeleteVirtualService",
		"ListVirtualServices",
		"CreateVirtualGateway",
		"DescribeVirtualGateway",
		"UpdateVirtualGateway",
		"DeleteVirtualGateway",
		"ListVirtualGateways",
		"CreateGatewayRoute",
		"DescribeGatewayRoute",
		"UpdateGatewayRoute",
		"DeleteGatewayRoute",
		"ListGatewayRoutes",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// RouteMatcher returns a function that matches App Mesh REST API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, appmeshPathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation returns the operation name for the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return parseOperation(c.Request().Method, c.Request().URL.Path)
}

// ExtractResource returns the primary resource identifier.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := splitPath(c.Request().URL.Path)
	if len(segs) >= segsSingle && segs[0] == pathSegMeshes {
		return segs[1]
	}

	return ""
}

// Handler returns the echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		method := c.Request().Method
		path := c.Request().URL.Path
		segs := splitPath(path)
		log.DebugContext(c.Request().Context(), "AppMesh request", "method", method, "path", path)

		if len(segs) == 0 {
			return c.JSON(http.StatusNotFound, errResp("NotFoundException", "not found"))
		}

		switch segs[0] {
		case pathSegTags:

			return h.handleListTags(c)
		case pathSegTag:

			return h.handleTagResource(c)
		case pathSegUntag:

			return h.handleUntagResource(c)
		case pathSegMeshes:

			return h.handleMeshes(c, segs)
		}

		return c.JSON(http.StatusNotFound, errResp("NotFoundException", "not found"))
	}
}

// ─── Route dispatch ───────────────────────────────────────────────────────────

func (h *Handler) handleMeshes(c *echo.Context, segs []string) error {
	// /meshes
	if len(segs) == segsCollection {
		switch c.Request().Method {
		case http.MethodPut:

			return h.handleCreateMesh(c)
		case http.MethodGet:

			return h.handleListMeshes(c)
		}

		return methodNotAllowed(c)
	}
	meshName := segs[1]

	// /meshes/{meshName}
	if len(segs) == segsSingle {
		switch c.Request().Method {
		case http.MethodGet:

			return h.handleDescribeMesh(c, meshName)
		case http.MethodPut:

			return h.handleUpdateMesh(c, meshName)
		case http.MethodDelete:

			return h.handleDeleteMesh(c, meshName)
		}

		return methodNotAllowed(c)
	}

	resource := segs[2]
	switch resource {
	case pathSegVirtualNodes:

		return h.handleVirtualNodes(c, segs, meshName)
	case pathSegVirtualRouters:

		return h.handleVirtualRouters(c, segs, meshName)
	case pathSegVirtualRouter:

		return h.handleRoutes(c, segs, meshName)
	case pathSegVirtualSvcs:

		return h.handleVirtualServices(c, segs, meshName)
	case pathSegVirtualGWs:

		return h.handleVirtualGateways(c, segs, meshName)
	case pathSegVirtualGW:

		return h.handleGatewayRoutes(c, segs, meshName)
	}

	return c.JSON(http.StatusNotFound, errResp("NotFoundException", "not found"))
}

// ─── Virtual Nodes ────────────────────────────────────────────────────────────

func (h *Handler) handleVirtualNodes(c *echo.Context, segs []string, meshName string) error {
	// /meshes/{meshName}/virtualNodes
	if len(segs) == segsSubCollection {
		switch c.Request().Method {
		case http.MethodPut:

			return h.handleCreateVirtualNode(c, meshName)
		case http.MethodGet:

			return h.handleListVirtualNodes(c, meshName)
		}

		return methodNotAllowed(c)
	}
	name := segs[3]
	// /meshes/{meshName}/virtualNodes/{name}
	switch c.Request().Method {
	case http.MethodGet:

		return h.handleDescribeVirtualNode(c, meshName, name)
	case http.MethodPut:

		return h.handleUpdateVirtualNode(c, meshName, name)
	case http.MethodDelete:

		return h.handleDeleteVirtualNode(c, meshName, name)
	}

	return methodNotAllowed(c)
}

// ─── Virtual Routers ──────────────────────────────────────────────────────────

func (h *Handler) handleVirtualRouters(c *echo.Context, segs []string, meshName string) error {
	// /meshes/{meshName}/virtualRouters
	if len(segs) == segsSubCollection {
		switch c.Request().Method {
		case http.MethodPut:

			return h.handleCreateVirtualRouter(c, meshName)
		case http.MethodGet:

			return h.handleListVirtualRouters(c, meshName)
		}

		return methodNotAllowed(c)
	}
	name := segs[3]
	// /meshes/{meshName}/virtualRouters/{name}
	switch c.Request().Method {
	case http.MethodGet:

		return h.handleDescribeVirtualRouter(c, meshName, name)
	case http.MethodPut:

		return h.handleUpdateVirtualRouter(c, meshName, name)
	case http.MethodDelete:

		return h.handleDeleteVirtualRouter(c, meshName, name)
	}

	return methodNotAllowed(c)
}

// ─── Routes (singular virtualRouter in path) ──────────────────────────────────

//nolint:dupl // handler pattern is structurally identical across resource types
func (h *Handler) handleRoutes(c *echo.Context, segs []string, meshName string) error {
	// /meshes/{meshName}/virtualRouter/{vrName}/routes
	if len(segs) < segsSubSingle {
		return c.JSON(http.StatusNotFound, errResp("NotFoundException", "not found"))
	}
	vrName := segs[3]
	if len(segs) == segsSubSingle || segs[4] != pathSegRoutes {
		return c.JSON(http.StatusNotFound, errResp("NotFoundException", "not found"))
	}
	// /meshes/{meshName}/virtualRouter/{vrName}/routes
	if len(segs) == segsNestedCollection {
		switch c.Request().Method {
		case http.MethodPut:

			return h.handleCreateRoute(c, meshName, vrName)
		case http.MethodGet:

			return h.handleListRoutes(c, meshName, vrName)
		}

		return methodNotAllowed(c)
	}
	routeName := segs[segsNestedSingle-1]
	// /meshes/{meshName}/virtualRouter/{vrName}/routes/{routeName}
	switch c.Request().Method {
	case http.MethodGet:

		return h.handleDescribeRoute(c, meshName, vrName, routeName)
	case http.MethodPut:

		return h.handleUpdateRoute(c, meshName, vrName, routeName)
	case http.MethodDelete:

		return h.handleDeleteRoute(c, meshName, vrName, routeName)
	}

	return methodNotAllowed(c)
}

// ─── Virtual Services ─────────────────────────────────────────────────────────

func (h *Handler) handleVirtualServices(c *echo.Context, segs []string, meshName string) error {
	// /meshes/{meshName}/virtualServices
	if len(segs) == segsSubCollection {
		switch c.Request().Method {
		case http.MethodPut:

			return h.handleCreateVirtualService(c, meshName)
		case http.MethodGet:

			return h.handleListVirtualServices(c, meshName)
		}

		return methodNotAllowed(c)
	}
	name := segs[3]
	// /meshes/{meshName}/virtualServices/{name}
	switch c.Request().Method {
	case http.MethodGet:

		return h.handleDescribeVirtualService(c, meshName, name)
	case http.MethodPut:

		return h.handleUpdateVirtualService(c, meshName, name)
	case http.MethodDelete:

		return h.handleDeleteVirtualService(c, meshName, name)
	}

	return methodNotAllowed(c)
}

// ─── Virtual Gateways ─────────────────────────────────────────────────────────

func (h *Handler) handleVirtualGateways(c *echo.Context, segs []string, meshName string) error {
	// /meshes/{meshName}/virtualGateways
	if len(segs) == segsSubCollection {
		switch c.Request().Method {
		case http.MethodPut:

			return h.handleCreateVirtualGateway(c, meshName)
		case http.MethodGet:

			return h.handleListVirtualGateways(c, meshName)
		}

		return methodNotAllowed(c)
	}
	name := segs[3]
	// /meshes/{meshName}/virtualGateways/{name}
	switch c.Request().Method {
	case http.MethodGet:

		return h.handleDescribeVirtualGateway(c, meshName, name)
	case http.MethodPut:

		return h.handleUpdateVirtualGateway(c, meshName, name)
	case http.MethodDelete:

		return h.handleDeleteVirtualGateway(c, meshName, name)
	}

	return methodNotAllowed(c)
}

// ─── Gateway Routes (singular virtualGateway in path) ────────────────────────

//nolint:dupl // handler pattern is structurally identical across resource types
func (h *Handler) handleGatewayRoutes(c *echo.Context, segs []string, meshName string) error {
	// /meshes/{meshName}/virtualGateway/{vgName}/gatewayRoutes[/{routeName}]
	if len(segs) < segsSubSingle {
		return c.JSON(http.StatusNotFound, errResp("NotFoundException", "not found"))
	}
	vgName := segs[3]
	if len(segs) == segsSubSingle || segs[4] != pathSegGatewayRoutes {
		return c.JSON(http.StatusNotFound, errResp("NotFoundException", "not found"))
	}
	// /meshes/{meshName}/virtualGateway/{vgName}/gatewayRoutes
	if len(segs) == segsNestedCollection {
		switch c.Request().Method {
		case http.MethodPut:

			return h.handleCreateGatewayRoute(c, meshName, vgName)
		case http.MethodGet:

			return h.handleListGatewayRoutes(c, meshName, vgName)
		}

		return methodNotAllowed(c)
	}
	routeName := segs[segsNestedSingle-1]
	// /meshes/{meshName}/virtualGateway/{vgName}/gatewayRoutes/{routeName}
	switch c.Request().Method {
	case http.MethodGet:

		return h.handleDescribeGatewayRoute(c, meshName, vgName, routeName)
	case http.MethodPut:

		return h.handleUpdateGatewayRoute(c, meshName, vgName, routeName)
	case http.MethodDelete:

		return h.handleDeleteGatewayRoute(c, meshName, vgName, routeName)
	}

	return methodNotAllowed(c)
}

// ─── Mesh handlers ────────────────────────────────────────────────────────────

func (h *Handler) handleCreateMesh(c *echo.Context) error {
	var body struct {
		MeshName    string          `json:"meshName"`
		ClientToken string          `json:"clientToken"`
		Spec        json.RawMessage `json:"spec"`
		Tags        []tagInput      `json:"tags"`
	}
	if err := c.Bind(&body); err != nil || body.MeshName == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "meshName is required"))
	}
	m, err := h.Backend.CreateMesh(body.MeshName, body.Spec, tagsToMap(body.Tags))
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyMesh: meshToWire(m)})
}

func (h *Handler) handleDescribeMesh(c *echo.Context, meshName string) error {
	m, err := h.Backend.DescribeMesh(meshName)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyMesh: meshToWire(m)})
}

func (h *Handler) handleUpdateMesh(c *echo.Context, meshName string) error {
	var body struct {
		ClientToken string          `json:"clientToken"`
		Spec        json.RawMessage `json:"spec"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "invalid request body"))
	}
	m, err := h.Backend.UpdateMesh(meshName, body.Spec)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyMesh: meshToWire(m)})
}

func (h *Handler) handleDeleteMesh(c *echo.Context, meshName string) error {
	m, err := h.Backend.DeleteMesh(meshName)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyMesh: meshToWire(m)})
}

func (h *Handler) handleListMeshes(c *echo.Context) error {
	maxResults, nextToken := listParams(c)
	items, next, err := h.Backend.ListMeshes(maxResults, nextToken)
	if err != nil {
		return h.mapErr(c, err)
	}
	refs := make([]any, 0, len(items))
	for _, ms := range items {
		refs = append(refs, meshSummaryToWire(ms))
	}

	return c.JSON(http.StatusOK, listResp("meshes", refs, next))
}

// ─── VirtualNode handlers ─────────────────────────────────────────────────────

func (h *Handler) handleCreateVirtualNode(c *echo.Context, meshName string) error {
	var body struct {
		VirtualNodeName string          `json:"virtualNodeName"`
		ClientToken     string          `json:"clientToken"`
		Spec            json.RawMessage `json:"spec"`
		Tags            []tagInput      `json:"tags"`
	}
	if err := c.Bind(&body); err != nil || body.VirtualNodeName == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "virtualNodeName is required"))
	}
	vn, err := h.Backend.CreateVirtualNode(meshName, body.VirtualNodeName, body.Spec, tagsToMap(body.Tags))
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualNode: vnToWire(vn)})
}

func (h *Handler) handleDescribeVirtualNode(c *echo.Context, meshName, name string) error {
	vn, err := h.Backend.DescribeVirtualNode(meshName, name)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualNode: vnToWire(vn)})
}

func (h *Handler) handleUpdateVirtualNode(c *echo.Context, meshName, name string) error {
	var body struct {
		ClientToken string          `json:"clientToken"`
		Spec        json.RawMessage `json:"spec"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "invalid request body"))
	}
	vn, err := h.Backend.UpdateVirtualNode(meshName, name, body.Spec)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualNode: vnToWire(vn)})
}

func (h *Handler) handleDeleteVirtualNode(c *echo.Context, meshName, name string) error {
	vn, err := h.Backend.DeleteVirtualNode(meshName, name)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualNode: vnToWire(vn)})
}

func (h *Handler) handleListVirtualNodes(c *echo.Context, meshName string) error {
	maxResults, nextToken := listParams(c)
	items, next, err := h.Backend.ListVirtualNodes(meshName, maxResults, nextToken)
	if err != nil {
		return h.mapErr(c, err)
	}
	refs := make([]any, 0, len(items))
	for _, vn := range items {
		refs = append(refs, vnSummaryToWire(vn))
	}

	return c.JSON(http.StatusOK, listResp("virtualNodes", refs, next))
}

// ─── VirtualRouter handlers ───────────────────────────────────────────────────

func (h *Handler) handleCreateVirtualRouter(c *echo.Context, meshName string) error {
	var body struct {
		VirtualRouterName string          `json:"virtualRouterName"`
		ClientToken       string          `json:"clientToken"`
		Spec              json.RawMessage `json:"spec"`
		Tags              []tagInput      `json:"tags"`
	}
	if err := c.Bind(&body); err != nil || body.VirtualRouterName == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "virtualRouterName is required"))
	}
	vr, err := h.Backend.CreateVirtualRouter(meshName, body.VirtualRouterName, body.Spec, tagsToMap(body.Tags))
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualRouter: vrToWire(vr)})
}

func (h *Handler) handleDescribeVirtualRouter(c *echo.Context, meshName, name string) error {
	vr, err := h.Backend.DescribeVirtualRouter(meshName, name)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualRouter: vrToWire(vr)})
}

func (h *Handler) handleUpdateVirtualRouter(c *echo.Context, meshName, name string) error {
	var body struct {
		ClientToken string          `json:"clientToken"`
		Spec        json.RawMessage `json:"spec"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "invalid request body"))
	}
	vr, err := h.Backend.UpdateVirtualRouter(meshName, name, body.Spec)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualRouter: vrToWire(vr)})
}

func (h *Handler) handleDeleteVirtualRouter(c *echo.Context, meshName, name string) error {
	vr, err := h.Backend.DeleteVirtualRouter(meshName, name)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualRouter: vrToWire(vr)})
}

func (h *Handler) handleListVirtualRouters(c *echo.Context, meshName string) error {
	maxResults, nextToken := listParams(c)
	items, next, err := h.Backend.ListVirtualRouters(meshName, maxResults, nextToken)
	if err != nil {
		return h.mapErr(c, err)
	}
	refs := make([]any, 0, len(items))
	for _, vr := range items {
		refs = append(refs, vrSummaryToWire(vr))
	}

	return c.JSON(http.StatusOK, listResp("virtualRouters", refs, next))
}

// ─── Route handlers ───────────────────────────────────────────────────────────

func (h *Handler) handleCreateRoute(c *echo.Context, meshName, vrName string) error {
	var body struct {
		RouteName   string          `json:"routeName"`
		ClientToken string          `json:"clientToken"`
		Spec        json.RawMessage `json:"spec"`
		Tags        []tagInput      `json:"tags"`
	}
	if err := c.Bind(&body); err != nil || body.RouteName == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "routeName is required"))
	}
	r, err := h.Backend.CreateRoute(meshName, vrName, body.RouteName, body.Spec, tagsToMap(body.Tags))
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyRoute: routeToWire(r)})
}

func (h *Handler) handleDescribeRoute(c *echo.Context, meshName, vrName, routeName string) error {
	r, err := h.Backend.DescribeRoute(meshName, vrName, routeName)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyRoute: routeToWire(r)})
}

func (h *Handler) handleUpdateRoute(c *echo.Context, meshName, vrName, routeName string) error {
	var body struct {
		ClientToken string          `json:"clientToken"`
		Spec        json.RawMessage `json:"spec"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "invalid request body"))
	}
	r, err := h.Backend.UpdateRoute(meshName, vrName, routeName, body.Spec)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyRoute: routeToWire(r)})
}

func (h *Handler) handleDeleteRoute(c *echo.Context, meshName, vrName, routeName string) error {
	r, err := h.Backend.DeleteRoute(meshName, vrName, routeName)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyRoute: routeToWire(r)})
}

func (h *Handler) handleListRoutes(c *echo.Context, meshName, vrName string) error {
	maxResults, nextToken := listParams(c)
	items, next, err := h.Backend.ListRoutes(meshName, vrName, maxResults, nextToken)
	if err != nil {
		return h.mapErr(c, err)
	}
	refs := make([]any, 0, len(items))
	for _, r := range items {
		refs = append(refs, routeSummaryToWire(r))
	}

	return c.JSON(http.StatusOK, listResp("routes", refs, next))
}

// ─── VirtualService handlers ──────────────────────────────────────────────────

func (h *Handler) handleCreateVirtualService(c *echo.Context, meshName string) error {
	var body struct {
		VirtualServiceName string          `json:"virtualServiceName"`
		ClientToken        string          `json:"clientToken"`
		Spec               json.RawMessage `json:"spec"`
		Tags               []tagInput      `json:"tags"`
	}
	if err := c.Bind(&body); err != nil || body.VirtualServiceName == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "virtualServiceName is required"))
	}
	vs, err := h.Backend.CreateVirtualService(meshName, body.VirtualServiceName, body.Spec, tagsToMap(body.Tags))
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualService: vsToWire(vs)})
}

func (h *Handler) handleDescribeVirtualService(c *echo.Context, meshName, name string) error {
	vs, err := h.Backend.DescribeVirtualService(meshName, name)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualService: vsToWire(vs)})
}

func (h *Handler) handleUpdateVirtualService(c *echo.Context, meshName, name string) error {
	var body struct {
		ClientToken string          `json:"clientToken"`
		Spec        json.RawMessage `json:"spec"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "invalid request body"))
	}
	vs, err := h.Backend.UpdateVirtualService(meshName, name, body.Spec)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualService: vsToWire(vs)})
}

func (h *Handler) handleDeleteVirtualService(c *echo.Context, meshName, name string) error {
	vs, err := h.Backend.DeleteVirtualService(meshName, name)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualService: vsToWire(vs)})
}

func (h *Handler) handleListVirtualServices(c *echo.Context, meshName string) error {
	maxResults, nextToken := listParams(c)
	items, next, err := h.Backend.ListVirtualServices(meshName, maxResults, nextToken)
	if err != nil {
		return h.mapErr(c, err)
	}
	refs := make([]any, 0, len(items))
	for _, vs := range items {
		refs = append(refs, vsSummaryToWire(vs))
	}

	return c.JSON(http.StatusOK, listResp("virtualServices", refs, next))
}

// ─── VirtualGateway handlers ──────────────────────────────────────────────────

func (h *Handler) handleCreateVirtualGateway(c *echo.Context, meshName string) error {
	var body struct {
		VirtualGatewayName string          `json:"virtualGatewayName"`
		ClientToken        string          `json:"clientToken"`
		Spec               json.RawMessage `json:"spec"`
		Tags               []tagInput      `json:"tags"`
	}
	if err := c.Bind(&body); err != nil || body.VirtualGatewayName == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "virtualGatewayName is required"))
	}
	vg, err := h.Backend.CreateVirtualGateway(meshName, body.VirtualGatewayName, body.Spec, tagsToMap(body.Tags))
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualGateway: vgToWire(vg)})
}

func (h *Handler) handleDescribeVirtualGateway(c *echo.Context, meshName, name string) error {
	vg, err := h.Backend.DescribeVirtualGateway(meshName, name)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualGateway: vgToWire(vg)})
}

func (h *Handler) handleUpdateVirtualGateway(c *echo.Context, meshName, name string) error {
	var body struct {
		ClientToken string          `json:"clientToken"`
		Spec        json.RawMessage `json:"spec"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "invalid request body"))
	}
	vg, err := h.Backend.UpdateVirtualGateway(meshName, name, body.Spec)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualGateway: vgToWire(vg)})
}

func (h *Handler) handleDeleteVirtualGateway(c *echo.Context, meshName, name string) error {
	vg, err := h.Backend.DeleteVirtualGateway(meshName, name)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVirtualGateway: vgToWire(vg)})
}

func (h *Handler) handleListVirtualGateways(c *echo.Context, meshName string) error {
	maxResults, nextToken := listParams(c)
	items, next, err := h.Backend.ListVirtualGateways(meshName, maxResults, nextToken)
	if err != nil {
		return h.mapErr(c, err)
	}
	refs := make([]any, 0, len(items))
	for _, vg := range items {
		refs = append(refs, vgSummaryToWire(vg))
	}

	return c.JSON(http.StatusOK, listResp("virtualGateways", refs, next))
}

// ─── GatewayRoute handlers ────────────────────────────────────────────────────

func (h *Handler) handleCreateGatewayRoute(c *echo.Context, meshName, vgName string) error {
	var body struct {
		GatewayRouteName string          `json:"gatewayRouteName"`
		ClientToken      string          `json:"clientToken"`
		Spec             json.RawMessage `json:"spec"`
		Tags             []tagInput      `json:"tags"`
	}
	if err := c.Bind(&body); err != nil || body.GatewayRouteName == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "gatewayRouteName is required"))
	}
	gr, err := h.Backend.CreateGatewayRoute(meshName, vgName, body.GatewayRouteName, body.Spec, tagsToMap(body.Tags))
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyGatewayRoute: grToWire(gr)})
}

func (h *Handler) handleDescribeGatewayRoute(c *echo.Context, meshName, vgName, routeName string) error {
	gr, err := h.Backend.DescribeGatewayRoute(meshName, vgName, routeName)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyGatewayRoute: grToWire(gr)})
}

func (h *Handler) handleUpdateGatewayRoute(c *echo.Context, meshName, vgName, routeName string) error {
	var body struct {
		ClientToken string          `json:"clientToken"`
		Spec        json.RawMessage `json:"spec"`
	}
	if err := c.Bind(&body); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "invalid request body"))
	}
	gr, err := h.Backend.UpdateGatewayRoute(meshName, vgName, routeName, body.Spec)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyGatewayRoute: grToWire(gr)})
}

func (h *Handler) handleDeleteGatewayRoute(c *echo.Context, meshName, vgName, routeName string) error {
	gr, err := h.Backend.DeleteGatewayRoute(meshName, vgName, routeName)
	if err != nil {
		return h.mapErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyGatewayRoute: grToWire(gr)})
}

func (h *Handler) handleListGatewayRoutes(c *echo.Context, meshName, vgName string) error {
	maxResults, nextToken := listParams(c)
	items, next, err := h.Backend.ListGatewayRoutes(meshName, vgName, maxResults, nextToken)
	if err != nil {
		return h.mapErr(c, err)
	}
	refs := make([]any, 0, len(items))
	for _, gr := range items {
		refs = append(refs, grSummaryToWire(gr))
	}

	return c.JSON(http.StatusOK, listResp("gatewayRoutes", refs, next))
}

// ─── Tag handlers ─────────────────────────────────────────────────────────────

func (h *Handler) handleListTags(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return methodNotAllowed(c)
	}
	resourceArn := c.QueryParam("resourceArn")
	if resourceArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "resourceArn is required"))
	}
	maxResults, nextToken := listParams(c)
	refs, next, err := h.Backend.ListTagsForResource(resourceArn, maxResults, nextToken)
	if err != nil {
		return h.mapErr(c, err)
	}
	wireRefs := make([]map[string]string, 0, len(refs))
	for _, r := range refs {
		wireRefs = append(wireRefs, map[string]string{"key": r.Key, "value": r.Value})
	}
	resp := map[string]any{"tags": wireRefs}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	if c.Request().Method != http.MethodPut {
		return methodNotAllowed(c)
	}
	var body struct {
		ResourceArn string     `json:"resourceArn"`
		Tags        []tagInput `json:"tags"`
	}
	if err := c.Bind(&body); err != nil || body.ResourceArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "resourceArn is required"))
	}
	if err := h.Backend.TagResource(body.ResourceArn, tagsToMap(body.Tags)); err != nil {
		return h.mapErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	if c.Request().Method != http.MethodPut {
		return methodNotAllowed(c)
	}
	var body struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	if err := c.Bind(&body); err != nil || body.ResourceArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "resourceArn is required"))
	}
	if err := h.Backend.UntagResource(body.ResourceArn, body.TagKeys); err != nil {
		return h.mapErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ─── Wire serialization ───────────────────────────────────────────────────────

func metaToWire(m ResourceMeta) map[string]any {
	return map[string]any{
		keyArn:           m.Arn,
		keyCreatedAt:     m.CreatedAt.Unix(),
		keyLastUpdatedAt: m.UpdatedAt.Unix(),
		keyMeshOwner:     m.MeshOwner,
		keyResourceOwner: m.ResourceOwner,
		"uid":            m.UID,
		keyVersion:       m.Version,
	}
}

func meshToWire(m *Mesh) map[string]any {
	return map[string]any{
		keyMeshName: m.Name,
		keyMetadata: metaToWire(m.Meta),
		keySpec:     specOrEmpty(m.Spec),
		keyStatus:   map[string]any{keyStatus: m.Status},
	}
}

func meshSummaryToWire(ms *MeshSummary) map[string]any {
	return map[string]any{
		keyArn:           ms.Arn,
		keyCreatedAt:     ms.CreatedAt.Unix(),
		keyLastUpdatedAt: ms.UpdatedAt.Unix(),
		keyMeshName:      ms.Name,
		keyMeshOwner:     ms.MeshOwner,
		keyResourceOwner: ms.ResourceOwner,
		keyVersion:       ms.Version,
	}
}

func vnToWire(vn *VirtualNode) map[string]any {
	return map[string]any{
		keyMeshName:        vn.MeshName,
		keyVirtualNodeName: vn.VirtualNodeName,
		keyMetadata:        metaToWire(vn.Meta),
		keySpec:            specOrEmpty(vn.Spec),
		keyStatus:          map[string]any{keyStatus: vn.Status},
	}
}

func vnSummaryToWire(vn *VirtualNodeSummary) map[string]any {
	return map[string]any{
		keyArn:             vn.Arn,
		keyCreatedAt:       vn.CreatedAt.Unix(),
		keyLastUpdatedAt:   vn.UpdatedAt.Unix(),
		keyMeshName:        vn.MeshName,
		keyVirtualNodeName: vn.VirtualNodeName,
		keyMeshOwner:       vn.MeshOwner,
		keyResourceOwner:   vn.ResourceOwner,
		keyVersion:         vn.Version,
	}
}

func vrToWire(vr *VirtualRouter) map[string]any {
	return map[string]any{
		keyMeshName:          vr.MeshName,
		keyVirtualRouterName: vr.VirtualRouterName,
		keyMetadata:          metaToWire(vr.Meta),
		keySpec:              specOrEmpty(vr.Spec),
		keyStatus:            map[string]any{keyStatus: vr.Status},
	}
}

func vrSummaryToWire(vr *VirtualRouterSummary) map[string]any {
	return map[string]any{
		keyArn:               vr.Arn,
		keyCreatedAt:         vr.CreatedAt.Unix(),
		keyLastUpdatedAt:     vr.UpdatedAt.Unix(),
		keyMeshName:          vr.MeshName,
		keyVirtualRouterName: vr.VirtualRouterName,
		keyMeshOwner:         vr.MeshOwner,
		keyResourceOwner:     vr.ResourceOwner,
		keyVersion:           vr.Version,
	}
}

func routeToWire(r *Route) map[string]any {
	return map[string]any{
		keyMeshName:          r.MeshName,
		keyVirtualRouterName: r.VirtualRouterName,
		keyRouteName:         r.RouteName,
		keyMetadata:          metaToWire(r.Meta),
		keySpec:              specOrEmpty(r.Spec),
		keyStatus:            map[string]any{keyStatus: r.Status},
	}
}

func routeSummaryToWire(r *RouteSummary) map[string]any {
	return map[string]any{
		keyArn:               r.Arn,
		keyCreatedAt:         r.CreatedAt.Unix(),
		keyLastUpdatedAt:     r.UpdatedAt.Unix(),
		keyMeshName:          r.MeshName,
		keyVirtualRouterName: r.VirtualRouterName,
		keyRouteName:         r.RouteName,
		keyMeshOwner:         r.MeshOwner,
		keyResourceOwner:     r.ResourceOwner,
		keyVersion:           r.Version,
	}
}

func vsToWire(vs *VirtualService) map[string]any {
	return map[string]any{
		keyMeshName:           vs.MeshName,
		keyVirtualServiceName: vs.VirtualServiceName,
		keyMetadata:           metaToWire(vs.Meta),
		keySpec:               specOrEmpty(vs.Spec),
		keyStatus:             map[string]any{keyStatus: vs.Status},
	}
}

func vsSummaryToWire(vs *VirtualServiceSummary) map[string]any {
	return map[string]any{
		keyArn:                vs.Arn,
		keyCreatedAt:          vs.CreatedAt.Unix(),
		keyLastUpdatedAt:      vs.UpdatedAt.Unix(),
		keyMeshName:           vs.MeshName,
		keyVirtualServiceName: vs.VirtualServiceName,
		keyMeshOwner:          vs.MeshOwner,
		keyResourceOwner:      vs.ResourceOwner,
		keyVersion:            vs.Version,
	}
}

func vgToWire(vg *VirtualGateway) map[string]any {
	return map[string]any{
		keyMeshName:           vg.MeshName,
		keyVirtualGatewayName: vg.VirtualGatewayName,
		keyMetadata:           metaToWire(vg.Meta),
		keySpec:               specOrEmpty(vg.Spec),
		keyStatus:             map[string]any{keyStatus: vg.Status},
	}
}

func vgSummaryToWire(vg *VirtualGatewaySummary) map[string]any {
	return map[string]any{
		keyArn:                vg.Arn,
		keyCreatedAt:          vg.CreatedAt.Unix(),
		keyLastUpdatedAt:      vg.UpdatedAt.Unix(),
		keyMeshName:           vg.MeshName,
		keyVirtualGatewayName: vg.VirtualGatewayName,
		keyMeshOwner:          vg.MeshOwner,
		keyResourceOwner:      vg.ResourceOwner,
		keyVersion:            vg.Version,
	}
}

func grToWire(gr *GatewayRoute) map[string]any {
	return map[string]any{
		keyMeshName:           gr.MeshName,
		keyVirtualGatewayName: gr.VirtualGatewayName,
		keyGatewayRouteName:   gr.GatewayRouteName,
		keyMetadata:           metaToWire(gr.Meta),
		keySpec:               specOrEmpty(gr.Spec),
		keyStatus:             map[string]any{keyStatus: gr.Status},
	}
}

func grSummaryToWire(gr *GatewayRouteSummary) map[string]any {
	return map[string]any{
		keyArn:                gr.Arn,
		keyCreatedAt:          gr.CreatedAt.Unix(),
		keyLastUpdatedAt:      gr.UpdatedAt.Unix(),
		keyMeshName:           gr.MeshName,
		keyVirtualGatewayName: gr.VirtualGatewayName,
		keyGatewayRouteName:   gr.GatewayRouteName,
		keyMeshOwner:          gr.MeshOwner,
		keyResourceOwner:      gr.ResourceOwner,
		keyVersion:            gr.Version,
	}
}

// specOrEmpty returns a JSON object, ensuring we never return null.
func specOrEmpty(spec json.RawMessage) any {
	if len(spec) == 0 {
		return map[string]any{}
	}
	var v any
	if err := json.Unmarshal(spec, &v); err != nil {
		return map[string]any{}
	}

	return v
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

type tagInput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func tagsToMap(tags []tagInput) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

func listParams(c *echo.Context) (int32, string) {
	nextToken := c.QueryParam("nextToken")
	maxResults := int32(defaultMaxResults)

	return maxResults, nextToken
}

func listResp(key string, items []any, nextToken string) map[string]any {
	resp := map[string]any{key: items}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp
}

func errResp(code, message string) map[string]any {
	return map[string]any{keyCode: code, keyMessage: message}
}

func methodNotAllowed(c *echo.Context) error {
	return c.JSON(http.StatusMethodNotAllowed, errResp("MethodNotAllowedException", "method not allowed"))
}

// mapErr maps backend errors to the correct HTTP status codes.
func (h *Handler) mapErr(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):

		return c.JSON(http.StatusNotFound, errResp("NotFoundException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):

		return c.JSON(http.StatusConflict, errResp("ConflictException", err.Error()))
	case errors.Is(err, awserr.ErrConflict):

		return c.JSON(http.StatusConflict, errResp("ResourceInUseException", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):

		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", err.Error()))
	default:

		return c.JSON(http.StatusInternalServerError, errResp("InternalServerErrorException", err.Error()))
	}
}

// splitPath splits a /v20190125/... path into segments after the version prefix.
func splitPath(path string) []string {
	path = strings.TrimPrefix(path, "/v20190125/")
	path = strings.TrimPrefix(path, "/v20190125")
	if path == "" {
		return nil
	}
	parts := strings.Split(path, "/")
	var segs []string
	for _, p := range parts {
		if p != "" {
			segs = append(segs, p)
		}
	}

	return segs
}

// parseOperation derives the operation name for observability.
func parseOperation(method, path string) string {
	segs := splitPath(path)
	if len(segs) == 0 {
		return opUnknown
	}

	switch segs[0] {
	case pathSegTags:

		return "ListTagsForResource"
	case pathSegTag:

		return "TagResource"
	case pathSegUntag:

		return "UntagResource"
	case pathSegMeshes:

		return parseMeshOp(method, segs)
	}

	return opUnknown
}

func parseMeshOp(method string, segs []string) string {
	if op := parseMeshTopLevel(method, segs); op != "" {
		return op
	}
	if len(segs) < segsSubCollection {
		return opUnknown
	}

	return parseMeshSubResource(method, segs)
}

// parseMeshTopLevel handles /meshes and /meshes/{name}.
func parseMeshTopLevel(method string, segs []string) string {
	switch len(segs) {
	case segsCollection:
		switch method {
		case http.MethodPut:

			return "CreateMesh"
		case http.MethodGet:

			return "ListMeshes"
		}
	case segsSingle:
		switch method {
		case http.MethodGet:

			return "DescribeMesh"
		case http.MethodPut:

			return "UpdateMesh"
		case http.MethodDelete:

			return "DeleteMesh"
		}
	}

	return ""
}

// parseMeshSubResource handles sub-resources under /meshes/{name}/.
func parseMeshSubResource(method string, segs []string) string {
	switch segs[2] {
	case pathSegVirtualNodes:

		return parseSubOp(method, segs, segsSubCollection, "VirtualNode", "VirtualNodes")
	case pathSegVirtualRouters:

		return parseSubOp(method, segs, segsSubCollection, "VirtualRouter", "VirtualRouters")
	case pathSegVirtualRouter:
		if len(segs) >= segsNestedCollection && segs[4] == pathSegRoutes {
			return parseSubOp(method, segs, segsNestedCollection, "Route", "Routes")
		}
	case pathSegVirtualSvcs:

		return parseSubOp(method, segs, segsSubCollection, "VirtualService", "VirtualServices")
	case pathSegVirtualGWs:

		return parseSubOp(method, segs, segsSubCollection, "VirtualGateway", "VirtualGateways")
	case pathSegVirtualGW:
		if len(segs) >= segsNestedCollection && segs[4] == pathSegGatewayRoutes {
			return parseSubOp(method, segs, segsNestedCollection, "GatewayRoute", "GatewayRoutes")
		}
	}

	return opUnknown
}

// parseSubOp returns an operation name for a sub-resource segment starting at idx.
func parseSubOp(method string, segs []string, idx int, singular, plural string) string {
	if len(segs) == idx {
		switch method {
		case http.MethodPut:

			return "Create" + singular
		case http.MethodGet:

			return "List" + plural
		}
	} else {
		switch method {
		case http.MethodGet:

			return "Describe" + singular
		case http.MethodPut:

			return "Update" + singular
		case http.MethodDelete:

			return "Delete" + singular
		}
	}

	return opUnknown
}
