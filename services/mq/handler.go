package mq

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	mqMatchPriority    = service.PriorityPathVersioned + 1 // 86 – higher than Kafka (85) to win /v1/configurations
	brokersPath        = "/v1/brokers"
	configurationsPath = "/v1/configurations"
	tagsPath           = "/v1/tags"
	rebootSuffix       = "/reboot"
	usersSuffix        = "/users"
)

// Handler is the Echo HTTP handler for Amazon MQ REST operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Amazon MQ handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MQ" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateBroker",
		"DescribeBroker",
		"ListBrokers",
		"UpdateBroker",
		"DeleteBroker",
		"RebootBroker",
		"CreateUser",
		"DescribeUser",
		"UpdateUser",
		"DeleteUser",
		"ListUsers",
		"CreateConfiguration",
		"DescribeConfiguration",
		"ListConfigurations",
		"UpdateConfiguration",
		"ListTags",
		"CreateTags",
		"DeleteTags",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "mq" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Amazon MQ REST API requests.
// MQ uses /v1/brokers, and MQ-signed /v1/configurations and /v1/tags paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		p := c.Request().URL.Path

		if strings.HasPrefix(p, brokersPath) {
			return true
		}

		if strings.HasPrefix(p, configurationsPath) || strings.HasPrefix(p, tagsPath) {
			return isMQRequest(c.Request())
		}

		return false
	}
}

// isMQRequest returns true if the request's Authorization header identifies the "mq" service.
func isMQRequest(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Authorization"), "/mq/")
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return mqMatchPriority }

// ExtractOperation returns the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return parseRoute(c.Request().Method, c.Request().URL.Path).operation
}

// ExtractResource extracts a resource ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return parseRoute(c.Request().Method, c.Request().URL.Path).resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		route := parseRoute(r.Method, r.URL.Path)

		return h.dispatch(c, route)
	}
}

// mqRoute holds the parsed route information.
type mqRoute struct {
	resource    string
	subresource string
	operation   string
}

// parseRoute maps HTTP method + path to an operation name and resource ID.
func parseRoute(method, path string) mqRoute {
	switch {
	case strings.HasPrefix(path, brokersPath):
		return parseBrokerRoute(method, strings.TrimPrefix(path, brokersPath))
	case strings.HasPrefix(path, configurationsPath):
		return parseConfigurationRoute(method, strings.TrimPrefix(path, configurationsPath))
	case strings.HasPrefix(path, tagsPath):
		return parseTagRoute(method, strings.TrimPrefix(path, tagsPath))
	}

	return mqRoute{operation: "Unknown"}
}

func parseBrokerRoute(method, suffix string) mqRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id == "" {
		switch method {
		case http.MethodGet:
			return mqRoute{operation: "ListBrokers"}
		case http.MethodPost:
			return mqRoute{operation: "CreateBroker"}
		}
	}

	// /v1/brokers/{id}/reboot
	if before, ok := strings.CutSuffix(id, rebootSuffix); ok {
		if method == http.MethodPost {
			return mqRoute{operation: "RebootBroker", resource: before}
		}
	}

	// /v1/brokers/{id}/users or /v1/brokers/{id}/users/{username}
	if before, after, ok := strings.Cut(id, usersSuffix); ok {
		return parseUserRoute(method, before, strings.TrimPrefix(after, "/"))
	}

	switch method {
	case http.MethodGet:
		return mqRoute{operation: "DescribeBroker", resource: id}
	case http.MethodPut:
		return mqRoute{operation: "UpdateBroker", resource: id}
	case http.MethodDelete:
		return mqRoute{operation: "DeleteBroker", resource: id}
	}

	return mqRoute{operation: "Unknown"}
}

// parseUserRoute returns the route for /v1/brokers/{id}/users[/{username}] paths.
func parseUserRoute(method, brokerID, username string) mqRoute {
	if username == "" {
		if method == http.MethodGet {
			return mqRoute{operation: "ListUsers", resource: brokerID}
		}

		return mqRoute{operation: "Unknown"}
	}

	switch method {
	case http.MethodGet:
		return mqRoute{operation: "DescribeUser", resource: brokerID, subresource: username}
	case http.MethodPost:
		return mqRoute{operation: "CreateUser", resource: brokerID, subresource: username}
	case http.MethodPut:
		return mqRoute{operation: "UpdateUser", resource: brokerID, subresource: username}
	case http.MethodDelete:
		return mqRoute{operation: "DeleteUser", resource: brokerID, subresource: username}
	}

	return mqRoute{operation: "Unknown"}
}

func parseConfigurationRoute(method, suffix string) mqRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id == "" {
		switch method {
		case http.MethodGet:
			return mqRoute{operation: "ListConfigurations"}
		case http.MethodPost:
			return mqRoute{operation: "CreateConfiguration"}
		}
	}

	switch method {
	case http.MethodGet:
		return mqRoute{operation: "DescribeConfiguration", resource: id}
	case http.MethodPut:
		return mqRoute{operation: "UpdateConfiguration", resource: id}
	}

	return mqRoute{operation: "Unknown"}
}

func parseTagRoute(method, suffix string) mqRoute {
	escaped := strings.TrimPrefix(suffix, "/")
	resourceARN, err := url.PathUnescape(escaped)
	if err != nil {
		resourceARN = escaped
	}

	switch method {
	case http.MethodGet:
		return mqRoute{operation: "ListTags", resource: resourceARN}
	case http.MethodPost:
		return mqRoute{operation: "CreateTags", resource: resourceARN}
	case http.MethodDelete:
		return mqRoute{operation: "DeleteTags", resource: resourceARN}
	}

	return mqRoute{operation: "Unknown"}
}

// dispatch routes the request to the appropriate handler based on the parsed route.
func (h *Handler) dispatch(c *echo.Context, route mqRoute) error {
	r := c.Request()
	log := logger.Load(r.Context())

	readBody := func() ([]byte, bool) {
		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "mq: failed to read request body", "error", err)

			return nil, false
		}

		return body, true
	}

	switch route.operation {
	case "ListBrokers":
		return h.handleListBrokers(c)
	case "DescribeBroker":
		return h.handleDescribeBroker(c, route.resource)
	case "DeleteBroker":
		return h.handleDeleteBroker(c, route.resource)
	case "RebootBroker":
		return h.handleRebootBroker(c, route.resource)
	case "ListUsers":
		return h.handleListUsers(c, route.resource)
	case "DescribeUser":
		return h.handleDescribeUser(c, route.resource, route.subresource)
	case "DeleteUser":
		return h.handleDeleteUser(c, route.resource, route.subresource)
	case "ListConfigurations":
		return h.handleListConfigurations(c)
	case "DescribeConfiguration":
		return h.handleDescribeConfiguration(c, route.resource)
	case "ListTags":
		return h.handleListTags(c, route.resource)
	case "DeleteTags":
		return h.handleDeleteTags(c, route.resource)
	}

	return h.dispatchMutating(c, route, readBody)
}

// dispatchMutating handles write operations that require reading a request body.
func (h *Handler) dispatchMutating(c *echo.Context, route mqRoute, readBody func() ([]byte, bool)) error {
	body, ok := readBody()
	if !ok {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalError", "internal server error"))
	}

	switch route.operation {
	case "CreateBroker":
		return h.handleCreateBroker(c, body)
	case "UpdateBroker":
		return h.handleUpdateBroker(c, route.resource, body)
	case "CreateUser":
		return h.handleCreateUser(c, route.resource, route.subresource, body)
	case "UpdateUser":
		return h.handleUpdateUser(c, route.resource, route.subresource, body)
	case "CreateConfiguration":
		return h.handleCreateConfiguration(c, body)
	case "UpdateConfiguration":
		return h.handleUpdateConfiguration(c, route.resource, body)
	case "CreateTags":
		return h.handleCreateTags(c, route.resource, body)
	}

	return c.JSON(
		http.StatusNotFound,
		errorResponse("NotFoundException", "unknown operation: "+c.Request().URL.Path),
	)
}

// --- Broker handlers ---

type createBrokerInput struct {
	BrokerName              string            `json:"brokerName"`
	DeploymentMode          string            `json:"deploymentMode"`
	EngineType              string            `json:"engineType"`
	EngineVersion           string            `json:"engineVersion"`
	HostInstanceType        string            `json:"hostInstanceType"`
	SecurityGroups          []string          `json:"securityGroups"`
	SubnetIDs               []string          `json:"subnetIds"`
	Tags                    map[string]string `json:"tags"`
	Users                   []createUserBody  `json:"users"`
	PubliclyAccessible      bool              `json:"publiclyAccessible"`
	AutoMinorVersionUpgrade bool              `json:"autoMinorVersionUpgrade"`
}

type createUserBody struct {
	Username string   `json:"username"`
	Password string   `json:"password"`
	Groups   []string `json:"groups"`
	Console  bool     `json:"consoleAccess"`
}

func (h *Handler) handleCreateBroker(c *echo.Context, body []byte) error {
	var in createBrokerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.BrokerName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "brokerName is required"))
	}

	if in.EngineType == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "engineType is required"))
	}

	users := make([]*User, 0, len(in.Users))
	for _, u := range in.Users {
		users = append(users, &User{
			Username: u.Username,
			Password: u.Password,
			Groups:   u.Groups,
			Console:  u.Console,
		})
	}

	br, err := h.Backend.CreateBroker(
		in.BrokerName,
		in.DeploymentMode,
		in.EngineType,
		in.EngineVersion,
		in.HostInstanceType,
		in.PubliclyAccessible,
		in.AutoMinorVersionUpgrade,
		in.SecurityGroups,
		in.SubnetIDs,
		users,
		in.Tags,
	)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"brokerId":  br.BrokerID,
		"brokerArn": br.BrokerArn,
	})
}

func (h *Handler) handleDescribeBroker(c *echo.Context, brokerID string) error {
	br, err := h.Backend.DescribeBroker(brokerID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, toBrokerResponse(br))
}

func (h *Handler) handleListBrokers(c *echo.Context) error {
	brokers := h.Backend.ListBrokers()

	summaries := make([]brokerSummary, 0, len(brokers))
	for _, br := range brokers {
		summaries = append(summaries, brokerSummary{
			BrokerArn:        br.BrokerArn,
			BrokerID:         br.BrokerID,
			BrokerName:       br.BrokerName,
			BrokerState:      br.BrokerState,
			DeploymentMode:   br.DeploymentMode,
			EngineType:       br.EngineType,
			HostInstanceType: br.HostInstanceType,
			Created:          br.Created,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"brokerSummaries": summaries})
}

type brokerSummary struct {
	BrokerArn        string `json:"brokerArn"`
	BrokerID         string `json:"brokerId"`
	BrokerName       string `json:"brokerName"`
	BrokerState      string `json:"brokerState"`
	DeploymentMode   string `json:"deploymentMode"`
	EngineType       string `json:"engineType"`
	HostInstanceType string `json:"hostInstanceType"`
	Created          string `json:"created"`
}

type updateBrokerInput struct {
	AutoMinorVersionUpgrade *bool    `json:"autoMinorVersionUpgrade"`
	EngineVersion           string   `json:"engineVersion"`
	HostInstanceType        string   `json:"hostInstanceType"`
	SecurityGroups          []string `json:"securityGroups"`
}

func (h *Handler) handleUpdateBroker(c *echo.Context, brokerID string, body []byte) error {
	var in updateBrokerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	br, err := h.Backend.UpdateBroker(
		brokerID,
		in.EngineVersion,
		in.HostInstanceType,
		in.AutoMinorVersionUpgrade,
		in.SecurityGroups,
	)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"brokerId": br.BrokerID})
}

func (h *Handler) handleDeleteBroker(c *echo.Context, brokerID string) error {
	br, err := h.Backend.DeleteBroker(brokerID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"brokerId": br.BrokerID})
}

func (h *Handler) handleRebootBroker(c *echo.Context, brokerID string) error {
	if err := h.Backend.RebootBroker(brokerID); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// brokerResponse is the full broker detail response.
type brokerResponse struct {
	Tags                    map[string]string `json:"tags,omitempty"`
	Configurations          *Configurations   `json:"configurations,omitempty"`
	BrokerArn               string            `json:"brokerArn"`
	BrokerName              string            `json:"brokerName"`
	BrokerState             string            `json:"brokerState"`
	DeploymentMode          string            `json:"deploymentMode"`
	EngineType              string            `json:"engineType"`
	EngineVersion           string            `json:"engineVersion"`
	HostInstanceType        string            `json:"hostInstanceType"`
	BrokerID                string            `json:"brokerId"`
	Created                 string            `json:"created"`
	BrokerInstances         []BrokerInstance  `json:"brokerInstances,omitempty"`
	SecurityGroups          []string          `json:"securityGroups,omitempty"`
	Users                   []UserSummary     `json:"users,omitempty"`
	SubnetIDs               []string          `json:"subnetIds,omitempty"`
	PubliclyAccessible      bool              `json:"publiclyAccessible"`
	AutoMinorVersionUpgrade bool              `json:"autoMinorVersionUpgrade"`
}

func toBrokerResponse(br *Broker) brokerResponse {
	users := make([]UserSummary, 0, len(br.Users))
	for _, u := range br.Users {
		users = append(users, UserSummary{Username: u.Username, Console: u.Console})
	}

	return brokerResponse{
		BrokerArn:               br.BrokerArn,
		BrokerID:                br.BrokerID,
		BrokerName:              br.BrokerName,
		BrokerState:             br.BrokerState,
		DeploymentMode:          br.DeploymentMode,
		EngineType:              br.EngineType,
		EngineVersion:           br.EngineVersion,
		HostInstanceType:        br.HostInstanceType,
		Configurations:          br.Configurations,
		BrokerInstances:         br.BrokerInstances,
		SubnetIDs:               br.SubnetIDs,
		SecurityGroups:          br.SecurityGroups,
		Tags:                    br.Tags,
		Users:                   users,
		Created:                 br.Created,
		PubliclyAccessible:      br.PubliclyAccessible,
		AutoMinorVersionUpgrade: br.AutoMinorVersionUpgrade,
	}
}

// --- User handlers ---

func (h *Handler) handleCreateUser(c *echo.Context, brokerID, username string, body []byte) error {
	var in createUserBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if err := h.Backend.CreateUser(brokerID, username, in.Password, in.Groups, in.Console); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusCreated)
}

func (h *Handler) handleDescribeUser(c *echo.Context, brokerID, username string) error {
	u, err := h.Backend.DescribeUser(brokerID, username)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"brokerId":      brokerID,
		"username":      u.Username,
		"consoleAccess": u.Console,
		"groups":        u.Groups,
	})
}

func (h *Handler) handleDeleteUser(c *echo.Context, brokerID, username string) error {
	if err := h.Backend.DeleteUser(brokerID, username); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type updateUserBody struct {
	Console  *bool    `json:"consoleAccess"`
	Password string   `json:"password"`
	Groups   []string `json:"groups"`
}

func (h *Handler) handleUpdateUser(c *echo.Context, brokerID, username string, body []byte) error {
	var in updateUserBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if err := h.Backend.UpdateUser(brokerID, username, in.Password, in.Groups, in.Console); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListUsers(c *echo.Context, brokerID string) error {
	users, err := h.Backend.ListUsers(brokerID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"brokerId": brokerID,
		"users":    users,
	})
}

// --- Configuration handlers ---

type createConfigurationInput struct {
	Tags          map[string]string `json:"tags"`
	Name          string            `json:"name"`
	Description   string            `json:"description"`
	EngineType    string            `json:"engineType"`
	EngineVersion string            `json:"engineVersion"`
}

func (h *Handler) handleCreateConfiguration(c *echo.Context, body []byte) error {
	var in createConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	if in.EngineType == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "engineType is required"))
	}

	cfg, err := h.Backend.CreateConfiguration(in.Name, in.Description, in.EngineType, in.EngineVersion, in.Tags)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"id":             cfg.ID,
		"arn":            cfg.Arn,
		"name":           cfg.Name,
		"latestRevision": cfg.LatestRevision,
	})
}

func (h *Handler) handleDescribeConfiguration(c *echo.Context, configID string) error {
	cfg, err := h.Backend.DescribeConfiguration(configID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, toConfigurationResponse(cfg))
}

func (h *Handler) handleListConfigurations(c *echo.Context) error {
	cfgs := h.Backend.ListConfigurations()
	if cfgs == nil {
		cfgs = []*Configuration{}
	}

	list := make([]any, 0, len(cfgs))
	for _, cfg := range cfgs {
		list = append(list, toConfigurationResponse(cfg))
	}

	return c.JSON(http.StatusOK, map[string]any{"configurations": list})
}

type updateConfigurationInput struct {
	Data        string `json:"data"`
	Description string `json:"description"`
}

func (h *Handler) handleUpdateConfiguration(c *echo.Context, configID string, body []byte) error {
	var in updateConfigurationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	cfg, err := h.Backend.UpdateConfiguration(configID, in.Description, in.Data)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"id":             cfg.ID,
		"arn":            cfg.Arn,
		"latestRevision": cfg.LatestRevision,
	})
}

// configurationResponse is the full configuration detail response.
type configurationResponse struct {
	Tags           map[string]string      `json:"tags,omitempty"`
	Arn            string                 `json:"arn"`
	ID             string                 `json:"id"`
	Name           string                 `json:"name"`
	Description    string                 `json:"description"`
	EngineType     string                 `json:"engineType"`
	EngineVersion  string                 `json:"engineVersion"`
	LatestRevision *ConfigurationRevision `json:"latestRevision"`
	Created        string                 `json:"created"`
}

func toConfigurationResponse(cfg *Configuration) configurationResponse {
	return configurationResponse{
		Arn:            cfg.Arn,
		ID:             cfg.ID,
		Name:           cfg.Name,
		Description:    cfg.Description,
		EngineType:     cfg.EngineType,
		EngineVersion:  cfg.EngineVersion,
		LatestRevision: cfg.LatestRevision,
		Created:        cfg.Created,
		Tags:           cfg.Tags,
	}
}

// --- Tag handlers ---

func (h *Handler) handleListTags(c *echo.Context, resourceARN string) error {
	tags := h.Backend.ListTags(resourceARN)
	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
}

type createTagsInput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleCreateTags(c *echo.Context, resourceARN string, body []byte) error {
	var in createTagsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	h.Backend.CreateTags(resourceARN, in.Tags)

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteTags(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]
	h.Backend.DeleteTags(resourceARN, tagKeys)

	return c.NoContent(http.StatusNoContent)
}

// --- Error handling ---

func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errorResponse("ConflictException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalError", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}
