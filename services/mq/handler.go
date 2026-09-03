package mq

import (
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
	opUnknown   = "Unknown"
	keyBrokerID = "brokerId"
	keyCreated  = "created"
)

const (
	opCreateBroker                  = "CreateBroker"
	opCreateConfiguration           = "CreateConfiguration"
	opCreateTags                    = "CreateTags"
	opCreateUser                    = "CreateUser"
	opDeleteBroker                  = "DeleteBroker"
	opDeleteConfiguration           = "DeleteConfiguration"
	opDeleteTags                    = "DeleteTags"
	opDeleteUser                    = "DeleteUser"
	opDescribeBroker                = "DescribeBroker"
	opDescribeBrokerEngineTypes     = "DescribeBrokerEngineTypes"
	opDescribeBrokerInstanceOptions = "DescribeBrokerInstanceOptions"
	opDescribeConfiguration         = "DescribeConfiguration"
	opDescribeConfigurationRevision = "DescribeConfigurationRevision"
	opDescribeSharedResources       = "DescribeSharedResources"
	opDescribeUser                  = "DescribeUser"
	opListBrokers                   = "ListBrokers"
	opListConfigurationRevisions    = "ListConfigurationRevisions"
	opListConfigurations            = "ListConfigurations"
	opListTags                      = "ListTags"
	opListUsers                     = "ListUsers"
	opPromote                       = "Promote"
	opRebootBroker                  = "RebootBroker"
	opUpdateBroker                  = "UpdateBroker"
	opUpdateConfiguration           = "UpdateConfiguration"
	opUpdateUser                    = "UpdateUser"
)

const (
	mqMatchPriority       = service.PriorityPathVersioned + 1 // 86 – higher than Kafka (85) to win /v1/configurations
	brokersPath           = "/v1/brokers"
	configurationsPath    = "/v1/configurations"
	tagsPath              = "/v1/tags"
	brokerEngineTypesPath = "/v1/broker-engine-types"
	brokerInstanceOptPath = "/v1/broker-instance-options"
	rebootSuffix          = "/reboot"
	promoteSuffix         = "/promote"
	sharedResourcesSuffix = "/shared-resources"
	usersSuffix           = "/users"
	revisionsSuffix       = "/revisions"
	// mqDefaultPageSize is ListBrokers/ListConfigurations' documented default
	// MaxResults (20), matching mqUsersDefaultPageSize (handler_users.go) --
	// see ListBrokersInput.MaxResults / ListConfigurationsInput.MaxResults in
	// aws-sdk-go-v2/service/mq ("20 by default... must be an integer from 5
	// to 100"). Not the same as the 5-100 max/min range those inputs also
	// document -- this is only the value used when MaxResults is omitted.
	mqDefaultPageSize = 20
)

// Handler is the Echo HTTP handler for Amazon MQ REST operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new Amazon MQ handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MQ" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateBroker,
		opCreateConfiguration,
		opCreateTags,
		opCreateUser,
		opDeleteBroker,
		opDeleteConfiguration,
		opDeleteTags,
		opDeleteUser,
		opDescribeBroker,
		opDescribeBrokerEngineTypes,
		opDescribeBrokerInstanceOptions,
		opDescribeConfiguration,
		opDescribeConfigurationRevision,
		opDescribeSharedResources,
		opDescribeUser,
		opListBrokers,
		opListConfigurationRevisions,
		opListConfigurations,
		opListTags,
		opListUsers,
		opPromote,
		opRebootBroker,
		opUpdateBroker,
		opUpdateConfiguration,
		opUpdateUser,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "mq" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// Reset clears the handler's backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// RouteMatcher returns a function that matches Amazon MQ REST API requests.
// MQ uses /v1/brokers, and MQ-signed /v1/configurations and /v1/tags paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		p := c.Request().URL.Path

		if strings.HasPrefix(p, brokersPath) {
			return true
		}

		if strings.HasPrefix(p, brokerEngineTypesPath) || strings.HasPrefix(p, brokerInstanceOptPath) {
			return isMQRequest(c.Request())
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
	case strings.HasPrefix(path, brokerEngineTypesPath):
		if method == http.MethodGet {
			return mqRoute{operation: opDescribeBrokerEngineTypes}
		}
	case strings.HasPrefix(path, brokerInstanceOptPath):
		if method == http.MethodGet {
			return mqRoute{operation: opDescribeBrokerInstanceOptions}
		}
	}

	return mqRoute{operation: opUnknown}
}

func parseBrokerRoute(method, suffix string) mqRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id == "" {
		switch method {
		case http.MethodGet:
			return mqRoute{operation: opListBrokers}
		case http.MethodPost:
			return mqRoute{operation: opCreateBroker}
		}
	}

	// /v1/brokers/{id}/reboot
	if before, ok := strings.CutSuffix(id, rebootSuffix); ok {
		if method == http.MethodPost {
			return mqRoute{operation: opRebootBroker, resource: before}
		}
	}

	// /v1/brokers/{id}/promote
	if before, ok := strings.CutSuffix(id, promoteSuffix); ok {
		if method == http.MethodPost {
			return mqRoute{operation: opPromote, resource: before}
		}
	}

	// /v1/brokers/{id}/shared-resources
	if before, ok := strings.CutSuffix(id, sharedResourcesSuffix); ok {
		if method == http.MethodGet {
			return mqRoute{operation: opDescribeSharedResources, resource: before}
		}
	}

	// /v1/brokers/{id}/users or /v1/brokers/{id}/users/{username}
	if before, after, ok := strings.Cut(id, usersSuffix); ok {
		return parseUserRoute(method, before, strings.TrimPrefix(after, "/"))
	}

	switch method {
	case http.MethodGet:
		return mqRoute{operation: opDescribeBroker, resource: id}
	case http.MethodPut:
		return mqRoute{operation: opUpdateBroker, resource: id}
	case http.MethodDelete:
		return mqRoute{operation: opDeleteBroker, resource: id}
	}

	return mqRoute{operation: opUnknown}
}

// parseUserRoute returns the route for /v1/brokers/{id}/users[/{username}] paths.
func parseUserRoute(method, brokerID, username string) mqRoute {
	if username == "" {
		if method == http.MethodGet {
			return mqRoute{operation: opListUsers, resource: brokerID}
		}

		return mqRoute{operation: opUnknown}
	}

	switch method {
	case http.MethodGet:
		return mqRoute{operation: opDescribeUser, resource: brokerID, subresource: username}
	case http.MethodPost:
		return mqRoute{operation: opCreateUser, resource: brokerID, subresource: username}
	case http.MethodPut:
		return mqRoute{operation: opUpdateUser, resource: brokerID, subresource: username}
	case http.MethodDelete:
		return mqRoute{operation: opDeleteUser, resource: brokerID, subresource: username}
	}

	return mqRoute{operation: opUnknown}
}

func parseConfigurationRoute(method, suffix string) mqRoute {
	id := strings.TrimPrefix(suffix, "/")

	if id == "" {
		switch method {
		case http.MethodGet:
			return mqRoute{operation: opListConfigurations}
		case http.MethodPost:
			return mqRoute{operation: opCreateConfiguration}
		}
	}

	// /v1/configurations/{id}/revisions[/{revision}]
	if before, after, ok := strings.Cut(id, revisionsSuffix); ok {
		return parseRevisionRoute(method, strings.TrimSuffix(before, "/"), strings.TrimPrefix(after, "/"))
	}

	switch method {
	case http.MethodGet:
		return mqRoute{operation: opDescribeConfiguration, resource: id}
	case http.MethodPut:
		return mqRoute{operation: opUpdateConfiguration, resource: id}
	case http.MethodDelete:
		return mqRoute{operation: opDeleteConfiguration, resource: id}
	}

	return mqRoute{operation: opUnknown}
}

// parseRevisionRoute returns the route for /v1/configurations/{id}/revisions[/{revision}] paths.
func parseRevisionRoute(method, configID, rev string) mqRoute {
	if method != http.MethodGet {
		return mqRoute{operation: opUnknown}
	}

	if rev == "" {
		return mqRoute{operation: opListConfigurationRevisions, resource: configID}
	}

	return mqRoute{operation: opDescribeConfigurationRevision, resource: configID, subresource: rev}
}

func parseTagRoute(method, suffix string) mqRoute {
	escaped := strings.TrimPrefix(suffix, "/")
	resourceARN, err := url.PathUnescape(escaped)
	if err != nil {
		resourceARN = escaped
	}

	switch method {
	case http.MethodGet:
		return mqRoute{operation: opListTags, resource: resourceARN}
	case http.MethodPost:
		return mqRoute{operation: opCreateTags, resource: resourceARN}
	case http.MethodDelete:
		return mqRoute{operation: opDeleteTags, resource: resourceARN}
	}

	return mqRoute{operation: opUnknown}
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

	if handled, err := h.dispatchReadOps(c, route); handled {
		return err
	}

	return h.dispatchMutating(c, route, readBody)
}

// dispatchReadOps handles read-only (no request body) operations.
// Returns (true, err) if the operation was matched, (false, nil) if not.
func (h *Handler) dispatchReadOps(c *echo.Context, route mqRoute) (bool, error) {
	switch route.operation {
	case opListBrokers:
		return true, h.handleListBrokers(c)
	case opDescribeBroker:
		return true, h.handleDescribeBroker(c, route.resource)
	case opDeleteBroker:
		return true, h.handleDeleteBroker(c, route.resource)
	case opRebootBroker:
		return true, h.handleRebootBroker(c, route.resource)
	case opDescribeSharedResources:
		return true, h.handleDescribeSharedResources(c, route.resource)
	case opDescribeBrokerEngineTypes:
		return true, h.handleDescribeBrokerEngineTypes(c)
	case opDescribeBrokerInstanceOptions:
		return true, h.handleDescribeBrokerInstanceOptions(c)
	case opListUsers:
		return true, h.handleListUsers(c, route.resource)
	case opDescribeUser:
		return true, h.handleDescribeUser(c, route.resource, route.subresource)
	case opDeleteUser:
		return true, h.handleDeleteUser(c, route.resource, route.subresource)
	}

	return h.dispatchReadConfigOps(c, route)
}

// dispatchReadConfigOps handles read-only configuration and tag operations.
// Returns (true, err) if the operation was matched, (false, nil) if not.
func (h *Handler) dispatchReadConfigOps(c *echo.Context, route mqRoute) (bool, error) {
	switch route.operation {
	case opListConfigurations:
		return true, h.handleListConfigurations(c)
	case opDescribeConfiguration:
		return true, h.handleDescribeConfiguration(c, route.resource)
	case opDeleteConfiguration:
		return true, h.handleDeleteConfiguration(c, route.resource)
	case opListConfigurationRevisions:
		return true, h.handleListConfigurationRevisions(c, route.resource)
	case opDescribeConfigurationRevision:
		return true, h.handleDescribeConfigurationRevision(c, route.resource, route.subresource)
	case opListTags:
		return true, h.handleListTags(c, route.resource)
	case opDeleteTags:
		return true, h.handleDeleteTags(c, route.resource)
	}

	return false, nil
}

// dispatchMutating handles write operations that require reading a request body.
func (h *Handler) dispatchMutating(c *echo.Context, route mqRoute, readBody func() ([]byte, bool)) error {
	body, ok := readBody()
	if !ok {
		return c.JSON(http.StatusInternalServerError,
			errorResponse("InternalServerErrorException", "internal server error"))
	}

	switch route.operation {
	case opCreateBroker:
		return h.handleCreateBroker(c, body)
	case opUpdateBroker:
		return h.handleUpdateBroker(c, route.resource, body)
	case opPromote:
		return h.handlePromote(c, route.resource, body)
	case opCreateUser:
		return h.handleCreateUser(c, route.resource, route.subresource, body)
	case opUpdateUser:
		return h.handleUpdateUser(c, route.resource, route.subresource, body)
	case opCreateConfiguration:
		return h.handleCreateConfiguration(c, body)
	case opUpdateConfiguration:
		return h.handleUpdateConfiguration(c, route.resource, body)
	case opCreateTags:
		return h.handleCreateTags(c, route.resource, body)
	}

	return c.JSON(
		http.StatusNotFound,
		errorResponse("NotFoundException", "unknown operation: "+c.Request().URL.Path),
	)
}

// tagsOrEmpty returns tags, or an empty (non-nil) map if tags is nil, so JSON
// responses always render an object rather than null.
func tagsOrEmpty(tags map[string]string) map[string]string {
	if tags == nil {
		return map[string]string{}
	}

	return tags
}

// --- Error handling ---

func (h *Handler) writeError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, ErrInUse):
		return c.JSON(http.StatusConflict, errorResponse("ConflictException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerErrorException", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}
