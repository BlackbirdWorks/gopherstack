package awsconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const awsConfigTargetPrefix = "StarlingDoveService."

var errUnknownAction = errors.New("unknown action")

type configurationRecorderNameInput struct {
	ConfigurationRecorderName string `json:"ConfigurationRecorderName"`
}

// emptyInput/emptyOutput are shared request/response placeholders for
// operations that carry no meaningful body, used across every op-family file.
type (
	emptyInput  struct{}
	emptyOutput struct{}
)

// Handler is the Echo HTTP handler for AWS Config operations.
type Handler struct {
	Backend       *InMemoryBackend
	dispatchTable map[string]service.JSONOpFunc
}

// NewHandler creates a new AWS Config handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.dispatchTable = h.buildDispatchTable()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "AWSConfig" }

// GetSupportedOperations returns the list of supported AWS Config operations.
func (h *Handler) GetSupportedOperations() []string {
	families := [][]string{
		configurationRecorderSupportedOps(),
		deliveryChannelSupportedOps(),
		configRuleSupportedOps(),
		aggregatorSupportedOps(),
		conformancePackSupportedOps(),
		organizationSupportedOps(),
		retentionSupportedOps(),
		remediationSupportedOps(),
		storedQuerySupportedOps(),
		tagSupportedOps(),
		resourceSupportedOps(),
	}

	total := 0
	for _, f := range families {
		total += len(f)
	}

	ops := make([]string, 0, total)
	for _, f := range families {
		ops = append(ops, f...)
	}

	return ops
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "config" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this AWS Config instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches AWS Config requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), awsConfigTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the AWS Config action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, awsConfigTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts a resource identifier from the request body based on the operation.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	switch h.ExtractOperation(c) {
	case opPutConfigurationRecorder:
		return extractConfigRecorderName(body)
	case opStartConfigurationRecorder, opStopConfigurationRecorder, opDeleteConfigurationRecorder:
		return extractTopLevelRecorderName(body)
	case opDescribeConfigurationRecorders, opDescribeConfigurationRecorderStatus:
		return extractFirstRecorderName(body)
	case opPutDeliveryChannel:
		return extractDeliveryChannelName(body)
	case opDescribeDeliveryChannels, opDeleteDeliveryChannel:
		return extractFirstDeliveryChannelName(body)
	case opDeleteConfigRule, opDescribeConfigRules, opGetComplianceDetailsByConfigRule, opDeleteEvaluationResults:
		return extractNamedField(body, "ConfigRuleName")
	case opDeleteConfigurationAggregator, opBatchGetAggregateResourceConfig:
		return extractNamedField(body, "ConfigurationAggregatorName")
	case opDeleteConformancePack:
		return extractNamedField(body, "ConformancePackName")
	case opDeleteOrganizationConfigRule:
		return extractNamedField(body, "OrganizationConfigRuleName")
	case opDeleteOrganizationConformancePack:
		return extractNamedField(body, "OrganizationConformancePackName")
	case opAssociateResourceTypes:
		return extractNamedField(body, "ConfigurationRecorderArn")
	default:
		return extractTopLevelRecorderName(body)
	}
}

// configNameBody is a JSON wrapper carrying a single "name" field, used for both
// configuration recorder and delivery channel name extraction.
type configNameBody struct {
	Name string `json:"name"`
}

// extractNamedField extracts a top-level string field by key from a JSON body.
func extractNamedField(body []byte, key string) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return ""
	}

	raw, ok := m[key]
	if !ok {
		return ""
	}

	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}

	return s
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"AWSConfig", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// buildDispatchTable assembles the full operation dispatch table from every
// op-family's own dispatch builder.
func (h *Handler) buildDispatchTable() map[string]service.JSONOpFunc {
	builders := []func() map[string]service.JSONOpFunc{
		h.buildConfigurationRecorderDispatch,
		h.buildDeliveryChannelDispatch,
		h.buildConfigRuleDispatch,
		h.buildAggregatorDispatch,
		h.buildConformancePackDispatch,
		h.buildOrganizationDispatch,
		h.buildRetentionDispatch,
		h.buildRemediationDispatch,
		h.buildStoredQueryDispatch,
		h.buildTagDispatch,
		h.buildResourceDispatch,
	}

	table := make(map[string]service.JSONOpFunc)

	for _, build := range builders {
		maps.Copy(table, build())
	}

	return table
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// marshalError serialises a typed AWS error response into bytes.
// Marshaling a struct with only string fields cannot fail; error is intentionally ignored.
func marshalError(errType, message string) []byte {
	payload, _ := json.Marshal(service.JSONErrorResponse{Type: errType, Message: message})

	return payload
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSONBlob(http.StatusNotFound, marshalError("NoSuchConfigurationRecorderException", err.Error()))
	case errors.Is(err, ErrNoSuchDeliveryChannel):
		return c.JSONBlob(http.StatusNotFound, marshalError("NoSuchDeliveryChannelException", err.Error()))
	case errors.Is(err, ErrNoSuchConfigRule):
		return c.JSONBlob(http.StatusNotFound, marshalError("NoSuchConfigRuleException", err.Error()))
	case errors.Is(err, ErrNoSuchAggregator):
		return c.JSONBlob(
			http.StatusNotFound,
			marshalError("NoSuchConfigurationAggregatorException", err.Error()),
		)
	case errors.Is(err, ErrNoSuchConformancePack):
		return c.JSONBlob(http.StatusNotFound, marshalError("NoSuchConformancePackException", err.Error()))
	case errors.Is(err, ErrNoSuchOrganizationConfigRule):
		return c.JSONBlob(
			http.StatusNotFound,
			marshalError("NoSuchOrganizationConfigRuleException", err.Error()),
		)
	case errors.Is(err, ErrNoSuchOrganizationConformancePack):
		return c.JSONBlob(
			http.StatusNotFound,
			marshalError("NoSuchOrganizationConformancePackException", err.Error()),
		)
	case errors.Is(err, ErrResourceNotFound):
		return c.JSONBlob(http.StatusBadRequest, marshalError("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSONBlob(
			http.StatusConflict,
			marshalError("MaxNumberOfConfigurationRecordersExceededException", err.Error()),
		)
	case errors.Is(err, ErrNoDeliveryChannel):
		return c.JSONBlob(http.StatusBadRequest, marshalError("NoAvailableDeliveryChannelException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSONBlob(http.StatusBadRequest, marshalError("ValidationException", err.Error()))
	case errors.Is(err, errUnknownAction), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}
