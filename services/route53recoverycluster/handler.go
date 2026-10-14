// Package route53recoverycluster emulates the Route 53 ARC cluster data plane: routing control state reads and updates.
package route53recoverycluster

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cc "github.com/blackbirdworks/gopherstack/services/route53recoverycontrolconfig"
)

const (
	signingName   = "route53-recovery-cluster"
	targetPrefix  = "ToggleCustomerAPI."
	contentType   = "application/x-amz-json-1.0"
	matchPriority = service.PriorityHeaderExact
	listDefault   = 500
	listMax       = 1000
)

// Handler serves the cluster data plane; state lives in the control-plane backend, resolved at call time.
type Handler struct {
	appConfig     any
	DefaultRegion string
}

// siblingServices is matched structurally against *CLI, which cannot be imported here.
type siblingServices interface {
	GetRoute53RecoveryControlConfigHandler() service.Registerable
}

// NewHandler creates the handler; cfg is service.AppContext.Config.
func NewHandler(cfg any) *Handler { return &Handler{appConfig: cfg} }

func (h *Handler) controlConfig() (*cc.InMemoryBackend, error) {
	if s, ok := h.appConfig.(siblingServices); ok {
		if ch, isCC := s.GetRoute53RecoveryControlConfigHandler().(*cc.Handler); isCC && ch != nil {
			return ch.Backend, nil
		}
	}

	return nil, &apiError{status: http.StatusInternalServerError, code: "InternalServerException",
		message: "The routing control store is unavailable"}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Route53RecoveryCluster" }

// GetSupportedOperations lists every implemented operation.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"GetRoutingControlState",
		"ListRoutingControls",
		"UpdateRoutingControlState",
		"UpdateRoutingControlStates",
	}
}

// ChaosServiceName returns the lowercase service name for fault rules.
func (h *Handler) ChaosServiceName() string { return signingName }

// ChaosOperations returns the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions served.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher matches the ToggleCustomerAPI target or the data-plane signing name.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()

		return strings.HasPrefix(httputils.HeaderValue(r.Header, "X-Amz-Target"), targetPrefix) ||
			(httputils.ExtractServiceFromRequest(r) == signingName && r.Method == http.MethodPost)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	_, op, _ := strings.Cut(httputils.HeaderValue(c.Request().Header, "X-Amz-Target"), ".")
	if op == "" {
		return "Unknown"
	}

	return op
}

// ExtractResource returns no resource label; operations name routing controls in the body.
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		clusterID := clusterFromPath(c.Request().URL.Path)
		dispatch := func(ctx context.Context, action string, body []byte) ([]byte, error) {
			return h.dispatch(ctx, action, clusterID, body)
		}

		return service.HandleTarget(c, logger.Load(c.Request().Context()), "", contentType,
			h.GetSupportedOperations(), dispatch, h.handleError)
	}
}

// clusterFromPath extracts <id> from /route53-recovery-cluster/<id>/<region>/.
func clusterFromPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 2 && parts[0] == signingName {
		return parts[1]
	}

	return ""
}

type apiError struct {
	code       string
	message    string
	resourceID string
	status     int
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var ae *apiError
	if !errors.As(err, &ae) {
		ae = mapError(err)
	}

	c.Response().Header().Set("X-Amzn-Errortype", ae.code)
	c.Response().Header().Set("Content-Type", contentType)

	body := map[string]any{
		"__type":  ae.code,
		"message": ae.message,
		"Message": ae.message,
	}
	if ae.resourceID != "" {
		body["resourceId"] = ae.resourceID
		body["resourceType"] = "SafetyRule"
	}

	return c.JSON(ae.status, body)
}

func mapError(err error) *apiError {
	if conflict, ok := errors.AsType[*cc.ConflictError](err); ok {
		return &apiError{
			status:     http.StatusConflict,
			code:       "ConflictException",
			message:    conflict.Message,
			resourceID: conflict.RuleARN,
		}
	}

	if rest, ok := errors.AsType[*restroute.Error](err); ok {
		return &apiError{status: rest.Status, code: rest.Code, message: rest.Message}
	}

	return &apiError{
		status:  http.StatusInternalServerError,
		code:    "InternalServerException",
		message: "Internal server error",
	}
}
