package route53resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const resolverTargetPrefix = "Route53Resolver."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

type resolverEndpointIDInput struct {
	ResolverEndpointID string `json:"ResolverEndpointId"`
}

type resolverRuleIDInput struct {
	ResolverRuleID string `json:"ResolverRuleId"`
}

type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

func (h *Handler) Name() string { return "Route53Resolver" }

func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateResolverEndpoint",
		"DeleteResolverEndpoint",
		"ListResolverEndpoints",
		"GetResolverEndpoint",
		"CreateResolverRule",
		"GetResolverRule",
		"DeleteResolverRule",
		"ListResolverRules",
	}
}

func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), resolverTargetPrefix)
	}
}

func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, resolverTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type extractResolverResourceInput struct {
	Name string `json:"Name"`
}

func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	var req extractResolverResourceInput
	_ = json.Unmarshal(body, &req)

	return req.Name
}

func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Route53Resolver", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	var result any
	var err error

	switch action {
	case "CreateResolverEndpoint":
		result, err = h.handleCreateResolverEndpoint(body)
	case "DeleteResolverEndpoint":
		result, err = h.handleDeleteResolverEndpoint(body)
	case "ListResolverEndpoints":
		result, err = h.handleListResolverEndpoints()
	case "GetResolverEndpoint":
		result, err = h.handleGetResolverEndpoint(body)
	case "CreateResolverRule":
		result, err = h.handleCreateResolverRule(body)
	case "GetResolverRule":
		result, err = h.handleGetResolverRule(body)
	case "DeleteResolverRule":
		result, err = h.handleDeleteResolverRule(body)
	case "ListResolverRules":
		result, err = h.handleListResolverRules()
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

type handleCreateResolverEndpointInput struct {
	Name             string   `json:"Name"`
	Direction        string   `json:"Direction"`
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
	IPAddresses      []struct {
		SubnetID string `json:"SubnetId"`
		IP       string `json:"Ip"`
	} `json:"IpAddresses"`
}

func (h *Handler) handleCreateResolverEndpoint(body []byte) (any, error) {
	var req handleCreateResolverEndpointInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	ips := make([]IPAddress, 0, len(req.IPAddresses))
	for _, ip := range req.IPAddresses {
		ips = append(ips, IPAddress{SubnetID: ip.SubnetID, IP: ip.IP})
	}

	ep, err := h.Backend.CreateResolverEndpoint(req.Name, req.Direction, "", ips)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ResolverEndpoint": endpointToMap(ep),
	}, nil
}

func (h *Handler) handleDeleteResolverEndpoint(body []byte) (any, error) {
	var req resolverEndpointIDInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if err := h.Backend.DeleteResolverEndpoint(req.ResolverEndpointID); err != nil {
		return nil, err
	}

	return map[string]string{}, nil
}

//nolint:unparam // error returned for consistent dispatch signature
func (h *Handler) handleListResolverEndpoints() (any, error) {
	eps := h.Backend.ListResolverEndpoints()
	items := make([]map[string]any, 0, len(eps))
	for _, ep := range eps {
		items = append(items, endpointToMap(ep))
	}

	return map[string]any{
		"ResolverEndpoints": items,
	}, nil
}

func (h *Handler) handleGetResolverEndpoint(body []byte) (any, error) {
	var req resolverEndpointIDInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	ep, err := h.Backend.GetResolverEndpoint(req.ResolverEndpointID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ResolverEndpoint": endpointToMap(ep),
	}, nil
}

type handleCreateResolverRuleInput struct {
	Name               string `json:"Name"`
	DomainName         string `json:"DomainName"`
	RuleType           string `json:"RuleType"`
	ResolverEndpointID string `json:"ResolverEndpointId"`
}

func (h *Handler) handleCreateResolverRule(body []byte) (any, error) {
	var req handleCreateResolverRuleInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	r, err := h.Backend.CreateResolverRule(req.Name, req.DomainName, req.RuleType, req.ResolverEndpointID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ResolverRule": ruleToMap(r),
	}, nil
}

func (h *Handler) handleGetResolverRule(body []byte) (any, error) {
	var req resolverRuleIDInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	r, err := h.Backend.GetResolverRule(req.ResolverRuleID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ResolverRule": ruleToMap(r),
	}, nil
}

func (h *Handler) handleDeleteResolverRule(body []byte) (any, error) {
	var req resolverRuleIDInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, errInvalidRequest
	}

	if err := h.Backend.DeleteResolverRule(req.ResolverRuleID); err != nil {
		return nil, err
	}

	return map[string]string{}, nil
}

//nolint:unparam // error returned for consistent dispatch signature
func (h *Handler) handleListResolverRules() (any, error) {
	rules := h.Backend.ListResolverRules()
	items := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		items = append(items, ruleToMap(r))
	}

	return map[string]any{
		"ResolverRules": items,
	}, nil
}

func endpointToMap(ep *ResolverEndpoint) map[string]any {
	ips := make([]map[string]string, 0, len(ep.IPAddresses))
	for _, ip := range ep.IPAddresses {
		ips = append(ips, map[string]string{"SubnetId": ip.SubnetID, "Ip": ip.IP})
	}

	return map[string]any{
		"Id":          ep.ID,
		"Arn":         ep.ARN,
		"Name":        ep.Name,
		"Direction":   ep.Direction,
		"Status":      ep.Status,
		"IpAddresses": ips,
	}
}

func ruleToMap(r *ResolverRule) map[string]any {
	return map[string]any{
		"Id":                 r.ID,
		"Arn":                r.ARN,
		"Name":               r.Name,
		"DomainName":         r.DomainName,
		"RuleType":           r.RuleType,
		"Status":             r.Status,
		"ResolverEndpointId": r.ResolverEndpointID,
	}
}
