package route53resolver

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	resolverTargetPrefix  = "Route53Resolver."
	resolverMatchPriority = 100
)

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

func (h *Handler) MatchPriority() int { return resolverMatchPriority }

func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, resolverTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	var req struct {
		Name string `json:"Name"`
	}
	_ = json.Unmarshal(body, &req)

	return req.Name
}

func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to read body"})
		}

		action := strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), resolverTargetPrefix)
		switch action {
		case "CreateResolverEndpoint":
			return h.handleCreateResolverEndpoint(c, body)
		case "DeleteResolverEndpoint":
			return h.handleDeleteResolverEndpoint(c, body)
		case "ListResolverEndpoints":
			return h.handleListResolverEndpoints(c)
		case "GetResolverEndpoint":
			return h.handleGetResolverEndpoint(c, body)
		case "CreateResolverRule":
			return h.handleCreateResolverRule(c, body)
		case "GetResolverRule":
			return h.handleGetResolverRule(c, body)
		case "DeleteResolverRule":
			return h.handleDeleteResolverRule(c, body)
		case "ListResolverRules":
			return h.handleListResolverRules(c)
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"message": "unknown action: " + action})
		}
	}
}

func (h *Handler) handleCreateResolverEndpoint(c *echo.Context, body []byte) error {
	var req struct {
		Name             string   `json:"Name"`
		Direction        string   `json:"Direction"`
		SecurityGroupIDs []string `json:"SecurityGroupIds"`
		IPAddresses      []struct {
			SubnetID string `json:"SubnetId"`
			IP       string `json:"Ip"`
		} `json:"IpAddresses"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	ips := make([]IPAddress, 0, len(req.IPAddresses))
	for _, ip := range req.IPAddresses {
		ips = append(ips, IPAddress{SubnetID: ip.SubnetID, IP: ip.IP})
	}

	ep, err := h.Backend.CreateResolverEndpoint(req.Name, req.Direction, "", ips)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResolverEndpoint": endpointToMap(ep),
	})
}

func (h *Handler) handleDeleteResolverEndpoint(c *echo.Context, body []byte) error {
	var req struct {
		ResolverEndpointID string `json:"ResolverEndpointId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.DeleteResolverEndpoint(req.ResolverEndpointID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (h *Handler) handleListResolverEndpoints(c *echo.Context) error {
	eps := h.Backend.ListResolverEndpoints()
	items := make([]map[string]any, 0, len(eps))
	for _, ep := range eps {
		items = append(items, endpointToMap(ep))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResolverEndpoints": items,
	})
}

func (h *Handler) handleGetResolverEndpoint(c *echo.Context, body []byte) error {
	var req struct {
		ResolverEndpointID string `json:"ResolverEndpointId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	ep, err := h.Backend.GetResolverEndpoint(req.ResolverEndpointID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResolverEndpoint": endpointToMap(ep),
	})
}

func (h *Handler) handleCreateResolverRule(c *echo.Context, body []byte) error {
	var req struct {
		Name               string `json:"Name"`
		DomainName         string `json:"DomainName"`
		RuleType           string `json:"RuleType"`
		ResolverEndpointID string `json:"ResolverEndpointId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	r, err := h.Backend.CreateResolverRule(req.Name, req.DomainName, req.RuleType, req.ResolverEndpointID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResolverRule": ruleToMap(r),
	})
}

func (h *Handler) handleGetResolverRule(c *echo.Context, body []byte) error {
	var req struct {
		ResolverRuleID string `json:"ResolverRuleId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	r, err := h.Backend.GetResolverRule(req.ResolverRuleID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResolverRule": ruleToMap(r),
	})
}

func (h *Handler) handleDeleteResolverRule(c *echo.Context, body []byte) error {
	var req struct {
		ResolverRuleID string `json:"ResolverRuleId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request"})
	}

	if err := h.Backend.DeleteResolverRule(req.ResolverRuleID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (h *Handler) handleListResolverRules(c *echo.Context) error {
	rules := h.Backend.ListResolverRules()
	items := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		items = append(items, ruleToMap(r))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResolverRules": items,
	})
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
