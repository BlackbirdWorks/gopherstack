package route53resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
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
}

func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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
		"ListTagsForResource",
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
	body, err := httputils.ReadBody(c.Request())
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

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateResolverEndpoint": service.WrapOp(h.handleCreateResolverEndpoint),
		"DeleteResolverEndpoint": service.WrapOp(h.handleDeleteResolverEndpoint),
		"ListResolverEndpoints":  service.WrapOp(h.handleListResolverEndpoints),
		"GetResolverEndpoint":    service.WrapOp(h.handleGetResolverEndpoint),
		"CreateResolverRule":     service.WrapOp(h.handleCreateResolverRule),
		"GetResolverRule":        service.WrapOp(h.handleGetResolverRule),
		"DeleteResolverRule":     service.WrapOp(h.handleDeleteResolverRule),
		"ListResolverRules":      service.WrapOp(h.handleListResolverRules),
		"ListTagsForResource":    service.WrapOp(h.handleListTagsForResource),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ResourceNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// resolverEndpointIPAddress holds the subnet and IP for a resolver endpoint IP address.
type resolverEndpointIPAddress struct {
	SubnetID string `json:"SubnetId"`
	IP       string `json:"Ip"`
}

type handleCreateResolverEndpointInput struct {
	Name             string                      `json:"Name"`
	Direction        string                      `json:"Direction"`
	SecurityGroupIDs []string                    `json:"SecurityGroupIds"`
	IPAddresses      []resolverEndpointIPAddress `json:"IpAddresses"`
}

type handleCreateResolverRuleInput struct {
	Name               string `json:"Name"`
	DomainName         string `json:"DomainName"`
	RuleType           string `json:"RuleType"`
	ResolverEndpointID string `json:"ResolverEndpointId"`
}

type resolverEndpointIPOutput struct {
	SubnetID string `json:"SubnetId"`
	IP       string `json:"Ip"`
}

type resolverEndpointOutput struct {
	ID          string                     `json:"Id"`
	Arn         string                     `json:"Arn"`
	Name        string                     `json:"Name"`
	Direction   string                     `json:"Direction"`
	Status      string                     `json:"Status"`
	IPAddresses []resolverEndpointIPOutput `json:"IpAddresses"`
}

type resolverRuleOutput struct {
	ID                 string `json:"Id"`
	Arn                string `json:"Arn"`
	Name               string `json:"Name"`
	DomainName         string `json:"DomainName"`
	RuleType           string `json:"RuleType"`
	Status             string `json:"Status"`
	ResolverEndpointID string `json:"ResolverEndpointId"`
}

type createResolverEndpointOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

type deleteResolverEndpointOutput struct{}

type listResolverEndpointsInput struct{}

type listResolverEndpointsOutput struct {
	ResolverEndpoints []resolverEndpointOutput `json:"ResolverEndpoints"`
}

type getResolverEndpointOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

type createResolverRuleOutput struct {
	ResolverRule resolverRuleOutput `json:"ResolverRule"`
}

type getResolverRuleOutput struct {
	ResolverRule resolverRuleOutput `json:"ResolverRule"`
}

type deleteResolverRuleOutput struct{}

type listResolverRulesInput struct{}

type listResolverRulesOutput struct {
	ResolverRules []resolverRuleOutput `json:"ResolverRules"`
}

func endpointToOutput(ep *ResolverEndpoint) resolverEndpointOutput {
	ips := make([]resolverEndpointIPOutput, 0, len(ep.IPAddresses))
	for _, ip := range ep.IPAddresses {
		ips = append(ips, resolverEndpointIPOutput(ip))
	}

	return resolverEndpointOutput{
		ID: ep.ID, Arn: ep.ARN, Name: ep.Name,
		Direction: ep.Direction, Status: ep.Status, IPAddresses: ips,
	}
}

func ruleToOutput(r *ResolverRule) resolverRuleOutput {
	return resolverRuleOutput{
		ID:                 r.ID,
		Arn:                r.ARN,
		Name:               r.Name,
		DomainName:         r.DomainName,
		RuleType:           r.RuleType,
		Status:             r.Status,
		ResolverEndpointID: r.ResolverEndpointID,
	}
}

func (h *Handler) handleCreateResolverEndpoint(
	_ context.Context,
	in *handleCreateResolverEndpointInput,
) (*createResolverEndpointOutput, error) {
	ips := make([]IPAddress, 0, len(in.IPAddresses))
	for _, ip := range in.IPAddresses {
		ips = append(ips, IPAddress(ip))
	}

	ep, err := h.Backend.CreateResolverEndpoint(in.Name, in.Direction, "", ips)
	if err != nil {
		return nil, err
	}

	return &createResolverEndpointOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

func (h *Handler) handleDeleteResolverEndpoint(
	_ context.Context,
	in *resolverEndpointIDInput,
) (*deleteResolverEndpointOutput, error) {
	if err := h.Backend.DeleteResolverEndpoint(in.ResolverEndpointID); err != nil {
		return nil, err
	}

	return &deleteResolverEndpointOutput{}, nil
}

func (h *Handler) handleListResolverEndpoints(
	_ context.Context,
	_ *listResolverEndpointsInput,
) (*listResolverEndpointsOutput, error) {
	eps := h.Backend.ListResolverEndpoints()
	items := make([]resolverEndpointOutput, 0, len(eps))
	for _, ep := range eps {
		items = append(items, endpointToOutput(ep))
	}

	return &listResolverEndpointsOutput{ResolverEndpoints: items}, nil
}

func (h *Handler) handleGetResolverEndpoint(
	_ context.Context,
	in *resolverEndpointIDInput,
) (*getResolverEndpointOutput, error) {
	ep, err := h.Backend.GetResolverEndpoint(in.ResolverEndpointID)
	if err != nil {
		return nil, err
	}

	return &getResolverEndpointOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

func (h *Handler) handleCreateResolverRule(
	_ context.Context,
	in *handleCreateResolverRuleInput,
) (*createResolverRuleOutput, error) {
	r, err := h.Backend.CreateResolverRule(in.Name, in.DomainName, in.RuleType, in.ResolverEndpointID)
	if err != nil {
		return nil, err
	}

	return &createResolverRuleOutput{ResolverRule: ruleToOutput(r)}, nil
}

func (h *Handler) handleGetResolverRule(_ context.Context, in *resolverRuleIDInput) (*getResolverRuleOutput, error) {
	r, err := h.Backend.GetResolverRule(in.ResolverRuleID)
	if err != nil {
		return nil, err
	}

	return &getResolverRuleOutput{ResolverRule: ruleToOutput(r)}, nil
}

func (h *Handler) handleDeleteResolverRule(
	_ context.Context,
	in *resolverRuleIDInput,
) (*deleteResolverRuleOutput, error) {
	if err := h.Backend.DeleteResolverRule(in.ResolverRuleID); err != nil {
		return nil, err
	}

	return &deleteResolverRuleOutput{}, nil
}

func (h *Handler) handleListResolverRules(
	_ context.Context,
	_ *listResolverRulesInput,
) (*listResolverRulesOutput, error) {
	rules := h.Backend.ListResolverRules()
	items := make([]resolverRuleOutput, 0, len(rules))
	for _, r := range rules {
		items = append(items, ruleToOutput(r))
	}

	return &listResolverRulesOutput{ResolverRules: items}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []svcTags.KV `json:"Tags"`
}

// handleListTagsForResource returns an empty tag list.
// Terraform calls this after creating a Route53 Resolver rule to read tags.
func (h *Handler) handleListTagsForResource(
	_ context.Context,
	_ *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	return &listTagsForResourceOutput{Tags: []svcTags.KV{}}, nil
}
