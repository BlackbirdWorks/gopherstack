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
		"AssociateFirewallRuleGroup",
		"AssociateResolverEndpointIpAddress",
		"AssociateResolverQueryLogConfig",
		"AssociateResolverRule",
		"CreateFirewallDomainList",
		"CreateFirewallRule",
		"CreateFirewallRuleGroup",
		"CreateOutpostResolver",
		"CreateResolverEndpoint",
		"CreateResolverQueryLogConfig",
		"CreateResolverRule",
		"DeleteFirewallDomainList",
		"DeleteResolverEndpoint",
		"DeleteResolverRule",
		"GetResolverEndpoint",
		"GetResolverRule",
		"ListResolverEndpointIpAddresses",
		"ListResolverEndpoints",
		"ListResolverRules",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "route53resolver" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Route53 Resolver instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

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
		"AssociateFirewallRuleGroup":         service.WrapOp(h.handleAssociateFirewallRuleGroup),
		"AssociateResolverEndpointIpAddress": service.WrapOp(h.handleAssociateResolverEndpointIPAddress),
		"AssociateResolverQueryLogConfig":    service.WrapOp(h.handleAssociateResolverQueryLogConfig),
		"AssociateResolverRule":              service.WrapOp(h.handleAssociateResolverRule),
		"CreateFirewallDomainList":           service.WrapOp(h.handleCreateFirewallDomainList),
		"CreateFirewallRule":                 service.WrapOp(h.handleCreateFirewallRule),
		"CreateFirewallRuleGroup":            service.WrapOp(h.handleCreateFirewallRuleGroup),
		"CreateOutpostResolver":              service.WrapOp(h.handleCreateOutpostResolver),
		"CreateResolverEndpoint":             service.WrapOp(h.handleCreateResolverEndpoint),
		"CreateResolverQueryLogConfig":       service.WrapOp(h.handleCreateResolverQueryLogConfig),
		"DeleteFirewallDomainList":           service.WrapOp(h.handleDeleteFirewallDomainList),
		"DeleteResolverEndpoint":             service.WrapOp(h.handleDeleteResolverEndpoint),
		"GetResolverEndpoint":                service.WrapOp(h.handleGetResolverEndpoint),
		"ListResolverEndpoints":              service.WrapOp(h.handleListResolverEndpoints),
		"ListResolverEndpointIpAddresses":    service.WrapOp(h.handleListResolverEndpointIPAddresses),
		"CreateResolverRule":                 service.WrapOp(h.handleCreateResolverRule),
		"GetResolverRule":                    service.WrapOp(h.handleGetResolverRule),
		"DeleteResolverRule":                 service.WrapOp(h.handleDeleteResolverRule),
		"ListResolverRules":                  service.WrapOp(h.handleListResolverRules),
		"ListTagsForResource":                service.WrapOp(h.handleListTagsForResource),
		"TagResource":                        service.WrapOp(h.handleTagResource),
		"UntagResource":                      service.WrapOp(h.handleUntagResource),
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
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
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

type resolverEndpointIPAddressDetail struct {
	IPID     string `json:"IpId"`
	SubnetID string `json:"SubnetId"`
	IP       string `json:"Ip"`
	Status   string `json:"Status"`
}

type listResolverEndpointIPAddressesInput struct {
	ResolverEndpointID string `json:"ResolverEndpointId"`
}

type listResolverEndpointIPAddressesOutput struct {
	IPAddresses []resolverEndpointIPAddressDetail `json:"IpAddresses"`
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
		ips = append(ips, resolverEndpointIPOutput{SubnetID: ip.SubnetID, IP: ip.IP})
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
		ips = append(ips, IPAddress{SubnetID: ip.SubnetID, IP: ip.IP})
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

func (h *Handler) handleListResolverEndpointIPAddresses(
	_ context.Context,
	in *listResolverEndpointIPAddressesInput,
) (*listResolverEndpointIPAddressesOutput, error) {
	ips, err := h.Backend.ListResolverEndpointIPAddresses(in.ResolverEndpointID)
	if err != nil {
		return nil, err
	}
	items := make([]resolverEndpointIPAddressDetail, 0, len(ips))
	for _, ip := range ips {
		items = append(items, resolverEndpointIPAddressDetail{
			IPID:     ip.IPID,
			SubnetID: ip.SubnetID,
			IP:       ip.IP,
			Status:   "ATTACHED",
		})
	}

	return &listResolverEndpointIPAddressesOutput{IPAddresses: items}, nil
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

// handleListTagsForResource returns tags for the given resource ARN.
func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	kvs := h.Backend.ListTagsForResource(in.ResourceArn)

	return &listTagsForResourceOutput{Tags: kvs}, nil
}

type tagResourceInput struct {
	ResourceArn string       `json:"ResourceArn"`
	Tags        []svcTags.KV `json:"Tags"`
}

type tagResourceOutput struct{}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// --- New operations ---

// firewallRuleGroupOutput is the JSON representation of a FirewallRuleGroup.
type firewallRuleGroupOutput struct {
	ID               string `json:"Id"`
	Arn              string `json:"Arn"`
	Name             string `json:"Name"`
	CreatorRequestID string `json:"CreatorRequestId"`
	Status           string `json:"Status"`
	OwnerID          string `json:"OwnerId"`
	RuleCount        int32  `json:"RuleCount"`
}

// firewallRuleGroupAssociationOutput is the JSON representation of a FirewallRuleGroupAssociation.
type firewallRuleGroupAssociationOutput struct {
	ID                  string `json:"Id"`
	Arn                 string `json:"Arn"`
	Name                string `json:"Name"`
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
	VpcID               string `json:"VpcId"`
	Status              string `json:"Status"`
	Priority            int32  `json:"Priority"`
}

// firewallDomainListOutput is the JSON representation of a FirewallDomainList.
type firewallDomainListOutput struct {
	ID               string `json:"Id"`
	Arn              string `json:"Arn"`
	Name             string `json:"Name"`
	CreatorRequestID string `json:"CreatorRequestId"`
	Status           string `json:"Status"`
	DomainCount      int32  `json:"DomainCount"`
}

// firewallRuleOutput is the JSON representation of a FirewallRule.
type firewallRuleOutput struct {
	Name                 string `json:"Name"`
	FirewallRuleGroupID  string `json:"FirewallRuleGroupId"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
	Action               string `json:"Action"`
	BlockResponse        string `json:"BlockResponse,omitempty"`
	Priority             int32  `json:"Priority"`
}

// outpostResolverOutput is the JSON representation of an OutpostResolver.
type outpostResolverOutput struct {
	ID                    string `json:"Id"`
	Arn                   string `json:"Arn"`
	Name                  string `json:"Name"`
	CreatorRequestID      string `json:"CreatorRequestId"`
	OutpostArn            string `json:"OutpostArn"`
	PreferredInstanceType string `json:"PreferredInstanceType"`
	Status                string `json:"Status"`
	InstanceCount         int32  `json:"InstanceCount"`
}

// resolverQueryLogConfigOutput is the JSON representation of a ResolverQueryLogConfig.
type resolverQueryLogConfigOutput struct {
	ID               string `json:"Id"`
	Arn              string `json:"Arn"`
	Name             string `json:"Name"`
	CreatorRequestID string `json:"CreatorRequestId"`
	DestinationArn   string `json:"DestinationArn"`
	Status           string `json:"Status"`
	OwnerID          string `json:"OwnerId"`
}

// resolverQueryLogConfigAssociationOutput is the JSON representation of a ResolverQueryLogConfigAssociation.
type resolverQueryLogConfigAssociationOutput struct {
	ID                       string `json:"Id"`
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
	ResourceID               string `json:"ResourceId"`
	Status                   string `json:"Status"`
}

// resolverRuleAssociationOutput is the JSON representation of a ResolverRuleAssociation.
type resolverRuleAssociationOutput struct {
	ID             string `json:"Id"`
	Name           string `json:"Name"`
	ResolverRuleID string `json:"ResolverRuleId"`
	VPCId          string `json:"VPCId"`
	Status         string `json:"Status"`
}

// --- CreateFirewallRuleGroup ---

type createFirewallRuleGroupInput struct {
	CreatorRequestID string `json:"CreatorRequestId"`
	Name             string `json:"Name"`
}

type createFirewallRuleGroupOutput struct {
	FirewallRuleGroup firewallRuleGroupOutput `json:"FirewallRuleGroup"`
}

func firewallRuleGroupToOutput(g *FirewallRuleGroup) firewallRuleGroupOutput {
	return firewallRuleGroupOutput{
		ID:               g.ID,
		Arn:              g.ARN,
		Name:             g.Name,
		CreatorRequestID: g.CreatorRequestID,
		Status:           g.Status,
		OwnerID:          g.OwnerID,
		RuleCount:        g.RuleCount,
	}
}

func (h *Handler) handleCreateFirewallRuleGroup(
	_ context.Context,
	in *createFirewallRuleGroupInput,
) (*createFirewallRuleGroupOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	g, err := h.Backend.CreateFirewallRuleGroup(in.Name, in.CreatorRequestID)
	if err != nil {
		return nil, err
	}

	return &createFirewallRuleGroupOutput{FirewallRuleGroup: firewallRuleGroupToOutput(g)}, nil
}

// --- AssociateFirewallRuleGroup ---

type associateFirewallRuleGroupInput struct {
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
	Name                string `json:"Name"`
	VpcID               string `json:"VpcId"`
	CreatorRequestID    string `json:"CreatorRequestId"`
	Priority            int32  `json:"Priority"`
}

type associateFirewallRuleGroupOutput struct {
	FirewallRuleGroupAssociation firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociation"`
}

func firewallRuleGroupAssociationToOutput(a *FirewallRuleGroupAssociation) firewallRuleGroupAssociationOutput {
	return firewallRuleGroupAssociationOutput{
		ID:                  a.ID,
		Arn:                 a.ARN,
		Name:                a.Name,
		FirewallRuleGroupID: a.FirewallRuleGroupID,
		VpcID:               a.VpcID,
		Priority:            a.Priority,
		Status:              a.Status,
	}
}

func (h *Handler) handleAssociateFirewallRuleGroup(
	_ context.Context,
	in *associateFirewallRuleGroupInput,
) (*associateFirewallRuleGroupOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}

	if in.VpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrValidation)
	}

	assoc, err := h.Backend.AssociateFirewallRuleGroup(
		in.FirewallRuleGroupID, in.VpcID, in.Name, in.CreatorRequestID, in.Priority,
	)
	if err != nil {
		return nil, err
	}

	return &associateFirewallRuleGroupOutput{
		FirewallRuleGroupAssociation: firewallRuleGroupAssociationToOutput(assoc),
	}, nil
}

// --- AssociateResolverEndpointIpAddress ---

type ipAddressUpdateInput struct {
	SubnetID string `json:"SubnetId"`
	IP       string `json:"Ip"`
}

type associateResolverEndpointIPAddressInput struct {
	ResolverEndpointID string               `json:"ResolverEndpointId"`
	IPAddress          ipAddressUpdateInput `json:"IpAddress"`
}

type associateResolverEndpointIPAddressOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

func (h *Handler) handleAssociateResolverEndpointIPAddress(
	_ context.Context,
	in *associateResolverEndpointIPAddressInput,
) (*associateResolverEndpointIPAddressOutput, error) {
	if in.ResolverEndpointID == "" {
		return nil, fmt.Errorf("%w: ResolverEndpointId is required", ErrValidation)
	}

	ep, err := h.Backend.AssociateResolverEndpointIPAddress(
		in.ResolverEndpointID, in.IPAddress.SubnetID, in.IPAddress.IP,
	)
	if err != nil {
		return nil, err
	}

	return &associateResolverEndpointIPAddressOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

// --- CreateResolverQueryLogConfig ---

type createResolverQueryLogConfigInput struct {
	CreatorRequestID string `json:"CreatorRequestId"`
	DestinationArn   string `json:"DestinationArn"`
	Name             string `json:"Name"`
}

type createResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfig resolverQueryLogConfigOutput `json:"ResolverQueryLogConfig"`
}

func queryLogConfigToOutput(c *ResolverQueryLogConfig) resolverQueryLogConfigOutput {
	return resolverQueryLogConfigOutput{
		ID:               c.ID,
		Arn:              c.ARN,
		Name:             c.Name,
		CreatorRequestID: c.CreatorRequestID,
		DestinationArn:   c.DestinationARN,
		Status:           c.Status,
		OwnerID:          c.OwnerID,
	}
}

func (h *Handler) handleCreateResolverQueryLogConfig(
	_ context.Context,
	in *createResolverQueryLogConfigInput,
) (*createResolverQueryLogConfigOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if in.DestinationArn == "" {
		return nil, fmt.Errorf("%w: DestinationArn is required", ErrValidation)
	}

	cfg, err := h.Backend.CreateResolverQueryLogConfig(in.Name, in.CreatorRequestID, in.DestinationArn)
	if err != nil {
		return nil, err
	}

	return &createResolverQueryLogConfigOutput{ResolverQueryLogConfig: queryLogConfigToOutput(cfg)}, nil
}

// --- AssociateResolverQueryLogConfig ---

type associateResolverQueryLogConfigInput struct {
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
	ResourceID               string `json:"ResourceId"`
}

type associateResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfigAssociation resolverQueryLogConfigAssociationOutput `json:"ResolverQueryLogConfigAssociation"`
}

func queryLogConfigAssociationToOutput(a *ResolverQueryLogConfigAssociation) resolverQueryLogConfigAssociationOutput {
	return resolverQueryLogConfigAssociationOutput{
		ID:                       a.ID,
		ResolverQueryLogConfigID: a.ResolverQueryLogConfigID,
		ResourceID:               a.ResourceID,
		Status:                   a.Status,
	}
}

func (h *Handler) handleAssociateResolverQueryLogConfig(
	_ context.Context,
	in *associateResolverQueryLogConfigInput,
) (*associateResolverQueryLogConfigOutput, error) {
	if in.ResolverQueryLogConfigID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigId is required", ErrValidation)
	}

	if in.ResourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}

	assoc, err := h.Backend.AssociateResolverQueryLogConfig(in.ResolverQueryLogConfigID, in.ResourceID)
	if err != nil {
		return nil, err
	}

	return &associateResolverQueryLogConfigOutput{
		ResolverQueryLogConfigAssociation: queryLogConfigAssociationToOutput(assoc),
	}, nil
}

// --- AssociateResolverRule ---

type associateResolverRuleInput struct {
	ResolverRuleID string `json:"ResolverRuleId"`
	VPCId          string `json:"VPCId"`
	Name           string `json:"Name"`
}

type associateResolverRuleOutput struct {
	ResolverRuleAssociation resolverRuleAssociationOutput `json:"ResolverRuleAssociation"`
}

func ruleAssociationToOutput(a *ResolverRuleAssociation) resolverRuleAssociationOutput {
	return resolverRuleAssociationOutput{
		ID:             a.ID,
		Name:           a.Name,
		ResolverRuleID: a.ResolverRuleID,
		VPCId:          a.VPCID,
		Status:         a.Status,
	}
}

func (h *Handler) handleAssociateResolverRule(
	_ context.Context,
	in *associateResolverRuleInput,
) (*associateResolverRuleOutput, error) {
	if in.ResolverRuleID == "" {
		return nil, fmt.Errorf("%w: ResolverRuleId is required", ErrValidation)
	}

	if in.VPCId == "" {
		return nil, fmt.Errorf("%w: VPCId is required", ErrValidation)
	}

	assoc, err := h.Backend.AssociateResolverRule(in.ResolverRuleID, in.VPCId, in.Name)
	if err != nil {
		return nil, err
	}

	return &associateResolverRuleOutput{ResolverRuleAssociation: ruleAssociationToOutput(assoc)}, nil
}

// --- CreateFirewallDomainList ---

type createFirewallDomainListInput struct {
	CreatorRequestID string `json:"CreatorRequestId"`
	Name             string `json:"Name"`
}

type createFirewallDomainListOutput struct {
	FirewallDomainList firewallDomainListOutput `json:"FirewallDomainList"`
}

func firewallDomainListToOutput(dl *FirewallDomainList) firewallDomainListOutput {
	return firewallDomainListOutput{
		ID:               dl.ID,
		Arn:              dl.ARN,
		Name:             dl.Name,
		CreatorRequestID: dl.CreatorRequestID,
		Status:           dl.Status,
		DomainCount:      dl.DomainCount,
	}
}

func (h *Handler) handleCreateFirewallDomainList(
	_ context.Context,
	in *createFirewallDomainListInput,
) (*createFirewallDomainListOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	dl, err := h.Backend.CreateFirewallDomainList(in.Name, in.CreatorRequestID)
	if err != nil {
		return nil, err
	}

	return &createFirewallDomainListOutput{FirewallDomainList: firewallDomainListToOutput(dl)}, nil
}

// --- DeleteFirewallDomainList ---

type deleteFirewallDomainListInput struct {
	FirewallDomainListID string `json:"FirewallDomainListId"`
}

type deleteFirewallDomainListOutput struct {
	FirewallDomainList firewallDomainListOutput `json:"FirewallDomainList"`
}

func (h *Handler) handleDeleteFirewallDomainList(
	_ context.Context,
	in *deleteFirewallDomainListInput,
) (*deleteFirewallDomainListOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}

	dl, err := h.Backend.DeleteFirewallDomainList(in.FirewallDomainListID)
	if err != nil {
		return nil, err
	}

	return &deleteFirewallDomainListOutput{FirewallDomainList: firewallDomainListToOutput(dl)}, nil
}

// --- CreateFirewallRule ---

type createFirewallRuleInput struct {
	Action               string `json:"Action"`
	CreatorRequestID     string `json:"CreatorRequestId"`
	FirewallRuleGroupID  string `json:"FirewallRuleGroupId"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
	Name                 string `json:"Name"`
	BlockResponse        string `json:"BlockResponse"`
	Priority             int32  `json:"Priority"`
}

type createFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func firewallRuleToOutput(r *FirewallRule) firewallRuleOutput {
	return firewallRuleOutput{
		Name:                 r.Name,
		FirewallRuleGroupID:  r.FirewallRuleGroupID,
		FirewallDomainListID: r.FirewallDomainListID,
		Action:               r.Action,
		Priority:             r.Priority,
		BlockResponse:        r.BlockResponse,
	}
}

func (h *Handler) handleCreateFirewallRule(
	_ context.Context,
	in *createFirewallRuleInput,
) (*createFirewallRuleOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}

	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if in.Action == "" {
		return nil, fmt.Errorf("%w: Action is required", ErrValidation)
	}

	rule, err := h.Backend.CreateFirewallRule(
		in.FirewallRuleGroupID, in.Name, in.Action, in.CreatorRequestID,
		in.Priority, in.FirewallDomainListID,
	)
	if err != nil {
		return nil, err
	}

	return &createFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// --- CreateOutpostResolver ---

type createOutpostResolverInput struct {
	CreatorRequestID      string `json:"CreatorRequestId"`
	Name                  string `json:"Name"`
	OutpostArn            string `json:"OutpostArn"`
	PreferredInstanceType string `json:"PreferredInstanceType"`
	InstanceCount         int32  `json:"InstanceCount"`
}

type createOutpostResolverOutput struct {
	OutpostResolver outpostResolverOutput `json:"OutpostResolver"`
}

func outpostResolverToOutput(r *OutpostResolver) outpostResolverOutput {
	return outpostResolverOutput{
		ID:                    r.ID,
		Arn:                   r.ARN,
		Name:                  r.Name,
		CreatorRequestID:      r.CreatorRequestID,
		OutpostArn:            r.OutpostARN,
		PreferredInstanceType: r.PreferredInstanceType,
		InstanceCount:         r.InstanceCount,
		Status:                r.Status,
	}
}

func (h *Handler) handleCreateOutpostResolver(
	_ context.Context,
	in *createOutpostResolverInput,
) (*createOutpostResolverOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if in.OutpostArn == "" {
		return nil, fmt.Errorf("%w: OutpostArn is required", ErrValidation)
	}

	if in.PreferredInstanceType == "" {
		return nil, fmt.Errorf("%w: PreferredInstanceType is required", ErrValidation)
	}

	r, err := h.Backend.CreateOutpostResolver(
		in.Name, in.CreatorRequestID, in.OutpostArn, in.PreferredInstanceType, in.InstanceCount,
	)
	if err != nil {
		return nil, err
	}

	return &createOutpostResolverOutput{OutpostResolver: outpostResolverToOutput(r)}, nil
}
