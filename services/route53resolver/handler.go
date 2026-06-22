package route53resolver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	keyMessageField = "message"

	defaultPageSizeSmall = 10
	defaultPageSizeLarge = 100
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
	Backend           StorageBackend
	ops               map[string]service.JSONOpFunc
	supportedOpsCache []string
}

func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()
	// Pre-compute the sorted ops slice once.
	keys := collections.SortedKeys(h.ops)
	h.supportedOpsCache = keys

	return h
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc { //nolint:funlen
	return map[string]service.JSONOpFunc{
		"AssociateFirewallRuleGroup": service.WrapOp(
			h.handleAssociateFirewallRuleGroup,
		),
		"AssociateResolverEndpointIpAddress": service.WrapOp(
			h.handleAssociateResolverEndpointIPAddress,
		),
		"AssociateResolverQueryLogConfig": service.WrapOp(
			h.handleAssociateResolverQueryLogConfig,
		),
		"AssociateResolverRule":    service.WrapOp(h.handleAssociateResolverRule),
		"CreateFirewallDomainList": service.WrapOp(h.handleCreateFirewallDomainList),
		"CreateFirewallRule":       service.WrapOp(h.handleCreateFirewallRule),
		"CreateFirewallRuleGroup":  service.WrapOp(h.handleCreateFirewallRuleGroup),
		"CreateOutpostResolver":    service.WrapOp(h.handleCreateOutpostResolver),
		"CreateResolverEndpoint":   service.WrapOp(h.handleCreateResolverEndpoint),
		"CreateResolverQueryLogConfig": service.WrapOp(
			h.handleCreateResolverQueryLogConfig,
		),
		"CreateResolverRule":       service.WrapOp(h.handleCreateResolverRule),
		"DeleteFirewallDomainList": service.WrapOp(h.handleDeleteFirewallDomainList),
		"DeleteFirewallRule":       service.WrapOp(h.handleDeleteFirewallRule),
		"DeleteFirewallRuleGroup":  service.WrapOp(h.handleDeleteFirewallRuleGroup),
		"DeleteOutpostResolver":    service.WrapOp(h.handleDeleteOutpostResolver),
		"DeleteResolverEndpoint":   service.WrapOp(h.handleDeleteResolverEndpoint),
		"DeleteResolverQueryLogConfig": service.WrapOp(
			h.handleDeleteResolverQueryLogConfig,
		),
		"DeleteResolverRule": service.WrapOp(h.handleDeleteResolverRule),
		"DisassociateFirewallRuleGroup": service.WrapOp(
			h.handleDisassociateFirewallRuleGroup,
		),
		"DisassociateResolverEndpointIpAddress": service.WrapOp(
			h.handleDisassociateResolverEndpointIPAddress,
		),
		"DisassociateResolverQueryLogConfig": service.WrapOp(
			h.handleDisassociateResolverQueryLogConfig,
		),
		"DisassociateResolverRule": service.WrapOp(h.handleDisassociateResolverRule),
		"GetFirewallConfig":        service.WrapOp(h.handleGetFirewallConfig),
		"GetFirewallDomainList":    service.WrapOp(h.handleGetFirewallDomainList),
		"GetFirewallRuleGroup":     service.WrapOp(h.handleGetFirewallRuleGroup),
		"GetFirewallRuleGroupAssociation": service.WrapOp(
			h.handleGetFirewallRuleGroupAssociation,
		),
		"GetFirewallRuleGroupPolicy": service.WrapOp(
			h.handleGetFirewallRuleGroupPolicy,
		),
		"GetOutpostResolver":        service.WrapOp(h.handleGetOutpostResolver),
		"GetResolverConfig":         service.WrapOp(h.handleGetResolverConfig),
		"GetResolverDnssecConfig":   service.WrapOp(h.handleGetResolverDnssecConfig),
		"GetResolverEndpoint":       service.WrapOp(h.handleGetResolverEndpoint),
		"GetResolverQueryLogConfig": service.WrapOp(h.handleGetResolverQueryLogConfig),
		"GetResolverQueryLogConfigAssociation": service.WrapOp(
			h.handleGetResolverQueryLogConfigAssociation,
		),
		"GetResolverQueryLogConfigPolicy": service.WrapOp(
			h.handleGetResolverQueryLogConfigPolicy,
		),
		"GetResolverRule": service.WrapOp(h.handleGetResolverRule),
		"GetResolverRuleAssociation": service.WrapOp(
			h.handleGetResolverRuleAssociation,
		),
		"GetResolverRulePolicy":   service.WrapOp(h.handleGetResolverRulePolicy),
		"ImportFirewallDomains":   service.WrapOp(h.handleImportFirewallDomains),
		"ListFirewallConfigs":     service.WrapOp(h.handleListFirewallConfigs),
		"ListFirewallDomainLists": service.WrapOp(h.handleListFirewallDomainLists),
		"ListFirewallDomains":     service.WrapOp(h.handleListFirewallDomains),
		"ListFirewallRuleGroupAssociations": service.WrapOp(
			h.handleListFirewallRuleGroupAssociations,
		),
		"ListFirewallRuleGroups":    service.WrapOp(h.handleListFirewallRuleGroups),
		"ListFirewallRules":         service.WrapOp(h.handleListFirewallRules),
		"ListOutpostResolvers":      service.WrapOp(h.handleListOutpostResolvers),
		"ListResolverConfigs":       service.WrapOp(h.handleListResolverConfigs),
		"ListResolverDnssecConfigs": service.WrapOp(h.handleListResolverDnssecConfigs),
		"ListResolverEndpointIpAddresses": service.WrapOp(
			h.handleListResolverEndpointIPAddresses,
		),
		"ListResolverEndpoints": service.WrapOp(h.handleListResolverEndpoints),
		"ListResolverQueryLogConfigAssociations": service.WrapOp(
			h.handleListResolverQueryLogConfigAssociations,
		),
		"ListResolverQueryLogConfigs": service.WrapOp(
			h.handleListResolverQueryLogConfigs,
		),
		"ListResolverRuleAssociations": service.WrapOp(
			h.handleListResolverRuleAssociations,
		),
		"ListResolverRules":   service.WrapOp(h.handleListResolverRules),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		"PutFirewallRuleGroupPolicy": service.WrapOp(
			h.handlePutFirewallRuleGroupPolicy,
		),
		"PutResolverQueryLogConfigPolicy": service.WrapOp(
			h.handlePutResolverQueryLogConfigPolicy,
		),
		"PutResolverRulePolicy": service.WrapOp(h.handlePutResolverRulePolicy),
		"TagResource":           service.WrapOp(h.handleTagResource),
		"UntagResource":         service.WrapOp(h.handleUntagResource),
		"UpdateFirewallConfig":  service.WrapOp(h.handleUpdateFirewallConfig),
		"UpdateFirewallDomains": service.WrapOp(h.handleUpdateFirewallDomains),
		"UpdateFirewallRule":    service.WrapOp(h.handleUpdateFirewallRule),
		"UpdateFirewallRuleGroupAssociation": service.WrapOp(
			h.handleUpdateFirewallRuleGroupAssociation,
		),
		"UpdateOutpostResolver": service.WrapOp(h.handleUpdateOutpostResolver),
		"UpdateResolverConfig":  service.WrapOp(h.handleUpdateResolverConfig),
		"UpdateResolverDnssecConfig": service.WrapOp(
			h.handleUpdateResolverDnssecConfig,
		),
		"UpdateResolverEndpoint": service.WrapOp(h.handleUpdateResolverEndpoint),
		"UpdateResolverRule":     service.WrapOp(h.handleUpdateResolverRule),
	}
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

func (h *Handler) Name() string { return "Route53Resolver" }

func (h *Handler) GetSupportedOperations() []string {
	return h.supportedOpsCache
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
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Route53Resolver", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleError,
		)
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
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
	case errors.Is(err, ErrInvalidParameter):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidParameterException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrValidation):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidRequestException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
	default:
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}
}

// resolverEndpointIPAddress holds the subnet and IP for a resolver endpoint IP address.
type resolverEndpointIPAddress struct {
	SubnetID string `json:"SubnetId"`
	IP       string `json:"Ip"`
	Ipv6     string `json:"Ipv6,omitempty"`
}

type resolverEndpointIPAddressDetail struct {
	IPID     string `json:"IpId"`
	SubnetID string `json:"SubnetId"`
	IP       string `json:"Ip"`
	Ipv6     string `json:"Ipv6,omitempty"`
	Status   string `json:"Status"`
}

type listResolverEndpointIPAddressesInput struct {
	ResolverEndpointID string `json:"ResolverEndpointId"`
}

type listResolverEndpointIPAddressesOutput struct {
	IPAddresses []resolverEndpointIPAddressDetail `json:"IpAddresses"`
}

type handleCreateResolverEndpointInput struct {
	Name                  string                      `json:"Name"`
	Direction             string                      `json:"Direction"`
	VpcID                 string                      `json:"VpcId"`
	SecurityGroupIDs      []string                    `json:"SecurityGroupIds"`
	ResolverEndpointType  string                      `json:"ResolverEndpointType"`
	Protocols             []string                    `json:"Protocols"`
	OutpostArn            string                      `json:"OutpostArn"`
	PreferredInstanceType string                      `json:"PreferredInstanceType"`
	CreatorRequestID      string                      `json:"CreatorRequestId"`
	Tags                  []svcTags.KV                `json:"Tags"`
	IPAddresses           []resolverEndpointIPAddress `json:"IpAddresses"`
}

type targetIP struct {
	IP       string `json:"Ip"`
	Ipv6     string `json:"Ipv6,omitempty"`
	Protocol string `json:"Protocol,omitempty"`
	Port     int32  `json:"Port"`
}

type resolverEndpointIPOutput struct {
	SubnetID string `json:"SubnetId"`
	IP       string `json:"Ip"`
	Ipv6     string `json:"Ipv6,omitempty"`
}

type resolverEndpointOutput struct {
	ID                    string                     `json:"Id"`
	Arn                   string                     `json:"Arn"`
	Name                  string                     `json:"Name"`
	Direction             string                     `json:"Direction"`
	Status                string                     `json:"Status"`
	StatusMessage         string                     `json:"StatusMessage,omitempty"`
	VpcID                 string                     `json:"VpcId"`
	HostVPCId             string                     `json:"HostVPCId"`
	ResolverEndpointType  string                     `json:"ResolverEndpointType"`
	OutpostArn            string                     `json:"OutpostArn,omitempty"`
	PreferredInstanceType string                     `json:"PreferredInstanceType,omitempty"`
	CreatorRequestID      string                     `json:"CreatorRequestId,omitempty"`
	CreationTime          string                     `json:"CreationTime,omitempty"`
	ModificationTime      string                     `json:"ModificationTime,omitempty"`
	SecurityGroupIDs      []string                   `json:"SecurityGroupIds"`
	IPAddresses           []resolverEndpointIPOutput `json:"IpAddresses"`
	Protocols             []string                   `json:"Protocols,omitempty"`
	IPAddressCount        int32                      `json:"IpAddressCount"`
}

type resolverRuleOutput struct {
	ID                 string     `json:"Id"`
	Arn                string     `json:"Arn"`
	Name               string     `json:"Name"`
	DomainName         string     `json:"DomainName"`
	RuleType           string     `json:"RuleType"`
	Status             string     `json:"Status"`
	StatusMessage      string     `json:"StatusMessage,omitempty"`
	ShareStatus        string     `json:"ShareStatus"`
	ResolverEndpointID string     `json:"ResolverEndpointId"`
	CreatorRequestID   string     `json:"CreatorRequestId,omitempty"`
	OwnerID            string     `json:"OwnerID,omitempty"`
	CreationTime       string     `json:"CreationTime,omitempty"`
	ModificationTime   string     `json:"ModificationTime,omitempty"`
	TargetIps          []targetIP `json:"TargetIps,omitempty"`
}

type createResolverEndpointOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

type deleteResolverEndpointOutput struct{}

type listResolverEndpointsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listResolverEndpointsOutput struct {
	NextToken         *string                  `json:"NextToken,omitempty"`
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

type listResolverRulesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listResolverRulesOutput struct {
	NextToken     *string              `json:"NextToken,omitempty"`
	ResolverRules []resolverRuleOutput `json:"ResolverRules"`
}

func endpointToOutput(ep *ResolverEndpoint) resolverEndpointOutput {
	ips := make([]resolverEndpointIPOutput, 0, len(ep.IPAddresses))
	for _, ip := range ep.IPAddresses {
		ips = append(ips, resolverEndpointIPOutput{SubnetID: ip.SubnetID, IP: ip.IP, Ipv6: ip.Ipv6})
	}

	sgIDs := ep.SecurityGroupIDs
	if sgIDs == nil {
		sgIDs = []string{}
	}

	epType := ep.ResolverEndpointType
	if epType == "" {
		epType = endpointTypeIPV4
	}

	//nolint:gosec // conversion is safe: IP count is always small
	ipCount := int32(len(ep.IPAddresses))

	return resolverEndpointOutput{
		ID:                    ep.ID,
		Arn:                   ep.ARN,
		Name:                  ep.Name,
		Direction:             ep.Direction,
		Status:                ep.Status,
		StatusMessage:         ep.StatusMessage,
		VpcID:                 ep.VpcID,
		HostVPCId:             ep.HostVPCID,
		ResolverEndpointType:  epType,
		IPAddressCount:        ipCount,
		SecurityGroupIDs:      sgIDs,
		IPAddresses:           ips,
		Protocols:             ep.Protocols,
		OutpostArn:            ep.OutpostArn,
		PreferredInstanceType: ep.PreferredInstanceType,
		CreatorRequestID:      ep.CreatorRequestID,
		CreationTime:          ep.CreationTime,
		ModificationTime:      ep.ModificationTime,
	}
}

func ruleToOutput(r *ResolverRule) resolverRuleOutput {
	tips := make([]targetIP, 0, len(r.TargetIps))
	for _, t := range r.TargetIps {
		tips = append(
			tips,
			targetIP(t),
		)
	}
	if len(tips) == 0 {
		tips = nil
	}

	return resolverRuleOutput{
		ID:                 r.ID,
		Arn:                r.ARN,
		Name:               r.Name,
		DomainName:         r.DomainName,
		RuleType:           r.RuleType,
		Status:             r.Status,
		StatusMessage:      r.StatusMessage,
		ShareStatus:        r.ShareStatus,
		ResolverEndpointID: r.ResolverEndpointID,
		TargetIps:          tips,
		CreatorRequestID:   r.CreatorRequestID,
		OwnerID:            r.OwnerID,
		CreationTime:       r.CreationTime,
		ModificationTime:   r.ModificationTime,
	}
}

func (h *Handler) handleCreateResolverEndpoint(
	ctx context.Context,
	in *handleCreateResolverEndpointInput,
) (*createResolverEndpointOutput, error) {
	ips := make([]IPAddress, 0, len(in.IPAddresses))
	for _, ip := range in.IPAddresses {
		ips = append(ips, IPAddress{SubnetID: ip.SubnetID, IP: ip.IP, Ipv6: ip.Ipv6})
	}

	ep, err := h.Backend.CreateResolverEndpoint(
		ctx,
		in.Name, in.Direction, in.VpcID, ips, in.SecurityGroupIDs, in.ResolverEndpointType,
		in.Protocols, in.OutpostArn, in.PreferredInstanceType, in.CreatorRequestID,
	)
	if err != nil {
		return nil, err
	}

	// Store tags if provided.
	if len(in.Tags) > 0 {
		tagErr := h.Backend.TagResource(ctx, ep.ARN, in.Tags)
		if tagErr != nil {
			return nil, tagErr
		}
	}

	return &createResolverEndpointOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

func (h *Handler) handleDeleteResolverEndpoint(
	ctx context.Context,
	in *resolverEndpointIDInput,
) (*deleteResolverEndpointOutput, error) {
	if err := h.Backend.DeleteResolverEndpoint(ctx, in.ResolverEndpointID); err != nil {
		return nil, err
	}

	return &deleteResolverEndpointOutput{}, nil
}

func (h *Handler) handleListResolverEndpoints(
	ctx context.Context,
	in *listResolverEndpointsInput,
) (*listResolverEndpointsOutput, error) {
	eps := h.Backend.ListResolverEndpoints(ctx)
	items := make([]resolverEndpointOutput, 0, len(eps))
	for _, ep := range eps {
		items = append(items, endpointToOutput(ep))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	pg := page.New(items, in.NextToken, int(in.MaxResults), defaultPageSizeSmall)
	out := &listResolverEndpointsOutput{ResolverEndpoints: pg.Data}
	if pg.Next != "" {
		out.NextToken = &pg.Next
	}

	return out, nil
}

func (h *Handler) handleGetResolverEndpoint(
	ctx context.Context,
	in *resolverEndpointIDInput,
) (*getResolverEndpointOutput, error) {
	ep, err := h.Backend.GetResolverEndpoint(ctx, in.ResolverEndpointID)
	if err != nil {
		return nil, err
	}

	return &getResolverEndpointOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

func (h *Handler) handleListResolverEndpointIPAddresses(
	ctx context.Context,
	in *listResolverEndpointIPAddressesInput,
) (*listResolverEndpointIPAddressesOutput, error) {
	ips, err := h.Backend.ListResolverEndpointIPAddresses(ctx, in.ResolverEndpointID)
	if err != nil {
		return nil, err
	}
	items := make([]resolverEndpointIPAddressDetail, 0, len(ips))
	for _, ip := range ips {
		items = append(items, resolverEndpointIPAddressDetail{
			IPID:     ip.IPID,
			SubnetID: ip.SubnetID,
			IP:       ip.IP,
			Ipv6:     ip.Ipv6,
			Status:   "ATTACHED",
		})
	}

	return &listResolverEndpointIPAddressesOutput{IPAddresses: items}, nil
}

type handleCreateResolverRuleInput struct {
	Name               string     `json:"Name"`
	DomainName         string     `json:"DomainName"`
	RuleType           string     `json:"RuleType"`
	ResolverEndpointID string     `json:"ResolverEndpointId"`
	CreatorRequestID   string     `json:"CreatorRequestId"`
	TargetIps          []targetIP `json:"TargetIps"`
}

func (h *Handler) handleCreateResolverRule(
	ctx context.Context,
	in *handleCreateResolverRuleInput,
) (*createResolverRuleOutput, error) {
	tips := make([]TargetIP, 0, len(in.TargetIps))
	for _, t := range in.TargetIps {
		tips = append(
			tips,
			TargetIP(t),
		)
	}

	r, err := h.Backend.CreateResolverRule(
		ctx,
		in.Name,
		in.DomainName,
		in.RuleType,
		in.ResolverEndpointID,
		in.CreatorRequestID,
		tips,
	)
	if err != nil {
		return nil, err
	}

	return &createResolverRuleOutput{ResolverRule: ruleToOutput(r)}, nil
}

func (h *Handler) handleGetResolverRule(
	ctx context.Context,
	in *resolverRuleIDInput,
) (*getResolverRuleOutput, error) {
	r, err := h.Backend.GetResolverRule(ctx, in.ResolverRuleID)
	if err != nil {
		return nil, err
	}

	return &getResolverRuleOutput{ResolverRule: ruleToOutput(r)}, nil
}

func (h *Handler) handleDeleteResolverRule(
	ctx context.Context,
	in *resolverRuleIDInput,
) (*deleteResolverRuleOutput, error) {
	if err := h.Backend.DeleteResolverRule(ctx, in.ResolverRuleID); err != nil {
		return nil, err
	}

	return &deleteResolverRuleOutput{}, nil
}

func (h *Handler) handleListResolverRules(
	ctx context.Context,
	in *listResolverRulesInput,
) (*listResolverRulesOutput, error) {
	rules := h.Backend.ListResolverRules(ctx)
	items := make([]resolverRuleOutput, 0, len(rules))
	for _, r := range rules {
		items = append(items, ruleToOutput(r))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	pg := page.New(items, in.NextToken, int(in.MaxResults), defaultPageSizeSmall)
	out := &listResolverRulesOutput{ResolverRules: pg.Data}
	if pg.Next != "" {
		out.NextToken = &pg.Next
	}

	return out, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []svcTags.KV `json:"Tags"`
}

// handleListTagsForResource returns tags for the given resource ARN.
func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	kvs := h.Backend.ListTagsForResource(ctx, in.ResourceArn)

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
	ctx context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(ctx, in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
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
	StatusMessage    string `json:"StatusMessage,omitempty"`
	OwnerID          string `json:"OwnerID"`
	ShareStatus      string `json:"ShareStatus"`
	CreationTime     string `json:"CreationTime,omitempty"`
	ModificationTime string `json:"ModificationTime,omitempty"`
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
	StatusMessage       string `json:"StatusMessage,omitempty"`
	MutationProtection  string `json:"MutationProtection"`
	ManagedOwnerName    string `json:"ManagedOwnerName,omitempty"`
	CreatorRequestID    string `json:"CreatorRequestId,omitempty"`
	CreationTime        string `json:"CreationTime,omitempty"`
	ModificationTime    string `json:"ModificationTime,omitempty"`
	Priority            int32  `json:"Priority"`
}

// firewallDomainListOutput is the JSON representation of a FirewallDomainList.
type firewallDomainListOutput struct {
	ID               string `json:"Id"`
	Arn              string `json:"Arn"`
	Name             string `json:"Name"`
	CreatorRequestID string `json:"CreatorRequestId"`
	Status           string `json:"Status"`
	ManagedOwnerName string `json:"ManagedOwnerName,omitempty"`
	DomainCount      int32  `json:"DomainCount"`
}

// firewallRuleOutput is the JSON representation of a FirewallRule.
type firewallRuleOutput struct {
	ID                   string `json:"Id"`
	Arn                  string `json:"Arn"`
	Name                 string `json:"Name"`
	FirewallRuleGroupID  string `json:"FirewallRuleGroupId"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
	Action               string `json:"Action"`
	BlockResponse        string `json:"BlockResponse,omitempty"`
	BlockOverrideDomain  string `json:"BlockOverrideDomain,omitempty"`
	BlockOverrideDNSType string `json:"BlockOverrideDNSType,omitempty"`
	Qtype                string `json:"Qtype,omitempty"`
	ConfidenceThreshold  string `json:"ConfidenceThreshold,omitempty"`
	CreatorRequestID     string `json:"CreatorRequestId,omitempty"`
	CreationTime         string `json:"CreationTime,omitempty"`
	ModificationTime     string `json:"ModificationTime,omitempty"`
	BlockOverrideTTL     int32  `json:"BlockOverrideTTL,omitempty"`
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
	OwnerID          string `json:"OwnerID"`
	ShareStatus      string `json:"ShareStatus"`
	CreationTime     string `json:"CreationTime,omitempty"`
	AssociationCount int32  `json:"AssociationCount"`
}

// resolverQueryLogConfigAssociationOutput is the JSON representation of a ResolverQueryLogConfigAssociation.
type resolverQueryLogConfigAssociationOutput struct {
	ID                       string `json:"Id"`
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
	ResourceID               string `json:"ResourceId"`
	Status                   string `json:"Status"`
	Error                    string `json:"Error,omitempty"`
	ErrorMessage             string `json:"ErrorMessage,omitempty"`
	CreationTime             string `json:"CreationTime,omitempty"`
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
	CreatorRequestID string       `json:"CreatorRequestId"`
	Name             string       `json:"Name"`
	Tags             []svcTags.KV `json:"Tags"`
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
		StatusMessage:    g.StatusMessage,
		OwnerID:          g.OwnerID,
		ShareStatus:      g.ShareStatus,
		RuleCount:        g.RuleCount,
		CreationTime:     g.CreationTime,
		ModificationTime: g.ModificationTime,
	}
}

func (h *Handler) handleCreateFirewallRuleGroup(
	ctx context.Context,
	in *createFirewallRuleGroupInput,
) (*createFirewallRuleGroupOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	g, err := h.Backend.CreateFirewallRuleGroup(ctx, in.Name, in.CreatorRequestID)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		if tagErr := h.Backend.TagResource(ctx, g.ARN, in.Tags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createFirewallRuleGroupOutput{FirewallRuleGroup: firewallRuleGroupToOutput(g)}, nil
}

// --- AssociateFirewallRuleGroup ---

type associateFirewallRuleGroupInput struct {
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
	Name                string `json:"Name"`
	VpcID               string `json:"VpcId"`
	CreatorRequestID    string `json:"CreatorRequestId"`
	MutationProtection  string `json:"MutationProtection"`
	Priority            int32  `json:"Priority"`
}

type associateFirewallRuleGroupOutput struct {
	FirewallRuleGroupAssociation firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociation"`
}

func firewallRuleGroupAssociationToOutput(
	a *FirewallRuleGroupAssociation,
) firewallRuleGroupAssociationOutput {
	return firewallRuleGroupAssociationOutput{
		ID:                  a.ID,
		Arn:                 a.ARN,
		Name:                a.Name,
		FirewallRuleGroupID: a.FirewallRuleGroupID,
		VpcID:               a.VpcID,
		Priority:            a.Priority,
		Status:              a.Status,
		StatusMessage:       a.StatusMessage,
		MutationProtection:  a.MutationProtection,
		ManagedOwnerName:    a.ManagedOwnerName,
		CreatorRequestID:    a.CreatorRequestID,
		CreationTime:        a.CreationTime,
		ModificationTime:    a.ModificationTime,
	}
}

func (h *Handler) handleAssociateFirewallRuleGroup(
	ctx context.Context,
	in *associateFirewallRuleGroupInput,
) (*associateFirewallRuleGroupOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}

	if in.VpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrValidation)
	}

	assoc, err := h.Backend.AssociateFirewallRuleGroup(
		ctx,
		in.FirewallRuleGroupID,
		in.VpcID,
		in.Name,
		in.CreatorRequestID,
		in.MutationProtection,
		in.Priority,
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
	Ipv6     string `json:"Ipv6,omitempty"`
}

type associateResolverEndpointIPAddressInput struct {
	ResolverEndpointID string               `json:"ResolverEndpointId"`
	IPAddress          ipAddressUpdateInput `json:"IpAddress"`
}

type associateResolverEndpointIPAddressOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

func (h *Handler) handleAssociateResolverEndpointIPAddress(
	ctx context.Context,
	in *associateResolverEndpointIPAddressInput,
) (*associateResolverEndpointIPAddressOutput, error) {
	if in.ResolverEndpointID == "" {
		return nil, fmt.Errorf("%w: ResolverEndpointId is required", ErrValidation)
	}

	ep, err := h.Backend.AssociateResolverEndpointIPAddress(
		ctx, in.ResolverEndpointID, in.IPAddress.SubnetID, in.IPAddress.IP, in.IPAddress.Ipv6,
	)
	if err != nil {
		return nil, err
	}

	return &associateResolverEndpointIPAddressOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

// --- CreateResolverQueryLogConfig ---

type createResolverQueryLogConfigInput struct {
	CreatorRequestID string       `json:"CreatorRequestId"`
	DestinationArn   string       `json:"DestinationArn"`
	Name             string       `json:"Name"`
	Tags             []svcTags.KV `json:"Tags"`
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
		AssociationCount: c.AssociationCount,
		ShareStatus:      c.ShareStatus,
		CreationTime:     c.CreationTime,
	}
}

func (h *Handler) handleCreateResolverQueryLogConfig(
	ctx context.Context,
	in *createResolverQueryLogConfigInput,
) (*createResolverQueryLogConfigOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if in.DestinationArn == "" {
		return nil, fmt.Errorf("%w: DestinationArn is required", ErrValidation)
	}

	cfg, err := h.Backend.CreateResolverQueryLogConfig(
		ctx,
		in.Name,
		in.CreatorRequestID,
		in.DestinationArn,
	)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		if tagErr := h.Backend.TagResource(ctx, cfg.ARN, in.Tags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createResolverQueryLogConfigOutput{
		ResolverQueryLogConfig: queryLogConfigToOutput(cfg),
	}, nil
}

// --- AssociateResolverQueryLogConfig ---

type associateResolverQueryLogConfigInput struct {
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
	ResourceID               string `json:"ResourceId"`
}

type associateResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfigAssociation resolverQueryLogConfigAssociationOutput `json:"ResolverQueryLogConfigAssociation"`
}

func queryLogConfigAssociationToOutput(
	a *ResolverQueryLogConfigAssociation,
) resolverQueryLogConfigAssociationOutput {
	return resolverQueryLogConfigAssociationOutput{
		ID:                       a.ID,
		ResolverQueryLogConfigID: a.ResolverQueryLogConfigID,
		ResourceID:               a.ResourceID,
		Status:                   a.Status,
		Error:                    a.Error,
		ErrorMessage:             a.ErrorMessage,
		CreationTime:             a.CreationTime,
	}
}

func (h *Handler) handleAssociateResolverQueryLogConfig(
	ctx context.Context,
	in *associateResolverQueryLogConfigInput,
) (*associateResolverQueryLogConfigOutput, error) {
	if in.ResolverQueryLogConfigID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigId is required", ErrValidation)
	}

	if in.ResourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}

	assoc, err := h.Backend.AssociateResolverQueryLogConfig(
		ctx,
		in.ResolverQueryLogConfigID,
		in.ResourceID,
	)
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
	ctx context.Context,
	in *associateResolverRuleInput,
) (*associateResolverRuleOutput, error) {
	if in.ResolverRuleID == "" {
		return nil, fmt.Errorf("%w: ResolverRuleId is required", ErrValidation)
	}

	if in.VPCId == "" {
		return nil, fmt.Errorf("%w: VPCId is required", ErrValidation)
	}

	assoc, err := h.Backend.AssociateResolverRule(ctx, in.ResolverRuleID, in.VPCId, in.Name)
	if err != nil {
		return nil, err
	}

	return &associateResolverRuleOutput{
		ResolverRuleAssociation: ruleAssociationToOutput(assoc),
	}, nil
}

// --- CreateFirewallDomainList ---

type createFirewallDomainListInput struct {
	CreatorRequestID string       `json:"CreatorRequestId"`
	Name             string       `json:"Name"`
	Tags             []svcTags.KV `json:"Tags"`
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
		ManagedOwnerName: dl.ManagedOwnerName,
	}
}

func (h *Handler) handleCreateFirewallDomainList(
	ctx context.Context,
	in *createFirewallDomainListInput,
) (*createFirewallDomainListOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	dl, err := h.Backend.CreateFirewallDomainList(ctx, in.Name, in.CreatorRequestID)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		if tagErr := h.Backend.TagResource(ctx, dl.ARN, in.Tags); tagErr != nil {
			return nil, tagErr
		}
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
	ctx context.Context,
	in *deleteFirewallDomainListInput,
) (*deleteFirewallDomainListOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}

	dl, err := h.Backend.DeleteFirewallDomainList(ctx, in.FirewallDomainListID)
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
	BlockOverrideDomain  string `json:"BlockOverrideDomain"`
	BlockOverrideDNSType string `json:"BlockOverrideDNSType"`
	Qtype                string `json:"Qtype"`
	ConfidenceThreshold  string `json:"ConfidenceThreshold"`
	BlockOverrideTTL     int32  `json:"BlockOverrideTTL"`
	Priority             int32  `json:"Priority"`
}

type createFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func firewallRuleToOutput(r *FirewallRule) firewallRuleOutput {
	return firewallRuleOutput{
		ID:                   r.ID,
		Arn:                  r.ARN,
		Name:                 r.Name,
		FirewallRuleGroupID:  r.FirewallRuleGroupID,
		FirewallDomainListID: r.FirewallDomainListID,
		Action:               r.Action,
		Priority:             r.Priority,
		BlockResponse:        r.BlockResponse,
		BlockOverrideDomain:  r.BlockOverrideDomain,
		BlockOverrideDNSType: r.BlockOverrideDNSType,
		BlockOverrideTTL:     r.BlockOverrideTTL,
		Qtype:                r.Qtype,
		ConfidenceThreshold:  r.ConfidenceThreshold,
		CreatorRequestID:     r.CreatorRequestID,
		CreationTime:         r.CreationTime,
		ModificationTime:     r.ModificationTime,
	}
}

func (h *Handler) handleCreateFirewallRule(
	ctx context.Context,
	in *createFirewallRuleInput,
) (*createFirewallRuleOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}

	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	switch in.Action {
	case firewallActionAllow, firewallActionBlock, firewallActionAlert:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: Action must be %s, %s, or %s",
			ErrValidation,
			firewallActionAllow,
			firewallActionBlock,
			firewallActionAlert,
		)
	}

	rule, err := h.Backend.CreateFirewallRule(ctx, CreateFirewallRuleParams{
		FirewallRuleGroupID:  in.FirewallRuleGroupID,
		Name:                 in.Name,
		Action:               in.Action,
		BlockResponse:        in.BlockResponse,
		BlockOverrideDomain:  in.BlockOverrideDomain,
		BlockOverrideDNSType: in.BlockOverrideDNSType,
		BlockOverrideTTL:     in.BlockOverrideTTL,
		Qtype:                in.Qtype,
		ConfidenceThreshold:  in.ConfidenceThreshold,
		CreatorRequestID:     in.CreatorRequestID,
		FirewallDomainListID: in.FirewallDomainListID,
		Priority:             in.Priority,
	})
	if err != nil {
		return nil, err
	}

	return &createFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// --- CreateOutpostResolver ---

type createOutpostResolverInput struct {
	CreatorRequestID      string       `json:"CreatorRequestId"`
	Name                  string       `json:"Name"`
	OutpostArn            string       `json:"OutpostArn"`
	PreferredInstanceType string       `json:"PreferredInstanceType"`
	Tags                  []svcTags.KV `json:"Tags"`
	InstanceCount         int32        `json:"InstanceCount"`
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
	ctx context.Context,
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
		ctx, in.Name, in.CreatorRequestID, in.OutpostArn, in.PreferredInstanceType, in.InstanceCount,
	)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		if tagErr := h.Backend.TagResource(ctx, r.ARN, in.Tags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createOutpostResolverOutput{OutpostResolver: outpostResolverToOutput(r)}, nil
}

// --- New handler types and functions for 46 missing operations ---

// firewallConfigOutput is the JSON representation of a FirewallConfig.
// AWS does not return an ARN for FirewallConfig.
type firewallConfigOutput struct {
	ID               string `json:"Id"`
	OwnerID          string `json:"OwnerID"`
	ResourceID       string `json:"ResourceId"`
	FirewallFailOpen string `json:"FirewallFailOpen"`
}

func firewallConfigToOutput(c *FirewallConfig) firewallConfigOutput {
	return firewallConfigOutput{
		ID:               c.ID,
		OwnerID:          c.OwnerID,
		ResourceID:       c.ResourceID,
		FirewallFailOpen: c.FirewallFailOpen,
	}
}

// resolverConfigOutput is the JSON representation of a ResolverConfig.
type resolverConfigOutput struct {
	ID                 string `json:"Id"`
	Arn                string `json:"Arn"`
	OwnerID            string `json:"OwnerID"`
	ResourceID         string `json:"ResourceId"`
	AutodefinedReverse string `json:"AutodefinedReverse"`
}

func resolverConfigToOutput(c *ResolverConfig) resolverConfigOutput {
	return resolverConfigOutput{
		ID:                 c.ID,
		Arn:                c.ARN,
		OwnerID:            c.OwnerID,
		ResourceID:         c.ResourceID,
		AutodefinedReverse: c.AutodefinedReverse,
	}
}

// resolverDnssecConfigOutput is the JSON representation of a ResolverDnssecConfig.
type resolverDnssecConfigOutput struct {
	ID               string `json:"Id"`
	OwnerID          string `json:"OwnerID"`
	ResourceID       string `json:"ResourceId"`
	ValidationStatus string `json:"ValidationStatus"`
}

func resolverDnssecConfigToOutput(c *ResolverDnssecConfig) resolverDnssecConfigOutput {
	return resolverDnssecConfigOutput{
		ID:               c.ID,
		OwnerID:          c.OwnerID,
		ResourceID:       c.ResourceID,
		ValidationStatus: c.ValidationStatus,
	}
}

// --- DeleteFirewallRule ---

type deleteFirewallRuleInput struct {
	FirewallRuleID string `json:"FirewallRuleId"`
}

type deleteFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func (h *Handler) handleDeleteFirewallRule(
	ctx context.Context,
	in *deleteFirewallRuleInput,
) (*deleteFirewallRuleOutput, error) {
	if in.FirewallRuleID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleId is required", ErrValidation)
	}
	rule, err := h.Backend.DeleteFirewallRule(ctx, in.FirewallRuleID)
	if err != nil {
		return nil, err
	}

	return &deleteFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// --- UpdateFirewallRule ---

type updateFirewallRuleInput struct {
	FirewallRuleID       string `json:"FirewallRuleId"`
	Name                 string `json:"Name"`
	Action               string `json:"Action"`
	BlockResponse        string `json:"BlockResponse"`
	BlockOverrideDomain  string `json:"BlockOverrideDomain"`
	BlockOverrideDNSType string `json:"BlockOverrideDNSType"`
	Qtype                string `json:"Qtype"`
	ConfidenceThreshold  string `json:"ConfidenceThreshold"`
	FirewallDomainListID string `json:"FirewallDomainListId"`
	BlockOverrideTTL     int32  `json:"BlockOverrideTTL"`
	Priority             int32  `json:"Priority"`
}

type updateFirewallRuleOutput struct {
	FirewallRule firewallRuleOutput `json:"FirewallRule"`
}

func (h *Handler) handleUpdateFirewallRule(
	ctx context.Context,
	in *updateFirewallRuleInput,
) (*updateFirewallRuleOutput, error) {
	if in.FirewallRuleID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleId is required", ErrValidation)
	}
	rule, err := h.Backend.UpdateFirewallRule(ctx, UpdateFirewallRuleParams{
		ID:                   in.FirewallRuleID,
		Name:                 in.Name,
		Action:               in.Action,
		BlockResponse:        in.BlockResponse,
		BlockOverrideDomain:  in.BlockOverrideDomain,
		BlockOverrideDNSType: in.BlockOverrideDNSType,
		BlockOverrideTTL:     in.BlockOverrideTTL,
		Qtype:                in.Qtype,
		ConfidenceThreshold:  in.ConfidenceThreshold,
		FirewallDomainListID: in.FirewallDomainListID,
		Priority:             in.Priority,
	})
	if err != nil {
		return nil, err
	}

	return &updateFirewallRuleOutput{FirewallRule: firewallRuleToOutput(rule)}, nil
}

// --- ListFirewallRules ---

type listFirewallRulesInput struct {
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
	NextToken           string `json:"NextToken"`
	MaxResults          int32  `json:"MaxResults"`
}

type listFirewallRulesOutput struct {
	NextToken     *string              `json:"NextToken,omitempty"`
	FirewallRules []firewallRuleOutput `json:"FirewallRules"`
}

func (h *Handler) handleListFirewallRules(
	ctx context.Context,
	in *listFirewallRulesInput,
) (*listFirewallRulesOutput, error) {
	rules := h.Backend.ListFirewallRules(ctx, in.FirewallRuleGroupID)
	items := make([]firewallRuleOutput, 0, len(rules))
	for _, r := range rules {
		items = append(items, firewallRuleToOutput(r))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	pg := page.New(items, in.NextToken, int(in.MaxResults), defaultPageSizeLarge)
	out := &listFirewallRulesOutput{FirewallRules: pg.Data}
	if pg.Next != "" {
		out.NextToken = &pg.Next
	}

	return out, nil
}

// --- DeleteFirewallRuleGroup ---

type deleteFirewallRuleGroupInput struct {
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
}

type deleteFirewallRuleGroupOutput struct {
	FirewallRuleGroup firewallRuleGroupOutput `json:"FirewallRuleGroup"`
}

func (h *Handler) handleDeleteFirewallRuleGroup(
	ctx context.Context,
	in *deleteFirewallRuleGroupInput,
) (*deleteFirewallRuleGroupOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}
	g, err := h.Backend.DeleteFirewallRuleGroup(ctx, in.FirewallRuleGroupID)
	if err != nil {
		return nil, err
	}

	return &deleteFirewallRuleGroupOutput{FirewallRuleGroup: firewallRuleGroupToOutput(g)}, nil
}

// --- GetFirewallRuleGroup ---

type getFirewallRuleGroupInput struct {
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
}

type getFirewallRuleGroupOutput struct {
	FirewallRuleGroup firewallRuleGroupOutput `json:"FirewallRuleGroup"`
}

func (h *Handler) handleGetFirewallRuleGroup(
	ctx context.Context,
	in *getFirewallRuleGroupInput,
) (*getFirewallRuleGroupOutput, error) {
	if in.FirewallRuleGroupID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupId is required", ErrValidation)
	}
	g, err := h.Backend.GetFirewallRuleGroup(ctx, in.FirewallRuleGroupID)
	if err != nil {
		return nil, err
	}

	return &getFirewallRuleGroupOutput{FirewallRuleGroup: firewallRuleGroupToOutput(g)}, nil
}

// --- ListFirewallRuleGroups ---

type listFirewallRuleGroupsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listFirewallRuleGroupsOutput struct {
	NextToken          *string                   `json:"NextToken,omitempty"`
	FirewallRuleGroups []firewallRuleGroupOutput `json:"FirewallRuleGroups"`
}

func (h *Handler) handleListFirewallRuleGroups(
	ctx context.Context,
	in *listFirewallRuleGroupsInput,
) (*listFirewallRuleGroupsOutput, error) {
	groups := h.Backend.ListFirewallRuleGroups(ctx)
	items := make([]firewallRuleGroupOutput, 0, len(groups))
	for _, g := range groups {
		items = append(items, firewallRuleGroupToOutput(g))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	pg := page.New(items, in.NextToken, int(in.MaxResults), defaultPageSizeLarge)
	out := &listFirewallRuleGroupsOutput{FirewallRuleGroups: pg.Data}
	if pg.Next != "" {
		out.NextToken = &pg.Next
	}

	return out, nil
}

// --- GetFirewallRuleGroupPolicy ---

type getFirewallRuleGroupPolicyInput struct {
	Arn string `json:"Arn"`
}

type getFirewallRuleGroupPolicyOutput struct {
	FirewallRuleGroupPolicy string `json:"FirewallRuleGroupPolicy"`
}

func (h *Handler) handleGetFirewallRuleGroupPolicy(
	ctx context.Context,
	in *getFirewallRuleGroupPolicyInput,
) (*getFirewallRuleGroupPolicyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}
	policy := h.Backend.GetFirewallRuleGroupPolicy(ctx, in.Arn)

	return &getFirewallRuleGroupPolicyOutput{FirewallRuleGroupPolicy: policy}, nil
}

// --- PutFirewallRuleGroupPolicy ---

type putFirewallRuleGroupPolicyInput struct {
	Arn                     string `json:"Arn"`
	FirewallRuleGroupPolicy string `json:"FirewallRuleGroupPolicy"`
}

type putFirewallRuleGroupPolicyOutput struct {
	ReturnValue bool `json:"ReturnValue"`
}

func (h *Handler) handlePutFirewallRuleGroupPolicy(
	ctx context.Context,
	in *putFirewallRuleGroupPolicyInput,
) (*putFirewallRuleGroupPolicyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}
	if err := h.Backend.PutFirewallRuleGroupPolicy(ctx, in.Arn, in.FirewallRuleGroupPolicy); err != nil {
		return nil, err
	}

	return &putFirewallRuleGroupPolicyOutput{ReturnValue: true}, nil
}

// --- GetFirewallRuleGroupAssociation ---

type getFirewallRuleGroupAssociationInput struct {
	FirewallRuleGroupAssociationID string `json:"FirewallRuleGroupAssociationId"`
}

type getFirewallRuleGroupAssociationOutput struct {
	FirewallRuleGroupAssociation firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociation"`
}

func (h *Handler) handleGetFirewallRuleGroupAssociation(
	ctx context.Context,
	in *getFirewallRuleGroupAssociationInput,
) (*getFirewallRuleGroupAssociationOutput, error) {
	if in.FirewallRuleGroupAssociationID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.GetFirewallRuleGroupAssociation(ctx, in.FirewallRuleGroupAssociationID)
	if err != nil {
		return nil, err
	}

	return &getFirewallRuleGroupAssociationOutput{
		FirewallRuleGroupAssociation: firewallRuleGroupAssociationToOutput(assoc),
	}, nil
}

// --- ListFirewallRuleGroupAssociations ---

type listFirewallRuleGroupAssociationsInput struct {
	VpcID               string `json:"VpcId"`
	FirewallRuleGroupID string `json:"FirewallRuleGroupId"`
}

type listFirewallRuleGroupAssociationsOutput struct {
	FirewallRuleGroupAssociations []firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociations"`
}

func (h *Handler) handleListFirewallRuleGroupAssociations(
	ctx context.Context,
	in *listFirewallRuleGroupAssociationsInput,
) (*listFirewallRuleGroupAssociationsOutput, error) {
	assocs := h.Backend.ListFirewallRuleGroupAssociations(ctx, in.VpcID, in.FirewallRuleGroupID)
	items := make([]firewallRuleGroupAssociationOutput, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, firewallRuleGroupAssociationToOutput(a))
	}

	return &listFirewallRuleGroupAssociationsOutput{FirewallRuleGroupAssociations: items}, nil
}

// --- DisassociateFirewallRuleGroup ---

type disassociateFirewallRuleGroupInput struct {
	FirewallRuleGroupAssociationID string `json:"FirewallRuleGroupAssociationId"`
}

type disassociateFirewallRuleGroupOutput struct {
	FirewallRuleGroupAssociation firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociation"`
}

func (h *Handler) handleDisassociateFirewallRuleGroup(
	ctx context.Context,
	in *disassociateFirewallRuleGroupInput,
) (*disassociateFirewallRuleGroupOutput, error) {
	if in.FirewallRuleGroupAssociationID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.DisassociateFirewallRuleGroup(ctx, in.FirewallRuleGroupAssociationID)
	if err != nil {
		return nil, err
	}

	return &disassociateFirewallRuleGroupOutput{
		FirewallRuleGroupAssociation: firewallRuleGroupAssociationToOutput(assoc),
	}, nil
}

// --- UpdateFirewallRuleGroupAssociation ---

type updateFirewallRuleGroupAssociationInput struct {
	FirewallRuleGroupAssociationID string `json:"FirewallRuleGroupAssociationId"`
	Name                           string `json:"Name"`
	MutationProtection             string `json:"MutationProtection"`
	Priority                       int32  `json:"Priority"`
}

type updateFirewallRuleGroupAssociationOutput struct {
	FirewallRuleGroupAssociation firewallRuleGroupAssociationOutput `json:"FirewallRuleGroupAssociation"`
}

func (h *Handler) handleUpdateFirewallRuleGroupAssociation(
	ctx context.Context,
	in *updateFirewallRuleGroupAssociationInput,
) (*updateFirewallRuleGroupAssociationOutput, error) {
	if in.FirewallRuleGroupAssociationID == "" {
		return nil, fmt.Errorf("%w: FirewallRuleGroupAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.UpdateFirewallRuleGroupAssociation(
		ctx, in.FirewallRuleGroupAssociationID, in.Name, in.MutationProtection, in.Priority,
	)
	if err != nil {
		return nil, err
	}

	return &updateFirewallRuleGroupAssociationOutput{
		FirewallRuleGroupAssociation: firewallRuleGroupAssociationToOutput(assoc),
	}, nil
}

// --- GetFirewallDomainList ---

type getFirewallDomainListInput struct {
	FirewallDomainListID string `json:"FirewallDomainListId"`
}

type getFirewallDomainListOutput struct {
	FirewallDomainList firewallDomainListOutput `json:"FirewallDomainList"`
}

func (h *Handler) handleGetFirewallDomainList(
	ctx context.Context,
	in *getFirewallDomainListInput,
) (*getFirewallDomainListOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}
	dl, err := h.Backend.GetFirewallDomainList(ctx, in.FirewallDomainListID)
	if err != nil {
		return nil, err
	}

	return &getFirewallDomainListOutput{FirewallDomainList: firewallDomainListToOutput(dl)}, nil
}

// --- ListFirewallDomainLists ---

type listFirewallDomainListsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listFirewallDomainListsOutput struct {
	NextToken           *string                    `json:"NextToken,omitempty"`
	FirewallDomainLists []firewallDomainListOutput `json:"FirewallDomainLists"`
}

func (h *Handler) handleListFirewallDomainLists(
	ctx context.Context,
	in *listFirewallDomainListsInput,
) (*listFirewallDomainListsOutput, error) {
	lists := h.Backend.ListFirewallDomainLists(ctx)
	items := make([]firewallDomainListOutput, 0, len(lists))
	for _, dl := range lists {
		items = append(items, firewallDomainListToOutput(dl))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	pg := page.New(items, in.NextToken, int(in.MaxResults), defaultPageSizeLarge)
	out := &listFirewallDomainListsOutput{FirewallDomainLists: pg.Data}
	if pg.Next != "" {
		out.NextToken = &pg.Next
	}

	return out, nil
}

// --- ListFirewallDomains ---

type listFirewallDomainsInput struct {
	FirewallDomainListID string `json:"FirewallDomainListId"`
}

type listFirewallDomainsOutput struct {
	Domains []string `json:"Domains"`
}

func (h *Handler) handleListFirewallDomains(
	ctx context.Context,
	in *listFirewallDomainsInput,
) (*listFirewallDomainsOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}
	domains, err := h.Backend.ListFirewallDomains(ctx, in.FirewallDomainListID)
	if err != nil {
		return nil, err
	}

	return &listFirewallDomainsOutput{Domains: domains}, nil
}

// --- UpdateFirewallDomains ---

type updateFirewallDomainsInput struct {
	FirewallDomainListID string   `json:"FirewallDomainListId"`
	Operation            string   `json:"Operation"`
	Domains              []string `json:"Domains"`
}

type updateFirewallDomainsOutput struct {
	FirewallDomainList firewallDomainListOutput `json:"FirewallDomainList"`
}

func (h *Handler) handleUpdateFirewallDomains(
	ctx context.Context,
	in *updateFirewallDomainsInput,
) (*updateFirewallDomainsOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}
	if in.Operation == "" {
		return nil, fmt.Errorf("%w: Operation is required", ErrValidation)
	}
	dl, err := h.Backend.UpdateFirewallDomains(ctx, in.FirewallDomainListID, in.Operation, in.Domains)
	if err != nil {
		return nil, err
	}

	return &updateFirewallDomainsOutput{FirewallDomainList: firewallDomainListToOutput(dl)}, nil
}

// --- ImportFirewallDomains ---

type importFirewallDomainsInput struct {
	FirewallDomainListID string `json:"FirewallDomainListId"`
	DomainFileURL        string `json:"DomainFileUrl"`
	Operation            string `json:"Operation"`
}

type importFirewallDomainsOutput struct {
	FirewallDomainList firewallDomainListOutput `json:"FirewallDomainList"`
}

func (h *Handler) handleImportFirewallDomains(
	ctx context.Context,
	in *importFirewallDomainsInput,
) (*importFirewallDomainsOutput, error) {
	if in.FirewallDomainListID == "" {
		return nil, fmt.Errorf("%w: FirewallDomainListId is required", ErrValidation)
	}
	if in.DomainFileURL == "" {
		return nil, fmt.Errorf("%w: DomainFileUrl is required", ErrValidation)
	}
	if in.Operation == "" {
		return nil, fmt.Errorf("%w: Operation is required", ErrValidation)
	}
	dl, err := h.Backend.ImportFirewallDomains(
		ctx,
		in.FirewallDomainListID,
		in.Operation,
		in.DomainFileURL,
	)
	if err != nil {
		return nil, err
	}

	return &importFirewallDomainsOutput{FirewallDomainList: firewallDomainListToOutput(dl)}, nil
}

// --- GetFirewallConfig ---

type getFirewallConfigInput struct {
	ResourceID string `json:"ResourceId"`
}

type getFirewallConfigOutput struct {
	FirewallConfig firewallConfigOutput `json:"FirewallConfig"`
}

func (h *Handler) handleGetFirewallConfig(
	ctx context.Context,
	in *getFirewallConfigInput,
) (*getFirewallConfigOutput, error) {
	if in.ResourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}
	cfg := h.Backend.GetFirewallConfig(ctx, in.ResourceID)

	return &getFirewallConfigOutput{FirewallConfig: firewallConfigToOutput(cfg)}, nil
}

// --- UpdateFirewallConfig ---

type updateFirewallConfigInput struct {
	ResourceID       string `json:"ResourceId"`
	FirewallFailOpen string `json:"FirewallFailOpen"`
}

type updateFirewallConfigOutput struct {
	FirewallConfig firewallConfigOutput `json:"FirewallConfig"`
}

func (h *Handler) handleUpdateFirewallConfig(
	ctx context.Context,
	in *updateFirewallConfigInput,
) (*updateFirewallConfigOutput, error) {
	if in.ResourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}
	cfg, err := h.Backend.UpdateFirewallConfig(ctx, in.ResourceID, in.FirewallFailOpen)
	if err != nil {
		return nil, err
	}

	return &updateFirewallConfigOutput{FirewallConfig: firewallConfigToOutput(cfg)}, nil
}

// --- ListFirewallConfigs ---

type listFirewallConfigsInput struct{}

type listFirewallConfigsOutput struct {
	FirewallConfigs []firewallConfigOutput `json:"FirewallConfigs"`
}

func (h *Handler) handleListFirewallConfigs(
	ctx context.Context,
	_ *listFirewallConfigsInput,
) (*listFirewallConfigsOutput, error) {
	configs := h.Backend.ListFirewallConfigs(ctx)
	items := make([]firewallConfigOutput, 0, len(configs))
	for _, c := range configs {
		items = append(items, firewallConfigToOutput(c))
	}

	return &listFirewallConfigsOutput{FirewallConfigs: items}, nil
}

// --- GetOutpostResolver ---

type getOutpostResolverInput struct {
	ID string `json:"Id"`
}

type getOutpostResolverOutput struct {
	OutpostResolver outpostResolverOutput `json:"OutpostResolver"`
}

func (h *Handler) handleGetOutpostResolver(
	ctx context.Context,
	in *getOutpostResolverInput,
) (*getOutpostResolverOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrValidation)
	}
	r, err := h.Backend.GetOutpostResolver(ctx, in.ID)
	if err != nil {
		return nil, err
	}

	return &getOutpostResolverOutput{OutpostResolver: outpostResolverToOutput(r)}, nil
}

// --- DeleteOutpostResolver ---

type deleteOutpostResolverInput struct {
	ID string `json:"Id"`
}

type deleteOutpostResolverOutput struct {
	OutpostResolver outpostResolverOutput `json:"OutpostResolver"`
}

func (h *Handler) handleDeleteOutpostResolver(
	ctx context.Context,
	in *deleteOutpostResolverInput,
) (*deleteOutpostResolverOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrValidation)
	}
	r, err := h.Backend.DeleteOutpostResolver(ctx, in.ID)
	if err != nil {
		return nil, err
	}

	return &deleteOutpostResolverOutput{OutpostResolver: outpostResolverToOutput(r)}, nil
}

// --- ListOutpostResolvers ---

type listOutpostResolversInput struct{}

type listOutpostResolversOutput struct {
	OutpostResolvers []outpostResolverOutput `json:"OutpostResolvers"`
}

func (h *Handler) handleListOutpostResolvers(
	ctx context.Context,
	_ *listOutpostResolversInput,
) (*listOutpostResolversOutput, error) {
	resolvers := h.Backend.ListOutpostResolvers(ctx)
	items := make([]outpostResolverOutput, 0, len(resolvers))
	for _, r := range resolvers {
		items = append(items, outpostResolverToOutput(r))
	}

	return &listOutpostResolversOutput{OutpostResolvers: items}, nil
}

// --- UpdateOutpostResolver ---

type updateOutpostResolverInput struct {
	ID                    string `json:"Id"`
	Name                  string `json:"Name"`
	PreferredInstanceType string `json:"PreferredInstanceType"`
	InstanceCount         int32  `json:"InstanceCount"`
}

type updateOutpostResolverOutput struct {
	OutpostResolver outpostResolverOutput `json:"OutpostResolver"`
}

func (h *Handler) handleUpdateOutpostResolver(
	ctx context.Context,
	in *updateOutpostResolverInput,
) (*updateOutpostResolverOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrValidation)
	}
	r, err := h.Backend.UpdateOutpostResolver(
		ctx,
		in.ID,
		in.Name,
		in.PreferredInstanceType,
		in.InstanceCount,
	)
	if err != nil {
		return nil, err
	}

	return &updateOutpostResolverOutput{OutpostResolver: outpostResolverToOutput(r)}, nil
}

// --- DeleteResolverQueryLogConfig ---

type deleteResolverQueryLogConfigInput struct {
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
}

type deleteResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfig resolverQueryLogConfigOutput `json:"ResolverQueryLogConfig"`
}

func (h *Handler) handleDeleteResolverQueryLogConfig(
	ctx context.Context,
	in *deleteResolverQueryLogConfigInput,
) (*deleteResolverQueryLogConfigOutput, error) {
	if in.ResolverQueryLogConfigID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigId is required", ErrValidation)
	}
	cfg, err := h.Backend.DeleteResolverQueryLogConfig(ctx, in.ResolverQueryLogConfigID)
	if err != nil {
		return nil, err
	}

	return &deleteResolverQueryLogConfigOutput{
		ResolverQueryLogConfig: queryLogConfigToOutput(cfg),
	}, nil
}

// --- GetResolverQueryLogConfig ---

type getResolverQueryLogConfigInput struct {
	ResolverQueryLogConfigID string `json:"ResolverQueryLogConfigId"`
}

type getResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfig resolverQueryLogConfigOutput `json:"ResolverQueryLogConfig"`
}

func (h *Handler) handleGetResolverQueryLogConfig(
	ctx context.Context,
	in *getResolverQueryLogConfigInput,
) (*getResolverQueryLogConfigOutput, error) {
	if in.ResolverQueryLogConfigID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigId is required", ErrValidation)
	}
	cfg, err := h.Backend.GetResolverQueryLogConfig(ctx, in.ResolverQueryLogConfigID)
	if err != nil {
		return nil, err
	}

	return &getResolverQueryLogConfigOutput{
		ResolverQueryLogConfig: queryLogConfigToOutput(cfg),
	}, nil
}

// --- ListResolverQueryLogConfigs ---

type listResolverQueryLogConfigsInput struct{}

type listResolverQueryLogConfigsOutput struct {
	NextToken               *string                        `json:"NextToken,omitempty"`
	ResolverQueryLogConfigs []resolverQueryLogConfigOutput `json:"ResolverQueryLogConfigs"`
}

func (h *Handler) handleListResolverQueryLogConfigs(
	ctx context.Context,
	_ *listResolverQueryLogConfigsInput,
) (*listResolverQueryLogConfigsOutput, error) {
	configs := h.Backend.ListResolverQueryLogConfigs(ctx)
	items := make([]resolverQueryLogConfigOutput, 0, len(configs))
	for _, c := range configs {
		items = append(items, queryLogConfigToOutput(c))
	}

	return &listResolverQueryLogConfigsOutput{ResolverQueryLogConfigs: items}, nil
}

// --- GetResolverQueryLogConfigAssociation ---

type getResolverQueryLogConfigAssociationInput struct {
	ResolverQueryLogConfigAssociationID string `json:"ResolverQueryLogConfigAssociationId"`
}

type getResolverQueryLogConfigAssociationOutput struct {
	ResolverQueryLogConfigAssociation resolverQueryLogConfigAssociationOutput `json:"ResolverQueryLogConfigAssociation"`
}

func (h *Handler) handleGetResolverQueryLogConfigAssociation(
	ctx context.Context,
	in *getResolverQueryLogConfigAssociationInput,
) (*getResolverQueryLogConfigAssociationOutput, error) {
	if in.ResolverQueryLogConfigAssociationID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.GetResolverQueryLogConfigAssociation(
		ctx,
		in.ResolverQueryLogConfigAssociationID,
	)
	if err != nil {
		return nil, err
	}

	return &getResolverQueryLogConfigAssociationOutput{
		ResolverQueryLogConfigAssociation: queryLogConfigAssociationToOutput(assoc),
	}, nil
}

// --- DisassociateResolverQueryLogConfig ---

type disassociateResolverQueryLogConfigInput struct {
	ResolverQueryLogConfigAssociationID string `json:"ResolverQueryLogConfigAssociationId"`
}

type disassociateResolverQueryLogConfigOutput struct {
	ResolverQueryLogConfigAssociation resolverQueryLogConfigAssociationOutput `json:"ResolverQueryLogConfigAssociation"`
}

func (h *Handler) handleDisassociateResolverQueryLogConfig(
	ctx context.Context,
	in *disassociateResolverQueryLogConfigInput,
) (*disassociateResolverQueryLogConfigOutput, error) {
	if in.ResolverQueryLogConfigAssociationID == "" {
		return nil, fmt.Errorf("%w: ResolverQueryLogConfigAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.DisassociateResolverQueryLogConfig(
		ctx,
		in.ResolverQueryLogConfigAssociationID,
	)
	if err != nil {
		return nil, err
	}

	return &disassociateResolverQueryLogConfigOutput{
		ResolverQueryLogConfigAssociation: queryLogConfigAssociationToOutput(assoc),
	}, nil
}

// --- ListResolverQueryLogConfigAssociations ---

type listResolverQueryLogConfigAssociationsInput struct{}

type queryLogAssocOutputSlice = []resolverQueryLogConfigAssociationOutput

type listResolverQueryLogConfigAssociationsOutput struct {
	ResolverQueryLogConfigAssociations queryLogAssocOutputSlice `json:"ResolverQueryLogConfigAssociations"`
}

func (h *Handler) handleListResolverQueryLogConfigAssociations(
	ctx context.Context,
	_ *listResolverQueryLogConfigAssociationsInput,
) (*listResolverQueryLogConfigAssociationsOutput, error) {
	assocs := h.Backend.ListResolverQueryLogConfigAssociations(ctx)
	items := make([]resolverQueryLogConfigAssociationOutput, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, queryLogConfigAssociationToOutput(a))
	}

	return &listResolverQueryLogConfigAssociationsOutput{
		ResolverQueryLogConfigAssociations: items,
	}, nil
}

// --- GetResolverQueryLogConfigPolicy ---

type getResolverQueryLogConfigPolicyInput struct {
	Arn string `json:"Arn"`
}

type getResolverQueryLogConfigPolicyOutput struct {
	ResolverQueryLogConfigPolicy string `json:"ResolverQueryLogConfigPolicy"`
}

func (h *Handler) handleGetResolverQueryLogConfigPolicy(
	ctx context.Context,
	in *getResolverQueryLogConfigPolicyInput,
) (*getResolverQueryLogConfigPolicyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}
	policy := h.Backend.GetResolverQueryLogConfigPolicy(ctx, in.Arn)

	return &getResolverQueryLogConfigPolicyOutput{ResolverQueryLogConfigPolicy: policy}, nil
}

// --- PutResolverQueryLogConfigPolicy ---

type putResolverQueryLogConfigPolicyInput struct {
	Arn                          string `json:"Arn"`
	ResolverQueryLogConfigPolicy string `json:"ResolverQueryLogConfigPolicy"`
}

type putResolverQueryLogConfigPolicyOutput struct {
	ReturnValue bool `json:"ReturnValue"`
}

func (h *Handler) handlePutResolverQueryLogConfigPolicy(
	ctx context.Context,
	in *putResolverQueryLogConfigPolicyInput,
) (*putResolverQueryLogConfigPolicyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}
	if err := h.Backend.PutResolverQueryLogConfigPolicy(ctx, in.Arn, in.ResolverQueryLogConfigPolicy); err != nil {
		return nil, err
	}

	return &putResolverQueryLogConfigPolicyOutput{ReturnValue: true}, nil
}

// --- GetResolverRuleAssociation ---

type getResolverRuleAssociationInput struct {
	ResolverRuleAssociationID string `json:"ResolverRuleAssociationId"`
}

type getResolverRuleAssociationOutput struct {
	ResolverRuleAssociation resolverRuleAssociationOutput `json:"ResolverRuleAssociation"`
}

func (h *Handler) handleGetResolverRuleAssociation(
	ctx context.Context,
	in *getResolverRuleAssociationInput,
) (*getResolverRuleAssociationOutput, error) {
	if in.ResolverRuleAssociationID == "" {
		return nil, fmt.Errorf("%w: ResolverRuleAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.GetResolverRuleAssociation(ctx, in.ResolverRuleAssociationID)
	if err != nil {
		return nil, err
	}

	return &getResolverRuleAssociationOutput{
		ResolverRuleAssociation: ruleAssociationToOutput(assoc),
	}, nil
}

// --- DisassociateResolverRule ---

type disassociateResolverRuleInput struct {
	ResolverRuleAssociationID string `json:"ResolverRuleAssociationId"`
}

type disassociateResolverRuleOutput struct {
	ResolverRuleAssociation resolverRuleAssociationOutput `json:"ResolverRuleAssociation"`
}

func (h *Handler) handleDisassociateResolverRule(
	ctx context.Context,
	in *disassociateResolverRuleInput,
) (*disassociateResolverRuleOutput, error) {
	if in.ResolverRuleAssociationID == "" {
		return nil, fmt.Errorf("%w: ResolverRuleAssociationId is required", ErrValidation)
	}
	assoc, err := h.Backend.DisassociateResolverRule(ctx, in.ResolverRuleAssociationID)
	if err != nil {
		return nil, err
	}

	return &disassociateResolverRuleOutput{
		ResolverRuleAssociation: ruleAssociationToOutput(assoc),
	}, nil
}

// --- ListResolverRuleAssociations ---

type listResolverRuleAssociationsInput struct{}

type listResolverRuleAssociationsOutput struct {
	ResolverRuleAssociations []resolverRuleAssociationOutput `json:"ResolverRuleAssociations"`
}

func (h *Handler) handleListResolverRuleAssociations(
	ctx context.Context,
	_ *listResolverRuleAssociationsInput,
) (*listResolverRuleAssociationsOutput, error) {
	assocs := h.Backend.ListResolverRuleAssociations(ctx)
	items := make([]resolverRuleAssociationOutput, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, ruleAssociationToOutput(a))
	}

	return &listResolverRuleAssociationsOutput{ResolverRuleAssociations: items}, nil
}

// --- GetResolverRulePolicy ---

type getResolverRulePolicyInput struct {
	Arn string `json:"Arn"`
}

type getResolverRulePolicyOutput struct {
	ResolverRulePolicy string `json:"ResolverRulePolicy"`
}

func (h *Handler) handleGetResolverRulePolicy(
	ctx context.Context,
	in *getResolverRulePolicyInput,
) (*getResolverRulePolicyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}
	policy := h.Backend.GetResolverRulePolicy(ctx, in.Arn)

	return &getResolverRulePolicyOutput{ResolverRulePolicy: policy}, nil
}

// --- PutResolverRulePolicy ---

type putResolverRulePolicyInput struct {
	Arn                string `json:"Arn"`
	ResolverRulePolicy string `json:"ResolverRulePolicy"`
}

type putResolverRulePolicyOutput struct {
	ReturnValue bool `json:"ReturnValue"`
}

func (h *Handler) handlePutResolverRulePolicy(
	ctx context.Context,
	in *putResolverRulePolicyInput,
) (*putResolverRulePolicyOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", ErrValidation)
	}
	if err := h.Backend.PutResolverRulePolicy(ctx, in.Arn, in.ResolverRulePolicy); err != nil {
		return nil, err
	}

	return &putResolverRulePolicyOutput{ReturnValue: true}, nil
}

// --- UpdateResolverEndpoint ---

type updateResolverEndpointInput struct {
	ResolverEndpointID   string   `json:"ResolverEndpointId"`
	Name                 string   `json:"Name"`
	ResolverEndpointType string   `json:"ResolverEndpointType"`
	Protocols            []string `json:"Protocols"`
}

type updateResolverEndpointOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

func (h *Handler) handleUpdateResolverEndpoint(
	ctx context.Context,
	in *updateResolverEndpointInput,
) (*updateResolverEndpointOutput, error) {
	if in.ResolverEndpointID == "" {
		return nil, fmt.Errorf("%w: ResolverEndpointId is required", ErrValidation)
	}
	ep, err := h.Backend.UpdateResolverEndpoint(
		ctx,
		in.ResolverEndpointID,
		in.Name,
		in.ResolverEndpointType,
		in.Protocols,
	)
	if err != nil {
		return nil, err
	}

	return &updateResolverEndpointOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

// --- DisassociateResolverEndpointIpAddress ---

type disassociateResolverEndpointIPAddressInput struct {
	ResolverEndpointID string               `json:"ResolverEndpointId"`
	IPAddress          ipAddressRemoveInput `json:"IpAddress"`
}

type ipAddressRemoveInput struct {
	IPID     string `json:"IpId"`
	IP       string `json:"Ip"`
	SubnetID string `json:"SubnetId"`
}

type disassociateResolverEndpointIPAddressOutput struct {
	ResolverEndpoint resolverEndpointOutput `json:"ResolverEndpoint"`
}

func (h *Handler) handleDisassociateResolverEndpointIPAddress(
	ctx context.Context,
	in *disassociateResolverEndpointIPAddressInput,
) (*disassociateResolverEndpointIPAddressOutput, error) {
	if in.ResolverEndpointID == "" {
		return nil, fmt.Errorf("%w: ResolverEndpointId is required", ErrValidation)
	}
	if in.IPAddress.IPID == "" {
		return nil, fmt.Errorf("%w: IpAddress.IpId is required", ErrValidation)
	}
	ep, err := h.Backend.DisassociateResolverEndpointIPAddress(
		ctx,
		in.ResolverEndpointID,
		in.IPAddress.IPID,
	)
	if err != nil {
		return nil, err
	}

	return &disassociateResolverEndpointIPAddressOutput{ResolverEndpoint: endpointToOutput(ep)}, nil
}

// --- UpdateResolverRule ---

type updateResolverRuleInput struct {
	ResolverRuleID string             `json:"ResolverRuleId"`
	Config         resolverRuleConfig `json:"Config"`
}

type resolverRuleConfig struct {
	Name               string     `json:"Name"`
	ResolverEndpointID string     `json:"ResolverEndpointId"`
	TargetIps          []targetIP `json:"TargetIps"`
}

type updateResolverRuleOutput struct {
	ResolverRule resolverRuleOutput `json:"ResolverRule"`
}

func (h *Handler) handleUpdateResolverRule(
	ctx context.Context,
	in *updateResolverRuleInput,
) (*updateResolverRuleOutput, error) {
	if in.ResolverRuleID == "" {
		return nil, fmt.Errorf("%w: ResolverRuleId is required", ErrValidation)
	}

	tips := make([]TargetIP, 0, len(in.Config.TargetIps))
	for _, t := range in.Config.TargetIps {
		tips = append(
			tips,
			TargetIP(t),
		)
	}
	if len(tips) == 0 {
		tips = nil
	}

	r, err := h.Backend.UpdateResolverRule(
		ctx,
		in.ResolverRuleID,
		in.Config.Name,
		in.Config.ResolverEndpointID,
		tips,
	)
	if err != nil {
		return nil, err
	}

	return &updateResolverRuleOutput{ResolverRule: ruleToOutput(r)}, nil
}

// --- GetResolverConfig ---

type getResolverConfigInput struct {
	ResourceID string `json:"ResourceId"`
}

type getResolverConfigOutput struct {
	ResolverConfig resolverConfigOutput `json:"ResolverConfig"`
}

func (h *Handler) handleGetResolverConfig(
	ctx context.Context,
	in *getResolverConfigInput,
) (*getResolverConfigOutput, error) {
	if in.ResourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}
	cfg := h.Backend.GetResolverConfig(ctx, in.ResourceID)

	return &getResolverConfigOutput{ResolverConfig: resolverConfigToOutput(cfg)}, nil
}

// --- UpdateResolverConfig ---

type updateResolverConfigInput struct {
	ResourceID         string `json:"ResourceId"`
	AutodefinedReverse string `json:"AutodefinedReverse"`
}

type updateResolverConfigOutput struct {
	ResolverConfig resolverConfigOutput `json:"ResolverConfig"`
}

func (h *Handler) handleUpdateResolverConfig(
	ctx context.Context,
	in *updateResolverConfigInput,
) (*updateResolverConfigOutput, error) {
	if in.ResourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}
	cfg, err := h.Backend.UpdateResolverConfig(ctx, in.ResourceID, in.AutodefinedReverse)
	if err != nil {
		return nil, err
	}

	return &updateResolverConfigOutput{ResolverConfig: resolverConfigToOutput(cfg)}, nil
}

// --- ListResolverConfigs ---

type listResolverConfigsInput struct{}

type listResolverConfigsOutput struct {
	ResolverConfigs []resolverConfigOutput `json:"ResolverConfigs"`
}

func (h *Handler) handleListResolverConfigs(
	ctx context.Context,
	_ *listResolverConfigsInput,
) (*listResolverConfigsOutput, error) {
	configs := h.Backend.ListResolverConfigs(ctx)
	items := make([]resolverConfigOutput, 0, len(configs))
	for _, c := range configs {
		items = append(items, resolverConfigToOutput(c))
	}

	return &listResolverConfigsOutput{ResolverConfigs: items}, nil
}

// --- GetResolverDnssecConfig ---

type getResolverDnssecConfigInput struct {
	ResourceID string `json:"ResourceId"`
}

type getResolverDnssecConfigOutput struct {
	ResolverDNSSECConfig resolverDnssecConfigOutput `json:"ResolverDNSSECConfig"`
}

func (h *Handler) handleGetResolverDnssecConfig(
	ctx context.Context,
	in *getResolverDnssecConfigInput,
) (*getResolverDnssecConfigOutput, error) {
	if in.ResourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}
	cfg := h.Backend.GetResolverDnssecConfig(ctx, in.ResourceID)

	return &getResolverDnssecConfigOutput{
		ResolverDNSSECConfig: resolverDnssecConfigToOutput(cfg),
	}, nil
}

// --- UpdateResolverDnssecConfig ---

type updateResolverDnssecConfigInput struct {
	ResourceID string `json:"ResourceId"`
	Validation string `json:"Validation"`
}

type updateResolverDnssecConfigOutput struct {
	ResolverDNSSECConfig resolverDnssecConfigOutput `json:"ResolverDNSSECConfig"`
}

func (h *Handler) handleUpdateResolverDnssecConfig(
	ctx context.Context,
	in *updateResolverDnssecConfigInput,
) (*updateResolverDnssecConfigOutput, error) {
	if in.ResourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidation)
	}
	cfg, err := h.Backend.UpdateResolverDnssecConfig(ctx, in.ResourceID, in.Validation)
	if err != nil {
		return nil, err
	}

	return &updateResolverDnssecConfigOutput{
		ResolverDNSSECConfig: resolverDnssecConfigToOutput(cfg),
	}, nil
}

// --- ListResolverDnssecConfigs ---

type listResolverDnssecConfigsInput struct{}

type listResolverDnssecConfigsOutput struct {
	ResolverDnssecConfigs []resolverDnssecConfigOutput `json:"ResolverDnssecConfigs"`
}

func (h *Handler) handleListResolverDnssecConfigs(
	ctx context.Context,
	_ *listResolverDnssecConfigsInput,
) (*listResolverDnssecConfigsOutput, error) {
	configs := h.Backend.ListResolverDnssecConfigs(ctx)
	items := make([]resolverDnssecConfigOutput, 0, len(configs))
	for _, c := range configs {
		items = append(items, resolverDnssecConfigToOutput(c))
	}

	return &listResolverDnssecConfigsOutput{ResolverDnssecConfigs: items}, nil
}
