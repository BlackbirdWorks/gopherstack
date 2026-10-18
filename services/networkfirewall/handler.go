package networkfirewall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// targetPrefix is the X-Amz-Target prefix, from awsjson.New10(schemas.NetworkFirewall_20201112)
// in aws-sdk-go-v2/service/networkfirewall@v1.76.1 api_client.go.
const targetPrefix = "NetworkFirewall_20201112."

// Handler is the HTTP handler for the AWS Network Firewall API.
type Handler struct {
	peers         *regionpeers.Set[Handler]
	Backend       *InMemoryBackend
	ops           map[string]service.JSONOpFunc
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a Network Firewall handler over backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "NetworkFirewall" }

// GetSupportedOperations returns the supported operation names.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for op := range h.ops {
		ops = append(ops, op)
	}

	slices.Sort(ops)

	return ops
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "network-firewall" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions this handler serves.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher matches requests carrying the NetworkFirewall_20201112 X-Amz-Target prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the routing priority for header-exact matching.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the action name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
}

// ExtractResource extracts the firewall, policy, rule group or resource identifier from the body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if json.Unmarshal(body, &data) != nil {
		return ""
	}

	for _, key := range []string{
		"FirewallName", "FirewallArn", "FirewallPolicyName", "FirewallPolicyArn", "RuleGroupName",
		"RuleGroupArn", "TLSInspectionConfigurationName", "TLSInspectionConfigurationArn", "ResourceArn",
		"ProxyName", "ProxyArn", "ProxyConfigurationName", "ProxyConfigurationArn", "ProxyRuleGroupName",
		"ProxyRuleGroupArn", "ContainerAssociationName", "ContainerAssociationArn", "VpcEndpointAssociationArn",
		"TransitGatewayAttachmentId",
	} {
		if s, ok := data[key].(string); ok && s != "" {
			return s
		}
	}

	return ""
}

func (h *Handler) homeHandler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"NetworkFirewall", "application/x-amz-json-1.0",
			h.GetSupportedOperations(),
			h.dispatch,
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
	_, isSyntax := errors.AsType[*json.SyntaxError](err)
	_, isType := errors.AsType[*json.UnmarshalTypeError](err)

	if isSyntax || isType {
		err = invalid("The request body is not valid: %v", err)
	}

	status, code := classifyError(err)
	if status == http.StatusInternalServerError {
		logger.Load(c.Request().Context()).
			ErrorContext(c.Request().Context(), "networkfirewall: internal error", "error", err)
	}

	return c.JSON(status, service.JSONErrorResponse{Type: code, Message: err.Error()})
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	b := h.Backend

	return map[string]service.JSONOpFunc{
		"CreateFirewall":                        service.WrapOp(b.CreateFirewall),
		"DescribeFirewall":                      service.WrapOp(b.DescribeFirewall),
		"DescribeFirewallMetadata":              service.WrapOp(b.DescribeFirewallMetadata),
		"ListFirewalls":                         service.WrapOp(b.ListFirewalls),
		"DeleteFirewall":                        service.WrapOp(b.DeleteFirewall),
		"UpdateFirewallDescription":             service.WrapOp(b.UpdateFirewallDescription),
		"UpdateFirewallDeleteProtection":        service.WrapOp(b.UpdateFirewallDeleteProtection),
		"UpdateSubnetChangeProtection":          service.WrapOp(b.UpdateSubnetChangeProtection),
		"UpdateFirewallPolicyChangeProtection":  service.WrapOp(b.UpdateFirewallPolicyChangeProtection),
		"UpdateFirewallEncryptionConfiguration": service.WrapOp(b.UpdateFirewallEncryptionConfiguration),
		"AssociateFirewallPolicy":               service.WrapOp(b.AssociateFirewallPolicy),
		"AssociateSubnets":                      service.WrapOp(b.AssociateSubnets),
		"DisassociateSubnets":                   service.WrapOp(b.DisassociateSubnets),
		"CreateFirewallPolicy":                  service.WrapOp(b.CreateFirewallPolicy),
		"DescribeFirewallPolicy":                service.WrapOp(b.DescribeFirewallPolicy),
		"ListFirewallPolicies":                  service.WrapOp(b.ListFirewallPolicies),
		"UpdateFirewallPolicy":                  service.WrapOp(b.UpdateFirewallPolicy),
		"DeleteFirewallPolicy":                  service.WrapOp(b.DeleteFirewallPolicy),
		"CreateRuleGroup":                       service.WrapOp(b.CreateRuleGroup),
		"DescribeRuleGroup":                     service.WrapOp(b.DescribeRuleGroup),
		"DescribeRuleGroupMetadata":             service.WrapOp(b.DescribeRuleGroupMetadata),
		"ListRuleGroups":                        service.WrapOp(b.ListRuleGroups),
		"UpdateRuleGroup":                       service.WrapOp(b.UpdateRuleGroup),
		"DeleteRuleGroup":                       service.WrapOp(b.DeleteRuleGroup),
		"CreateTLSInspectionConfiguration":      service.WrapOp(b.CreateTLSInspectionConfiguration),
		"DescribeTLSInspectionConfiguration":    service.WrapOp(b.DescribeTLSInspectionConfiguration),
		"ListTLSInspectionConfigurations":       service.WrapOp(b.ListTLSInspectionConfigurations),
		"UpdateTLSInspectionConfiguration":      service.WrapOp(b.UpdateTLSInspectionConfiguration),
		"DeleteTLSInspectionConfiguration":      service.WrapOp(b.DeleteTLSInspectionConfiguration),
		"DescribeLoggingConfiguration":          service.WrapOp(b.DescribeLoggingConfiguration),
		"UpdateLoggingConfiguration":            service.WrapOp(b.UpdateLoggingConfiguration),
		"PutResourcePolicy":                     service.WrapOp(b.PutResourcePolicy),
		"DescribeResourcePolicy":                service.WrapOp(b.DescribeResourcePolicy),
		"DeleteResourcePolicy":                  service.WrapOp(b.DeleteResourcePolicy),
		"TagResource":                           service.WrapOp(b.TagResource),
		"UntagResource":                         service.WrapOp(b.UntagResource),
		"ListTagsForResource":                   service.WrapOp(b.ListTagsForResource),
		"AcceptNetworkFirewallTransitGatewayAttachment": service.WrapOp(
			b.AcceptNetworkFirewallTransitGatewayAttachment,
		),
		"AssociateAvailabilityZones":           service.WrapOp(b.AssociateAvailabilityZones),
		"AttachRuleGroupsToProxyConfiguration": service.WrapOp(b.AttachRuleGroupsToProxyConfiguration),
		"CreateContainerAssociation":           service.WrapOp(b.CreateContainerAssociation),
		"CreateProxy":                          service.WrapOp(b.CreateProxy),
		"CreateProxyConfiguration":             service.WrapOp(b.CreateProxyConfiguration),
		"CreateProxyRuleGroup":                 service.WrapOp(b.CreateProxyRuleGroup),
		"CreateProxyRules":                     service.WrapOp(b.CreateProxyRules),
		"CreateVpcEndpointAssociation":         service.WrapOp(b.CreateVpcEndpointAssociation),
		"DeleteContainerAssociation":           service.WrapOp(b.DeleteContainerAssociation),
		"DeleteNetworkFirewallTransitGatewayAttachment": service.WrapOp(
			b.DeleteNetworkFirewallTransitGatewayAttachment,
		),
		"DeleteProxy":                            service.WrapOp(b.DeleteProxy),
		"DeleteProxyConfiguration":               service.WrapOp(b.DeleteProxyConfiguration),
		"DeleteProxyRuleGroup":                   service.WrapOp(b.DeleteProxyRuleGroup),
		"DeleteProxyRules":                       service.WrapOp(b.DeleteProxyRules),
		"DeleteVpcEndpointAssociation":           service.WrapOp(b.DeleteVpcEndpointAssociation),
		"DescribeContainerAssociation":           service.WrapOp(b.DescribeContainerAssociation),
		"DescribeFlowOperation":                  service.WrapOp(b.DescribeFlowOperation),
		"DescribeProxy":                          service.WrapOp(b.DescribeProxy),
		"DescribeProxyConfiguration":             service.WrapOp(b.DescribeProxyConfiguration),
		"DescribeProxyRule":                      service.WrapOp(b.DescribeProxyRule),
		"DescribeProxyRuleGroup":                 service.WrapOp(b.DescribeProxyRuleGroup),
		"DescribeRuleGroupSummary":               service.WrapOp(b.DescribeRuleGroupSummary),
		"DescribeVpcEndpointAssociation":         service.WrapOp(b.DescribeVpcEndpointAssociation),
		"DetachRuleGroupsFromProxyConfiguration": service.WrapOp(b.DetachRuleGroupsFromProxyConfiguration),
		"DisassociateAvailabilityZones":          service.WrapOp(b.DisassociateAvailabilityZones),
		"GetAnalysisReportResults":               service.WrapOp(b.GetAnalysisReportResults),
		"ListAnalysisReports":                    service.WrapOp(b.ListAnalysisReports),
		"ListContainerAssociations":              service.WrapOp(b.ListContainerAssociations),
		"ListFlowOperationResults":               service.WrapOp(b.ListFlowOperationResults),
		"ListFlowOperations":                     service.WrapOp(b.ListFlowOperations),
		"ListProxies":                            service.WrapOp(b.ListProxies),
		"ListProxyConfigurations":                service.WrapOp(b.ListProxyConfigurations),
		"ListProxyRuleGroups":                    service.WrapOp(b.ListProxyRuleGroups),
		"ListVpcEndpointAssociations":            service.WrapOp(b.ListVpcEndpointAssociations),
		"RejectNetworkFirewallTransitGatewayAttachment": service.WrapOp(
			b.RejectNetworkFirewallTransitGatewayAttachment,
		),
		"StartAnalysisReport":                    service.WrapOp(b.StartAnalysisReport),
		"StartFlowCapture":                       service.WrapOp(b.StartFlowCapture),
		"StartFlowFlush":                         service.WrapOp(b.StartFlowFlush),
		"UpdateAvailabilityZoneChangeProtection": service.WrapOp(b.UpdateAvailabilityZoneChangeProtection),
		"UpdateContainerAssociation":             service.WrapOp(b.UpdateContainerAssociation),
		"UpdateFirewallAnalysisSettings":         service.WrapOp(b.UpdateFirewallAnalysisSettings),
		"UpdateProxy":                            service.WrapOp(b.UpdateProxy),
		"UpdateProxyConfiguration":               service.WrapOp(b.UpdateProxyConfiguration),
		"UpdateProxyRule":                        service.WrapOp(b.UpdateProxyRule),
		"UpdateProxyRuleGroupPriorities":         service.WrapOp(b.UpdateProxyRuleGroupPriorities),
		"UpdateProxyRulePriorities":              service.WrapOp(b.UpdateProxyRulePriorities),
		"UpdateProxySettings":                    service.WrapOp(b.UpdateProxySettings),
	}
}
