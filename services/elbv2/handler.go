package elbv2

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	elbv2Version = "2015-12-01"
	elbv2XMLNS   = "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/"
)

// Handler is the Echo HTTP handler for ELBv2 operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new ELBv2 handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ELBv2" }

// GetSupportedOperations returns the list of supported ELBv2 operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateLoadBalancer",
		"DeleteLoadBalancer",
		"DescribeLoadBalancers",
		"ModifyLoadBalancerAttributes",
		"DescribeLoadBalancerAttributes",
		"CreateTargetGroup",
		"DeleteTargetGroup",
		"DescribeTargetGroups",
		"ModifyTargetGroup",
		"ModifyTargetGroupAttributes",
		"DescribeTargetGroupAttributes",
		"RegisterTargets",
		"DeregisterTargets",
		"DescribeTargetHealth",
		"CreateListener",
		"DeleteListener",
		"DescribeListeners",
		"ModifyListener",
		"ModifyListenerAttributes",
		"DescribeListenerAttributes",
		"CreateRule",
		"DeleteRule",
		"DescribeRules",
		"ModifyRule",
		"SetRulePriorities",
		"AddTags",
		"RemoveTags",
		"DescribeTags",
		"SetSecurityGroups",
		"SetSubnets",
		"SetIpAddressType",
		"AddListenerCertificates",
		"AddTrustStoreRevocations",
		"CreateTrustStore",
		"DeleteSharedTrustStoreAssociation",
		"DeleteTrustStore",
		"DescribeAccountLimits",
		"DescribeCapacityReservation",
		"DescribeListenerCertificates",
		"DescribeSSLPolicies",
		"DescribeTrustStoreAssociations",
		"DescribeTrustStoreRevocations",
		"DescribeTrustStores",
		"GetResourcePolicy",
		"GetTrustStoreCaCertificatesBundle",
		"GetTrustStoreRevocationContent",
		"ModifyCapacityReservation",
		"ModifyIpPools",
		"ModifyTrustStore",
		"RemoveListenerCertificates",
		"RemoveTrustStoreRevocations",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "elasticloadbalancingv2" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches ELBv2 requests.
// ELBv2 requests are form-encoded POSTs with Version=2015-12-01.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}

		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}

		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == elbv2Version
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityFormStandard }

// ExtractOperation extracts the ELBv2 action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return "Unknown"
	}

	action := r.Form.Get("Action")
	if action == "" {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the primary resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	if name := r.Form.Get("Name"); name != "" {
		return name
	}

	return r.Form.Get("LoadBalancerArn")
}

// Handler returns the Echo handler function for ELBv2 operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		if err := r.ParseForm(); err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		vals := r.Form
		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		log := logger.Load(r.Context())
		log.Debug("elbv2 request", "action", action)

		resp, opErr := h.dispatch(action, vals)
		if opErr != nil {
			return h.handleOpError(c, action, opErr)
		}

		xmlBytes, err := marshalXML(resp)
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

// dispatch routes the ELBv2 action to the appropriate handler.
type dispatchFunc func(url.Values) (any, error)

func (h *Handler) buildDispatchTable() map[string]dispatchFunc {
	return map[string]dispatchFunc{
		"CreateLoadBalancer":                h.handleCreateLoadBalancer,
		"DeleteLoadBalancer":                h.handleDeleteLoadBalancer,
		"DescribeLoadBalancers":             h.handleDescribeLoadBalancers,
		"ModifyLoadBalancerAttributes":      h.handleModifyLoadBalancerAttributes,
		"DescribeLoadBalancerAttributes":    h.handleDescribeLoadBalancerAttributes,
		"SetSecurityGroups":                 h.handleSetSecurityGroups,
		"SetSubnets":                        h.handleSetSubnets,
		"SetIpAddressType":                  h.handleSetIPAddressType,
		"CreateTargetGroup":                 h.handleCreateTargetGroup,
		"DeleteTargetGroup":                 h.handleDeleteTargetGroup,
		"DescribeTargetGroups":              h.handleDescribeTargetGroups,
		"ModifyTargetGroup":                 h.handleModifyTargetGroup,
		"ModifyTargetGroupAttributes":       h.handleModifyTargetGroupAttributes,
		"DescribeTargetGroupAttributes":     h.handleDescribeTargetGroupAttributes,
		"RegisterTargets":                   h.handleRegisterTargets,
		"DeregisterTargets":                 h.handleDeregisterTargets,
		"DescribeTargetHealth":              h.handleDescribeTargetHealth,
		"CreateListener":                    h.handleCreateListener,
		"DeleteListener":                    h.handleDeleteListener,
		"DescribeListeners":                 h.handleDescribeListeners,
		"ModifyListener":                    h.handleModifyListener,
		"ModifyListenerAttributes":          h.handleModifyListenerAttributes,
		"DescribeListenerAttributes":        h.handleDescribeListenerAttributes,
		"CreateRule":                        h.handleCreateRule,
		"DeleteRule":                        h.handleDeleteRule,
		"DescribeRules":                     h.handleDescribeRules,
		"ModifyRule":                        h.handleModifyRule,
		"SetRulePriorities":                 h.handleSetRulePriorities,
		"AddTags":                           h.handleAddTags,
		"RemoveTags":                        h.handleRemoveTags,
		"DescribeTags":                      h.handleDescribeTags,
		"AddListenerCertificates":           h.handleAddListenerCertificates,
		"AddTrustStoreRevocations":          h.handleAddTrustStoreRevocations,
		"CreateTrustStore":                  h.handleCreateTrustStore,
		"DeleteSharedTrustStoreAssociation": h.handleDeleteSharedTrustStoreAssociation,
		"DeleteTrustStore":                  h.handleDeleteTrustStore,
		"DescribeAccountLimits":             h.handleDescribeAccountLimits,
		"DescribeCapacityReservation":       h.handleDescribeCapacityReservation,
		"DescribeListenerCertificates":      h.handleDescribeListenerCertificates,
		"DescribeSSLPolicies":               h.handleDescribeSSLPolicies,
		"DescribeTrustStoreAssociations":    h.handleDescribeTrustStoreAssociations,
		"DescribeTrustStoreRevocations":     h.handleDescribeTrustStoreRevocations,
		"DescribeTrustStores":               h.handleDescribeTrustStores,
		"GetResourcePolicy":                 h.handleGetResourcePolicy,
		"GetTrustStoreCaCertificatesBundle": h.handleGetTrustStoreCaCertificatesBundle,
		"GetTrustStoreRevocationContent":    h.handleGetTrustStoreRevocationContent,
		"ModifyCapacityReservation":         h.handleModifyCapacityReservation,
		"ModifyIpPools":                     h.handleModifyIPPools,
		"ModifyTrustStore":                  h.handleModifyTrustStore,
		"RemoveListenerCertificates":        h.handleRemoveListenerCertificates,
		"RemoveTrustStoreRevocations":       h.handleRemoveTrustStoreRevocations,
	}
}

func (h *Handler) dispatch(action string, vals url.Values) (any, error) {
	table := h.buildDispatchTable()

	fn, ok := table[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownAction, action)
	}

	return fn(vals)
}

// --- load balancer handlers ---

func (h *Handler) handleCreateLoadBalancer(vals url.Values) (any, error) {
	name := vals.Get("Name")
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	azs := parseMembers(vals, "AvailabilityZones.member")
	sgs := parseMembers(vals, "SecurityGroups.member")
	tagKVs := parseTagKVs(vals)

	lb, err := h.Backend.CreateLoadBalancer(CreateLoadBalancerInput{
		Name:              name,
		Scheme:            vals.Get("Scheme"),
		Type:              vals.Get("Type"),
		IPAddressType:     vals.Get("IpAddressType"),
		AvailabilityZones: azs,
		SecurityGroups:    sgs,
		Tags:              tagKVs,
	})
	if err != nil {
		return nil, err
	}

	return &createLoadBalancerResponse{
		Xmlns: elbv2XMLNS,
		Result: createLoadBalancerResult{
			LoadBalancers: xmlLoadBalancerList{
				Members: []xmlLoadBalancer{toXMLLoadBalancer(lb)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-create-" + name},
	}, nil
}

func (h *Handler) handleDeleteLoadBalancer(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteLoadBalancer(lbArn); err != nil {
		return nil, err
	}

	return &deleteLoadBalancerResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-lb"},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancers(vals url.Values) (any, error) {
	arns := parseMembers(vals, "LoadBalancerArns.member")
	names := parseMembers(vals, "Names.member")

	lbs, err := h.Backend.DescribeLoadBalancers(arns, names)
	if err != nil {
		return nil, err
	}

	members := make([]xmlLoadBalancer, 0, len(lbs))
	for i := range lbs {
		members = append(members, toXMLLoadBalancer(&lbs[i]))
	}

	return &describeLoadBalancersResponse{
		Xmlns: elbv2XMLNS,
		Result: describeLoadBalancersResult{
			LoadBalancers: xmlLoadBalancerList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-lbs"},
	}, nil
}

func (h *Handler) handleModifyLoadBalancerAttributes(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	_, err := h.Backend.ModifyLoadBalancerAttributes(lbArn)
	if err != nil {
		return nil, err
	}

	return &modifyLoadBalancerAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyLoadBalancerAttributesResult{
			Attributes: xmlLBAttributeList{Members: []xmlLBAttribute{}},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-lb-attrs"},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancerAttributes(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	_, err := h.Backend.ModifyLoadBalancerAttributes(lbArn)
	if err != nil {
		return nil, err
	}

	return &describeLoadBalancerAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeLoadBalancerAttributesResult{
			Attributes: xmlLBAttributeList{
				Members: []xmlLBAttribute{
					{Key: "access_logs.s3.enabled", Value: "false"},
					{Key: "deletion_protection.enabled", Value: "false"},
					{Key: "idle_timeout.timeout_seconds", Value: "60"},
				},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-lb-attrs"},
	}, nil
}

func (h *Handler) handleSetSecurityGroups(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	return &setSecurityGroupsResponse{
		Xmlns:            elbv2XMLNS,
		Result:           setSecurityGroupsResult{SecurityGroupIDs: xmlStringList{Members: []xmlStringValue{}}},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-sgs"},
	}, nil
}

func (h *Handler) handleSetSubnets(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	return &setSubnetsResponse{
		Xmlns:            elbv2XMLNS,
		Result:           setSubnetsResult{AvailabilityZones: xmlStringList{Members: []xmlStringValue{}}},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-subnets"},
	}, nil
}

func (h *Handler) handleSetIPAddressType(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	return &setIPAddressTypeResponse{
		Xmlns:            elbv2XMLNS,
		Result:           setIPAddressTypeResult{IPAddressType: vals.Get("IpAddressType")},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-ip-type"},
	}, nil
}

// --- target group handlers ---

func (h *Handler) handleCreateTargetGroup(vals url.Values) (any, error) {
	name := vals.Get("Name")
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	portStr := vals.Get("Port")
	if portStr == "" {
		return nil, fmt.Errorf("%w: Port is required", ErrInvalidParameter)
	}

	port, err := parseInt32(portStr)
	if err != nil || port <= 0 {
		return nil, fmt.Errorf("%w: invalid Port", ErrInvalidParameter)
	}

	tagKVs := parseTagKVs(vals)

	tg, createErr := h.Backend.CreateTargetGroup(CreateTargetGroupInput{
		Name:       name,
		Protocol:   vals.Get("Protocol"),
		Port:       port,
		VpcID:      vals.Get("VpcId"),
		TargetType: vals.Get("TargetType"),
		Tags:       tagKVs,
	})
	if createErr != nil {
		return nil, createErr
	}

	return &createTargetGroupResponse{
		Xmlns: elbv2XMLNS,
		Result: createTargetGroupResult{
			TargetGroups: xmlTargetGroupList{
				Members: []xmlTargetGroup{toXMLTargetGroup(tg)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-create-tg-" + name},
	}, nil
}

func (h *Handler) handleDeleteTargetGroup(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteTargetGroup(tgArn); err != nil {
		return nil, err
	}

	return &deleteTargetGroupResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-tg"},
	}, nil
}

func (h *Handler) handleDescribeTargetGroups(vals url.Values) (any, error) {
	arns := parseMembers(vals, "TargetGroupArns.member")
	names := parseMembers(vals, "Names.member")
	lbArn := vals.Get("LoadBalancerArn")

	tgs, err := h.Backend.DescribeTargetGroups(arns, names, lbArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTargetGroup, 0, len(tgs))
	for i := range tgs {
		members = append(members, toXMLTargetGroup(&tgs[i]))
	}

	return &describeTargetGroupsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTargetGroupsResult{
			TargetGroups: xmlTargetGroupList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-tgs"},
	}, nil
}

func (h *Handler) handleModifyTargetGroup(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	tgs, err := h.Backend.DescribeTargetGroups([]string{tgArn}, nil, "")
	if err != nil {
		return nil, err
	}

	if len(tgs) == 0 {
		return nil, ErrTargetGroupNotFound
	}

	return &modifyTargetGroupResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyTargetGroupResult{
			TargetGroups: xmlTargetGroupList{
				Members: []xmlTargetGroup{toXMLTargetGroup(&tgs[0])},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-tg"},
	}, nil
}

func (h *Handler) handleModifyTargetGroupAttributes(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	tgs, err := h.Backend.DescribeTargetGroups([]string{tgArn}, nil, "")
	if err != nil {
		return nil, err
	}

	if len(tgs) == 0 {
		return nil, ErrTargetGroupNotFound
	}

	return &modifyTargetGroupAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyTargetGroupAttributesResult{
			Attributes: xmlTGAttributeList{Members: []xmlTGAttribute{}},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-tg-attrs"},
	}, nil
}

func (h *Handler) handleDescribeTargetGroupAttributes(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	tgs, err := h.Backend.DescribeTargetGroups([]string{tgArn}, nil, "")
	if err != nil {
		return nil, err
	}

	if len(tgs) == 0 {
		return nil, ErrTargetGroupNotFound
	}

	return &describeTargetGroupAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTargetGroupAttributesResult{
			Attributes: xmlTGAttributeList{
				Members: []xmlTGAttribute{
					{Key: "deregistration_delay.timeout_seconds", Value: "300"},
					{Key: "stickiness.enabled", Value: "false"},
					{Key: "load_balancing.algorithm.type", Value: "round_robin"},
				},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-tg-attrs"},
	}, nil
}

// --- target handlers ---

func (h *Handler) handleRegisterTargets(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	targets := parseTargets(vals, "Targets.member")

	if err := h.Backend.RegisterTargets(tgArn, targets); err != nil {
		return nil, err
	}

	return &registerTargetsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-register-targets"},
	}, nil
}

func (h *Handler) handleDeregisterTargets(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	targets := parseTargets(vals, "Targets.member")

	if err := h.Backend.DeregisterTargets(tgArn, targets); err != nil {
		return nil, err
	}

	return &deregisterTargetsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-deregister-targets"},
	}, nil
}

func (h *Handler) handleDescribeTargetHealth(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	targets, err := h.Backend.DescribeTargetHealth(tgArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTargetHealthDescription, 0, len(targets))
	for _, t := range targets {
		members = append(members, xmlTargetHealthDescription{
			Target: xmlTargetDescription(t),
			TargetHealth: xmlTargetHealth{
				State: "healthy",
			},
		})
	}

	return &describeTargetHealthResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTargetHealthResult{
			TargetHealthDescriptions: xmlTargetHealthDescriptionList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-target-health"},
	}, nil
}

// --- listener handlers ---

func (h *Handler) handleCreateListener(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	portStr := vals.Get("Port")
	if portStr == "" {
		return nil, fmt.Errorf("%w: Port is required", ErrInvalidParameter)
	}

	port, err := parseInt32(portStr)
	if err != nil || port <= 0 {
		return nil, fmt.Errorf("%w: invalid Port", ErrInvalidParameter)
	}

	protocol := vals.Get("Protocol")
	if protocol == "" {
		return nil, fmt.Errorf("%w: Protocol is required", ErrInvalidParameter)
	}

	actions := parseActions(vals, "DefaultActions.member")
	tagKVs := parseTagKVs(vals)

	listener, createErr := h.Backend.CreateListener(CreateListenerInput{
		LoadBalancerArn: lbArn,
		Protocol:        protocol,
		Port:            port,
		DefaultActions:  actions,
		Tags:            tagKVs,
	})
	if createErr != nil {
		return nil, createErr
	}

	return &createListenerResponse{
		Xmlns: elbv2XMLNS,
		Result: createListenerResult{
			Listeners: xmlListenerList{
				Members: []xmlListener{toXMLListener(listener)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-create-listener"},
	}, nil
}

func (h *Handler) handleDeleteListener(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteListener(listenerArn); err != nil {
		return nil, err
	}

	return &deleteListenerResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-listener"},
	}, nil
}

func (h *Handler) handleDescribeListeners(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	listenerArns := parseMembers(vals, "ListenerArns.member")

	listeners, err := h.Backend.DescribeListeners(lbArn, listenerArns)
	if err != nil {
		return nil, err
	}

	members := make([]xmlListener, 0, len(listeners))
	for i := range listeners {
		members = append(members, toXMLListener(&listeners[i]))
	}

	return &describeListenersResponse{
		Xmlns: elbv2XMLNS,
		Result: describeListenersResult{
			Listeners: xmlListenerList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-listeners"},
	}, nil
}

func (h *Handler) handleModifyListener(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	listeners, err := h.Backend.DescribeListeners("", []string{listenerArn})
	if err != nil {
		return nil, err
	}

	if len(listeners) == 0 {
		return nil, ErrListenerNotFound
	}

	return &modifyListenerResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyListenerResult{
			Listeners: xmlListenerList{
				Members: []xmlListener{toXMLListener(&listeners[0])},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-listener"},
	}, nil
}

func (h *Handler) handleModifyListenerAttributes(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	listeners, err := h.Backend.DescribeListeners("", []string{listenerArn})
	if err != nil {
		return nil, err
	}

	if len(listeners) == 0 {
		return nil, ErrListenerNotFound
	}

	return &modifyListenerAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyListenerAttributesResult{
			Attributes: xmlListenerAttributeList{Members: []xmlListenerAttribute{}},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-listener-attrs"},
	}, nil
}

func (h *Handler) handleDescribeListenerAttributes(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	listeners, err := h.Backend.DescribeListeners("", []string{listenerArn})
	if err != nil {
		return nil, err
	}

	if len(listeners) == 0 {
		return nil, ErrListenerNotFound
	}

	return &describeListenerAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeListenerAttributesResult{
			Attributes: xmlListenerAttributeList{
				Members: []xmlListenerAttribute{
					{Key: "routing.http.desync_mitigation_mode", Value: "defensive"},
					{Key: "routing.http2.enabled", Value: "true"},
					{Key: "idle_timeout.timeout_seconds", Value: "60"},
				},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-listener-attrs"},
	}, nil
}

// --- rule handlers ---

func (h *Handler) handleCreateRule(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	actions := parseActions(vals, "Actions.member")
	conditions := parseConditions(vals, "Conditions.member")

	rule, err := h.Backend.CreateRule(CreateRuleInput{
		ListenerArn: listenerArn,
		Priority:    vals.Get("Priority"),
		Actions:     actions,
		Conditions:  conditions,
		Tags:        parseTagKVs(vals),
	})
	if err != nil {
		return nil, err
	}

	return &createRuleResponse{
		Xmlns: elbv2XMLNS,
		Result: createRuleResult{
			Rules: xmlRuleList{
				Members: []xmlRule{toXMLRule(rule)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-create-rule"},
	}, nil
}

func (h *Handler) handleDeleteRule(vals url.Values) (any, error) {
	ruleArn := vals.Get("RuleArn")
	if ruleArn == "" {
		return nil, fmt.Errorf("%w: RuleArn is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteRule(ruleArn); err != nil {
		return nil, err
	}

	return &deleteRuleResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-rule"},
	}, nil
}

func (h *Handler) handleDescribeRules(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	ruleArns := parseMembers(vals, "RuleArns.member")

	rules, err := h.Backend.DescribeRules(listenerArn, ruleArns)
	if err != nil {
		return nil, err
	}

	members := make([]xmlRule, 0, len(rules))
	for i := range rules {
		members = append(members, toXMLRule(&rules[i]))
	}

	return &describeRulesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeRulesResult{
			Rules: xmlRuleList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-rules"},
	}, nil
}

func (h *Handler) handleModifyRule(vals url.Values) (any, error) {
	ruleArn := vals.Get("RuleArn")
	if ruleArn == "" {
		return nil, fmt.Errorf("%w: RuleArn is required", ErrInvalidParameter)
	}

	actions := parseActions(vals, "Actions.member")
	conditions := parseConditions(vals, "Conditions.member")

	rule, err := h.Backend.ModifyRule(ruleArn, actions, conditions)
	if err != nil {
		return nil, err
	}

	return &modifyRuleResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyRuleResult{
			Rules: xmlRuleList{
				Members: []xmlRule{toXMLRule(rule)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-rule"},
	}, nil
}

// --- tag handlers ---

func (h *Handler) handleAddTags(vals url.Values) (any, error) {
	resourceArns := parseMembers(vals, "ResourceArns.member")
	if len(resourceArns) == 0 {
		return nil, fmt.Errorf("%w: at least one ResourceArn is required", ErrInvalidParameter)
	}

	kvs := parseTagKVs(vals)

	if err := h.Backend.AddTags(resourceArns, kvs); err != nil {
		return nil, err
	}

	return &addTagsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-addtags"},
	}, nil
}

func (h *Handler) handleRemoveTags(vals url.Values) (any, error) {
	resourceArns := parseMembers(vals, "ResourceArns.member")
	if len(resourceArns) == 0 {
		return nil, fmt.Errorf("%w: at least one ResourceArn is required", ErrInvalidParameter)
	}

	keys := parseTagKeys(vals, "TagKeys.member")

	if err := h.Backend.RemoveTags(resourceArns, keys); err != nil {
		return nil, err
	}

	return &removeTagsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-removetags"},
	}, nil
}

func (h *Handler) handleDescribeTags(vals url.Values) (any, error) {
	resourceArns := parseMembers(vals, "ResourceArns.member")
	if len(resourceArns) == 0 {
		return nil, fmt.Errorf("%w: at least one ResourceArn is required", ErrInvalidParameter)
	}

	tagMap, err := h.Backend.DescribeTags(resourceArns)
	if err != nil {
		return nil, err
	}

	tagDescs := make([]xmlTagDescription, 0, len(resourceArns))
	for _, resArn := range resourceArns {
		kvs := tagMap[resArn]
		xmlKVs := make([]xmlTag, 0, len(kvs))

		for _, kv := range kvs {
			xmlKVs = append(xmlKVs, xmlTag{Key: kv.Key, Value: kv.Value})
		}

		tagDescs = append(tagDescs, xmlTagDescription{
			ResourceArn: resArn,
			Tags:        xmlTagList{Members: xmlKVs},
		})
	}

	return &describeTagsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTagsResult{
			TagDescriptions: xmlTagDescriptionList{Members: tagDescs},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describetags"},
	}, nil
}

// --- trust store handlers ---

func (h *Handler) handleCreateTrustStore(vals url.Values) (any, error) {
	name := vals.Get("Name")
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	kvs := parseTagKVs(vals)

	ts, err := h.Backend.CreateTrustStore(name, kvs)
	if err != nil {
		return nil, err
	}

	return &createTrustStoreResponse{
		Xmlns: elbv2XMLNS,
		Result: createTrustStoreResult{
			TrustStores: xmlTrustStoreList{
				Members: []xmlTrustStore{toXMLTrustStore(ts)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-create-ts-" + name},
	}, nil
}

func (h *Handler) handleDeleteTrustStore(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteTrustStore(tsArn); err != nil {
		return nil, err
	}

	return &deleteTrustStoreResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-ts"},
	}, nil
}

func (h *Handler) handleDeleteSharedTrustStoreAssociation(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	// Verify the trust store exists.
	stores, err := h.Backend.DescribeTrustStores([]string{tsArn}, nil)
	if err != nil {
		return nil, err
	}

	if len(stores) == 0 {
		return nil, ErrTrustStoreNotFound
	}

	return &deleteSharedTrustStoreAssociationResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-ts-assoc"},
	}, nil
}

func (h *Handler) handleAddTrustStoreRevocations(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	revocations := parseMembers(vals, "RevocationContents.member")

	if err := h.Backend.AddTrustStoreRevocations(tsArn, revocations); err != nil {
		return nil, err
	}

	return &addTrustStoreRevocationsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-add-ts-revocations"},
	}, nil
}

func (h *Handler) handleDescribeTrustStoreAssociations(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	assocs, err := h.Backend.DescribeTrustStoreAssociations(tsArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTrustStoreAssociation, 0, len(assocs))
	for _, resArn := range assocs {
		members = append(members, xmlTrustStoreAssociation{ResourceArn: resArn})
	}

	return &describeTrustStoreAssociationsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTrustStoreAssociationsResult{
			TrustStoreAssociations: xmlTrustStoreAssociationList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-ts-assocs"},
	}, nil
}

// --- listener certificate handlers ---

// parseCertArns extracts certificate ARNs from indexed form parameters.
func parseCertArns(vals url.Values) []string {
	arns := make([]string, 0)
	for i := 1; ; i++ {
		c := vals.Get(fmt.Sprintf("Certificates.member.%d.CertificateArn", i))
		if c == "" {
			break
		}

		arns = append(arns, c)
	}

	return arns
}

func (h *Handler) handleAddListenerCertificates(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	certArns := parseCertArns(vals)
	if len(certArns) == 0 {
		return nil, fmt.Errorf("%w: at least one certificate ARN is required", ErrInvalidParameter)
	}

	if err := h.Backend.AddListenerCertificates(listenerArn, certArns); err != nil {
		return nil, err
	}

	members := make([]xmlListenerCertificate, 0, len(certArns))
	for _, c := range certArns {
		members = append(members, xmlListenerCertificate{CertificateArn: c})
	}

	return &addListenerCertificatesResponse{
		Xmlns: elbv2XMLNS,
		Result: addListenerCertificatesResult{
			Certificates: xmlListenerCertificateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-add-listener-certs"},
	}, nil
}

func (h *Handler) handleDescribeListenerCertificates(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	certs, err := h.Backend.DescribeListenerCertificates(listenerArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlListenerCertificate, 0, len(certs))
	for _, c := range certs {
		members = append(members, xmlListenerCertificate{CertificateArn: c})
	}

	return &describeListenerCertificatesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeListenerCertificatesResult{
			Certificates: xmlListenerCertificateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-listener-certs"},
	}, nil
}

func (h *Handler) handleRemoveListenerCertificates(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	certArns := parseCertArns(vals)
	if len(certArns) == 0 {
		return nil, fmt.Errorf("%w: at least one certificate ARN is required", ErrInvalidParameter)
	}

	if err := h.Backend.RemoveListenerCertificates(listenerArn, certArns); err != nil {
		return nil, err
	}

	return &removeListenerCertificatesResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-remove-listener-certs"},
	}, nil
}

// --- misc/stub handlers ---

func (h *Handler) handleDescribeAccountLimits(_ url.Values) (any, error) {
	limits := []xmlAccountLimit{
		{Name: "target-groups", Max: "3000"},
		{Name: "targets-per-target-group", Max: "1000"},
		{Name: "load-balancers", Max: "50"},
		{Name: "listeners-per-load-balancer", Max: "50"},
		{Name: "rules-per-load-balancer", Max: "200"},
		{Name: "certificates-per-listener", Max: "25"},
	}

	return &describeAccountLimitsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeAccountLimitsResult{
			Limits: xmlAccountLimitList{Members: limits},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-account-limits"},
	}, nil
}

func (h *Handler) handleDescribeCapacityReservation(vals url.Values) (any, error) {
	const defaultDecreaseRequestsRemaining = 5

	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	lbs, err := h.Backend.DescribeLoadBalancers([]string{lbArn}, nil)
	if err != nil {
		return nil, err
	}

	if len(lbs) == 0 {
		return nil, ErrLoadBalancerNotFound
	}

	return &describeCapacityReservationResponse{
		Xmlns: elbv2XMLNS,
		Result: describeCapacityReservationResult{
			LastModifiedTime:          "",
			DecreaseRequestsRemaining: defaultDecreaseRequestsRemaining,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-capacity-reservation"},
	}, nil
}

func (h *Handler) handleDescribeSSLPolicies(_ url.Values) (any, error) {
	const (
		priority1 = 1
		priority2 = 2
		priority3 = 3
		priority4 = 4
		priority5 = 5
		priority6 = 6
		priority7 = 7
		priority8 = 8
	)

	policies := []xmlSSLPolicy{
		{
			Name: "ELBSecurityPolicy-2016-08",
			Ciphers: xmlCipherList{Members: []xmlCipher{
				{Name: "ECDHE-ECDSA-AES128-GCM-SHA256", Priority: priority1},
				{Name: "ECDHE-RSA-AES128-GCM-SHA256", Priority: priority2},
				{Name: "ECDHE-ECDSA-AES128-SHA256", Priority: priority3},
				{Name: "ECDHE-RSA-AES128-SHA256", Priority: priority4},
				{Name: "ECDHE-ECDSA-AES256-GCM-SHA384", Priority: priority5},
				{Name: "ECDHE-RSA-AES256-GCM-SHA384", Priority: priority6},
				{Name: "ECDHE-ECDSA-AES256-SHA384", Priority: priority7},
				{Name: "ECDHE-RSA-AES256-SHA384", Priority: priority8},
			}},
			SslProtocols: xmlSSLProtocolList{Members: []xmlSSLProtocol{
				{Value: "TLSv1.2"},
			}},
		},
		{
			Name: "ELBSecurityPolicy-TLS13-1-2-2021-06",
			Ciphers: xmlCipherList{Members: []xmlCipher{
				{Name: "TLS_AES_128_GCM_SHA256", Priority: priority1},
				{Name: "TLS_AES_256_GCM_SHA384", Priority: priority2},
				{Name: "TLS_CHACHA20_POLY1305_SHA256", Priority: priority3},
			}},
			SslProtocols: xmlSSLProtocolList{Members: []xmlSSLProtocol{
				{Value: "TLSv1.3"},
			}},
		},
	}

	return &describeSSLPoliciesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeSSLPoliciesResult{
			SslPolicies: xmlSSLPolicyList{Members: policies},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-ssl-policies"},
	}, nil
}

func (h *Handler) handleDescribeTrustStores(vals url.Values) (any, error) {
	arns := parseMembers(vals, "TrustStoreArns.member")
	names := parseMembers(vals, "Names.member")

	stores, err := h.Backend.DescribeTrustStores(arns, names)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTrustStore, 0, len(stores))
	for i := range stores {
		members = append(members, toXMLTrustStore(&stores[i]))
	}

	return &describeTrustStoresResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTrustStoresResult{
			TrustStores: xmlTrustStoreList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-ts"},
	}, nil
}

func (h *Handler) handleModifyTrustStore(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	ts, err := h.Backend.ModifyTrustStore(tsArn, vals.Get("Name"))
	if err != nil {
		return nil, err
	}

	return &modifyTrustStoreResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyTrustStoreResult{
			TrustStores: xmlTrustStoreList{
				Members: []xmlTrustStore{toXMLTrustStore(ts)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-ts"},
	}, nil
}

func (h *Handler) handleDescribeTrustStoreRevocations(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	revocations, err := h.Backend.DescribeTrustStoreRevocations(tsArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlRevocationContent, 0, len(revocations))
	for _, r := range revocations {
		members = append(members, xmlRevocationContent{RevocationID: r})
	}

	return &describeTrustStoreRevocationsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTrustStoreRevocationsResult{
			RevocationContents: xmlRevocationContentList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-ts-revocations"},
	}, nil
}

func (h *Handler) handleRemoveTrustStoreRevocations(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	revocationIDs := parseMembers(vals, "RevocationIds.member")
	if len(revocationIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one RevocationId is required", ErrInvalidParameter)
	}

	if err := h.Backend.RemoveTrustStoreRevocations(tsArn, revocationIDs); err != nil {
		return nil, err
	}

	return &removeTrustStoreRevocationsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-remove-ts-revocations"},
	}, nil
}

func (h *Handler) handleSetRulePriorities(vals url.Values) (any, error) {
	priorities := make([]RulePriority, 0)

	for i := 1; ; i++ {
		ruleArn := vals.Get(fmt.Sprintf("RulePriorities.member.%d.RuleArn", i))
		if ruleArn == "" {
			break
		}

		priorities = append(priorities, RulePriority{
			RuleArn:  ruleArn,
			Priority: vals.Get(fmt.Sprintf("RulePriorities.member.%d.Priority", i)),
		})
	}

	if len(priorities) == 0 {
		return nil, fmt.Errorf("%w: at least one RulePriority is required", ErrInvalidParameter)
	}

	rules, err := h.Backend.SetRulePriorities(priorities)
	if err != nil {
		return nil, err
	}

	members := make([]xmlRule, 0, len(rules))
	for i := range rules {
		members = append(members, toXMLRule(&rules[i]))
	}

	return &setRulePrioritiesResponse{
		Xmlns: elbv2XMLNS,
		Result: setRulePrioritiesResult{
			Rules: xmlRuleList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-rule-priorities"},
	}, nil
}

// --- stub handlers for less-critical operations ---

func (h *Handler) handleGetResourcePolicy(vals url.Values) (any, error) {
	resourceArn := vals.Get("ResourceArn")
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	return &getResourcePolicyResponse{
		Xmlns:            elbv2XMLNS,
		Result:           getResourcePolicyResult{Policy: ""},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-get-resource-policy"},
	}, nil
}

func (h *Handler) handleGetTrustStoreCaCertificatesBundle(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	stores, err := h.Backend.DescribeTrustStores([]string{tsArn}, nil)
	if err != nil {
		return nil, err
	}

	if len(stores) == 0 {
		return nil, ErrTrustStoreNotFound
	}

	return &getTrustStoreCaCertificatesBundleResponse{
		Xmlns:            elbv2XMLNS,
		Result:           getTrustStoreCaCertificatesBundleResult{Location: ""},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-get-ts-ca-bundle"},
	}, nil
}

func (h *Handler) handleGetTrustStoreRevocationContent(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	stores, err := h.Backend.DescribeTrustStores([]string{tsArn}, nil)
	if err != nil {
		return nil, err
	}

	if len(stores) == 0 {
		return nil, ErrTrustStoreNotFound
	}

	return &getTrustStoreRevocationContentResponse{
		Xmlns:            elbv2XMLNS,
		Result:           getTrustStoreRevocationContentResult{Location: ""},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-get-ts-revocation-content"},
	}, nil
}

func (h *Handler) handleModifyCapacityReservation(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	lbs, err := h.Backend.DescribeLoadBalancers([]string{lbArn}, nil)
	if err != nil {
		return nil, err
	}

	if len(lbs) == 0 {
		return nil, ErrLoadBalancerNotFound
	}

	return &modifyCapacityReservationResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-capacity-reservation"},
	}, nil
}

func (h *Handler) handleModifyIPPools(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	lbs, err := h.Backend.DescribeLoadBalancers([]string{lbArn}, nil)
	if err != nil {
		return nil, err
	}

	if len(lbs) == 0 {
		return nil, ErrLoadBalancerNotFound
	}

	return &modifyIPPoolsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-ip-pools"},
	}, nil
}

// --- error handling ---

// handleOpError translates an operation error into an HTTP response.
func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	code, statusCode := elbv2ErrorCode(opErr)

	if code == "" {
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("elbv2 internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func elbv2ErrorCode(opErr error) (string, int) {
	type errorMapping struct {
		sentinel error
		code     string
		httpCode int
	}

	mappings := []errorMapping{
		{ErrLoadBalancerNotFound, "LoadBalancerNotFound", http.StatusNotFound},
		{ErrTargetGroupNotFound, "TargetGroupNotFound", http.StatusNotFound},
		{ErrListenerNotFound, "ListenerNotFound", http.StatusNotFound},
		{ErrRuleNotFound, "RuleNotFound", http.StatusNotFound},
		{ErrTrustStoreNotFound, "TrustStoreNotFound", http.StatusNotFound},
		{ErrLoadBalancerAlreadyExists, "DuplicateLoadBalancerName", http.StatusConflict},
		{ErrTargetGroupAlreadyExists, "DuplicateTargetGroupName", http.StatusConflict},
		{ErrTrustStoreAlreadyExists, "DuplicateTrustStoreName", http.StatusConflict},
		{ErrUnknownAction, "InvalidAction", http.StatusBadRequest},
		{awserr.ErrInvalidParameter, "ValidationError", http.StatusBadRequest},
	}

	for _, m := range mappings {
		if errors.Is(opErr, m.sentinel) {
			return m.code, m.httpCode
		}
	}

	return "", http.StatusInternalServerError
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &elbv2ErrorResponse{
		Xmlns:     elbv2XMLNS,
		Error:     elbv2Error{Code: code, Message: message, Type: "Sender"},
		RequestID: "elbv2-error",
	}

	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// --- helper functions ---

func parseInt32(s string) (int32, error) {
	if s == "" {
		return 0, nil
	}

	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, err
	}

	return int32(n), nil
}

// parseMembers extracts indexed form values (e.g. "Names.member.1").
func parseMembers(vals url.Values, prefix string) []string {
	result := make([]string, 0)

	for i := 1; ; i++ {
		key := fmt.Sprintf("%s.%d", prefix, i)
		v := vals.Get(key)

		if v == "" {
			break
		}

		result = append(result, v)
	}

	return result
}

// parseTagKVs extracts key-value tag pairs from Tags.member.N.Key/Value form values.
func parseTagKVs(vals url.Values) []tags.KV {
	const prefix = "Tags.member"

	result := make([]tags.KV, 0)

	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("%s.%d.Key", prefix, i))
		if k == "" {
			break
		}

		result = append(result, tags.KV{Key: k, Value: vals.Get(fmt.Sprintf("%s.%d.Value", prefix, i))})
	}

	return result
}

// parseTagKeys extracts tag keys from TagKeys.member.N form values (for RemoveTags).
func parseTagKeys(vals url.Values, prefix string) []string {
	result := make([]string, 0)

	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("%s.%d", prefix, i))
		if k == "" {
			break
		}

		result = append(result, k)
	}

	return result
}

// parseTargets extracts target descriptions from Targets.member.N.Id/Port form values.
func parseTargets(vals url.Values, prefix string) []Target {
	result := make([]Target, 0)

	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("%s.%d.Id", prefix, i))
		if id == "" {
			break
		}

		port, _ := parseInt32(vals.Get(fmt.Sprintf("%s.%d.Port", prefix, i)))

		result = append(result, Target{ID: id, Port: port})
	}

	return result
}

// parseActions extracts action definitions from form values.
func parseActions(vals url.Values, prefix string) []Action {
	result := make([]Action, 0)

	for i := 1; ; i++ {
		actionType := vals.Get(fmt.Sprintf("%s.%d.Type", prefix, i))
		if actionType == "" {
			break
		}

		result = append(result, Action{
			Type:           actionType,
			TargetGroupArn: vals.Get(fmt.Sprintf("%s.%d.TargetGroupArn", prefix, i)),
		})
	}

	return result
}

// parseConditions extracts rule conditions from form values.
// Supported fields: host-header, path-pattern, http-header, http-request-method,
// query-string, source-ip.
func parseConditions(vals url.Values, prefix string) []Condition {
	result := make([]Condition, 0)

	for i := 1; parseConditionAt(vals, prefix, i, &result); i++ {
	}

	return result
}

// parseConditionAt parses a single indexed condition and appends it to result.
// Returns false when there are no more conditions to parse.
func parseConditionAt(vals url.Values, prefix string, i int, result *[]Condition) bool {
	field := vals.Get(fmt.Sprintf("%s.%d.Field", prefix, i))
	if field == "" {
		return false
	}

	cond := Condition{Field: field}

	switch field {
	case "host-header":
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.HostHeaderConfig.Values.member", prefix, i))
	case "path-pattern":
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.PathPatternConfig.Values.member", prefix, i))
	case "http-request-method":
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.HttpRequestMethodConfig.Values.member", prefix, i))
	case "source-ip":
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.SourceIpConfig.Values.member", prefix, i))
	case "http-header":
		cond.HttpHeaderName = vals.Get(fmt.Sprintf("%s.%d.HttpHeaderConfig.HttpHeaderName", prefix, i))
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.HttpHeaderConfig.Values.member", prefix, i))
	case "query-string":
		cond.QueryStringPairs = parseQueryStringPairs(vals, prefix, i)
	}

	*result = append(*result, cond)

	return true
}

// parseQueryStringPairs extracts query-string key/value pairs for the Nth condition.
func parseQueryStringPairs(vals url.Values, prefix string, condIdx int) []QueryStringPair {
	pairs := make([]QueryStringPair, 0)

	for j := 1; parseQueryStringPairAt(vals, prefix, condIdx, j, &pairs); j++ {
	}

	return pairs
}

// parseQueryStringPairAt parses a single query-string pair.
// Returns false when there are no more pairs to parse.
func parseQueryStringPairAt(vals url.Values, prefix string, condIdx, pairIdx int, pairs *[]QueryStringPair) bool {
	v := vals.Get(fmt.Sprintf("%s.%d.QueryStringConfig.Values.member.%d.Value", prefix, condIdx, pairIdx))
	if v == "" {
		return false
	}

	*pairs = append(*pairs, QueryStringPair{
		Key:   vals.Get(fmt.Sprintf("%s.%d.QueryStringConfig.Values.member.%d.Key", prefix, condIdx, pairIdx)),
		Value: v,
	})

	return true
}

// --- XML conversion helpers ---

func toXMLLoadBalancer(lb *LoadBalancer) xmlLoadBalancer {
	azs := make([]xmlAZMapping, 0, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		azs = append(azs, xmlAZMapping{ZoneName: az, SubnetID: ""})
	}

	sgs := make([]xmlStringValue, 0, len(lb.SecurityGroups))
	for _, sg := range lb.SecurityGroups {
		sgs = append(sgs, xmlStringValue{Value: sg})
	}

	return xmlLoadBalancer{
		LoadBalancerArn:       lb.LoadBalancerArn,
		LoadBalancerName:      lb.LoadBalancerName,
		DNSName:               lb.DNSName,
		CanonicalHostedZoneID: lb.CanonicalHostedZoneID,
		CreatedTime:           lb.CreatedTime.UTC().Format("2006-01-02T15:04:05Z"),
		Scheme:                lb.Scheme,
		Type:                  lb.Type,
		IPAddressType:         lb.IPAddressType,
		VpcID:                 lb.VpcID,
		State:                 xmlLoadBalancerState{Code: lb.State.Code, Reason: lb.State.Description},
		AvailabilityZones:     xmlAZMappingList{Members: azs},
		SecurityGroups:        xmlStringList{Members: sgs},
	}
}

func toXMLTargetGroup(tg *TargetGroup) xmlTargetGroup {
	return xmlTargetGroup{
		TargetGroupArn:      tg.TargetGroupArn,
		TargetGroupName:     tg.TargetGroupName,
		Protocol:            tg.Protocol,
		Port:                tg.Port,
		VpcID:               tg.VpcID,
		TargetType:          tg.TargetType,
		HealthCheckProtocol: tg.HealthCheckProtocol,
		HealthCheckPort:     tg.HealthCheckPort,
		HealthCheckPath:     tg.HealthCheckPath,
		HealthCheckEnabled:  tg.HealthCheckEnabled,
	}
}

func toXMLListener(l *Listener) xmlListener {
	actions := make([]xmlAction, 0, len(l.DefaultActions))
	for _, a := range l.DefaultActions {
		actions = append(actions, xmlAction(a))
	}

	return xmlListener{
		ListenerArn:     l.ListenerArn,
		LoadBalancerArn: l.LoadBalancerArn,
		Protocol:        l.Protocol,
		Port:            l.Port,
		DefaultActions:  xmlActionList{Members: actions},
	}
}

func toXMLRule(r *Rule) xmlRule {
	actions := make([]xmlAction, 0, len(r.Actions))
	for _, a := range r.Actions {
		actions = append(actions, xmlAction(a))
	}

	conds := make([]xmlCondition, 0, len(r.Conditions))
	for _, c := range r.Conditions {
		xc := xmlCondition{Field: c.Field}

		switch c.Field {
		case "host-header":
			vals := make([]xmlStringValue, 0, len(c.Values))
			for _, v := range c.Values {
				vals = append(vals, xmlStringValue{Value: v})
			}
			xc.HostHeaderConfig = &xmlConditionValuesConfig{Values: xmlStringList{Members: vals}}
		case "path-pattern":
			vals := make([]xmlStringValue, 0, len(c.Values))
			for _, v := range c.Values {
				vals = append(vals, xmlStringValue{Value: v})
			}
			xc.PathPatternConfig = &xmlConditionValuesConfig{Values: xmlStringList{Members: vals}}
		case "http-request-method":
			vals := make([]xmlStringValue, 0, len(c.Values))
			for _, v := range c.Values {
				vals = append(vals, xmlStringValue{Value: v})
			}
			xc.HttpRequestMethodConfig = &xmlConditionValuesConfig{Values: xmlStringList{Members: vals}}
		case "source-ip":
			vals := make([]xmlStringValue, 0, len(c.Values))
			for _, v := range c.Values {
				vals = append(vals, xmlStringValue{Value: v})
			}
			xc.SourceIpConfig = &xmlConditionValuesConfig{Values: xmlStringList{Members: vals}}
		case "http-header":
			vals := make([]xmlStringValue, 0, len(c.Values))
			for _, v := range c.Values {
				vals = append(vals, xmlStringValue{Value: v})
			}
			xc.HttpHeaderConfig = &xmlHttpHeaderConfig{
				HttpHeaderName: c.HttpHeaderName,
				Values:         xmlStringList{Members: vals},
			}
		case "query-string":
			pairs := make([]xmlQueryStringKeyValue, 0, len(c.QueryStringPairs))
			for _, p := range c.QueryStringPairs {
				pairs = append(pairs, xmlQueryStringKeyValue{Key: p.Key, Value: p.Value})
			}
			xc.QueryStringConfig = &xmlQueryStringConfig{Values: xmlQueryStringList{Members: pairs}}
		}

		conds = append(conds, xc)
	}

	return xmlRule{
		RuleArn:    r.RuleArn,
		Priority:   r.Priority,
		IsDefault:  r.IsDefault,
		Actions:    xmlActionList{Members: actions},
		Conditions: xmlConditionList{Members: conds},
	}
}

// --- XML types ---

type elbv2Error struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type elbv2ErrorResponse struct {
	XMLName   xml.Name   `xml:"ErrorResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	Error     elbv2Error `xml:"Error"`
	RequestID string     `xml:"RequestId"`
}

type xmlResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

type xmlStringValue struct {
	Value string `xml:",chardata"`
}

type xmlStringList struct {
	Members []xmlStringValue `xml:"member"`
}

type xmlTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlTagList struct {
	Members []xmlTag `xml:"member"`
}

type xmlTagDescription struct {
	ResourceArn string     `xml:"ResourceArn"`
	Tags        xmlTagList `xml:"Tags"`
}

type xmlTagDescriptionList struct {
	Members []xmlTagDescription `xml:"member"`
}

type xmlLoadBalancerState struct {
	Code   string `xml:"Code"`
	Reason string `xml:"Reason,omitempty"`
}

type xmlAZMapping struct {
	ZoneName string `xml:"ZoneName"`
	SubnetID string `xml:"SubnetId,omitempty"`
}

type xmlAZMappingList struct {
	Members []xmlAZMapping `xml:"member"`
}

type xmlLoadBalancer struct {
	LoadBalancerArn       string               `xml:"LoadBalancerArn"`
	LoadBalancerName      string               `xml:"LoadBalancerName"`
	DNSName               string               `xml:"DNSName"`
	CanonicalHostedZoneID string               `xml:"CanonicalHostedZoneId"`
	CreatedTime           string               `xml:"CreatedTime"`
	Scheme                string               `xml:"Scheme"`
	Type                  string               `xml:"Type"`
	IPAddressType         string               `xml:"IpAddressType"`
	VpcID                 string               `xml:"VpcId"`
	State                 xmlLoadBalancerState `xml:"State"`
	AvailabilityZones     xmlAZMappingList     `xml:"AvailabilityZones"`
	SecurityGroups        xmlStringList        `xml:"SecurityGroups"`
}

type xmlLoadBalancerList struct {
	Members []xmlLoadBalancer `xml:"member"`
}

type createLoadBalancerResult struct {
	LoadBalancers xmlLoadBalancerList `xml:"LoadBalancers"`
}

type createLoadBalancerResponse struct {
	XMLName          xml.Name                 `xml:"CreateLoadBalancerResponse"`
	Xmlns            string                   `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata      `xml:"ResponseMetadata"`
	Result           createLoadBalancerResult `xml:"CreateLoadBalancerResult"`
}

type deleteLoadBalancerResponse struct {
	XMLName          xml.Name            `xml:"DeleteLoadBalancerResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeLoadBalancersResult struct {
	NextMarker    string              `xml:"NextMarker,omitempty"`
	LoadBalancers xmlLoadBalancerList `xml:"LoadBalancers"`
}

type describeLoadBalancersResponse struct {
	XMLName          xml.Name                    `xml:"DescribeLoadBalancersResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeLoadBalancersResult `xml:"DescribeLoadBalancersResult"`
}

type xmlLBAttribute struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlLBAttributeList struct {
	Members []xmlLBAttribute `xml:"member"`
}

type modifyLoadBalancerAttributesResult struct {
	Attributes xmlLBAttributeList `xml:"Attributes"`
}

type modifyLoadBalancerAttributesResponse struct {
	XMLName          xml.Name                           `xml:"ModifyLoadBalancerAttributesResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           modifyLoadBalancerAttributesResult `xml:"ModifyLoadBalancerAttributesResult"`
}

type describeLoadBalancerAttributesResult struct {
	Attributes xmlLBAttributeList `xml:"Attributes"`
}

type describeLoadBalancerAttributesResponse struct {
	XMLName          xml.Name                             `xml:"DescribeLoadBalancerAttributesResponse"`
	Xmlns            string                               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                  `xml:"ResponseMetadata"`
	Result           describeLoadBalancerAttributesResult `xml:"DescribeLoadBalancerAttributesResult"`
}

type setSecurityGroupsResult struct {
	SecurityGroupIDs xmlStringList `xml:"SecurityGroupIds"`
}

type setSecurityGroupsResponse struct {
	XMLName          xml.Name                `xml:"SetSecurityGroupsResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           setSecurityGroupsResult `xml:"SetSecurityGroupsResult"`
}

type setSubnetsResult struct {
	AvailabilityZones xmlStringList `xml:"AvailabilityZones"`
}

type setSubnetsResponse struct {
	XMLName          xml.Name            `xml:"SetSubnetsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           setSubnetsResult    `xml:"SetSubnetsResult"`
}

type setIPAddressTypeResult struct {
	IPAddressType string `xml:"IpAddressType"`
}

type setIPAddressTypeResponse struct {
	XMLName          xml.Name               `xml:"SetIpAddressTypeResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	Result           setIPAddressTypeResult `xml:"SetIpAddressTypeResult"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
}

// --- target group XML types ---

type xmlTargetGroup struct {
	TargetGroupArn      string `xml:"TargetGroupArn"`
	TargetGroupName     string `xml:"TargetGroupName"`
	Protocol            string `xml:"Protocol"`
	VpcID               string `xml:"VpcId,omitempty"`
	TargetType          string `xml:"TargetType"`
	HealthCheckProtocol string `xml:"HealthCheckProtocol"`
	HealthCheckPort     string `xml:"HealthCheckPort"`
	HealthCheckPath     string `xml:"HealthCheckPath,omitempty"`
	Port                int32  `xml:"Port,omitempty"`
	HealthCheckEnabled  bool   `xml:"HealthCheckEnabled"`
}

type xmlTargetGroupList struct {
	Members []xmlTargetGroup `xml:"member"`
}

type createTargetGroupResult struct {
	TargetGroups xmlTargetGroupList `xml:"TargetGroups"`
}

type createTargetGroupResponse struct {
	XMLName          xml.Name                `xml:"CreateTargetGroupResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           createTargetGroupResult `xml:"CreateTargetGroupResult"`
}

type deleteTargetGroupResponse struct {
	XMLName          xml.Name            `xml:"DeleteTargetGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeTargetGroupsResult struct {
	NextMarker   string             `xml:"NextMarker,omitempty"`
	TargetGroups xmlTargetGroupList `xml:"TargetGroups"`
}

type describeTargetGroupsResponse struct {
	XMLName          xml.Name                   `xml:"DescribeTargetGroupsResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata        `xml:"ResponseMetadata"`
	Result           describeTargetGroupsResult `xml:"DescribeTargetGroupsResult"`
}

type modifyTargetGroupResult struct {
	TargetGroups xmlTargetGroupList `xml:"TargetGroups"`
}

type modifyTargetGroupResponse struct {
	XMLName          xml.Name                `xml:"ModifyTargetGroupResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           modifyTargetGroupResult `xml:"ModifyTargetGroupResult"`
}

// --- target health XML types ---

type xmlTargetDescription struct {
	ID   string `xml:"Id"`
	Port int32  `xml:"Port,omitempty"`
}

type xmlTargetHealth struct {
	State       string `xml:"State"`
	Reason      string `xml:"Reason,omitempty"`
	Description string `xml:"Description,omitempty"`
}

type xmlTargetHealthDescription struct {
	TargetHealth xmlTargetHealth      `xml:"TargetHealth"`
	Target       xmlTargetDescription `xml:"Target"`
}

type xmlTargetHealthDescriptionList struct {
	Members []xmlTargetHealthDescription `xml:"member"`
}

type registerTargetsResponse struct {
	XMLName          xml.Name            `xml:"RegisterTargetsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deregisterTargetsResponse struct {
	XMLName          xml.Name            `xml:"DeregisterTargetsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeTargetHealthResult struct {
	TargetHealthDescriptions xmlTargetHealthDescriptionList `xml:"TargetHealthDescriptions"`
}

type describeTargetHealthResponse struct {
	XMLName          xml.Name                   `xml:"DescribeTargetHealthResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata        `xml:"ResponseMetadata"`
	Result           describeTargetHealthResult `xml:"DescribeTargetHealthResult"`
}

// --- listener XML types ---

type xmlAction struct {
	Type           string `xml:"Type"`
	TargetGroupArn string `xml:"TargetGroupArn,omitempty"`
}

type xmlActionList struct {
	Members []xmlAction `xml:"member"`
}

type xmlListener struct {
	ListenerArn     string        `xml:"ListenerArn"`
	LoadBalancerArn string        `xml:"LoadBalancerArn"`
	Protocol        string        `xml:"Protocol"`
	DefaultActions  xmlActionList `xml:"DefaultActions"`
	Port            int32         `xml:"Port"`
}

type xmlListenerList struct {
	Members []xmlListener `xml:"member"`
}

type createListenerResult struct {
	Listeners xmlListenerList `xml:"Listeners"`
}

type createListenerResponse struct {
	XMLName          xml.Name             `xml:"CreateListenerResponse"`
	Xmlns            string               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata  `xml:"ResponseMetadata"`
	Result           createListenerResult `xml:"CreateListenerResult"`
}

type deleteListenerResponse struct {
	XMLName          xml.Name            `xml:"DeleteListenerResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeListenersResult struct {
	NextMarker string          `xml:"NextMarker,omitempty"`
	Listeners  xmlListenerList `xml:"Listeners"`
}

type describeListenersResponse struct {
	XMLName          xml.Name                `xml:"DescribeListenersResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           describeListenersResult `xml:"DescribeListenersResult"`
}

type modifyListenerResult struct {
	Listeners xmlListenerList `xml:"Listeners"`
}

type modifyListenerResponse struct {
	XMLName          xml.Name             `xml:"ModifyListenerResponse"`
	Xmlns            string               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata  `xml:"ResponseMetadata"`
	Result           modifyListenerResult `xml:"ModifyListenerResult"`
}

// --- rule XML types ---

type xmlConditionValuesConfig struct {
	Values xmlStringList `xml:"Values"`
}

type xmlHttpHeaderConfig struct {
	HttpHeaderName string        `xml:"HttpHeaderName"`
	Values         xmlStringList `xml:"Values"`
}

type xmlQueryStringKeyValue struct {
	Key   string `xml:"Key,omitempty"`
	Value string `xml:"Value"`
}

type xmlQueryStringList struct {
	Members []xmlQueryStringKeyValue `xml:"member"`
}

type xmlQueryStringConfig struct {
	Values xmlQueryStringList `xml:"Values"`
}

type xmlCondition struct {
	Field                   string                    `xml:"Field"`
	HostHeaderConfig        *xmlConditionValuesConfig `xml:"HostHeaderConfig,omitempty"`
	PathPatternConfig       *xmlConditionValuesConfig `xml:"PathPatternConfig,omitempty"`
	HttpHeaderConfig        *xmlHttpHeaderConfig      `xml:"HttpHeaderConfig,omitempty"`
	HttpRequestMethodConfig *xmlConditionValuesConfig `xml:"HttpRequestMethodConfig,omitempty"`
	QueryStringConfig       *xmlQueryStringConfig     `xml:"QueryStringConfig,omitempty"`
	SourceIpConfig          *xmlConditionValuesConfig `xml:"SourceIpConfig,omitempty"`
}

type xmlConditionList struct {
	Members []xmlCondition `xml:"member"`
}

type xmlRule struct {
	RuleArn    string           `xml:"RuleArn"`
	Priority   string           `xml:"Priority"`
	Actions    xmlActionList    `xml:"Actions"`
	Conditions xmlConditionList `xml:"Conditions"`
	IsDefault  bool             `xml:"IsDefault"`
}

type xmlRuleList struct {
	Members []xmlRule `xml:"member"`
}

type createRuleResult struct {
	Rules xmlRuleList `xml:"Rules"`
}

type createRuleResponse struct {
	XMLName          xml.Name            `xml:"CreateRuleResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           createRuleResult    `xml:"CreateRuleResult"`
}

type deleteRuleResponse struct {
	XMLName          xml.Name            `xml:"DeleteRuleResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeRulesResult struct {
	NextMarker string      `xml:"NextMarker,omitempty"`
	Rules      xmlRuleList `xml:"Rules"`
}

type describeRulesResponse struct {
	XMLName          xml.Name            `xml:"DescribeRulesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           describeRulesResult `xml:"DescribeRulesResult"`
}

type modifyRuleResult struct {
	Rules xmlRuleList `xml:"Rules"`
}

type modifyRuleResponse struct {
	XMLName          xml.Name            `xml:"ModifyRuleResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           modifyRuleResult    `xml:"ModifyRuleResult"`
}

// --- tag XML types ---

type addTagsResponse struct {
	XMLName          xml.Name            `xml:"AddTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type removeTagsResponse struct {
	XMLName          xml.Name            `xml:"RemoveTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeTagsResult struct {
	TagDescriptions xmlTagDescriptionList `xml:"TagDescriptions"`
}

type describeTagsResponse struct {
	XMLName          xml.Name            `xml:"DescribeTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           describeTagsResult  `xml:"DescribeTagsResult"`
}

// --- target group attributes XML types ---

type xmlTGAttribute struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlTGAttributeList struct {
	Members []xmlTGAttribute `xml:"member"`
}

type modifyTargetGroupAttributesResult struct {
	Attributes xmlTGAttributeList `xml:"Attributes"`
}

type modifyTargetGroupAttributesResponse struct {
	XMLName          xml.Name                          `xml:"ModifyTargetGroupAttributesResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
	Result           modifyTargetGroupAttributesResult `xml:"ModifyTargetGroupAttributesResult"`
}

type describeTargetGroupAttributesResult struct {
	Attributes xmlTGAttributeList `xml:"Attributes"`
}

type describeTargetGroupAttributesResponse struct {
	XMLName          xml.Name                            `xml:"DescribeTargetGroupAttributesResponse"`
	Xmlns            string                              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                 `xml:"ResponseMetadata"`
	Result           describeTargetGroupAttributesResult `xml:"DescribeTargetGroupAttributesResult"`
}

// --- listener attributes XML types ---

type xmlListenerAttribute struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlListenerAttributeList struct {
	Members []xmlListenerAttribute `xml:"member"`
}

type modifyListenerAttributesResult struct {
	Attributes xmlListenerAttributeList `xml:"Attributes"`
}

type modifyListenerAttributesResponse struct {
	XMLName          xml.Name                       `xml:"ModifyListenerAttributesResponse"`
	Xmlns            string                         `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata            `xml:"ResponseMetadata"`
	Result           modifyListenerAttributesResult `xml:"ModifyListenerAttributesResult"`
}

type describeListenerAttributesResult struct {
	Attributes xmlListenerAttributeList `xml:"Attributes"`
}

type describeListenerAttributesResponse struct {
	XMLName          xml.Name                         `xml:"DescribeListenerAttributesResponse"`
	Xmlns            string                           `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata              `xml:"ResponseMetadata"`
	Result           describeListenerAttributesResult `xml:"DescribeListenerAttributesResult"`
}

// --- trust store XML types ---

type xmlTrustStore struct {
	TrustStoreArn       string `xml:"TrustStoreArn"`
	Name                string `xml:"Name"`
	Status              string `xml:"Status"`
	NumberOfCaCerts     int    `xml:"NumberOfCaCerts"`
	TotalRevokedEntries int64  `xml:"TotalRevokedEntries"`
}

type xmlTrustStoreList struct {
	Members []xmlTrustStore `xml:"member"`
}

type createTrustStoreResult struct {
	TrustStores xmlTrustStoreList `xml:"TrustStores"`
}

type createTrustStoreResponse struct {
	XMLName          xml.Name               `xml:"CreateTrustStoreResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           createTrustStoreResult `xml:"CreateTrustStoreResult"`
}

type deleteTrustStoreResponse struct {
	XMLName          xml.Name            `xml:"DeleteTrustStoreResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteSharedTrustStoreAssociationResponse struct {
	XMLName          xml.Name            `xml:"DeleteSharedTrustStoreAssociationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type addTrustStoreRevocationsResponse struct {
	XMLName          xml.Name            `xml:"AddTrustStoreRevocationsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlTrustStoreAssociation struct {
	ResourceArn string `xml:"ResourceArn"`
}

type xmlTrustStoreAssociationList struct {
	Members []xmlTrustStoreAssociation `xml:"member"`
}

type describeTrustStoreAssociationsResult struct {
	TrustStoreAssociations xmlTrustStoreAssociationList `xml:"TrustStoreAssociations"`
}

type describeTrustStoreAssociationsResponse struct {
	XMLName          xml.Name                             `xml:"DescribeTrustStoreAssociationsResponse"`
	Xmlns            string                               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                  `xml:"ResponseMetadata"`
	Result           describeTrustStoreAssociationsResult `xml:"DescribeTrustStoreAssociationsResult"`
}

// --- listener certificate XML types ---

type xmlListenerCertificate struct {
	CertificateArn string `xml:"CertificateArn"`
	IsDefault      bool   `xml:"IsDefault,omitempty"`
}

type xmlListenerCertificateList struct {
	Members []xmlListenerCertificate `xml:"member"`
}

type addListenerCertificatesResult struct {
	Certificates xmlListenerCertificateList `xml:"Certificates"`
}

type addListenerCertificatesResponse struct {
	XMLName          xml.Name                      `xml:"AddListenerCertificatesResponse"`
	Xmlns            string                        `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata           `xml:"ResponseMetadata"`
	Result           addListenerCertificatesResult `xml:"AddListenerCertificatesResult"`
}

type describeListenerCertificatesResult struct {
	NextMarker   string                     `xml:"NextMarker,omitempty"`
	Certificates xmlListenerCertificateList `xml:"Certificates"`
}

type describeListenerCertificatesResponse struct {
	XMLName          xml.Name                           `xml:"DescribeListenerCertificatesResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           describeListenerCertificatesResult `xml:"DescribeListenerCertificatesResult"`
}

// --- account limits XML types ---

type xmlAccountLimit struct {
	Name string `xml:"Name"`
	Max  string `xml:"Max"`
}

type xmlAccountLimitList struct {
	Members []xmlAccountLimit `xml:"member"`
}

type describeAccountLimitsResult struct {
	Limits xmlAccountLimitList `xml:"Limits"`
}

type describeAccountLimitsResponse struct {
	XMLName          xml.Name                    `xml:"DescribeAccountLimitsResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeAccountLimitsResult `xml:"DescribeAccountLimitsResult"`
}

// --- capacity reservation XML types ---

type describeCapacityReservationResult struct {
	LastModifiedTime          string `xml:"LastModifiedTime,omitempty"`
	DecreaseRequestsRemaining int    `xml:"DecreaseRequestsRemaining"`
}

type describeCapacityReservationResponse struct {
	XMLName          xml.Name                          `xml:"DescribeCapacityReservationResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
	Result           describeCapacityReservationResult `xml:"DescribeCapacityReservationResult"`
}

// --- SSL policy XML types ---

type xmlCipher struct {
	Name     string `xml:"Name"`
	Priority int    `xml:"Priority"`
}

type xmlCipherList struct {
	Members []xmlCipher `xml:"member"`
}

type xmlSSLProtocol struct {
	Value string `xml:",chardata"`
}

type xmlSSLProtocolList struct {
	Members []xmlSSLProtocol `xml:"member"`
}

type xmlSSLPolicy struct {
	Name         string             `xml:"Name"`
	Ciphers      xmlCipherList      `xml:"Ciphers"`
	SslProtocols xmlSSLProtocolList `xml:"SslProtocols"`
}

type xmlSSLPolicyList struct {
	Members []xmlSSLPolicy `xml:"member"`
}

type describeSSLPoliciesResult struct {
	SslPolicies xmlSSLPolicyList `xml:"SslPolicies"`
}

type describeSSLPoliciesResponse struct {
	XMLName          xml.Name                  `xml:"DescribeSSLPoliciesResponse"`
	Xmlns            string                    `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata       `xml:"ResponseMetadata"`
	Result           describeSSLPoliciesResult `xml:"DescribeSSLPoliciesResult"`
}

// --- XML conversion helpers for new types ---

func toXMLTrustStore(ts *TrustStore) xmlTrustStore {
	return xmlTrustStore{
		TrustStoreArn:       ts.TrustStoreArn,
		Name:                ts.Name,
		Status:              ts.Status,
		NumberOfCaCerts:     0,
		TotalRevokedEntries: int64(len(ts.Revocations)),
	}
}

// --- additional XML types for newly implemented operations ---

type describeTrustStoresResult struct {
	TrustStores xmlTrustStoreList `xml:"TrustStores"`
}

type describeTrustStoresResponse struct {
	XMLName          xml.Name                  `xml:"DescribeTrustStoresResponse"`
	Xmlns            string                    `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata       `xml:"ResponseMetadata"`
	Result           describeTrustStoresResult `xml:"DescribeTrustStoresResult"`
}

type modifyTrustStoreResult struct {
	TrustStores xmlTrustStoreList `xml:"TrustStores"`
}

type modifyTrustStoreResponse struct {
	XMLName          xml.Name               `xml:"ModifyTrustStoreResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           modifyTrustStoreResult `xml:"ModifyTrustStoreResult"`
}

type xmlRevocationContent struct {
	RevocationID string `xml:"RevocationId"`
}

type xmlRevocationContentList struct {
	Members []xmlRevocationContent `xml:"member"`
}

type describeTrustStoreRevocationsResult struct {
	RevocationContents xmlRevocationContentList `xml:"RevocationContents"`
}

type describeTrustStoreRevocationsResponse struct {
	XMLName          xml.Name                            `xml:"DescribeTrustStoreRevocationsResponse"`
	Xmlns            string                              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                 `xml:"ResponseMetadata"`
	Result           describeTrustStoreRevocationsResult `xml:"DescribeTrustStoreRevocationsResult"`
}

type removeTrustStoreRevocationsResponse struct {
	XMLName          xml.Name            `xml:"RemoveTrustStoreRevocationsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type removeListenerCertificatesResponse struct {
	XMLName          xml.Name            `xml:"RemoveListenerCertificatesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type setRulePrioritiesResult struct {
	Rules xmlRuleList `xml:"Rules"`
}

type setRulePrioritiesResponse struct {
	XMLName          xml.Name                `xml:"SetRulePrioritiesResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           setRulePrioritiesResult `xml:"SetRulePrioritiesResult"`
}

type getResourcePolicyResult struct {
	Policy string `xml:"Policy,omitempty"`
}

type getResourcePolicyResponse struct {
	XMLName          xml.Name                `xml:"GetResourcePolicyResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           getResourcePolicyResult `xml:"GetResourcePolicyResult"`
}

type getTrustStoreCaCertificatesBundleResult struct {
	Location string `xml:"Location,omitempty"`
}

type getTrustStoreCaCertificatesBundleResponse struct {
	XMLName          xml.Name                                `xml:"GetTrustStoreCaCertificatesBundleResponse"`
	Xmlns            string                                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                     `xml:"ResponseMetadata"`
	Result           getTrustStoreCaCertificatesBundleResult `xml:"GetTrustStoreCaCertificatesBundleResult"`
}

type getTrustStoreRevocationContentResult struct {
	Location string `xml:"Location,omitempty"`
}

type getTrustStoreRevocationContentResponse struct {
	XMLName          xml.Name                             `xml:"GetTrustStoreRevocationContentResponse"`
	Xmlns            string                               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                  `xml:"ResponseMetadata"`
	Result           getTrustStoreRevocationContentResult `xml:"GetTrustStoreRevocationContentResult"`
}

type modifyCapacityReservationResponse struct {
	XMLName          xml.Name            `xml:"ModifyCapacityReservationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type modifyIPPoolsResponse struct {
	XMLName          xml.Name            `xml:"ModifyIpPoolsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
