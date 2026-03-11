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
		"RegisterTargets",
		"DeregisterTargets",
		"DescribeTargetHealth",
		"CreateListener",
		"DeleteListener",
		"DescribeListeners",
		"ModifyListener",
		"CreateRule",
		"DeleteRule",
		"DescribeRules",
		"ModifyRule",
		"AddTags",
		"RemoveTags",
		"DescribeTags",
		"SetSecurityGroups",
		"SetSubnets",
		"SetIpAddressType",
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
		"CreateLoadBalancer":             h.handleCreateLoadBalancer,
		"DeleteLoadBalancer":             h.handleDeleteLoadBalancer,
		"DescribeLoadBalancers":          h.handleDescribeLoadBalancers,
		"ModifyLoadBalancerAttributes":   h.handleModifyLoadBalancerAttributes,
		"DescribeLoadBalancerAttributes": h.handleDescribeLoadBalancerAttributes,
		"SetSecurityGroups":              h.handleSetSecurityGroups,
		"SetSubnets":                     h.handleSetSubnets,
		"SetIpAddressType":               h.handleSetIPAddressType,
		"CreateTargetGroup":              h.handleCreateTargetGroup,
		"DeleteTargetGroup":              h.handleDeleteTargetGroup,
		"DescribeTargetGroups":           h.handleDescribeTargetGroups,
		"ModifyTargetGroup":              h.handleModifyTargetGroup,
		"RegisterTargets":                h.handleRegisterTargets,
		"DeregisterTargets":              h.handleDeregisterTargets,
		"DescribeTargetHealth":           h.handleDescribeTargetHealth,
		"CreateListener":                 h.handleCreateListener,
		"DeleteListener":                 h.handleDeleteListener,
		"DescribeListeners":              h.handleDescribeListeners,
		"ModifyListener":                 h.handleModifyListener,
		"CreateRule":                     h.handleCreateRule,
		"DeleteRule":                     h.handleDeleteRule,
		"DescribeRules":                  h.handleDescribeRules,
		"ModifyRule":                     h.handleModifyRule,
		"AddTags":                        h.handleAddTags,
		"RemoveTags":                     h.handleRemoveTags,
		"DescribeTags":                   h.handleDescribeTags,
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

	port, err := parseInt32(vals.Get("Port"))
	if err != nil {
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

	port, err := parseInt32(vals.Get("Port"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Port", ErrInvalidParameter)
	}

	actions := parseActions(vals, "DefaultActions.member")
	tagKVs := parseTagKVs(vals)

	listener, createErr := h.Backend.CreateListener(CreateListenerInput{
		LoadBalancerArn: lbArn,
		Protocol:        vals.Get("Protocol"),
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

// --- rule handlers ---

func (h *Handler) handleCreateRule(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	actions := parseActions(vals, "Actions.member")

	rule, err := h.Backend.CreateRule(CreateRuleInput{
		ListenerArn: listenerArn,
		Priority:    vals.Get("Priority"),
		Actions:     actions,
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

	rules, err := h.Backend.DescribeRules("", []string{ruleArn})
	if err != nil {
		return nil, err
	}

	if len(rules) == 0 {
		return nil, ErrRuleNotFound
	}

	return &modifyRuleResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyRuleResult{
			Rules: xmlRuleList{
				Members: []xmlRule{toXMLRule(&rules[0])},
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
		{ErrLoadBalancerAlreadyExists, "DuplicateLoadBalancerName", http.StatusConflict},
		{ErrTargetGroupAlreadyExists, "DuplicateTargetGroupName", http.StatusConflict},
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

	return xmlRule{
		RuleArn:   r.RuleArn,
		Priority:  r.Priority,
		IsDefault: r.IsDefault,
		Actions:   xmlActionList{Members: actions},
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

type xmlRule struct {
	RuleArn   string        `xml:"RuleArn"`
	Priority  string        `xml:"Priority"`
	Actions   xmlActionList `xml:"Actions"`
	IsDefault bool          `xml:"IsDefault"`
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
