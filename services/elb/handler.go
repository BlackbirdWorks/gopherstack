package elb

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	elbVersion = "2012-06-01"
	elbXMLNS   = "http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/"
)

// Handler is the Echo HTTP handler for Classic ELB operations.
type Handler struct {
	Backend StorageBackend
	// ops is the pre-built dispatch table mapping action names to handler functions.
	ops map[string]func(url.Values) (any, error)
}

// NewHandler creates a new ELB handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// buildOps returns the action-to-handler dispatch table.
func (h *Handler) buildOps() map[string]func(url.Values) (any, error) {
	return map[string]func(url.Values) (any, error){
		"CreateLoadBalancer":                      h.handleCreateLoadBalancer,
		"DeleteLoadBalancer":                      h.handleDeleteLoadBalancer,
		"DescribeLoadBalancers":                   h.handleDescribeLoadBalancers,
		"CreateLoadBalancerListeners":             h.handleCreateLoadBalancerListeners,
		"DeleteLoadBalancerListeners":             h.handleDeleteLoadBalancerListeners,
		"RegisterInstancesWithLoadBalancer":       h.handleRegisterInstances,
		"DeregisterInstancesFromLoadBalancer":     h.handleDeregisterInstances,
		"ConfigureHealthCheck":                    h.handleConfigureHealthCheck,
		"ModifyLoadBalancerAttributes":            h.handleModifyLoadBalancerAttributes,
		"DescribeLoadBalancerAttributes":          h.handleDescribeLoadBalancerAttributes,
		"AddTags":                                 h.handleAddTags,
		"DescribeTags":                            h.handleDescribeTags,
		"RemoveTags":                              h.handleRemoveTags,
		"ApplySecurityGroupsToLoadBalancer":       h.handleApplySecurityGroupsToLoadBalancer,
		"AttachLoadBalancerToSubnets":             h.handleAttachLoadBalancerToSubnets,
		"DetachLoadBalancerFromSubnets":           h.handleDetachLoadBalancerFromSubnets,
		"EnableAvailabilityZonesForLoadBalancer":  h.handleEnableAvailabilityZonesForLoadBalancer,
		"DisableAvailabilityZonesForLoadBalancer": h.handleDisableAvailabilityZonesForLoadBalancer,
		"SetLoadBalancerListenerSSLCertificate":   h.handleSetLoadBalancerListenerSSLCertificate,
		"SetLoadBalancerPoliciesOfListener":       h.handleSetLoadBalancerPoliciesOfListener,
		"SetLoadBalancerPoliciesForBackendServer": h.handleSetLoadBalancerPoliciesForBackendServer,
		"CreateAppCookieStickinessPolicy":         h.handleCreateAppCookieStickinessPolicy,
		"CreateLBCookieStickinessPolicy":          h.handleCreateLBCookieStickinessPolicy,
		"CreateLoadBalancerPolicy":                h.handleCreateLoadBalancerPolicy,
		"DeleteLoadBalancerPolicy":                h.handleDeleteLoadBalancerPolicy,
		"DescribeAccountLimits":                   h.handleDescribeAccountLimits,
		"DescribeInstanceHealth":                  h.handleDescribeInstanceHealth,
		"DescribeLoadBalancerPolicies":            h.handleDescribeLoadBalancerPolicies,
		"DescribeLoadBalancerPolicyTypes":         h.handleDescribeLoadBalancerPolicyTypes,
	}
}

// Reset clears the backend state, delegating to the underlying StorageBackend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "ELB" }

// GetSupportedOperations returns the list of supported ELB operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateLoadBalancer",
		"DeleteLoadBalancer",
		"DescribeLoadBalancers",
		"CreateLoadBalancerListeners",
		"DeleteLoadBalancerListeners",
		"RegisterInstancesWithLoadBalancer",
		"DeregisterInstancesFromLoadBalancer",
		"ConfigureHealthCheck",
		"ModifyLoadBalancerAttributes",
		"DescribeLoadBalancerAttributes",
		"AddTags",
		"DescribeTags",
		"RemoveTags",
		"ApplySecurityGroupsToLoadBalancer",
		"AttachLoadBalancerToSubnets",
		"DetachLoadBalancerFromSubnets",
		"EnableAvailabilityZonesForLoadBalancer",
		"DisableAvailabilityZonesForLoadBalancer",
		"SetLoadBalancerListenerSSLCertificate",
		"SetLoadBalancerPoliciesOfListener",
		"SetLoadBalancerPoliciesForBackendServer",
		"CreateAppCookieStickinessPolicy",
		"CreateLBCookieStickinessPolicy",
		"CreateLoadBalancerPolicy",
		"DeleteLoadBalancerPolicy",
		"DescribeAccountLimits",
		"DescribeInstanceHealth",
		"DescribeLoadBalancerPolicies",
		"DescribeLoadBalancerPolicyTypes",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "elasticloadbalancing" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Classic ELB requests.
// ELB requests are form-encoded POSTs with Version=2012-06-01.
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

		return vals.Get("Version") == elbVersion
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityFormStandard }

// ExtractOperation extracts the ELB action from the request.
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

// ExtractResource extracts the load balancer name from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("LoadBalancerName")
}

// Handler returns the Echo handler function for ELB operations.
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
		log.Debug("elb request", "action", action)

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

// dispatch routes the ELB action to the appropriate handler.
func (h *Handler) dispatch(action string, vals url.Values) (any, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownAction, action)
	}

	return fn(vals)
}

func (h *Handler) handleCreateLoadBalancer(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	listeners, err := parseListeners(vals)
	if err != nil {
		return nil, err
	}

	azs := parseMembers(vals, "AvailabilityZones.member")
	sgs := parseMembers(vals, "SecurityGroups.member")
	subnets := parseMembers(vals, "Subnets.member")
	scheme := vals.Get("Scheme")

	lb, createErr := h.Backend.CreateLoadBalancer(CreateLoadBalancerInput{
		LoadBalancerName:  name,
		Scheme:            scheme,
		AvailabilityZones: azs,
		SecurityGroups:    sgs,
		Subnets:           subnets,
		Listeners:         listeners,
	})
	if createErr != nil {
		return nil, createErr
	}

	// AWS allows passing initial Tags at CreateLoadBalancer time.
	if initialTags := parseTagKVs(vals, "Tags.member"); len(initialTags) > 0 {
		if tagErr := h.Backend.AddTags([]string{name}, initialTags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createLoadBalancerResponse{
		Xmlns: elbXMLNS,
		Result: createLoadBalancerResult{
			DNSName: lb.DNSName,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-" + name},
	}, nil
}

func (h *Handler) handleDeleteLoadBalancer(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteLoadBalancer(name); err != nil {
		return nil, err
	}

	return &deleteLoadBalancerResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-delete-" + name},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancers(vals url.Values) (any, error) {
	names := parseMembers(vals, "LoadBalancerNames.member")

	lbs, err := h.Backend.DescribeLoadBalancers(names)
	if err != nil {
		return nil, err
	}

	// Pagination: Marker (name of last LB on previous page) and PageSize (1-400).
	const maxPageSize = 400

	pageSize := maxPageSize

	if ps := vals.Get("PageSize"); ps != "" {
		if n, parseErr := strconv.Atoi(ps); parseErr == nil && n > 0 && n <= maxPageSize {
			pageSize = n
		}
	}

	marker := vals.Get("Marker")
	startIdx := 0

	if marker != "" {
		found := false

		for i, lb := range lbs {
			if lb.LoadBalancerName == marker {
				startIdx = i + 1
				found = true

				break
			}
		}

		if !found {
			return nil, fmt.Errorf(
				"%w: Marker %q does not match any existing load balancer",
				ErrInvalidParameter,
				marker,
			)
		}
	}

	if startIdx > len(lbs) {
		startIdx = len(lbs)
	}

	lbs = lbs[startIdx:]

	nextMarker := ""

	if len(lbs) > pageSize {
		nextMarker = lbs[pageSize-1].LoadBalancerName
		lbs = lbs[:pageSize]
	}

	members := make([]xmlLoadBalancerDescription, 0, len(lbs))
	for i := range lbs {
		members = append(members, toXMLLoadBalancer(&lbs[i]))
	}

	return &describeLoadBalancersResponse{
		Xmlns: elbXMLNS,
		Result: describeLoadBalancersResult{
			LoadBalancerDescriptions: xmlLoadBalancerList{Members: members},
			NextMarker:               nextMarker,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-describe"},
	}, nil
}

func (h *Handler) handleRegisterInstances(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	instances := parseInstances(vals)

	remaining, err := h.Backend.RegisterInstancesWithLoadBalancer(name, instances)
	if err != nil {
		return nil, err
	}

	xmlInsts := toXMLInstances(remaining)

	return &registerInstancesResponse{
		Xmlns: elbXMLNS,
		Result: registerInstancesResult{
			Instances: xmlInstanceList{Members: xmlInsts},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-register-" + name},
	}, nil
}

func (h *Handler) handleDeregisterInstances(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	instances := parseInstances(vals)

	remaining, err := h.Backend.DeregisterInstancesFromLoadBalancer(name, instances)
	if err != nil {
		return nil, err
	}

	xmlInsts := toXMLInstances(remaining)

	return &deregisterInstancesResponse{
		Xmlns: elbXMLNS,
		Result: deregisterInstancesResult{
			Instances: xmlInstanceList{Members: xmlInsts},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-deregister-" + name},
	}, nil
}

func (h *Handler) handleConfigureHealthCheck(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	// Check LB exists before validating the remaining parameters; AWS returns
	// LoadBalancerNotFound before complaining about invalid HC params.
	if _, err := h.Backend.DescribeLoadBalancers([]string{name}); err != nil {
		return nil, err
	}

	hc, err := parseHealthCheck(vals)
	if err != nil {
		return nil, err
	}

	result, hcErr := h.Backend.ConfigureHealthCheck(name, hc)
	if hcErr != nil {
		return nil, hcErr
	}

	return &configureHealthCheckResponse{
		Xmlns: elbXMLNS,
		Result: configureHealthCheckResult{
			HealthCheck: toXMLHealthCheck(result),
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-hc-" + name},
	}, nil
}

// parseHealthCheck validates and parses health check parameters from form values.
func parseHealthCheck(vals url.Values) (HealthCheck, error) {
	target := vals.Get("HealthCheck.Target")
	if target == "" {
		return HealthCheck{}, fmt.Errorf("%w: HealthCheck.Target is required", ErrInvalidParameter)
	}

	if err := validateHealthCheckTarget(target); err != nil {
		return HealthCheck{}, err
	}

	// Normalize target protocol to uppercase (AWS stores uppercase).
	if colonIdx := strings.Index(target, ":"); colonIdx > 0 {
		target = strings.ToUpper(target[:colonIdx]) + target[colonIdx:]
	}

	interval, timeout, err := parseHealthCheckTimings(vals)
	if err != nil {
		return HealthCheck{}, err
	}

	unhealthy, healthy, err := parseHealthCheckThresholds(vals)
	if err != nil {
		return HealthCheck{}, err
	}

	return HealthCheck{
		Target:             target,
		Interval:           interval,
		Timeout:            timeout,
		UnhealthyThreshold: unhealthy,
		HealthyThreshold:   healthy,
	}, nil
}

// parseHealthCheckTimings validates and returns the Interval and Timeout parameters.
func parseHealthCheckTimings(vals url.Values) (int32, int32, error) {
	interval, parseErr := parseInt32(vals.Get("HealthCheck.Interval"))
	if parseErr != nil || interval < 5 || interval > 300 {
		return 0, 0, fmt.Errorf("%w: HealthCheck.Interval must be between 5 and 300", ErrInvalidParameter)
	}

	timeout, parseErr := parseInt32(vals.Get("HealthCheck.Timeout"))
	if parseErr != nil || timeout < 2 || timeout > 60 {
		return 0, 0, fmt.Errorf("%w: HealthCheck.Timeout must be between 2 and 60", ErrInvalidParameter)
	}

	if timeout >= interval {
		return 0, 0, fmt.Errorf(
			"%w: HealthCheck.Timeout must be less than HealthCheck.Interval",
			ErrInvalidParameter,
		)
	}

	return interval, timeout, nil
}

// parseHealthCheckThresholds validates and returns UnhealthyThreshold and HealthyThreshold.
func parseHealthCheckThresholds(vals url.Values) (int32, int32, error) {
	unhealthy, parseErr := parseInt32(vals.Get("HealthCheck.UnhealthyThreshold"))
	if parseErr != nil || unhealthy < 2 || unhealthy > 10 {
		return 0, 0, fmt.Errorf(
			"%w: HealthCheck.UnhealthyThreshold must be between 2 and 10",
			ErrInvalidParameter,
		)
	}

	healthy, parseErr := parseInt32(vals.Get("HealthCheck.HealthyThreshold"))
	if parseErr != nil || healthy < 2 || healthy > 10 {
		return 0, 0, fmt.Errorf(
			"%w: HealthCheck.HealthyThreshold must be between 2 and 10",
			ErrInvalidParameter,
		)
	}

	return unhealthy, healthy, nil
}

func (h *Handler) handleAddTags(vals url.Values) (any, error) {
	names := parseMembers(vals, "LoadBalancerNames.member")
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: at least one LoadBalancerName is required", ErrInvalidParameter)
	}

	kvs := parseTagKVs(vals, "Tags.member")

	for _, kv := range kvs {
		if strings.HasPrefix(kv.Key, "aws:") {
			return nil, fmt.Errorf("%w: Tag keys starting with 'aws:' are reserved", ErrInvalidParameter)
		}
	}

	if err := h.Backend.AddTags(names, kvs); err != nil {
		return nil, err
	}

	return &addTagsResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-addtags"},
	}, nil
}

func (h *Handler) handleDescribeTags(vals url.Values) (any, error) {
	names := parseMembers(vals, "LoadBalancerNames.member")
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: at least one LoadBalancerName is required", ErrInvalidParameter)
	}

	tagMap, err := h.Backend.DescribeTags(names)
	if err != nil {
		return nil, err
	}

	tagDescs := make([]xmlTagDescription, 0, len(names))
	for _, name := range names {
		kvs := tagMap[name]
		xmlKVs := make([]xmlTag, 0, len(kvs))

		for _, kv := range kvs {
			xmlKVs = append(xmlKVs, xmlTag{Key: kv.Key, Value: kv.Value})
		}

		tagDescs = append(tagDescs, xmlTagDescription{
			LoadBalancerName: name,
			Tags:             xmlTagList{Members: xmlKVs},
		})
	}

	return &describeTagsResponse{
		Xmlns: elbXMLNS,
		Result: describeTagsResult{
			TagDescriptions: xmlTagDescriptionList{Members: tagDescs},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-describetags"},
	}, nil
}

func (h *Handler) handleRemoveTags(vals url.Values) (any, error) {
	names := parseMembers(vals, "LoadBalancerNames.member")
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: at least one LoadBalancerName is required", ErrInvalidParameter)
	}

	keys := parseTagKeys(vals, "Tags.member")

	if err := h.Backend.RemoveTags(names, keys); err != nil {
		return nil, err
	}

	return &removeTagsResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-removetags"},
	}, nil
}

func (h *Handler) handleCreateLoadBalancerListeners(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	listeners, parseErr := parseListeners(vals)
	if parseErr != nil {
		return nil, parseErr
	}

	if len(listeners) == 0 {
		return nil, fmt.Errorf("%w: at least one listener is required", ErrInvalidParameter)
	}

	if createErr := h.Backend.CreateLoadBalancerListeners(name, listeners); createErr != nil {
		return nil, createErr
	}

	return &createLoadBalancerListenersResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-createlisteners-" + name},
	}, nil
}

func (h *Handler) handleDeleteLoadBalancerListeners(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	ports := parseListenerPorts(vals, "LoadBalancerPorts.member")

	if err := h.Backend.DeleteLoadBalancerListeners(name, ports); err != nil {
		return nil, err
	}

	return &deleteLoadBalancerListenersResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-deletelisteners-" + name},
	}, nil
}

func (h *Handler) handleModifyLoadBalancerAttributes(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	attrs := parseLoadBalancerAttributes(vals)

	const minTimeout = 1
	const maxIdleTimeout = 3600
	const maxDrainingTimeout = 3600

	if attrs.IdleTimeout < minTimeout || attrs.IdleTimeout > maxIdleTimeout {
		return nil, fmt.Errorf(
			"%w: IdleTimeout must be between 1 and 3600 seconds",
			ErrInvalidParameter,
		)
	}

	if attrs.ConnectionDraining &&
		(attrs.ConnectionDrainingTimeout < minTimeout || attrs.ConnectionDrainingTimeout > maxDrainingTimeout) {
		return nil, fmt.Errorf(
			"%w: ConnectionDrainingTimeout must be between 1 and 3600 seconds",
			ErrInvalidParameter,
		)
	}

	validDesyncModes := map[string]bool{"defensive": true, "strictest": true, "monitor": true}
	if attrs.DesyncMitigationMode != "" && !validDesyncModes[attrs.DesyncMitigationMode] {
		return nil, fmt.Errorf(
			"%w: DesyncMitigationMode must be one of 'defensive', 'strictest', 'monitor'",
			ErrInvalidParameter,
		)
	}

	result, err := h.Backend.ModifyLoadBalancerAttributes(name, attrs)
	if err != nil {
		return nil, err
	}

	return &modifyLoadBalancerAttributesResponse{
		Xmlns: elbXMLNS,
		Result: modifyLoadBalancerAttributesResult{
			LoadBalancerAttributes: toXMLLoadBalancerAttributes(result),
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-modifyattrs-" + name},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancerAttributes(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	attrs, err := h.Backend.DescribeLoadBalancerAttributes(name)
	if err != nil {
		return nil, err
	}

	return &describeLoadBalancerAttributesResponse{
		Xmlns: elbXMLNS,
		Result: describeLoadBalancerAttributesResult{
			LoadBalancerAttributes: toXMLLoadBalancerAttributes(attrs),
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-describeattrs-" + name},
	}, nil
}

func (h *Handler) handleApplySecurityGroupsToLoadBalancer(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	sgs := parseMembers(vals, "SecurityGroups.member")

	result, err := h.Backend.ApplySecurityGroupsToLoadBalancer(name, sgs)
	if err != nil {
		return nil, err
	}

	sgMembers := make([]xmlStringValue, 0, len(result))
	for _, sg := range result {
		sgMembers = append(sgMembers, xmlStringValue{Value: sg})
	}

	return &applySecurityGroupsResponse{
		Xmlns: elbXMLNS,
		Result: applySecurityGroupsResult{
			SecurityGroups: xmlStringValueList{Members: sgMembers},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-applysg-" + name},
	}, nil
}

func (h *Handler) handleAttachLoadBalancerToSubnets(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	subnets := parseMembers(vals, "Subnets.member")

	result, err := h.Backend.AttachLoadBalancerToSubnets(name, subnets)
	if err != nil {
		return nil, err
	}

	subnetMembers := make([]xmlStringValue, 0, len(result))
	for _, s := range result {
		subnetMembers = append(subnetMembers, xmlStringValue{Value: s})
	}

	return &attachLoadBalancerToSubnetsResponse{
		Xmlns: elbXMLNS,
		Result: attachLoadBalancerToSubnetsResult{
			Subnets: xmlStringValueList{Members: subnetMembers},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-attachsubnets-" + name},
	}, nil
}

func (h *Handler) handleDetachLoadBalancerFromSubnets(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	subnets := parseMembers(vals, "Subnets.member")

	result, err := h.Backend.DetachLoadBalancerFromSubnets(name, subnets)
	if err != nil {
		return nil, err
	}

	subnetMembers := make([]xmlStringValue, 0, len(result))
	for _, s := range result {
		subnetMembers = append(subnetMembers, xmlStringValue{Value: s})
	}

	return &detachLoadBalancerFromSubnetsResponse{
		Xmlns: elbXMLNS,
		Result: detachLoadBalancerFromSubnetsResult{
			Subnets: xmlStringValueList{Members: subnetMembers},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-detachsubnets-" + name},
	}, nil
}

func (h *Handler) handleEnableAvailabilityZonesForLoadBalancer(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	azs := parseMembers(vals, "AvailabilityZones.member")

	result, err := h.Backend.EnableAvailabilityZonesForLoadBalancer(name, azs)
	if err != nil {
		return nil, err
	}

	azMembers := make([]xmlStringValue, 0, len(result))
	for _, az := range result {
		azMembers = append(azMembers, xmlStringValue{Value: az})
	}

	return &enableAvailabilityZonesResponse{
		Xmlns: elbXMLNS,
		Result: enableAvailabilityZonesResult{
			AvailabilityZones: xmlStringValueList{Members: azMembers},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-enableazs-" + name},
	}, nil
}

func (h *Handler) handleDisableAvailabilityZonesForLoadBalancer(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	azs := parseMembers(vals, "AvailabilityZones.member")

	result, err := h.Backend.DisableAvailabilityZonesForLoadBalancer(name, azs)
	if err != nil {
		return nil, err
	}

	azMembers := make([]xmlStringValue, 0, len(result))
	for _, az := range result {
		azMembers = append(azMembers, xmlStringValue{Value: az})
	}

	return &disableAvailabilityZonesResponse{
		Xmlns: elbXMLNS,
		Result: disableAvailabilityZonesResult{
			AvailabilityZones: xmlStringValueList{Members: azMembers},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-disableazs-" + name},
	}, nil
}

func (h *Handler) handleSetLoadBalancerListenerSSLCertificate(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	port, err := parseInt32(vals.Get("LoadBalancerPort"))
	if err != nil || port == 0 {
		return nil, fmt.Errorf("%w: LoadBalancerPort is required", ErrInvalidParameter)
	}

	certID := vals.Get("SSLCertificateId")
	if certID == "" {
		return nil, fmt.Errorf("%w: SSLCertificateId is required", ErrInvalidParameter)
	}

	if setErr := h.Backend.SetLoadBalancerListenerSSLCertificate(name, port, certID); setErr != nil {
		return nil, setErr
	}

	return &setLoadBalancerListenerSSLCertificateResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-sslcert-" + name},
	}, nil
}

func (h *Handler) handleSetLoadBalancerPoliciesOfListener(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	port, err := parseInt32(vals.Get("LoadBalancerPort"))
	if err != nil || port == 0 {
		return nil, fmt.Errorf("%w: LoadBalancerPort is required", ErrInvalidParameter)
	}

	policyNames := parseMembers(vals, "PolicyNames.member")

	if setErr := h.Backend.SetLoadBalancerPoliciesOfListener(name, port, policyNames); setErr != nil {
		return nil, setErr
	}

	return &setLoadBalancerPoliciesOfListenerResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-listpolicies-" + name},
	}, nil
}

func (h *Handler) handleSetLoadBalancerPoliciesForBackendServer(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	instancePort, err := parseInt32(vals.Get("InstancePort"))
	if err != nil || instancePort == 0 {
		return nil, fmt.Errorf("%w: InstancePort is required", ErrInvalidParameter)
	}

	policyNames := parseMembers(vals, "PolicyNames.member")

	if setErr := h.Backend.SetLoadBalancerPoliciesForBackendServer(name, instancePort, policyNames); setErr != nil {
		return nil, setErr
	}

	return &setLoadBalancerPoliciesForBackendServerResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-backendpolicies-" + name},
	}, nil
}

func (h *Handler) handleCreateAppCookieStickinessPolicy(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	policyName := vals.Get("PolicyName")
	if policyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrInvalidParameter)
	}

	if !policyNameRe.MatchString(policyName) {
		return nil, fmt.Errorf(
			"%w: PolicyName must be 1-32 alphanumeric characters or hyphens, starting and ending with alphanumeric",
			ErrInvalidParameter,
		)
	}

	cookieName := vals.Get("CookieName")

	if cookieName == "" {
		return nil, fmt.Errorf("%w: CookieName is required", ErrInvalidParameter)
	}

	if err := h.Backend.CreateAppCookieStickinessPolicy(name, policyName, cookieName); err != nil {
		return nil, err
	}

	return &createAppCookieStickinessPolicyResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-appcookie-" + name},
	}, nil
}

func (h *Handler) handleCreateLBCookieStickinessPolicy(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	policyName := vals.Get("PolicyName")
	if policyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrInvalidParameter)
	}

	if !policyNameRe.MatchString(policyName) {
		return nil, fmt.Errorf(
			"%w: PolicyName must be 1-32 alphanumeric characters or hyphens, starting and ending with alphanumeric",
			ErrInvalidParameter,
		)
	}

	var cookieExpiration int64
	if v := vals.Get("CookieExpirationPeriod"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid CookieExpirationPeriod", ErrInvalidParameter)
		}

		cookieExpiration = n
	}

	if err := h.Backend.CreateLBCookieStickinessPolicy(name, policyName, cookieExpiration); err != nil {
		return nil, err
	}

	return &createLBCookieStickinessPolicyResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-lbcookie-" + name},
	}, nil
}

func (h *Handler) handleCreateLoadBalancerPolicy(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	policyName := vals.Get("PolicyName")
	if policyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrInvalidParameter)
	}

	if !policyNameRe.MatchString(policyName) {
		return nil, fmt.Errorf(
			"%w: PolicyName must be 1-32 alphanumeric characters or hyphens, starting and ending with alphanumeric",
			ErrInvalidParameter,
		)
	}

	policyTypeName := vals.Get("PolicyTypeName")
	if policyTypeName == "" {
		return nil, fmt.Errorf("%w: PolicyTypeName is required", ErrInvalidParameter)
	}

	if _, ok := knownPolicyTypes[policyTypeName]; !ok {
		const validTypes = "AppCookieStickinessPolicyType, LBCookieStickinessPolicyType, " +
			"ProxyProtocolPolicyType, SSLNegotiationPolicyType, BackendServerAuthenticationPolicyType"

		return nil, fmt.Errorf(
			"%w: unknown PolicyTypeName %q; must be one of %s",
			ErrInvalidParameter,
			policyTypeName,
			validTypes,
		)
	}

	attrs := parsePolicyAttributes(vals)

	if err := h.Backend.CreateLoadBalancerPolicy(name, policyName, policyTypeName, attrs); err != nil {
		return nil, err
	}

	return &createLoadBalancerPolicyResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-createpolicy-" + name},
	}, nil
}

func (h *Handler) handleDeleteLoadBalancerPolicy(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	policyName := vals.Get("PolicyName")
	if policyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteLoadBalancerPolicy(name, policyName); err != nil {
		return nil, err
	}

	return &deleteLoadBalancerPolicyResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-deletepolicy-" + name},
	}, nil
}

func (h *Handler) handleDescribeAccountLimits(_ url.Values) (any, error) {
	limits, err := h.Backend.DescribeAccountLimits()
	if err != nil {
		return nil, err
	}

	xmlLimits := make([]xmlAccountLimit, 0, len(limits))
	for _, l := range limits {
		xmlLimits = append(xmlLimits, xmlAccountLimit(l))
	}

	return &describeAccountLimitsResponse{
		Xmlns: elbXMLNS,
		Result: describeAccountLimitsResult{
			Limits: xmlAccountLimitList{Members: xmlLimits},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-acctlimits"},
	}, nil
}

func (h *Handler) handleDescribeInstanceHealth(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	instances := parseInstances(vals)

	states, err := h.Backend.DescribeInstanceHealth(name, instances)
	if err != nil {
		return nil, err
	}

	xmlStates := make([]xmlInstanceState, 0, len(states))
	for _, s := range states {
		xmlStates = append(xmlStates, xmlInstanceState(s))
	}

	return &describeInstanceHealthResponse{
		Xmlns: elbXMLNS,
		Result: describeInstanceHealthResult{
			InstanceStates: xmlInstanceStateList{Members: xmlStates},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-instancehealth-" + name},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancerPolicies(vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	policyNames := parseMembers(vals, "PolicyNames.member")

	policies, err := h.Backend.DescribeLoadBalancerPolicies(name, policyNames)
	if err != nil {
		return nil, err
	}

	xmlPolicies := make([]xmlPolicyDescription, 0, len(policies))
	for _, p := range policies {
		xmlAttrs := make([]xmlPolicyAttributeDescription, 0, len(p.PolicyAttributeDescriptions))
		for _, a := range p.PolicyAttributeDescriptions {
			xmlAttrs = append(xmlAttrs, xmlPolicyAttributeDescription(a))
		}

		xmlPolicies = append(xmlPolicies, xmlPolicyDescription{
			PolicyName:                  p.PolicyName,
			PolicyTypeName:              p.PolicyTypeName,
			PolicyAttributeDescriptions: xmlPolicyAttributeDescriptionList{Members: xmlAttrs},
		})
	}

	return &describeLoadBalancerPoliciesResponse{
		Xmlns: elbXMLNS,
		Result: describeLoadBalancerPoliciesResult{
			PolicyDescriptions: xmlPolicyDescriptionList{Members: xmlPolicies},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-policies"},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancerPolicyTypes(vals url.Values) (any, error) {
	typeNames := parseMembers(vals, "PolicyTypeNames.member")

	types, err := h.Backend.DescribeLoadBalancerPolicyTypes(typeNames)
	if err != nil {
		return nil, err
	}

	xmlTypes := make([]xmlPolicyTypeDescription, 0, len(types))
	for _, t := range types {
		xmlAttrTypes := make([]xmlPolicyAttributeTypeDescription, 0, len(t.PolicyAttributeTypeDescriptions))
		for _, at := range t.PolicyAttributeTypeDescriptions {
			xmlAttrTypes = append(xmlAttrTypes, xmlPolicyAttributeTypeDescription(at))
		}

		xmlTypes = append(xmlTypes, xmlPolicyTypeDescription{
			PolicyTypeName:                  t.PolicyTypeName,
			Description:                     t.Description,
			PolicyAttributeTypeDescriptions: xmlPolicyAttributeTypeDescriptionList{Members: xmlAttrTypes},
		})
	}

	return &describeLoadBalancerPolicyTypesResponse{
		Xmlns: elbXMLNS,
		Result: describeLoadBalancerPolicyTypesResult{
			PolicyTypeDescriptions: xmlPolicyTypeDescriptionList{Members: xmlTypes},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-policytypes"},
	}, nil
}

// handleOpError translates an operation error into an HTTP response.
func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	code, statusCode := elbErrorCode(opErr)

	if code == "" {
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("elb internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func elbErrorCode(opErr error) (string, int) {
	type errorMapping struct {
		sentinel error
		code     string
		httpCode int
	}

	// Order matters: more-specific sentinels must come before generic ones.
	mappings := []errorMapping{
		{ErrPolicyNotFound, "PolicyNotFound", http.StatusNotFound},
		{ErrPolicyAlreadyExists, "DuplicatePolicyName", http.StatusConflict},
		{ErrListenerNotFound, "ListenerNotFound", http.StatusNotFound},
		{ErrInvalidInstance, "InvalidInstance", http.StatusBadRequest},
		{ErrLoadBalancerNotFound, "LoadBalancerNotFound", http.StatusNotFound},
		{ErrLoadBalancerAlreadyExists, "DuplicateLoadBalancerName", http.StatusConflict},
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
	errResp := &elbErrorResponse{
		Xmlns:     elbXMLNS,
		Error:     elbError{Code: code, Message: message, Type: "Sender"},
		RequestID: "elb-error",
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

// validateHealthCheckTarget validates the HealthCheck Target format expected by AWS:
// PROTOCOL:PORT for TCP/SSL or PROTOCOL:PORT/PATH for HTTP/HTTPS.
func validateHealthCheckTarget(target string) error {
	colonIdx := strings.Index(target, ":")
	if colonIdx < 1 {
		return fmt.Errorf(
			"%w: HealthCheck.Target must be in the format PROTOCOL:PORT or PROTOCOL:PORT/PATH",
			ErrInvalidParameter,
		)
	}

	proto := strings.ToUpper(target[:colonIdx])
	rest := target[colonIdx+1:]

	switch proto {
	case "HTTP", "HTTPS":
		slashIdx := strings.Index(rest, "/")
		if slashIdx < 1 {
			return fmt.Errorf(
				"%w: HealthCheck.Target for HTTP/HTTPS must include a path (e.g. HTTP:80/health)",
				ErrInvalidParameter,
			)
		}

		portStr := rest[:slashIdx]
		p, err := strconv.ParseInt(portStr, 10, 32)

		if err != nil || p < 1 || p > 65535 {
			return fmt.Errorf("%w: HealthCheck.Target port must be between 1 and 65535", ErrInvalidParameter)
		}
	case "TCP", "SSL":
		p, err := strconv.ParseInt(rest, 10, 32)

		if err != nil || p < 1 || p > 65535 {
			return fmt.Errorf("%w: HealthCheck.Target port must be between 1 and 65535", ErrInvalidParameter)
		}
	default:
		return fmt.Errorf("%w: HealthCheck.Target protocol must be one of HTTP, HTTPS, TCP, SSL", ErrInvalidParameter)
	}

	return nil
}

// parseMembers extracts indexed form values (e.g. "LoadBalancerNames.member.1").
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

// parseListeners extracts listener definitions from Listeners.member.N.* form values.
func parseListeners(vals url.Values) ([]Listener, error) {
	result := make([]Listener, 0)

	for i := 1; ; i++ {
		proto := vals.Get(fmt.Sprintf("Listeners.member.%d.Protocol", i))
		if proto == "" {
			break
		}

		proto = strings.ToUpper(proto)

		switch proto {
		case "HTTP", "HTTPS", "TCP", "SSL":
		default:
			return nil, fmt.Errorf("%w: Protocol must be one of HTTP, HTTPS, TCP, SSL", ErrInvalidParameter)
		}

		lbPort, err := parseInt32(vals.Get(fmt.Sprintf("Listeners.member.%d.LoadBalancerPort", i)))
		if err != nil || lbPort < 1 || lbPort > 65535 {
			return nil, fmt.Errorf("%w: LoadBalancerPort must be between 1 and 65535", ErrInvalidParameter)
		}

		instProto := strings.ToUpper(vals.Get(fmt.Sprintf("Listeners.member.%d.InstanceProtocol", i)))
		if instProto == "" {
			instProto = proto
		}

		instPort, err := parseInt32(vals.Get(fmt.Sprintf("Listeners.member.%d.InstancePort", i)))
		if err != nil || instPort < 1 || instPort > 65535 {
			return nil, fmt.Errorf("%w: InstancePort must be between 1 and 65535", ErrInvalidParameter)
		}

		certID := vals.Get(fmt.Sprintf("Listeners.member.%d.SSLCertificateId", i))

		result = append(result, Listener{
			Protocol:         proto,
			LoadBalancerPort: lbPort,
			InstanceProtocol: instProto,
			InstancePort:     instPort,
			SSLCertificateID: certID,
		})
	}

	return result, nil
}

// parseInstances extracts instance IDs from Instances.member.N.InstanceId form values.
func parseInstances(vals url.Values) []Instance {
	result := make([]Instance, 0)

	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("Instances.member.%d.InstanceId", i))
		if id == "" {
			break
		}

		result = append(result, Instance{InstanceID: id})
	}

	return result
}

// parseTagKVs extracts key-value tag pairs from Tags.member.N.Key/Value form values.
func parseTagKVs(vals url.Values, prefix string) []tags.KV {
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

// parseTagKeys extracts tag keys from Tags.member.N.Key form values (for RemoveTags).
func parseTagKeys(vals url.Values, prefix string) []string {
	result := make([]string, 0)

	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("%s.%d.Key", prefix, i))
		if k == "" {
			break
		}

		result = append(result, k)
	}

	return result
}

// parseListenerPorts extracts integer ports from LoadBalancerPorts.member.N form values.
func parseListenerPorts(vals url.Values, prefix string) []int32 {
	result := make([]int32, 0)

	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("%s.%d", prefix, i))
		if v == "" {
			break
		}

		p, err := parseInt32(v)
		if err != nil {
			continue
		}

		if p >= 1 {
			result = append(result, p)
		}
	}

	return result
}

// parseLoadBalancerAttributes reads LoadBalancerAttributes.* form values into a
// LoadBalancerAttributes struct. Missing values fall back to the service defaults.
func parseLoadBalancerAttributes(vals url.Values) LoadBalancerAttributes {
	attrs := defaultLBAttributes()

	if v := vals.Get("LoadBalancerAttributes.CrossZoneLoadBalancing.Enabled"); v != "" {
		attrs.CrossZoneLoadBalancing = v == "true"
	}

	if v := vals.Get("LoadBalancerAttributes.ConnectionDraining.Enabled"); v != "" {
		attrs.ConnectionDraining = v == "true"
	}

	if v := vals.Get("LoadBalancerAttributes.ConnectionDraining.Timeout"); v != "" {
		if n, err := parseInt32(v); err == nil {
			attrs.ConnectionDrainingTimeout = n
		}
	}

	if v := vals.Get("LoadBalancerAttributes.ConnectionSettings.IdleTimeout"); v != "" {
		if n, err := parseInt32(v); err == nil {
			attrs.IdleTimeout = n
		}
	}

	// The desync mitigation mode is passed as an AdditionalAttribute with
	// key "elb.http.desyncmitigationmode".
	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("LoadBalancerAttributes.AdditionalAttributes.member.%d.Key", i))
		if k == "" {
			break
		}

		v := vals.Get(fmt.Sprintf("LoadBalancerAttributes.AdditionalAttributes.member.%d.Value", i))

		if k == "elb.http.desyncmitigationmode" {
			attrs.DesyncMitigationMode = v
		}
	}

	return attrs
}

// parsePolicyAttributes extracts policy attribute pairs from PolicyAttributes.member.N.* form values.
func parsePolicyAttributes(vals url.Values) []PolicyAttribute {
	result := make([]PolicyAttribute, 0)

	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("PolicyAttributes.member.%d.AttributeName", i))
		if k == "" {
			break
		}

		result = append(result, PolicyAttribute{
			AttributeName:  k,
			AttributeValue: vals.Get(fmt.Sprintf("PolicyAttributes.member.%d.AttributeValue", i)),
		})
	}

	return result
}

// toXMLLoadBalancerAttributes converts a LoadBalancerAttributes to its XML wire representation.
func toXMLLoadBalancerAttributes(attrs *LoadBalancerAttributes) xmlLoadBalancerAttributes {
	additionalAttrs := []xmlAdditionalAttribute{
		{Key: "elb.http.desyncmitigationmode", Value: attrs.DesyncMitigationMode},
	}

	return xmlLoadBalancerAttributes{
		CrossZoneLoadBalancing: xmlBoolAttribute{Enabled: attrs.CrossZoneLoadBalancing},
		ConnectionDraining: xmlConnectionDraining{
			Enabled: attrs.ConnectionDraining,
			Timeout: attrs.ConnectionDrainingTimeout,
		},
		ConnectionSettings: xmlConnectionSettings{IdleTimeout: attrs.IdleTimeout},
		AdditionalAttributes: xmlAdditionalAttributeList{
			Members: additionalAttrs,
		},
	}
}

// toXMLLoadBalancer converts a LoadBalancer to its XML representation.
func toXMLLoadBalancer(lb *LoadBalancer) xmlLoadBalancerDescription {
	azs := make([]xmlStringValue, 0, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		azs = append(azs, xmlStringValue{Value: az})
	}

	sgs := make([]xmlStringValue, 0, len(lb.SecurityGroups))
	for _, sg := range lb.SecurityGroups {
		sgs = append(sgs, xmlStringValue{Value: sg})
	}

	subnets := make([]xmlStringValue, 0, len(lb.Subnets))
	for _, s := range lb.Subnets {
		subnets = append(subnets, xmlStringValue{Value: s})
	}

	listeners := make([]xmlListenerDescription, 0, len(lb.Listeners))
	for _, l := range lb.Listeners {
		policyMembers := make([]xmlStringValue, 0, len(l.PolicyNames))
		for _, p := range l.PolicyNames {
			policyMembers = append(policyMembers, xmlStringValue{Value: p})
		}

		listeners = append(listeners, xmlListenerDescription{
			Listener: xmlListener{
				Protocol:         l.Protocol,
				InstanceProtocol: l.InstanceProtocol,
				LoadBalancerPort: l.LoadBalancerPort,
				InstancePort:     l.InstancePort,
				SSLCertificateID: l.SSLCertificateID,
			},
			PolicyNames: xmlStringValueList{Members: policyMembers},
		})
	}

	bsds := make([]xmlBackendServerDescription, 0, len(lb.BackendServerDescriptions))
	for _, bsd := range lb.BackendServerDescriptions {
		policyMembers := make([]xmlStringValue, 0, len(bsd.PolicyNames))
		for _, p := range bsd.PolicyNames {
			policyMembers = append(policyMembers, xmlStringValue{Value: p})
		}

		bsds = append(bsds, xmlBackendServerDescription{
			InstancePort: bsd.InstancePort,
			PolicyNames:  xmlStringValueList{Members: policyMembers},
		})
	}

	instances := toXMLInstances(lb.Instances)

	d := xmlLoadBalancerDescription{
		LoadBalancerName:          lb.LoadBalancerName,
		DNSName:                   lb.DNSName,
		CanonicalHostedZoneName:   lb.CanonicalHostedZoneName,
		CanonicalHostedZoneNameID: lb.CanonicalHostedZoneNameID,
		CreatedTime:               lb.CreatedTime.UTC().Format(time.RFC3339),
		Scheme:                    lb.Scheme,
		VPCId:                     lb.VPCId,
		SourceSecurityGroup: xmlSourceSecurityGroup{
			GroupName:  "default",
			OwnerAlias: lb.AccountID,
		},
		AvailabilityZones:         xmlStringValueList{Members: azs},
		SecurityGroups:            xmlStringValueList{Members: sgs},
		Subnets:                   xmlStringValueList{Members: subnets},
		ListenerDescriptions:      xmlListenerDescriptionList{Members: listeners},
		BackendServerDescriptions: xmlBackendServerDescriptionList{Members: bsds},
		Instances:                 xmlInstanceList{Members: instances},
	}

	if lb.HealthCheck != nil {
		d.HealthCheck = toXMLHealthCheck(lb.HealthCheck)
	}

	return d
}

func toXMLInstances(instances []Instance) []xmlInstance {
	xmlInsts := make([]xmlInstance, 0, len(instances))
	for _, inst := range instances {
		xmlInsts = append(xmlInsts, xmlInstance(inst))
	}

	return xmlInsts
}

func toXMLHealthCheck(hc *HealthCheck) xmlHealthCheck {
	return xmlHealthCheck{
		Target:             hc.Target,
		Interval:           hc.Interval,
		Timeout:            hc.Timeout,
		UnhealthyThreshold: hc.UnhealthyThreshold,
		HealthyThreshold:   hc.HealthyThreshold,
	}
}

// --- XML error types ---

type elbError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type elbErrorResponse struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	Error     elbError `xml:"Error"`
	RequestID string   `xml:"RequestId"`
}

// --- XML response types ---

type xmlResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

type xmlStringValue struct {
	Value string `xml:",chardata"`
}

type xmlStringValueList struct {
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
	LoadBalancerName string     `xml:"LoadBalancerName"`
	Tags             xmlTagList `xml:"Tags"`
}

type xmlTagDescriptionList struct {
	Members []xmlTagDescription `xml:"member"`
}

type xmlInstance struct {
	InstanceID string `xml:"InstanceId"`
}

type xmlInstanceList struct {
	Members []xmlInstance `xml:"member"`
}

type xmlListener struct {
	Protocol         string `xml:"Protocol"`
	InstanceProtocol string `xml:"InstanceProtocol"`
	SSLCertificateID string `xml:"SSLCertificateId,omitempty"`
	LoadBalancerPort int32  `xml:"LoadBalancerPort"`
	InstancePort     int32  `xml:"InstancePort"`
}

type xmlListenerDescription struct {
	Listener    xmlListener        `xml:"Listener"`
	PolicyNames xmlStringValueList `xml:"PolicyNames"`
}

type xmlListenerDescriptionList struct {
	Members []xmlListenerDescription `xml:"member"`
}

type xmlBackendServerDescription struct {
	PolicyNames  xmlStringValueList `xml:"PolicyNames"`
	InstancePort int32              `xml:"InstancePort"`
}

type xmlBackendServerDescriptionList struct {
	Members []xmlBackendServerDescription `xml:"member"`
}

type xmlHealthCheck struct {
	Target             string `xml:"Target"`
	Interval           int32  `xml:"Interval"`
	Timeout            int32  `xml:"Timeout"`
	UnhealthyThreshold int32  `xml:"UnhealthyThreshold"`
	HealthyThreshold   int32  `xml:"HealthyThreshold"`
}

type xmlSourceSecurityGroup struct {
	GroupName  string `xml:"GroupName"`
	OwnerAlias string `xml:"OwnerAlias"`
}

type xmlLoadBalancerDescription struct {
	LoadBalancerName          string                          `xml:"LoadBalancerName"`
	DNSName                   string                          `xml:"DNSName"`
	CanonicalHostedZoneName   string                          `xml:"CanonicalHostedZoneName"`
	CanonicalHostedZoneNameID string                          `xml:"CanonicalHostedZoneNameID"`
	CreatedTime               string                          `xml:"CreatedTime"`
	Scheme                    string                          `xml:"Scheme"`
	VPCId                     string                          `xml:"VPCId,omitempty"`
	AvailabilityZones         xmlStringValueList              `xml:"AvailabilityZones"`
	SecurityGroups            xmlStringValueList              `xml:"SecurityGroups"`
	Subnets                   xmlStringValueList              `xml:"Subnets"`
	SourceSecurityGroup       xmlSourceSecurityGroup          `xml:"SourceSecurityGroup"`
	ListenerDescriptions      xmlListenerDescriptionList      `xml:"ListenerDescriptions"`
	BackendServerDescriptions xmlBackendServerDescriptionList `xml:"BackendServerDescriptions"`
	Instances                 xmlInstanceList                 `xml:"Instances"`
	HealthCheck               xmlHealthCheck                  `xml:"HealthCheck"`
}

type xmlLoadBalancerList struct {
	Members []xmlLoadBalancerDescription `xml:"member"`
}

// CreateLoadBalancer response.

type createLoadBalancerResult struct {
	DNSName string `xml:"DNSName"`
}

type createLoadBalancerResponse struct {
	XMLName          xml.Name                 `xml:"CreateLoadBalancerResponse"`
	Xmlns            string                   `xml:"xmlns,attr"`
	Result           createLoadBalancerResult `xml:"CreateLoadBalancerResult"`
	ResponseMetadata xmlResponseMetadata      `xml:"ResponseMetadata"`
}

// DeleteLoadBalancer response.

type deleteLoadBalancerResponse struct {
	XMLName          xml.Name            `xml:"DeleteLoadBalancerResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// DescribeLoadBalancers response.

type describeLoadBalancersResult struct {
	NextMarker               string              `xml:"NextMarker,omitempty"`
	LoadBalancerDescriptions xmlLoadBalancerList `xml:"LoadBalancerDescriptions"`
}

type describeLoadBalancersResponse struct {
	XMLName          xml.Name                    `xml:"DescribeLoadBalancersResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeLoadBalancersResult `xml:"DescribeLoadBalancersResult"`
}

// RegisterInstances response.

type registerInstancesResult struct {
	Instances xmlInstanceList `xml:"Instances"`
}

type registerInstancesResponse struct {
	XMLName          xml.Name                `xml:"RegisterInstancesWithLoadBalancerResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           registerInstancesResult `xml:"RegisterInstancesWithLoadBalancerResult"`
}

// DeregisterInstances response.

type deregisterInstancesResult struct {
	Instances xmlInstanceList `xml:"Instances"`
}

type deregisterInstancesResponse struct {
	XMLName          xml.Name                  `xml:"DeregisterInstancesFromLoadBalancerResponse"`
	Xmlns            string                    `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata       `xml:"ResponseMetadata"`
	Result           deregisterInstancesResult `xml:"DeregisterInstancesFromLoadBalancerResult"`
}

// ConfigureHealthCheck response.

type configureHealthCheckResult struct {
	HealthCheck xmlHealthCheck `xml:"HealthCheck"`
}

type configureHealthCheckResponse struct {
	XMLName          xml.Name                   `xml:"ConfigureHealthCheckResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata        `xml:"ResponseMetadata"`
	Result           configureHealthCheckResult `xml:"ConfigureHealthCheckResult"`
}

// AddTags response.

type addTagsResponse struct {
	Result           struct{}            `xml:"AddTagsResult"`
	XMLName          xml.Name            `xml:"AddTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// DescribeTags response.

type describeTagsResult struct {
	TagDescriptions xmlTagDescriptionList `xml:"TagDescriptions"`
}

type describeTagsResponse struct {
	XMLName          xml.Name            `xml:"DescribeTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           describeTagsResult  `xml:"DescribeTagsResult"`
}

// RemoveTags response.

type removeTagsResponse struct {
	Result           struct{}            `xml:"RemoveTagsResult"`
	XMLName          xml.Name            `xml:"RemoveTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// CreateLoadBalancerListeners response.

type createLoadBalancerListenersResult struct{}

type createLoadBalancerListenersResponse struct {
	XMLName          xml.Name                          `xml:"CreateLoadBalancerListenersResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	Result           createLoadBalancerListenersResult `xml:"CreateLoadBalancerListenersResult"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
}

// DeleteLoadBalancerListeners response.

type deleteLoadBalancerListenersResult struct{}

type deleteLoadBalancerListenersResponse struct {
	XMLName          xml.Name                          `xml:"DeleteLoadBalancerListenersResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	Result           deleteLoadBalancerListenersResult `xml:"DeleteLoadBalancerListenersResult"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
}

// LoadBalancerAttributes XML types.

type xmlBoolAttribute struct {
	Enabled bool `xml:"Enabled"`
}

type xmlConnectionDraining struct {
	Enabled bool  `xml:"Enabled"`
	Timeout int32 `xml:"Timeout"`
}

type xmlConnectionSettings struct {
	IdleTimeout int32 `xml:"IdleTimeout"`
}

type xmlAdditionalAttribute struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlAdditionalAttributeList struct {
	Members []xmlAdditionalAttribute `xml:"member"`
}

type xmlLoadBalancerAttributes struct {
	AdditionalAttributes   xmlAdditionalAttributeList `xml:"AdditionalAttributes"`
	ConnectionDraining     xmlConnectionDraining      `xml:"ConnectionDraining"`
	ConnectionSettings     xmlConnectionSettings      `xml:"ConnectionSettings"`
	CrossZoneLoadBalancing xmlBoolAttribute           `xml:"CrossZoneLoadBalancing"`
}

// ModifyLoadBalancerAttributes response.

type modifyLoadBalancerAttributesResult struct {
	LoadBalancerAttributes xmlLoadBalancerAttributes `xml:"LoadBalancerAttributes"`
}

type modifyLoadBalancerAttributesResponse struct {
	XMLName          xml.Name                           `xml:"ModifyLoadBalancerAttributesResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           modifyLoadBalancerAttributesResult `xml:"ModifyLoadBalancerAttributesResult"`
}

// DescribeLoadBalancerAttributes response.

type describeLoadBalancerAttributesResult struct {
	LoadBalancerAttributes xmlLoadBalancerAttributes `xml:"LoadBalancerAttributes"`
}

type describeLoadBalancerAttributesResponse struct {
	XMLName          xml.Name                             `xml:"DescribeLoadBalancerAttributesResponse"`
	Xmlns            string                               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                  `xml:"ResponseMetadata"`
	Result           describeLoadBalancerAttributesResult `xml:"DescribeLoadBalancerAttributesResult"`
}

// ApplySecurityGroupsToLoadBalancer response.

type applySecurityGroupsResult struct {
	SecurityGroups xmlStringValueList `xml:"SecurityGroups"`
}

type applySecurityGroupsResponse struct {
	XMLName          xml.Name                  `xml:"ApplySecurityGroupsToLoadBalancerResponse"`
	Xmlns            string                    `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata       `xml:"ResponseMetadata"`
	Result           applySecurityGroupsResult `xml:"ApplySecurityGroupsToLoadBalancerResult"`
}

// AttachLoadBalancerToSubnets response.

type attachLoadBalancerToSubnetsResult struct {
	Subnets xmlStringValueList `xml:"Subnets"`
}

type attachLoadBalancerToSubnetsResponse struct {
	XMLName          xml.Name                          `xml:"AttachLoadBalancerToSubnetsResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
	Result           attachLoadBalancerToSubnetsResult `xml:"AttachLoadBalancerToSubnetsResult"`
}

// DetachLoadBalancerFromSubnets response.

type detachLoadBalancerFromSubnetsResult struct {
	Subnets xmlStringValueList `xml:"Subnets"`
}

type detachLoadBalancerFromSubnetsResponse struct {
	XMLName          xml.Name                            `xml:"DetachLoadBalancerFromSubnetsResponse"`
	Xmlns            string                              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                 `xml:"ResponseMetadata"`
	Result           detachLoadBalancerFromSubnetsResult `xml:"DetachLoadBalancerFromSubnetsResult"`
}

// EnableAvailabilityZonesForLoadBalancer response.

type enableAvailabilityZonesResult struct {
	AvailabilityZones xmlStringValueList `xml:"AvailabilityZones"`
}

type enableAvailabilityZonesResponse struct {
	XMLName          xml.Name                      `xml:"EnableAvailabilityZonesForLoadBalancerResponse"`
	Xmlns            string                        `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata           `xml:"ResponseMetadata"`
	Result           enableAvailabilityZonesResult `xml:"EnableAvailabilityZonesForLoadBalancerResult"`
}

// DisableAvailabilityZonesForLoadBalancer response.

type disableAvailabilityZonesResult struct {
	AvailabilityZones xmlStringValueList `xml:"AvailabilityZones"`
}

type disableAvailabilityZonesResponse struct {
	XMLName          xml.Name                       `xml:"DisableAvailabilityZonesForLoadBalancerResponse"`
	Xmlns            string                         `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata            `xml:"ResponseMetadata"`
	Result           disableAvailabilityZonesResult `xml:"DisableAvailabilityZonesForLoadBalancerResult"`
}

// SetLoadBalancerListenerSSLCertificate response.

type setLoadBalancerListenerSSLCertificateResponse struct {
	XMLName          xml.Name            `xml:"SetLoadBalancerListenerSSLCertificateResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// SetLoadBalancerPoliciesOfListener response.

type setLoadBalancerPoliciesOfListenerResponse struct {
	XMLName          xml.Name            `xml:"SetLoadBalancerPoliciesOfListenerResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// SetLoadBalancerPoliciesForBackendServer response.

type setLoadBalancerPoliciesForBackendServerResponse struct {
	XMLName          xml.Name            `xml:"SetLoadBalancerPoliciesForBackendServerResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// CreateAppCookieStickinessPolicy response.

type createAppCookieStickinessPolicyResponse struct {
	XMLName          xml.Name            `xml:"CreateAppCookieStickinessPolicyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// CreateLBCookieStickinessPolicy response.

type createLBCookieStickinessPolicyResponse struct {
	XMLName          xml.Name            `xml:"CreateLBCookieStickinessPolicyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// CreateLoadBalancerPolicy response.

type createLoadBalancerPolicyResponse struct {
	XMLName          xml.Name            `xml:"CreateLoadBalancerPolicyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// DeleteLoadBalancerPolicy response.

type deleteLoadBalancerPolicyResponse struct {
	XMLName          xml.Name            `xml:"DeleteLoadBalancerPolicyResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// DescribeAccountLimits response.

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

// DescribeInstanceHealth response.

type xmlInstanceState struct {
	InstanceID  string `xml:"InstanceId"`
	State       string `xml:"State"`
	ReasonCode  string `xml:"ReasonCode"`
	Description string `xml:"Description"`
}

type xmlInstanceStateList struct {
	Members []xmlInstanceState `xml:"member"`
}

type describeInstanceHealthResult struct {
	InstanceStates xmlInstanceStateList `xml:"InstanceStates"`
}

type describeInstanceHealthResponse struct {
	XMLName          xml.Name                     `xml:"DescribeInstanceHealthResponse"`
	Xmlns            string                       `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata          `xml:"ResponseMetadata"`
	Result           describeInstanceHealthResult `xml:"DescribeInstanceHealthResult"`
}

// DescribeLoadBalancerPolicies response.

type xmlPolicyAttributeDescription struct {
	AttributeName  string `xml:"AttributeName"`
	AttributeValue string `xml:"AttributeValue"`
}

type xmlPolicyAttributeDescriptionList struct {
	Members []xmlPolicyAttributeDescription `xml:"member"`
}

type xmlPolicyDescription struct {
	PolicyName                  string                            `xml:"PolicyName"`
	PolicyTypeName              string                            `xml:"PolicyTypeName"`
	PolicyAttributeDescriptions xmlPolicyAttributeDescriptionList `xml:"PolicyAttributeDescriptions"`
}

type xmlPolicyDescriptionList struct {
	Members []xmlPolicyDescription `xml:"member"`
}

type describeLoadBalancerPoliciesResult struct {
	PolicyDescriptions xmlPolicyDescriptionList `xml:"PolicyDescriptions"`
}

type describeLoadBalancerPoliciesResponse struct {
	XMLName          xml.Name                           `xml:"DescribeLoadBalancerPoliciesResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           describeLoadBalancerPoliciesResult `xml:"DescribeLoadBalancerPoliciesResult"`
}

// DescribeLoadBalancerPolicyTypes response.

type xmlPolicyAttributeTypeDescription struct {
	AttributeName string `xml:"AttributeName"`
	AttributeType string `xml:"AttributeType"`
	Cardinality   string `xml:"Cardinality"`
	DefaultValue  string `xml:"DefaultValue,omitempty"`
	Description   string `xml:"Description,omitempty"`
}

type xmlPolicyAttributeTypeDescriptionList struct {
	Members []xmlPolicyAttributeTypeDescription `xml:"member"`
}

type xmlPolicyTypeDescription struct {
	PolicyTypeName                  string                                `xml:"PolicyTypeName"`
	Description                     string                                `xml:"Description"`
	PolicyAttributeTypeDescriptions xmlPolicyAttributeTypeDescriptionList `xml:"PolicyAttributeTypeDescriptions"`
}

type xmlPolicyTypeDescriptionList struct {
	Members []xmlPolicyTypeDescription `xml:"member"`
}

type describeLoadBalancerPolicyTypesResult struct {
	PolicyTypeDescriptions xmlPolicyTypeDescriptionList `xml:"PolicyTypeDescriptions"`
}

type describeLoadBalancerPolicyTypesResponse struct {
	XMLName          xml.Name                              `xml:"DescribeLoadBalancerPolicyTypesResponse"`
	Xmlns            string                                `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                   `xml:"ResponseMetadata"`
	Result           describeLoadBalancerPolicyTypesResult `xml:"DescribeLoadBalancerPolicyTypesResult"`
}
