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
}

// NewHandler creates a new ELB handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
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
	switch action {
	case "CreateLoadBalancer":
		return h.handleCreateLoadBalancer(vals)
	case "DeleteLoadBalancer":
		return h.handleDeleteLoadBalancer(vals)
	case "DescribeLoadBalancers":
		return h.handleDescribeLoadBalancers(vals)
	case "CreateLoadBalancerListeners":
		return h.handleCreateLoadBalancerListeners(vals)
	case "DeleteLoadBalancerListeners":
		return h.handleDeleteLoadBalancerListeners(vals)
	case "RegisterInstancesWithLoadBalancer":
		return h.handleRegisterInstances(vals)
	case "DeregisterInstancesFromLoadBalancer":
		return h.handleDeregisterInstances(vals)
	case "ConfigureHealthCheck":
		return h.handleConfigureHealthCheck(vals)
	case "ModifyLoadBalancerAttributes":
		return h.handleModifyLoadBalancerAttributes(vals)
	case "DescribeLoadBalancerAttributes":
		return h.handleDescribeLoadBalancerAttributes(vals)
	case "AddTags":
		return h.handleAddTags(vals)
	case "DescribeTags":
		return h.handleDescribeTags(vals)
	case "RemoveTags":
		return h.handleRemoveTags(vals)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownAction, action)
	}
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

	members := make([]xmlLoadBalancerDescription, 0, len(lbs))
	for i := range lbs {
		members = append(members, toXMLLoadBalancer(&lbs[i]))
	}

	return &describeLoadBalancersResponse{
		Xmlns: elbXMLNS,
		Result: describeLoadBalancersResult{
			LoadBalancerDescriptions: xmlLoadBalancerList{Members: members},
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

	interval, err := parseInt32(vals.Get("HealthCheck.Interval"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheck.Interval", ErrInvalidParameter)
	}

	timeout, err := parseInt32(vals.Get("HealthCheck.Timeout"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheck.Timeout", ErrInvalidParameter)
	}

	unhealthy, err := parseInt32(vals.Get("HealthCheck.UnhealthyThreshold"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheck.UnhealthyThreshold", ErrInvalidParameter)
	}

	healthy, err := parseInt32(vals.Get("HealthCheck.HealthyThreshold"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheck.HealthyThreshold", ErrInvalidParameter)
	}

	hc := HealthCheck{
		Target:             vals.Get("HealthCheck.Target"),
		Interval:           interval,
		Timeout:            timeout,
		UnhealthyThreshold: unhealthy,
		HealthyThreshold:   healthy,
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

func (h *Handler) handleAddTags(vals url.Values) (any, error) {
	names := parseMembers(vals, "LoadBalancerNames.member")
	if len(names) == 0 {
		return nil, fmt.Errorf("%w: at least one LoadBalancerName is required", ErrInvalidParameter)
	}

	kvs := parseTagKVs(vals, "Tags.member")

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

	mappings := []errorMapping{
		{awserr.ErrNotFound, "LoadBalancerNotFound", http.StatusNotFound},
		{awserr.ErrAlreadyExists, "DuplicateLoadBalancerName", http.StatusConflict},
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

		lbPort, err := parseInt32(vals.Get(fmt.Sprintf("Listeners.member.%d.LoadBalancerPort", i)))
		if err != nil {
			return nil, fmt.Errorf("%w: invalid LoadBalancerPort", ErrInvalidParameter)
		}

		instProto := vals.Get(fmt.Sprintf("Listeners.member.%d.InstanceProtocol", i))
		if instProto == "" {
			instProto = proto
		}

		instPort, err := parseInt32(vals.Get(fmt.Sprintf("Listeners.member.%d.InstancePort", i)))
		if err != nil {
			return nil, fmt.Errorf("%w: invalid InstancePort", ErrInvalidParameter)
		}

		result = append(result, Listener{
			Protocol:         proto,
			LoadBalancerPort: lbPort,
			InstanceProtocol: instProto,
			InstancePort:     instPort,
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

		result = append(result, p)
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
		listeners = append(listeners, xmlListenerDescription{
			Listener: xmlListener(l),
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
		AvailabilityZones:         xmlStringValueList{Members: azs},
		SecurityGroups:            xmlStringValueList{Members: sgs},
		Subnets:                   xmlStringValueList{Members: subnets},
		ListenerDescriptions:      xmlListenerDescriptionList{Members: listeners},
		Instances:                 xmlInstanceList{Members: instances},
	}

	if lb.HealthCheck != nil {
		hc := toXMLHealthCheck(lb.HealthCheck)
		d.HealthCheck = &hc
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
	LoadBalancerPort int32  `xml:"LoadBalancerPort"`
	InstancePort     int32  `xml:"InstancePort"`
}

type xmlListenerDescription struct {
	Listener xmlListener `xml:"Listener"`
}

type xmlListenerDescriptionList struct {
	Members []xmlListenerDescription `xml:"member"`
}

type xmlHealthCheck struct {
	Target             string `xml:"Target"`
	Interval           int32  `xml:"Interval"`
	Timeout            int32  `xml:"Timeout"`
	UnhealthyThreshold int32  `xml:"UnhealthyThreshold"`
	HealthyThreshold   int32  `xml:"HealthyThreshold"`
}

type xmlLoadBalancerDescription struct {
	HealthCheck               *xmlHealthCheck            `xml:"HealthCheck,omitempty"`
	LoadBalancerName          string                     `xml:"LoadBalancerName"`
	DNSName                   string                     `xml:"DNSName"`
	CanonicalHostedZoneName   string                     `xml:"CanonicalHostedZoneName"`
	CanonicalHostedZoneNameID string                     `xml:"CanonicalHostedZoneNameID"`
	CreatedTime               string                     `xml:"CreatedTime"`
	Scheme                    string                     `xml:"Scheme"`
	AvailabilityZones         xmlStringValueList         `xml:"AvailabilityZones"`
	SecurityGroups            xmlStringValueList         `xml:"SecurityGroups"`
	Subnets                   xmlStringValueList         `xml:"Subnets"`
	ListenerDescriptions      xmlListenerDescriptionList `xml:"ListenerDescriptions"`
	Instances                 xmlInstanceList            `xml:"Instances"`
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
