package elbv2

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	elbv2Version      = "2015-12-01"
	elbv2XMLNS        = "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/"
	attrValueFalse    = "false"
	attrValueTrue     = "true"
	actionTypeForward = "forward"

	// TLS cipher suite constants used in SSL policy definitions.
	cipherECDHEECDSAAES128GCM = "ECDHE-ECDSA-AES128-GCM-SHA256"
	cipherECDHERSAAES128GCM   = "ECDHE-RSA-AES128-GCM-SHA256"
	cipherECDHEECDSAAES128SHA = "ECDHE-ECDSA-AES128-SHA256"
	cipherECDHERSAAES128SHA   = "ECDHE-RSA-AES128-SHA256"
	cipherECDHEECDSAAES256GCM = "ECDHE-ECDSA-AES256-GCM-SHA384"
	cipherECDHERSAAES256GCM   = "ECDHE-RSA-AES256-GCM-SHA384"
	cipherECDHEECDSAAES256SHA = "ECDHE-ECDSA-AES256-SHA384"
	cipherECDHERSAAES256SHA   = "ECDHE-RSA-AES256-SHA384"
	cipherECDHERSAAES128SHA1  = "ECDHE-RSA-AES128-SHA"
	cipherTLSAES128GCM        = "TLS_AES_128_GCM_SHA256"
	cipherTLSAES256GCM        = "TLS_AES_256_GCM_SHA384"
	cipherTLSCHACHA20         = "TLS_CHACHA20_POLY1305_SHA256"

	tlsV12 = "TLSv1.2"
	tlsV13 = "TLSv1.3"

	// SSL cipher priority constants.
	cipherPriority2 = 2
	cipherPriority3 = 3
	cipherPriority4 = 4
	cipherPriority5 = 5
	cipherPriority6 = 6
	cipherPriority7 = 7
	cipherPriority8 = 8
	cipherPriority9 = 9
)

// Handler is the Echo HTTP handler for ELBv2 operations.
type Handler struct {
	Backend       StorageBackend
	dispatchTable map[string]dispatchFunc
}

// NewHandler creates a new ELBv2 handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.dispatchTable = h.buildDispatchTable()

	return h
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
	fn, ok := h.dispatchTable[action]
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

	subnets := parseMembers(vals, "Subnets.member")
	sgs := parseMembers(vals, "SecurityGroups.member")
	tagKVs := parseTagKVs(vals)

	subnetMappings := parseSubnetMappings(vals)

	lb, err := h.Backend.CreateLoadBalancer(CreateLoadBalancerInput{
		Name:           name,
		Scheme:         vals.Get("Scheme"),
		Type:           vals.Get("Type"),
		IPAddressType:  vals.Get("IpAddressType"),
		Subnets:        subnets,
		SubnetMappings: subnetMappings,
		SecurityGroups: sgs,
		Tags:           tagKVs,
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

	marker, pageSize := parsePagination(vals)

	startIdx := 0
	if marker != "" {
		for i, lb := range lbs {
			if lb.LoadBalancerArn == marker {
				startIdx = i + 1

				break
			}
		}
	}

	lbs = lbs[startIdx:]

	var nextMarker string
	if len(lbs) > pageSize {
		nextMarker = lbs[pageSize-1].LoadBalancerArn
		lbs = lbs[:pageSize]
	}

	members := make([]xmlLoadBalancer, 0, len(lbs))
	for i := range lbs {
		members = append(members, toXMLLoadBalancer(&lbs[i]))
	}

	return &describeLoadBalancersResponse{
		Xmlns: elbv2XMLNS,
		Result: describeLoadBalancersResult{
			NextMarker:    nextMarker,
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

	attrs := parseKVAttrs(vals, "Attributes.member")

	lb, err := h.Backend.ModifyLoadBalancerAttributes(lbArn, attrs)
	if err != nil {
		return nil, err
	}

	members := sortedLBAttributes(lb.Attributes)

	return &modifyLoadBalancerAttributesResponse{
		Xmlns:            elbv2XMLNS,
		Result:           modifyLoadBalancerAttributesResult{Attributes: xmlLBAttributeList{Members: members}},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-lb-attrs"},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancerAttributes(vals url.Values) (any, error) {
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

	members := make([]xmlLBAttribute, 0, len(lbs[0].Attributes))
	for k, v := range lbs[0].Attributes {
		members = append(members, xmlLBAttribute{Key: k, Value: v})
	}

	sort.Slice(members, func(i, j int) bool { return members[i].Key < members[j].Key })

	return &describeLoadBalancerAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeLoadBalancerAttributesResult{
			Attributes: xmlLBAttributeList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-lb-attrs"},
	}, nil
}

func (h *Handler) handleSetSecurityGroups(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	sgs := parseMembers(vals, "SecurityGroups.member")

	lb, err := h.Backend.SetSecurityGroups(lbArn, sgs)
	if err != nil {
		return nil, err
	}

	members := make([]xmlStringValue, 0, len(lb.SecurityGroups))
	for _, sg := range lb.SecurityGroups {
		members = append(members, xmlStringValue{Value: sg})
	}

	return &setSecurityGroupsResponse{
		Xmlns:            elbv2XMLNS,
		Result:           setSecurityGroupsResult{SecurityGroupIDs: xmlStringList{Members: members}},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-sgs"},
	}, nil
}

func (h *Handler) handleSetSubnets(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	// Prefer SubnetMappings; fall back to plain Subnets.member list.
	mappings := parseSubnetMappings(vals)
	if len(mappings) == 0 {
		for _, s := range parseMembers(vals, "Subnets.member") {
			mappings = append(mappings, SubnetMapping{SubnetID: s})
		}
	}

	lb, err := h.Backend.SetSubnets(lbArn, mappings)
	if err != nil {
		return nil, err
	}

	azMembers := make([]xmlAZMapping, 0, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		azMembers = append(azMembers, xmlAZMapping(az))
	}

	return &setSubnetsResponse{
		Xmlns:            elbv2XMLNS,
		Result:           setSubnetsResult{AvailabilityZones: xmlAZMappingList{Members: azMembers}},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-subnets"},
	}, nil
}

func (h *Handler) handleSetIPAddressType(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	ipType := vals.Get("IpAddressType")
	if ipType == "" {
		return nil, fmt.Errorf("%w: IpAddressType is required", ErrInvalidParameter)
	}

	lb, err := h.Backend.SetIPAddressType(lbArn, ipType)
	if err != nil {
		return nil, err
	}

	return &setIPAddressTypeResponse{
		Xmlns:            elbv2XMLNS,
		Result:           setIPAddressTypeResult{IPAddressType: lb.IPAddressType},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-ip-type"},
	}, nil
}

// --- target group handlers ---

func (h *Handler) handleCreateTargetGroup(vals url.Values) (any, error) {
	name := vals.Get("Name")
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	// Lambda target groups do not require a port.
	targetType := vals.Get("TargetType")

	port, err := parseTGPort(targetType, vals.Get("Port"))
	if err != nil {
		return nil, err
	}

	tagKVs := parseTagKVs(vals)

	hcInterval, err := parseOptionalInt32(vals, "HealthCheckIntervalSeconds")
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckIntervalSeconds", ErrInvalidParameter)
	}

	hcTimeout, err := parseOptionalInt32(vals, "HealthCheckTimeoutSeconds")
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckTimeoutSeconds", ErrInvalidParameter)
	}

	healthyThreshold, err := parseOptionalInt32(vals, "HealthyThresholdCount")
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthyThresholdCount", ErrInvalidParameter)
	}

	unhealthyThreshold, err := parseOptionalInt32(vals, "UnhealthyThresholdCount")
	if err != nil {
		return nil, fmt.Errorf("%w: invalid UnhealthyThresholdCount", ErrInvalidParameter)
	}

	hcEnabledStr := vals.Get("HealthCheckEnabled")
	var hcEnabled bool
	if hcEnabledStr == "" {
		hcEnabled = vals.Get("TargetType") != targetTypeLambda
	} else {
		hcEnabled = hcEnabledStr != attrValueFalse
	}

	tg, createErr := h.Backend.CreateTargetGroup(CreateTargetGroupInput{
		Name:                name,
		Protocol:            vals.Get("Protocol"),
		ProtocolVersion:     vals.Get("ProtocolVersion"),
		Port:                port,
		VpcID:               vals.Get("VpcId"),
		TargetType:          vals.Get("TargetType"),
		Tags:                tagKVs,
		HealthCheckProtocol: vals.Get("HealthCheckProtocol"),
		HealthCheckPort:     vals.Get("HealthCheckPort"),
		HealthCheckPath:     vals.Get("HealthCheckPath"),
		Matcher: Matcher{
			HTTPCode: vals.Get("Matcher.HTTPCode"),
			GrpcCode: vals.Get("Matcher.GrpcCode"),
		},
		HealthCheckIntervalSeconds: hcInterval,
		HealthCheckTimeoutSeconds:  hcTimeout,
		HealthyThresholdCount:      healthyThreshold,
		UnhealthyThresholdCount:    unhealthyThreshold,
		HealthCheckEnabled:         hcEnabled,
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

	marker, pageSize := parsePagination(vals)

	startIdx := 0
	if marker != "" {
		for i, tg := range tgs {
			if tg.TargetGroupArn == marker {
				startIdx = i + 1

				break
			}
		}
	}

	tgs = tgs[startIdx:]

	var nextMarker string
	if len(tgs) > pageSize {
		nextMarker = tgs[pageSize-1].TargetGroupArn
		tgs = tgs[:pageSize]
	}

	members := make([]xmlTargetGroup, 0, len(tgs))
	for i := range tgs {
		members = append(members, toXMLTargetGroup(&tgs[i]))
	}

	return &describeTargetGroupsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTargetGroupsResult{
			NextMarker:   nextMarker,
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

	hcInterval, mErr := parseOptionalInt32(vals, "HealthCheckIntervalSeconds")
	if mErr != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckIntervalSeconds", ErrInvalidParameter)
	}

	hcTimeout, mErr := parseOptionalInt32(vals, "HealthCheckTimeoutSeconds")
	if mErr != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckTimeoutSeconds", ErrInvalidParameter)
	}

	healthyThreshold, mErr := parseOptionalInt32(vals, "HealthyThresholdCount")
	if mErr != nil {
		return nil, fmt.Errorf("%w: invalid HealthyThresholdCount", ErrInvalidParameter)
	}

	unhealthyThreshold, mErr := parseOptionalInt32(vals, "UnhealthyThresholdCount")
	if mErr != nil {
		return nil, fmt.Errorf("%w: invalid UnhealthyThresholdCount", ErrInvalidParameter)
	}

	// HealthCheckEnabled is optional: only update the field when the parameter is present.
	var hcEnabled *bool
	if hce := vals.Get("HealthCheckEnabled"); hce != "" {
		v := hce == attrValueTrue
		hcEnabled = &v
	}

	tg, err := h.Backend.ModifyTargetGroup(ModifyTargetGroupInput{
		TargetGroupArn:      tgArn,
		HealthCheckProtocol: vals.Get("HealthCheckProtocol"),
		HealthCheckPort:     vals.Get("HealthCheckPort"),
		HealthCheckPath:     vals.Get("HealthCheckPath"),
		Matcher: Matcher{
			HTTPCode: vals.Get("Matcher.HTTPCode"),
			GrpcCode: vals.Get("Matcher.GrpcCode"),
		},
		HealthCheckEnabled:         hcEnabled,
		HealthCheckIntervalSeconds: hcInterval,
		HealthCheckTimeoutSeconds:  hcTimeout,
		HealthyThresholdCount:      healthyThreshold,
		UnhealthyThresholdCount:    unhealthyThreshold,
	})
	if err != nil {
		return nil, err
	}

	return &modifyTargetGroupResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyTargetGroupResult{
			TargetGroups: xmlTargetGroupList{
				Members: []xmlTargetGroup{toXMLTargetGroup(tg)},
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

	attrs := parseKVAttrs(vals, "Attributes.member")

	tg, err := h.Backend.ModifyTargetGroupAttributes(tgArn, attrs)
	if err != nil {
		return nil, err
	}

	members := sortedTGAttributes(tg.TargetGroupAttributes)

	return &modifyTargetGroupAttributesResponse{
		Xmlns:            elbv2XMLNS,
		Result:           modifyTargetGroupAttributesResult{Attributes: xmlTGAttributeList{Members: members}},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-tg-attrs"},
	}, nil
}

func (h *Handler) handleDescribeTargetGroupAttributes(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	attrs, err := h.Backend.DescribeTargetGroupAttributes(tgArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTGAttribute, 0, len(attrs))
	for k, v := range attrs {
		members = append(members, xmlTGAttribute{Key: k, Value: v})
	}

	sort.Slice(members, func(i, j int) bool { return members[i].Key < members[j].Key })

	return &describeTargetGroupAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTargetGroupAttributesResult{
			Attributes: xmlTGAttributeList{Members: members},
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

	// When specific targets are requested, include only those targets.
	// Targets that are requested but not registered get state "unused" with
	// reason "Target.NotRegistered", matching real AWS behaviour.
	requestedTargets := parseTargets(vals, "Targets.member")
	if len(requestedTargets) > 0 {
		registeredMap := make(map[string]TargetHealthDescription, len(targets))
		for _, t := range targets {
			registeredMap[t.Target.ID+":"+strconv.Itoa(int(t.Target.Port))] = t
		}

		filtered := make([]TargetHealthDescription, 0, len(requestedTargets))
		for _, rt := range requestedTargets {
			key := rt.ID + ":" + strconv.Itoa(int(rt.Port))
			if registered, ok := registeredMap[key]; ok {
				filtered = append(filtered, registered)
			} else {
				filtered = append(filtered, TargetHealthDescription{
					Target:       rt,
					HealthState:  "unused",
					HealthReason: "Target.NotRegistered",
				})
			}
		}

		targets = filtered
	}

	members := make([]xmlTargetHealthDescription, 0, len(targets))
	for _, t := range targets {
		members = append(members, xmlTargetHealthDescription{
			Target: xmlTargetDescription{ID: t.Target.ID, Port: t.Target.Port},
			TargetHealth: xmlTargetHealth{
				State:  t.HealthState,
				Reason: t.HealthReason,
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
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Port", ErrInvalidParameter)
	}

	if vErr := validatePort(port); vErr != nil {
		return nil, vErr
	}

	protocol := vals.Get("Protocol")
	if protocol == "" {
		return nil, fmt.Errorf("%w: Protocol is required", ErrInvalidParameter)
	}

	actions := parseActions(vals, "DefaultActions.member")
	if len(actions) == 0 {
		return nil, fmt.Errorf("%w: DefaultActions must contain at least one action", ErrInvalidParameter)
	}

	if actErr := validateActionTypes(actions); actErr != nil {
		return nil, actErr
	}

	tagKVs := parseTagKVs(vals)
	certs := parseCerts(vals)

	// Mark first cert as default for HTTPS/TLS listeners.
	if (protocol == protoHTTPS || protocol == protoTLS) && len(certs) > 0 {
		certs[0].IsDefault = true
	}

	var mutualAuth *MutualAuthentication
	if mode := vals.Get("MutualAuthentication.Mode"); mode != "" {
		mutualAuth = &MutualAuthentication{
			Mode:          mode,
			TrustStoreArn: vals.Get("MutualAuthentication.TrustStoreArn"),
			IgnoreClientCertificateExpiration: vals.Get(
				"MutualAuthentication.IgnoreClientCertificateExpiration",
			) == attrValueTrue,
		}
	} else if tsArn := vals.Get("MutualAuthentication.TrustStoreArn"); tsArn != "" {
		mutualAuth = &MutualAuthentication{
			Mode:          "verify",
			TrustStoreArn: tsArn,
		}
	}

	listener, createErr := h.Backend.CreateListener(CreateListenerInput{
		LoadBalancerArn:      lbArn,
		Protocol:             protocol,
		Port:                 port,
		DefaultActions:       actions,
		Tags:                 tagKVs,
		Certificates:         certs,
		SSLPolicy:            vals.Get("SslPolicy"),
		AlpnPolicy:           vals.Get("AlpnPolicy.member.1"),
		MutualAuthentication: mutualAuth,
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

	marker, pageSize := parsePagination(vals)
	listeners, nextMarker := applyListenerPage(listeners, marker, pageSize)

	members := make([]xmlListener, 0, len(listeners))
	for i := range listeners {
		members = append(members, toXMLListener(&listeners[i]))
	}

	return &describeListenersResponse{
		Xmlns: elbv2XMLNS,
		Result: describeListenersResult{
			NextMarker: nextMarker,
			Listeners:  xmlListenerList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-listeners"},
	}, nil
}

// applyListenerPage applies marker-based pagination to a listener slice.
func applyListenerPage(listeners []Listener, marker string, pageSize int) ([]Listener, string) {
	if marker != "" {
		for i, l := range listeners {
			if l.ListenerArn == marker {
				listeners = listeners[i+1:]

				break
			}
		}
	}

	var nextMarker string
	if len(listeners) > pageSize {
		nextMarker = listeners[pageSize-1].ListenerArn
		listeners = listeners[:pageSize]
	}

	return listeners, nextMarker
}

func (h *Handler) handleModifyListener(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	portStr := vals.Get("Port")
	var port int32

	if portStr != "" {
		p, err := parseInt32(portStr)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid Port", ErrInvalidParameter)
		}

		port = p
	}

	var mutualAuth *MutualAuthentication
	if mode := vals.Get("MutualAuthentication.Mode"); mode != "" {
		mutualAuth = &MutualAuthentication{
			Mode:          mode,
			TrustStoreArn: vals.Get("MutualAuthentication.TrustStoreArn"),
			IgnoreClientCertificateExpiration: vals.Get(
				"MutualAuthentication.IgnoreClientCertificateExpiration",
			) == attrValueTrue,
		}
	} else if tsArn := vals.Get("MutualAuthentication.TrustStoreArn"); tsArn != "" {
		mutualAuth = &MutualAuthentication{
			Mode:          "verify",
			TrustStoreArn: tsArn,
		}
	}

	listener, err := h.Backend.ModifyListener(ModifyListenerInput{
		ListenerArn:          listenerArn,
		Protocol:             vals.Get("Protocol"),
		Port:                 port,
		DefaultActions:       parseActions(vals, "DefaultActions.member"),
		Certificates:         parseCerts(vals),
		SSLPolicy:            vals.Get("SslPolicy"),
		AlpnPolicy:           vals.Get("AlpnPolicy.member.1"),
		MutualAuthentication: mutualAuth,
	})
	if err != nil {
		return nil, err
	}

	return &modifyListenerResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyListenerResult{
			Listeners: xmlListenerList{
				Members: []xmlListener{toXMLListener(listener)},
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

	attrs := parseKVAttrs(vals, "Attributes.member")

	listener, err := h.Backend.ModifyListenerAttributes(listenerArn, attrs)
	if err != nil {
		return nil, err
	}

	members := sortedListenerAttributes(listener.Attributes)

	return &modifyListenerAttributesResponse{
		Xmlns:            elbv2XMLNS,
		Result:           modifyListenerAttributesResult{Attributes: xmlListenerAttributeList{Members: members}},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-listener-attrs"},
	}, nil
}

func (h *Handler) handleDescribeListenerAttributes(vals url.Values) (any, error) {
	listenerArn := vals.Get("ListenerArn")
	if listenerArn == "" {
		return nil, fmt.Errorf("%w: ListenerArn is required", ErrInvalidParameter)
	}

	attrs, err := h.Backend.DescribeListenerAttributes(listenerArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlListenerAttribute, 0, len(attrs))
	for k, v := range attrs {
		members = append(members, xmlListenerAttribute{Key: k, Value: v})
	}

	sort.Slice(members, func(i, j int) bool { return members[i].Key < members[j].Key })

	return &describeListenerAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeListenerAttributesResult{
			Attributes: xmlListenerAttributeList{Members: members},
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

	if vals.Get("Priority") == "" {
		return nil, fmt.Errorf("%w: Priority is required", ErrInvalidParameter)
	}

	actions := parseActions(vals, "Actions.member")
	if len(actions) == 0 {
		return nil, fmt.Errorf("%w: Actions must contain at least one action", ErrInvalidParameter)
	}

	if actErr := validateActionTypes(actions); actErr != nil {
		return nil, actErr
	}

	conditions, err := parseConditions(vals, "Conditions.member")
	if err != nil {
		return nil, err
	}

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

	marker, pageSize := parsePagination(vals)
	rules, nextMarker := applyRulePage(rules, marker, pageSize)

	members := make([]xmlRule, 0, len(rules))
	for i := range rules {
		members = append(members, toXMLRule(&rules[i]))
	}

	return &describeRulesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeRulesResult{
			NextMarker: nextMarker,
			Rules:      xmlRuleList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-rules"},
	}, nil
}

// applyRulePage applies marker-based pagination to a rule slice.
func applyRulePage(rules []Rule, marker string, pageSize int) ([]Rule, string) {
	if marker != "" {
		for i, r := range rules {
			if r.RuleArn == marker {
				rules = rules[i+1:]

				break
			}
		}
	}

	var nextMarker string
	if len(rules) > pageSize {
		nextMarker = rules[pageSize-1].RuleArn
		rules = rules[:pageSize]
	}

	return rules, nextMarker
}

func (h *Handler) handleModifyRule(vals url.Values) (any, error) {
	ruleArn := vals.Get("RuleArn")
	if ruleArn == "" {
		return nil, fmt.Errorf("%w: RuleArn is required", ErrInvalidParameter)
	}

	actions := parseActions(vals, "Actions.member")

	conditions, err := parseConditions(vals, "Conditions.member")
	if err != nil {
		return nil, err
	}

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

	revocations := parseTrustStoreRevocations(vals)

	if err := h.Backend.AddTrustStoreRevocations(tsArn, revocations); err != nil {
		return nil, err
	}

	return &addTrustStoreRevocationsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-add-ts-revocations"},
	}, nil
}

// parseTrustStoreRevocations extracts RevocationContents from form values.
// Supports both plain RevocationId fields and S3-structured entries.
// Iteration is capped at 1000 to prevent potential DoS from malicious input.
func parseTrustStoreRevocations(vals url.Values) []TrustStoreRevocation {
	const maxRevocations = 1000
	revocations := make([]TrustStoreRevocation, 0)

	for i := 1; i <= maxRevocations; i++ {
		prefix := fmt.Sprintf("RevocationContents.member.%d.", i)
		// S3-structured entry fields.
		s3Bucket := vals.Get(prefix + "S3Bucket")
		s3Key := vals.Get(prefix + "S3Key")
		revType := vals.Get(prefix + "RevocationType")
		plain := vals.Get(fmt.Sprintf("RevocationContents.member.%d", i))

		if s3Bucket == "" && s3Key == "" && revType == "" && plain == "" {
			break
		}

		if revType == "" {
			revType = "CRL"
		}

		revID := plain
		if revID == "" {
			// S3-format entries have no plain value; assign a unique ID server-side
			// so callers can reference the revocation in RemoveTrustStoreRevocations.
			revID = "s3-" + uuid.New().String()
		}

		revocations = append(revocations, TrustStoreRevocation{
			RevocationID:   revID,
			RevocationType: revType,
		})
	}

	return revocations
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

// parseCerts extracts certificates from indexed form parameters.
func parseCerts(vals url.Values) []Certificate {
	certs := make([]Certificate, 0)
	for i := 1; ; i++ {
		arn := vals.Get(fmt.Sprintf("Certificates.member.%d.CertificateArn", i))
		if arn == "" {
			break
		}

		certs = append(certs, Certificate{CertificateArn: arn})
	}

	return certs
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

	certs := parseCerts(vals)
	if err := h.Backend.AddListenerCertificates(listenerArn, certs); err != nil {
		return nil, err
	}

	members := make([]xmlListenerCertificate, 0, len(certs))
	for _, c := range certs {
		members = append(members, xmlListenerCertificate(c))
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
		members = append(members, xmlListenerCertificate(c))
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
		{Name: "target-group-rules-per-listener", Max: "100"},
		{Name: "condition-values-per-alb-rule", Max: "5"},
		{Name: "condition-wildcards-per-alb-rule", Max: "5"},
		{Name: "target-groups-per-alb-listener-rule", Max: "5"},
		{Name: "target-groups-per-nlb-listener", Max: "1"},
		{Name: "subnets-per-load-balancer", Max: "8"},
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

func (h *Handler) handleDescribeSSLPolicies(vals url.Values) (any, error) {
	allPolicies := allSSLPolicies()

	// Filter by Names if provided.
	names := parseMembers(vals, "Names.member")
	policies := filterSSLPoliciesByName(allPolicies, names)

	return &describeSSLPoliciesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeSSLPoliciesResult{
			SslPolicies: xmlSSLPolicyList{Members: policies},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-ssl-policies"},
	}, nil
}

// filterSSLPoliciesByName returns all policies if names is empty, else only those with matching names.
func filterSSLPoliciesByName(all []xmlSSLPolicy, names []string) []xmlSSLPolicy {
	if len(names) == 0 {
		return all
	}

	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	result := make([]xmlSSLPolicy, 0, len(names))
	for _, p := range all {
		if nameSet[p.Name] {
			result = append(result, p)
		}
	}

	return result
}

// allSSLPolicies returns the full list of supported SSL policies.
func allSSLPolicies() []xmlSSLPolicy {
	return []xmlSSLPolicy{
		sslPolicy201608(),
		sslPolicyTLS1312202106(),
		sslPolicyTLS1313202211(),
		sslPolicyFS12Res202010(),
		sslPolicyFS201806(),
		sslPolicyTLS1312Ext2202106(),
	}
}

func sslPolicy201608() xmlSSLPolicy {
	return xmlSSLPolicy{
		Name: "ELBSecurityPolicy-2016-08",
		Ciphers: xmlCipherList{Members: []xmlCipher{
			{Name: cipherECDHEECDSAAES128GCM, Priority: 1},
			{Name: cipherECDHERSAAES128GCM, Priority: cipherPriority2},
			{Name: cipherECDHEECDSAAES128SHA, Priority: cipherPriority3},
			{Name: cipherECDHERSAAES128SHA, Priority: cipherPriority4},
			{Name: cipherECDHEECDSAAES256GCM, Priority: cipherPriority5},
			{Name: cipherECDHERSAAES256GCM, Priority: cipherPriority6},
			{Name: cipherECDHEECDSAAES256SHA, Priority: cipherPriority7},
			{Name: cipherECDHERSAAES256SHA, Priority: cipherPriority8},
		}},
		SslProtocols: xmlSSLProtocolList{Members: []xmlSSLProtocol{{Value: tlsV12}}},
	}
}

func sslPolicyTLS1312202106() xmlSSLPolicy {
	return xmlSSLPolicy{
		Name: "ELBSecurityPolicy-TLS13-1-2-2021-06",
		Ciphers: xmlCipherList{Members: []xmlCipher{
			{Name: cipherTLSAES128GCM, Priority: 1},
			{Name: cipherTLSAES256GCM, Priority: cipherPriority2},
			{Name: cipherTLSCHACHA20, Priority: cipherPriority3},
			{Name: cipherECDHEECDSAAES128GCM, Priority: cipherPriority4},
			{Name: cipherECDHERSAAES128GCM, Priority: cipherPriority5},
			{Name: cipherECDHEECDSAAES256GCM, Priority: cipherPriority6},
			{Name: cipherECDHERSAAES256GCM, Priority: cipherPriority7},
		}},
		SslProtocols: xmlSSLProtocolList{Members: []xmlSSLProtocol{
			{Value: tlsV13},
			{Value: tlsV12},
		}},
	}
}

func sslPolicyTLS1313202211() xmlSSLPolicy {
	return xmlSSLPolicy{
		Name: "ELBSecurityPolicy-TLS13-1-3-2022-11",
		Ciphers: xmlCipherList{Members: []xmlCipher{
			{Name: cipherTLSAES128GCM, Priority: 1},
			{Name: cipherTLSAES256GCM, Priority: cipherPriority2},
			{Name: cipherTLSCHACHA20, Priority: cipherPriority3},
		}},
		SslProtocols: xmlSSLProtocolList{Members: []xmlSSLProtocol{{Value: tlsV13}}},
	}
}

func sslPolicyFS12Res202010() xmlSSLPolicy {
	return xmlSSLPolicy{
		Name: "ELBSecurityPolicy-FS-1-2-Res-2020-10",
		Ciphers: xmlCipherList{Members: []xmlCipher{
			{Name: cipherECDHEECDSAAES128GCM, Priority: 1},
			{Name: cipherECDHERSAAES128GCM, Priority: cipherPriority2},
			{Name: cipherECDHEECDSAAES256GCM, Priority: cipherPriority3},
			{Name: cipherECDHERSAAES256GCM, Priority: cipherPriority4},
			{Name: cipherECDHEECDSAAES128SHA, Priority: cipherPriority5},
			{Name: cipherECDHERSAAES128SHA, Priority: cipherPriority6},
			{Name: cipherECDHEECDSAAES256SHA, Priority: cipherPriority7},
			{Name: cipherECDHERSAAES256SHA, Priority: cipherPriority8},
		}},
		SslProtocols: xmlSSLProtocolList{Members: []xmlSSLProtocol{{Value: tlsV12}}},
	}
}

func sslPolicyFS201806() xmlSSLPolicy {
	return xmlSSLPolicy{
		Name: "ELBSecurityPolicy-FS-2018-06",
		Ciphers: xmlCipherList{Members: []xmlCipher{
			{Name: cipherECDHEECDSAAES128GCM, Priority: 1},
			{Name: cipherECDHERSAAES128GCM, Priority: cipherPriority2},
			{Name: cipherECDHEECDSAAES256GCM, Priority: cipherPriority3},
			{Name: cipherECDHERSAAES256GCM, Priority: cipherPriority4},
			{Name: cipherECDHEECDSAAES128SHA, Priority: cipherPriority5},
			{Name: cipherECDHERSAAES128SHA, Priority: cipherPriority6},
			{Name: cipherECDHEECDSAAES256SHA, Priority: cipherPriority7},
			{Name: cipherECDHERSAAES256SHA, Priority: cipherPriority8},
			{Name: cipherECDHERSAAES128SHA1, Priority: cipherPriority9},
		}},
		SslProtocols: xmlSSLProtocolList{Members: []xmlSSLProtocol{
			{Value: tlsV12},
			{Value: "TLSv1.1"},
		}},
	}
}

func sslPolicyTLS1312Ext2202106() xmlSSLPolicy {
	return xmlSSLPolicy{
		Name: "ELBSecurityPolicy-TLS13-1-2-Ext2-2021-06",
		Ciphers: xmlCipherList{Members: []xmlCipher{
			{Name: cipherTLSAES128GCM, Priority: 1},
			{Name: cipherTLSAES256GCM, Priority: cipherPriority2},
			{Name: cipherTLSCHACHA20, Priority: cipherPriority3},
			{Name: cipherECDHEECDSAAES128GCM, Priority: cipherPriority4},
			{Name: cipherECDHERSAAES128GCM, Priority: cipherPriority5},
			{Name: cipherECDHEECDSAAES256GCM, Priority: cipherPriority6},
			{Name: cipherECDHERSAAES256GCM, Priority: cipherPriority7},
			{Name: cipherECDHEECDSAAES128SHA, Priority: cipherPriority8},
			{Name: cipherECDHERSAAES128SHA, Priority: cipherPriority9},
		}},
		SslProtocols: xmlSSLProtocolList{Members: []xmlSSLProtocol{
			{Value: tlsV13},
			{Value: tlsV12},
		}},
	}
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
		members = append(members, xmlRevocationContent(r))
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
		{ErrDuplicateListener, "DuplicateListener", http.StatusConflict},
		{ErrDuplicateRulePriority, "DuplicatePriority", http.StatusBadRequest},
		{ErrTargetGroupInUse, "ResourceInUse", http.StatusBadRequest},
		{ErrOperationNotPermitted, "OperationNotPermitted", http.StatusBadRequest},
		{ErrInvalidConfigurationRequest, "InvalidConfigurationRequest", http.StatusBadRequest},
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

// defaultPageSize is the default number of results to return per page, matching AWS defaults.
const defaultPageSize = 400

// parsePagination extracts Marker and PageSize from form values.
// Returns the marker string and the effective page size.
func parsePagination(vals url.Values) (string, int) {
	m := vals.Get("Marker")
	ps := defaultPageSize

	if pageSizeStr := vals.Get("PageSize"); pageSizeStr != "" {
		if n, err := parseInt32(pageSizeStr); err == nil && n > 0 {
			ps = int(n)
		}
	}

	return m, ps
}

// parseTGPort parses the Port form value for a CreateTargetGroup request.
// Lambda target groups do not require a port; all other types do.
func parseTGPort(targetType, portStr string) (int32, error) {
	if targetType == targetTypeLambda {
		if portStr == "" {
			return 0, nil
		}
	} else if portStr == "" {
		return 0, fmt.Errorf("%w: Port is required", ErrInvalidParameter)
	}

	p, err := parseInt32(portStr)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid Port", ErrInvalidParameter)
	}

	if targetType != targetTypeLambda {
		if vErr := validatePort(p); vErr != nil {
			return 0, vErr
		}
	}

	return p, nil
}

// parseOptionalInt32 parses an integer form field, returning an error only when
// the field is present but cannot be parsed. An absent field returns (0, nil).
func parseOptionalInt32(vals url.Values, key string) (int32, error) {
	s := vals.Get(key)
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

// parseKVAttrs extracts key-value attribute pairs from Attributes.member.N.Key/Value form values.
func parseKVAttrs(vals url.Values, prefix string) map[string]string {
	attrs := make(map[string]string)

	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("%s.%d.Key", prefix, i))
		if k == "" {
			break
		}

		attrs[k] = vals.Get(fmt.Sprintf("%s.%d.Value", prefix, i))
	}

	return attrs
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
// parseSubnetMappings extracts SubnetMappings.member.N.* from form values.
func parseSubnetMappings(vals url.Values) []SubnetMapping {
	var out []SubnetMapping

	for i := 1; ; i++ {
		subnetID := vals.Get(fmt.Sprintf("SubnetMappings.member.%d.SubnetId", i))
		if subnetID == "" {
			break
		}

		out = append(out, SubnetMapping{
			SubnetID:           subnetID,
			AllocationID:       vals.Get(fmt.Sprintf("SubnetMappings.member.%d.AllocationId", i)),
			PrivateIPv4Address: vals.Get(fmt.Sprintf("SubnetMappings.member.%d.PrivateIPv4Address", i)),
			IPv6Address:        vals.Get(fmt.Sprintf("SubnetMappings.member.%d.IPv6Address", i)),
		})
	}

	return out
}

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
		p := fmt.Sprintf("%s.%d", prefix, i)
		actionType := vals.Get(p + ".Type")

		if actionType == "" {
			break
		}

		order, _ := parseInt32(vals.Get(p + ".Order"))
		action := Action{
			Type:           actionType,
			TargetGroupArn: vals.Get(p + ".TargetGroupArn"),
			Order:          order,
		}

		applyActionConfig(vals, p, actionType, &action)
		result = append(result, action)
	}

	return result
}

// isValidActionType returns true if the action type is a recognized ELBv2 value.
func isValidActionType(t string) bool {
	switch t {
	case actionTypeForward, "redirect", "fixed-response", "authenticate-cognito", "authenticate-oidc":
		return true
	}

	return false
}

// applyActionConfig populates action-type-specific config fields from form values.
func applyActionConfig(vals url.Values, p, actionType string, action *Action) {
	switch actionType {
	case "redirect":
		action.RedirectConfig = &RedirectConfig{
			Protocol:   vals.Get(p + ".RedirectConfig.Protocol"),
			Port:       vals.Get(p + ".RedirectConfig.Port"),
			Host:       vals.Get(p + ".RedirectConfig.Host"),
			Path:       vals.Get(p + ".RedirectConfig.Path"),
			Query:      vals.Get(p + ".RedirectConfig.Query"),
			StatusCode: vals.Get(p + ".RedirectConfig.StatusCode"),
		}
	case "fixed-response":
		action.FixedResponseConfig = &FixedResponseConfig{
			StatusCode:  vals.Get(p + ".FixedResponseConfig.StatusCode"),
			MessageBody: vals.Get(p + ".FixedResponseConfig.MessageBody"),
			ContentType: vals.Get(p + ".FixedResponseConfig.ContentType"),
		}
	case actionTypeForward:
		tgs := parseForwardConfigTargetGroups(vals, p+".ForwardConfig.TargetGroups.member")
		if len(tgs) > 0 {
			action.ForwardConfig = &ForwardConfig{TargetGroups: tgs}
		}
	case "authenticate-cognito":
		applyAuthCognitoConfig(vals, p, action)
	case "authenticate-oidc":
		applyAuthOidcConfig(vals, p, action)
	}
}

// validateActionTypes returns an error if any action has an unknown type.
func validateActionTypes(actions []Action) error {
	for _, a := range actions {
		if !isValidActionType(a.Type) {
			return fmt.Errorf(
				"%w: invalid action type %q; must be forward, redirect, fixed-response, authenticate-cognito, or authenticate-oidc",
				ErrInvalidParameter,
				a.Type,
			)
		}
	}

	return nil
}

func applyAuthCognitoConfig(vals url.Values, p string, action *Action) {
	action.AuthenticateCognitoConfig = &AuthenticateCognitoConfig{
		UserPoolArn:              vals.Get(p + ".AuthenticateCognitoConfig.UserPoolArn"),
		UserPoolClientID:         vals.Get(p + ".AuthenticateCognitoConfig.UserPoolClientId"),
		UserPoolDomain:           vals.Get(p + ".AuthenticateCognitoConfig.UserPoolDomain"),
		SessionCookieName:        vals.Get(p + ".AuthenticateCognitoConfig.SessionCookieName"),
		Scope:                    vals.Get(p + ".AuthenticateCognitoConfig.Scope"),
		OnUnauthenticatedRequest: vals.Get(p + ".AuthenticateCognitoConfig.OnUnauthenticatedRequest"),
	}

	if st := vals.Get(p + ".AuthenticateCognitoConfig.SessionTimeout"); st != "" {
		n, err := strconv.ParseInt(st, 10, 64)
		if err == nil {
			action.AuthenticateCognitoConfig.SessionTimeout = n
		}
	}
}

func applyAuthOidcConfig(vals url.Values, p string, action *Action) {
	action.AuthenticateOidcConfig = &AuthenticateOidcConfig{
		Issuer:                   vals.Get(p + ".AuthenticateOidcConfig.Issuer"),
		AuthorizationEndpoint:    vals.Get(p + ".AuthenticateOidcConfig.AuthorizationEndpoint"),
		TokenEndpoint:            vals.Get(p + ".AuthenticateOidcConfig.TokenEndpoint"),
		UserInfoEndpoint:         vals.Get(p + ".AuthenticateOidcConfig.UserInfoEndpoint"),
		ClientID:                 vals.Get(p + ".AuthenticateOidcConfig.ClientId"),
		ClientSecret:             vals.Get(p + ".AuthenticateOidcConfig.ClientSecret"),
		SessionCookieName:        vals.Get(p + ".AuthenticateOidcConfig.SessionCookieName"),
		Scope:                    vals.Get(p + ".AuthenticateOidcConfig.Scope"),
		OnUnauthenticatedRequest: vals.Get(p + ".AuthenticateOidcConfig.OnUnauthenticatedRequest"),
	}

	if st := vals.Get(p + ".AuthenticateOidcConfig.SessionTimeout"); st != "" {
		n, err := strconv.ParseInt(st, 10, 64)
		if err == nil {
			action.AuthenticateOidcConfig.SessionTimeout = n
		}
	}
}

// parseForwardConfigTargetGroups extracts weighted target groups from ForwardConfig form values.
func parseForwardConfigTargetGroups(vals url.Values, prefix string) []TargetGroupTuple {
	result := make([]TargetGroupTuple, 0)

	for i := 1; ; i++ {
		tgArn := vals.Get(fmt.Sprintf("%s.%d.TargetGroupArn", prefix, i))
		if tgArn == "" {
			break
		}

		weight, _ := parseInt32(vals.Get(fmt.Sprintf("%s.%d.Weight", prefix, i)))
		result = append(result, TargetGroupTuple{TargetGroupArn: tgArn, Weight: weight})
	}

	return result
}

// allowedHTTPMethods returns the whitelist of allowed HTTP methods for http-request-method conditions.
func allowedHTTPMethods() map[string]bool {
	return map[string]bool{
		"GET":     true,
		"HEAD":    true,
		"POST":    true,
		"PUT":     true,
		"DELETE":  true,
		"OPTIONS": true,
		"PATCH":   true,
	}
}

// parseConditions extracts rule conditions from form values.
// Supported fields: host-header, path-pattern, http-header, http-request-method,
// query-string, source-ip.
func parseConditions(vals url.Values, prefix string) ([]Condition, error) {
	result := make([]Condition, 0)

	for i := 1; ; i++ {
		ok, err := parseConditionAt(vals, prefix, i, &result)
		if err != nil {
			return nil, err
		}

		if !ok {
			break
		}
	}

	return result, nil
}

// parseConditionAt parses a single indexed condition and appends it to result.
// Returns (false, nil) when there are no more conditions to parse.
func parseConditionAt(vals url.Values, prefix string, i int, result *[]Condition) (bool, error) {
	field := vals.Get(fmt.Sprintf("%s.%d.Field", prefix, i))
	if field == "" {
		return false, nil
	}

	cond := Condition{Field: field}

	switch field {
	case "host-header":
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.HostHeaderConfig.Values.member", prefix, i))
	case "path-pattern":
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.PathPatternConfig.Values.member", prefix, i))
	case "http-request-method":
		methods := parseMembers(vals, fmt.Sprintf("%s.%d.HttpRequestMethodConfig.Values.member", prefix, i))
		for _, m := range methods {
			if !allowedHTTPMethods()[strings.ToUpper(m)] {
				return false, fmt.Errorf(
					"%w: invalid HTTP method %q; valid methods are GET, HEAD, POST, PUT, DELETE, OPTIONS, PATCH",
					ErrInvalidParameter, m,
				)
			}
		}

		cond.Values = methods
	case "source-ip":
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.SourceIpConfig.Values.member", prefix, i))
	case "http-header":
		cond.HTTPHeaderName = vals.Get(fmt.Sprintf("%s.%d.HttpHeaderConfig.HttpHeaderName", prefix, i))
		cond.Values = parseMembers(vals, fmt.Sprintf("%s.%d.HttpHeaderConfig.Values.member", prefix, i))
	case "query-string":
		cond.QueryStringPairs = parseQueryStringPairs(vals, prefix, i)
	}

	*result = append(*result, cond)

	return true, nil
}

// parseQueryStringPairs extracts query-string key/value pairs for the Nth condition.
func parseQueryStringPairs(vals url.Values, prefix string, condIdx int) []QueryStringPair {
	pairs := make([]QueryStringPair, 0)
	j := 1

	for parseQueryStringPairAt(vals, prefix, condIdx, j, &pairs) {
		j++
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
		azs = append(azs, xmlAZMapping(az))
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
	xtg := xmlTargetGroup{
		TargetGroupArn:             tg.TargetGroupArn,
		TargetGroupName:            tg.TargetGroupName,
		Protocol:                   tg.Protocol,
		ProtocolVersion:            tg.ProtocolVersion,
		Port:                       tg.Port,
		VpcID:                      tg.VpcID,
		TargetType:                 tg.TargetType,
		HealthCheckProtocol:        tg.HealthCheckProtocol,
		HealthCheckPort:            tg.HealthCheckPort,
		HealthCheckPath:            tg.HealthCheckPath,
		HealthCheckEnabled:         tg.HealthCheckEnabled,
		HealthCheckIntervalSeconds: tg.HealthCheckIntervalSeconds,
		HealthCheckTimeoutSeconds:  tg.HealthCheckTimeoutSeconds,
		HealthyThresholdCount:      tg.HealthyThresholdCount,
		UnhealthyThresholdCount:    tg.UnhealthyThresholdCount,
		CrossZoneLoadBalancing:     tg.CrossZoneLoadBalancing,
	}

	if tg.Matcher.HTTPCode != "" || tg.Matcher.GrpcCode != "" {
		xtg.Matcher = &xmlMatcher{
			HTTPCode: tg.Matcher.HTTPCode,
			GrpcCode: tg.Matcher.GrpcCode,
		}
	} else if tg.HealthCheckProtocol == "HTTP" || tg.HealthCheckProtocol == "HTTPS" {
		// Default matcher for HTTP/HTTPS health checks.
		xtg.Matcher = &xmlMatcher{HTTPCode: "200"}
	}

	if len(tg.LoadBalancerArns) > 0 {
		lbArns := make([]xmlStringValue, 0, len(tg.LoadBalancerArns))
		for _, a := range tg.LoadBalancerArns {
			lbArns = append(lbArns, xmlStringValue{Value: a})
		}

		xtg.LoadBalancerArns = &xmlStringList{Members: lbArns}
	}

	return xtg
}

func toXMLAction(a Action) xmlAction {
	xa := xmlAction{
		Type:           a.Type,
		TargetGroupArn: a.TargetGroupArn,
		Order:          a.Order,
	}

	if a.RedirectConfig != nil {
		xa.RedirectConfig = &xmlRedirectConfig{
			Protocol:   a.RedirectConfig.Protocol,
			Port:       a.RedirectConfig.Port,
			Host:       a.RedirectConfig.Host,
			Path:       a.RedirectConfig.Path,
			Query:      a.RedirectConfig.Query,
			StatusCode: a.RedirectConfig.StatusCode,
		}
	}

	if a.FixedResponseConfig != nil {
		xa.FixedResponseConfig = &xmlFixedResponseConfig{
			StatusCode:  a.FixedResponseConfig.StatusCode,
			MessageBody: a.FixedResponseConfig.MessageBody,
			ContentType: a.FixedResponseConfig.ContentType,
		}
	}

	if a.ForwardConfig != nil {
		tuples := make([]xmlTargetGroupTuple, 0, len(a.ForwardConfig.TargetGroups))
		for _, tgt := range a.ForwardConfig.TargetGroups {
			tuples = append(tuples, xmlTargetGroupTuple(tgt))
		}

		xa.ForwardConfig = &xmlForwardConfig{
			TargetGroups: xmlTargetGroupTupleList{Members: tuples},
		}
	} else if a.Type == actionTypeForward && a.TargetGroupArn != "" {
		xa.ForwardConfig = &xmlForwardConfig{
			TargetGroups: xmlTargetGroupTupleList{Members: []xmlTargetGroupTuple{
				{TargetGroupArn: a.TargetGroupArn, Weight: 1},
			}},
		}
	}

	if a.AuthenticateCognitoConfig != nil {
		xa.AuthenticateCognitoConfig = &xmlAuthenticateCognitoConfig{
			UserPoolArn:              a.AuthenticateCognitoConfig.UserPoolArn,
			UserPoolClientID:         a.AuthenticateCognitoConfig.UserPoolClientID,
			UserPoolDomain:           a.AuthenticateCognitoConfig.UserPoolDomain,
			SessionCookieName:        a.AuthenticateCognitoConfig.SessionCookieName,
			Scope:                    a.AuthenticateCognitoConfig.Scope,
			OnUnauthenticatedRequest: a.AuthenticateCognitoConfig.OnUnauthenticatedRequest,
			SessionTimeout:           a.AuthenticateCognitoConfig.SessionTimeout,
		}
	}

	if a.AuthenticateOidcConfig != nil {
		xa.AuthenticateOidcConfig = &xmlAuthenticateOidcConfig{
			Issuer:                   a.AuthenticateOidcConfig.Issuer,
			AuthorizationEndpoint:    a.AuthenticateOidcConfig.AuthorizationEndpoint,
			TokenEndpoint:            a.AuthenticateOidcConfig.TokenEndpoint,
			UserInfoEndpoint:         a.AuthenticateOidcConfig.UserInfoEndpoint,
			ClientID:                 a.AuthenticateOidcConfig.ClientID,
			SessionCookieName:        a.AuthenticateOidcConfig.SessionCookieName,
			Scope:                    a.AuthenticateOidcConfig.Scope,
			OnUnauthenticatedRequest: a.AuthenticateOidcConfig.OnUnauthenticatedRequest,
			SessionTimeout:           a.AuthenticateOidcConfig.SessionTimeout,
		}
	}

	return xa
}

func toXMLListener(l *Listener) xmlListener {
	actions := make([]xmlAction, 0, len(l.DefaultActions))
	for _, a := range l.DefaultActions {
		actions = append(actions, toXMLAction(a))
	}

	xl := xmlListener{
		ListenerArn:     l.ListenerArn,
		LoadBalancerArn: l.LoadBalancerArn,
		Protocol:        l.Protocol,
		Port:            l.Port,
		DefaultActions:  xmlActionList{Members: actions},
		SslPolicy:       l.SSLPolicy,
		AlpnPolicy:      l.AlpnPolicy,
	}

	if l.MutualAuthentication != nil {
		xl.MutualAuthentication = &xmlMutualAuthentication{
			Mode:                              l.MutualAuthentication.Mode,
			TrustStoreArn:                     l.MutualAuthentication.TrustStoreArn,
			IgnoreClientCertificateExpiration: l.MutualAuthentication.IgnoreClientCertificateExpiration,
		}
	}

	if len(l.Certificates) > 0 {
		certs := make([]xmlListenerCertificate, 0, len(l.Certificates))
		for _, c := range l.Certificates {
			certs = append(certs, xmlListenerCertificate(c))
		}

		xl.Certificates = &xmlListenerCertificateList{Members: certs}
	}

	return xl
}

// toStringValuesConfig converts a slice of strings into an xmlConditionValuesConfig pointer.
func toStringValuesConfig(values []string) *xmlConditionValuesConfig {
	members := make([]xmlStringValue, 0, len(values))
	for _, v := range values {
		members = append(members, xmlStringValue{Value: v})
	}

	return &xmlConditionValuesConfig{Values: xmlStringList{Members: members}}
}

// buildXMLCondition converts a single backend Condition into its XML representation.
func buildXMLCondition(c Condition) xmlCondition {
	xc := xmlCondition{Field: c.Field}

	switch c.Field {
	case "host-header":
		xc.HostHeaderConfig = toStringValuesConfig(c.Values)
	case "path-pattern":
		xc.PathPatternConfig = toStringValuesConfig(c.Values)
	case "http-request-method":
		xc.HTTPRequestMethodConfig = toStringValuesConfig(c.Values)
	case "source-ip":
		xc.SourceIPConfig = toStringValuesConfig(c.Values)
	case "http-header":
		xc.HTTPHeaderConfig = &xmlHTTPHeaderConfig{
			HTTPHeaderName: c.HTTPHeaderName,
			Values:         toStringValuesConfig(c.Values).Values,
		}
	case "query-string":
		pairs := make([]xmlQueryStringKeyValue, 0, len(c.QueryStringPairs))
		for _, p := range c.QueryStringPairs {
			pairs = append(pairs, xmlQueryStringKeyValue(p))
		}
		xc.QueryStringConfig = &xmlQueryStringConfig{Values: xmlQueryStringList{Members: pairs}}
	}

	return xc
}

func toXMLRule(r *Rule) xmlRule {
	actions := make([]xmlAction, 0, len(r.Actions))
	for _, a := range r.Actions {
		actions = append(actions, toXMLAction(a))
	}

	conds := make([]xmlCondition, 0, len(r.Conditions))
	for _, c := range r.Conditions {
		conds = append(conds, buildXMLCondition(c))
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
	AvailabilityZones xmlAZMappingList `xml:"AvailabilityZones"`
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
	Matcher                    *xmlMatcher    `xml:"Matcher,omitempty"`
	LoadBalancerArns           *xmlStringList `xml:"LoadBalancerArns,omitempty"`
	TargetGroupArn             string         `xml:"TargetGroupArn"`
	TargetGroupName            string         `xml:"TargetGroupName"`
	Protocol                   string         `xml:"Protocol"`
	ProtocolVersion            string         `xml:"ProtocolVersion,omitempty"`
	VpcID                      string         `xml:"VpcId,omitempty"`
	TargetType                 string         `xml:"TargetType"`
	HealthCheckProtocol        string         `xml:"HealthCheckProtocol"`
	HealthCheckPort            string         `xml:"HealthCheckPort"`
	HealthCheckPath            string         `xml:"HealthCheckPath,omitempty"`
	Port                       int32          `xml:"Port,omitempty"`
	HealthCheckIntervalSeconds int32          `xml:"HealthCheckIntervalSeconds,omitempty"`
	HealthCheckTimeoutSeconds  int32          `xml:"HealthCheckTimeoutSeconds,omitempty"`
	HealthyThresholdCount      int32          `xml:"HealthyThresholdCount,omitempty"`
	UnhealthyThresholdCount    int32          `xml:"UnhealthyThresholdCount,omitempty"`
	HealthCheckEnabled         bool           `xml:"HealthCheckEnabled"`
	CrossZoneLoadBalancing     bool           `xml:"CrossZoneLoadBalancing"`
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
	Result           struct{}            `xml:"RegisterTargetsResult"`
	XMLName          xml.Name            `xml:"RegisterTargetsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deregisterTargetsResponse struct {
	Result           struct{}            `xml:"DeregisterTargetsResult"`
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

// xmlRedirectConfig serialises RedirectConfig for XML responses.
type xmlRedirectConfig struct {
	Protocol   string `xml:"Protocol,omitempty"`
	Port       string `xml:"Port,omitempty"`
	Host       string `xml:"Host,omitempty"`
	Path       string `xml:"Path,omitempty"`
	Query      string `xml:"Query,omitempty"`
	StatusCode string `xml:"StatusCode"`
}

// xmlFixedResponseConfig serialises FixedResponseConfig for XML responses.
type xmlFixedResponseConfig struct {
	StatusCode  string `xml:"StatusCode"`
	MessageBody string `xml:"MessageBody,omitempty"`
	ContentType string `xml:"ContentType,omitempty"`
}

// xmlTargetGroupTuple serialises a weighted target group tuple.
type xmlTargetGroupTuple struct {
	TargetGroupArn string `xml:"TargetGroupArn"`
	Weight         int32  `xml:"Weight,omitempty"`
}

// xmlTargetGroupTupleList is a list of xmlTargetGroupTuple.
type xmlTargetGroupTupleList struct {
	Members []xmlTargetGroupTuple `xml:"member"`
}

// xmlForwardConfig serialises ForwardConfig for XML responses.
type xmlForwardConfig struct {
	TargetGroups xmlTargetGroupTupleList `xml:"TargetGroups"`
}

// xmlAuthenticateCognitoConfig serialises AuthenticateCognitoConfig.
type xmlAuthenticateCognitoConfig struct {
	UserPoolArn              string `xml:"UserPoolArn"`
	UserPoolClientID         string `xml:"UserPoolClientId"`
	UserPoolDomain           string `xml:"UserPoolDomain"`
	SessionCookieName        string `xml:"SessionCookieName,omitempty"`
	Scope                    string `xml:"Scope,omitempty"`
	OnUnauthenticatedRequest string `xml:"OnUnauthenticatedRequest,omitempty"`
	SessionTimeout           int64  `xml:"SessionTimeout,omitempty"`
}

// xmlAuthenticateOidcConfig serialises AuthenticateOidcConfig.
type xmlAuthenticateOidcConfig struct {
	Issuer                   string `xml:"Issuer"`
	AuthorizationEndpoint    string `xml:"AuthorizationEndpoint"`
	TokenEndpoint            string `xml:"TokenEndpoint"`
	UserInfoEndpoint         string `xml:"UserInfoEndpoint"`
	ClientID                 string `xml:"ClientId"`
	SessionCookieName        string `xml:"SessionCookieName,omitempty"`
	Scope                    string `xml:"Scope,omitempty"`
	OnUnauthenticatedRequest string `xml:"OnUnauthenticatedRequest,omitempty"`
	SessionTimeout           int64  `xml:"SessionTimeout,omitempty"`
}

// xmlMutualAuthentication serialises MutualAuthentication for XML responses.
type xmlMutualAuthentication struct {
	TrustStoreArn                     string `xml:"TrustStoreArn,omitempty"`
	Mode                              string `xml:"Mode"`
	IgnoreClientCertificateExpiration bool   `xml:"IgnoreClientCertificateExpiration,omitempty"`
}

// xmlMatcher serialises Matcher for XML responses.
type xmlMatcher struct {
	HTTPCode string `xml:"HTTPCode,omitempty"`
	GrpcCode string `xml:"GrpcCode,omitempty"`
}

type xmlAction struct {
	RedirectConfig            *xmlRedirectConfig            `xml:"RedirectConfig,omitempty"`
	FixedResponseConfig       *xmlFixedResponseConfig       `xml:"FixedResponseConfig,omitempty"`
	ForwardConfig             *xmlForwardConfig             `xml:"ForwardConfig,omitempty"`
	AuthenticateCognitoConfig *xmlAuthenticateCognitoConfig `xml:"AuthenticateCognitoConfig,omitempty"`
	AuthenticateOidcConfig    *xmlAuthenticateOidcConfig    `xml:"AuthenticateOidcConfig,omitempty"`
	Type                      string                        `xml:"Type"`
	TargetGroupArn            string                        `xml:"TargetGroupArn,omitempty"`
	Order                     int32                         `xml:"Order,omitempty"`
}

type xmlActionList struct {
	Members []xmlAction `xml:"member"`
}

type xmlListener struct {
	MutualAuthentication *xmlMutualAuthentication    `xml:"MutualAuthentication,omitempty"`
	Certificates         *xmlListenerCertificateList `xml:"Certificates,omitempty"`
	ListenerArn          string                      `xml:"ListenerArn"`
	LoadBalancerArn      string                      `xml:"LoadBalancerArn"`
	Protocol             string                      `xml:"Protocol"`
	SslPolicy            string                      `xml:"SslPolicy,omitempty"`
	AlpnPolicy           string                      `xml:"AlpnPolicy,omitempty"`
	DefaultActions       xmlActionList               `xml:"DefaultActions"`
	Port                 int32                       `xml:"Port"`
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

type xmlHTTPHeaderConfig struct {
	HTTPHeaderName string        `xml:"HttpHeaderName"`
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
	HostHeaderConfig        *xmlConditionValuesConfig `xml:"HostHeaderConfig,omitempty"`
	PathPatternConfig       *xmlConditionValuesConfig `xml:"PathPatternConfig,omitempty"`
	HTTPHeaderConfig        *xmlHTTPHeaderConfig      `xml:"HttpHeaderConfig,omitempty"`
	HTTPRequestMethodConfig *xmlConditionValuesConfig `xml:"HttpRequestMethodConfig,omitempty"`
	QueryStringConfig       *xmlQueryStringConfig     `xml:"QueryStringConfig,omitempty"`
	SourceIPConfig          *xmlConditionValuesConfig `xml:"SourceIpConfig,omitempty"`
	Field                   string                    `xml:"Field"`
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
	Result           struct{}            `xml:"AddTagsResult"`
	XMLName          xml.Name            `xml:"AddTagsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type removeTagsResponse struct {
	Result           struct{}            `xml:"RemoveTagsResult"`
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
	RevocationID           string `xml:"RevocationId"`
	RevocationType         string `xml:"RevocationType,omitempty"`
	NumberOfRevokedEntries int64  `xml:"NumberOfRevokedEntries,omitempty"`
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

// sortedLBAttributes converts a map to a sorted xmlLBAttribute slice.
func sortedLBAttributes(m map[string]string) []xmlLBAttribute {
	out := make([]xmlLBAttribute, 0, len(m))
	for k, v := range m {
		out = append(out, xmlLBAttribute{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}

// sortedTGAttributes converts a map to a sorted xmlTGAttribute slice.
func sortedTGAttributes(m map[string]string) []xmlTGAttribute {
	out := make([]xmlTGAttribute, 0, len(m))
	for k, v := range m {
		out = append(out, xmlTGAttribute{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}

// sortedListenerAttributes converts a map to a sorted xmlListenerAttribute slice.
func sortedListenerAttributes(m map[string]string) []xmlListenerAttribute {
	out := make([]xmlListenerAttribute, 0, len(m))
	for k, v := range m {
		out = append(out, xmlListenerAttribute{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}
