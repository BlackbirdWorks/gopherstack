package elb

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	elbVersion = "2012-06-01"

	elbXMLNS = "http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/"

	boolStrTrue = "true"

	boolStrFalse = "false"

	unknownOp = "Unknown"
)

// Handler is the Echo HTTP handler for Classic ELB operations.
type Handler struct {
	Backend StorageBackend
	// ops is the pre-built dispatch table mapping action names to handler functions.
	ops map[string]func(context.Context, url.Values) (any, error)
}

// NewHandler creates a new ELB handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// buildOps returns the action-to-handler dispatch table.
func (h *Handler) buildOps() map[string]func(context.Context, url.Values) (any, error) {
	return map[string]func(context.Context, url.Values) (any, error){
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
func (h *Handler) ChaosRegions() []string {
	if h.Backend == nil {
		return []string{config.DefaultRegion}
	}

	if ib, ok := h.Backend.(*InMemoryBackend); ok {
		return []string{ib.Region()}
	}

	return []string{config.DefaultRegion}
}

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
			// Body unreadable (e.g. oversized): fall back to the User-Agent
			// marker every aws-sdk-go-v2 elasticloadbalancing client sets
			// (api_client.go's AddSDKAgentKeyValue -- "api/elasticloadbalancing").
			// That still identifies this as ours, so claim it and let
			// Handler() produce the typed error instead of masking the
			// read failure as a 404.
			return service.MatchesUserAgentMarker(r.Header, "api/elasticloadbalancing")
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
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return unknownOp
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return unknownOp
	}

	action := vals.Get("Action")
	if action == "" {
		return unknownOp
	}

	return action
}

// ExtractResource extracts the load balancer name from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}

	return vals.Get("LoadBalancerName")
}

// Handler returns the Echo handler function for ELB operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		body, err := httputils.ReadBody(r)
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to parse request body")
		}

		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		ctx := h.contextWithRegion(c)

		logger.Load(ctx).Debug("elb request", "action", action)

		resp, opErr := h.dispatch(ctx, action, vals)
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

// contextWithRegion returns the request context with the resolved AWS region
// attached under regionContextKey so that backend operations are routed to the
// correct region. The region is extracted from the request's SigV4
// Authorization header (or X-Amz headers), falling back to the backend's
// default region.
func (h *Handler) contextWithRegion(c *echo.Context) context.Context {
	region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

	return context.WithValue(c.Request().Context(), regionContextKey{}, region)
}

// dispatch routes the ELB action to the appropriate handler.
func (h *Handler) dispatch(ctx context.Context, action string, vals url.Values) (any, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownAction, action)
	}

	return fn(ctx, vals)
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
		{ErrPolicyTypeNotFound, "PolicyTypeNotFound", http.StatusBadRequest},
		{ErrPolicyNotFound, "PolicyNotFound", http.StatusBadRequest},
		{ErrPolicyAlreadyExists, "DuplicatePolicyName", http.StatusBadRequest},
		{ErrDuplicateListener, "DuplicateListener", http.StatusBadRequest},
		{ErrListenerNotFound, "ListenerNotFound", http.StatusBadRequest},
		{ErrInvalidInstance, "InvalidInstance", http.StatusBadRequest},
		{ErrLoadBalancerNotFound, "LoadBalancerNotFound", http.StatusBadRequest},
		{ErrLoadBalancerAlreadyExists, "DuplicateLoadBalancerName", http.StatusBadRequest},
		{ErrUnknownAction, "InvalidAction", http.StatusBadRequest},
		{ErrInvalidConfiguration, "InvalidConfigurationRequest", http.StatusBadRequest},
		{ErrTooManyLoadBalancers, "TooManyLoadBalancers", http.StatusBadRequest},
		{ErrTooManyTags, "TooManyTags", http.StatusBadRequest},
		{ErrDuplicateTagKeys, "DuplicateTagKeys", http.StatusBadRequest},
		{ErrInvalidScheme, "InvalidScheme", http.StatusBadRequest},
		{ErrUnsupportedProtocol, "UnsupportedProtocol", http.StatusBadRequest},
		{ErrInvalidSecurityGroup, "InvalidSecurityGroup", http.StatusBadRequest},
		{ErrSubnetNotFound, "SubnetNotFound", http.StatusBadRequest},
		{ErrCertificateNotFound, "CertificateNotFound", http.StatusBadRequest},
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

// collectMemberIndexes returns the sorted list of member indexes present in vals
// for keys starting with the given dotPrefix (e.g. "AvailabilityZones.member.").
func collectMemberIndexes(vals url.Values, dotPrefix string) []int {
	seen := make(map[int]struct{})

	for k := range vals {
		if !strings.HasPrefix(k, dotPrefix) {
			continue
		}

		rest := k[len(dotPrefix):]

		idxStr, _, _ := strings.Cut(rest, ".")

		if n, err := strconv.Atoi(idxStr); err == nil && n > 0 {
			seen[n] = struct{}{}
		}
	}

	indexes := make([]int, 0, len(seen))
	for i := range seen {
		indexes = append(indexes, i)
	}

	sort.Ints(indexes)

	return indexes
}

// parseMembers extracts indexed form values (e.g. "LoadBalancerNames.member.1").
// It collects all present indexes rather than stopping at the first gap, matching AWS behavior.
func parseMembers(vals url.Values, prefix string) []string {
	indexes := collectMemberIndexes(vals, prefix+".")
	result := make([]string, 0, len(indexes))

	for _, i := range indexes {
		result = append(result, vals.Get(fmt.Sprintf("%s.%d", prefix, i)))
	}

	return result
}

// xmlNamedList is the XML result wrapper shared by ELB operations that report a
// single named list of strings after mutating a load-balancer subresource
// (availability zones, subnets). AWS gives every such operation's result element,
// and the list element nested inside it, their own name (e.g.
// "EnableAvailabilityZonesForLoadBalancerResult" wrapping "AvailabilityZones", vs.
// "AttachLoadBalancerToSubnetsResult" wrapping "Subnets") -- names a single
// struct's tags can't parameterize, so MarshalXML sets them at runtime instead of
// relying on static `xml:"..."` tags.
type xmlNamedList struct {
	ResultElem string
	ListElem   string
	Members    []xmlStringValue
}

// MarshalXML implements xml.Marshaler.
func (r xmlNamedList) MarshalXML(e *xml.Encoder, _ xml.StartElement) error {
	start := xml.StartElement{Name: xml.Name{Local: r.ResultElem}}
	if err := e.EncodeToken(start); err != nil {
		return err
	}

	listElem := xml.StartElement{Name: xml.Name{Local: r.ListElem}}
	if err := e.EncodeElement(xmlStringValueList{Members: r.Members}, listElem); err != nil {
		return err
	}

	return e.EncodeToken(start.End())
}

// xmlNamedListResponse is the response envelope for the same family of
// operations as xmlNamedList; its root element name likewise varies per
// operation and is set at runtime rather than via a struct tag.
type xmlNamedListResponse struct {
	RootElem         string
	ResponseMetadata xmlResponseMetadata
	Result           xmlNamedList
}

// MarshalXML implements xml.Marshaler.
func (r xmlNamedListResponse) MarshalXML(e *xml.Encoder, _ xml.StartElement) error {
	start := xml.StartElement{
		Name: xml.Name{Local: r.RootElem},
		Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: elbXMLNS}},
	}
	if err := e.EncodeToken(start); err != nil {
		return err
	}

	meta := xml.StartElement{Name: xml.Name{Local: "ResponseMetadata"}}
	if err := e.EncodeElement(r.ResponseMetadata, meta); err != nil {
		return err
	}

	if err := e.Encode(r.Result); err != nil {
		return err
	}

	return e.EncodeToken(start.End())
}

// handleStringListOp is the shared implementation behind the ELB operations that
// mutate a single string-list attribute of a load balancer (attaching/detaching
// subnets, enabling/disabling availability zones): parse the LoadBalancerName and a
// repeated member-list parameter, invoke the backend mutator, and wrap the result in
// the shared named-list response envelope.
func (h *Handler) handleStringListOp(
	ctx context.Context,
	vals url.Values,
	memberPrefix, requestIDPrefix, rootElem, resultElem, listElem string,
	mutate func(ctx context.Context, name string, values []string) ([]string, error),
) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	values := parseMembers(vals, memberPrefix)

	result, err := mutate(ctx, name, values)
	if err != nil {
		return nil, err
	}

	members := make([]xmlStringValue, 0, len(result))
	for _, v := range result {
		members = append(members, xmlStringValue{Value: v})
	}

	return &xmlNamedListResponse{
		RootElem:         rootElem,
		ResponseMetadata: xmlResponseMetadata{RequestID: requestIDPrefix + name},
		Result:           xmlNamedList{ResultElem: resultElem, ListElem: listElem, Members: members},
	}, nil
}

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

type xmlResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

type xmlStringValue struct {
	Value string `xml:",chardata"`
}

type xmlStringValueList struct {
	Members []xmlStringValue `xml:"member"`
}
