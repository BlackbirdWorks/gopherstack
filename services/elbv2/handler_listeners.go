package elbv2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"sort"
	"strconv"
)

const actionTypeForward = "forward"

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
		AlpnPolicy:           parseMembers(vals, "AlpnPolicy.member"),
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
		AlpnPolicy:           parseMembers(vals, "AlpnPolicy.member"),
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

	if len(l.AlpnPolicy) > 0 {
		members := make([]xmlStringValue, 0, len(l.AlpnPolicy))
		for _, p := range l.AlpnPolicy {
			members = append(members, xmlStringValue{Value: p})
		}

		xl.AlpnPolicy = &xmlStringList{Members: members}
	}

	return xl
}

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
	AlpnPolicy           *xmlStringList              `xml:"AlpnPolicy,omitempty"`
	ListenerArn          string                      `xml:"ListenerArn"`
	LoadBalancerArn      string                      `xml:"LoadBalancerArn"`
	Protocol             string                      `xml:"Protocol"`
	SslPolicy            string                      `xml:"SslPolicy,omitempty"`
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

// sortedListenerAttributes converts a map to a sorted xmlListenerAttribute slice.
func sortedListenerAttributes(m map[string]string) []xmlListenerAttribute {
	out := make([]xmlListenerAttribute, 0, len(m))
	for k, v := range m {
		out = append(out, xmlListenerAttribute{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}
