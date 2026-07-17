package elb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

func (h *Handler) handleSetLoadBalancerPoliciesOfListener(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	port, err := parseInt32(vals.Get("LoadBalancerPort"))
	if err != nil || port == 0 {
		return nil, fmt.Errorf("%w: LoadBalancerPort is required", ErrInvalidParameter)
	}

	policyNames := parseMembers(vals, "PolicyNames.member")

	if setErr := h.Backend.SetLoadBalancerPoliciesOfListener(ctx, name, port, policyNames); setErr != nil {
		return nil, setErr
	}

	return &setLoadBalancerPoliciesOfListenerResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-listpolicies-" + name},
	}, nil
}

func (h *Handler) handleCreateAppCookieStickinessPolicy(ctx context.Context, vals url.Values) (any, error) {
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

	if err := h.Backend.CreateAppCookieStickinessPolicy(ctx, name, policyName, cookieName); err != nil {
		return nil, err
	}

	return &createAppCookieStickinessPolicyResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-appcookie-" + name},
	}, nil
}

func (h *Handler) handleCreateLBCookieStickinessPolicy(ctx context.Context, vals url.Values) (any, error) {
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

	if err := h.Backend.CreateLBCookieStickinessPolicy(ctx, name, policyName, cookieExpiration); err != nil {
		return nil, err
	}

	return &createLBCookieStickinessPolicyResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-lbcookie-" + name},
	}, nil
}

func (h *Handler) handleCreateLoadBalancerPolicy(ctx context.Context, vals url.Values) (any, error) {
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
		return nil, fmt.Errorf("%w: %q", ErrPolicyTypeNotFound, policyTypeName)
	}

	attrs := parsePolicyAttributes(vals)

	if err := h.Backend.CreateLoadBalancerPolicy(ctx, name, policyName, policyTypeName, attrs); err != nil {
		return nil, err
	}

	return &createLoadBalancerPolicyResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-createpolicy-" + name},
	}, nil
}

func (h *Handler) handleDeleteLoadBalancerPolicy(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	policyName := vals.Get("PolicyName")
	if policyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteLoadBalancerPolicy(ctx, name, policyName); err != nil {
		return nil, err
	}

	return &deleteLoadBalancerPolicyResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-deletepolicy-" + name},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancerPolicies(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	policyNames := parseMembers(vals, "PolicyNames.member")

	policies, err := h.Backend.DescribeLoadBalancerPolicies(ctx, name, policyNames)
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

func (h *Handler) handleDescribeLoadBalancerPolicyTypes(ctx context.Context, vals url.Values) (any, error) {
	typeNames := parseMembers(vals, "PolicyTypeNames.member")

	types, err := h.Backend.DescribeLoadBalancerPolicyTypes(ctx, typeNames)
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

// parsePolicyAttributes extracts policy attribute pairs from PolicyAttributes.member.N.* form values.
// Uses gap-tolerant scanning.
func parsePolicyAttributes(vals url.Values) []PolicyAttribute {
	indexes := collectMemberIndexes(vals, "PolicyAttributes.member.")
	result := make([]PolicyAttribute, 0, len(indexes))

	for _, i := range indexes {
		k := vals.Get(fmt.Sprintf("PolicyAttributes.member.%d.AttributeName", i))
		if k != "" {
			result = append(result, PolicyAttribute{
				AttributeName:  k,
				AttributeValue: vals.Get(fmt.Sprintf("PolicyAttributes.member.%d.AttributeValue", i)),
			})
		}
	}

	return result
}

type setLoadBalancerPoliciesOfListenerResult struct{}

type setLoadBalancerPoliciesOfListenerResponse struct {
	XMLName          xml.Name                                `xml:"SetLoadBalancerPoliciesOfListenerResponse"`
	Xmlns            string                                  `xml:"xmlns,attr"`
	Result           setLoadBalancerPoliciesOfListenerResult `xml:"SetLoadBalancerPoliciesOfListenerResult"`
	ResponseMetadata xmlResponseMetadata                     `xml:"ResponseMetadata"`
}

type createAppCookieStickinessPolicyResult struct{}

type createAppCookieStickinessPolicyResponse struct {
	XMLName          xml.Name                              `xml:"CreateAppCookieStickinessPolicyResponse"`
	Xmlns            string                                `xml:"xmlns,attr"`
	Result           createAppCookieStickinessPolicyResult `xml:"CreateAppCookieStickinessPolicyResult"`
	ResponseMetadata xmlResponseMetadata                   `xml:"ResponseMetadata"`
}

type createLBCookieStickinessPolicyResult struct{}

type createLBCookieStickinessPolicyResponse struct {
	XMLName          xml.Name                             `xml:"CreateLBCookieStickinessPolicyResponse"`
	Xmlns            string                               `xml:"xmlns,attr"`
	Result           createLBCookieStickinessPolicyResult `xml:"CreateLBCookieStickinessPolicyResult"`
	ResponseMetadata xmlResponseMetadata                  `xml:"ResponseMetadata"`
}

type createLoadBalancerPolicyResult struct{}

type createLoadBalancerPolicyResponse struct {
	XMLName          xml.Name                       `xml:"CreateLoadBalancerPolicyResponse"`
	Xmlns            string                         `xml:"xmlns,attr"`
	Result           createLoadBalancerPolicyResult `xml:"CreateLoadBalancerPolicyResult"`
	ResponseMetadata xmlResponseMetadata            `xml:"ResponseMetadata"`
}

type deleteLoadBalancerPolicyResult struct{}

type deleteLoadBalancerPolicyResponse struct {
	XMLName          xml.Name                       `xml:"DeleteLoadBalancerPolicyResponse"`
	Xmlns            string                         `xml:"xmlns,attr"`
	Result           deleteLoadBalancerPolicyResult `xml:"DeleteLoadBalancerPolicyResult"`
	ResponseMetadata xmlResponseMetadata            `xml:"ResponseMetadata"`
}

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
