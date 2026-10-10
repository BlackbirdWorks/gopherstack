package elbv2

import (
	"encoding/xml"
	"net/url"
	"slices"
)

func (h *Handler) handleDescribeSSLPolicies(vals url.Values) (any, error) {
	allPolicies := allSSLPolicies()

	if lbType := vals.Get("LoadBalancerType"); lbType != "" {
		allPolicies = filterSSLPoliciesByType(allPolicies, lbType)
	}

	// Filter by Names if provided.
	names := parseMembers(vals, "Names.member")
	marker, pageSize := parsePagination(vals)
	policies, nextMarker := applyMarkerPage(
		filterSSLPoliciesByName(allPolicies, names), marker, pageSize, func(p xmlSSLPolicy) string { return p.Name },
	)

	return &describeSSLPoliciesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeSSLPoliciesResult{
			SslPolicies: xmlSSLPolicyList{Members: policies},
			NextMarker:  nextMarker,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-ssl-policies"},
	}, nil
}

// filterSSLPoliciesByType keeps the policies whose SupportedLoadBalancerTypes include lbType.
func filterSSLPoliciesByType(all []xmlSSLPolicy, lbType string) []xmlSSLPolicy {
	out := make([]xmlSSLPolicy, 0, len(all))

	for _, p := range all {
		if slices.ContainsFunc(
			p.SupportedLoadBalancerTypes.Members,
			func(t xmlSSLProtocol) bool { return t.Value == lbType },
		) {
			out = append(out, p)
		}
	}

	return out
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

// allSSLPolicies returns the predefined security policies.
func allSSLPolicies() []xmlSSLPolicy {
	groups := sslCipherGroups()
	specs := sslPolicySpecs()
	out := make([]xmlSSLPolicy, 0, len(specs))

	for _, s := range specs {
		ciphers := make([]xmlCipher, 0, len(groups[s.ciphers]))
		for i, name := range groups[s.ciphers] {
			ciphers = append(ciphers, xmlCipher{Name: name, Priority: i + 1})
		}

		protocols := make([]xmlSSLProtocol, 0, len(s.protocols))
		for _, p := range s.protocols {
			protocols = append(protocols, xmlSSLProtocol{Value: p})
		}

		types := make([]xmlSSLProtocol, 0, len(s.types))
		for _, t := range s.types {
			types = append(types, xmlSSLProtocol{Value: t})
		}

		out = append(out, xmlSSLPolicy{
			Name:                       s.name,
			Ciphers:                    xmlCipherList{Members: ciphers},
			SslProtocols:               xmlSSLProtocolList{Members: protocols},
			SupportedLoadBalancerTypes: xmlSSLProtocolList{Members: types},
		})
	}

	return out
}

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

	SupportedLoadBalancerTypes xmlSSLProtocolList `xml:"SupportedLoadBalancerTypes"`
}

type xmlSSLPolicyList struct {
	Members []xmlSSLPolicy `xml:"member"`
}

type describeSSLPoliciesResult struct {
	NextMarker  string           `xml:"NextMarker,omitempty"`
	SslPolicies xmlSSLPolicyList `xml:"SslPolicies"`
}

type describeSSLPoliciesResponse struct {
	XMLName          xml.Name                  `xml:"DescribeSSLPoliciesResponse"`
	Xmlns            string                    `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata       `xml:"ResponseMetadata"`
	Result           describeSSLPoliciesResult `xml:"DescribeSSLPoliciesResult"`
}
