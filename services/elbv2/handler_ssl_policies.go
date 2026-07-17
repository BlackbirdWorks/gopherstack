package elbv2

import (
	"encoding/xml"
	"net/url"
)

const (
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
