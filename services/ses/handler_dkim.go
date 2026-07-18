package ses

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleGetIdentityDkimAttributes(vals url.Values, reqID string) any {
	identities := parseSESMemberList(vals, "Identities")
	attrs := h.Backend.GetIdentityDkimAttributes(identities)

	entries := make([]xmlDkimEntry, 0, len(attrs))
	for k, v := range attrs {
		tokens := make([]xmlMember, 0, len(v.DkimTokens))
		for _, t := range v.DkimTokens {
			tokens = append(tokens, xmlMember{Value: t})
		}

		entries = append(entries, xmlDkimEntry{
			Key: k,
			Value: xmlDkimAttributes{
				DkimEnabled:            v.DkimEnabled,
				DkimVerificationStatus: v.DkimVerificationStatus,
				DkimTokens:             xmlMemberList{Members: tokens},
			},
		})
	}

	return &getIdentityDkimAttributesResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    getIdentityDkimAttributesResult{DkimAttributes: xmlDkimMap{Entries: entries}},
	}
}

func (h *Handler) handleSetIdentityDkimEnabled(vals url.Values, reqID string) (any, error) {
	enabled := vals.Get("DkimEnabled") == boolTrue
	if err := h.Backend.SetIdentityDkimEnabled(vals.Get("Identity"), enabled); err != nil {
		return nil, err
	}

	return newEmptyResponseWithResult("SetIdentityDkimEnabled", reqID), nil
}

func (h *Handler) handleVerifyDomainDkim(vals url.Values, reqID string) (any, error) {
	tokens, err := h.Backend.VerifyDomainDkim(vals.Get("Domain"))
	if err != nil {
		return nil, err
	}

	members := make([]xmlMember, 0, len(tokens))
	for _, t := range tokens {
		members = append(members, xmlMember{Value: t})
	}

	return &verifyDomainDkimResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    verifyDomainDkimResult{DkimTokens: xmlMemberList{Members: members}},
	}, nil
}

type xmlDkimAttributes struct {
	DkimVerificationStatus string        `xml:"DkimVerificationStatus"`
	DkimTokens             xmlMemberList `xml:"DkimTokens"`
	DkimEnabled            bool          `xml:"DkimEnabled"`
}

type xmlDkimEntry struct {
	Key   string            `xml:"key"`
	Value xmlDkimAttributes `xml:"value"`
}

type xmlDkimMap struct {
	Entries []xmlDkimEntry `xml:"entry"`
}

type getIdentityDkimAttributesResult struct {
	DkimAttributes xmlDkimMap `xml:"DkimAttributes"`
}

type getIdentityDkimAttributesResponse struct {
	XMLName   xml.Name                        `xml:"GetIdentityDkimAttributesResponse"`
	Xmlns     string                          `xml:"xmlns,attr"`
	RequestID string                          `xml:"ResponseMetadata>RequestId"`
	Result    getIdentityDkimAttributesResult `xml:"GetIdentityDkimAttributesResult"`
}

type verifyDomainDkimResult struct {
	DkimTokens xmlMemberList `xml:"DkimTokens"`
}

type verifyDomainDkimResponse struct {
	XMLName   xml.Name               `xml:"VerifyDomainDkimResponse"`
	Xmlns     string                 `xml:"xmlns,attr"`
	RequestID string                 `xml:"ResponseMetadata>RequestId"`
	Result    verifyDomainDkimResult `xml:"VerifyDomainDkimResult"`
}
