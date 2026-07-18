package ses

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleVerifyDomainIdentity(vals url.Values, reqID string) (any, error) {
	token, err := h.Backend.VerifyDomainIdentity(vals.Get("Domain"))
	if err != nil {
		return nil, err
	}

	return &verifyDomainIdentityResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    verifyDomainIdentityResult{VerificationToken: token},
	}, nil
}

func (h *Handler) handleVerifyEmailAddress(vals url.Values, reqID string) (any, error) {
	email := vals.Get("EmailAddress")
	if err := h.Backend.VerifyEmailAddress(email); err != nil {
		return nil, err
	}

	return newEmptyResponse("VerifyEmailAddress", reqID), nil
}

func (h *Handler) handleDeleteVerifiedEmailAddress(vals url.Values, reqID string) any {
	h.Backend.DeleteVerifiedEmailAddress(vals.Get("EmailAddress"))

	return newEmptyResponse("DeleteVerifiedEmailAddress", reqID)
}

func (h *Handler) handleListVerifiedEmailAddresses(reqID string) any {
	emails := h.Backend.ListVerifiedEmailAddresses()
	members := make([]xmlMember, 0, len(emails))

	for _, e := range emails {
		members = append(members, xmlMember{Value: e})
	}

	return &listVerifiedEmailAddressesResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    listVerifiedEmailAddressesResult{VerifiedEmailAddresses: xmlMemberList{Members: members}},
	}
}

type verifyDomainIdentityResult struct {
	VerificationToken string `xml:"VerificationToken"`
}

type verifyDomainIdentityResponse struct {
	XMLName   xml.Name                   `xml:"VerifyDomainIdentityResponse"`
	Xmlns     string                     `xml:"xmlns,attr"`
	RequestID string                     `xml:"ResponseMetadata>RequestId"`
	Result    verifyDomainIdentityResult `xml:"VerifyDomainIdentityResult"`
}

type listVerifiedEmailAddressesResult struct {
	VerifiedEmailAddresses xmlMemberList `xml:"VerifiedEmailAddresses"`
}

type listVerifiedEmailAddressesResponse struct {
	XMLName   xml.Name                         `xml:"ListVerifiedEmailAddressesResponse"`
	Xmlns     string                           `xml:"xmlns,attr"`
	RequestID string                           `xml:"ResponseMetadata>RequestId"`
	Result    listVerifiedEmailAddressesResult `xml:"ListVerifiedEmailAddressesResult"`
}
