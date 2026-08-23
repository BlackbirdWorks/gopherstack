package ses

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

func (h *Handler) handleVerifyEmailIdentity(vals url.Values, reqID string) (any, error) {
	identity := vals.Get("EmailAddress")
	if identity == "" {
		identity = vals.Get("Identity")
	}

	if err := h.Backend.VerifyEmailIdentity(identity); err != nil {
		return nil, err
	}

	return &verifyEmailIdentityResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleDeleteIdentity(vals url.Values, reqID string) any {
	identity := vals.Get("Identity")

	h.Backend.DeleteIdentity(identity)

	return &deleteIdentityResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}
}

func (h *Handler) handleListIdentities(vals url.Values, reqID string) (any, error) {
	nextToken := vals.Get("NextToken")
	maxItems := 0
	if s := vals.Get("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxItems = n
		}
	}

	identityType := vals.Get("IdentityType")
	if identityType != "" && identityType != identityTypeEmailAddress && identityType != identityTypeDomain {
		return nil, fmt.Errorf("%w: IdentityType must be EmailAddress or Domain", ErrValidation)
	}

	p := h.Backend.ListIdentities(nextToken, maxItems, identityType)
	members := make([]xmlMember, 0, len(p.Data))

	for _, id := range p.Data {
		members = append(members, xmlMember{Value: id})
	}

	return &listIdentitiesResponse{
		Xmlns: sesXMLNS,
		Result: listIdentitiesResult{
			Identities: xmlMemberList{Members: members},
			NextToken:  p.Next,
		},
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleGetIdentityVerificationAttributes(vals url.Values, reqID string) any {
	identities := parseSESMemberList(vals, "Identities")

	attrs := h.Backend.GetIdentityVerificationAttributes(identities)
	entries := make([]xmlVerificationEntry, 0, len(attrs))

	for id, status := range attrs {
		entries = append(entries, xmlVerificationEntry{
			Key: id,
			Value: xmlVerificationAttributes{
				VerificationStatus: status,
			},
		})
	}

	return &getIdentityVerificationAttributesResponse{
		Xmlns: sesXMLNS,
		Result: getIdentityVerificationAttributesResult{
			VerificationAttributes: xmlVerificationMap{Entries: entries},
		},
		RequestID: reqID,
	}
}

type verifyEmailIdentityResponse struct {
	XMLName   xml.Name                  `xml:"VerifyEmailIdentityResponse"`
	Xmlns     string                    `xml:"xmlns,attr"`
	Result    verifyEmailIdentityResult `xml:"VerifyEmailIdentityResult"`
	RequestID string                    `xml:"ResponseMetadata>RequestId"`
}

type verifyEmailIdentityResult struct{}

type deleteIdentityResponse struct {
	XMLName   xml.Name             `xml:"DeleteIdentityResponse"`
	Xmlns     string               `xml:"xmlns,attr"`
	Result    deleteIdentityResult `xml:"DeleteIdentityResult"`
	RequestID string               `xml:"ResponseMetadata>RequestId"`
}

type deleteIdentityResult struct{}

type listIdentitiesResult struct {
	NextToken  string        `xml:"NextToken,omitempty"`
	Identities xmlMemberList `xml:"Identities"`
}

type listIdentitiesResponse struct {
	XMLName   xml.Name             `xml:"ListIdentitiesResponse"`
	Xmlns     string               `xml:"xmlns,attr"`
	RequestID string               `xml:"ResponseMetadata>RequestId"`
	Result    listIdentitiesResult `xml:"ListIdentitiesResult"`
}

type xmlVerificationAttributes struct {
	VerificationStatus string `xml:"VerificationStatus"`
}

type xmlVerificationEntry struct {
	Key   string                    `xml:"key"`
	Value xmlVerificationAttributes `xml:"value"`
}

type xmlVerificationMap struct {
	Entries []xmlVerificationEntry `xml:"entry"`
}

type getIdentityVerificationAttributesResult struct {
	VerificationAttributes xmlVerificationMap `xml:"VerificationAttributes"`
}

type getIdentityVerificationAttributesResponse struct {
	XMLName   xml.Name                                `xml:"GetIdentityVerificationAttributesResponse"`
	Xmlns     string                                  `xml:"xmlns,attr"`
	RequestID string                                  `xml:"ResponseMetadata>RequestId"`
	Result    getIdentityVerificationAttributesResult `xml:"GetIdentityVerificationAttributesResult"`
}

func (h *Handler) handlePutIdentityPolicy(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.PutIdentityPolicy(
		vals.Get("Identity"),
		vals.Get("PolicyName"),
		vals.Get("Policy"),
	); err != nil {
		return nil, err
	}

	return newEmptyResponseWithResult("PutIdentityPolicy", reqID), nil
}

func (h *Handler) handleDeleteIdentityPolicy(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteIdentityPolicy(vals.Get("Identity"), vals.Get("PolicyName")); err != nil {
		return nil, err
	}

	return newEmptyResponseWithResult("DeleteIdentityPolicy", reqID), nil
}

func (h *Handler) handleGetIdentityPolicies(vals url.Values, reqID string) (any, error) {
	identity := vals.Get("Identity")
	policyNames := parseSESMemberList(vals, "PolicyNames")

	policies, err := h.Backend.GetIdentityPolicies(identity, policyNames)
	if err != nil {
		return nil, err
	}

	entries := make([]xmlPolicyEntry, 0, len(policies))
	for k, v := range policies {
		entries = append(entries, xmlPolicyEntry{Key: k, Value: v})
	}

	return &getIdentityPoliciesResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    getIdentityPoliciesResult{Policies: xmlPolicyMap{Entries: entries}},
	}, nil
}

func (h *Handler) handleListIdentityPolicies(vals url.Values, reqID string) (any, error) {
	names, err := h.Backend.ListIdentityPolicies(vals.Get("Identity"))
	if err != nil {
		return nil, err
	}

	members := make([]xmlMember, 0, len(names))
	for _, n := range names {
		members = append(members, xmlMember{Value: n})
	}

	return &listIdentityPoliciesResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    listIdentityPoliciesResult{PolicyNames: xmlMemberList{Members: members}},
	}, nil
}

func (h *Handler) handleGetIdentityMailFromDomainAttributes(vals url.Values, reqID string) any {
	identities := parseSESMemberList(vals, "Identities")
	attrs := h.Backend.GetIdentityMailFromDomainAttributes(identities)

	entries := make([]xmlMailFromEntry, 0, len(attrs))
	for k, v := range attrs {
		entries = append(entries, xmlMailFromEntry{Key: k, Value: xmlMailFromDomainAttributes(v)})
	}

	return &getIdentityMailFromDomainAttributesResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result: getIdentityMailFromDomainAttributesResult{
			MailFromDomainAttributes: xmlMailFromMap{Entries: entries},
		},
	}
}

func (h *Handler) handleGetIdentityNotificationAttributes(vals url.Values, reqID string) any {
	identities := parseSESMemberList(vals, "Identities")
	attrs := h.Backend.GetIdentityNotificationAttributes(identities)

	entries := make([]xmlNotifEntry, 0, len(attrs))
	for k, v := range attrs {
		entries = append(entries, xmlNotifEntry{Key: k, Value: xmlNotificationAttributes(v)})
	}

	return &getIdentityNotificationAttributesResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    getIdentityNotificationAttributesResult{NotificationAttributes: xmlNotifMap{Entries: entries}},
	}
}

func (h *Handler) handleSetIdentityFeedbackForwardingEnabled(vals url.Values, reqID string) (any, error) {
	enabled := vals.Get("ForwardingEnabled") == boolTrue
	if err := h.Backend.SetIdentityFeedbackForwardingEnabled(vals.Get("Identity"), enabled); err != nil {
		return nil, err
	}

	return newEmptyResponseWithResult("SetIdentityFeedbackForwardingEnabled", reqID), nil
}

func (h *Handler) handleSetIdentityHeadersInNotificationsEnabled(vals url.Values, reqID string) (any, error) {
	enabled := vals.Get("Enabled") == boolTrue
	if err := h.Backend.SetIdentityHeadersInNotificationsEnabled(
		vals.Get("Identity"),
		vals.Get("NotificationType"),
		enabled,
	); err != nil {
		return nil, err
	}

	return newEmptyResponseWithResult("SetIdentityHeadersInNotificationsEnabled", reqID), nil
}

func (h *Handler) handleSetIdentityMailFromDomain(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.SetIdentityMailFromDomain(
		vals.Get("Identity"),
		vals.Get("MailFromDomain"),
		vals.Get("BehaviorOnMXFailure"),
	); err != nil {
		return nil, err
	}

	return newEmptyResponseWithResult("SetIdentityMailFromDomain", reqID), nil
}

func (h *Handler) handleSetIdentityNotificationTopic(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.SetIdentityNotificationTopic(
		vals.Get("Identity"),
		vals.Get("NotificationType"),
		vals.Get("SnsTopic"),
	); err != nil {
		return nil, err
	}

	return newEmptyResponseWithResult("SetIdentityNotificationTopic", reqID), nil
}

type xmlPolicyEntry struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

type xmlPolicyMap struct {
	Entries []xmlPolicyEntry `xml:"entry"`
}

type getIdentityPoliciesResult struct {
	Policies xmlPolicyMap `xml:"Policies"`
}

type getIdentityPoliciesResponse struct {
	XMLName   xml.Name                  `xml:"GetIdentityPoliciesResponse"`
	Xmlns     string                    `xml:"xmlns,attr"`
	RequestID string                    `xml:"ResponseMetadata>RequestId"`
	Result    getIdentityPoliciesResult `xml:"GetIdentityPoliciesResult"`
}

type listIdentityPoliciesResult struct {
	PolicyNames xmlMemberList `xml:"PolicyNames"`
}

type listIdentityPoliciesResponse struct {
	XMLName   xml.Name                   `xml:"ListIdentityPoliciesResponse"`
	Xmlns     string                     `xml:"xmlns,attr"`
	RequestID string                     `xml:"ResponseMetadata>RequestId"`
	Result    listIdentityPoliciesResult `xml:"ListIdentityPoliciesResult"`
}

// MailFromDomain is required on types.IdentityMailFromDomainAttributes and,
// unlike BehaviorOnMXFailure/MailFromDomainStatus, decodes as a *string on
// the real SDK type (ses@v1.37.4 types/types.go:557-577) -- an omitted
// element decodes to a nil pointer there, not the empty string a present
// element with no chardata would give, so dropping the tag for an
// unconfigured identity is a real, client-visible difference. The other two
// fields are non-pointer enums/strings where omitted vs present-empty
// decode identically, so their omitempty removal below is cleanup, not a
// distinct fix.
type xmlMailFromDomainAttributes struct {
	MailFromDomain       string `xml:"MailFromDomain"`
	MailFromDomainStatus string `xml:"MailFromDomainStatus"`
	BehaviorOnMXFailure  string `xml:"BehaviorOnMXFailure"`
}

type xmlMailFromEntry struct {
	Key   string                      `xml:"key"`
	Value xmlMailFromDomainAttributes `xml:"value"`
}

type xmlMailFromMap struct {
	Entries []xmlMailFromEntry `xml:"entry"`
}

type getIdentityMailFromDomainAttributesResult struct {
	MailFromDomainAttributes xmlMailFromMap `xml:"MailFromDomainAttributes"`
}

type getIdentityMailFromDomainAttributesResponse struct {
	XMLName   xml.Name                                  `xml:"GetIdentityMailFromDomainAttributesResponse"`
	Xmlns     string                                    `xml:"xmlns,attr"`
	RequestID string                                    `xml:"ResponseMetadata>RequestId"`
	Result    getIdentityMailFromDomainAttributesResult `xml:"GetIdentityMailFromDomainAttributesResult"`
}

// BounceTopic/ComplaintTopic/DeliveryTopic are all required *string members
// on the real types.IdentityNotificationAttributes (ses@v1.37.4
// types/types.go): an identity with no SNS topic configured for a given
// notification type -- the common state before SetIdentityNotificationTopic
// is ever called -- has an empty Go string here, and omitempty was dropping
// the element entirely, decoding to a nil pointer on a real client instead
// of a pointer to "". HeadersIn*'s XML tags were also wrong against the
// real deserializer's key names (HeadersInBounceNotificationsEnabled etc.,
// deserializers.go's IdentityNotificationAttributes case-switch) -- fixed
// alongside since it's the same struct, but these fields aren't
// Smithy-required so it's not part of this required-output-member cut.
type xmlNotificationAttributes struct {
	BounceTopic        string `xml:"BounceTopic"`
	ComplaintTopic     string `xml:"ComplaintTopic"`
	DeliveryTopic      string `xml:"DeliveryTopic"`
	ForwardingEnabled  bool   `xml:"ForwardingEnabled"`
	HeadersInBounce    bool   `xml:"HeadersInBounceNotificationsEnabled"`
	HeadersInComplaint bool   `xml:"HeadersInComplaintNotificationsEnabled"`
	HeadersInDelivery  bool   `xml:"HeadersInDeliveryNotificationsEnabled"`
}

type xmlNotifEntry struct {
	Key   string                    `xml:"key"`
	Value xmlNotificationAttributes `xml:"value"`
}

type xmlNotifMap struct {
	Entries []xmlNotifEntry `xml:"entry"`
}

type getIdentityNotificationAttributesResult struct {
	NotificationAttributes xmlNotifMap `xml:"NotificationAttributes"`
}

type getIdentityNotificationAttributesResponse struct {
	XMLName   xml.Name                                `xml:"GetIdentityNotificationAttributesResponse"`
	Xmlns     string                                  `xml:"xmlns,attr"`
	RequestID string                                  `xml:"ResponseMetadata>RequestId"`
	Result    getIdentityNotificationAttributesResult `xml:"GetIdentityNotificationAttributesResult"`
}
