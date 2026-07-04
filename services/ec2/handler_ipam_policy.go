package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

// handler_ipam_policy.go implements the HTTP handlers for the IPAM Policy / Governance
// sub-family: IPAM policies (Create/Describe/DeleteIpamPolicy), their enablement for the
// current account and for Organizations targets (Enable/DisableIpamPolicy,
// GetEnabledIpamPolicy, GetIpamPolicyOrganizationTargets), a policy's public IPv4 allocation
// rules (Get/ModifyIpamPolicyAllocationRules), the IPAM Organizations delegated admin account
// setting (Enable/DisableIpamOrganizationAdminAccount), and moving an existing BYOIP CIDR into
// an IPAM pool (MoveByoipCidrToIpam). The IPAM core (Ipam/IpamScope/IpamPool) lives in
// handler_advanced_networking.go; this file extends the same family.

// ---- Handler registration ----

func registerIpamPolicyOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateIpamPolicy"] = h.handleCreateIpamPolicy
	ops["DeleteIpamPolicy"] = h.handleDeleteIpamPolicy
	ops["DescribeIpamPolicies"] = h.handleDescribeIpamPolicies
	ops["EnableIpamPolicy"] = h.handleEnableIpamPolicy
	ops["DisableIpamPolicy"] = h.handleDisableIpamPolicy
	ops["GetEnabledIpamPolicy"] = h.handleGetEnabledIpamPolicy
	ops["GetIpamPolicyAllocationRules"] = h.handleGetIpamPolicyAllocationRules
	ops["ModifyIpamPolicyAllocationRules"] = h.handleModifyIpamPolicyAllocationRules
	ops["GetIpamPolicyOrganizationTargets"] = h.handleGetIpamPolicyOrganizationTargets
	ops["EnableIpamOrganizationAdminAccount"] = h.handleEnableIpamOrganizationAdminAccount
	ops["DisableIpamOrganizationAdminAccount"] = h.handleDisableIpamOrganizationAdminAccount
	ops["MoveByoipCidrToIpam"] = h.handleMoveByoipCidrToIpam
}

func ipamPolicySupportedOperations() []string {
	return []string{
		"CreateIpamPolicy",
		"DeleteIpamPolicy",
		"DescribeIpamPolicies",
		"EnableIpamPolicy",
		"DisableIpamPolicy",
		"GetEnabledIpamPolicy",
		"GetIpamPolicyAllocationRules",
		"ModifyIpamPolicyAllocationRules",
		"GetIpamPolicyOrganizationTargets",
		"EnableIpamOrganizationAdminAccount",
		"DisableIpamOrganizationAdminAccount",
		"MoveByoipCidrToIpam",
	}
}

// ---- Shared param parsing ----

// parseIpamPolicyAllocationRules parses the flattened "AllocationRule.N.SourceIpamPoolId" form
// values accepted by ModifyIpamPolicyAllocationRules.
func parseIpamPolicyAllocationRules(vals url.Values) []IpamPolicyAllocationRule {
	var rules []IpamPolicyAllocationRule

	for i := 1; ; i++ {
		poolID := vals.Get(fmt.Sprintf("AllocationRule.%d.SourceIpamPoolId", i))
		if poolID == "" {
			return rules
		}

		rules = append(rules, IpamPolicyAllocationRule{SourceIpamPoolID: poolID})
	}
}

// ---- XML response types ----

type ipamPolicyAllocationRuleItem struct {
	SourceIpamPoolID string `xml:"sourceIpamPoolId,omitempty"`
}

func toIpamPolicyAllocationRuleItem(r IpamPolicyAllocationRule) ipamPolicyAllocationRuleItem {
	return ipamPolicyAllocationRuleItem(r)
}

type ipamPolicyItem struct {
	IpamID           string `xml:"ipamId,omitempty"`
	IpamPolicyARN    string `xml:"ipamPolicyArn,omitempty"`
	IpamPolicyID     string `xml:"ipamPolicyId,omitempty"`
	IpamPolicyRegion string `xml:"ipamPolicyRegion,omitempty"`
	OwnerID          string `xml:"ownerId,omitempty"`
	State            string `xml:"state,omitempty"`
	StateMessage     string `xml:"stateMessage,omitempty"`
}

func toIpamPolicyItem(p *IpamPolicy) ipamPolicyItem {
	return ipamPolicyItem{
		IpamID:           p.IpamID,
		IpamPolicyARN:    p.IpamPolicyARN,
		IpamPolicyID:     p.IpamPolicyID,
		IpamPolicyRegion: p.IpamPolicyRegion,
		OwnerID:          p.OwnerID,
		State:            p.State,
		StateMessage:     p.StateMessage,
	}
}

type ipamPolicyDocumentItem struct {
	IpamPolicyID      string `xml:"ipamPolicyId,omitempty"`
	Locale            string `xml:"locale,omitempty"`
	ResourceType      string `xml:"resourceType,omitempty"`
	AllocationRuleSet struct {
		Items []ipamPolicyAllocationRuleItem `xml:"item"`
	} `xml:"allocationRuleSet"`
}

func toIpamPolicyDocumentItem(d *IpamPolicyDocument) ipamPolicyDocumentItem {
	item := ipamPolicyDocumentItem{
		IpamPolicyID: d.IpamPolicyID,
		Locale:       d.Locale,
		ResourceType: d.ResourceType,
	}

	for _, r := range d.AllocationRules {
		item.AllocationRuleSet.Items = append(item.AllocationRuleSet.Items, toIpamPolicyAllocationRuleItem(r))
	}

	return item
}

type createIpamPolicyResponse struct {
	XMLName    xml.Name       `xml:"CreateIpamPolicyResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	RequestID  string         `xml:"requestId"`
	IpamPolicy ipamPolicyItem `xml:"ipamPolicy"`
}

type deleteIpamPolicyResponse struct {
	XMLName    xml.Name       `xml:"DeleteIpamPolicyResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	RequestID  string         `xml:"requestId"`
	IpamPolicy ipamPolicyItem `xml:"ipamPolicy"`
}

type describeIpamPoliciesResponse struct {
	XMLName       xml.Name `xml:"DescribeIpamPoliciesResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	IpamPolicySet struct {
		Items []ipamPolicyItem `xml:"item"`
	} `xml:"ipamPolicySet"`
}

type enableIpamPolicyResponse struct {
	XMLName      xml.Name `xml:"EnableIpamPolicyResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	IpamPolicyID string   `xml:"ipamPolicyId,omitempty"`
}

type disableIpamPolicyResponse struct {
	XMLName   xml.Name `xml:"DisableIpamPolicyResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type getEnabledIpamPolicyResponse struct {
	XMLName           xml.Name `xml:"GetEnabledIpamPolicyResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	RequestID         string   `xml:"requestId"`
	IpamPolicyID      string   `xml:"ipamPolicyId,omitempty"`
	ManagedBy         string   `xml:"managedBy,omitempty"`
	IpamPolicyEnabled bool     `xml:"ipamPolicyEnabled,omitempty"`
}

type getIpamPolicyAllocationRulesResponse struct {
	XMLName               xml.Name `xml:"GetIpamPolicyAllocationRulesResponse"`
	Xmlns                 string   `xml:"xmlns,attr"`
	RequestID             string   `xml:"requestId"`
	IpamPolicyDocumentSet struct {
		Items []ipamPolicyDocumentItem `xml:"item"`
	} `xml:"ipamPolicyDocumentSet"`
}

type modifyIpamPolicyAllocationRulesResponse struct {
	XMLName            xml.Name               `xml:"ModifyIpamPolicyAllocationRulesResponse"`
	Xmlns              string                 `xml:"xmlns,attr"`
	RequestID          string                 `xml:"requestId"`
	IpamPolicyDocument ipamPolicyDocumentItem `xml:"ipamPolicyDocument"`
}

type ipamPolicyOrganizationTargetItem struct {
	OrganizationTargetID string `xml:"organizationTargetId,omitempty"`
}

type getIpamPolicyOrganizationTargetsResponse struct {
	XMLName               xml.Name `xml:"GetIpamPolicyOrganizationTargetsResponse"`
	Xmlns                 string   `xml:"xmlns,attr"`
	RequestID             string   `xml:"requestId"`
	OrganizationTargetSet struct {
		Items []ipamPolicyOrganizationTargetItem `xml:"item"`
	} `xml:"organizationTargetSet"`
}

type enableIpamOrganizationAdminAccountResponse struct {
	XMLName   xml.Name `xml:"EnableIpamOrganizationAdminAccountResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Success   bool     `xml:"success"`
}

type disableIpamOrganizationAdminAccountResponse struct {
	XMLName   xml.Name `xml:"DisableIpamOrganizationAdminAccountResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Success   bool     `xml:"success"`
}

type moveByoipCidrToIpamResponse struct {
	XMLName   xml.Name      `xml:"MoveByoipCidrToIpamResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	ByoipCidr byoipCidrItem `xml:"byoipCidr"`
}

// ---- Handlers ----

func (h *Handler) handleCreateIpamPolicy(vals url.Values, reqID string) (any, error) {
	p, err := h.Backend.CreateIpamPolicy(vals.Get("IpamId"))
	if err != nil {
		return nil, err
	}

	return &createIpamPolicyResponse{Xmlns: ec2XMLNS, RequestID: reqID, IpamPolicy: toIpamPolicyItem(p)}, nil
}

func (h *Handler) handleDeleteIpamPolicy(vals url.Values, reqID string) (any, error) {
	p, err := h.Backend.DeleteIpamPolicy(vals.Get("IpamPolicyId"))
	if err != nil {
		return nil, err
	}

	return &deleteIpamPolicyResponse{Xmlns: ec2XMLNS, RequestID: reqID, IpamPolicy: toIpamPolicyItem(p)}, nil
}

func (h *Handler) handleDescribeIpamPolicies(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamPolicyId")
	policies := h.Backend.DescribeIpamPolicies(ids)

	resp := &describeIpamPoliciesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, p := range policies {
		resp.IpamPolicySet.Items = append(resp.IpamPolicySet.Items, toIpamPolicyItem(p))
	}

	return resp, nil
}

func (h *Handler) handleEnableIpamPolicy(vals url.Values, reqID string) (any, error) {
	id := vals.Get("IpamPolicyId")

	if err := h.Backend.EnableIpamPolicy(id, vals.Get("OrganizationTargetId")); err != nil {
		return nil, err
	}

	return &enableIpamPolicyResponse{Xmlns: ec2XMLNS, RequestID: reqID, IpamPolicyID: id}, nil
}

func (h *Handler) handleDisableIpamPolicy(vals url.Values, reqID string) (any, error) {
	id := vals.Get("IpamPolicyId")

	if err := h.Backend.DisableIpamPolicy(id, vals.Get("OrganizationTargetId")); err != nil {
		return nil, err
	}

	return &disableIpamPolicyResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleGetEnabledIpamPolicy(_ url.Values, reqID string) (any, error) {
	id, enabled, managedBy := h.Backend.GetEnabledIpamPolicy()

	return &getEnabledIpamPolicyResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		IpamPolicyEnabled: enabled,
		IpamPolicyID:      id,
		ManagedBy:         managedBy,
	}, nil
}

func (h *Handler) handleGetIpamPolicyAllocationRules(vals url.Values, reqID string) (any, error) {
	docs, err := h.Backend.GetIpamPolicyAllocationRules(
		vals.Get("IpamPolicyId"), vals.Get("Locale"), vals.Get("ResourceType"),
	)
	if err != nil {
		return nil, err
	}

	resp := &getIpamPolicyAllocationRulesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, d := range docs {
		resp.IpamPolicyDocumentSet.Items = append(resp.IpamPolicyDocumentSet.Items, toIpamPolicyDocumentItem(d))
	}

	return resp, nil
}

func (h *Handler) handleModifyIpamPolicyAllocationRules(vals url.Values, reqID string) (any, error) {
	rules := parseIpamPolicyAllocationRules(vals)

	doc, err := h.Backend.ModifyIpamPolicyAllocationRules(
		vals.Get("IpamPolicyId"), vals.Get("Locale"), vals.Get("ResourceType"), rules,
	)
	if err != nil {
		return nil, err
	}

	return &modifyIpamPolicyAllocationRulesResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamPolicyDocument: toIpamPolicyDocumentItem(doc),
	}, nil
}

func (h *Handler) handleGetIpamPolicyOrganizationTargets(vals url.Values, reqID string) (any, error) {
	targets, err := h.Backend.GetIpamPolicyOrganizationTargets(vals.Get("IpamPolicyId"))
	if err != nil {
		return nil, err
	}

	resp := &getIpamPolicyOrganizationTargetsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, t := range targets {
		resp.OrganizationTargetSet.Items = append(
			resp.OrganizationTargetSet.Items, ipamPolicyOrganizationTargetItem{OrganizationTargetID: t},
		)
	}

	return resp, nil
}

func (h *Handler) handleEnableIpamOrganizationAdminAccount(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.EnableIpamOrganizationAdminAccount(vals.Get("DelegatedAdminAccountId")); err != nil {
		return nil, err
	}

	return &enableIpamOrganizationAdminAccountResponse{Xmlns: ec2XMLNS, RequestID: reqID, Success: true}, nil
}

func (h *Handler) handleDisableIpamOrganizationAdminAccount(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DisableIpamOrganizationAdminAccount(vals.Get("DelegatedAdminAccountId")); err != nil {
		return nil, err
	}

	return &disableIpamOrganizationAdminAccountResponse{Xmlns: ec2XMLNS, RequestID: reqID, Success: true}, nil
}

func (h *Handler) handleMoveByoipCidrToIpam(vals url.Values, reqID string) (any, error) {
	entry, err := h.Backend.MoveByoipCidrToIpam(
		vals.Get("Cidr"), vals.Get("IpamPoolId"), vals.Get("IpamPoolOwner"),
	)
	if err != nil {
		return nil, err
	}

	return &moveByoipCidrToIpamResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ByoipCidr: byoipCidrItem{
			Cidr:          entry.Cidr,
			State:         entry.State,
			StatusMessage: entry.StatusMessage,
		},
	}, nil
}
