package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// handler_ipam_discovery.go implements the HTTP handlers for the IPAM Resource Discovery
// (user-created discoveries + associations, Resource CIDR monitoring), BYOASN, External
// Resource Verification Token, and Prefix List Resolver (+ Targets/Rules/Versions)
// sub-families. The IPAM core (Ipam/IpamScope/IpamPool) and the default-discovery Describe
// ops live in handler_advanced_networking.go; this file extends the same family.

// ---- Handler registration ----

func registerIpamDiscoveryOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateIpamResourceDiscovery"] = h.handleCreateIpamResourceDiscovery
	ops["DeleteIpamResourceDiscovery"] = h.handleDeleteIpamResourceDiscovery
	ops["AssociateIpamResourceDiscovery"] = h.handleAssociateIpamResourceDiscovery
	ops["DisassociateIpamResourceDiscovery"] = h.handleDisassociateIpamResourceDiscovery
	ops["ModifyIpamResourceDiscovery"] = h.handleModifyIpamResourceDiscovery
	ops["GetIpamResourceCidrs"] = h.handleGetIpamResourceCidrs
	ops["ModifyIpamResourceCidr"] = h.handleModifyIpamResourceCidr

	ops["ProvisionIpamByoasn"] = h.handleProvisionIpamByoasn
	ops["DeprovisionIpamByoasn"] = h.handleDeprovisionIpamByoasn
	ops["DescribeIpamByoasn"] = h.handleDescribeIpamByoasn
	ops["AssociateIpamByoasn"] = h.handleAssociateIpamByoasn
	ops["DisassociateIpamByoasn"] = h.handleDisassociateIpamByoasn

	ops["CreateIpamExternalResourceVerificationToken"] = h.handleCreateIpamExternalResourceVerificationToken
	ops["DeleteIpamExternalResourceVerificationToken"] = h.handleDeleteIpamExternalResourceVerificationToken
	ops["DescribeIpamExternalResourceVerificationTokens"] = h.handleDescribeIpamExternalResourceVerificationTokens

	ops["CreateIpamPrefixListResolver"] = h.handleCreateIpamPrefixListResolver
	ops["DeleteIpamPrefixListResolver"] = h.handleDeleteIpamPrefixListResolver
	ops["DescribeIpamPrefixListResolvers"] = h.handleDescribeIpamPrefixListResolvers
	ops["ModifyIpamPrefixListResolver"] = h.handleModifyIpamPrefixListResolver
	ops["CreateIpamPrefixListResolverTarget"] = h.handleCreateIpamPrefixListResolverTarget
	ops["DeleteIpamPrefixListResolverTarget"] = h.handleDeleteIpamPrefixListResolverTarget
	ops["DescribeIpamPrefixListResolverTargets"] = h.handleDescribeIpamPrefixListResolverTargets
	ops["ModifyIpamPrefixListResolverTarget"] = h.handleModifyIpamPrefixListResolverTarget
	ops["GetIpamPrefixListResolverRules"] = h.handleGetIpamPrefixListResolverRules
	ops["GetIpamPrefixListResolverVersions"] = h.handleGetIpamPrefixListResolverVersions
	ops["GetIpamPrefixListResolverVersionEntries"] = h.handleGetIpamPrefixListResolverVersionEntries
}

func ipamDiscoverySupportedOperations() []string {
	return []string{
		"CreateIpamResourceDiscovery",
		"DeleteIpamResourceDiscovery",
		"AssociateIpamResourceDiscovery",
		"DisassociateIpamResourceDiscovery",
		"ModifyIpamResourceDiscovery",
		"GetIpamResourceCidrs",
		"ModifyIpamResourceCidr",
		"ProvisionIpamByoasn",
		"DeprovisionIpamByoasn",
		"DescribeIpamByoasn",
		"AssociateIpamByoasn",
		"DisassociateIpamByoasn",
		"CreateIpamExternalResourceVerificationToken",
		"DeleteIpamExternalResourceVerificationToken",
		"DescribeIpamExternalResourceVerificationTokens",
		"CreateIpamPrefixListResolver",
		"DeleteIpamPrefixListResolver",
		"DescribeIpamPrefixListResolvers",
		"ModifyIpamPrefixListResolver",
		"CreateIpamPrefixListResolverTarget",
		"DeleteIpamPrefixListResolverTarget",
		"DescribeIpamPrefixListResolverTargets",
		"ModifyIpamPrefixListResolverTarget",
		"GetIpamPrefixListResolverRules",
		"GetIpamPrefixListResolverVersions",
		"GetIpamPrefixListResolverVersionEntries",
	}
}

// ---- Shared param parsing ----

// parseRegionNameList parses "<prefix>.N.RegionName" indexed form values, used for both
// OperatingRegion (create) and AddOperatingRegion/RemoveOperatingRegion (modify) lists.
func parseRegionNameList(vals url.Values, prefix string) []string {
	var regions []string

	for i := 1; ; i++ {
		r := vals.Get(fmt.Sprintf("%s.%d.RegionName", prefix, i))
		if r == "" {
			return regions
		}

		regions = append(regions, r)
	}
}

// parseIpamPrefixListResolverRules parses the "Rule.N.*" / "Rule.N.Condition.M.*" indexed form
// values accepted by CreateIpamPrefixListResolver and ModifyIpamPrefixListResolver. It returns
// (rules, true) if at least one "Rule.N.RuleType" was present, else (nil, false) so callers can
// distinguish "no Rules parameter at all" from "Rules explicitly cleared".
func parseIpamPrefixListResolverRules(vals url.Values) ([]IpamPrefixListResolverRule, bool) {
	var rules []IpamPrefixListResolverRule

	for i := 1; ; i++ {
		prefix := fmt.Sprintf("Rule.%d", i)

		ruleType := vals.Get(prefix + ".RuleType")
		if ruleType == "" {
			break
		}

		rule := IpamPrefixListResolverRule{
			RuleType:     ruleType,
			IpamScopeID:  vals.Get(prefix + ".IpamScopeId"),
			ResourceType: vals.Get(prefix + ".ResourceType"),
			StaticCidr:   vals.Get(prefix + ".StaticCidr"),
		}

		for j := 1; ; j++ {
			condPrefix := fmt.Sprintf("%s.Condition.%d", prefix, j)

			op := vals.Get(condPrefix + ".Operation")
			if op == "" {
				break
			}

			rule.Conditions = append(rule.Conditions, IpamPrefixListResolverRuleCondition{
				Operation:      op,
				Cidr:           vals.Get(condPrefix + ".Cidr"),
				IpamPoolID:     vals.Get(condPrefix + ".IpamPoolId"),
				ResourceID:     vals.Get(condPrefix + ".ResourceId"),
				ResourceOwner:  vals.Get(condPrefix + ".ResourceOwner"),
				ResourceRegion: vals.Get(condPrefix + ".ResourceRegion"),
			})
		}

		rules = append(rules, rule)
	}

	return rules, len(rules) > 0
}

// parseOptionalInt64 parses key as an int64 pointer, returning nil if the value is absent or
// unparsable (mirrors how the SDK's optional Long-typed fields are surfaced).
func parseOptionalInt64(vals url.Values, key string) *int64 {
	raw := vals.Get(key)
	if raw == "" {
		return nil
	}

	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil
	}

	return &n
}

// parseOptionalBool parses key as a *bool, returning nil if the value is absent.
func parseOptionalBool(vals url.Values, key string) *bool {
	raw := vals.Get(key)
	if raw == "" {
		return nil
	}

	v := raw == ec2BooleanTrue

	return &v
}

// ---- XML response types: Resource Discoveries / Resource CIDRs ----

// toIpamResourceDiscoveryItem builds the full wire representation of a resource discovery,
// including its operating regions. Shared by the Create/Delete/Modify/Describe handlers.
func toIpamResourceDiscoveryItem(d *IpamResourceDiscovery) ipamResourceDiscoveryItem {
	item := ipamResourceDiscoveryItem{
		IpamResourceDiscoveryID:     d.IpamResourceDiscoveryID,
		OwnerID:                     d.OwnerID,
		IpamResourceDiscoveryARN:    d.IpamResourceDiscoveryARN,
		IpamResourceDiscoveryRegion: d.Region,
		State:                       d.State,
		Description:                 d.Description,
		IsDefault:                   d.IsDefault,
	}

	for _, r := range d.OperatingRegions {
		item.OperatingRegionSet.Items = append(item.OperatingRegionSet.Items, ipamOperatingRegionItem{RegionName: r})
	}

	return item
}

type createIpamResourceDiscoveryResponse struct {
	XMLName               xml.Name                  `xml:"CreateIpamResourceDiscoveryResponse"`
	Xmlns                 string                    `xml:"xmlns,attr"`
	RequestID             string                    `xml:"requestId"`
	IpamResourceDiscovery ipamResourceDiscoveryItem `xml:"ipamResourceDiscovery"`
}

type deleteIpamResourceDiscoveryResponse struct {
	XMLName               xml.Name                  `xml:"DeleteIpamResourceDiscoveryResponse"`
	Xmlns                 string                    `xml:"xmlns,attr"`
	RequestID             string                    `xml:"requestId"`
	IpamResourceDiscovery ipamResourceDiscoveryItem `xml:"ipamResourceDiscovery"`
}

type modifyIpamResourceDiscoveryResponse struct {
	XMLName               xml.Name                  `xml:"ModifyIpamResourceDiscoveryResponse"`
	Xmlns                 string                    `xml:"xmlns,attr"`
	RequestID             string                    `xml:"requestId"`
	IpamResourceDiscovery ipamResourceDiscoveryItem `xml:"ipamResourceDiscovery"`
}

type associateIpamResourceDiscoveryResponse struct {
	XMLName                          xml.Name                             `xml:"AssociateIpamResourceDiscoveryResponse"`
	Xmlns                            string                               `xml:"xmlns,attr"`
	RequestID                        string                               `xml:"requestId"`
	IpamResourceDiscoveryAssociation ipamResourceDiscoveryAssociationItem `xml:"ipamResourceDiscoveryAssociation"`
}

type disassociateIpamResourceDiscoveryResponse struct {
	XMLName                          xml.Name                             `xml:"DisassociateIpamResourceDiscoveryResponse"`
	Xmlns                            string                               `xml:"xmlns,attr"`
	RequestID                        string                               `xml:"requestId"`
	IpamResourceDiscoveryAssociation ipamResourceDiscoveryAssociationItem `xml:"ipamResourceDiscoveryAssociation"`
}

type ipamResourceCidrItem struct {
	IpamID          string  `xml:"ipamId,omitempty"`
	IpamPoolID      string  `xml:"ipamPoolId,omitempty"`
	IpamScopeID     string  `xml:"ipamScopeId,omitempty"`
	ResourceID      string  `xml:"resourceId,omitempty"`
	ResourceCidr    string  `xml:"resourceCidr,omitempty"`
	ResourceRegion  string  `xml:"resourceRegion,omitempty"`
	ResourceType    string  `xml:"resourceType,omitempty"`
	ResourceOwnerID string  `xml:"resourceOwnerId,omitempty"`
	ManagementState string  `xml:"managementState,omitempty"`
	IPUsage         float64 `xml:"ipUsage,omitempty"`
}

func toIpamResourceCidrItem(c *IpamResourceCidr) ipamResourceCidrItem {
	return ipamResourceCidrItem{
		IpamID:          c.IpamID,
		IpamPoolID:      c.IpamPoolID,
		IpamScopeID:     c.IpamScopeID,
		ResourceID:      c.ResourceID,
		ResourceCidr:    c.ResourceCidr,
		ResourceRegion:  c.ResourceRegion,
		ResourceType:    c.ResourceType,
		ResourceOwnerID: c.ResourceOwnerID,
		ManagementState: c.ManagementState,
		IPUsage:         c.IPUsage,
	}
}

type getIpamResourceCidrsResponse struct {
	XMLName             xml.Name `xml:"GetIpamResourceCidrsResponse"`
	Xmlns               string   `xml:"xmlns,attr"`
	RequestID           string   `xml:"requestId"`
	IpamResourceCidrSet struct {
		Items []ipamResourceCidrItem `xml:"item"`
	} `xml:"ipamResourceCidrSet"`
}

type modifyIpamResourceCidrResponse struct {
	XMLName          xml.Name             `xml:"ModifyIpamResourceCidrResponse"`
	Xmlns            string               `xml:"xmlns,attr"`
	RequestID        string               `xml:"requestId"`
	IpamResourceCidr ipamResourceCidrItem `xml:"ipamResourceCidr"`
}

// ---- XML response types: BYOASN ----

type byoasnItem struct {
	Asn           string `xml:"asn,omitempty"`
	IpamID        string `xml:"ipamId,omitempty"`
	State         string `xml:"state,omitempty"`
	StatusMessage string `xml:"statusMessage,omitempty"`
}

func toByoasnItem(a *IpamByoasn) byoasnItem {
	return byoasnItem{Asn: a.Asn, IpamID: a.IpamID, State: a.State, StatusMessage: a.StatusMessage}
}

type provisionIpamByoasnResponse struct {
	XMLName   xml.Name   `xml:"ProvisionIpamByoasnResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	RequestID string     `xml:"requestId"`
	Byoasn    byoasnItem `xml:"byoasn"`
}

type deprovisionIpamByoasnResponse struct {
	XMLName   xml.Name   `xml:"DeprovisionIpamByoasnResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	RequestID string     `xml:"requestId"`
	Byoasn    byoasnItem `xml:"byoasn"`
}

type describeIpamByoasnResponse struct {
	XMLName   xml.Name `xml:"DescribeIpamByoasnResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	ByoasnSet struct {
		Items []byoasnItem `xml:"item"`
	} `xml:"byoasnSet"`
}

type asnAssociationItem struct {
	Asn           string `xml:"asn,omitempty"`
	Cidr          string `xml:"cidr,omitempty"`
	State         string `xml:"state,omitempty"`
	StatusMessage string `xml:"statusMessage,omitempty"`
}

func toAsnAssociationItem(a *IpamAsnAssociation) asnAssociationItem {
	return asnAssociationItem{Asn: a.Asn, Cidr: a.Cidr, State: a.State, StatusMessage: a.StatusMessage}
}

type associateIpamByoasnResponse struct {
	XMLName        xml.Name           `xml:"AssociateIpamByoasnResponse"`
	Xmlns          string             `xml:"xmlns,attr"`
	RequestID      string             `xml:"requestId"`
	AsnAssociation asnAssociationItem `xml:"asnAssociation"`
}

type disassociateIpamByoasnResponse struct {
	XMLName        xml.Name           `xml:"DisassociateIpamByoasnResponse"`
	Xmlns          string             `xml:"xmlns,attr"`
	RequestID      string             `xml:"requestId"`
	AsnAssociation asnAssociationItem `xml:"asnAssociation"`
}

// ---- XML response types: External Resource Verification Tokens ----

type ipamExternalResourceVerificationTokenItem struct {
	IpamExternalResourceVerificationTokenID  string `xml:"ipamExternalResourceVerificationTokenId,omitempty"`
	IpamExternalResourceVerificationTokenARN string `xml:"ipamExternalResourceVerificationTokenArn,omitempty"`
	IpamID                                   string `xml:"ipamId,omitempty"`
	IpamARN                                  string `xml:"ipamArn,omitempty"`
	IpamRegion                               string `xml:"ipamRegion,omitempty"`
	State                                    string `xml:"state,omitempty"`
	Status                                   string `xml:"status,omitempty"`
	TokenName                                string `xml:"tokenName,omitempty"`
	TokenValue                               string `xml:"tokenValue,omitempty"`
	NotAfter                                 string `xml:"notAfter,omitempty"`
}

func toIpamExternalResourceVerificationTokenItem(
	t *IpamExternalResourceVerificationToken,
) ipamExternalResourceVerificationTokenItem {
	item := ipamExternalResourceVerificationTokenItem{
		IpamExternalResourceVerificationTokenID:  t.IpamExternalResourceVerificationTokenID,
		IpamExternalResourceVerificationTokenARN: t.IpamExternalResourceVerificationTokenARN,
		IpamID:                                   t.IpamID,
		IpamARN:                                  t.IpamARN,
		IpamRegion:                               t.IpamRegion,
		State:                                    t.State,
		Status:                                   t.Status,
		TokenName:                                t.TokenName,
		TokenValue:                               t.TokenValue,
	}
	if !t.NotAfter.IsZero() {
		item.NotAfter = t.NotAfter.Format(time.RFC3339)
	}

	return item
}

type createIpamExternalResourceVerificationTokenResponse struct {
	XMLName   xml.Name                                  `xml:"CreateIpamExternalResourceVerificationTokenResponse"`
	Xmlns     string                                    `xml:"xmlns,attr"`
	RequestID string                                    `xml:"requestId"`
	Token     ipamExternalResourceVerificationTokenItem `xml:"ipamExternalResourceVerificationToken"`
}

type deleteIpamExternalResourceVerificationTokenResponse struct {
	XMLName   xml.Name                                  `xml:"DeleteIpamExternalResourceVerificationTokenResponse"`
	Xmlns     string                                    `xml:"xmlns,attr"`
	RequestID string                                    `xml:"requestId"`
	Token     ipamExternalResourceVerificationTokenItem `xml:"ipamExternalResourceVerificationToken"`
}

type describeIpamExternalResourceVerificationTokensResponse struct {
	XMLName                                  xml.Name `xml:"DescribeIpamExternalResourceVerificationTokensResponse"`
	Xmlns                                    string   `xml:"xmlns,attr"`
	RequestID                                string   `xml:"requestId"`
	IpamExternalResourceVerificationTokenSet struct {
		Items []ipamExternalResourceVerificationTokenItem `xml:"item"`
	} `xml:"ipamExternalResourceVerificationTokenSet"`
}

// ---- XML response types: Prefix List Resolvers ----

type ipamPrefixListResolverRuleConditionItem struct {
	Operation      string `xml:"operation,omitempty"`
	Cidr           string `xml:"cidr,omitempty"`
	IpamPoolID     string `xml:"ipamPoolId,omitempty"`
	ResourceID     string `xml:"resourceId,omitempty"`
	ResourceOwner  string `xml:"resourceOwner,omitempty"`
	ResourceRegion string `xml:"resourceRegion,omitempty"`
}

type ipamPrefixListResolverRuleItem struct {
	RuleType     string `xml:"ruleType,omitempty"`
	IpamScopeID  string `xml:"ipamScopeId,omitempty"`
	ResourceType string `xml:"resourceType,omitempty"`
	StaticCidr   string `xml:"staticCidr,omitempty"`
	ConditionSet struct {
		Items []ipamPrefixListResolverRuleConditionItem `xml:"item"`
	} `xml:"conditionSet"`
}

func toIpamPrefixListResolverRuleItem(r IpamPrefixListResolverRule) ipamPrefixListResolverRuleItem {
	item := ipamPrefixListResolverRuleItem{
		RuleType:     r.RuleType,
		IpamScopeID:  r.IpamScopeID,
		ResourceType: r.ResourceType,
		StaticCidr:   r.StaticCidr,
	}

	for _, c := range r.Conditions {
		item.ConditionSet.Items = append(item.ConditionSet.Items, ipamPrefixListResolverRuleConditionItem(c))
	}

	return item
}

type ipamPrefixListResolverItem struct {
	IpamPrefixListResolverID         string `xml:"ipamPrefixListResolverId,omitempty"`
	IpamPrefixListResolverARN        string `xml:"ipamPrefixListResolverArn,omitempty"`
	IpamID                           string `xml:"ipamId,omitempty"`
	IpamARN                          string `xml:"ipamArn,omitempty"`
	IpamRegion                       string `xml:"ipamRegion,omitempty"`
	OwnerID                          string `xml:"ownerId,omitempty"`
	AddressFamily                    string `xml:"addressFamily,omitempty"`
	Description                      string `xml:"description,omitempty"`
	State                            string `xml:"state,omitempty"`
	LastVersionCreationStatus        string `xml:"lastVersionCreationStatus,omitempty"`
	LastVersionCreationStatusMessage string `xml:"lastVersionCreationStatusMessage,omitempty"`
}

func toIpamPrefixListResolverItem(r *IpamPrefixListResolver) ipamPrefixListResolverItem {
	return ipamPrefixListResolverItem{
		IpamPrefixListResolverID:         r.IpamPrefixListResolverID,
		IpamPrefixListResolverARN:        r.IpamPrefixListResolverARN,
		IpamID:                           r.IpamID,
		IpamARN:                          r.IpamARN,
		IpamRegion:                       r.IpamRegion,
		OwnerID:                          r.OwnerID,
		AddressFamily:                    r.AddressFamily,
		Description:                      r.Description,
		State:                            r.State,
		LastVersionCreationStatus:        r.LastVersionCreationStatus,
		LastVersionCreationStatusMessage: r.LastVersionCreationStatusMessage,
	}
}

type createIpamPrefixListResolverResponse struct {
	XMLName                xml.Name                   `xml:"CreateIpamPrefixListResolverResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	RequestID              string                     `xml:"requestId"`
	IpamPrefixListResolver ipamPrefixListResolverItem `xml:"ipamPrefixListResolver"`
}

type deleteIpamPrefixListResolverResponse struct {
	XMLName                xml.Name                   `xml:"DeleteIpamPrefixListResolverResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	RequestID              string                     `xml:"requestId"`
	IpamPrefixListResolver ipamPrefixListResolverItem `xml:"ipamPrefixListResolver"`
}

type modifyIpamPrefixListResolverResponse struct {
	XMLName                xml.Name                   `xml:"ModifyIpamPrefixListResolverResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	RequestID              string                     `xml:"requestId"`
	IpamPrefixListResolver ipamPrefixListResolverItem `xml:"ipamPrefixListResolver"`
}

type describeIpamPrefixListResolversResponse struct {
	XMLName                   xml.Name `xml:"DescribeIpamPrefixListResolversResponse"`
	Xmlns                     string   `xml:"xmlns,attr"`
	RequestID                 string   `xml:"requestId"`
	IpamPrefixListResolverSet struct {
		Items []ipamPrefixListResolverItem `xml:"item"`
	} `xml:"ipamPrefixListResolverSet"`
}

type getIpamPrefixListResolverRulesResponse struct {
	XMLName   xml.Name `xml:"GetIpamPrefixListResolverRulesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	RuleSet   struct {
		Items []ipamPrefixListResolverRuleItem `xml:"item"`
	} `xml:"ruleSet"`
}

type ipamPrefixListResolverVersionItem struct {
	Version int64 `xml:"version"`
}

type getIpamPrefixListResolverVersionsResponse struct {
	XMLName                          xml.Name `xml:"GetIpamPrefixListResolverVersionsResponse"`
	Xmlns                            string   `xml:"xmlns,attr"`
	RequestID                        string   `xml:"requestId"`
	IpamPrefixListResolverVersionSet struct {
		Items []ipamPrefixListResolverVersionItem `xml:"item"`
	} `xml:"ipamPrefixListResolverVersionSet"`
}

type ipamPrefixListResolverVersionEntryItem struct {
	Cidr string `xml:"cidr,omitempty"`
}

type getIpamPrefixListResolverVersionEntriesResponse struct {
	XMLName   xml.Name `xml:"GetIpamPrefixListResolverVersionEntriesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	EntrySet  struct {
		Items []ipamPrefixListResolverVersionEntryItem `xml:"item"`
	} `xml:"entrySet"`
}

// ---- XML response types: Prefix List Resolver Targets ----

type ipamPrefixListResolverTargetItem struct {
	IpamPrefixListResolverTargetID  string `xml:"ipamPrefixListResolverTargetId,omitempty"`
	IpamPrefixListResolverTargetARN string `xml:"ipamPrefixListResolverTargetArn,omitempty"`
	IpamPrefixListResolverID        string `xml:"ipamPrefixListResolverId,omitempty"`
	OwnerID                         string `xml:"ownerId,omitempty"`
	PrefixListID                    string `xml:"prefixListId,omitempty"`
	PrefixListRegion                string `xml:"prefixListRegion,omitempty"`
	State                           string `xml:"state,omitempty"`
	StateMessage                    string `xml:"stateMessage,omitempty"`
	DesiredVersion                  int64  `xml:"desiredVersion,omitempty"`
	LastSyncedVersion               int64  `xml:"lastSyncedVersion,omitempty"`
	TrackLatestVersion              bool   `xml:"trackLatestVersion"`
}

func toIpamPrefixListResolverTargetItem(t *IpamPrefixListResolverTarget) ipamPrefixListResolverTargetItem {
	item := ipamPrefixListResolverTargetItem{
		IpamPrefixListResolverTargetID:  t.IpamPrefixListResolverTargetID,
		IpamPrefixListResolverTargetARN: t.IpamPrefixListResolverTargetARN,
		IpamPrefixListResolverID:        t.IpamPrefixListResolverID,
		OwnerID:                         t.OwnerID,
		PrefixListID:                    t.PrefixListID,
		PrefixListRegion:                t.PrefixListRegion,
		TrackLatestVersion:              t.TrackLatestVersion,
		State:                           t.State,
		StateMessage:                    t.StateMessage,
	}
	if t.DesiredVersion != nil {
		item.DesiredVersion = *t.DesiredVersion
	}

	if t.LastSyncedVersion != nil {
		item.LastSyncedVersion = *t.LastSyncedVersion
	}

	return item
}

type createIpamPrefixListResolverTargetResponse struct {
	XMLName                      xml.Name                         `xml:"CreateIpamPrefixListResolverTargetResponse"`
	Xmlns                        string                           `xml:"xmlns,attr"`
	RequestID                    string                           `xml:"requestId"`
	IpamPrefixListResolverTarget ipamPrefixListResolverTargetItem `xml:"ipamPrefixListResolverTarget"`
}

type deleteIpamPrefixListResolverTargetResponse struct {
	XMLName                      xml.Name                         `xml:"DeleteIpamPrefixListResolverTargetResponse"`
	Xmlns                        string                           `xml:"xmlns,attr"`
	RequestID                    string                           `xml:"requestId"`
	IpamPrefixListResolverTarget ipamPrefixListResolverTargetItem `xml:"ipamPrefixListResolverTarget"`
}

type modifyIpamPrefixListResolverTargetResponse struct {
	XMLName                      xml.Name                         `xml:"ModifyIpamPrefixListResolverTargetResponse"`
	Xmlns                        string                           `xml:"xmlns,attr"`
	RequestID                    string                           `xml:"requestId"`
	IpamPrefixListResolverTarget ipamPrefixListResolverTargetItem `xml:"ipamPrefixListResolverTarget"`
}

type describeIpamPrefixListResolverTargetsResponse struct {
	XMLName                         xml.Name `xml:"DescribeIpamPrefixListResolverTargetsResponse"`
	Xmlns                           string   `xml:"xmlns,attr"`
	RequestID                       string   `xml:"requestId"`
	IpamPrefixListResolverTargetSet struct {
		Items []ipamPrefixListResolverTargetItem `xml:"item"`
	} `xml:"ipamPrefixListResolverTargetSet"`
}

// ---- Resource Discovery handlers ----

func (h *Handler) handleCreateIpamResourceDiscovery(vals url.Values, reqID string) (any, error) {
	d, err := h.Backend.CreateIpamResourceDiscovery(
		vals.Get("Description"), parseRegionNameList(vals, "OperatingRegion"),
	)
	if err != nil {
		return nil, err
	}

	return &createIpamResourceDiscoveryResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamResourceDiscovery: toIpamResourceDiscoveryItem(d),
	}, nil
}

func (h *Handler) handleDeleteIpamResourceDiscovery(vals url.Values, reqID string) (any, error) {
	d, err := h.Backend.DeleteIpamResourceDiscovery(vals.Get("IpamResourceDiscoveryId"))
	if err != nil {
		return nil, err
	}

	return &deleteIpamResourceDiscoveryResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamResourceDiscovery: toIpamResourceDiscoveryItem(d),
	}, nil
}

func (h *Handler) handleAssociateIpamResourceDiscovery(vals url.Values, reqID string) (any, error) {
	assoc, err := h.Backend.AssociateIpamResourceDiscovery(vals.Get("IpamId"), vals.Get("IpamResourceDiscoveryId"))
	if err != nil {
		return nil, err
	}

	return &associateIpamResourceDiscoveryResponse{
		Xmlns:                            ec2XMLNS,
		RequestID:                        reqID,
		IpamResourceDiscoveryAssociation: toIpamResourceDiscoveryAssociationItem(assoc),
	}, nil
}

func (h *Handler) handleDisassociateIpamResourceDiscovery(vals url.Values, reqID string) (any, error) {
	assoc, err := h.Backend.DisassociateIpamResourceDiscovery(vals.Get("IpamResourceDiscoveryAssociationId"))
	if err != nil {
		return nil, err
	}

	return &disassociateIpamResourceDiscoveryResponse{
		Xmlns:                            ec2XMLNS,
		RequestID:                        reqID,
		IpamResourceDiscoveryAssociation: toIpamResourceDiscoveryAssociationItem(assoc),
	}, nil
}

func (h *Handler) handleModifyIpamResourceDiscovery(vals url.Values, reqID string) (any, error) {
	d, err := h.Backend.ModifyIpamResourceDiscovery(
		vals.Get("IpamResourceDiscoveryId"),
		vals.Get("Description"),
		parseRegionNameList(vals, "AddOperatingRegion"),
		parseRegionNameList(vals, "RemoveOperatingRegion"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyIpamResourceDiscoveryResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamResourceDiscovery: toIpamResourceDiscoveryItem(d),
	}, nil
}

func (h *Handler) handleGetIpamResourceCidrs(vals url.Values, reqID string) (any, error) {
	cidrs, err := h.Backend.GetIpamResourceCidrs(
		vals.Get("IpamScopeId"),
		vals.Get("IpamPoolId"),
		vals.Get("ResourceId"),
		vals.Get("ResourceOwner"),
		vals.Get("ResourceType"),
	)
	if err != nil {
		return nil, err
	}

	resp := &getIpamResourceCidrsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, c := range cidrs {
		resp.IpamResourceCidrSet.Items = append(resp.IpamResourceCidrSet.Items, toIpamResourceCidrItem(c))
	}

	return resp, nil
}

func (h *Handler) handleModifyIpamResourceCidr(vals url.Values, reqID string) (any, error) {
	c, err := h.Backend.ModifyIpamResourceCidr(
		vals.Get("CurrentIpamScopeId"),
		vals.Get("ResourceCidr"),
		vals.Get("ResourceId"),
		vals.Get("ResourceRegion"),
		vals.Get("Monitored") == ec2BooleanTrue,
		vals.Get("DestinationIpamScopeId"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyIpamResourceCidrResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamResourceCidr: toIpamResourceCidrItem(c),
	}, nil
}

// ---- BYOASN handlers ----

func (h *Handler) handleProvisionIpamByoasn(vals url.Values, reqID string) (any, error) {
	b, err := h.Backend.ProvisionIpamByoasn(vals.Get("IpamId"), vals.Get("Asn"))
	if err != nil {
		return nil, err
	}

	return &provisionIpamByoasnResponse{Xmlns: ec2XMLNS, RequestID: reqID, Byoasn: toByoasnItem(b)}, nil
}

func (h *Handler) handleDeprovisionIpamByoasn(vals url.Values, reqID string) (any, error) {
	b, err := h.Backend.DeprovisionIpamByoasn(vals.Get("IpamId"), vals.Get("Asn"))
	if err != nil {
		return nil, err
	}

	return &deprovisionIpamByoasnResponse{Xmlns: ec2XMLNS, RequestID: reqID, Byoasn: toByoasnItem(b)}, nil
}

func (h *Handler) handleDescribeIpamByoasn(_ url.Values, reqID string) (any, error) {
	items := h.Backend.DescribeIpamByoasn()

	resp := &describeIpamByoasnResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, a := range items {
		resp.ByoasnSet.Items = append(resp.ByoasnSet.Items, toByoasnItem(a))
	}

	return resp, nil
}

func (h *Handler) handleAssociateIpamByoasn(vals url.Values, reqID string) (any, error) {
	assoc, err := h.Backend.AssociateIpamByoasn(vals.Get("Asn"), vals.Get("Cidr"))
	if err != nil {
		return nil, err
	}

	return &associateIpamByoasnResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, AsnAssociation: toAsnAssociationItem(assoc),
	}, nil
}

func (h *Handler) handleDisassociateIpamByoasn(vals url.Values, reqID string) (any, error) {
	assoc, err := h.Backend.DisassociateIpamByoasn(vals.Get("Asn"), vals.Get("Cidr"))
	if err != nil {
		return nil, err
	}

	return &disassociateIpamByoasnResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, AsnAssociation: toAsnAssociationItem(assoc),
	}, nil
}

// ---- External Resource Verification Token handlers ----

func (h *Handler) handleCreateIpamExternalResourceVerificationToken(vals url.Values, reqID string) (any, error) {
	t, err := h.Backend.CreateIpamExternalResourceVerificationToken(vals.Get("IpamId"))
	if err != nil {
		return nil, err
	}

	return &createIpamExternalResourceVerificationTokenResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		Token: toIpamExternalResourceVerificationTokenItem(t),
	}, nil
}

func (h *Handler) handleDeleteIpamExternalResourceVerificationToken(vals url.Values, reqID string) (any, error) {
	t, err := h.Backend.DeleteIpamExternalResourceVerificationToken(
		vals.Get("IpamExternalResourceVerificationTokenId"),
	)
	if err != nil {
		return nil, err
	}

	return &deleteIpamExternalResourceVerificationTokenResponse{
		Xmlns: ec2XMLNS, RequestID: reqID,
		Token: toIpamExternalResourceVerificationTokenItem(t),
	}, nil
}

func (h *Handler) handleDescribeIpamExternalResourceVerificationTokens(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamExternalResourceVerificationTokenId")
	tokens := h.Backend.DescribeIpamExternalResourceVerificationTokens(ids)

	resp := &describeIpamExternalResourceVerificationTokensResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, t := range tokens {
		resp.IpamExternalResourceVerificationTokenSet.Items = append(
			resp.IpamExternalResourceVerificationTokenSet.Items,
			toIpamExternalResourceVerificationTokenItem(t),
		)
	}

	return resp, nil
}

// ---- Prefix List Resolver handlers ----

func (h *Handler) handleCreateIpamPrefixListResolver(vals url.Values, reqID string) (any, error) {
	rules, _ := parseIpamPrefixListResolverRules(vals)

	r, err := h.Backend.CreateIpamPrefixListResolver(
		vals.Get("IpamId"), vals.Get("AddressFamily"), vals.Get("Description"), rules,
	)
	if err != nil {
		return nil, err
	}

	return &createIpamPrefixListResolverResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamPrefixListResolver: toIpamPrefixListResolverItem(r),
	}, nil
}

func (h *Handler) handleDeleteIpamPrefixListResolver(vals url.Values, reqID string) (any, error) {
	r, err := h.Backend.DeleteIpamPrefixListResolver(vals.Get("IpamPrefixListResolverId"))
	if err != nil {
		return nil, err
	}

	return &deleteIpamPrefixListResolverResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamPrefixListResolver: toIpamPrefixListResolverItem(r),
	}, nil
}

func (h *Handler) handleDescribeIpamPrefixListResolvers(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamPrefixListResolverId")
	resolvers := h.Backend.DescribeIpamPrefixListResolvers(ids)

	resp := &describeIpamPrefixListResolversResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range resolvers {
		resp.IpamPrefixListResolverSet.Items = append(
			resp.IpamPrefixListResolverSet.Items, toIpamPrefixListResolverItem(r),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyIpamPrefixListResolver(vals url.Values, reqID string) (any, error) {
	rules, rulesProvided := parseIpamPrefixListResolverRules(vals)

	r, err := h.Backend.ModifyIpamPrefixListResolver(
		vals.Get("IpamPrefixListResolverId"), vals.Get("Description"), rules, rulesProvided,
	)
	if err != nil {
		return nil, err
	}

	return &modifyIpamPrefixListResolverResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamPrefixListResolver: toIpamPrefixListResolverItem(r),
	}, nil
}

func (h *Handler) handleGetIpamPrefixListResolverRules(vals url.Values, reqID string) (any, error) {
	rules, err := h.Backend.GetIpamPrefixListResolverRules(vals.Get("IpamPrefixListResolverId"))
	if err != nil {
		return nil, err
	}

	resp := &getIpamPrefixListResolverRulesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range rules {
		resp.RuleSet.Items = append(resp.RuleSet.Items, toIpamPrefixListResolverRuleItem(r))
	}

	return resp, nil
}

func (h *Handler) handleGetIpamPrefixListResolverVersions(vals url.Values, reqID string) (any, error) {
	versions, err := h.Backend.GetIpamPrefixListResolverVersions(vals.Get("IpamPrefixListResolverId"))
	if err != nil {
		return nil, err
	}

	resp := &getIpamPrefixListResolverVersionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, v := range versions {
		resp.IpamPrefixListResolverVersionSet.Items = append(
			resp.IpamPrefixListResolverVersionSet.Items, ipamPrefixListResolverVersionItem{Version: v},
		)
	}

	return resp, nil
}

func (h *Handler) handleGetIpamPrefixListResolverVersionEntries(vals url.Values, reqID string) (any, error) {
	version, _ := strconv.ParseInt(vals.Get("IpamPrefixListResolverVersion"), 10, 64)

	entries, err := h.Backend.GetIpamPrefixListResolverVersionEntries(vals.Get("IpamPrefixListResolverId"), version)
	if err != nil {
		return nil, err
	}

	resp := &getIpamPrefixListResolverVersionEntriesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, cidr := range entries {
		resp.EntrySet.Items = append(resp.EntrySet.Items, ipamPrefixListResolverVersionEntryItem{Cidr: cidr})
	}

	return resp, nil
}

// ---- Prefix List Resolver Target handlers ----

func (h *Handler) handleCreateIpamPrefixListResolverTarget(vals url.Values, reqID string) (any, error) {
	t, err := h.Backend.CreateIpamPrefixListResolverTarget(
		vals.Get("IpamPrefixListResolverId"),
		vals.Get("PrefixListId"),
		vals.Get("PrefixListRegion"),
		vals.Get("TrackLatestVersion") == ec2BooleanTrue,
		parseOptionalInt64(vals, "DesiredVersion"),
	)
	if err != nil {
		return nil, err
	}

	return &createIpamPrefixListResolverTargetResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamPrefixListResolverTarget: toIpamPrefixListResolverTargetItem(t),
	}, nil
}

func (h *Handler) handleDeleteIpamPrefixListResolverTarget(vals url.Values, reqID string) (any, error) {
	t, err := h.Backend.DeleteIpamPrefixListResolverTarget(vals.Get("IpamPrefixListResolverTargetId"))
	if err != nil {
		return nil, err
	}

	return &deleteIpamPrefixListResolverTargetResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamPrefixListResolverTarget: toIpamPrefixListResolverTargetItem(t),
	}, nil
}

func (h *Handler) handleDescribeIpamPrefixListResolverTargets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "IpamPrefixListResolverTargetId")
	targets := h.Backend.DescribeIpamPrefixListResolverTargets(vals.Get("IpamPrefixListResolverId"), ids)

	resp := &describeIpamPrefixListResolverTargetsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, t := range targets {
		resp.IpamPrefixListResolverTargetSet.Items = append(
			resp.IpamPrefixListResolverTargetSet.Items, toIpamPrefixListResolverTargetItem(t),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyIpamPrefixListResolverTarget(vals url.Values, reqID string) (any, error) {
	t, err := h.Backend.ModifyIpamPrefixListResolverTarget(
		vals.Get("IpamPrefixListResolverTargetId"),
		parseOptionalInt64(vals, "DesiredVersion"),
		parseOptionalBool(vals, "TrackLatestVersion"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyIpamPrefixListResolverTargetResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, IpamPrefixListResolverTarget: toIpamPrefixListResolverTargetItem(t),
	}, nil
}
