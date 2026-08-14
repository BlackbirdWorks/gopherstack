package ec2

import (
	"encoding/xml"
	"net/url"
)

type createManagedPrefixListResponse struct {
	XMLName    xml.Name              `xml:"CreateManagedPrefixListResponse"`
	RequestID  string                `xml:"requestId"`
	PrefixList managedPrefixListItem `xml:"prefixList"`
}

type describeManagedPrefixListsResponse struct {
	XMLName       xml.Name `xml:"DescribeManagedPrefixListsResponse"`
	RequestID     string   `xml:"requestId"`
	PrefixListSet struct {
		Items []managedPrefixListItem `xml:"item"`
	} `xml:"prefixListSet"`
}

type prefixListEntryItem struct {
	Cidr        string `xml:"cidr"`
	Description string `xml:"description,omitempty"`
}

type getManagedPrefixListEntriesResponse struct {
	XMLName   xml.Name `xml:"GetManagedPrefixListEntriesResponse"`
	RequestID string   `xml:"requestId"`
	EntrySet  struct {
		Items []prefixListEntryItem `xml:"item"`
	} `xml:"entrySet"`
}

type getManagedPrefixListAssociationsResponse struct {
	XMLName        xml.Name `xml:"GetManagedPrefixListAssociationsResponse"`
	RequestID      string   `xml:"requestId"`
	AssociationSet struct {
		Items []struct{} `xml:"item"`
	} `xml:"associationSet"`
}

// clientVpnEndpointStatusItem mirrors AWS's ClientVpnEndpointStatus shape
// (a nested <status><code>...</code></status>, not a flat string).

// clientVpnEndpointStatusItem mirrors AWS's ClientVpnEndpointStatus shape
// (a nested <status><code>...</code></status>, not a flat string).
type clientVpnEndpointStatusItem struct {
	Code string `xml:"code"`
}

// stringItemSet renders a flat list of strings as repeated <item> elements,
// e.g. <dnsServer><item>..</item></dnsServer>.

// stringItemSet renders a flat list of strings as repeated <item> elements,
// e.g. <dnsServer><item>..</item></dnsServer>.
type stringItemSet struct {
	Items []string `xml:"item"`
}

type clientVpnEndpointItem struct {
	VpcID                string                      `xml:"vpcId,omitempty"`
	SelfServicePortalURL string                      `xml:"selfServicePortalUrl,omitempty"`
	Status               clientVpnEndpointStatusItem `xml:"status"`
	Description          string                      `xml:"description,omitempty"`
	ClientCidrBlock      string                      `xml:"clientCidrBlock,omitempty"`
	DNSName              string                      `xml:"dnsName,omitempty"`
	VpnProtocol          string                      `xml:"vpnProtocol,omitempty"`
	ClientVpnEndpointID  string                      `xml:"clientVpnEndpointId"`
	TransportProtocol    string                      `xml:"transportProtocol,omitempty"`
	CreationTime         string                      `xml:"creationTime,omitempty"`
	ServerCertificateArn string                      `xml:"serverCertificateArn,omitempty"`
	DNSServers           stringItemSet               `xml:"dnsServer"`
	SecurityGroupIDSet   stringItemSet               `xml:"securityGroupIdSet"`
	VpnPort              int32                       `xml:"vpnPort,omitempty"`
	SessionTimeoutHours  int32                       `xml:"sessionTimeoutHours,omitempty"`
	SplitTunnel          bool                        `xml:"splitTunnel,omitempty"`
}

func toManagedPrefixListItem(pl *ManagedPrefixList, tags map[string]string) managedPrefixListItem {
	return managedPrefixListItem{
		PrefixListID:   pl.PrefixListID,
		PrefixListName: pl.PrefixListName,
		PrefixListArn:  pl.PrefixListArn,
		AddressFamily:  pl.AddressFamily,
		State:          pl.State,
		MaxEntries:     pl.MaxEntries,
		Version:        pl.Version,
		OwnerID:        pl.OwnerID,
		TagSet:         tagItemsFromMap(tags),
	}
}

func (h *Handler) handleCreateManagedPrefixList(vals url.Values, reqID string) (any, error) {
	name := vals.Get("PrefixListName")
	af := vals.Get("AddressFamily")
	maxEntries := 0
	if v := vals.Get("MaxEntries"); v != "" {
		parseIntValue(v, &maxEntries)
	}

	pl, err := h.Backend.CreateManagedPrefixList(name, af, maxEntries)
	if err != nil {
		return nil, err
	}

	return &createManagedPrefixListResponse{
		RequestID:  reqID,
		PrefixList: toManagedPrefixListItem(pl, h.Backend.TagsForResource(pl.PrefixListID)),
	}, nil
}

func (h *Handler) handleDeleteManagedPrefixList(vals url.Values, reqID string) (any, error) {
	id := vals.Get("PrefixListId")
	tags := h.Backend.TagsForResource(id)

	pl, err := h.Backend.DeleteManagedPrefixList(id)
	if err != nil {
		return nil, err
	}

	return &deleteManagedPrefixListResponse{
		RequestID:  reqID,
		PrefixList: toManagedPrefixListItem(pl, tags),
	}, nil
}

type deleteManagedPrefixListResponse struct {
	XMLName    xml.Name              `xml:"DeleteManagedPrefixListResponse"`
	RequestID  string                `xml:"requestId"`
	PrefixList managedPrefixListItem `xml:"prefixList"`
}

func (h *Handler) handleDescribeManagedPrefixLists(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "PrefixListId")
	pls := h.Backend.DescribeManagedPrefixLists(ids)

	resp := &describeManagedPrefixListsResponse{RequestID: reqID}
	for _, pl := range pls {
		resp.PrefixListSet.Items = append(
			resp.PrefixListSet.Items,
			toManagedPrefixListItem(pl, h.Backend.TagsForResource(pl.PrefixListID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleGetManagedPrefixListEntries(vals url.Values, reqID string) (any, error) {
	id := vals.Get("PrefixListId")
	entries, err := h.Backend.GetManagedPrefixListEntries(id)
	if err != nil {
		return nil, err
	}

	resp := &getManagedPrefixListEntriesResponse{RequestID: reqID}
	for _, e := range entries {
		resp.EntrySet.Items = append(resp.EntrySet.Items, prefixListEntryItem(e))
	}

	return resp, nil
}

func (h *Handler) handleGetManagedPrefixListAssociations(_ url.Values, reqID string) (any, error) {
	return &getManagedPrefixListAssociationsResponse{RequestID: reqID}, nil
}

func (h *Handler) handleModifyManagedPrefixList(vals url.Values, reqID string) (any, error) {
	id := vals.Get("PrefixListId")
	// Parse AddEntry.N.Cidr and RemoveEntry.N.Cidr
	var addEntries, removeEntries []PrefixListEntry
	for i := 1; ; i++ {
		cidr := vals.Get("AddEntry." + itoa(i) + ".Cidr")
		if cidr == "" {
			break
		}
		addEntries = append(addEntries, PrefixListEntry{
			Cidr:        cidr,
			Description: vals.Get("AddEntry." + itoa(i) + ".Description"),
		})
	}
	for i := 1; ; i++ {
		cidr := vals.Get("RemoveEntry." + itoa(i) + ".Cidr")
		if cidr == "" {
			break
		}
		removeEntries = append(removeEntries, PrefixListEntry{Cidr: cidr})
	}

	pl, err := h.Backend.ModifyManagedPrefixList(id, addEntries, removeEntries)
	if err != nil {
		return nil, err
	}

	return &modifyManagedPrefixListResponse{
		RequestID:  reqID,
		PrefixList: toManagedPrefixListItem(pl, h.Backend.TagsForResource(id)),
	}, nil
}

type modifyManagedPrefixListResponse struct {
	XMLName    xml.Name              `xml:"ModifyManagedPrefixListResponse"`
	RequestID  string                `xml:"requestId"`
	PrefixList managedPrefixListItem `xml:"prefixList"`
}

func (h *Handler) handleRestoreManagedPrefixListVersion(vals url.Values, reqID string) (any, error) {
	id := vals.Get("PrefixListId")
	version := 0
	parseIntValue(vals.Get("PreviousVersion"), &version)
	pl, err := h.Backend.RestoreManagedPrefixListVersion(id, int64(version))
	if err != nil {
		return nil, err
	}

	return &restoreManagedPrefixListVersionResponse{
		RequestID:  reqID,
		PrefixList: toManagedPrefixListItem(pl, h.Backend.TagsForResource(id)),
	}, nil
}

type restoreManagedPrefixListVersionResponse struct {
	XMLName    xml.Name              `xml:"RestoreManagedPrefixListVersionResponse"`
	RequestID  string                `xml:"requestId"`
	PrefixList managedPrefixListItem `xml:"prefixList"`
}

// ---- ClientVPN handlers ----

type managedPrefixListItem struct {
	PrefixListID   string          `xml:"prefixListId"`
	PrefixListName string          `xml:"prefixListName"`
	PrefixListArn  string          `xml:"prefixListArn"`
	AddressFamily  string          `xml:"addressFamily"`
	State          string          `xml:"state"`
	OwnerID        string          `xml:"ownerId"`
	TagSet         []simpleTagItem `xml:"tagSet>item"`
	Version        int64           `xml:"version"`
	MaxEntries     int             `xml:"maxEntries"`
}

// registerPrefixListsOps registers the PrefixLists operation handlers.
func registerPrefixListsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateManagedPrefixList"] = h.handleCreateManagedPrefixList
	ops["DeleteManagedPrefixList"] = h.handleDeleteManagedPrefixList
	ops["DescribeManagedPrefixLists"] = h.handleDescribeManagedPrefixLists
	ops["GetManagedPrefixListEntries"] = h.handleGetManagedPrefixListEntries
	ops["GetManagedPrefixListAssociations"] = h.handleGetManagedPrefixListAssociations
	ops["ModifyManagedPrefixList"] = h.handleModifyManagedPrefixList
	ops["RestoreManagedPrefixListVersion"] = h.handleRestoreManagedPrefixListVersion
}

// prefixListsSupportedOperations lists the operation names registered by
// registerPrefixListsOps, for GetSupportedOperations().
func prefixListsSupportedOperations() []string {
	return []string{
		"CreateManagedPrefixList",
		"DeleteManagedPrefixList",
		"DescribeManagedPrefixLists",
		"GetManagedPrefixListEntries",
		"GetManagedPrefixListAssociations",
		"ModifyManagedPrefixList",
		"RestoreManagedPrefixListVersion",
	}
}
