package ec2

import (
	"encoding/xml"
	"net/url"
)

type createVerifiedAccessEndpointResponse struct {
	XMLName                xml.Name                   `xml:"CreateVerifiedAccessEndpointResponse"`
	RequestID              string                     `xml:"requestId"`
	VerifiedAccessEndpoint verifiedAccessEndpointItem `xml:"verifiedAccessEndpoint"`
}

type describeVerifiedAccessEndpointsResponse struct {
	XMLName                   xml.Name `xml:"DescribeVerifiedAccessEndpointsResponse"`
	RequestID                 string   `xml:"requestId"`
	VerifiedAccessEndpointSet struct {
		Items []verifiedAccessEndpointItem `xml:"item"`
	} `xml:"verifiedAccessEndpointSet"`
}

type verifiedAccessGroupItem struct {
	VerifiedAccessGroupID    string          `xml:"verifiedAccessGroupId"`
	VerifiedAccessInstanceID string          `xml:"verifiedAccessInstanceId"`
	Status                   string          `xml:"status"`
	Description              string          `xml:"description,omitempty"`
	TagSet                   []simpleTagItem `xml:"tagSet>item"`
}

// toVerifiedAccessGroupItem converts a backend VerifiedAccessGroup into its
// wire item, including any tags applied via the shared CreateTags op.
func (h *Handler) toVerifiedAccessGroupItem(grp *VerifiedAccessGroup) verifiedAccessGroupItem {
	return verifiedAccessGroupItem{
		VerifiedAccessGroupID:    grp.VerifiedAccessGroupID,
		VerifiedAccessInstanceID: grp.VerifiedAccessInstanceID,
		Status:                   grp.Status,
		Description:              grp.Description,
		TagSet:                   tagItemsFromMap(h.Backend.TagsForResource(grp.VerifiedAccessGroupID)),
	}
}

type createVerifiedAccessGroupResponse struct {
	XMLName             xml.Name                `xml:"CreateVerifiedAccessGroupResponse"`
	RequestID           string                  `xml:"requestId"`
	VerifiedAccessGroup verifiedAccessGroupItem `xml:"verifiedAccessGroup"`
}

type describeVerifiedAccessGroupsResponse struct {
	XMLName                xml.Name `xml:"DescribeVerifiedAccessGroupsResponse"`
	RequestID              string   `xml:"requestId"`
	VerifiedAccessGroupSet struct {
		Items []verifiedAccessGroupItem `xml:"item"`
	} `xml:"verifiedAccessGroupSet"`
}

type verifiedAccessTrustProviderCondensedItem struct {
	VerifiedAccessTrustProviderID string `xml:"verifiedAccessTrustProviderId,omitempty"`
	TrustProviderType             string `xml:"trustProviderType,omitempty"`
	Description                   string `xml:"description,omitempty"`
}

type verifiedAccessInstanceItem struct {
	VerifiedAccessInstanceID       string `xml:"verifiedAccessInstanceId"`
	Status                         string `xml:"status"`
	Description                    string `xml:"description,omitempty"`
	VerifiedAccessTrustProviderSet struct {
		Items []verifiedAccessTrustProviderCondensedItem `xml:"item"`
	} `xml:"verifiedAccessTrustProviderSet"`
	TagSet []simpleTagItem `xml:"tagSet>item"`
}

// toVerifiedAccessInstanceItem converts a backend VerifiedAccessInstance into
// its wire item, resolving AttachedTrustProviderIDs into the condensed
// verifiedAccessTrustProviderSet the real DescribeVerifiedAccessInstances /
// CreateVerifiedAccessInstance / ModifyVerifiedAccessInstance responses carry
// (VerifiedAccessInstance.VerifiedAccessTrustProviders in the SDK).
func (h *Handler) toVerifiedAccessInstanceItem(inst *VerifiedAccessInstance) verifiedAccessInstanceItem {
	item := verifiedAccessInstanceItem{
		VerifiedAccessInstanceID: inst.VerifiedAccessInstanceID,
		Status:                   inst.Status,
		Description:              inst.Description,
		TagSet:                   tagItemsFromMap(h.Backend.TagsForResource(inst.VerifiedAccessInstanceID)),
	}
	if len(inst.AttachedTrustProviderIDs) == 0 {
		return item
	}
	// DescribeVerifiedAccessTrustProviders(nil) returns every trust provider
	// in the account, not none, so the empty-list case must short-circuit above.
	for _, tp := range h.Backend.DescribeVerifiedAccessTrustProviders(inst.AttachedTrustProviderIDs) {
		item.VerifiedAccessTrustProviderSet.Items = append(
			item.VerifiedAccessTrustProviderSet.Items,
			verifiedAccessTrustProviderCondensedItem{
				VerifiedAccessTrustProviderID: tp.VerifiedAccessTrustProviderID,
				TrustProviderType:             tp.TrustProviderType,
				Description:                   tp.Description,
			},
		)
	}

	return item
}

type createVerifiedAccessInstanceResponse struct {
	XMLName                xml.Name                   `xml:"CreateVerifiedAccessInstanceResponse"`
	RequestID              string                     `xml:"requestId"`
	VerifiedAccessInstance verifiedAccessInstanceItem `xml:"verifiedAccessInstance"`
}

type describeVerifiedAccessInstancesResponse struct {
	XMLName                   xml.Name `xml:"DescribeVerifiedAccessInstancesResponse"`
	RequestID                 string   `xml:"requestId"`
	VerifiedAccessInstanceSet struct {
		Items []verifiedAccessInstanceItem `xml:"item"`
	} `xml:"verifiedAccessInstanceSet"`
}

type verifiedAccessTrustProviderItem struct {
	VerifiedAccessTrustProviderID string          `xml:"verifiedAccessTrustProviderId"`
	TrustProviderType             string          `xml:"trustProviderType"`
	Status                        string          `xml:"status"`
	Description                   string          `xml:"description,omitempty"`
	TagSet                        []simpleTagItem `xml:"tagSet>item"`
}

type createVerifiedAccessTrustProviderResponse struct {
	XMLName                     xml.Name                        `xml:"CreateVerifiedAccessTrustProviderResponse"`
	RequestID                   string                          `xml:"requestId"`
	VerifiedAccessTrustProvider verifiedAccessTrustProviderItem `xml:"verifiedAccessTrustProvider"`
}

type describeVerifiedAccessTrustProvidersResponse struct {
	XMLName                        xml.Name `xml:"DescribeVerifiedAccessTrustProvidersResponse"`
	RequestID                      string   `xml:"requestId"`
	VerifiedAccessTrustProviderSet struct {
		Items []verifiedAccessTrustProviderItem `xml:"item"`
	} `xml:"verifiedAccessTrustProviderSet"`
}

// ---- ManagedPrefixList handlers ----

func (h *Handler) handleCreateVerifiedAccessEndpoint(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("VerifiedAccessGroupId")
	endpointType := vals.Get("EndpointType")
	description := vals.Get("Description")

	ep, err := h.Backend.CreateVerifiedAccessEndpoint(groupID, endpointType, description)
	if err != nil {
		return nil, err
	}

	return &createVerifiedAccessEndpointResponse{
		RequestID:              reqID,
		VerifiedAccessEndpoint: h.toVerifiedAccessEndpointItem(ep),
	}, nil
}

func (h *Handler) handleDeleteVerifiedAccessEndpoint(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessEndpointId")
	ep, err := h.Backend.DeleteVerifiedAccessEndpoint(id)
	if err != nil {
		return nil, err
	}

	return &deleteVerifiedAccessEndpointResponse{
		RequestID:              reqID,
		VerifiedAccessEndpoint: h.toVerifiedAccessEndpointItem(ep),
	}, nil
}

type deleteVerifiedAccessEndpointResponse struct {
	XMLName                xml.Name                   `xml:"DeleteVerifiedAccessEndpointResponse"`
	RequestID              string                     `xml:"requestId"`
	VerifiedAccessEndpoint verifiedAccessEndpointItem `xml:"verifiedAccessEndpoint"`
}

func (h *Handler) handleDescribeVerifiedAccessEndpoints(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VerifiedAccessEndpointId")
	eps := h.Backend.DescribeVerifiedAccessEndpoints(ids)

	resp := &describeVerifiedAccessEndpointsResponse{RequestID: reqID}
	for _, ep := range eps {
		resp.VerifiedAccessEndpointSet.Items = append(
			resp.VerifiedAccessEndpointSet.Items,
			h.toVerifiedAccessEndpointItem(ep),
		)
	}

	return resp, nil
}

func (h *Handler) handleModifyVerifiedAccessEndpoint(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessEndpointId")
	description := vals.Get("Description")
	ep, err := h.Backend.ModifyVerifiedAccessEndpoint(id, description)
	if err != nil {
		return nil, err
	}

	return &modifyVerifiedAccessEndpointResponse{
		RequestID:              reqID,
		VerifiedAccessEndpoint: h.toVerifiedAccessEndpointItem(ep),
	}, nil
}

type modifyVerifiedAccessEndpointResponse struct {
	XMLName                xml.Name                   `xml:"ModifyVerifiedAccessEndpointResponse"`
	RequestID              string                     `xml:"requestId"`
	VerifiedAccessEndpoint verifiedAccessEndpointItem `xml:"verifiedAccessEndpoint"`
}

func (h *Handler) handleCreateVerifiedAccessGroup(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("VerifiedAccessInstanceId")
	description := vals.Get("Description")

	grp, err := h.Backend.CreateVerifiedAccessGroup(instanceID, description)
	if err != nil {
		return nil, err
	}

	return &createVerifiedAccessGroupResponse{
		RequestID:           reqID,
		VerifiedAccessGroup: h.toVerifiedAccessGroupItem(grp),
	}, nil
}

func (h *Handler) handleDeleteVerifiedAccessGroup(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessGroupId")
	grp, err := h.Backend.DeleteVerifiedAccessGroup(id)
	if err != nil {
		return nil, err
	}

	return &deleteVerifiedAccessGroupResponse{
		RequestID:           reqID,
		VerifiedAccessGroup: h.toVerifiedAccessGroupItem(grp),
	}, nil
}

type deleteVerifiedAccessGroupResponse struct {
	XMLName             xml.Name                `xml:"DeleteVerifiedAccessGroupResponse"`
	RequestID           string                  `xml:"requestId"`
	VerifiedAccessGroup verifiedAccessGroupItem `xml:"verifiedAccessGroup"`
}

func (h *Handler) handleDescribeVerifiedAccessGroups(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VerifiedAccessGroupId")
	groups := h.Backend.DescribeVerifiedAccessGroups(ids)

	resp := &describeVerifiedAccessGroupsResponse{RequestID: reqID}
	for _, grp := range groups {
		resp.VerifiedAccessGroupSet.Items = append(
			resp.VerifiedAccessGroupSet.Items,
			h.toVerifiedAccessGroupItem(grp),
		)
	}

	return resp, nil
}

func (h *Handler) handleCreateVerifiedAccessInstance(vals url.Values, reqID string) (any, error) {
	description := vals.Get("Description")

	inst, err := h.Backend.CreateVerifiedAccessInstance(description)
	if err != nil {
		return nil, err
	}

	return &createVerifiedAccessInstanceResponse{
		RequestID:              reqID,
		VerifiedAccessInstance: h.toVerifiedAccessInstanceItem(inst),
	}, nil
}

func (h *Handler) handleDeleteVerifiedAccessInstance(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessInstanceId")
	inst, err := h.Backend.DeleteVerifiedAccessInstance(id)
	if err != nil {
		return nil, err
	}

	return &deleteVerifiedAccessInstanceResponse{
		RequestID:              reqID,
		VerifiedAccessInstance: h.toVerifiedAccessInstanceItem(inst),
	}, nil
}

type deleteVerifiedAccessInstanceResponse struct {
	XMLName                xml.Name                   `xml:"DeleteVerifiedAccessInstanceResponse"`
	RequestID              string                     `xml:"requestId"`
	VerifiedAccessInstance verifiedAccessInstanceItem `xml:"verifiedAccessInstance"`
}

func (h *Handler) handleDescribeVerifiedAccessInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VerifiedAccessInstanceId")
	instances := h.Backend.DescribeVerifiedAccessInstances(ids)

	resp := &describeVerifiedAccessInstancesResponse{RequestID: reqID}
	for _, inst := range instances {
		resp.VerifiedAccessInstanceSet.Items = append(
			resp.VerifiedAccessInstanceSet.Items,
			h.toVerifiedAccessInstanceItem(inst),
		)
	}

	return resp, nil
}

func (h *Handler) handleCreateVerifiedAccessTrustProvider(vals url.Values, reqID string) (any, error) {
	providerType := vals.Get("TrustProviderType")
	description := vals.Get("Description")

	tp, err := h.Backend.CreateVerifiedAccessTrustProvider(providerType, description)
	if err != nil {
		return nil, err
	}

	return &createVerifiedAccessTrustProviderResponse{
		RequestID:                   reqID,
		VerifiedAccessTrustProvider: h.toVerifiedAccessTrustProviderItem(tp),
	}, nil
}

func (h *Handler) handleDeleteVerifiedAccessTrustProvider(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessTrustProviderId")
	tp, err := h.Backend.DeleteVerifiedAccessTrustProvider(id)
	if err != nil {
		return nil, err
	}

	return &deleteVerifiedAccessTrustProviderResponse{
		RequestID:                   reqID,
		VerifiedAccessTrustProvider: h.toVerifiedAccessTrustProviderItem(tp),
	}, nil
}

type deleteVerifiedAccessTrustProviderResponse struct {
	XMLName                     xml.Name                        `xml:"DeleteVerifiedAccessTrustProviderResponse"`
	RequestID                   string                          `xml:"requestId"`
	VerifiedAccessTrustProvider verifiedAccessTrustProviderItem `xml:"verifiedAccessTrustProvider"`
}

func (h *Handler) handleDescribeVerifiedAccessTrustProviders(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "VerifiedAccessTrustProviderId")
	providers := h.Backend.DescribeVerifiedAccessTrustProviders(ids)

	resp := &describeVerifiedAccessTrustProvidersResponse{RequestID: reqID}
	for _, tp := range providers {
		resp.VerifiedAccessTrustProviderSet.Items = append(
			resp.VerifiedAccessTrustProviderSet.Items,
			h.toVerifiedAccessTrustProviderItem(tp),
		)
	}

	return resp, nil
}

type attachVerifiedAccessTrustProviderResponse struct {
	VerifiedAccessTrustProvider verifiedAccessTrustProviderItem `xml:"verifiedAccessTrustProvider"`
	XMLName                     xml.Name                        `xml:"AttachVerifiedAccessTrustProviderResponse"`
	RequestID                   string                          `xml:"requestId"`
	VerifiedAccessInstance      verifiedAccessInstanceItem      `xml:"verifiedAccessInstance"`
}

type detachVerifiedAccessTrustProviderResponse struct {
	VerifiedAccessTrustProvider verifiedAccessTrustProviderItem `xml:"verifiedAccessTrustProvider"`
	XMLName                     xml.Name                        `xml:"DetachVerifiedAccessTrustProviderResponse"`
	RequestID                   string                          `xml:"requestId"`
	VerifiedAccessInstance      verifiedAccessInstanceItem      `xml:"verifiedAccessInstance"`
}

// toVerifiedAccessTrustProviderItem converts a backend VerifiedAccessTrustProvider
// into its wire item, including any tags applied via the shared CreateTags op.
func (h *Handler) toVerifiedAccessTrustProviderItem(tp *VerifiedAccessTrustProvider) verifiedAccessTrustProviderItem {
	return verifiedAccessTrustProviderItem{
		VerifiedAccessTrustProviderID: tp.VerifiedAccessTrustProviderID,
		TrustProviderType:             tp.TrustProviderType,
		Status:                        tp.Status,
		Description:                   tp.Description,
		TagSet:                        tagItemsFromMap(h.Backend.TagsForResource(tp.VerifiedAccessTrustProviderID)),
	}
}

func (h *Handler) handleAttachVerifiedAccessTrustProvider(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("VerifiedAccessInstanceId")
	providerID := vals.Get("VerifiedAccessTrustProviderId")
	if err := h.Backend.AttachVerifiedAccessTrustProvider(instanceID, providerID); err != nil {
		return nil, err
	}

	inst, tp := h.describeVerifiedAccessInstanceAndProvider(instanceID, providerID)

	return &attachVerifiedAccessTrustProviderResponse{
		RequestID:                   reqID,
		VerifiedAccessInstance:      inst,
		VerifiedAccessTrustProvider: tp,
	}, nil
}

func (h *Handler) handleDetachVerifiedAccessTrustProvider(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("VerifiedAccessInstanceId")
	providerID := vals.Get("VerifiedAccessTrustProviderId")
	if err := h.Backend.DetachVerifiedAccessTrustProvider(instanceID, providerID); err != nil {
		return nil, err
	}

	inst, tp := h.describeVerifiedAccessInstanceAndProvider(instanceID, providerID)

	return &detachVerifiedAccessTrustProviderResponse{
		RequestID:                   reqID,
		VerifiedAccessInstance:      inst,
		VerifiedAccessTrustProvider: tp,
	}, nil
}

// describeVerifiedAccessInstanceAndProvider re-fetches the instance and trust
// provider after an attach/detach, matching the real
// Attach/DetachVerifiedAccessTrustProviderOutput shape (both carry the
// post-operation VerifiedAccessInstance and VerifiedAccessTrustProvider, not
// a bare boolean).
func (h *Handler) describeVerifiedAccessInstanceAndProvider(
	instanceID, providerID string,
) (verifiedAccessInstanceItem, verifiedAccessTrustProviderItem) {
	var instItem verifiedAccessInstanceItem
	if insts := h.Backend.DescribeVerifiedAccessInstances([]string{instanceID}); len(insts) > 0 {
		instItem = h.toVerifiedAccessInstanceItem(insts[0])
	}

	var tpItem verifiedAccessTrustProviderItem
	if tps := h.Backend.DescribeVerifiedAccessTrustProviders([]string{providerID}); len(tps) > 0 {
		tpItem = h.toVerifiedAccessTrustProviderItem(tps[0])
	}

	return instItem, tpItem
}

// ---- Helpers ----

// itoa converts an int to decimal string.

type modifyVerifiedAccessGroupResponse struct {
	XMLName             xml.Name                `xml:"ModifyVerifiedAccessGroupResponse"`
	RequestID           string                  `xml:"requestId"`
	VerifiedAccessGroup verifiedAccessGroupItem `xml:"verifiedAccessGroup"`
}

func (h *Handler) handleModifyVerifiedAccessGroup(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessGroupId")
	instanceID := vals.Get("VerifiedAccessInstanceId")
	description := vals.Get("Description")

	grp, err := h.Backend.ModifyVerifiedAccessGroup(id, instanceID, description)
	if err != nil {
		return nil, err
	}

	return &modifyVerifiedAccessGroupResponse{
		RequestID: reqID,
		VerifiedAccessGroup: verifiedAccessGroupItem{
			VerifiedAccessGroupID:    grp.VerifiedAccessGroupID,
			VerifiedAccessInstanceID: grp.VerifiedAccessInstanceID,
			Status:                   grp.Status,
			Description:              grp.Description,
		},
	}, nil
}

type modifyVerifiedAccessInstanceResponse struct {
	XMLName                xml.Name                   `xml:"ModifyVerifiedAccessInstanceResponse"`
	RequestID              string                     `xml:"requestId"`
	VerifiedAccessInstance verifiedAccessInstanceItem `xml:"verifiedAccessInstance"`
}

func (h *Handler) handleModifyVerifiedAccessInstance(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessInstanceId")
	description := vals.Get("Description")

	inst, err := h.Backend.ModifyVerifiedAccessInstance(id, description)
	if err != nil {
		return nil, err
	}

	return &modifyVerifiedAccessInstanceResponse{
		RequestID:              reqID,
		VerifiedAccessInstance: h.toVerifiedAccessInstanceItem(inst),
	}, nil
}

type modifyVerifiedAccessTrustProviderResponse struct {
	XMLName                     xml.Name                        `xml:"ModifyVerifiedAccessTrustProviderResponse"`
	RequestID                   string                          `xml:"requestId"`
	VerifiedAccessTrustProvider verifiedAccessTrustProviderItem `xml:"verifiedAccessTrustProvider"`
}

func (h *Handler) handleModifyVerifiedAccessTrustProvider(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VerifiedAccessTrustProviderId")
	description := vals.Get("Description")

	tp, err := h.Backend.ModifyVerifiedAccessTrustProvider(id, description)
	if err != nil {
		return nil, err
	}

	return &modifyVerifiedAccessTrustProviderResponse{
		RequestID: reqID,
		VerifiedAccessTrustProvider: verifiedAccessTrustProviderItem{
			VerifiedAccessTrustProviderID: tp.VerifiedAccessTrustProviderID,
			TrustProviderType:             tp.TrustProviderType,
			Status:                        tp.Status,
			Description:                   tp.Description,
		},
	}, nil
}

// ---- Transit Gateway route propagation + unified attachment describe ----

type tgwRouteTablePropagationItem struct {
	ResourceID                             string `xml:"resourceId,omitempty"`
	ResourceType                           string `xml:"resourceType,omitempty"`
	State                                  string `xml:"state"`
	TransitGatewayAttachmentID             string `xml:"transitGatewayAttachmentId,omitempty"`
	TransitGatewayRouteTableAnnouncementID string `xml:"transitGatewayRouteTableAnnouncementId,omitempty"`
}

// registerVerifiedAccessOps registers the VerifiedAccess operation handlers.
func registerVerifiedAccessOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateVerifiedAccessEndpoint"] = h.handleCreateVerifiedAccessEndpoint
	ops["DeleteVerifiedAccessEndpoint"] = h.handleDeleteVerifiedAccessEndpoint
	ops["DescribeVerifiedAccessEndpoints"] = h.handleDescribeVerifiedAccessEndpoints
	ops["ModifyVerifiedAccessEndpoint"] = h.handleModifyVerifiedAccessEndpoint
	ops["CreateVerifiedAccessGroup"] = h.handleCreateVerifiedAccessGroup
	ops["DeleteVerifiedAccessGroup"] = h.handleDeleteVerifiedAccessGroup
	ops["DescribeVerifiedAccessGroups"] = h.handleDescribeVerifiedAccessGroups
	ops["CreateVerifiedAccessInstance"] = h.handleCreateVerifiedAccessInstance
	ops["DeleteVerifiedAccessInstance"] = h.handleDeleteVerifiedAccessInstance
	ops["DescribeVerifiedAccessInstances"] = h.handleDescribeVerifiedAccessInstances
	ops["CreateVerifiedAccessTrustProvider"] = h.handleCreateVerifiedAccessTrustProvider
	ops["DeleteVerifiedAccessTrustProvider"] = h.handleDeleteVerifiedAccessTrustProvider
	ops["DescribeVerifiedAccessTrustProviders"] = h.handleDescribeVerifiedAccessTrustProviders
	ops["AttachVerifiedAccessTrustProvider"] = h.handleAttachVerifiedAccessTrustProvider
	ops["DetachVerifiedAccessTrustProvider"] = h.handleDetachVerifiedAccessTrustProvider
	ops["ModifyVerifiedAccessGroup"] = h.handleModifyVerifiedAccessGroup
	ops["ModifyVerifiedAccessInstance"] = h.handleModifyVerifiedAccessInstance
	ops["ModifyVerifiedAccessTrustProvider"] = h.handleModifyVerifiedAccessTrustProvider
}

// verifiedAccessSupportedOperations lists the operation names registered by
// registerVerifiedAccessOps, for GetSupportedOperations().
func verifiedAccessSupportedOperations() []string {
	return []string{
		"CreateVerifiedAccessEndpoint",
		"DeleteVerifiedAccessEndpoint",
		"DescribeVerifiedAccessEndpoints",
		"ModifyVerifiedAccessEndpoint",
		"CreateVerifiedAccessGroup",
		"DeleteVerifiedAccessGroup",
		"DescribeVerifiedAccessGroups",
		"CreateVerifiedAccessInstance",
		"DeleteVerifiedAccessInstance",
		"DescribeVerifiedAccessInstances",
		"CreateVerifiedAccessTrustProvider",
		"DeleteVerifiedAccessTrustProvider",
		"DescribeVerifiedAccessTrustProviders",
		"AttachVerifiedAccessTrustProvider",
		"DetachVerifiedAccessTrustProvider",
		"ModifyVerifiedAccessGroup",
		"ModifyVerifiedAccessInstance",
		"ModifyVerifiedAccessTrustProvider",
	}
}
