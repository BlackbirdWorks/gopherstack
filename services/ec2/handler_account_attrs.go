package ec2

import (
	"encoding/xml"
	"net/url"
)

type describeAccountAttributesResponse struct {
	XMLName             xml.Name `xml:"DescribeAccountAttributesResponse"`
	RequestID           string   `xml:"requestId"`
	AccountAttributeSet struct {
		Items []accountAttributeItem `xml:"item"`
	} `xml:"accountAttributeSet"`
}

type cidrItem struct {
	CIDR string `xml:"cidrIp"`
}

type prefixListItem struct {
	PrefixListID   string     `xml:"prefixListId"`
	PrefixListName string     `xml:"prefixListName"`
	CidrsSet       []cidrItem `xml:"cidrSet>item"`
}

type describePrefixListsResponse struct {
	XMLName       xml.Name `xml:"DescribePrefixListsResponse"`
	RequestID     string   `xml:"requestId"`
	PrefixListSet struct {
		Items []prefixListItem `xml:"item"`
	} `xml:"prefixListSet"`
}

type idFormatItem struct {
	Resource   string `xml:"resource"`
	UseLongIDs bool   `xml:"useLongIds"`
}

type describeIDFormatResponse struct {
	XMLName   xml.Name `xml:"DescribeIdFormatResponse"`
	RequestID string   `xml:"requestId"`
	StatusSet struct {
		Items []idFormatItem `xml:"item"`
	} `xml:"statusSet"`
}

type describeAggregateIDFormatResponse struct {
	XMLName   xml.Name `xml:"DescribeAggregateIdFormatResponse"`
	RequestID string   `xml:"requestId"`
	Statuses  struct {
		Items []idFormatItem `xml:"item"`
	} `xml:"statuses"`
	UseLongIDsAggregated bool `xml:"useLongIdsAggregated"`
}

type describePrincipalIDFormatResponse struct {
	XMLName    xml.Name `xml:"DescribePrincipalIdFormatResponse"`
	RequestID  string   `xml:"requestId"`
	Principals struct {
		Items []idFormatItem `xml:"item"`
	} `xml:"principals"`
}

type instanceEventNotifAttrsResponse struct {
	XMLName              xml.Name `xml:"DescribeInstanceEventNotificationAttributesResponse"`
	RequestID            string   `xml:"requestId"`
	InstanceTagAttribute struct {
		IncludeAllTagsOfInstance bool `xml:"includeAllTagsOfInstance"`
	} `xml:"instanceTagAttribute"`
}

// ---- Handler implementations ----

func (h *Handler) handleDescribeAccountAttributes(vals url.Values, reqID string) (any, error) {
	names := parseMemberList(vals, "AttributeName")
	attrs := h.Backend.DescribeAccountAttributes(names)

	resp := &describeAccountAttributesResponse{RequestID: reqID}
	for _, attr := range attrs {
		item := accountAttributeItem{AttributeName: attr.Name}
		for _, v := range attr.Values {
			item.AttributeValues = append(
				item.AttributeValues,
				accountAttributeValueItem{AttributeValue: v},
			)
		}
		resp.AccountAttributeSet.Items = append(resp.AccountAttributeSet.Items, item)
	}

	return resp, nil
}

func (h *Handler) handleDescribePrefixLists(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "PrefixListId")
	lists := h.Backend.DescribePrefixLists(ids)

	resp := &describePrefixListsResponse{RequestID: reqID}
	for _, pl := range lists {
		item := prefixListItem{
			PrefixListID:   pl.PrefixListID,
			PrefixListName: pl.PrefixListName,
		}
		for _, cidr := range pl.CIDRs {
			item.CidrsSet = append(item.CidrsSet, cidrItem{CIDR: cidr})
		}
		resp.PrefixListSet.Items = append(resp.PrefixListSet.Items, item)
	}

	return resp, nil
}

func (h *Handler) handleDescribeIDFormat(vals url.Values, reqID string) (any, error) {
	resources := parseMemberList(vals, "Resource")
	items := h.Backend.DescribeIDFormat(resources)

	resp := &describeIDFormatResponse{RequestID: reqID}
	for _, item := range items {
		resp.StatusSet.Items = append(resp.StatusSet.Items, idFormatItem{
			Resource:   item.Resource,
			UseLongIDs: item.UseLongIDs,
		})
	}

	return resp, nil
}

func (h *Handler) handleModifyIDFormat(vals url.Values, reqID string) (any, error) {
	resource := vals.Get("Resource")
	useLong := vals.Get("UseLongIds") == ec2BooleanTrue
	if err := h.Backend.ModifyIDFormat(resource, useLong); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIdFormatResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeIdentityIDFormat(vals url.Values, reqID string) (any, error) {
	principalARN := vals.Get("PrincipalArn")
	resources := parseMemberList(vals, "Resource")
	items := h.Backend.DescribeIdentityIDFormat(principalARN, resources)

	resp := &describeIDFormatResponse{RequestID: reqID}
	for _, item := range items {
		resp.StatusSet.Items = append(resp.StatusSet.Items, idFormatItem{
			Resource:   item.Resource,
			UseLongIDs: item.UseLongIDs,
		})
	}

	return resp, nil
}

func (h *Handler) handleModifyIdentityIDFormat(vals url.Values, reqID string) (any, error) {
	principalARN := vals.Get("PrincipalArn")
	resource := vals.Get("Resource")
	useLong := vals.Get("UseLongIds") == ec2BooleanTrue
	if err := h.Backend.ModifyIdentityIDFormat(principalARN, resource, useLong); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyIdentityIdFormatResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeAggregateIDFormat(_ url.Values, reqID string) (any, error) {
	items := h.Backend.DescribeAggregateIDFormat()
	resp := &describeAggregateIDFormatResponse{RequestID: reqID}
	for _, item := range items {
		resp.Statuses.Items = append(resp.Statuses.Items, idFormatItem{
			Resource:   item.Resource,
			UseLongIDs: item.UseLongIDs,
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribePrincipalIDFormat(vals url.Values, reqID string) (any, error) {
	principalARN := vals.Get("PrincipalArn")
	items := h.Backend.DescribePrincipalIDFormat(principalARN)
	resp := &describePrincipalIDFormatResponse{RequestID: reqID}
	for _, item := range items {
		resp.Principals.Items = append(resp.Principals.Items, idFormatItem{
			Resource:   item.Resource,
			UseLongIDs: item.UseLongIDs,
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribeInstanceEventNotificationAttributes(
	_ url.Values,
	reqID string,
) (any, error) {
	attrs := h.Backend.DescribeInstanceEventNotificationAttributes()
	resp := &instanceEventNotifAttrsResponse{RequestID: reqID}
	resp.InstanceTagAttribute.IncludeAllTagsOfInstance = attrs.IncludeAllTagsOfInstance

	return resp, nil
}

func (h *Handler) handleDeregisterInstanceEventNotificationAttributes(
	_ url.Values,
	reqID string,
) (any, error) {
	h.Backend.DeregisterInstanceEventNotificationAttributes()

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeregisterInstanceEventNotificationAttributesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleRegisterInstanceEventNotificationAttributes(
	vals url.Values,
	reqID string,
) (any, error) {
	includeAllTags := vals.Get("InstanceTagAttribute.IncludeAllTagsOfInstance") == ec2BooleanTrue
	h.Backend.RegisterInstanceEventNotificationAttributes(includeAllTags)

	return &stubResponse{
		XMLName:   xml.Name{Local: "RegisterInstanceEventNotificationAttributesResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// registerAccountAttrsOps registers the AccountAttrs operation handlers.
func registerAccountAttrsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DescribeAccountAttributes"] = h.handleDescribeAccountAttributes
	ops["DescribePrefixLists"] = h.handleDescribePrefixLists
	ops["DescribeIdFormat"] = h.handleDescribeIDFormat
	ops["ModifyIdFormat"] = h.handleModifyIDFormat
	ops["DescribeIdentityIdFormat"] = h.handleDescribeIdentityIDFormat
	ops["ModifyIdentityIdFormat"] = h.handleModifyIdentityIDFormat
	ops["DescribeAggregateIdFormat"] = h.handleDescribeAggregateIDFormat
	ops["DescribePrincipalIdFormat"] = h.handleDescribePrincipalIDFormat
	ops["DescribeInstanceEventNotificationAttributes"] = h.handleDescribeInstanceEventNotificationAttributes
	ops["DeregisterInstanceEventNotificationAttributes"] = h.handleDeregisterInstanceEventNotificationAttributes
	ops["RegisterInstanceEventNotificationAttributes"] = h.handleRegisterInstanceEventNotificationAttributes
}

// accountAttrsSupportedOperations lists the operation names registered by
// registerAccountAttrsOps, for GetSupportedOperations().
func accountAttrsSupportedOperations() []string {
	return []string{
		"DescribeAccountAttributes",
		"DescribePrefixLists",
		"DescribeIdFormat",
		"ModifyIdFormat",
		"DescribeIdentityIdFormat",
		"ModifyIdentityIdFormat",
		"DescribeAggregateIdFormat",
		"DescribePrincipalIdFormat",
		"DescribeInstanceEventNotificationAttributes",
		"DeregisterInstanceEventNotificationAttributes",
		"RegisterInstanceEventNotificationAttributes",
	}
}
