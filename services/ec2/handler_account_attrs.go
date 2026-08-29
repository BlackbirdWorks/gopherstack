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

type prefixListItem struct {
	PrefixListID   string   `xml:"prefixListId"`
	PrefixListName string   `xml:"prefixListName"`
	CidrsSet       []string `xml:"cidrSet>item"`
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

// describeAggregateIDFormatResponse wraps its list under statusSet
// (deserializers.go:196919), not "statuses" -- the real client's
// deserializer only matches "statusSet" and would otherwise decode an empty
// Statuses slice.
type describeAggregateIDFormatResponse struct {
	XMLName   xml.Name `xml:"DescribeAggregateIdFormatResponse"`
	RequestID string   `xml:"requestId"`
	Statuses  struct {
		Items []idFormatItem `xml:"item"`
	} `xml:"statusSet"`
	UseLongIDsAggregated bool `xml:"useLongIdsAggregated"`
}

// principalIDFormatItem matches types.PrincipalIdFormat (ec2@v1.319.1
// deserializers.go:143696): a principal ARN plus its per-resource-type ID
// format statuses, not a flat idFormatItem list.
type principalIDFormatItem struct {
	Arn       string `xml:"arn,omitempty"`
	StatusSet struct {
		Items []idFormatItem `xml:"item"`
	} `xml:"statusSet"`
}

// describePrincipalIDFormatResponse wraps its list under principalSet
// (deserializers.go:203012), not "principals" -- the real client's
// deserializer only matches "principalSet" and would otherwise decode an
// empty Principals slice.
type describePrincipalIDFormatResponse struct {
	XMLName      xml.Name `xml:"DescribePrincipalIdFormatResponse"`
	RequestID    string   `xml:"requestId"`
	PrincipalSet struct {
		Items []principalIDFormatItem `xml:"item"`
	} `xml:"principalSet"`
}

// instanceEventNotifAttrsResponse is shared by Describe/Register/Deregister
// InstanceEventNotificationAttributes, which all share this exact shape.
// XMLName carries no tag -- a tagged XMLName field's tag wins unconditionally
// over a runtime-set value in encoding/xml's Marshal, which would silently
// force every response to one hardcoded root element name.
type instanceEventNotifAttrsResponse struct {
	XMLName              xml.Name
	RequestID            string                   `xml:"requestId"`
	InstanceTagAttribute instanceTagAttributeItem `xml:"instanceTagAttribute"`
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
		item.CidrsSet = append(item.CidrsSet, pl.CIDRs...)
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
	principal := principalIDFormatItem{Arn: principalARN}
	for _, item := range items {
		principal.StatusSet.Items = append(principal.StatusSet.Items, idFormatItem{
			Resource:   item.Resource,
			UseLongIDs: item.UseLongIDs,
		})
	}
	resp.PrincipalSet.Items = append(resp.PrincipalSet.Items, principal)

	return resp, nil
}

func (h *Handler) handleDescribeInstanceEventNotificationAttributes(
	_ url.Values,
	reqID string,
) (any, error) {
	attrs := h.Backend.DescribeInstanceEventNotificationAttributes()
	resp := &instanceEventNotifAttrsResponse{
		XMLName:   xml.Name{Local: "DescribeInstanceEventNotificationAttributesResponse"},
		RequestID: reqID,
	}
	resp.InstanceTagAttribute.IncludeAllTagsOfInstance = attrs.IncludeAllTagsOfInstance

	return resp, nil
}

// instanceTagAttributeItem matches types.InstanceTagNotificationAttribute
// (ec2@v1.319.1 types/types.go). InstanceTagKeys is never populated: this
// backend only tracks the all-tags boolean (account_attrs.go's
// InstanceEventNotificationAttributes), not individually registered keys.
type instanceTagAttributeItem struct {
	InstanceTagKeys          []string `xml:"instanceTagKeySet>item,omitempty"`
	IncludeAllTagsOfInstance bool     `xml:"includeAllTagsOfInstance"`
}

func (h *Handler) handleDeregisterInstanceEventNotificationAttributes(
	_ url.Values,
	reqID string,
) (any, error) {
	h.Backend.DeregisterInstanceEventNotificationAttributes()

	return &instanceEventNotifAttrsResponse{
		XMLName:              xml.Name{Local: "DeregisterInstanceEventNotificationAttributesResponse"},
		RequestID:            reqID,
		InstanceTagAttribute: instanceTagAttributeItem{},
	}, nil
}

func (h *Handler) handleRegisterInstanceEventNotificationAttributes(
	vals url.Values,
	reqID string,
) (any, error) {
	includeAllTags := vals.Get("InstanceTagAttribute.IncludeAllTagsOfInstance") == ec2BooleanTrue
	h.Backend.RegisterInstanceEventNotificationAttributes(includeAllTags)

	return &instanceEventNotifAttrsResponse{
		XMLName:              xml.Name{Local: "RegisterInstanceEventNotificationAttributesResponse"},
		RequestID:            reqID,
		InstanceTagAttribute: instanceTagAttributeItem{IncludeAllTagsOfInstance: includeAllTags},
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
