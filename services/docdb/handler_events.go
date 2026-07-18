package docdb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleAddSourceIdentifierToSubscription(ctx context.Context, vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	sourceID := vals.Get("SourceIdentifier")
	sub, err := h.Backend.AddSourceIdentifierToSubscription(ctx, subscriptionName, sourceID)
	if err != nil {
		return nil, err
	}

	return &addSourceIdentifierToSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleCreateEventSubscription(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	sourceIDs := parseSourceIDMembers(vals)
	eventCategories := parseEventCategoryMembers(vals)
	sub, err := h.Backend.CreateEventSubscription(ctx, name, snsTopicARN, sourceType, sourceIDs, eventCategories)
	if err != nil {
		return nil, err
	}

	return &createEventSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDeleteEventSubscription(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	sub, err := h.Backend.DeleteEventSubscription(ctx, name)
	if err != nil {
		return nil, err
	}

	return &deleteEventSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEventSubscriptions(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	subs := h.Backend.DescribeEventSubscriptions(ctx, name)
	members := make([]xmlEventSubscription, 0, len(subs))
	for _, sub := range subs {
		cp := sub
		members = append(members, toXMLEventSubscription(&cp))
	}

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeEventSubscriptionsResponse{
		Xmlns: docdbXMLNS,
		Result: describeEventSubscriptionsResult{
			EventSubscriptionsList: xmlEventSubscriptionList{Members: members},
			Marker:                 nextMarker,
		},
	}, nil
}

func (h *Handler) handleModifyEventSubscription(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	eventCategories := parseEventCategoryMembers(vals)
	sub, err := h.Backend.ModifyEventSubscription(ctx, name, snsTopicARN, sourceType, eventCategories)
	if err != nil {
		return nil, err
	}

	return &modifyEventSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleRemoveSourceIdentifierFromSubscription(ctx context.Context, vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	sourceID := vals.Get("SourceIdentifier")
	sub, err := h.Backend.RemoveSourceIdentifierFromSubscription(ctx, subscriptionName, sourceID)
	if err != nil {
		return nil, err
	}

	return &removeSourceIdentifierFromSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEvents(_ url.Values) (any, error) {
	return &describeEventsResponse{
		Xmlns: docdbXMLNS,
		Result: describeEventsResult{
			Events: xmlEventList{},
		},
	}, nil
}

func (h *Handler) handleDescribeEventCategories(ctx context.Context, vals url.Values) (any, error) {
	sourceType := vals.Get("SourceType")
	cats := h.Backend.DescribeEventCategories(ctx, sourceType)
	members := make([]xmlEventCategoryMap, 0, len(cats))
	for _, cat := range cats {
		catCopy := make([]string, len(cat.EventCategories))
		copy(catCopy, cat.EventCategories)
		members = append(members, xmlEventCategoryMap{
			SourceType:      cat.SourceType,
			EventCategories: xmlEventCategoryList{Members: catCopy},
		})
	}

	return &describeEventCategoriesResponse{
		Xmlns: docdbXMLNS,
		Result: describeEventCategoriesResult{
			EventCategoriesMapList: xmlEventCategoriesMapList{Members: members},
		},
	}, nil
}

type xmlSourceIDList struct {
	Members []string `xml:"SourceId"`
}

type xmlEventSubscription struct {
	SubscriptionName string          `xml:"CustSubscriptionId"`
	SnsTopicARN      string          `xml:"SnsTopicArn,omitempty"`
	SourceType       string          `xml:"SourceType,omitempty"`
	Status           string          `xml:"Status"`
	SourceIDsList    xmlSourceIDList `xml:"SourceIdsList"`
}

type addSourceIdentifierToSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"AddSourceIdentifierToSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"AddSourceIdentifierToSubscriptionResult>EventSubscription"`
}

type createEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"CreateEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"CreateEventSubscriptionResult>EventSubscription"`
}

type deleteEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"DeleteEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"DeleteEventSubscriptionResult>EventSubscription"`
}

func toXMLEventSubscription(sub *EventSubscription) xmlEventSubscription {
	ids := make([]string, len(sub.SourceIDs))
	copy(ids, sub.SourceIDs)

	return xmlEventSubscription{
		SubscriptionName: sub.SubscriptionName,
		SnsTopicARN:      sub.SnsTopicARN,
		SourceType:       sub.SourceType,
		Status:           sub.Status,
		SourceIDsList:    xmlSourceIDList{Members: ids},
	}
}

type xmlEventSubscriptionList struct {
	Members []xmlEventSubscription `xml:"EventSubscription"`
}

type describeEventSubscriptionsResult struct {
	Marker                 string                   `xml:"Marker,omitempty"`
	EventSubscriptionsList xmlEventSubscriptionList `xml:"EventSubscriptionsList"`
}

type describeEventSubscriptionsResponse struct {
	XMLName xml.Name                         `xml:"DescribeEventSubscriptionsResponse"`
	Xmlns   string                           `xml:"xmlns,attr"`
	Result  describeEventSubscriptionsResult `xml:"DescribeEventSubscriptionsResult"`
}

type modifyEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"ModifyEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"ModifyEventSubscriptionResult>EventSubscription"`
}

type removeSourceIdentifierFromSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"RemoveSourceIdentifierFromSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"RemoveSourceIdentifierFromSubscriptionResult>EventSubscription"`
}

type xmlEvent struct {
	Message    string `xml:"Message,omitempty"`
	SourceType string `xml:"SourceType,omitempty"`
}

type xmlEventList struct {
	Members []xmlEvent `xml:"Event"`
}

type describeEventsResult struct {
	Events xmlEventList `xml:"Events"`
}

type describeEventsResponse struct {
	XMLName xml.Name             `xml:"DescribeEventsResponse"`
	Xmlns   string               `xml:"xmlns,attr"`
	Result  describeEventsResult `xml:"DescribeEventsResult"`
}

type xmlEventCategoryList struct {
	Members []string `xml:"EventCategory"`
}

type xmlEventCategoryMap struct {
	SourceType      string               `xml:"SourceType"`
	EventCategories xmlEventCategoryList `xml:"EventCategories"`
}

type xmlEventCategoriesMapList struct {
	Members []xmlEventCategoryMap `xml:"EventCategoryMap"`
}

type describeEventCategoriesResult struct {
	EventCategoriesMapList xmlEventCategoriesMapList `xml:"EventCategoriesMapList"`
}

type describeEventCategoriesResponse struct {
	XMLName xml.Name                      `xml:"DescribeEventCategoriesResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  describeEventCategoriesResult `xml:"DescribeEventCategoriesResult"`
}

func parseSourceIDMembers(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("SourceIds.SourceId.%d", i))
		if id == "" {
			return ids
		}
		ids = append(ids, id)
	}
}

func parseEventCategoryMembers(vals url.Values) []string {
	var cats []string
	for i := 1; ; i++ {
		cat := vals.Get(fmt.Sprintf("EventCategories.EventCategory.%d", i))
		if cat == "" {
			return cats
		}
		cats = append(cats, cat)
	}
}
