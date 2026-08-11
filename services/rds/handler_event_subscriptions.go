package rds

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"strings"
)

func (h *Handler) handleAddSourceIdentifierToSubscription(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	sourceIdentifier := vals.Get("SourceIdentifier")

	sub, err := h.Backend.AddSourceIdentifierToSubscription(subscriptionName, sourceIdentifier)
	if err != nil {
		return nil, err
	}

	return &addSourceIdentifierToSubscriptionResponse{
		Xmlns:             rdsXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func toXMLEventSubscription(sub *EventSubscription) xmlEventSubscription {
	ids := make([]string, len(sub.SourceIDs))
	copy(ids, sub.SourceIDs)
	cats := make([]string, len(sub.EventCategories))
	copy(cats, sub.EventCategories)

	return xmlEventSubscription{
		CustSubscriptionID:   sub.SubscriptionName,
		SnsTopicArn:          sub.SnsTopicArn,
		EventSubscriptionArn: sub.EventSubscriptionArn,
		Status:               sub.Status,
		SourceType:           sub.SourceType,
		Enabled:              sub.Enabled,
		SourceIDsList:        xmlSourceIDList{Members: ids},
		EventCategoriesList:  xmlEventCategoryList{Members: cats},
	}
}

type xmlSourceIDList struct {
	Members []string `xml:"SourceId"`
}

type xmlEventCategoryList struct {
	Members []string `xml:"EventCategory"`
}

type xmlEventSubscription struct {
	CustSubscriptionID   string               `xml:"CustSubscriptionId"`
	SnsTopicArn          string               `xml:"SnsTopicArn,omitempty"`
	EventSubscriptionArn string               `xml:"EventSubscriptionArn,omitempty"`
	Status               string               `xml:"Status"`
	SourceType           string               `xml:"SourceType,omitempty"`
	SourceIDsList        xmlSourceIDList      `xml:"SourceIdsList"`
	EventCategoriesList  xmlEventCategoryList `xml:"EventCategoriesList,omitempty"`
	Enabled              bool                 `xml:"Enabled,omitempty"`
}

type addSourceIdentifierToSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"AddSourceIdentifierToSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"AddSourceIdentifierToSubscriptionResult>EventSubscription"`
}

func (h *Handler) handleRemoveSourceIdentifierFromSubscription(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	sourceIdentifier := vals.Get("SourceIdentifier")

	sub, err := h.Backend.RemoveSourceIdentifierFromSubscription(subscriptionName, sourceIdentifier)
	if err != nil {
		return nil, err
	}

	return &removeSourceIdentifierFromSubscriptionResponse{
		Xmlns:             rdsXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

type removeSourceIdentifierFromSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"RemoveSourceIdentifierFromSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"RemoveSourceIdentifierFromSubscriptionResult>EventSubscription"`
}

func (h *Handler) handleCreateEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	// rds@v1.124.1 serializers.go: SourceIds/EventCategories serialize as
	// "SourceId"/"EventCategory", not the smithy default "member".
	sourceIDs := parseMultiValueParam(vals, "SourceIds.SourceId")
	eventCategories := parseMultiValueParam(vals, "EventCategories.EventCategory")
	sub, err := h.Backend.CreateEventSubscription(name, snsTopicARN, sourceType, sourceIDs, eventCategories)
	if err != nil {
		return nil, err
	}

	h.applyCreateTags(vals, sub.EventSubscriptionArn)

	return &createEventSubscriptionResponse{
		Xmlns:             rdsXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDeleteEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	sub, err := h.Backend.DeleteEventSubscription(name)
	if err != nil {
		return nil, err
	}

	return &deleteEventSubscriptionResponse{
		Xmlns:             rdsXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEventSubscriptions(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	subs, err := h.Backend.DescribeEventSubscriptions(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlEventSubscription, 0, len(subs))
	for i := range subs {
		members = append(members, toXMLEventSubscription(&subs[i]))
	}

	return &describeEventSubscriptionsResponse{
		Xmlns:                  rdsXMLNS,
		EventSubscriptionsList: xmlEventSubscriptionList{Members: members},
	}, nil
}

func (h *Handler) handleModifyEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	// ModifyEventSubscriptionInput has no SourceIds member (rds@v1.124.1
	// serializers.go) -- only EventCategories, serialized as "EventCategory".
	eventCategories := parseMultiValueParam(vals, "EventCategories.EventCategory")
	var enabled *bool
	if v := vals.Get("Enabled"); v != "" {
		b := strings.EqualFold(v, "true")
		enabled = &b
	}
	sub, err := h.Backend.ModifyEventSubscription(name, snsTopicARN, sourceType, nil, eventCategories, enabled)
	if err != nil {
		return nil, err
	}

	return &modifyEventSubscriptionResponse{
		Xmlns:             rdsXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEvents(vals url.Values) (any, error) {
	sourceID := vals.Get("SourceIdentifier")
	sourceType := vals.Get("SourceType")
	durationMinutes := 0
	if v := vals.Get("Duration"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			durationMinutes = n
		}
	}
	events, err := h.Backend.DescribeEvents(sourceID, sourceType, durationMinutes)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, events, func(a, b Event) bool {
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}

		return a.SourceIdentifier < b.SourceIdentifier
	}, func(ev Event) xmlEvent {
		return xmlEvent{
			SourceIdentifier: ev.SourceIdentifier,
			SourceType:       ev.SourceType,
			Message:          ev.Message,
			Date:             ev.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		}
	})
	if err != nil {
		return nil, err
	}

	return &describeEventsResponse{
		Xmlns:  rdsXMLNS,
		Marker: marker,
		Events: xmlEventList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeEventCategories(vals url.Values) (any, error) {
	sourceType := vals.Get("SourceType")
	cats, err := h.Backend.DescribeEventCategories(sourceType)
	if err != nil {
		return nil, err
	}
	catMap := xmlEventCategoriesMap{
		SourceType:      sourceType,
		EventCategories: xmlStringList{Members: cats},
	}

	return &describeEventCategoriesResponse{
		Xmlns:                  rdsXMLNS,
		EventCategoriesMapList: xmlEventCategoriesMapList{Members: []xmlEventCategoriesMap{catMap}},
	}, nil
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

type xmlEventSubscriptionList struct {
	Members []xmlEventSubscription `xml:"EventSubscription"`
}

type describeEventSubscriptionsResponse struct {
	XMLName                xml.Name                 `xml:"DescribeEventSubscriptionsResponse"`
	Xmlns                  string                   `xml:"xmlns,attr"`
	EventSubscriptionsList xmlEventSubscriptionList `xml:"DescribeEventSubscriptionsResult>EventSubscriptionsList"`
}

type modifyEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"ModifyEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"ModifyEventSubscriptionResult>EventSubscription"`
}

type xmlEvent struct {
	SourceIdentifier string `xml:"SourceIdentifier,omitempty"`
	SourceType       string `xml:"SourceType,omitempty"`
	Message          string `xml:"Message,omitempty"`
	Date             string `xml:"Date,omitempty"`
}

type xmlEventList struct {
	Members []xmlEvent `xml:"Event"`
}

type describeEventsResponse struct {
	XMLName xml.Name     `xml:"DescribeEventsResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Marker  string       `xml:"DescribeEventsResult>Marker,omitempty"`
	Events  xmlEventList `xml:"DescribeEventsResult>Events"`
}

type xmlStringList struct {
	Members []string `xml:"member"`
}

type xmlEventCategoriesMap struct {
	SourceType      string        `xml:"SourceType,omitempty"`
	EventCategories xmlStringList `xml:"EventCategories"`
}

type xmlEventCategoriesMapList struct {
	Members []xmlEventCategoriesMap `xml:"EventCategoriesMap"`
}

type describeEventCategoriesResponse struct {
	XMLName                xml.Name                  `xml:"DescribeEventCategoriesResponse"`
	Xmlns                  string                    `xml:"xmlns,attr"`
	EventCategoriesMapList xmlEventCategoriesMapList `xml:"DescribeEventCategoriesResult>EventCategoriesMapList"`
}
