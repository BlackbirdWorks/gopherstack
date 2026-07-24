package docdb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
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
	// NB: CreateEventSubscription's backend signature takes
	// (eventCategories, sourceIDs) in that order -- a real, previously
	// undetected bug had these two swapped positionally here, so a real
	// client's SourceIds silently came back as EventCategories and vice
	// versa (both are []string, so nothing type-checked away the mistake).
	sourceIDs := parseSourceIDMembers(vals)
	eventCategories := parseEventCategoryMembers(vals)
	enabled := parseBoolParam(vals, "Enabled")
	sub, err := h.Backend.CreateEventSubscription(
		ctx, name, snsTopicARN, sourceType, eventCategories, sourceIDs, enabled,
	)
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
	enabled := parseBoolParam(vals, "Enabled")
	sub, err := h.Backend.ModifyEventSubscription(ctx, name, snsTopicARN, sourceType, eventCategories, enabled)
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

func (h *Handler) handleDescribeEvents(ctx context.Context, vals url.Values) (any, error) {
	filter := EventsFilter{
		SourceIdentifier: vals.Get("SourceIdentifier"),
		SourceType:       vals.Get("SourceType"),
		StartTime:        vals.Get("StartTime"),
		EndTime:          vals.Get("EndTime"),
		EventCategories:  parseEventCategoryMembers(vals),
	}
	if d := vals.Get("Duration"); d != "" {
		if n, err := strconv.Atoi(d); err == nil {
			filter.Duration = n
		}
	}
	events := h.Backend.DescribeEvents(ctx, filter)
	members := make([]xmlEvent, 0, len(events))
	for _, e := range events {
		members = append(members, toXMLEvent(&e))
	}
	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeEventsResponse{
		Xmlns: docdbXMLNS,
		Result: describeEventsResult{
			Events: xmlEventList{Members: members},
			Marker: nextMarker,
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

// xmlEventSubscription mirrors types.EventSubscription's full wire shape
// (awsAwsquery_deserializeDocumentEventSubscription). EventCategoriesList/
// EventSubscriptionArn/Enabled/CustomerAwsId/SubscriptionCreationTime were
// previously entirely absent from this struct -- a real caller reading back
// the event categories or ARN it just set on Create/Modify always saw them
// silently dropped, even though the backend tracked EventCategories
// correctly internally.
type xmlEventSubscription struct {
	SubscriptionName         string               `xml:"CustSubscriptionId"`
	SnsTopicARN              string               `xml:"SnsTopicArn,omitempty"`
	SourceType               string               `xml:"SourceType,omitempty"`
	Status                   string               `xml:"Status"`
	EventSubscriptionArn     string               `xml:"EventSubscriptionArn,omitempty"`
	CustomerAwsID            string               `xml:"CustomerAwsId,omitempty"`
	SubscriptionCreationTime string               `xml:"SubscriptionCreationTime,omitempty"`
	SourceIDsList            xmlSourceIDList      `xml:"SourceIdsList"`
	EventCategoriesList      xmlEventCategoryList `xml:"EventCategoriesList"`
	Enabled                  bool                 `xml:"Enabled"`
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
	cats := make([]string, len(sub.EventCategories))
	copy(cats, sub.EventCategories)

	return xmlEventSubscription{
		SubscriptionName:         sub.SubscriptionName,
		SnsTopicARN:              sub.SnsTopicARN,
		SourceType:               sub.SourceType,
		Status:                   sub.Status,
		EventSubscriptionArn:     sub.EventSubscriptionArn,
		CustomerAwsID:            sub.CustomerAwsID,
		SubscriptionCreationTime: sub.SubscriptionCreationTime,
		SourceIDsList:            xmlSourceIDList{Members: ids},
		EventCategoriesList:      xmlEventCategoryList{Members: cats},
		Enabled:                  sub.Enabled,
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

// xmlEvent mirrors types.Event's full wire shape
// (awsAwsquery_deserializeDocumentEvent): Date/EventCategories/Message/
// SourceArn/SourceIdentifier/SourceType. Date/SourceIdentifier/EventCategories
// were previously entirely absent -- DescribeEvents always answered an empty
// list regardless (see events_log.go), so this struct was never actually
// exercised against real event data until now.
type xmlEvent struct {
	Message          string               `xml:"Message,omitempty"`
	SourceType       string               `xml:"SourceType,omitempty"`
	SourceIdentifier string               `xml:"SourceIdentifier,omitempty"`
	SourceArn        string               `xml:"SourceArn,omitempty"`
	Date             string               `xml:"Date,omitempty"`
	EventCategories  xmlEventCategoryList `xml:"EventCategories"`
}

func toXMLEvent(e *Event) xmlEvent {
	cats := make([]string, len(e.EventCategories))
	copy(cats, e.EventCategories)

	return xmlEvent{
		Message:          e.Message,
		SourceType:       e.SourceType,
		SourceIdentifier: e.SourceIdentifier,
		SourceArn:        e.SourceArn,
		Date:             e.Date,
		EventCategories:  xmlEventCategoryList{Members: cats},
	}
}

type xmlEventList struct {
	Members []xmlEvent `xml:"Event"`
}

type describeEventsResult struct {
	Marker string       `xml:"Marker,omitempty"`
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
