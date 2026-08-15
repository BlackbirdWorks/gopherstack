package neptune

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

func (h *Handler) handleAddSourceIdentifierToSubscription(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	name := vals.Get("SubscriptionName")
	sourceID := vals.Get("SourceIdentifier")
	sub, err := h.Backend.AddSourceIdentifierToSubscription(ctx, name, sourceID)
	if err != nil {
		return nil, err
	}

	return &addSourceIdentifierToSubscriptionResponse{
		Xmlns:             neptuneXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleCreateEventSubscription(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	enabled := vals.Get("Enabled") != "false"
	sourceIDs := parseSourceIDMembers(vals)
	tags := parseTagEntries(vals)
	if err := validateTagEntries(tags); err != nil {
		return nil, err
	}
	sub, err := h.Backend.CreateEventSubscription(
		ctx,
		name,
		snsTopicARN,
		sourceType,
		sourceIDs,
		enabled,
	)
	if err != nil {
		return nil, err
	}
	if len(tags) > 0 {
		_ = h.Backend.AddTagsToResource(ctx, sub.EventSubscriptionArn, tags)
	}

	return &createEventSubscriptionResponse{
		Xmlns:             neptuneXMLNS,
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
		Xmlns:             neptuneXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEventSubscriptions(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	name := vals.Get("SubscriptionName")
	subs, err := h.Backend.DescribeEventSubscriptions(ctx, name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlEventSubscription, 0, len(subs))
	for _, sub := range subs {
		cp := sub
		members = append(members, toXMLEventSubscription(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeEventSubscriptionsResponse{
		Xmlns: neptuneXMLNS,
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
	enabled := vals.Get("Enabled")
	eventCategories := parseMemberList(vals, "EventCategories.member")
	sub, err := h.Backend.ModifyEventSubscription(
		ctx, name, snsTopicARN, sourceType, enabled, eventCategories,
	)
	if err != nil {
		return nil, err
	}

	return &modifyEventSubscriptionResponse{
		Xmlns:             neptuneXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleRemoveSourceIdentifierFromSubscription(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	name := vals.Get("SubscriptionName")
	sourceID := vals.Get("SourceIdentifier")
	sub, err := h.Backend.RemoveSourceIdentifierFromSubscription(ctx, name, sourceID)
	if err != nil {
		return nil, err
	}

	return &removeSourceIdentifierFromSubscriptionResponse{
		Xmlns:             neptuneXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEventCategories(_ context.Context, _ url.Values) (any, error) {
	return &describeEventCategoriesResponse{
		Xmlns: neptuneXMLNS,
		EventCategoriesMapList: xmlEventCategoriesMapList{
			Members: []xmlEventCategoriesMap{
				{SourceType: "db-cluster", EventCategories: xmlEventCategoryList{Members: []string{
					"failover", "maintenance", sourceTypeNotification, "failure", "availability",
				}}},
				{SourceType: "db-instance", EventCategories: xmlEventCategoryList{Members: []string{
					"availability", "deletion", "failover", "failure", "maintenance",
					sourceTypeNotification, "recovery", "restoration",
				}}},
				{
					SourceType: "db-parameter-group",
					EventCategories: xmlEventCategoryList{Members: []string{
						"configuration change",
					}},
				},
				{
					SourceType: "db-cluster-snapshot",
					EventCategories: xmlEventCategoryList{Members: []string{
						"backup", sourceTypeNotification,
					}},
				},
			},
		},
	}, nil
}

func (h *Handler) handleDescribeEvents(ctx context.Context, vals url.Values) (any, error) {
	duration := 0
	if d := vals.Get("Duration"); d != "" {
		if v, err := strconv.Atoi(d); err == nil {
			duration = v
		}
	}
	filter := EventsFilter{
		SourceIdentifier: vals.Get("SourceIdentifier"),
		SourceType:       vals.Get("SourceType"),
		StartTime:        vals.Get("StartTime"),
		EndTime:          vals.Get("EndTime"),
		Duration:         duration,
		EventCategories:  parseMemberList(vals, "EventCategories.member"),
	}
	events := h.Backend.DescribeEvents(ctx, filter)
	members := make([]xmlEvent, 0, len(events))
	for _, e := range events {
		members = append(members, toXMLEvent(&e))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeEventsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeEventsResult{
			Events: xmlEventList{Members: members},
			Marker: nextMarker,
		},
	}, nil
}

// toXMLEvent renders an Event as its wire shape.
func toXMLEvent(e *Event) xmlEvent {
	cats := make([]string, len(e.EventCategories))
	copy(cats, e.EventCategories)

	return xmlEvent{
		SourceIdentifier: e.SourceIdentifier,
		SourceType:       e.SourceType,
		Message:          e.Message,
		Date:             e.Date,
		EventCategories:  xmlEventCategoryItemList{Members: cats},
	}
}

func toXMLEventSubscription(sub *EventSubscription) xmlEventSubscription {
	ids := make([]xmlSourceID, 0, len(sub.SourceIDs))
	for _, id := range sub.SourceIDs {
		ids = append(ids, xmlSourceID{Member: id})
	}
	cats := make([]string, len(sub.EventCategoriesList))
	copy(cats, sub.EventCategoriesList)

	return xmlEventSubscription{
		CustSubscriptionID:       sub.CustSubscriptionID,
		CustomerAwsID:            sub.CustomerAwsID,
		EventSubscriptionArn:     sub.EventSubscriptionArn,
		SnsTopicARN:              sub.SnsTopicARN,
		Status:                   sub.Status,
		SourceType:               sub.SourceType,
		SubscriptionCreationTime: sub.SubscriptionCreationTime,
		SourceIDs:                xmlEventSourceIDList{Members: ids},
		EventCategoriesList:      xmlEventCategoryItemList{Members: cats},
		Enabled:                  sub.Enabled,
	}
}

// The real request serializer (neptune@v1.48.4 serializers.go:5980,
// awsAwsquery_serializeDocumentSourceIdsList) encodes each entry as
// "SourceIds.SourceId.N", not "SourceIds.member.N".
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

type xmlSourceID struct {
	Member string `xml:",chardata"`
}

// xmlSourceIDList backs DBClusterEndpoint.StaticMembers/ExcludedMembers,
// which decode via the generic StringList deserializer (neptune@v1.48.4
// deserializers.go:22316, wraps each entry in <member>).
type xmlSourceIDList struct {
	Members []xmlSourceID `xml:"member"`
}

// xmlEventSourceIDList backs EventSubscription.SourceIds, a distinct AWS
// shape (SourceIdsList) from the StringList used for StaticMembers/
// ExcludedMembers above: it wraps each entry in <SourceId>, not <member>
// (neptune@v1.48.4 deserializers.go:22089).
type xmlEventSourceIDList struct {
	Members []xmlSourceID `xml:"SourceId"`
}

type xmlEventCategoryItemList struct {
	Members []string `xml:"EventCategory"`
}

type xmlEventSubscription struct {
	CustSubscriptionID       string                   `xml:"CustSubscriptionId"`
	CustomerAwsID            string                   `xml:"CustomerAwsId,omitempty"`
	EventSubscriptionArn     string                   `xml:"EventSubscriptionArn,omitempty"`
	SnsTopicARN              string                   `xml:"SnsTopicArn"`
	Status                   string                   `xml:"Status"`
	SourceType               string                   `xml:"SourceType,omitempty"`
	SubscriptionCreationTime string                   `xml:"SubscriptionCreationTime,omitempty"`
	SourceIDs                xmlEventSourceIDList     `xml:"SourceIdsList"`
	EventCategoriesList      xmlEventCategoryItemList `xml:"EventCategoriesList"`
	Enabled                  bool                     `xml:"Enabled"`
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

type deleteEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"DeleteEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"DeleteEventSubscriptionResult>EventSubscription"`
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

type xmlEventCategoryList struct {
	Members []string `xml:"EventCategory"`
}

type xmlEventCategoriesMap struct {
	SourceType      string               `xml:"SourceType"`
	EventCategories xmlEventCategoryList `xml:"EventCategories"`
}

type xmlEventCategoriesMapList struct {
	Members []xmlEventCategoriesMap `xml:"EventCategoriesMap"`
}

type describeEventCategoriesResponse struct {
	XMLName                xml.Name                  `xml:"DescribeEventCategoriesResponse"`
	Xmlns                  string                    `xml:"xmlns,attr"`
	EventCategoriesMapList xmlEventCategoriesMapList `xml:"DescribeEventCategoriesResult>EventCategoriesMapList"`
}

type xmlEvent struct {
	SourceIdentifier string                   `xml:"SourceIdentifier,omitempty"`
	SourceType       string                   `xml:"SourceType,omitempty"`
	Message          string                   `xml:"Message,omitempty"`
	Date             string                   `xml:"Date,omitempty"`
	EventCategories  xmlEventCategoryItemList `xml:"EventCategories"`
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

// dispatchEventSubscriptionAction handles EventSubscription actions; see
// dispatch's doc comment for the chaining rationale.
func (h *Handler) dispatchEventSubscriptionAction(
	ctx context.Context, action string, vals url.Values,
) (any, error) {
	switch action {
	case "AddSourceIdentifierToSubscription":
		return h.handleAddSourceIdentifierToSubscription(ctx, vals)
	case "CreateEventSubscription":
		return h.handleCreateEventSubscription(ctx, vals)
	case "DeleteEventSubscription":
		return h.handleDeleteEventSubscription(ctx, vals)
	case "DescribeEventSubscriptions":
		return h.handleDescribeEventSubscriptions(ctx, vals)
	case "ModifyEventSubscription":
		return h.handleModifyEventSubscription(ctx, vals)
	case "RemoveSourceIdentifierFromSubscription":
		return h.handleRemoveSourceIdentifierFromSubscription(ctx, vals)
	case "DescribeEventCategories":
		return h.handleDescribeEventCategories(ctx, vals)
	case "DescribeEvents":
		return h.handleDescribeEvents(ctx, vals)
	default:
		return h.dispatchGlobalClusterAndTagAction(ctx, action, vals)
	}
}
