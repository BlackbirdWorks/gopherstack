package redshift

import (
	"encoding/xml"
	"net/url"
	"time"
)

const (
	valFalse = "false"
)

// ---- Logging XML types ----

type xmlLoggingStatus struct {
	BucketName     string `xml:"BucketName,omitempty"`
	S3KeyPrefix    string `xml:"S3KeyPrefix,omitempty"`
	LoggingEnabled bool   `xml:"LoggingEnabled"`
}

// ---- EnableLogging ----

type enableLoggingResponse struct {
	XMLName xml.Name         `xml:"EnableLoggingResponse"`
	Xmlns   string           `xml:"xmlns,attr"`
	Result  xmlLoggingStatus `xml:"EnableLoggingResult"`
}

func (h *Handler) handleEnableLogging(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	bucketName := vals.Get("BucketName")
	s3KeyPrefix := vals.Get("S3KeyPrefix")

	status, err := h.Backend.EnableLogging(clusterID, bucketName, s3KeyPrefix)
	if err != nil {
		return nil, err
	}

	return &enableLoggingResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlLoggingStatus{
			LoggingEnabled: status.LoggingEnabled,
			BucketName:     status.BucketName,
			S3KeyPrefix:    status.S3KeyPrefix,
		},
	}, nil
}

// ---- DisableLogging ----

type disableLoggingResponse struct {
	XMLName xml.Name         `xml:"DisableLoggingResponse"`
	Xmlns   string           `xml:"xmlns,attr"`
	Result  xmlLoggingStatus `xml:"DisableLoggingResult"`
}

func (h *Handler) handleDisableLogging(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")

	status, err := h.Backend.DisableLogging(clusterID)
	if err != nil {
		return nil, err
	}

	return &disableLoggingResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlLoggingStatus{
			LoggingEnabled: status.LoggingEnabled,
		},
	}, nil
}

// ---- Events XML types ----

type xmlEvent struct {
	Date             string `xml:"Date,omitempty"`
	SourceIdentifier string `xml:"SourceIdentifier,omitempty"`
	SourceType       string `xml:"SourceType,omitempty"`
	Message          string `xml:"Message,omitempty"`
	EventID          string `xml:"EventId,omitempty"`
	Severity         string `xml:"Severity,omitempty"`
}

type xmlEventList struct {
	Members []xmlEvent `xml:"Event"`
}

// ---- DescribeEvents ----

type describeEventsResponse struct {
	XMLName xml.Name     `xml:"DescribeEventsResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Events  xmlEventList `xml:"DescribeEventsResult>Events"`
}

func (h *Handler) handleDescribeEvents(vals url.Values) (any, error) {
	sourceID := vals.Get("SourceIdentifier")
	sourceType := vals.Get("SourceType")

	events, err := h.Backend.DescribeEvents(sourceID, sourceType)
	if err != nil {
		return nil, err
	}

	members := make([]xmlEvent, 0, len(events))
	for _, e := range events {
		members = append(members, xmlEvent{
			Date:             e.Date.Format(time.RFC3339),
			SourceIdentifier: e.SourceIdentifier,
			SourceType:       e.SourceType,
			Message:          e.Message,
			EventID:          e.EventID,
			Severity:         e.Severity,
		})
	}

	return &describeEventsResponse{
		Xmlns:  redshiftXMLNS,
		Events: xmlEventList{Members: members},
	}, nil
}

// ---- DescribeEventCategories ----

type xmlEventInfoMap struct {
	EventCategory     string   `xml:"EventCategory"`
	SourceType        string   `xml:"SourceType"`
	EventDescriptions []string `xml:"EventInfoMapList>EventInfoMap>EventDescription,omitempty"`
}

type xmlEventCategoriesResult struct {
	EventCategoriesMapList []xmlEventInfoMap `xml:"EventCategoriesMapList>EventCategoriesMap"`
}

type describeEventCategoriesResponse struct {
	XMLName xml.Name                 `xml:"DescribeEventCategoriesResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Result  xmlEventCategoriesResult `xml:"DescribeEventCategoriesResult"`
}

func (h *Handler) handleDescribeEventCategories(_ url.Values) (any, error) {
	return &describeEventCategoriesResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlEventCategoriesResult{
			EventCategoriesMapList: []xmlEventInfoMap{
				{SourceType: keyResourceCluster, EventCategory: "maintenance"},
				{SourceType: keyResourceCluster, EventCategory: "monitoring"},
				{SourceType: keyResourceCluster, EventCategory: "security"},
				{SourceType: "cluster-snapshot", EventCategory: "backup"},
				{SourceType: "cluster-parameter-group", EventCategory: "configuration"},
				{SourceType: "cluster-security-group", EventCategory: "configuration"},
			},
		},
	}, nil
}

// ---- EventSubscription XML types ----

type xmlEventSubscription struct {
	SubscriptionCreationTime string   `xml:"SubscriptionCreationTime,omitempty"`
	CustSubscriptionID       string   `xml:"CustSubscriptionId"`
	CustomerAwsID            string   `xml:"CustomerAwsId,omitempty"`
	SnsTopicArn              string   `xml:"SnsTopicArn"`
	Status                   string   `xml:"Status"`
	SourceType               string   `xml:"SourceType,omitempty"`
	Severity                 string   `xml:"Severity,omitempty"`
	SourceIDs                []string `xml:"SourceIdsList>SourceId,omitempty"`
	EventCategories          []string `xml:"EventCategoriesList>EventCategory,omitempty"`
	Enabled                  bool     `xml:"Enabled"`
}

type xmlEventSubscriptionList struct {
	Members []xmlEventSubscription `xml:"EventSubscription"`
}

func eventSubscriptionToXML(sub *EventSubscription) xmlEventSubscription {
	x := xmlEventSubscription{
		CustSubscriptionID: sub.CustSubscriptionID,
		CustomerAwsID:      sub.CustomerAwsID,
		SnsTopicArn:        sub.SnsTopicArn,
		Status:             sub.Status,
		SourceType:         sub.SourceType,
		Severity:           sub.Severity,
		SourceIDs:          sub.SourceIDs,
		EventCategories:    sub.EventCategories,
		Enabled:            sub.Enabled,
	}

	if !sub.SubscriptionCreated.IsZero() {
		x.SubscriptionCreationTime = sub.SubscriptionCreated.Format(time.RFC3339)
	}

	return x
}

// ---- CreateEventSubscription ----

type createEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"CreateEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"CreateEventSubscriptionResult>EventSubscription"`
}

func (h *Handler) handleCreateEventSubscription(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	snsTopicArn := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	severity := vals.Get("Severity")
	sourceIDs := parseStringList(vals, "SourceIds.SourceId.")
	eventCategories := parseStringList(vals, "EventCategories.EventCategory.")
	enabled := vals.Get("Enabled") != valFalse

	sub, err := h.Backend.CreateEventSubscription(
		subscriptionName, snsTopicArn, sourceType, severity,
		sourceIDs, eventCategories, enabled,
	)
	if err != nil {
		return nil, err
	}

	return &createEventSubscriptionResponse{
		Xmlns:             redshiftXMLNS,
		EventSubscription: eventSubscriptionToXML(sub),
	}, nil
}

// ---- DeleteEventSubscription ----

type deleteEventSubscriptionResponse struct {
	XMLName xml.Name `xml:"DeleteEventSubscriptionResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteEventSubscription(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")

	if err := h.Backend.DeleteEventSubscription(subscriptionName); err != nil {
		return nil, err
	}

	return &deleteEventSubscriptionResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeEventSubscriptions ----

type describeEventSubscriptionsResponse struct {
	XMLName            xml.Name                 `xml:"DescribeEventSubscriptionsResponse"`
	Xmlns              string                   `xml:"xmlns,attr"`
	EventSubscriptions xmlEventSubscriptionList `xml:"DescribeEventSubscriptionsResult>EventSubscriptionsList"`
}

func (h *Handler) handleDescribeEventSubscriptions(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")

	subs, err := h.Backend.DescribeEventSubscriptions(subscriptionName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlEventSubscription, 0, len(subs))
	for _, s := range subs {
		sp := s
		members = append(members, eventSubscriptionToXML(&sp))
	}

	return &describeEventSubscriptionsResponse{
		Xmlns:              redshiftXMLNS,
		EventSubscriptions: xmlEventSubscriptionList{Members: members},
	}, nil
}

// ---- ModifyEventSubscription ----

type modifyEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"ModifyEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"ModifyEventSubscriptionResult>EventSubscription"`
}

func (h *Handler) handleModifyEventSubscription(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	snsTopicArn := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	severity := vals.Get("Severity")
	sourceIDs := parseStringList(vals, "SourceIds.SourceId.")
	eventCategories := parseStringList(vals, "EventCategories.EventCategory.")

	var enabled *bool
	if v := vals.Get("Enabled"); v != "" {
		b := v != valFalse
		enabled = &b
	}

	sub, err := h.Backend.ModifyEventSubscription(
		subscriptionName, snsTopicArn, sourceType, severity,
		sourceIDs, eventCategories, enabled,
	)
	if err != nil {
		return nil, err
	}

	return &modifyEventSubscriptionResponse{
		Xmlns:             redshiftXMLNS,
		EventSubscription: eventSubscriptionToXML(sub),
	}, nil
}
