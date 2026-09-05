package dms

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createEventSubscriptionInput struct {
	SubscriptionName *string    `json:"SubscriptionName"`
	SnsTopicArn      *string    `json:"SnsTopicArn"`
	SourceType       *string    `json:"SourceType"`
	SourceIDs        []string   `json:"SourceIds"`
	EventCategories  []string   `json:"EventCategories"`
	Enabled          *bool      `json:"Enabled"`
	Tags             []tagEntry `json:"Tags"`
}

// eventSubscriptionJSON is the wire shape of types.EventSubscription. Unlike
// CreateEventSubscriptionMessage/CreateEventSubscriptionInput (which use
// SubscriptionName/EventCategories on the request), the real EventSubscription
// response type uses CustSubscriptionId and EventCategoriesList -- the
// asymmetry is genuine AWS behavior, not a naming bug. Don't "fix" the two
// to match each other.
type eventSubscriptionJSON struct {
	CustSubscriptionID  string   `json:"CustSubscriptionId"`
	SnsTopicArn         string   `json:"SnsTopicArn"`
	SourceType          string   `json:"SourceType,omitempty"`
	Status              string   `json:"Status"`
	SourceIDsList       []string `json:"SourceIdsList"`
	EventCategoriesList []string `json:"EventCategoriesList"`
	Enabled             bool     `json:"Enabled"`
}

type createEventSubscriptionOutput struct {
	EventSubscription eventSubscriptionJSON `json:"EventSubscription"`
}

func (h *Handler) handleCreateEventSubscription(
	ctx context.Context, in *createEventSubscriptionInput,
) (*createEventSubscriptionOutput, error) {
	name := ptrconv.String(in.SubscriptionName)
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrValidation)
	}

	snsTopicArn := ptrconv.String(in.SnsTopicArn)
	if snsTopicArn == "" {
		return nil, fmt.Errorf("%w: SnsTopicArn is required", ErrValidation)
	}

	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}

	kv := tagsToMap(in.Tags)
	es, err := h.Backend.CreateEventSubscription(
		ctx,
		name,
		snsTopicArn,
		ptrconv.String(in.SourceType),
		in.SourceIDs,
		in.EventCategories,
		enabled,
		kv,
	)
	if err != nil {
		return nil, err
	}

	return &createEventSubscriptionOutput{EventSubscription: esToJSON(es)}, nil
}

func esToJSON(es *EventSubscription) eventSubscriptionJSON {
	return eventSubscriptionJSON{
		CustSubscriptionID:  es.SubscriptionName,
		SnsTopicArn:         es.SnsTopicArn,
		SourceType:          es.SourceType,
		SourceIDsList:       ensureNonNil(es.SourceIDsList),
		EventCategoriesList: ensureNonNil(es.EventCategories),
		Status:              es.Status,
		Enabled:             es.Enabled,
	}
}

type deleteEventSubscriptionInput struct {
	SubscriptionName *string `json:"SubscriptionName"`
}

type deleteEventSubscriptionOutput struct {
	EventSubscription eventSubscriptionJSON `json:"EventSubscription"`
}

func (h *Handler) handleDeleteEventSubscription(
	ctx context.Context, in *deleteEventSubscriptionInput,
) (*deleteEventSubscriptionOutput, error) {
	es, err := h.Backend.DeleteEventSubscription(ctx, ptrconv.String(in.SubscriptionName))
	if err != nil {
		return nil, err
	}

	return &deleteEventSubscriptionOutput{EventSubscription: esToJSON(es)}, nil
}

type describeEventCategoriesInput struct {
	SourceType *string       `json:"SourceType"`
	Filters    []filterEntry `json:"Filters"`
}

type describeEventCategoriesOutput struct {
	EventCategoryGroupList []map[string]any `json:"EventCategoryGroupList"`
}

type eventCategoryGroupJSON struct {
	SourceType      string   `json:"SourceType"`
	EventCategories []string `json:"EventCategories"`
}

func dmsEventCategoryGroupList() []eventCategoryGroupJSON {
	return []eventCategoryGroupJSON{
		{
			SourceType: "replication-instance",
			EventCategories: []string{
				"low storage",
				"configuration change",
				"maintenance",
				"deletion",
				"creation",
				"failover",
				"failure",
			},
		},
		{
			SourceType: "replication-task",
			EventCategories: []string{
				"state change",
				"configuration change",
				"deletion",
				"creation",
				"failure",
			},
		},
	}
}

func (h *Handler) handleDescribeEventCategories(
	_ context.Context, in *describeEventCategoriesInput,
) (*describeEventCategoriesOutput, error) {
	sourceType := ptrconv.String(in.SourceType)
	if sourceType == "" {
		sourceType = extractFilterValue(in.Filters, "source-type")
	}

	groups := dmsEventCategoryGroupList()
	result := make([]eventCategoryGroupJSON, 0, len(groups))

	for _, group := range groups {
		if sourceType == "" || group.SourceType == sourceType {
			result = append(result, group)
		}
	}

	out := make([]map[string]any, 0, len(result))
	for _, g := range result {
		out = append(out, map[string]any{
			"SourceType":      g.SourceType,
			"EventCategories": g.EventCategories,
		})
	}

	return &describeEventCategoriesOutput{EventCategoryGroupList: out}, nil
}

type describeEventSubscriptionsInput struct {
	SubscriptionName *string       `json:"SubscriptionName"`
	Marker           *string       `json:"Marker"`
	MaxRecords       *int32        `json:"MaxRecords"`
	Filters          []filterEntry `json:"Filters"`
}

type describeEventSubscriptionsOutput struct {
	Marker                 *string                 `json:"Marker,omitempty"`
	EventSubscriptionsList []eventSubscriptionJSON `json:"EventSubscriptionsList"`
}

func (h *Handler) handleDescribeEventSubscriptions(
	ctx context.Context, in *describeEventSubscriptionsInput,
) (*describeEventSubscriptionsOutput, error) {
	// EventSubscription has no distinct ARN in this emulation (see
	// eventSubscriptionJSON), so event-subscription-arn and
	// event-subscription-id both resolve against SubscriptionName, the only
	// identifier that exists.
	name := ptrconv.String(in.SubscriptionName)
	if name == "" {
		name = extractFilterValue(in.Filters, "event-subscription-arn", "event-subscription-id")
	}

	list, err := h.Backend.DescribeEventSubscriptions(ctx, name)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].SubscriptionName < list[j].SubscriptionName
	})

	all := make([]eventSubscriptionJSON, 0, len(list))
	for _, es := range list {
		all = append(all, esToJSON(es))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeEventSubscriptionsOutput{EventSubscriptionsList: data, Marker: nextMarker}, nil
}

type describeEventsInput struct {
	Marker           *string       `json:"Marker"`
	MaxRecords       *int32        `json:"MaxRecords"`
	Filters          []filterEntry `json:"Filters"`
	SourceIdentifier *string       `json:"SourceIdentifier"`
	SourceType       *string       `json:"SourceType"`
	StartTime        *float64      `json:"StartTime"`
	EndTime          *float64      `json:"EndTime"`
	EventCategories  []string      `json:"EventCategories"`
}

type describeEventsOutput struct {
	Marker *string          `json:"Marker,omitempty"`
	Events []map[string]any `json:"Events"`
}

// eventCategoriesIntersect reports whether any category in want also
// appears in have.
func eventCategoriesIntersect(have, want []string) bool {
	for _, w := range want {
		if slices.Contains(have, w) {
			return true
		}
	}

	return false
}

// eventFilters holds DescribeEvents' constraining request members --
// "The only valid filter is replication-instance-id" per DescribeEventsInput,
// so SourceIdentifier/SourceType/StartTime/EndTime/EventCategories are
// separate top-level request members, not part of Filters. riFilter and
// sourceIdentifier are kept distinct (rather than merged) because a real
// client could set both to different values, and AWS's semantics for that
// combination is "no event matches", not "the more specific one wins".
type eventFilters struct {
	startTime        time.Time
	endTime          time.Time
	riFilter         string
	sourceIdentifier string
	sourceType       string
	categories       []string
}

func eventFiltersFrom(in *describeEventsInput) eventFilters {
	f := eventFilters{
		riFilter:         extractFilterValue(in.Filters, "replication-instance-id"),
		sourceIdentifier: ptrconv.String(in.SourceIdentifier),
		sourceType:       ptrconv.String(in.SourceType),
		categories:       in.EventCategories,
	}

	if in.StartTime != nil {
		f.startTime = time.Unix(0, int64(*in.StartTime*float64(time.Second))).UTC()
	}

	if in.EndTime != nil {
		f.endTime = time.Unix(0, int64(*in.EndTime*float64(time.Second))).UTC()
	}

	return f
}

func (f eventFilters) matchesIdentifiers(e *Event) bool {
	if f.riFilter != "" && e.SourceIdentifier != f.riFilter {
		return false
	}

	if f.sourceIdentifier != "" && e.SourceIdentifier != f.sourceIdentifier {
		return false
	}

	return f.sourceType == "" || e.SourceType == f.sourceType
}

func (f eventFilters) matchesWindow(e *Event) bool {
	if !f.startTime.IsZero() && e.Date.Before(f.startTime) {
		return false
	}

	return f.endTime.IsZero() || !e.Date.After(f.endTime)
}

func (f eventFilters) matches(e *Event) bool {
	if !f.matchesIdentifiers(e) || !f.matchesWindow(e) {
		return false
	}

	return len(f.categories) == 0 || eventCategoriesIntersect(e.EventCategories, f.categories)
}

func (h *Handler) handleDescribeEvents(
	ctx context.Context, in *describeEventsInput,
) (*describeEventsOutput, error) {
	list, err := h.Backend.DescribeEvents(ctx)
	if err != nil {
		return nil, err
	}

	filters := eventFiltersFrom(in)

	all := make([]map[string]any, 0, len(list))
	for _, e := range list {
		if !filters.matches(e) {
			continue
		}

		// Date deserializes from a json.Number via ParseEpochSeconds --
		// confirmed against aws-sdk-go-v2/service/
		// databasemigrationservice@v1.66.4's deserializers.go
		// (awsAwsjson11_deserializeDocumentEvent, case "Date"). An RFC3339
		// string failed DescribeEvents' decode outright.
		all = append(all, map[string]any{
			"SourceIdentifier": e.SourceIdentifier,
			"SourceType":       e.SourceType,
			"Message":          e.Message,
			"EventCategories":  e.EventCategories,
			"Date":             awstime.Epoch(e.Date),
		})
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeEventsOutput{Events: data, Marker: nextMarker}, nil
}

type modifyEventSubscriptionInput struct {
	SubscriptionName *string `json:"SubscriptionName"`
	Enabled          *bool   `json:"Enabled"`
}

type modifyEventSubscriptionOutput struct {
	EventSubscription eventSubscriptionJSON `json:"EventSubscription"`
}

func (h *Handler) handleModifyEventSubscription(
	ctx context.Context, in *modifyEventSubscriptionInput,
) (*modifyEventSubscriptionOutput, error) {
	es, err := h.Backend.ModifyEventSubscription(ctx, ptrconv.String(in.SubscriptionName), in.Enabled)
	if err != nil {
		return nil, err
	}

	return &modifyEventSubscriptionOutput{EventSubscription: esToJSON(es)}, nil
}

type updateSubscriptionsToEventBridgeInput struct {
	ForceMove *bool `json:"ForceMove"`
}

type updateSubscriptionsToEventBridgeOutput struct {
	Applied bool `json:"Applied"`
}

func (h *Handler) handleUpdateSubscriptionsToEventBridge(
	_ context.Context, _ *updateSubscriptionsToEventBridgeInput,
) (*updateSubscriptionsToEventBridgeOutput, error) {
	return &updateSubscriptionsToEventBridgeOutput{Applied: false}, nil
}

// opsEventSubscriptions returns the dispatch-table entries for the event_subscriptions operation family.
func (h *Handler) opsEventSubscriptions() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateEventSubscription: service.WrapOp(
			h.handleCreateEventSubscription,
		),
		opDeleteEventSubscription: service.WrapOp(
			h.handleDeleteEventSubscription,
		),
		opDescribeEventCategories: service.WrapOp(
			h.handleDescribeEventCategories,
		),
		opDescribeEventSubscriptions: service.WrapOp(
			h.handleDescribeEventSubscriptions,
		),
		opDescribeEvents: service.WrapOp(h.handleDescribeEvents),
		opModifyEventSubscription: service.WrapOp(
			h.handleModifyEventSubscription,
		),
		opUpdateSubscriptionsToEventBridge: service.WrapOp(
			h.handleUpdateSubscriptionsToEventBridge,
		),
	}
}
