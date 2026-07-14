package dms

import (
	"context"
	"fmt"
	"sort"

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

type eventSubscriptionJSON struct {
	SubscriptionName string   `json:"SubscriptionName"`
	SnsTopicArn      string   `json:"SnsTopicArn"`
	SourceType       string   `json:"SourceType,omitempty"`
	Status           string   `json:"Status"`
	SourceIDsList    []string `json:"SourceIdsList"`
	EventCategories  []string `json:"EventCategories"`
	Enabled          bool     `json:"Enabled"`
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
		SubscriptionName: es.SubscriptionName,
		SnsTopicArn:      es.SnsTopicArn,
		SourceType:       es.SourceType,
		SourceIDsList:    ensureNonNil(es.SourceIDsList),
		EventCategories:  ensureNonNil(es.EventCategories),
		Status:           es.Status,
		Enabled:          es.Enabled,
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
	SourceType *string `json:"SourceType"`
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
	SubscriptionName *string `json:"SubscriptionName"`
	Marker           *string `json:"Marker"`
	MaxRecords       *int32  `json:"MaxRecords"`
}

type describeEventSubscriptionsOutput struct {
	Marker                 *string                 `json:"Marker,omitempty"`
	EventSubscriptionsList []eventSubscriptionJSON `json:"EventSubscriptionsList"`
}

func (h *Handler) handleDescribeEventSubscriptions(
	ctx context.Context, in *describeEventSubscriptionsInput,
) (*describeEventSubscriptionsOutput, error) {
	list, err := h.Backend.DescribeEventSubscriptions(ctx, ptrconv.String(in.SubscriptionName))
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
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeEventsOutput struct {
	Marker *string          `json:"Marker,omitempty"`
	Events []map[string]any `json:"Events"`
}

func (h *Handler) handleDescribeEvents(
	ctx context.Context, in *describeEventsInput,
) (*describeEventsOutput, error) {
	list, err := h.Backend.DescribeEvents(ctx)
	if err != nil {
		return nil, err
	}

	all := make([]map[string]any, 0, len(list))
	for _, e := range list {
		all = append(all, map[string]any{
			"SourceIdentifier": e.SourceIdentifier,
			"SourceType":       e.SourceType,
			"Message":          e.Message,
			"EventCategories":  e.EventCategories,
			"Date":             e.Date,
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
