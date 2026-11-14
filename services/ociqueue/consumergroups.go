package ociqueue

import (
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	// A queue holds at most 10 consumer groups, the primary one included.
	// https://docs.oracle.com/en-us/iaas/Content/queue/fanout-top.htm
	maxGroupsPerQueue  = 10
	defaultPrimaryName = "Primary Consumer Group"
	stateInactive      = "INACTIVE"
	groupTypeKey       = "CONSUMER_GROUPS"
)

// ConsumerGroup is a named delivery path over a queue with its own message and dead letter state.
type ConsumerGroup struct {
	TimeCreated  time.Time  `json:"timeCreated"`
	TimeUpdated  time.Time  `json:"timeUpdated"`
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	DLQCount     *int       `json:"dlqCount,omitempty"`
	Tags         Tags       `json:"tags"`
	ID           string     `json:"id"`
	QueueID      string     `json:"queueId"`
	Name         string     `json:"name"`
	State        string     `json:"state"`
	Filter       string     `json:"filter"`
	Etag         string     `json:"etag"`
	Messages     []*Message `json:"messages,omitempty"`
	DLQ          []*Message `json:"dlq,omitempty"`
	Seq          int64      `json:"seq"`
	Enabled      bool       `json:"enabled"`
}

// lane is one consumer group's view of a queue: its live messages, dead letters and delivery limit.
type lane struct {
	msgs     *[]*Message
	dlq      *[]*Message
	dlqCount int
}

func (q *Queue) primaryLane() lane {
	n := q.DLQCount
	if q.GroupsOn && q.PrimaryDLQ != nil {
		n = *q.PrimaryDLQ
	}

	return lane{msgs: &q.Messages, dlq: &q.DLQ, dlqCount: n}
}

func (q *Queue) groupLane(g *ConsumerGroup) lane {
	n := q.DLQCount
	if g.DLQCount != nil {
		n = *g.DLQCount
	}

	return lane{msgs: &g.Messages, dlq: &g.DLQ, dlqCount: n}
}

func (q *Queue) lanes() []lane {
	out := make([]lane, 1, 1+len(q.Groups))
	out[0] = q.primaryLane()

	for _, g := range q.Groups {
		out = append(out, q.groupLane(g))
	}

	return out
}

func (g *ConsumerGroup) live() bool { return g.State != stateDeleted }

func (g *ConsumerGroup) receives() bool {
	return g.Enabled && g.State != stateDeleting && g.State != stateDeleted
}

func settleGroups(q *Queue, now time.Time) {
	for _, g := range q.Groups {
		if g.TransitionAt == nil || g.TransitionAt.After(now) {
			continue
		}

		g.TransitionAt = nil

		switch g.State {
		case stateCreating, stateUpdating:
			g.State = groupSteadyState(g.Enabled)
		case stateDeleting:
			g.State = stateDeleted
			g.Messages, g.DLQ = nil, nil
		}
	}
}

func groupSteadyState(enabled bool) string {
	if enabled {
		return stateActive
	}

	return stateInactive
}

// laneFor resolves a consumerGroupId query value; empty and the queue's own OCID mean the primary group.
func (q *Queue) laneFor(id string) (lane, *APIError) {
	if id == "" || id == q.ID {
		return q.primaryLane(), nil
	}

	g, ok := q.Groups[id]
	if !ok || !g.live() {
		return lane{}, errNotFound(id)
	}

	return q.groupLane(g), nil
}

// fanOut stores a published message in every group that receives it.
func (q *Queue) fanOut(m *Message) {
	if !q.GroupsOn || !q.PrimaryOff {
		q.Messages = append(q.Messages, m)
	}

	for _, g := range q.Groups {
		if !g.receives() {
			continue
		}

		match, err := compileFilter(g.Filter)
		if err != nil || !match(m.Props) {
			continue
		}

		cp := *m
		g.Messages = append(g.Messages, &cp)
	}
}

// CreateConsumerGroupIn is the CreateConsumerGroup request body.
type CreateConsumerGroupIn struct {
	RetryToken string `json:"-"`
	Tags
	Filter    *string `json:"filter"`
	IsEnabled *bool   `json:"isEnabled"`
	DLQCount  *int    `json:"deadLetterQueueDeliveryCount"`
	Name      string  `json:"displayName"`
	QueueID   string  `json:"queueId"`
}

// UpdateConsumerGroupIn is the UpdateConsumerGroup request body.
type UpdateConsumerGroupIn struct {
	Tags
	Name      *string `json:"displayName"`
	Filter    *string `json:"filter"`
	IsEnabled *bool   `json:"isEnabled"`
	DLQCount  *int    `json:"deadLetterQueueDeliveryCount"`
}

// ConsumerGroupWire is the ConsumerGroup response.
type ConsumerGroupWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	DLQCount       *int                      `json:"deadLetterQueueDeliveryCount,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	Name           string                    `json:"displayName"`
	LifecycleState string                    `json:"lifecycleState"`
	Filter         string                    `json:"filter"`
	QueueID        string                    `json:"queueId"`
}

// CgSummaryWire is the consumer-group summary wire shape.
type CgSummaryWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	Name           string                    `json:"displayName"`
	LifecycleState string                    `json:"lifecycleState"`
	Filter         string                    `json:"filter"`
	QueueID        string                    `json:"queueId"`
}

type cgItem struct {
	wire ConsumerGroupWire
	seq  int64
}

func (q *Queue) primaryName() string {
	if q.PrimaryName == "" {
		return defaultPrimaryName
	}

	return q.PrimaryName
}

func (q *Queue) primaryItem() cgItem {
	state := groupSteadyState(!q.PrimaryOff)
	if q.State == stateDeleted {
		state = stateDeleted
	}

	return cgItem{seq: q.Seq, wire: ConsumerGroupWire{
		ID: q.ID, QueueID: q.ID, Name: q.primaryName(), LifecycleState: state, DLQCount: q.PrimaryDLQ,
		TimeCreated: q.TimeCreated, TimeUpdated: q.TimeUpdated,
		FreeformTags: q.Tags.FreeformTags, DefinedTags: q.Tags.DefinedTags,
	}}
}

func groupItem(g *ConsumerGroup) cgItem {
	return cgItem{seq: g.Seq, wire: ConsumerGroupWire{
		ID: g.ID, QueueID: g.QueueID, Name: g.Name, LifecycleState: g.State, Filter: g.Filter, DLQCount: g.DLQCount,
		TimeCreated: g.TimeCreated, TimeUpdated: g.TimeUpdated,
		FreeformTags: g.Tags.FreeformTags, DefinedTags: g.Tags.DefinedTags,
	}}
}

func summaryOf(w ConsumerGroupWire) CgSummaryWire {
	return CgSummaryWire{
		TimeCreated: w.TimeCreated, TimeUpdated: w.TimeUpdated, FreeformTags: w.FreeformTags,
		DefinedTags: w.DefinedTags, ID: w.ID, Name: w.Name, LifecycleState: w.LifecycleState,
		Filter: w.Filter, QueueID: w.QueueID,
	}
}

func errConflict(msg string) *APIError { return newErr(http.StatusConflict, "Conflict", msg) }

func (q *Queue) nameTaken(name, exceptID string) bool {
	if q.GroupsOn && q.ID != exceptID && strs.Equal(q.primaryName(), name) {
		return true
	}

	for _, g := range q.Groups {
		if g.live() && g.ID != exceptID && strs.Equal(g.Name, name) {
			return true
		}
	}

	return false
}

func (q *Queue) liveGroupCount() int {
	n := 1

	for _, g := range q.Groups {
		if g.live() {
			n++
		}
	}

	return n
}

func checkFilter(f *string) *APIError {
	if f == nil {
		return nil
	}

	if _, err := compileFilter(*f); err != nil {
		return errParam("filter: " + strings.TrimPrefix(err.Error(), errFilterSyntax.Error()+": "))
	}

	return nil
}

func (b *InMemoryBackend) groupQueue(id string) (*Queue, *APIError) {
	b.settleID(id)

	q, aerr := b.mutableQueue(id)
	if aerr != nil {
		return nil, aerr
	}

	if !q.GroupsOn {
		return nil, errState("Queue " + id + " does not have the CONSUMER_GROUPS capability enabled")
	}

	return q, nil
}

func (b *InMemoryBackend) replayGroupCreate(wrID string) (string, error) {
	wr, ok := b.workReqs[wrID]
	if !ok {
		return "", errInvalidatedToken()
	}

	for _, q := range b.queues {
		if g, found := q.Groups[wr.EntityID]; found && g.State != stateDeleting && g.State != stateDeleted {
			return wrID, nil
		}
	}

	return "", errInvalidatedToken()
}

func validateGroupCreate(in CreateConsumerGroupIn) *APIError {
	if in.QueueID == "" {
		return errMissing("queueId is required")
	}

	return firstErr(
		validateName(in.Name),
		inRange("deadLetterQueueDeliveryCount", in.DLQCount, 0, maxDLQCount),
		checkFilter(in.Filter),
	)
}

// CreateConsumerGroup adds a consumer group in CREATING state and returns its work request.
func (b *InMemoryBackend) CreateConsumerGroup(in CreateConsumerGroupIn) (string, error) {
	if aerr := validateGroupCreate(in); aerr != nil {
		return "", aerr
	}

	b.mu.Lock("CreateConsumerGroup")
	defer b.mu.Unlock()

	now := b.nowUTC()
	if wr, ok := b.retry.Lookup("CreateConsumerGroup", in.RetryToken, now); ok {
		return b.replayGroupCreate(wr)
	}

	q, aerr := b.groupQueue(in.QueueID)
	if aerr != nil {
		return "", aerr
	}

	if aerr = b.checkTags(in.Tags); aerr != nil {
		return "", aerr
	}

	if q.nameTaken(in.Name, "") {
		return "", errConflict("A consumer group named " + in.Name + " already exists in the queue")
	}

	if q.liveGroupCount() >= maxGroupsPerQueue {
		return "", errParam("a queue holds at most 10 consumer groups")
	}

	g := &ConsumerGroup{
		ID: newOCID("queueconsumergroup"), QueueID: q.ID, Name: in.Name, State: stateCreating,
		TimeCreated: now, TimeUpdated: now, TransitionAt: b.transitionAt(), Tags: in.Tags, Etag: newEtag(),
		Enabled: in.IsEnabled == nil || *in.IsEnabled, DLQCount: in.DLQCount, Seq: b.nextSeq(),
	}
	if in.Filter != nil {
		g.Filter = *in.Filter
	}

	if q.Groups == nil {
		q.Groups = map[string]*ConsumerGroup{}
	}

	q.Groups[g.ID] = g

	wr := b.newWRFor("consumergroup", "CREATE_CONSUMER_GROUP", q.CompartmentID, g.ID, "CREATED",
		"/consumerGroups/"+g.ID, *g.TransitionAt)
	b.retry.Remember("CreateConsumerGroup", in.RetryToken, wr, now)

	return wr, nil
}

// findGroup locates a consumer group by OCID; a queue's own OCID names its primary group (nil group).
func (b *InMemoryBackend) findGroup(id string) (*Queue, *ConsumerGroup, *APIError) {
	if q, ok := b.queues[id]; ok && q.GroupsOn && q.State != stateDeleted {
		b.settle(q, b.nowUTC())

		return q, nil, nil
	}

	for _, q := range b.queues {
		if g, ok := q.Groups[id]; ok && g.live() && q.State != stateDeleted {
			b.settle(q, b.nowUTC())

			if !g.live() {
				break
			}

			return q, g, nil
		}
	}

	return nil, nil, errNotFound(id)
}

// GetConsumerGroup returns one consumer group and its etag.
func (b *InMemoryBackend) GetConsumerGroup(id string) (ConsumerGroupWire, string, error) {
	b.mu.Lock("GetConsumerGroup")
	defer b.mu.Unlock()

	q, g, aerr := b.findGroup(id)
	if aerr != nil {
		return ConsumerGroupWire{}, "", aerr
	}

	if g == nil {
		return q.primaryItem().wire, q.Etag, nil
	}

	return groupItem(g).wire, g.Etag, nil
}

// CGListQuery carries the ListConsumerGroups filters.
type CGListQuery struct {
	ID, QueueID, Name, State string
}

func (f CGListQuery) match(w ConsumerGroupWire) bool {
	return (f.ID == "" || w.ID == f.ID) && (f.QueueID == "" || w.QueueID == f.QueueID) &&
		(f.Name == "" || w.Name == f.Name) && (f.State == "" || strings.EqualFold(w.LifecycleState, f.State))
}

// ListConsumerGroups lists consumer groups, primary groups included.
func (b *InMemoryBackend) ListConsumerGroups(f CGListQuery, o ListOpts) (page.Page[CgSummaryWire], error) {
	b.mu.Lock("ListConsumerGroups")
	defer b.mu.Unlock()

	now := b.nowUTC()

	var all []*cgItem

	add := func(it cgItem) {
		if f.match(it.wire) {
			all = append(all, &it)
		}
	}

	for _, q := range b.queues {
		b.settle(q, now)

		if q.GroupsOn && q.State != stateDeleted {
			add(q.primaryItem())
		}

		for _, g := range q.Groups {
			add(groupItem(g))
		}
	}

	pg := paginate(all, o, func(i *cgItem) string { return i.wire.Name },
		func(i *cgItem) (time.Time, int64) { return i.wire.TimeCreated, i.seq })

	out := make([]CgSummaryWire, 0, len(pg.Data))
	for _, it := range pg.Data {
		out = append(out, summaryOf(it.wire))
	}

	return page.Page[CgSummaryWire]{Data: out, Next: pg.Next}, nil
}

func (b *InMemoryBackend) editableGroup(id, ifMatch string) (*Queue, *ConsumerGroup, *APIError) {
	q, g, aerr := b.findGroup(id)
	if aerr != nil {
		return nil, nil, aerr
	}

	if q.State == stateCreating || q.State == stateDeleting {
		return nil, nil, errState("Queue " + q.ID + " is in state " + q.State + "; the operation requires ACTIVE")
	}

	if g == nil {
		return q, nil, checkEtag(q.Etag, ifMatch)
	}

	if g.State == stateCreating || g.State == stateDeleting {
		return nil, nil, errState("Consumer group " + id + " is in state " + g.State)
	}

	return q, g, checkEtag(g.Etag, ifMatch)
}

// UpdateConsumerGroup changes a consumer group; non-primary groups pass through UPDATING.
func (b *InMemoryBackend) UpdateConsumerGroup(id, ifMatch string, in UpdateConsumerGroupIn) (string, error) {
	if aerr := firstErr(
		inRange("deadLetterQueueDeliveryCount", in.DLQCount, 0, maxDLQCount),
		checkFilter(in.Filter),
	); aerr != nil {
		return "", aerr
	}

	if in.Name != nil {
		if aerr := validateName(*in.Name); aerr != nil {
			return "", aerr
		}
	}

	b.mu.Lock("UpdateConsumerGroup")
	defer b.mu.Unlock()

	q, g, aerr := b.editableGroup(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if aerr = b.checkTags(in.Tags); aerr != nil {
		return "", aerr
	}

	if in.Name != nil && q.nameTaken(*in.Name, id) {
		return "", errConflict("A consumer group named " + *in.Name + " already exists in the queue")
	}

	now := b.nowUTC()
	finish := now

	if g == nil {
		if in.Filter != nil && *in.Filter != "" {
			return "", errParam("the primary consumer group cannot have a filter")
		}

		applyPrimaryUpdate(q, in)
		q.TimeUpdated, q.Etag = now, newEtag()
		q.notify()
	} else {
		applyGroupUpdate(g, in)
		g.State, g.TransitionAt, g.TimeUpdated, g.Etag = stateUpdating, b.transitionAt(), now, newEtag()
		finish = *g.TransitionAt
	}

	return b.newWRFor("consumergroup", "UPDATE_CONSUMER_GROUP", q.CompartmentID, id, "UPDATED",
		"/consumerGroups/"+id, finish), nil
}

func applyPrimaryUpdate(q *Queue, in UpdateConsumerGroupIn) {
	if in.Name != nil {
		q.PrimaryName = *in.Name
	}

	if in.IsEnabled != nil {
		q.PrimaryOff = !*in.IsEnabled
	}

	if in.DLQCount != nil {
		q.PrimaryDLQ = in.DLQCount
	}
}

func applyGroupUpdate(g *ConsumerGroup, in UpdateConsumerGroupIn) {
	if in.Name != nil {
		g.Name = *in.Name
	}

	if in.Filter != nil {
		g.Filter = *in.Filter
	}

	if in.IsEnabled != nil {
		g.Enabled = *in.IsEnabled
	}

	if in.DLQCount != nil {
		g.DLQCount = in.DLQCount
	}

	g.Tags.merge(in.Tags)
}

// DeleteConsumerGroup moves a non-primary consumer group to DELETING; its messages go with it.
func (b *InMemoryBackend) DeleteConsumerGroup(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteConsumerGroup")
	defer b.mu.Unlock()

	q, g, aerr := b.editableGroup(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if g == nil {
		return "", errParam("the primary consumer group cannot be deleted; disable it instead")
	}

	g.State, g.TransitionAt, g.TimeUpdated, g.Etag = stateDeleting, b.transitionAt(), b.nowUTC(), newEtag()
	q.notify()

	return b.newWRFor("consumergroup", "DELETE_CONSUMER_GROUP", q.CompartmentID, id, "DELETED",
		"/consumerGroups/"+id, *g.TransitionAt), nil
}

func (h *Handler) createConsumerGroup(q *req) error {
	var in CreateConsumerGroupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)

	wr, err := h.Backend.CreateConsumerGroup(in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getConsumerGroup(q *req) error {
	v, etag, err := h.Backend.GetConsumerGroup(q.param("consumerGroupId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listConsumerGroups(q *req) error {
	pg, err := h.Backend.ListConsumerGroups(CGListQuery{
		ID: q.query("id"), QueueID: q.query("queueId"), Name: q.query("displayName"), State: q.query("lifecycleState"),
	}, q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) updateConsumerGroup(q *req) error {
	var in UpdateConsumerGroupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateConsumerGroup(q.param("consumerGroupId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteConsumerGroup(q *req) error {
	wr, err := h.Backend.DeleteConsumerGroup(q.param("consumerGroupId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}
