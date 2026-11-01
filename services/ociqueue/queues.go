package ociqueue

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	minRetention     = 10
	maxRetention     = 604800
	defaultRetention = 86400
	maxVisibility    = 43200
	defaultVisible   = 30
	maxPollTimeout   = 30
	defaultTimeout   = 30
	maxDLQCount      = 20
	defaultDLQCount  = 5
	maxChannelLimit  = 100
)

// CreateQueueIn is the CreateQueue request body.
type CreateQueueIn struct {
	Tags
	Retention    *int              `json:"retentionInSeconds"`
	Visibility   *int              `json:"visibilityInSeconds"`
	Timeout      *int              `json:"timeoutInSeconds"`
	ChannelLimit *int              `json:"channelConsumptionLimit"`
	DLQCount     *int              `json:"deadLetterQueueDeliveryCount"`
	KMSKeyID     *string           `json:"customEncryptionKeyId"`
	Name         string            `json:"displayName"`
	Compartment  string            `json:"compartmentId"`
	Capabilities []json.RawMessage `json:"capabilities"`
}

// UpdateQueueIn is the UpdateQueue request body.
type UpdateQueueIn struct {
	Tags
	Name         *string           `json:"displayName"`
	Visibility   *int              `json:"visibilityInSeconds"`
	Timeout      *int              `json:"timeoutInSeconds"`
	ChannelLimit *int              `json:"channelConsumptionLimit"`
	DLQCount     *int              `json:"deadLetterQueueDeliveryCount"`
	KMSKeyID     *string           `json:"customEncryptionKeyId"`
	Capabilities []json.RawMessage `json:"capabilities"`
}

// PurgeIn is the PurgeQueue request body.
type PurgeIn struct {
	ConsumerGroupID *string  `json:"consumerGroupId"`
	PurgeType       string   `json:"purgeType"`
	ChannelIDs      []string `json:"channelIds"`
}

type QueueWire struct {
	TimeCreated      time.Time                 `json:"timeCreated"`
	TimeUpdated      time.Time                 `json:"timeUpdated"`
	FreeformTags     map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags      map[string]map[string]any `json:"definedTags,omitempty"`
	KMSKeyID         *string                   `json:"customEncryptionKeyId,omitempty"`
	ID               string                    `json:"id"`
	CompartmentID    string                    `json:"compartmentId"`
	Name             string                    `json:"displayName"`
	LifecycleState   string                    `json:"lifecycleState"`
	MessagesEndpoint string                    `json:"messagesEndpoint"`
	Retention        int                       `json:"retentionInSeconds"`
	Visibility       int                       `json:"visibilityInSeconds"`
	Timeout          int                       `json:"timeoutInSeconds"`
	DLQCount         int                       `json:"deadLetterQueueDeliveryCount"`
	ChannelLimit     int                       `json:"channelConsumptionLimit"`
}

type queueSummaryWire struct {
	TimeCreated      time.Time                 `json:"timeCreated"`
	TimeUpdated      time.Time                 `json:"timeUpdated"`
	FreeformTags     map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags      map[string]map[string]any `json:"definedTags,omitempty"`
	ID               string                    `json:"id"`
	CompartmentID    string                    `json:"compartmentId"`
	Name             string                    `json:"displayName"`
	LifecycleState   string                    `json:"lifecycleState"`
	MessagesEndpoint string                    `json:"messagesEndpoint"`
}

func queueView(q *Queue, endpoint string) QueueWire {
	w := QueueWire{
		ID: q.ID, CompartmentID: q.CompartmentID, Name: q.Name, LifecycleState: q.State, MessagesEndpoint: endpoint,
		Retention: q.Retention, Visibility: q.Visibility, Timeout: q.Timeout, DLQCount: q.DLQCount,
		ChannelLimit: q.ChannelLimit, TimeCreated: q.TimeCreated, TimeUpdated: q.TimeUpdated,
		FreeformTags: q.Tags.FreeformTags, DefinedTags: q.Tags.DefinedTags,
	}
	if q.KMSKeyID != "" {
		w.KMSKeyID = &q.KMSKeyID
	}

	return w
}

func inRange(name string, v *int, lo, hi int) *APIError {
	if v != nil && (*v < lo || *v > hi) {
		return errParam(name + " must be between " + strconv.Itoa(lo) + " and " + strconv.Itoa(hi))
	}

	return nil
}

func validateName(name string) *APIError {
	if strings.TrimSpace(name) == "" {
		return errMissing("displayName is required")
	}

	if len(name) > maxName {
		return errParam("displayName must be at most 255 characters")
	}

	return nil
}

func firstErr(errs ...*APIError) *APIError {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

func checkCapabilities(c []json.RawMessage) *APIError {
	if len(c) > 0 {
		return errParam("capabilities are not supported by this emulator")
	}

	return nil
}

func def(v *int, d int) int {
	if v == nil {
		return d
	}

	return *v
}

func (b *InMemoryBackend) liveQueue(id string) (*Queue, *APIError) {
	q, ok := b.queues[id]
	if !ok || q.State == stateDeleted {
		return nil, errNotFound(id)
	}

	return q, nil
}

func (b *InMemoryBackend) mutableQueue(id string) (*Queue, *APIError) {
	q, aerr := b.liveQueue(id)
	if aerr != nil {
		return nil, aerr
	}

	if q.State == stateCreating || q.State == stateDeleting {
		return nil, errState("Queue " + id + " is in state " + q.State + "; the operation requires ACTIVE")
	}

	return q, nil
}

// CreateQueue creates a queue in CREATING state and returns its work request.
func (b *InMemoryBackend) CreateQueue(in CreateQueueIn) (string, error) {
	if in.Compartment == "" {
		return "", errMissing("compartmentId is required")
	}

	if aerr := firstErr(
		validateName(in.Name),
		inRange("retentionInSeconds", in.Retention, minRetention, maxRetention),
		inRange("visibilityInSeconds", in.Visibility, 0, maxVisibility),
		inRange("timeoutInSeconds", in.Timeout, 0, maxPollTimeout),
		inRange("deadLetterQueueDeliveryCount", in.DLQCount, 0, maxDLQCount),
		inRange("channelConsumptionLimit", in.ChannelLimit, 1, maxChannelLimit),
		checkCapabilities(in.Capabilities),
	); aerr != nil {
		return "", aerr
	}

	b.mu.Lock("CreateQueue")
	defer b.mu.Unlock()

	now := b.nowUTC()
	q := &Queue{
		ID: newOCID("queue"), CompartmentID: in.Compartment, Name: in.Name, State: stateCreating,
		TimeCreated: now, TimeUpdated: now, TransitionAt: b.transitionAt(), Tags: in.Tags, Etag: newEtag(),
		Retention: def(in.Retention, defaultRetention), Visibility: def(in.Visibility, defaultVisible),
		Timeout: def(in.Timeout, defaultTimeout), DLQCount: def(in.DLQCount, defaultDLQCount),
		ChannelLimit: def(in.ChannelLimit, maxChannelLimit), NextMsgID: 1, Seq: b.nextSeq(),
	}

	if in.KMSKeyID != nil && *in.KMSKeyID != "" {
		if aerr := b.checkKey(*in.KMSKeyID); aerr != nil {
			return "", aerr
		}

		q.KMSKeyID = *in.KMSKeyID
	}

	b.queues[q.ID] = q

	return b.newWR("CREATE_QUEUE", q.CompartmentID, q.ID, "CREATED", "/queues/"+q.ID, *q.TransitionAt), nil
}

// GetQueue returns one queue and its etag.
func (b *InMemoryBackend) GetQueue(id, endpoint string) (QueueWire, string, error) {
	b.mu.Lock("GetQueue")
	defer b.mu.Unlock()

	q, ok := b.queues[id]
	if !ok {
		return QueueWire{}, "", errNotFound(id)
	}

	b.settle(q, b.nowUTC())

	return queueView(q, endpoint), q.Etag, nil
}

// ListQueues lists the queues of a compartment.
func (b *InMemoryBackend) ListQueues(
	compartment, id, name, state, endpoint string,
	o ListOpts,
) (page.Page[queueSummaryWire], error) {
	if compartment == "" && id == "" {
		return page.Page[queueSummaryWire]{}, errMissing("compartmentId is required")
	}

	b.mu.Lock("ListQueues")
	defer b.mu.Unlock()

	now := b.nowUTC()

	var all []*Queue

	for _, q := range b.queues {
		b.settle(q, now)

		if (compartment == "" || q.CompartmentID == compartment) && (id == "" || q.ID == id) &&
			(name == "" || q.Name == name) && (state == "" || strings.EqualFold(q.State, state)) {
			all = append(all, q)
		}
	}

	pg := paginate(all, o, func(q *Queue) string { return q.Name },
		func(q *Queue) (time.Time, int64) { return q.TimeCreated, q.Seq })

	out := make([]queueSummaryWire, 0, len(pg.Data))
	for _, q := range pg.Data {
		out = append(out, queueSummaryWire{
			ID: q.ID, CompartmentID: q.CompartmentID, Name: q.Name, LifecycleState: q.State,
			MessagesEndpoint: endpoint, TimeCreated: q.TimeCreated, TimeUpdated: q.TimeUpdated,
			FreeformTags: q.Tags.FreeformTags, DefinedTags: q.Tags.DefinedTags,
		})
	}

	return page.Page[queueSummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdateQueue changes the mutable settings of a queue; it passes through UPDATING.
func (b *InMemoryBackend) UpdateQueue(id, ifMatch string, in UpdateQueueIn) (string, error) {
	if aerr := firstErr(
		inRange("visibilityInSeconds", in.Visibility, 0, maxVisibility),
		inRange("timeoutInSeconds", in.Timeout, 0, maxPollTimeout),
		inRange("deadLetterQueueDeliveryCount", in.DLQCount, 0, maxDLQCount),
		inRange("channelConsumptionLimit", in.ChannelLimit, 1, maxChannelLimit),
		checkCapabilities(in.Capabilities),
	); aerr != nil {
		return "", aerr
	}

	if in.Name != nil {
		if aerr := validateName(*in.Name); aerr != nil {
			return "", aerr
		}
	}

	b.mu.Lock("UpdateQueue")
	defer b.mu.Unlock()

	b.settleID(id)

	q, aerr := b.mutableQueue(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(q.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if in.KMSKeyID != nil && *in.KMSKeyID != "" {
		if aerr = b.checkKey(*in.KMSKeyID); aerr != nil {
			return "", aerr
		}

		q.KMSKeyID = *in.KMSKeyID
	}

	applyUpdate(q, in)
	q.State, q.TransitionAt, q.TimeUpdated, q.Etag = stateUpdating, b.transitionAt(), b.nowUTC(), newEtag()
	q.notify()

	return b.newWR("UPDATE_QUEUE", q.CompartmentID, q.ID, "UPDATED", "/queues/"+q.ID, *q.TransitionAt), nil
}

func applyUpdate(q *Queue, in UpdateQueueIn) {
	if in.Name != nil {
		q.Name = *in.Name
	}

	q.Visibility = def(in.Visibility, q.Visibility)
	q.Timeout = def(in.Timeout, q.Timeout)
	q.DLQCount = def(in.DLQCount, q.DLQCount)
	q.ChannelLimit = def(in.ChannelLimit, q.ChannelLimit)
	q.Tags.merge(in.Tags)
}

// DeleteQueue moves a queue to DELETING.
func (b *InMemoryBackend) DeleteQueue(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteQueue")
	defer b.mu.Unlock()

	b.settleID(id)

	q, aerr := b.mutableQueue(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(q.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	q.State, q.TransitionAt, q.TimeUpdated, q.Etag = stateDeleting, b.transitionAt(), b.nowUTC(), newEtag()
	q.notify()

	return b.newWR("DELETE_QUEUE", q.CompartmentID, q.ID, "DELETED", "/queues/"+q.ID, *q.TransitionAt), nil
}

// ChangeQueueCompartment moves a queue to another compartment.
func (b *InMemoryBackend) ChangeQueueCompartment(id, ifMatch, compartment string) (string, error) {
	if compartment == "" {
		return "", errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeQueueCompartment")
	defer b.mu.Unlock()

	b.settleID(id)

	q, aerr := b.mutableQueue(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(q.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	q.CompartmentID, q.Etag, q.TimeUpdated = compartment, newEtag(), b.nowUTC()

	return b.newWR("MOVE_QUEUE", compartment, q.ID, "UPDATED", "/queues/"+q.ID, b.nowUTC()), nil
}

// PurgeQueue deletes the messages of a queue, its dead letter queue, or both, optionally per channel.
func (b *InMemoryBackend) PurgeQueue(id, ifMatch string, in PurgeIn) (string, error) {
	switch in.PurgeType {
	case "NORMAL", "DLQ", "BOTH":
	case "":
		return "", errMissing("purgeType is required")
	default:
		return "", errParam("purgeType must be NORMAL, DLQ or BOTH")
	}

	if in.ConsumerGroupID != nil && *in.ConsumerGroupID != "" {
		return "", errParam("consumer groups are not supported by this emulator")
	}

	b.mu.Lock("PurgeQueue")
	defer b.mu.Unlock()

	b.settleID(id)

	q, aerr := b.mutableQueue(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(q.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	drop := func(m *Message) bool {
		return len(in.ChannelIDs) == 0 || slices.Contains(in.ChannelIDs, m.Channel)
	}

	if in.PurgeType != "DLQ" {
		q.Messages = slices.DeleteFunc(q.Messages, drop)
	}

	if in.PurgeType != "NORMAL" {
		q.DLQ = slices.DeleteFunc(q.DLQ, drop)
	}

	q.TimeUpdated = b.nowUTC()
	q.notify()

	return b.newWR("PURGE_QUEUE", q.CompartmentID, q.ID, "UPDATED", "/queues/"+q.ID,
		b.nowUTC().Add(b.transition)), nil
}

type changeCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

func (h *Handler) createQueue(q *req) error {
	var in CreateQueueIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.CreateQueue(in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getQueue(q *req) error {
	v, etag, err := h.Backend.GetQueue(q.param("queueId"), q.endpoint())

	return reply(q, v, etag, err)
}

func (h *Handler) listQueues(q *req) error {
	pg, err := h.Backend.ListQueues(q.query("compartmentId"), q.query("id"), q.query("displayName"),
		q.query("lifecycleState"), q.endpoint(), q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, map[string]any{itemsKey: pg.Data}, "")
}

func (h *Handler) updateQueue(q *req) error {
	var in UpdateQueueIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateQueue(q.param("queueId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteQueue(q *req) error {
	wr, err := h.Backend.DeleteQueue(q.param("queueId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeQueueCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.ChangeQueueCompartment(q.param("queueId"), q.ifMatch(), in.CompartmentID)

	return acceptedWR(q, wr, err)
}

func (h *Handler) purgeQueue(q *req) error {
	var in PurgeIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.PurgeQueue(q.param("queueId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}
