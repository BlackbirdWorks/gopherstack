package ocinotifications

import (
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateTopicIn is the CreateTopic request body.
type CreateTopicIn struct {
	Tags
	Description *string `json:"description"`
	Name        string  `json:"name"`
	Compartment string  `json:"compartmentId"`
}

// UpdateTopicIn is the UpdateTopic request body.
type UpdateTopicIn struct {
	Tags
	Description *string `json:"description"`
}

type TopicWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	Name           string                    `json:"name"`
	TopicID        string                    `json:"topicId"`
	CompartmentID  string                    `json:"compartmentId"`
	LifecycleState string                    `json:"lifecycleState"`
	APIEndpoint    string                    `json:"apiEndpoint"`
	ShortTopicID   string                    `json:"shortTopicId"`
	Description    string                    `json:"description,omitempty"`
	Etag           string                    `json:"etag"`
}

func topicView(t *Topic, endpoint string) TopicWire {
	return TopicWire{
		Name:           t.Name,
		TopicID:        t.ID,
		CompartmentID:  t.CompartmentID,
		LifecycleState: t.State,
		TimeCreated:    t.TimeCreated,
		APIEndpoint:    endpoint,
		ShortTopicID:   t.ShortID,
		Description:    t.Description,
		Etag:           t.Etag,
		FreeformTags:   t.Tags.FreeformTags,
		DefinedTags:    t.Tags.DefinedTags,
	}
}

func errTopicState(t *Topic) *APIError {
	return errState("Topic " + t.ID + " is in state " + t.State + "; the operation requires ACTIVE")
}

// topic returns a topic that has not started deleting away; the caller holds the lock.
func (b *InMemoryBackend) topic(id string) (*Topic, *APIError) {
	b.sweepLocked()

	t, ok := b.topics[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return t, nil
}

func (b *InMemoryBackend) activeTopic(id string) (*Topic, *APIError) {
	t, aerr := b.topic(id)
	if aerr != nil {
		return nil, aerr
	}

	if t.State != stateActive {
		return nil, errTopicState(t)
	}

	return t, nil
}

// CreateTopic creates a topic in CREATING state.
func (b *InMemoryBackend) CreateTopic(in CreateTopicIn, endpoint string) (TopicWire, error) {
	if in.Compartment == "" {
		return TopicWire{}, errMissing("compartmentId is required")
	}

	if strings.TrimSpace(in.Name) == "" {
		return TopicWire{}, errMissing("name is required")
	}

	if len(in.Name) > maxName {
		return TopicWire{}, errParam("name must be at most 256 characters")
	}

	b.mu.Lock("CreateTopic")
	defer b.mu.Unlock()

	b.sweepLocked()

	for _, t := range b.topics {
		if t.Name == in.Name {
			return TopicWire{}, errExists("A topic named " + in.Name + " already exists")
		}
	}

	t := &Topic{
		ID: newOCID("onstopic"), ShortID: strings.ToUpper(awsid.NewN("", shortIDLen)), CompartmentID: in.Compartment,
		Name: in.Name, State: stateCreating, TimeCreated: b.nowUTC(), Tags: in.Tags, Etag: newEtag(),
		Seq: b.nextSeq(),
	}
	if in.Description != nil {
		t.Description = *in.Description
	}

	at := b.nowUTC().Add(b.transition)
	t.TransitionAt = &at
	b.topics[t.ID] = t

	return topicView(t, endpoint), nil
}

// GetTopic returns one topic.
func (b *InMemoryBackend) GetTopic(id, endpoint string) (TopicWire, error) {
	b.mu.Lock("GetTopic")
	defer b.mu.Unlock()

	t, aerr := b.topic(id)
	if aerr != nil {
		return TopicWire{}, aerr
	}

	return topicView(t, endpoint), nil
}

// ListTopics lists the topics of a compartment.
func (b *InMemoryBackend) ListTopics(
	compartment, id, name, state, endpoint string,
	o ListOpts,
) (page.Page[TopicWire], error) {
	if compartment == "" {
		return page.Page[TopicWire]{}, errMissing("compartmentId is required")
	}

	b.mu.Lock("ListTopics")
	defer b.mu.Unlock()

	b.sweepLocked()

	var all []*Topic

	for _, t := range b.topics {
		if t.CompartmentID == compartment && (id == "" || t.ID == id) && (name == "" || t.Name == name) &&
			(state == "" || strings.EqualFold(t.State, state)) {
			all = append(all, t)
		}
	}

	pg := paginate(all, o, func(t *Topic) string { return t.State },
		func(t *Topic) (time.Time, int64) { return t.TimeCreated, t.Seq })

	out := make([]TopicWire, 0, len(pg.Data))
	for _, t := range pg.Data {
		out = append(out, topicView(t, endpoint))
	}

	return page.Page[TopicWire]{Data: out, Next: pg.Next}, nil
}

// UpdateTopic changes a topic's description and tags.
func (b *InMemoryBackend) UpdateTopic(id, ifMatch, endpoint string, in UpdateTopicIn) (TopicWire, error) {
	b.mu.Lock("UpdateTopic")
	defer b.mu.Unlock()

	t, aerr := b.activeTopic(id)
	if aerr != nil {
		return TopicWire{}, aerr
	}

	if aerr = checkEtag(t.Etag, ifMatch); aerr != nil {
		return TopicWire{}, aerr
	}

	if in.Description != nil {
		t.Description = *in.Description
	}

	t.Tags.merge(in.Tags)
	t.Etag = newEtag()

	return topicView(t, endpoint), nil
}

// DeleteTopic moves a topic to DELETING; its subscriptions go with it once the transition completes.
func (b *InMemoryBackend) DeleteTopic(id, ifMatch string) error {
	b.mu.Lock("DeleteTopic")
	defer b.mu.Unlock()

	t, aerr := b.activeTopic(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(t.Etag, ifMatch); aerr != nil {
		return aerr
	}

	at := b.nowUTC().Add(b.transition)
	t.State, t.TransitionAt, t.Etag = stateDeleting, &at, newEtag()

	return nil
}

// ChangeTopicCompartment moves a topic to another compartment.
func (b *InMemoryBackend) ChangeTopicCompartment(id, ifMatch, compartment string) error {
	if compartment == "" {
		return errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeTopicCompartment")
	defer b.mu.Unlock()

	t, aerr := b.activeTopic(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(t.Etag, ifMatch); aerr != nil {
		return aerr
	}

	t.CompartmentID, t.Etag = compartment, newEtag()

	return nil
}

type changeCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

func (h *Handler) createTopic(q *req) error {
	var in CreateTopicIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, err := h.Backend.CreateTopic(in, q.endpoint())
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, v, v.Etag)
}

func (h *Handler) getTopic(q *req) error {
	v, err := h.Backend.GetTopic(q.param("topicId"), q.endpoint())
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, v, v.Etag)
}

func (h *Handler) listTopics(q *req) error {
	pg, err := h.Backend.ListTopics(q.query("compartmentId"), q.query("id"), q.query("name"),
		q.query("lifecycleState"), q.endpoint(), q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) updateTopic(q *req) error {
	var in UpdateTopicIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, err := h.Backend.UpdateTopic(q.param("topicId"), q.ifMatch(), q.endpoint(), in)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, v, v.Etag)
}

func (h *Handler) deleteTopic(q *req) error {
	if err := h.Backend.DeleteTopic(q.param("topicId"), q.ifMatch()); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusAccepted)

	return nil
}

func (h *Handler) changeTopicCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	if err := h.Backend.ChangeTopicCompartment(q.param("topicId"), q.ifMatch(), in.CompartmentID); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusAccepted)

	return nil
}
