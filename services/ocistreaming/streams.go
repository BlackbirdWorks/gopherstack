package ocistreaming

import (
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxPartitions    = 1000
	minRetention     = 24
	maxRetention     = 168
	defaultRetention = 24
)

// CreateStreamIn is the CreateStream request body.
type CreateStreamIn struct {
	Partitions       *int   `json:"partitions"`
	RetentionInHours *int   `json:"retentionInHours"`
	Name             string `json:"name"`
	Compartment      string `json:"compartmentId"`
	PoolID           string `json:"streamPoolId"`
	Tags
}

// UpdateStreamIn is the UpdateStream request body.
type UpdateStreamIn struct {
	PoolID *string `json:"streamPoolId"`
	Tags
}

type streamWire struct {
	TimeCreated      time.Time                 `json:"timeCreated"`
	FreeformTags     map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags      map[string]map[string]any `json:"definedTags,omitempty"`
	ID               string                    `json:"id"`
	Name             string                    `json:"name"`
	CompartmentID    string                    `json:"compartmentId"`
	PoolID           string                    `json:"streamPoolId"`
	LifecycleState   string                    `json:"lifecycleState"`
	MessagesEndpoint string                    `json:"messagesEndpoint"`
	Partitions       int                       `json:"partitions"`
	RetentionInHours int                       `json:"retentionInHours"`
}

type streamSummaryWire struct {
	TimeCreated      time.Time                 `json:"timeCreated"`
	FreeformTags     map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags      map[string]map[string]any `json:"definedTags,omitempty"`
	ID               string                    `json:"id"`
	Name             string                    `json:"name"`
	CompartmentID    string                    `json:"compartmentId"`
	PoolID           string                    `json:"streamPoolId"`
	LifecycleState   string                    `json:"lifecycleState"`
	MessagesEndpoint string                    `json:"messagesEndpoint"`
	Partitions       int                       `json:"partitions"`
}

func streamView(s *Stream, endpoint string) streamWire {
	return streamWire{
		ID: s.ID, Name: s.Name, CompartmentID: s.CompartmentID, PoolID: s.PoolID, LifecycleState: s.State,
		MessagesEndpoint: endpoint, Partitions: len(s.Parts), RetentionInHours: s.Retention,
		TimeCreated: s.TimeCreated, FreeformTags: s.Tags.FreeformTags, DefinedTags: s.Tags.DefinedTags,
	}
}

func (b *InMemoryBackend) liveStream(id string) (*Stream, *APIError) {
	s, ok := b.streams[id]
	if !ok || s.State == stateDeleted {
		return nil, errNotFound(id)
	}

	return s, nil
}

func (b *InMemoryBackend) activeStream(id string) (*Stream, *APIError) {
	s, aerr := b.liveStream(id)
	if aerr != nil {
		return nil, aerr
	}

	if s.State != stateActive {
		return nil, errState("Stream " + id + " is in state " + s.State + "; the operation requires ACTIVE")
	}

	return s, nil
}

func (b *InMemoryBackend) nameTaken(poolID, name, except string) bool {
	for _, s := range b.streams {
		if s.PoolID == poolID && s.Name == name && s.ID != except && s.State != stateDeleted &&
			s.State != stateDeleting {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) streamPool(in CreateStreamIn) (*Pool, *APIError) {
	if in.PoolID != "" {
		p, aerr := b.livePool(in.PoolID)
		if aerr != nil {
			return nil, aerr
		}

		if in.Compartment != "" && in.Compartment != p.CompartmentID {
			return nil, errParam("compartmentId does not match the compartment of the stream pool")
		}

		return p, nil
	}

	if in.Compartment == "" {
		return nil, errMissing("compartmentId or streamPoolId is required")
	}

	return b.defaultPool(in.Compartment), nil
}

func validateStreamShape(in CreateStreamIn) (int, int, *APIError) {
	if aerr := validateName(in.Name); aerr != nil {
		return 0, 0, aerr
	}

	if in.Partitions == nil {
		return 0, 0, errMissing("partitions is required")
	}

	if *in.Partitions < 1 || *in.Partitions > maxPartitions {
		return 0, 0, errParam("partitions must be between 1 and 1000")
	}

	retention := defaultRetention
	if in.RetentionInHours != nil {
		retention = *in.RetentionInHours
	}

	if retention < minRetention || retention > maxRetention {
		return 0, 0, errParam("retentionInHours must be between 24 and 168")
	}

	return *in.Partitions, retention, nil
}

// CreateStream creates a stream in the given pool, or in the compartment's default pool.
func (b *InMemoryBackend) CreateStream(in CreateStreamIn, endpoint string) (streamWire, string, string, error) {
	parts, retention, aerr := validateStreamShape(in)
	if aerr != nil {
		return streamWire{}, "", "", aerr
	}

	b.mu.Lock("CreateStream")
	defer b.mu.Unlock()

	pool, aerr := b.streamPool(in)
	if aerr != nil {
		return streamWire{}, "", "", aerr
	}

	if b.nameTaken(pool.ID, in.Name, "") {
		return streamWire{}, "", "", errExists("A stream named " + in.Name + " already exists in the stream pool")
	}

	s := &Stream{
		ID: newOCID("stream"), PoolID: pool.ID, CompartmentID: pool.CompartmentID, Name: in.Name,
		State: stateCreating, TimeCreated: b.nowUTC(), TransitionAt: b.transitionAt(), Retention: retention,
		Tags: in.Tags, Etag: newEtag(), Seq: b.nextSeq(), Parts: make([]*Partition, parts),
	}
	for i := range s.Parts {
		s.Parts[i] = &Partition{}
	}

	b.streams[s.ID] = s
	wr := b.newWR("CREATE_STREAM", s.CompartmentID, "stream", s.ID, "CREATED", "/streams/"+s.ID, *s.TransitionAt)

	return streamView(s, endpoint), s.Etag, wr, nil
}

// GetStream returns one stream.
func (b *InMemoryBackend) GetStream(id, endpoint string) (streamWire, string, error) {
	b.mu.RLock("GetStream")
	defer b.mu.RUnlock()

	s, ok := b.streams[id]
	if !ok {
		return streamWire{}, "", errNotFound(id)
	}

	return streamView(s, endpoint), s.Etag, nil
}

// ListStreams lists streams by compartment, pool, id or name.
func (b *InMemoryBackend) ListStreams(
	compartment, poolID, id, name, state, endpoint string, o ListOpts,
) (page.Page[streamSummaryWire], error) {
	if compartment == "" && poolID == "" && id == "" {
		return page.Page[streamSummaryWire]{}, errMissing("compartmentId or streamPoolId is required")
	}

	b.mu.RLock("ListStreams")
	defer b.mu.RUnlock()

	var all []*Stream

	for _, s := range b.streams {
		if (compartment == "" || s.CompartmentID == compartment) && (poolID == "" || s.PoolID == poolID) &&
			(id == "" || s.ID == id) && (name == "" || s.Name == name) &&
			(state == "" || strings.EqualFold(s.State, state)) {
			all = append(all, s)
		}
	}

	pg := paginate(all, o, func(s *Stream) string { return s.Name },
		func(s *Stream) (time.Time, int64) { return s.TimeCreated, s.Seq })

	out := make([]streamSummaryWire, 0, len(pg.Data))
	for _, s := range pg.Data {
		out = append(out, streamSummaryWire{
			ID: s.ID, Name: s.Name, CompartmentID: s.CompartmentID, PoolID: s.PoolID, LifecycleState: s.State,
			MessagesEndpoint: endpoint, Partitions: len(s.Parts), TimeCreated: s.TimeCreated,
			FreeformTags: s.Tags.FreeformTags, DefinedTags: s.Tags.DefinedTags,
		})
	}

	return page.Page[streamSummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdateStream moves a stream to another pool and changes its tags.
func (b *InMemoryBackend) UpdateStream(
	id, ifMatch, endpoint string, in UpdateStreamIn,
) (streamWire, string, string, error) {
	b.mu.Lock("UpdateStream")
	defer b.mu.Unlock()

	s, aerr := b.activeStream(id)
	if aerr != nil {
		return streamWire{}, "", "", aerr
	}

	if aerr = checkEtag(s.Etag, ifMatch); aerr != nil {
		return streamWire{}, "", "", aerr
	}

	if in.PoolID != nil && *in.PoolID != s.PoolID {
		pool, perr := b.livePool(*in.PoolID)
		if perr != nil {
			return streamWire{}, "", "", perr
		}

		if b.nameTaken(pool.ID, s.Name, s.ID) {
			return streamWire{}, "", "", errExists("A stream named " + s.Name + " already exists in the target pool")
		}

		s.PoolID, s.CompartmentID = pool.ID, pool.CompartmentID
	}

	s.Tags.merge(in.Tags)
	s.Etag = newEtag()
	wr := b.newWR("UPDATE_STREAM", s.CompartmentID, "stream", s.ID, "UPDATED", "/streams/"+s.ID, b.nowUTC())

	return streamView(s, endpoint), s.Etag, wr, nil
}

// DeleteStream moves a stream to DELETING; its messages are dropped when it becomes DELETED.
func (b *InMemoryBackend) DeleteStream(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteStream")
	defer b.mu.Unlock()

	s, aerr := b.activeStream(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(s.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	s.State, s.TransitionAt, s.Etag = stateDeleting, b.transitionAt(), newEtag()

	return b.newWR("DELETE_STREAM", s.CompartmentID, "stream", s.ID, "DELETED", "/streams/"+s.ID, *s.TransitionAt), nil
}

// ChangeStreamCompartment moves a stream to another compartment.
func (b *InMemoryBackend) ChangeStreamCompartment(id, ifMatch, compartment string) (string, error) {
	if compartment == "" {
		return "", errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeStreamCompartment")
	defer b.mu.Unlock()

	s, aerr := b.activeStream(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(s.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	s.CompartmentID, s.Etag = compartment, newEtag()

	return b.newWR("UPDATE_STREAM", compartment, "stream", s.ID, "UPDATED", "/streams/"+s.ID, b.nowUTC()), nil
}

func (h *Handler) createStream(q *req) error {
	var in CreateStreamIn
	if err := q.decode(&in); err != nil {
		return err
	}

	s, etag, wr, err := h.Backend.CreateStream(in, q.endpoint())

	return replyWR(q, s, etag, wr, err)
}

func (h *Handler) getStream(q *req) error {
	s, etag, err := h.Backend.GetStream(q.param("streamId"), q.endpoint())

	return reply(q, s, etag, err)
}

func (h *Handler) listStreams(q *req) error {
	pg, err := h.Backend.ListStreams(q.query("compartmentId"), q.query("streamPoolId"), q.query("id"),
		q.query("name"), q.query("lifecycleState"), q.endpoint(), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateStream(q *req) error {
	var in UpdateStreamIn
	if err := q.decode(&in); err != nil {
		return err
	}

	s, etag, wr, err := h.Backend.UpdateStream(q.param("streamId"), q.ifMatch(), q.endpoint(), in)

	return replyWR(q, s, etag, wr, err)
}

func (h *Handler) deleteStream(q *req) error {
	wr, err := h.Backend.DeleteStream(q.param("streamId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeStreamCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.ChangeStreamCompartment(q.param("streamId"), q.ifMatch(), in.CompartmentID)

	return acceptedWR(q, wr, err)
}
