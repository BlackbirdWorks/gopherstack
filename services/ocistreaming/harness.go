package ocistreaming

import (
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type harnessIn struct {
	Tags
	Name        string `json:"name"`
	Compartment string `json:"compartmentId"`
}

type harnessWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	Name           string                    `json:"name"`
	CompartmentID  string                    `json:"compartmentId"`
	LifecycleState string                    `json:"lifecycleState"`
}

func harnessView(h *Harness) harnessWire {
	return harnessWire{
		ID: h.ID, Name: h.Name, CompartmentID: h.CompartmentID, LifecycleState: h.State, TimeCreated: h.TimeCreated,
		FreeformTags: h.Tags.FreeformTags, DefinedTags: h.Tags.DefinedTags,
	}
}

func (b *InMemoryBackend) activeHarness(id string) (*Harness, *APIError) {
	h, ok := b.harnesses[id]
	if !ok || h.State == stateDeleted {
		return nil, errNotFound(id)
	}

	if h.State != stateActive {
		return nil, errState("Connect harness " + id + " is in state " + h.State + "; the operation requires ACTIVE")
	}

	return h, nil
}

// CreateHarness creates a connect harness.
func (b *InMemoryBackend) CreateHarness(in harnessIn) (harnessWire, string, string, error) {
	if in.Compartment == "" {
		return harnessWire{}, "", "", errMissing("compartmentId is required")
	}

	if aerr := validateName(in.Name); aerr != nil {
		return harnessWire{}, "", "", aerr
	}

	b.mu.Lock("CreateHarness")
	defer b.mu.Unlock()

	h := &Harness{
		ID: newOCID("connectharness"), CompartmentID: in.Compartment, Name: in.Name, State: stateCreating,
		TimeCreated: b.nowUTC(), TransitionAt: b.transitionAt(), Tags: in.Tags, Etag: newEtag(), Seq: b.nextSeq(),
	}
	b.harnesses[h.ID] = h
	wr := b.newWR("CREATE_CONNECT_HARNESS", h.CompartmentID, "connectharness", h.ID, "CREATED",
		"/connectharnesses/"+h.ID, *h.TransitionAt)

	return harnessView(h), h.Etag, wr, nil
}

// GetHarness returns one connect harness.
func (b *InMemoryBackend) GetHarness(id string) (harnessWire, string, error) {
	b.mu.RLock("GetHarness")
	defer b.mu.RUnlock()

	h, ok := b.harnesses[id]
	if !ok {
		return harnessWire{}, "", errNotFound(id)
	}

	return harnessView(h), h.Etag, nil
}

// ListHarnesses lists the connect harnesses of a compartment.
func (b *InMemoryBackend) ListHarnesses(
	compartment, id, name, state string,
	o ListOpts,
) (page.Page[harnessWire], error) {
	if compartment == "" {
		return page.Page[harnessWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListHarnesses")
	defer b.mu.RUnlock()

	var all []*Harness

	for _, h := range b.harnesses {
		if h.CompartmentID == compartment && (id == "" || h.ID == id) && (name == "" || h.Name == name) &&
			(state == "" || strings.EqualFold(h.State, state)) {
			all = append(all, h)
		}
	}

	pg := paginate(all, o, func(h *Harness) string { return h.Name },
		func(h *Harness) (time.Time, int64) { return h.TimeCreated, h.Seq })

	out := make([]harnessWire, 0, len(pg.Data))
	for _, h := range pg.Data {
		out = append(out, harnessView(h))
	}

	return page.Page[harnessWire]{Data: out, Next: pg.Next}, nil
}

// UpdateHarness changes the tags of a connect harness.
func (b *InMemoryBackend) UpdateHarness(id, ifMatch string, tags Tags) (harnessWire, string, string, error) {
	b.mu.Lock("UpdateHarness")
	defer b.mu.Unlock()

	h, aerr := b.activeHarness(id)
	if aerr != nil {
		return harnessWire{}, "", "", aerr
	}

	if aerr = checkEtag(h.Etag, ifMatch); aerr != nil {
		return harnessWire{}, "", "", aerr
	}

	h.Tags.merge(tags)
	h.Etag = newEtag()
	wr := b.newWR("UPDATE_CONNECT_HARNESS", h.CompartmentID, "connectharness", h.ID, "UPDATED",
		"/connectharnesses/"+h.ID, b.nowUTC())

	return harnessView(h), h.Etag, wr, nil
}

// DeleteHarness moves a connect harness to DELETING.
func (b *InMemoryBackend) DeleteHarness(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteHarness")
	defer b.mu.Unlock()

	h, aerr := b.activeHarness(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(h.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	h.State, h.TransitionAt, h.Etag = stateDeleting, b.transitionAt(), newEtag()

	return b.newWR("DELETE_CONNECT_HARNESS", h.CompartmentID, "connectharness", h.ID, "DELETED",
		"/connectharnesses/"+h.ID, *h.TransitionAt), nil
}

// ChangeHarnessCompartment moves a connect harness to another compartment.
func (b *InMemoryBackend) ChangeHarnessCompartment(id, ifMatch, compartment string) (string, error) {
	if compartment == "" {
		return "", errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeHarnessCompartment")
	defer b.mu.Unlock()

	h, aerr := b.activeHarness(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(h.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	h.CompartmentID, h.Etag = compartment, newEtag()

	return b.newWR("UPDATE_CONNECT_HARNESS", compartment, "connectharness", h.ID, "UPDATED",
		"/connectharnesses/"+h.ID, b.nowUTC()), nil
}

func (h *Handler) createHarness(q *req) error {
	var in harnessIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, wr, err := h.Backend.CreateHarness(in)

	return replyWR(q, v, etag, wr, err)
}

func (h *Handler) getHarness(q *req) error {
	v, etag, err := h.Backend.GetHarness(q.param("connectHarnessId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listHarnesses(q *req) error {
	pg, err := h.Backend.ListHarnesses(q.query("compartmentId"), q.query("id"), q.query("name"),
		q.query("lifecycleState"), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateHarness(q *req) error {
	var in Tags
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, wr, err := h.Backend.UpdateHarness(q.param("connectHarnessId"), q.ifMatch(), in)

	return replyWR(q, v, etag, wr, err)
}

func (h *Handler) deleteHarness(q *req) error {
	wr, err := h.Backend.DeleteHarness(q.param("connectHarnessId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeHarnessCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.ChangeHarnessCompartment(q.param("connectHarnessId"), q.ifMatch(), in.CompartmentID)

	return acceptedWR(q, wr, err)
}
