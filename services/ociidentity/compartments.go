package ociidentity

import (
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxCompartmentDepth   = 6
	maxCompartmentNameLen = 100
	maxDescriptionLen     = 400
	maxCompartments       = 1000
)

// CreateCompartmentIn is the CreateCompartment request body.
type CreateCompartmentIn struct {
	Tags
	CompartmentID string `json:"compartmentId"`
	Name          string `json:"name"`
	Description   string `json:"description"`
}

// UpdateCompartmentIn is the UpdateCompartment request body.
type UpdateCompartmentIn struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	Tags
}

func validateName(name string, maxLen int) *APIError {
	if strings.TrimSpace(name) == "" {
		return errMissing("name is required")
	}

	if len(name) > maxLen {
		return errParam("name must be at most " + itoa(maxLen) + " characters")
	}

	return nil
}

func validateDescription(desc string) *APIError {
	if strings.TrimSpace(desc) == "" {
		return errMissing("description is required")
	}

	if len(desc) > maxDescriptionLen {
		return errParam("description must be at most " + itoa(maxDescriptionLen) + " characters")
	}

	return nil
}

func (b *InMemoryBackend) rootView() compartmentWire {
	return compartmentWire{IsAccessible: true, Meta: Meta{
		ID: TenancyID, CompartmentID: TenancyID, Name: tenancyName, Description: "Root compartment of the tenancy",
		TimeCreated: b.created, LifecycleState: stateActive, Tags: nonNilTags(Tags{}),
	}}
}

func (c *Compartment) view() compartmentWire {
	return compartmentWire{Meta: c.Meta, IsAccessible: true}
}

func (b *InMemoryBackend) compartmentByID(id string) (*Compartment, *APIError) {
	c, ok := b.compartment[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return c, nil
}

// compartmentExists reports whether id is the tenancy or a known compartment.
func (b *InMemoryBackend) compartmentExists(id string) bool {
	if id == TenancyID {
		return true
	}

	_, ok := b.compartment[id]

	return ok
}

func (b *InMemoryBackend) activeParent(id string) *APIError {
	if id == TenancyID {
		return nil
	}

	p, aerr := b.compartmentByID(id)
	if aerr != nil {
		return aerr
	}

	if p.LifecycleState != stateActive {
		return errState("Compartment " + id + " is in state " + p.LifecycleState + "; the operation requires ACTIVE")
	}

	return nil
}

// depthOf is 0 for the tenancy and 1 for its direct children.
func (b *InMemoryBackend) depthOf(id string) int {
	depth := 0

	for id != TenancyID {
		c, ok := b.compartment[id]
		if !ok {
			break
		}

		depth++
		id = c.CompartmentID
	}

	return depth
}

func (b *InMemoryBackend) subtreeHeight(id string) int {
	h := 0

	for _, c := range b.compartment {
		if c.CompartmentID == id && c.LifecycleState != stateDeleted {
			h = max(h, 1+b.subtreeHeight(c.ID))
		}
	}

	return h
}

func (b *InMemoryBackend) isDescendant(id, ancestor string) bool {
	for id != TenancyID {
		if id == ancestor {
			return true
		}

		c, ok := b.compartment[id]
		if !ok {
			return false
		}

		id = c.CompartmentID
	}

	return false
}

func (b *InMemoryBackend) nameTaken(parent, name, except string) bool {
	for _, c := range b.compartment {
		if c.CompartmentID == parent && c.ID != except && c.LifecycleState != stateDeleted && strs.Equal(c.Name, name) {
			return true
		}
	}

	return false
}

func errCompartmentExists(name string) *APIError {
	return errCode(http.StatusConflict, "CompartmentAlreadyExists", "Compartment "+name+" already exists in this parent")
}

// CreateCompartment creates a compartment under the tenancy or another compartment.
func (b *InMemoryBackend) CreateCompartment(in CreateCompartmentIn) (compartmentWire, string, error) {
	if in.CompartmentID == "" {
		return compartmentWire{}, "", errMissing("compartmentId is required")
	}

	if aerr := validateName(in.Name, maxCompartmentNameLen); aerr != nil {
		return compartmentWire{}, "", aerr
	}

	if aerr := validateDescription(in.Description); aerr != nil {
		return compartmentWire{}, "", aerr
	}

	b.mu.Lock("CreateCompartment")
	defer b.mu.Unlock()

	if aerr := b.activeParent(in.CompartmentID); aerr != nil {
		return compartmentWire{}, "", aerr
	}

	if b.depthOf(in.CompartmentID)+1 > maxCompartmentDepth {
		return compartmentWire{}, "", errLimit("Compartments can be nested at most six levels deep")
	}

	if len(b.compartment) >= maxCompartments {
		return compartmentWire{}, "", errLimit("The tenancy compartment limit (1000) has been reached")
	}

	if b.nameTaken(in.CompartmentID, in.Name, "") {
		return compartmentWire{}, "", errCompartmentExists(in.Name)
	}

	c := &Compartment{
		Meta:     b.newMeta("compartment", in.CompartmentID, in.Name, in.Description, in.Tags),
		Internal: b.newInternal(),
	}
	b.compartment[c.ID] = c

	return c.view(), c.Internal.Etag, nil
}

// GetCompartment returns one compartment (the tenancy id addresses the root compartment).
func (b *InMemoryBackend) GetCompartment(id string) (compartmentWire, string, error) {
	b.mu.RLock("GetCompartment")
	defer b.mu.RUnlock()

	if id == TenancyID {
		return b.rootView(), "", nil
	}

	c, aerr := b.compartmentByID(id)
	if aerr != nil {
		return compartmentWire{}, "", aerr
	}

	return c.view(), c.Internal.Etag, nil
}

// CompartmentFilter holds the ListCompartments query.
type CompartmentFilter struct {
	CompartmentID string
	AccessLevel   string
	Name          string
	State         string
	Subtree       bool
}

// ListCompartments lists the children (or the whole subtree) of a compartment.
func (b *InMemoryBackend) ListCompartments(f CompartmentFilter, o ListOpts) (page.Page[compartmentWire], error) {
	if f.CompartmentID == "" {
		return page.Page[compartmentWire]{}, errMissing("compartmentId is required")
	}

	if aerr := validListOpts(o, "TIMECREATED", "NAME"); aerr != nil {
		return page.Page[compartmentWire]{}, aerr
	}

	if f.AccessLevel != "" && !strings.EqualFold(f.AccessLevel, "ANY") && !strings.EqualFold(f.AccessLevel, "ACCESSIBLE") {
		return page.Page[compartmentWire]{}, errParam("accessLevel must be ANY or ACCESSIBLE")
	}

	if aerr := validState(f.State, stateCreating, stateActive, "INACTIVE", stateDeleting, stateDeleted); aerr != nil {
		return page.Page[compartmentWire]{}, aerr
	}

	if f.Subtree && f.CompartmentID != TenancyID {
		return page.Page[compartmentWire]{}, errParam("compartmentIdInSubtree can only be set when listing the tenancy")
	}

	b.mu.RLock("ListCompartments")
	defer b.mu.RUnlock()

	if !b.compartmentExists(f.CompartmentID) {
		return page.Page[compartmentWire]{}, errNotFound(f.CompartmentID)
	}

	var all []*Compartment

	for _, c := range b.compartment {
		inScope := c.CompartmentID == f.CompartmentID || f.Subtree

		if !inScope || (f.Name != "" && c.Name != f.Name) || !matchState(c.LifecycleState, f.State) {
			continue
		}

		all = append(all, c)
	}

	pg := paginate(all, o, func(c *Compartment) string { return c.Name },
		func(c *Compartment) (time.Time, int64) { return c.TimeCreated, c.Internal.Seq })

	out := make([]compartmentWire, 0, len(pg.Data))
	for _, c := range pg.Data {
		out = append(out, c.view())
	}

	return page.Page[compartmentWire]{Data: out, Next: pg.Next}, nil
}

// UpdateCompartment changes name, description and tags.
func (b *InMemoryBackend) UpdateCompartment(id, ifMatch string, in UpdateCompartmentIn) (compartmentWire, string, error) {
	b.mu.Lock("UpdateCompartment")
	defer b.mu.Unlock()

	if id == TenancyID {
		return compartmentWire{}, "", errParam("The root compartment cannot be updated through the compartment API")
	}

	c, aerr := b.compartmentByID(id)
	if aerr != nil {
		return compartmentWire{}, "", aerr
	}

	if aerr = checkEtag(c.Internal.Etag, ifMatch); aerr != nil {
		return compartmentWire{}, "", aerr
	}

	if c.LifecycleState != stateActive {
		return compartmentWire{}, "", errState("Compartment " + id + " is in state " + c.LifecycleState)
	}

	if in.Name != nil {
		if aerr = validateName(*in.Name, maxCompartmentNameLen); aerr != nil {
			return compartmentWire{}, "", aerr
		}

		if b.nameTaken(c.CompartmentID, *in.Name, id) {
			return compartmentWire{}, "", errCompartmentExists(*in.Name)
		}

		c.Name = *in.Name
	}

	if in.Description != nil {
		if aerr = validateDescription(*in.Description); aerr != nil {
			return compartmentWire{}, "", aerr
		}

		c.Description = *in.Description
	}

	applyTags(&c.Tags, in.Tags)
	c.Internal.Etag = newEtag()

	return c.view(), c.Internal.Etag, nil
}

func applyTags(dst *Tags, src Tags) {
	if src.FreeformTags != nil {
		dst.FreeformTags = src.FreeformTags
	}

	if src.DefinedTags != nil {
		dst.DefinedTags = src.DefinedTags
	}
}

func (b *InMemoryBackend) compartmentContents(id string) string {
	for _, c := range b.compartment {
		if c.CompartmentID == id && c.LifecycleState != stateDeleted {
			return "compartment " + c.ID
		}
	}

	for _, p := range b.policies {
		if p.CompartmentID == id {
			return "policy " + p.ID
		}
	}

	for _, n := range b.tagNS {
		if n.CompartmentID == id && n.LifecycleState != stateDeleted {
			return "tag namespace " + n.ID
		}
	}

	for _, d := range b.tagDefaults {
		if d.CompartmentID == id {
			return "tag default " + d.ID
		}
	}

	return ""
}

// DeleteCompartment starts the asynchronous deletion of an empty compartment and returns the work request id.
func (b *InMemoryBackend) DeleteCompartment(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteCompartment")
	defer b.mu.Unlock()

	if id == TenancyID {
		return "", errParam("The root compartment cannot be deleted")
	}

	c, aerr := b.compartmentByID(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(c.Internal.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if c.LifecycleState != stateActive {
		return "", errState("Compartment " + id + " is in state " + c.LifecycleState + "; the operation requires ACTIVE")
	}

	if what := b.compartmentContents(id); what != "" {
		return "", errCode(http.StatusConflict, "Conflict", "Compartment "+id+" is not empty: it still contains "+what)
	}

	finish := b.nowUTC().Add(transitionWindow)
	c.LifecycleState, c.Internal.TransitionAt = stateDeleting, &finish

	return b.newWR("DELETE_COMPARTMENT", c.CompartmentID, "compartment", id, "DELETED", finish), nil
}

// MoveCompartment re-parents a compartment tree and returns the work request id.
func (b *InMemoryBackend) MoveCompartment(id, target, ifMatch string) (string, error) {
	if target == "" {
		return "", errMissing("targetCompartmentId is required")
	}

	b.mu.Lock("MoveCompartment")
	defer b.mu.Unlock()

	if id == TenancyID {
		return "", errParam("The root compartment cannot be moved")
	}

	c, aerr := b.compartmentByID(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(c.Internal.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if c.LifecycleState != stateActive {
		return "", errState("Compartment " + id + " is in state " + c.LifecycleState + "; the operation requires ACTIVE")
	}

	if aerr = b.activeParent(target); aerr != nil {
		return "", aerr
	}

	switch {
	case target == c.CompartmentID:
		return "", errParam("The compartment is already a child of the target compartment")
	case b.isDescendant(target, id):
		return "", errParam("A compartment cannot be moved into itself or one of its descendants")
	case b.depthOf(target)+1+b.subtreeHeight(id) > maxCompartmentDepth:
		return "", errLimit("The move would nest compartments more than six levels deep")
	case b.nameTaken(target, c.Name, id):
		return "", errCompartmentExists(c.Name)
	}

	old := c.CompartmentID
	c.CompartmentID, c.Internal.Etag = target, newEtag()

	return b.newWR("MOVE_COMPARTMENT", old, "compartment", id, "UPDATED", b.nowUTC().Add(transitionWindow)), nil
}

// RecoverCompartment restores a DELETED compartment to ACTIVE.
func (b *InMemoryBackend) RecoverCompartment(id string) (compartmentWire, string, error) {
	b.mu.Lock("RecoverCompartment")
	defer b.mu.Unlock()

	c, aerr := b.compartmentByID(id)
	if aerr != nil {
		return compartmentWire{}, "", aerr
	}

	if c.LifecycleState != stateDeleted {
		return compartmentWire{}, "", errState("Compartment " + id + " is in state " + c.LifecycleState + "; the operation requires DELETED")
	}

	if c.Internal.TimeDeleted != nil && b.nowUTC().Sub(*c.Internal.TimeDeleted) > recoverWindow {
		return compartmentWire{}, "", errState("Compartment " + id + " was deleted more than 90 days ago and cannot be recovered")
	}

	if aerr = b.activeParent(c.CompartmentID); aerr != nil {
		return compartmentWire{}, "", aerr
	}

	if b.nameTaken(c.CompartmentID, c.Name, id) {
		return compartmentWire{}, "", errCompartmentExists(c.Name)
	}

	c.LifecycleState, c.Internal.TimeDeleted, c.Internal.Etag = stateActive, nil, newEtag()

	return c.view(), c.Internal.Etag, nil
}

func (h *Handler) createCompartment(q *req) error {
	var in CreateCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreateCompartment(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getCompartment(q *req) error {
	v, etag, err := h.Backend.GetCompartment(q.param("compartmentId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listCompartments(q *req) error {
	f := CompartmentFilter{
		CompartmentID: q.query("compartmentId"), AccessLevel: q.query("accessLevel"), Name: q.query("name"),
		State: q.query("lifecycleState"), Subtree: strings.EqualFold(q.query("compartmentIdInSubtree"), "true"),
	}
	pg, err := h.Backend.ListCompartments(f, q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateCompartment(q *req) error {
	var in UpdateCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateCompartment(q.param("compartmentId"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteCompartment(q *req) error {
	wr, err := h.Backend.DeleteCompartment(q.param("compartmentId"), q.ifMatch())

	return accepted(q, wr, err)
}

func (h *Handler) moveCompartment(q *req) error {
	var in struct {
		TargetCompartmentID string `json:"targetCompartmentId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.MoveCompartment(q.param("compartmentId"), in.TargetCompartmentID, q.ifMatch())

	return accepted(q, wr, err)
}

func (h *Handler) recoverCompartment(q *req) error {
	v, etag, err := h.Backend.RecoverCompartment(q.param("compartmentId"))

	return reply(q, v, etag, err)
}
