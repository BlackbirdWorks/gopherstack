package ocilogging

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateLogGroupIn is the CreateLogGroup request body.
type CreateLogGroupIn struct {
	Tags
	Description *string `json:"description"`
	Name        string  `json:"displayName"`
	Compartment string  `json:"compartmentId"`
}

// UpdateLogGroupIn is the UpdateLogGroup request body.
type UpdateLogGroupIn struct {
	Tags
	Name        *string `json:"displayName"`
	Description *string `json:"description"`
}

// LogGroupWire is the GetLogGroup response and the ListLogGroups item.
type LogGroupWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeModified   time.Time                 `json:"timeLastModified"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	CompartmentID  string                    `json:"compartmentId"`
	Name           string                    `json:"displayName"`
	Description    string                    `json:"description,omitempty"`
	LifecycleState string                    `json:"lifecycleState"`
}

func groupView(g *LogGroup) LogGroupWire {
	return LogGroupWire{
		ID: g.ID, CompartmentID: g.CompartmentID, Name: g.Name, Description: g.Description, LifecycleState: g.State,
		TimeCreated: g.TimeCreated, TimeModified: g.TimeModified, FreeformTags: g.Tags.FreeformTags,
		DefinedTags: g.Tags.DefinedTags,
	}
}

func (b *InMemoryBackend) liveGroup(id string) (*LogGroup, *APIError) {
	g, ok := b.groups[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return g, nil
}

func (b *InMemoryBackend) mutableGroup(id string) (*LogGroup, *APIError) {
	g, aerr := b.liveGroup(id)
	if aerr != nil {
		return nil, aerr
	}

	if g.State != stateActive {
		return nil, errState("Log group " + id + " is in state " + g.State + "; the operation requires ACTIVE")
	}

	return g, nil
}

// groupNameTaken reports whether another live group in the compartment already uses name.
func (b *InMemoryBackend) groupNameTaken(compartment, name, except string) bool {
	for _, g := range b.groups {
		if g.ID != except && g.CompartmentID == compartment && g.Name == name {
			return true
		}
	}

	return false
}

func validDescription(d *string) *APIError {
	if d != nil && len(*d) > maxDescription {
		return errParam("description must be at most 400 characters")
	}

	return nil
}

// CreateLogGroup creates a log group in CREATING state and returns its work request.
func (b *InMemoryBackend) CreateLogGroup(in CreateLogGroupIn) (string, error) {
	if in.Compartment == "" {
		return "", errMissing("compartmentId is required")
	}

	if aerr := firstErr(validateName(in.Name), validDescription(in.Description)); aerr != nil {
		return "", aerr
	}

	b.mu.Lock("CreateLogGroup")
	defer b.mu.Unlock()

	b.settleAll()

	if b.groupNameTaken(in.Compartment, in.Name, "") {
		return "", errConflict("A log group named " + in.Name + " already exists in the compartment")
	}

	now := b.nowUTC()
	g := &LogGroup{
		ID: newOCID("loggroup"), CompartmentID: in.Compartment, Name: in.Name, State: stateCreating,
		TimeCreated: now, TimeModified: now, TransitionAt: b.transitionAt(), Tags: in.Tags, Etag: newEtag(),
		Seq: b.nextSeq(),
	}
	if in.Description != nil {
		g.Description = *in.Description
	}

	b.groups[g.ID] = g

	return b.newWR("CREATE_LOG_GROUP", "loggroup", g.CompartmentID, g.ID, "CREATED", *g.TransitionAt), nil
}

// GetLogGroup returns one log group and its etag.
func (b *InMemoryBackend) GetLogGroup(id string) (LogGroupWire, string, error) {
	b.mu.Lock("GetLogGroup")
	defer b.mu.Unlock()

	b.settleAll()

	g, aerr := b.liveGroup(id)
	if aerr != nil {
		return LogGroupWire{}, "", aerr
	}

	return groupView(g), g.Etag, nil
}

// ListLogGroups lists the log groups of a compartment.
func (b *InMemoryBackend) ListLogGroups(compartment, name string, o ListOpts) (page.Page[LogGroupWire], error) {
	if compartment == "" {
		return page.Page[LogGroupWire]{}, errMissing("compartmentId is required")
	}

	b.mu.Lock("ListLogGroups")
	defer b.mu.Unlock()

	b.settleAll()

	var all []*LogGroup

	for _, g := range b.groups {
		if g.CompartmentID == compartment && (name == "" || g.Name == name) {
			all = append(all, g)
		}
	}

	pg := paginate(all, o, func(g *LogGroup) string { return g.Name },
		func(g *LogGroup) (time.Time, int64) { return g.TimeCreated, g.Seq })

	out := make([]LogGroupWire, 0, len(pg.Data))
	for _, g := range pg.Data {
		out = append(out, groupView(g))
	}

	return page.Page[LogGroupWire]{Data: out, Next: pg.Next}, nil
}

// UpdateLogGroup changes the mutable fields of a log group; it passes through UPDATING.
func (b *InMemoryBackend) UpdateLogGroup(id, ifMatch string, in UpdateLogGroupIn) (string, error) {
	if in.Name != nil {
		if aerr := validateName(*in.Name); aerr != nil {
			return "", aerr
		}
	}

	if aerr := validDescription(in.Description); aerr != nil {
		return "", aerr
	}

	b.mu.Lock("UpdateLogGroup")
	defer b.mu.Unlock()

	b.settleAll()

	g, aerr := b.mutableGroup(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(g.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if in.Name != nil {
		if b.groupNameTaken(g.CompartmentID, *in.Name, g.ID) {
			return "", errConflict("A log group named " + *in.Name + " already exists in the compartment")
		}

		g.Name = *in.Name
	}

	if in.Description != nil {
		g.Description = *in.Description
	}

	g.Tags.merge(in.Tags)
	g.State, g.TransitionAt, g.TimeModified, g.Etag = stateUpdating, b.transitionAt(), b.nowUTC(), newEtag()

	return b.newWR("UPDATE_LOG_GROUP", "loggroup", g.CompartmentID, g.ID, "UPDATED", *g.TransitionAt), nil
}

// DeleteLogGroup moves an empty log group to DELETING.
func (b *InMemoryBackend) DeleteLogGroup(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteLogGroup")
	defer b.mu.Unlock()

	b.settleAll()

	g, aerr := b.mutableGroup(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(g.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	for _, l := range b.logs {
		if l.GroupID == id {
			return "", errConflict("Log group " + id + " still contains logs; delete them first")
		}
	}

	g.State, g.TransitionAt, g.TimeModified, g.Etag = stateDeleting, b.transitionAt(), b.nowUTC(), newEtag()

	return b.newWR("DELETE_LOG_GROUP", "loggroup", g.CompartmentID, g.ID, "DELETED", *g.TransitionAt), nil
}

// ChangeLogGroupCompartment moves a log group to another compartment.
func (b *InMemoryBackend) ChangeLogGroupCompartment(id, ifMatch, compartment string) (string, error) {
	if compartment == "" {
		return "", errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeLogGroupCompartment")
	defer b.mu.Unlock()

	b.settleAll()

	g, aerr := b.mutableGroup(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(g.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if b.groupNameTaken(compartment, g.Name, g.ID) {
		return "", errConflict("A log group named " + g.Name + " already exists in the target compartment")
	}

	g.CompartmentID, g.Etag, g.TimeModified = compartment, newEtag(), b.nowUTC()

	return b.newWR("MOVE_LOG_GROUP", "loggroup", compartment, g.ID, "UPDATED", b.nowUTC()), nil
}

type changeCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

func (h *Handler) createLogGroup(q *req) error {
	var in CreateLogGroupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.CreateLogGroup(in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getLogGroup(q *req) error {
	v, etag, err := h.Backend.GetLogGroup(q.param("logGroupId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listLogGroups(q *req) error {
	pg, err := h.Backend.ListLogGroups(q.query("compartmentId"), q.query("displayName"), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateLogGroup(q *req) error {
	var in UpdateLogGroupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateLogGroup(q.param("logGroupId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteLogGroup(q *req) error {
	wr, err := h.Backend.DeleteLogGroup(q.param("logGroupId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeLogGroupCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.ChangeLogGroupCompartment(q.param("logGroupId"), q.ifMatch(), in.CompartmentID)

	return acceptedWR(q, wr, err)
}
