package ociidentity

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxTagNameLen   = 100
	maxCostTracking = 10
	stateInactive   = "INACTIVE"
)

// CreateTagNamespaceIn is the CreateTagNamespace request body.
type CreateTagNamespaceIn struct {
	Tags
	CompartmentID string `json:"compartmentId"`
	Name          string `json:"name"`
	Description   string `json:"description"`
}

// UpdateTagNamespaceIn is the UpdateTagNamespace request body.
type UpdateTagNamespaceIn struct {
	Description *string `json:"description"`
	IsRetired   *bool   `json:"isRetired"`
	Tags
}

// CreateTagIn is the CreateTag request body.
type CreateTagIn struct {
	Validator      map[string]any `json:"validator"`
	IsCostTracking *bool          `json:"isCostTracking"`
	Tags
	Name        string `json:"name"`
	Description string `json:"description"`
}

// UpdateTagIn is the UpdateTag request body.
type UpdateTagIn struct {
	Description    *string        `json:"description"`
	IsRetired      *bool          `json:"isRetired"`
	IsCostTracking *bool          `json:"isCostTracking"`
	Validator      map[string]any `json:"validator"`
	Tags
}

// CreateTagDefaultIn is the CreateTagDefault request body.
type CreateTagDefaultIn struct {
	IsRequired      *bool  `json:"isRequired"`
	CompartmentID   string `json:"compartmentId"`
	TagDefinitionID string `json:"tagDefinitionId"`
	Value           string `json:"value"`
}

// UpdateTagDefaultIn is the UpdateTagDefault request body.
type UpdateTagDefaultIn struct {
	IsRequired *bool  `json:"isRequired"`
	Value      string `json:"value"`
}

// TagDefaultFilter holds the ListTagDefaults query.
type TagDefaultFilter struct {
	ID, CompartmentID, TagDefinitionID, State string
}

func (n *TagNamespace) view() tagNamespaceWire {
	w := tagNamespaceWire{Meta: n.Meta, IsRetired: n.IsRetired}
	if n.IsRetired && w.LifecycleState == stateActive {
		w.LifecycleState = stateInactive
	}

	return w
}

func (d *TagDef) view() tagWire {
	w := tagWire{
		Meta: d.Meta, Validator: d.Validator, TagNamespaceID: d.NamespaceID, TagNamespaceName: d.NamespaceName,
		IsRetired: d.IsRetired, IsCostTracking: d.IsCostTrack,
	}
	if d.IsRetired && w.LifecycleState == stateActive {
		w.LifecycleState = stateInactive
	}

	return w
}

func (b *InMemoryBackend) tagNSByID(id string) (*TagNamespace, *APIError) {
	n, ok := b.tagNS[id]
	if !ok || n.LifecycleState == stateDeleted {
		return nil, errNotFound(id)
	}

	return n, nil
}

// CreateTagNamespace creates a tag namespace.
func (b *InMemoryBackend) CreateTagNamespace(in CreateTagNamespaceIn) (tagNamespaceWire, string, error) {
	if in.CompartmentID == "" {
		return tagNamespaceWire{}, "", errMissing("compartmentId is required")
	}

	if aerr := validateName(in.Name, maxTagNameLen); aerr != nil {
		return tagNamespaceWire{}, "", aerr
	}

	if aerr := validateDescription(in.Description); aerr != nil {
		return tagNamespaceWire{}, "", aerr
	}

	b.mu.Lock("CreateTagNamespace")
	defer b.mu.Unlock()

	if aerr := b.activeParent(in.CompartmentID); aerr != nil {
		return tagNamespaceWire{}, "", aerr
	}

	for _, n := range b.tagNS {
		if n.LifecycleState != stateDeleted && strs.Equal(n.Name, in.Name) {
			return tagNamespaceWire{}, "", errConflict("TagNamespaceAlreadyExists",
				"Tag namespace "+in.Name+" already exists in the tenancy",
			)
		}
	}

	n := &TagNamespace{
		Meta:     b.newMeta("tagnamespace", in.CompartmentID, in.Name, in.Description, in.Tags),
		Internal: b.newInternal(),
	}
	b.tagNS[n.ID] = n

	return n.view(), n.Internal.Etag, nil
}

// GetTagNamespace returns one namespace.
func (b *InMemoryBackend) GetTagNamespace(id string) (tagNamespaceWire, string, error) {
	b.mu.RLock("GetTagNamespace")
	defer b.mu.RUnlock()

	n, aerr := b.tagNSByID(id)
	if aerr != nil {
		return tagNamespaceWire{}, "", aerr
	}

	return n.view(), n.Internal.Etag, nil
}

// ListTagNamespaces lists the namespaces of a compartment.
func (b *InMemoryBackend) ListTagNamespaces(
	compartment, state string,
	subtree bool,
	o ListOpts,
) (page.Page[tagNamespaceWire], error) {
	if compartment == "" {
		return page.Page[tagNamespaceWire]{}, errMissing("compartmentId is required")
	}

	if aerr := validListOpts(o, "TIMECREATED", "NAME"); aerr != nil {
		return page.Page[tagNamespaceWire]{}, aerr
	}

	if aerr := validState(state, stateActive, stateInactive, stateDeleting, stateDeleted); aerr != nil {
		return page.Page[tagNamespaceWire]{}, aerr
	}

	b.mu.RLock("ListTagNamespaces")
	defer b.mu.RUnlock()

	if !b.compartmentExists(compartment) {
		return page.Page[tagNamespaceWire]{}, errNotFound(compartment)
	}

	var all []*TagNamespace

	for _, n := range b.tagNS {
		inScope := n.CompartmentID == compartment ||
			(subtree && (compartment == TenancyID || b.isDescendant(n.CompartmentID, compartment)))
		if inScope && matchState(n.view().LifecycleState, state) {
			all = append(all, n)
		}
	}

	pg := paginate(all, o, func(n *TagNamespace) string { return n.Name },
		func(n *TagNamespace) (time.Time, int64) { return n.TimeCreated, n.Internal.Seq })

	out := make([]tagNamespaceWire, 0, len(pg.Data))
	for _, n := range pg.Data {
		out = append(out, n.view())
	}

	return page.Page[tagNamespaceWire]{Data: out, Next: pg.Next}, nil
}

// UpdateTagNamespace changes description, retirement and tags.
func (b *InMemoryBackend) UpdateTagNamespace(
	id, ifMatch string,
	in UpdateTagNamespaceIn,
) (tagNamespaceWire, string, error) {
	b.mu.Lock("UpdateTagNamespace")
	defer b.mu.Unlock()

	n, aerr := b.tagNSByID(id)
	if aerr != nil {
		return tagNamespaceWire{}, "", aerr
	}

	if aerr = checkEtag(n.Internal.Etag, ifMatch); aerr != nil {
		return tagNamespaceWire{}, "", aerr
	}

	if in.Description != nil {
		if aerr = validateDescription(*in.Description); aerr != nil {
			return tagNamespaceWire{}, "", aerr
		}

		n.Description = *in.Description
	}

	if in.IsRetired != nil {
		n.IsRetired = *in.IsRetired
	}

	applyTags(&n.Tags, in.Tags)
	n.Internal.Etag = newEtag()

	return n.view(), n.Internal.Etag, nil
}

// DeleteTagNamespace removes a namespace that holds no tag definitions.
func (b *InMemoryBackend) DeleteTagNamespace(id, ifMatch string) error {
	b.mu.Lock("DeleteTagNamespace")
	defer b.mu.Unlock()

	n, aerr := b.tagNSByID(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(n.Internal.Etag, ifMatch); aerr != nil {
		return aerr
	}

	for _, d := range b.tagDefs {
		if d.NamespaceID == id && d.LifecycleState != stateDeleted {
			return errConflict("Conflict", "Tag namespace "+id+" still contains tag definitions")
		}
	}

	n.LifecycleState, n.Internal.Etag = stateDeleted, newEtag()

	return nil
}

func (b *InMemoryBackend) tagByName(nsID, name string) (*TagDef, *APIError) {
	for _, d := range b.tagDefs {
		if d.NamespaceID == nsID && d.LifecycleState != stateDeleted && strs.Equal(d.Name, name) {
			return d, nil
		}
	}

	return nil, errNotFound(name)
}

func (b *InMemoryBackend) costTrackingCount() int {
	n := 0

	for _, d := range b.tagDefs {
		if d.IsCostTrack && d.LifecycleState != stateDeleted {
			n++
		}
	}

	return n
}

// CreateTag creates a tag definition in a namespace.
func (b *InMemoryBackend) CreateTag(nsID string, in CreateTagIn) (tagWire, string, error) {
	if aerr := validateName(in.Name, maxTagNameLen); aerr != nil {
		return tagWire{}, "", aerr
	}

	if aerr := validateDescription(in.Description); aerr != nil {
		return tagWire{}, "", aerr
	}

	b.mu.Lock("CreateTag")
	defer b.mu.Unlock()

	n, aerr := b.tagNSByID(nsID)
	if aerr != nil {
		return tagWire{}, "", aerr
	}

	if n.IsRetired {
		return tagWire{}, "", errState("Tag namespace " + nsID + " is retired")
	}

	if _, found := b.tagByName(nsID, in.Name); found == nil {
		return tagWire{}, "", errConflict("TagDefinitionAlreadyExists",
			"Tag "+in.Name+" already exists in the namespace",
		)
	}

	cost := in.IsCostTracking != nil && *in.IsCostTracking
	if cost && b.costTrackingCount() >= maxCostTracking {
		return tagWire{}, "", errLimit("A tenancy can have at most 10 cost-tracking tags")
	}

	d := &TagDef{
		Meta:        b.newMeta("tagdefinition", n.CompartmentID, in.Name, in.Description, in.Tags),
		NamespaceID: nsID, NamespaceName: n.Name, Validator: in.Validator, IsCostTrack: cost, Internal: b.newInternal(),
	}
	b.tagDefs[d.ID] = d

	return d.view(), d.Internal.Etag, nil
}

// GetTag returns one tag definition.
func (b *InMemoryBackend) GetTag(nsID, name string) (tagWire, string, error) {
	b.mu.RLock("GetTag")
	defer b.mu.RUnlock()

	if _, aerr := b.tagNSByID(nsID); aerr != nil {
		return tagWire{}, "", aerr
	}

	d, aerr := b.tagByName(nsID, name)
	if aerr != nil {
		return tagWire{}, "", aerr
	}

	return d.view(), d.Internal.Etag, nil
}

// ListTags lists the tag definitions of a namespace.
func (b *InMemoryBackend) ListTags(nsID, state string, o ListOpts) (page.Page[tagWire], error) {
	if aerr := validListOpts(ListOpts{Page: o.Page, Limit: o.Limit}); aerr != nil {
		return page.Page[tagWire]{}, aerr
	}

	if aerr := validState(state, stateActive, stateInactive, stateDeleting, stateDeleted); aerr != nil {
		return page.Page[tagWire]{}, aerr
	}

	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	if _, aerr := b.tagNSByID(nsID); aerr != nil {
		return page.Page[tagWire]{}, aerr
	}

	var all []*TagDef

	for _, d := range b.tagDefs {
		if d.NamespaceID == nsID && matchState(d.view().LifecycleState, state) {
			all = append(all, d)
		}
	}

	pg := paginate(all, ListOpts{Page: o.Page, Limit: o.Limit}, func(d *TagDef) string { return d.Name },
		func(d *TagDef) (time.Time, int64) { return d.TimeCreated, d.Internal.Seq })

	out := make([]tagWire, 0, len(pg.Data))
	for _, d := range pg.Data {
		out = append(out, d.view())
	}

	return page.Page[tagWire]{Data: out, Next: pg.Next}, nil
}

// UpdateTag changes a tag definition.
func (b *InMemoryBackend) UpdateTag(nsID, name, ifMatch string, in UpdateTagIn) (tagWire, string, error) {
	b.mu.Lock("UpdateTag")
	defer b.mu.Unlock()

	if _, aerr := b.tagNSByID(nsID); aerr != nil {
		return tagWire{}, "", aerr
	}

	d, aerr := b.tagByName(nsID, name)
	if aerr != nil {
		return tagWire{}, "", aerr
	}

	if aerr = checkEtag(d.Internal.Etag, ifMatch); aerr != nil {
		return tagWire{}, "", aerr
	}

	if in.Description != nil {
		if aerr = validateDescription(*in.Description); aerr != nil {
			return tagWire{}, "", aerr
		}

		d.Description = *in.Description
	}

	if in.IsCostTracking != nil {
		if *in.IsCostTracking && !d.IsCostTrack && b.costTrackingCount() >= maxCostTracking {
			return tagWire{}, "", errLimit("A tenancy can have at most 10 cost-tracking tags")
		}

		d.IsCostTrack = *in.IsCostTracking
	}

	if in.IsRetired != nil {
		d.IsRetired = *in.IsRetired
	}

	if in.Validator != nil {
		d.Validator = in.Validator
	}

	applyTags(&d.Tags, in.Tags)
	d.Internal.Etag = newEtag()

	return d.view(), d.Internal.Etag, nil
}

// DeleteTag starts the asynchronous deletion of a tag definition and its defaults.
func (b *InMemoryBackend) DeleteTag(nsID, name, ifMatch string) (string, error) {
	b.mu.Lock("DeleteTag")
	defer b.mu.Unlock()

	n, aerr := b.tagNSByID(nsID)
	if aerr != nil {
		return "", aerr
	}

	d, aerr := b.tagByName(nsID, name)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(d.Internal.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if d.LifecycleState != stateActive {
		return "", errState("Tag " + name + " is already being deleted")
	}

	for id, td := range b.tagDefaults {
		if td.TagID == d.ID {
			delete(b.tagDefaults, id)
		}
	}

	finish := b.nowUTC().Add(transitionWindow)
	d.LifecycleState, d.Internal.TransitionAt, d.IsCostTrack = stateDeleting, &finish, false

	return b.newWR("DELETE_TAG_DEFINITION", n.CompartmentID, "tag", d.ID, "DELETED", finish), nil
}

func (t *TagDefault) view() tagDefaultWire {
	return tagDefaultWire{
		TimeCreated: t.TimeCreated, ID: t.ID, CompartmentID: t.CompartmentID, TagNamespaceID: t.NamespaceID,
		TagDefinitionID: t.TagID, TagDefinitionName: t.TagName, Value: t.Value, LifecycleState: stateActive,
		IsRequired: t.IsRequired,
	}
}

func validateTagValue(d *TagDef, value string) *APIError {
	if d.Validator == nil {
		return nil
	}

	if kind, _ := d.Validator["validatorType"].(string); !strs.Equal(kind, "ENUM") {
		return nil
	}

	vals, _ := d.Validator["values"].([]any)
	if !slices.Contains(vals, any(value)) {
		return errParam("Value '" + value + "' is not one of the values allowed by the tag's ENUM validator")
	}

	return nil
}

// CreateTagDefault creates a default value for a tag in a compartment.
func (b *InMemoryBackend) CreateTagDefault(in CreateTagDefaultIn) (tagDefaultWire, string, error) {
	if in.CompartmentID == "" || in.TagDefinitionID == "" {
		return tagDefaultWire{}, "", errMissing("compartmentId and tagDefinitionId are required")
	}

	if in.Value == "" {
		return tagDefaultWire{}, "", errMissing("value is required")
	}

	b.mu.Lock("CreateTagDefault")
	defer b.mu.Unlock()

	if aerr := b.activeParent(in.CompartmentID); aerr != nil {
		return tagDefaultWire{}, "", aerr
	}

	d, ok := b.tagDefs[in.TagDefinitionID]
	if !ok || d.LifecycleState != stateActive {
		return tagDefaultWire{}, "", errNotFound(in.TagDefinitionID)
	}

	if aerr := validateTagValue(d, in.Value); aerr != nil {
		return tagDefaultWire{}, "", aerr
	}

	for _, t := range b.tagDefaults {
		if t.CompartmentID == in.CompartmentID && t.TagID == d.ID {
			return tagDefaultWire{}, "", errConflict("Conflict",
				"A default for this tag already exists in the compartment",
			)
		}
	}

	t := &TagDefault{
		TimeCreated:   b.nowUTC(),
		ID:            newOCID("tagdefault"),
		CompartmentID: in.CompartmentID,
		NamespaceID:   d.NamespaceID,
		TagID:         d.ID,
		TagName:       d.Name,
		Value:         in.Value,
		IsRequired:    in.IsRequired != nil && *in.IsRequired,
		Seq:           b.nextSeq(),
	}
	b.tagDefaults[t.ID] = t

	return t.view(), newEtag(), nil
}

// GetTagDefault returns one tag default.
func (b *InMemoryBackend) GetTagDefault(id string) (tagDefaultWire, error) {
	b.mu.RLock("GetTagDefault")
	defer b.mu.RUnlock()

	t, ok := b.tagDefaults[id]
	if !ok {
		return tagDefaultWire{}, errNotFound(id)
	}

	return t.view(), nil
}

// ListTagDefaults lists tag defaults filtered by id, compartment or tag.
func (b *InMemoryBackend) ListTagDefaults(f TagDefaultFilter, o ListOpts) (page.Page[tagDefaultWire], error) {
	if aerr := validState(f.State, stateActive); aerr != nil {
		return page.Page[tagDefaultWire]{}, aerr
	}

	b.mu.RLock("ListTagDefaults")
	defer b.mu.RUnlock()

	var all []*TagDefault

	for _, t := range b.tagDefaults {
		if (f.ID == "" || t.ID == f.ID) && (f.CompartmentID == "" || t.CompartmentID == f.CompartmentID) &&
			(f.TagDefinitionID == "" || t.TagID == f.TagDefinitionID) {
			all = append(all, t)
		}
	}

	pg := paginate(all, ListOpts{Page: o.Page, Limit: o.Limit}, func(t *TagDefault) string { return t.TagName },
		func(t *TagDefault) (time.Time, int64) { return t.TimeCreated, t.Seq })

	out := make([]tagDefaultWire, 0, len(pg.Data))
	for _, t := range pg.Data {
		out = append(out, t.view())
	}

	return page.Page[tagDefaultWire]{Data: out, Next: pg.Next}, nil
}

// UpdateTagDefault changes the value or requirement of a tag default.
func (b *InMemoryBackend) UpdateTagDefault(id string, in UpdateTagDefaultIn) (tagDefaultWire, string, error) {
	if in.Value == "" {
		return tagDefaultWire{}, "", errMissing("value is required")
	}

	b.mu.Lock("UpdateTagDefault")
	defer b.mu.Unlock()

	t, ok := b.tagDefaults[id]
	if !ok {
		return tagDefaultWire{}, "", errNotFound(id)
	}

	if d, found := b.tagDefs[t.TagID]; found {
		if aerr := validateTagValue(d, in.Value); aerr != nil {
			return tagDefaultWire{}, "", aerr
		}
	}

	t.Value = in.Value
	if in.IsRequired != nil {
		t.IsRequired = *in.IsRequired
	}

	return t.view(), newEtag(), nil
}

// DeleteTagDefault removes a tag default.
func (b *InMemoryBackend) DeleteTagDefault(id string) error {
	b.mu.Lock("DeleteTagDefault")
	defer b.mu.Unlock()

	if _, ok := b.tagDefaults[id]; !ok {
		return errNotFound(id)
	}

	delete(b.tagDefaults, id)

	return nil
}

func (h *Handler) createTagNamespace(q *req) error {
	var in CreateTagNamespaceIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreateTagNamespace(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getTagNamespace(q *req) error {
	v, etag, err := h.Backend.GetTagNamespace(q.param("tagNamespaceId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listTagNamespaces(q *req) error {
	sub := q.query("includeSubcompartments") == "true"
	pg, err := h.Backend.ListTagNamespaces(q.query("compartmentId"), q.query("lifecycleState"), sub, q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateTagNamespace(q *req) error {
	var in UpdateTagNamespaceIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateTagNamespace(q.param("tagNamespaceId"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteTagNamespace(q *req) error {
	return done(q, h.Backend.DeleteTagNamespace(q.param("tagNamespaceId"), q.ifMatch()))
}

func (h *Handler) createTag(q *req) error {
	var in CreateTagIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreateTag(q.param("tagNamespaceId"), in)

	return reply(q, v, etag, err)
}

func (h *Handler) getTag(q *req) error {
	v, etag, err := h.Backend.GetTag(q.param("tagNamespaceId"), q.param("tagName"))

	return reply(q, v, etag, err)
}

func (h *Handler) listTags(q *req) error {
	pg, err := h.Backend.ListTags(q.param("tagNamespaceId"), q.query("lifecycleState"), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateTag(q *req) error {
	var in UpdateTagIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateTag(q.param("tagNamespaceId"), q.param("tagName"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteTag(q *req) error {
	wr, err := h.Backend.DeleteTag(q.param("tagNamespaceId"), q.param("tagName"), q.ifMatch())

	return accepted(q, wr, err)
}

func (h *Handler) createTagDefault(q *req) error {
	var in CreateTagDefaultIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreateTagDefault(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getTagDefault(q *req) error {
	v, err := h.Backend.GetTagDefault(q.param("tagDefaultId"))

	return reply(q, v, "", err)
}

func (h *Handler) listTagDefaults(q *req) error {
	f := TagDefaultFilter{
		ID: q.query("id"), CompartmentID: q.query("compartmentId"), TagDefinitionID: q.query("tagDefinitionId"),
		State: q.query("lifecycleState"),
	}
	pg, err := h.Backend.ListTagDefaults(f, q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateTagDefault(q *req) error {
	var in UpdateTagDefaultIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateTagDefault(q.param("tagDefaultId"), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteTagDefault(q *req) error {
	return done(q, h.Backend.DeleteTagDefault(q.param("tagDefaultId")))
}
