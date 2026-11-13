package ocilogging

import (
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const stateSavedSearchActive = "ACTIVE"

// CreateSavedSearchIn is the CreateLogSavedSearch request body.
type CreateSavedSearchIn struct {
	Tags
	RetryToken  string  `json:"-"`
	Description *string `json:"description"`
	Name        string  `json:"name"`
	Query       string  `json:"query"`
	Compartment string  `json:"compartmentId"`
}

// UpdateSavedSearchIn is the UpdateLogSavedSearch request body.
type UpdateSavedSearchIn struct {
	Tags
	Name        *string `json:"name"`
	Description *string `json:"description"`
	Query       *string `json:"query"`
}

// SavedSearchWire is the LogSavedSearch response and the summary item.
type SavedSearchWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeModified   time.Time                 `json:"timeLastModified"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	CompartmentID  string                    `json:"compartmentId"`
	Name           string                    `json:"name"`
	Query          string                    `json:"query"`
	Description    string                    `json:"description,omitempty"`
	LifecycleState string                    `json:"lifecycleState"`
}

func savedSearchView(s *SavedSearch) SavedSearchWire {
	return SavedSearchWire{
		ID: s.ID, CompartmentID: s.CompartmentID, Name: s.Name, Query: s.Query, Description: s.Description,
		LifecycleState: stateSavedSearchActive, TimeCreated: s.TimeCreated, TimeModified: s.TimeModified,
		FreeformTags: s.Tags.FreeformTags, DefinedTags: s.Tags.DefinedTags,
	}
}

func validSavedSearch(name, query string) *APIError {
	if strings.TrimSpace(name) == "" {
		return errMissing("name is required")
	}

	if strings.TrimSpace(query) == "" {
		return errMissing("query is required")
	}

	return nil
}

// CreateSavedSearch stores a saved search; it is ACTIVE immediately.
func (b *InMemoryBackend) CreateSavedSearch(in CreateSavedSearchIn) (SavedSearchWire, string, error) {
	if in.Compartment == "" {
		return SavedSearchWire{}, "", errMissing("compartmentId is required")
	}

	if aerr := firstErr(validSavedSearch(in.Name, in.Query), validDescription(in.Description)); aerr != nil {
		return SavedSearchWire{}, "", aerr
	}

	b.mu.Lock("CreateSavedSearch")
	defer b.mu.Unlock()

	if v, ok := b.retry.Lookup("CreateLogSavedSearch", in.RetryToken, b.nowUTC()); ok {
		s, live := b.savedSearches[v]
		if !live {
			return SavedSearchWire{}, "", errInvalidatedToken()
		}

		return savedSearchView(s), s.Etag, nil
	}

	if aerr := b.checkTags(in.Tags); aerr != nil {
		return SavedSearchWire{}, "", aerr
	}

	now := b.nowUTC()
	s := &SavedSearch{
		ID: newOCID("logsavedsearch"), CompartmentID: in.Compartment, Name: in.Name, Query: in.Query,
		TimeCreated: now, TimeModified: now, Tags: in.Tags, Etag: newEtag(), Seq: b.nextSeq(),
	}
	if in.Description != nil {
		s.Description = *in.Description
	}

	b.savedSearches[s.ID] = s
	b.retry.Remember("CreateLogSavedSearch", in.RetryToken, s.ID, now)

	return savedSearchView(s), s.Etag, nil
}

// GetSavedSearch returns one saved search and its etag.
func (b *InMemoryBackend) GetSavedSearch(id string) (SavedSearchWire, string, error) {
	b.mu.RLock("GetSavedSearch")
	defer b.mu.RUnlock()

	s, ok := b.savedSearches[id]
	if !ok {
		return SavedSearchWire{}, "", errNotFound(id)
	}

	return savedSearchView(s), s.Etag, nil
}

// ListSavedSearches lists the saved searches of a compartment.
func (b *InMemoryBackend) ListSavedSearches(
	compartment, id, name string, o ListOpts,
) (page.Page[SavedSearchWire], error) {
	if compartment == "" {
		return page.Page[SavedSearchWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListSavedSearches")
	defer b.mu.RUnlock()

	var all []*SavedSearch

	for _, s := range b.savedSearches {
		if s.CompartmentID == compartment && (id == "" || s.ID == id) && (name == "" || s.Name == name) {
			all = append(all, s)
		}
	}

	pg := paginate(all, o, func(s *SavedSearch) string { return s.Name },
		func(s *SavedSearch) (time.Time, int64) { return s.TimeCreated, s.Seq })

	out := make([]SavedSearchWire, 0, len(pg.Data))
	for _, s := range pg.Data {
		out = append(out, savedSearchView(s))
	}

	return page.Page[SavedSearchWire]{Data: out, Next: pg.Next}, nil
}

// UpdateSavedSearch changes the mutable fields of a saved search.
func (b *InMemoryBackend) UpdateSavedSearch(
	id, ifMatch string,
	in UpdateSavedSearchIn,
) (SavedSearchWire, string, error) {
	if in.Description != nil {
		if aerr := validDescription(in.Description); aerr != nil {
			return SavedSearchWire{}, "", aerr
		}
	}

	b.mu.Lock("UpdateSavedSearch")
	defer b.mu.Unlock()

	s, ok := b.savedSearches[id]
	if !ok {
		return SavedSearchWire{}, "", errNotFound(id)
	}

	if aerr := checkEtag(s.Etag, ifMatch); aerr != nil {
		return SavedSearchWire{}, "", aerr
	}

	name, query := s.Name, s.Query
	if in.Name != nil {
		name = *in.Name
	}

	if in.Query != nil {
		query = *in.Query
	}

	if aerr := validSavedSearch(name, query); aerr != nil {
		return SavedSearchWire{}, "", aerr
	}

	if aerr := b.checkTags(in.Tags); aerr != nil {
		return SavedSearchWire{}, "", aerr
	}

	s.Name, s.Query = name, query
	if in.Description != nil {
		s.Description = *in.Description
	}

	s.Tags.merge(in.Tags)
	s.TimeModified, s.Etag = b.nowUTC(), newEtag()

	return savedSearchView(s), s.Etag, nil
}

// DeleteSavedSearch removes a saved search.
func (b *InMemoryBackend) DeleteSavedSearch(id, ifMatch string) error {
	b.mu.Lock("DeleteSavedSearch")
	defer b.mu.Unlock()

	s, ok := b.savedSearches[id]
	if !ok {
		return errNotFound(id)
	}

	if aerr := checkEtag(s.Etag, ifMatch); aerr != nil {
		return aerr
	}

	delete(b.savedSearches, id)

	return nil
}

// ChangeSavedSearchCompartment moves a saved search to another compartment.
func (b *InMemoryBackend) ChangeSavedSearchCompartment(id, ifMatch, compartment string) error {
	if compartment == "" {
		return errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeSavedSearchCompartment")
	defer b.mu.Unlock()

	s, ok := b.savedSearches[id]
	if !ok {
		return errNotFound(id)
	}

	if aerr := checkEtag(s.Etag, ifMatch); aerr != nil {
		return aerr
	}

	s.CompartmentID, s.TimeModified, s.Etag = compartment, b.nowUTC(), newEtag()

	return nil
}

func (h *Handler) createSavedSearch(q *req) error {
	var in CreateSavedSearchIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)

	v, etag, err := h.Backend.CreateSavedSearch(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getSavedSearch(q *req) error {
	v, etag, err := h.Backend.GetSavedSearch(q.param("logSavedSearchId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listSavedSearches(q *req) error {
	pg, err := h.Backend.ListSavedSearches(
		q.query("compartmentId"), q.query("logSavedSearchId"), q.query("name"), q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	data := pg.Data
	if data == nil {
		data = []SavedSearchWire{}
	}

	return q.json(http.StatusOK, map[string]any{"items": data}, "")
}

func (h *Handler) updateSavedSearch(q *req) error {
	var in UpdateSavedSearchIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateSavedSearch(q.param("logSavedSearchId"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteSavedSearch(q *req) error {
	if err := h.Backend.DeleteSavedSearch(q.param("logSavedSearchId"), q.ifMatch()); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) changeSavedSearchCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	if err := h.Backend.ChangeSavedSearchCompartment(
		q.param("logSavedSearchId"),
		q.ifMatch(),
		in.CompartmentID,
	); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}
