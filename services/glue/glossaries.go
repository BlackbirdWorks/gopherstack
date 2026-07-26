package glue

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// Glossary represents a Glue Data Catalog business glossary. Field-diffed
// against aws-sdk-go-v2/service/glue's CreateGlossaryOutput/GetGlossaryOutput/
// UpdateGlossaryOutput/types.GlossaryItem, all of which share exactly these
// three fields, so this one struct doubles as the wire shape for every
// glossary read/write response (see handler_glossaries.go).
type Glossary struct {
	ID          string `json:"Id"`
	Name        string `json:"Name"`
	Description string `json:"Description,omitempty"`
}

// GlossaryTerm represents a term within a business glossary. Field-diffed
// against CreateGlossaryTermOutput/GetGlossaryTermOutput/
// UpdateGlossaryTermOutput, which share exactly these five fields.
type GlossaryTerm struct {
	ID               string `json:"Id"`
	GlossaryID       string `json:"GlossaryId"`
	Name             string `json:"Name"`
	ShortDescription string `json:"ShortDescription,omitempty"`
	LongDescription  string `json:"LongDescription,omitempty"`
}

// GlossaryTermItem is the summary shape returned by ListGlossaryTerms,
// field-diffed against types.GlossaryTermItem (Id/Name/ShortDescription only
// -- no GlossaryId or LongDescription, unlike the full GlossaryTerm shape).
type GlossaryTermItem struct {
	ID               string `json:"Id"`
	Name             string `json:"Name"`
	ShortDescription string `json:"ShortDescription,omitempty"`
}

func cloneGlossary(g *Glossary) *Glossary {
	cp := *g

	return &cp
}

func cloneGlossaryTerm(t *GlossaryTerm) *GlossaryTerm {
	cp := *t

	return &cp
}

// CreateGlossary creates a new business glossary.
func (b *InMemoryBackend) CreateGlossary(name, description string) (*Glossary, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.mu.Lock("CreateGlossary")
	defer b.mu.Unlock()

	g := &Glossary{ID: "gls-" + uuid.NewString()[:8], Name: name, Description: description}
	b.glossaries.Put(g)

	return cloneGlossary(g), nil
}

// GetGlossary returns a glossary by its identifier.
func (b *InMemoryBackend) GetGlossary(id string) (*Glossary, error) {
	b.mu.RLock("GetGlossary")
	defer b.mu.RUnlock()

	g, ok := b.glossaries.Get(id)
	if !ok {
		return nil, fmt.Errorf("glossary %q not found: %w", id, ErrNotFound)
	}

	return cloneGlossary(g), nil
}

// UpdateGlossary updates a glossary's name and/or description. A nil field
// leaves the current value unchanged, matching UpdateGlossaryInput's optional
// Name/Description members.
func (b *InMemoryBackend) UpdateGlossary(id string, name, description *string) (*Glossary, error) {
	b.mu.Lock("UpdateGlossary")
	defer b.mu.Unlock()

	g, ok := b.glossaries.Get(id)
	if !ok {
		return nil, fmt.Errorf("glossary %q not found: %w", id, ErrNotFound)
	}

	if name != nil {
		g.Name = *name
	}
	if description != nil {
		g.Description = *description
	}

	return cloneGlossary(g), nil
}

// DeleteGlossary deletes a glossary. Per AWS's documented behavior (confirmed
// in deserializers.go's error switch for DeleteGlossary, which lists
// ConflictException), a glossary cannot be deleted while it still contains
// glossary terms.
func (b *InMemoryBackend) DeleteGlossary(id string) error {
	b.mu.Lock("DeleteGlossary")
	defer b.mu.Unlock()

	if !b.glossaries.Has(id) {
		return fmt.Errorf("glossary %q not found: %w", id, ErrNotFound)
	}

	for _, t := range b.glossaryTerms.All() {
		if t.GlossaryID == id {
			return fmt.Errorf("%w: glossary %q still contains glossary terms", ErrConflict, id)
		}
	}

	b.glossaries.Delete(id)

	return nil
}

// ListGlossaries returns every glossary, sorted by ID for deterministic
// pagination (see paginateSlice in handler.go).
func (b *InMemoryBackend) ListGlossaries() []*Glossary {
	b.mu.RLock("ListGlossaries")
	defer b.mu.RUnlock()

	src := b.glossaries.All()
	out := make([]*Glossary, 0, len(src))
	for _, g := range src {
		out = append(out, cloneGlossary(g))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// CreateGlossaryTerm creates a term within an existing glossary.
func (b *InMemoryBackend) CreateGlossaryTerm(glossaryID, name, shortDesc, longDesc string) (*GlossaryTerm, error) {
	if glossaryID == "" {
		return nil, fmt.Errorf("%w: GlossaryIdentifier is required", ErrValidation)
	}
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.mu.Lock("CreateGlossaryTerm")
	defer b.mu.Unlock()

	if !b.glossaries.Has(glossaryID) {
		return nil, fmt.Errorf("glossary %q not found: %w", glossaryID, ErrNotFound)
	}

	t := &GlossaryTerm{
		ID:               "term-" + uuid.NewString()[:8],
		GlossaryID:       glossaryID,
		Name:             name,
		ShortDescription: shortDesc,
		LongDescription:  longDesc,
	}
	b.glossaryTerms.Put(t)

	return cloneGlossaryTerm(t), nil
}

// GetGlossaryTerm returns a glossary term by its identifier.
func (b *InMemoryBackend) GetGlossaryTerm(id string) (*GlossaryTerm, error) {
	b.mu.RLock("GetGlossaryTerm")
	defer b.mu.RUnlock()

	t, ok := b.glossaryTerms.Get(id)
	if !ok {
		return nil, fmt.Errorf("glossary term %q not found: %w", id, ErrNotFound)
	}

	return cloneGlossaryTerm(t), nil
}

// UpdateGlossaryTerm updates a term's name and/or descriptions. A nil field
// leaves the current value unchanged, matching UpdateGlossaryTermInput's
// optional members.
func (b *InMemoryBackend) UpdateGlossaryTerm(id string, name, shortDesc, longDesc *string) (*GlossaryTerm, error) {
	b.mu.Lock("UpdateGlossaryTerm")
	defer b.mu.Unlock()

	t, ok := b.glossaryTerms.Get(id)
	if !ok {
		return nil, fmt.Errorf("glossary term %q not found: %w", id, ErrNotFound)
	}

	if name != nil {
		t.Name = *name
	}
	if shortDesc != nil {
		t.ShortDescription = *shortDesc
	}
	if longDesc != nil {
		t.LongDescription = *longDesc
	}

	return cloneGlossaryTerm(t), nil
}

// DeleteGlossaryTerm deletes a glossary term. Beyond the primary-table
// delete, this also disassociates the term from any asset (and any iterable
// form item) that referenced it, so GetAsset/BatchGetIterableForms never
// return a dangling glossary-term ID for a term that no longer exists. This
// cleanup is not separately documented by DeleteGlossaryTerm's own shape, but
// is the same referential-integrity discipline this backend already applies
// to every other cascade (e.g. BatchDeleteTable cascading to partitions).
func (b *InMemoryBackend) DeleteGlossaryTerm(id string) error {
	b.mu.Lock("DeleteGlossaryTerm")
	defer b.mu.Unlock()

	if !b.glossaryTerms.Has(id) {
		return fmt.Errorf("glossary term %q not found: %w", id, ErrNotFound)
	}

	b.glossaryTerms.Delete(id)
	b.disassociateTermEverywhereLocked(id)

	return nil
}

// disassociateTermEverywhereLocked removes termID from every asset's
// GlossaryTerms and from every iterable form item's GlossaryTerms. Caller
// must hold b.mu for writing.
func (b *InMemoryBackend) disassociateTermEverywhereLocked(termID string) {
	for _, a := range b.assets.All() {
		a.GlossaryTerms = removeString(a.GlossaryTerms, termID)
	}

	for _, forms := range b.iterableFormItems {
		for _, items := range forms {
			for _, item := range items {
				item.GlossaryTerms = removeString(item.GlossaryTerms, termID)
			}
		}
	}
}

// removeString returns a copy of s with every occurrence of v removed,
// preserving order. A nil slice in produces a nil slice out.
func removeString(s []string, v string) []string {
	if s == nil {
		return nil
	}

	out := make([]string, 0, len(s))
	for _, item := range s {
		if item != v {
			out = append(out, item)
		}
	}

	return out
}

// ListGlossaryTerms returns every term belonging to a glossary, sorted by ID.
func (b *InMemoryBackend) ListGlossaryTerms(glossaryID string) ([]*GlossaryTerm, error) {
	b.mu.RLock("ListGlossaryTerms")
	defer b.mu.RUnlock()

	if !b.glossaries.Has(glossaryID) {
		return nil, fmt.Errorf("glossary %q not found: %w", glossaryID, ErrNotFound)
	}

	out := make([]*GlossaryTerm, 0)
	for _, t := range b.glossaryTerms.All() {
		if t.GlossaryID == glossaryID {
			out = append(out, cloneGlossaryTerm(t))
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}

// AssociateGlossaryTerms associates one or more glossary terms with an asset,
// returning the asset's complete (deduplicated) set of associated term IDs.
func (b *InMemoryBackend) AssociateGlossaryTerms(assetID string, termIDs []string) ([]string, error) {
	b.mu.Lock("AssociateGlossaryTerms")
	defer b.mu.Unlock()

	a, ok := b.assets.Get(assetID)
	if !ok {
		return nil, fmt.Errorf("asset %q not found: %w", assetID, ErrNotFound)
	}

	for _, tid := range termIDs {
		if !b.glossaryTerms.Has(tid) {
			return nil, fmt.Errorf("glossary term %q not found: %w", tid, ErrNotFound)
		}
	}

	existing := make(map[string]bool, len(a.GlossaryTerms))
	for _, tid := range a.GlossaryTerms {
		existing[tid] = true
	}

	for _, tid := range termIDs {
		if !existing[tid] {
			a.GlossaryTerms = append(a.GlossaryTerms, tid)
			existing[tid] = true
		}
	}

	a.UpdatedAt = nowEpochSeconds()

	return append([]string(nil), a.GlossaryTerms...), nil
}

// DisassociateGlossaryTerms removes one or more glossary terms from an
// asset's associations, returning the asset's remaining term IDs.
func (b *InMemoryBackend) DisassociateGlossaryTerms(assetID string, termIDs []string) ([]string, error) {
	b.mu.Lock("DisassociateGlossaryTerms")
	defer b.mu.Unlock()

	a, ok := b.assets.Get(assetID)
	if !ok {
		return nil, fmt.Errorf("asset %q not found: %w", assetID, ErrNotFound)
	}

	for _, tid := range termIDs {
		if !b.glossaryTerms.Has(tid) {
			return nil, fmt.Errorf("glossary term %q not found: %w", tid, ErrNotFound)
		}
	}

	for _, tid := range termIDs {
		a.GlossaryTerms = removeString(a.GlossaryTerms, tid)
	}

	a.UpdatedAt = nowEpochSeconds()

	return append([]string(nil), a.GlossaryTerms...), nil
}
