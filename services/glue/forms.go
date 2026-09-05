package glue

import (
	"fmt"
	"sort"
	"unicode"
)

// FormType defines the schema for structured metadata that can be attached to
// assets. Field-diffed against PutFormTypeOutput/GetFormTypeOutput, which
// share exactly these three fields.
type FormType struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Schema string `json:"Schema,omitempty"`
}

// FormTypeItem is the summary shape returned by ListFormTypes, field-diffed
// against types.FormTypeItem (Id/Name only -- no Schema).
type FormTypeItem struct {
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

func cloneFormType(f *FormType) *FormType {
	cp := *f

	return &cp
}

// PutFormType creates or updates (upsert, keyed by Name) a form type. AWS
// documents that Name "[m]ust start with an uppercase letter".
func (b *InMemoryBackend) PutFormType(name, schema string) (*FormType, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	if r := []rune(name)[0]; !unicode.IsUpper(r) {
		return nil, fmt.Errorf("%w: Name must start with an uppercase letter", ErrValidation)
	}
	if schema == "" {
		return nil, fmt.Errorf("%w: Schema is required", ErrValidation)
	}

	b.mu.Lock("PutFormType")
	defer b.mu.Unlock()

	ft := &FormType{ID: name, Name: name, Schema: schema}
	b.formTypes.Put(ft)

	return cloneFormType(ft), nil
}

// GetFormType returns a form type by its identifier.
func (b *InMemoryBackend) GetFormType(id string) (*FormType, error) {
	b.mu.RLock("GetFormType")
	defer b.mu.RUnlock()

	ft, ok := b.formTypes.Get(id)
	if !ok {
		return nil, fmt.Errorf("form type %q not found: %w", id, ErrNotFound)
	}

	return cloneFormType(ft), nil
}

// DeleteFormType deletes a form type. Per AWS's documented behavior
// (confirmed in deserializers.go's error switch for DeleteFormType, which
// lists ConflictException), a form type cannot be deleted while it is still
// referenced by an asset type's Forms. DeleteFormType's error switch has no
// EntityNotFoundException case (unlike GetFormType's), so an unknown
// Identifier surfaces as InvalidInputException instead.
func (b *InMemoryBackend) DeleteFormType(id string) error {
	b.mu.Lock("DeleteFormType")
	defer b.mu.Unlock()

	if !b.formTypes.Has(id) {
		return fmt.Errorf("form type %q not found: %w", id, ErrValidation)
	}

	for _, at := range b.assetTypes.All() {
		for _, ref := range at.Forms {
			if ref.FormTypeIdentifier == id {
				return fmt.Errorf("%w: form type %q is still referenced by asset type %q", ErrConflict, id, at.ID)
			}
		}
	}

	b.formTypes.Delete(id)

	return nil
}

// ListFormTypes returns every form type, sorted by ID.
func (b *InMemoryBackend) ListFormTypes() []*FormType {
	b.mu.RLock("ListFormTypes")
	defer b.mu.RUnlock()

	src := b.formTypes.All()
	out := make([]*FormType, 0, len(src))
	for _, ft := range src {
		out = append(out, cloneFormType(ft))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}
